// /addons/whatsapp_client/client.js
// WhatsApp Client — event-driven bridge for HA addons
// v2.0.5 — Baileys engine (no Chromium/Puppeteer)

const { default: makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion, makeCacheableSignalKeyStore, Browsers } = require('@whiskeysockets/baileys');
const QRCode = require('qrcode');
const axios = require('axios');
const express = require('express');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');
const pino = require('pino');

// ────────────────────────────────────────────────────────────
// TIMESTAMPED LOGGING
// ────────────────────────────────────────────────────────────
const _origLog = console.log;
const _origErr = console.error;
const _ts = () => new Date().toLocaleString('he-IL', { timeZone: 'Asia/Jerusalem', hour: '2-digit', minute: '2-digit', second: '2-digit', day: '2-digit', month: '2-digit' });
console.log = (...args) => _origLog(`[${_ts()}]`, ...args);
console.error = (...args) => _origErr(`[${_ts()}]`, ...args);

// Baileys logger — suppress noisy internal logs
const logger = pino({ level: 'error' });

// ────────────────────────────────────────────────────────────
// ERROR RECOVERY
// ────────────────────────────────────────────────────────────
process.on('unhandledRejection', (reason) => {
  const msg = (reason && reason.message) ? reason.message : String(reason);
  console.error('[WARN] Unhandled Rejection:', msg);
});

process.on('uncaughtException', (err) => {
  console.error('[FATAL] Uncaught Exception:', err.message);
  // Give time to flush logs, then exit for supervisor restart
  setTimeout(() => process.exit(1), 3000);
});

// ────────────────────────────────────────────────────────────
// CONFIG
// ────────────────────────────────────────────────────────────
const SUPERVISOR_TOKEN = process.env.SUPERVISOR_TOKEN;
const INGRESS_PORT = 3001;
const COMMAND_DELAY_MS = 3000; // 3s between commands (Baileys is faster than Puppeteer)
const COMMAND_TIMEOUT_MS = 30000; // 30s max per command
const HEARTBEAT_INTERVAL_MS = 2 * 60 * 1000;
const WS_RECONNECT_DELAY_MS = 5000;
const AUTH_DIR = '/data/baileys_auth';

let options = {};
try {
  options = JSON.parse(fs.readFileSync('/data/options.json', 'utf8'));
  console.log('Loaded options:', JSON.stringify(options));
} catch (_) {
  console.log('No options.json, using defaults.');
}

const CONNECTED_NUMBER = options.CONNECTED_NUMBER || '';
const TEST_MESSAGE = options.TEST_MESSAGE !== undefined ? options.TEST_MESSAGE : true;
const RESTART_HOURS = options.RESTART_HOURS || 0;
const SAFE_MODE = options.SAFE_MODE || false;
const MIGRATION_FLAG = '/data/' + (options.MIGRATION_FLAG || '.migrated_v200_baileys');

if (!SUPERVISOR_TOKEN) {
  console.error('WARNING: SUPERVISOR_TOKEN not available — HA integration will not work.');
}

// ────────────────────────────────────────────────────────────
// STATE
// ────────────────────────────────────────────────────────────
let connectionStatus = 'initializing';
let currentQRDataUrl = null;
let clientReady = false;
let lastHeartbeat = null;
let heartbeatCount = 0;
let recentMessages = [];
let sock = null;
let connectedNumber = null;
let connectedAt = 0; // timestamp (seconds) when we connected — ignore older messages
let isFirstConnect = true; // only set connectedAt on first connection, not reconnects
let restartTimer = null; // track scheduled restart to avoid stacking on reconnect
let reconnectAttempts = 0; // exponential backoff counter for reconnects

function pushRecentMessage(sender, body, timestamp) {
  recentMessages.push({
    sender,
    body: (body || '').substring(0, 50),
    time: new Date((timestamp || 0) * 1000).toLocaleString('he-IL', { timeZone: 'Asia/Jerusalem', hour: '2-digit', minute: '2-digit' })
  });
  if (recentMessages.length > 3) recentMessages.shift();
}

// ────────────────────────────────────────────────────────────
// HELPERS
// ────────────────────────────────────────────────────────────
function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

function withTimeout(promise, ms, label) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
    promise.then(
      (val) => { clearTimeout(timer); resolve(val); },
      (err) => { clearTimeout(timer); reject(err); }
    );
  });
}

async function retry(fn, label, maxRetries = 3, baseDelayMs = 5000) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    if (!clientReady) throw new Error('WhatsApp not connected');
    try {
      return await withTimeout(fn(), COMMAND_TIMEOUT_MS, label);
    } catch (err) {
      console.error(`[${label}] Attempt ${attempt}/${maxRetries} failed: ${err.message}`);
      if (!clientReady) throw new Error('WhatsApp disconnected during operation');
      if (attempt === maxRetries) throw err;
      await delay(baseDelayMs * attempt);
    }
  }
}

// ────────────────────────────────────────────────────────────
// HA EVENT HELPERS
// ────────────────────────────────────────────────────────────
async function fireHAEvent(eventType, eventData) {
  if (!SUPERVISOR_TOKEN) return;
  try {
    await axios.post(
      `http://supervisor/core/api/events/${eventType}`,
      eventData,
      {
        headers: {
          'Authorization': `Bearer ${SUPERVISOR_TOKEN}`,
          'Content-Type': 'application/json'
        },
        timeout: 10000
      }
    );
  } catch (err) {
    console.error(`[HA] Failed to fire ${eventType}: ${err.message}`);
  }
}

// ────────────────────────────────────────────────────────────
// BAILEYS — MESSAGE STORE (for quoted replies & reactions)
// ────────────────────────────────────────────────────────────
const messageStore = new Map(); // serialized key -> message
const MAX_STORE_SIZE = 5000;

function storeMessage(msg) {
  if (!msg.key || !msg.key.id) return;
  const key = `${msg.key.remoteJid}|${msg.key.id}`;
  messageStore.set(key, msg);
  // Evict oldest if store too large
  if (messageStore.size > MAX_STORE_SIZE) {
    const firstKey = messageStore.keys().next().value;
    messageStore.delete(firstKey);
  }
}

function findStoredMessage(chatId, messageId) {
  // Try direct lookup
  const key = `${chatId}|${messageId}`;
  if (messageStore.has(key)) return messageStore.get(key);
  // Search all keys (messageId might be from a different serialization)
  for (const [k, v] of messageStore) {
    if (v.key && v.key.id === messageId) return v;
  }
  return null;
}

// ────────────────────────────────────────────────────────────
// BAILEYS — CONNECTION
// ────────────────────────────────────────────────────────────
async function startBaileys() {
  // Ensure auth directory exists
  if (!fs.existsSync(AUTH_DIR)) {
    fs.mkdirSync(AUTH_DIR, { recursive: true });
  }

  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);

  const { version } = await fetchLatestBaileysVersion();
  console.log(`[INIT] Baileys version: ${version.join('.')}`);
  console.log(`[INIT] Auth directory: ${AUTH_DIR}`);

  sock = makeWASocket({
    version,
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys, logger),
    },
    logger,
    browser: Browsers.ubuntu('HA-WhatsApp'),
    generateHighQualityLinkPreview: false,
    markOnlineOnConnect: false,
    // getMessage for retry system
    getMessage: async (key) => {
      const stored = findStoredMessage(key.remoteJid, key.id);
      return stored?.message || undefined;
    }
  });

  // --- Save credentials on update ---
  sock.ev.on('creds.update', saveCreds);

  // --- Connection updates ---
  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;

    // QR Code
    if (qr) {
      console.log('');
      console.log('╔══════════════════════════════════════════╗');
      console.log('║   SCAN THIS QR CODE WITH WHATSAPP        ║');
      console.log('╚══════════════════════════════════════════╝');
      console.log('');

      connectionStatus = 'qr_required';
      try {
        currentQRDataUrl = await QRCode.toDataURL(qr, { width: 300 });
      } catch (e) {
        console.error('Failed to generate QR data URL:', e.message);
      }

      fireHAEvent('whatsapp_status', { status: 'qr_required', timestamp: Date.now() });
    }

    // Connected
    if (connection === 'open') {
      clientReady = true;
      connectionStatus = 'connected';
      currentQRDataUrl = null;

      // Extract connected number
      try {
        connectedNumber = sock.user?.id?.split(':')[0] || sock.user?.id?.split('@')[0] || 'unknown';
      } catch (_) {
        connectedNumber = 'unknown';
      }

      // Reset reconnect backoff on successful connection
      reconnectAttempts = 0;

      // Track connection time — only on first connect to avoid dropping
      // messages that arrived during a brief disconnect gap on reconnects
      if (isFirstConnect) {
        connectedAt = Math.floor(Date.now() / 1000);
        isFirstConnect = false;
      }

      console.log('');
      console.log('╔══════════════════════════════════════════╗');
      console.log(`║   ✅ CONNECTED as ${connectedNumber}            ║`);
      console.log('╚══════════════════════════════════════════╝');
      console.log('');

      console.log(`[DEBUG] WID: ${sock.user?.id}`);
      console.log(`[DEBUG] Platform: Baileys (no Chromium)`);
      console.log(`[DEBUG] Pushname: ${sock.user?.name || 'N/A'}`);

      fireHAEvent('whatsapp_status', { status: 'connected', timestamp: Date.now() });

      // Send test message
      if (TEST_MESSAGE && connectedNumber !== 'unknown') {
        try {
          const testJid = connectedNumber + '@s.whatsapp.net';
          await sock.sendMessage(testJid, { text: '✅ WhatsApp Client connected successfully! (Baileys engine — no Chromium)' });
          console.log(`[TEST] ✅ Test message sent to self (${connectedNumber})`);
        } catch (err) {
          console.error(`[TEST] ❌ Test message failed: ${err.message}`);
        }
      }

      // Schedule restart if configured (clear previous timer to avoid stacking on reconnect)
      if (RESTART_HOURS > 0) {
        if (restartTimer) clearTimeout(restartTimer);
        const restartMs = RESTART_HOURS * 60 * 60 * 1000;
        console.log(`[RESTART] Scheduled automatic restart in ${RESTART_HOURS} hour(s)`);
        restartTimer = setTimeout(() => {
          console.log('');
          console.log('╔══════════════════════════════════════════╗');
          console.log('║   🔄 SCHEDULED RESTART                    ║');
          console.log('╚══════════════════════════════════════════╝');
          console.log(`[RESTART] Uptime: ${RESTART_HOURS * 60} minutes`);
          console.log(`[RESTART] Status: ${connectionStatus}`);
          console.log(`[RESTART] Heartbeats sent: ${heartbeatCount}`);
          console.log(`[RESTART] Command queue depth: ${getTotalQueueLength()}`);
          console.log('[RESTART] Closing WhatsApp connection cleanly...');
          clientReady = false;
          if (sock) {
            try { sock.end(); } catch (_) {}
          }
          setTimeout(() => {
            console.log('[RESTART] Exiting now — supervisor will restart...');
            process.exit(0);
          }, 2000);
        }, restartMs);
      }
    }

    // Disconnected
    if (connection === 'close') {
      clientReady = false;
      connectionStatus = 'disconnected';

      const statusCode = lastDisconnect?.error?.output?.statusCode;
      const reason = lastDisconnect?.error?.message || 'unknown';
      console.error(`[Disconnected] Code: ${statusCode}, Reason: ${reason}`);

      fireHAEvent('whatsapp_status', { status: 'disconnected', reason, timestamp: Date.now() });

      // Check if we should reconnect
      const shouldReconnect = statusCode !== DisconnectReason.loggedOut;

      if (shouldReconnect) {
        // Clean up old socket to prevent stacking event listeners
        if (sock) {
          try { sock.ev.removeAllListeners(); } catch (_) {}
        }
        reconnectAttempts++;
        const backoffMs = Math.min(5000 * Math.pow(2, reconnectAttempts - 1), 60000);
        console.log(`[RECONNECT] Reconnecting in ${backoffMs / 1000}s (attempt ${reconnectAttempts})...`);
        await delay(backoffMs);
        startBaileys();
      } else {
        console.error('[LOGOUT] Session was logged out — clearing auth and exiting.');
        try {
          fs.rmSync(AUTH_DIR, { recursive: true, force: true });
        } catch (_) {}
        // Exit so supervisor restarts fresh with QR scan
        setTimeout(() => process.exit(1), 3000);
      }
    }
  });

  // ────────────────────────────────────────────────────────────
  // MESSAGE LISTENERS — fire HA events for each message
  // ────────────────────────────────────────────────────────────

  sock.ev.on('messaging-history.set', async ({ chats }) => {
    try {
      const groups = (chats || []).filter(c => c.id && c.id.endsWith('@g.us'));
      
      // Sort by conversationTimestamp descending (most recent first)
      groups.sort((a, b) => {
        const timeA = a.conversationTimestamp ? Number(a.conversationTimestamp) : 0;
        const timeB = b.conversationTimestamp ? Number(b.conversationTimestamp) : 0;
        return timeB - timeA;
      });

      // Take top 20
      const top20 = groups.slice(0, 20);

      console.log('');
      console.log('╔══════════════════════════════════════════╗');
      console.log('║   👥 LAST 20 ACTIVE GROUPS (STARTUP)      ║');
      console.log('╚══════════════════════════════════════════╝');
      for (const g of top20) {
        const name = g.name || g.subject || 'Unknown Name';
        const dateStr = g.conversationTimestamp 
          ? new Date(Number(g.conversationTimestamp) * 1000).toLocaleString('he-IL', { timeZone: 'Asia/Jerusalem', dateStyle: 'short', timeStyle: 'short' })
          : 'N/A';
        console.log(`• [${dateStr}] ${name}: ${g.id}`);
      }
      console.log('╚══════════════════════════════════════════╝');
      console.log('');
    } catch (err) {
      console.error('[GROUPS] Failed to process active groups history:', err.message);
    }
  });

  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    try {
      if (!clientReady) return;

      for (const msg of messages) {
        // Store message for later lookups (quoted replies, reactions)
        storeMessage(msg);

        // Skip protocol/system messages
        if (!msg.message) continue;

        // Skip old messages from initial history sync (only process new ones)
        const timestamp = msg.messageTimestamp ? Number(msg.messageTimestamp) : 0;
        if (timestamp > 0 && timestamp < connectedAt - 30) {
          // Store for reactions/replies but don't fire HA events
          continue;
        }

        const body = msg.message.conversation ||
                     msg.message.extendedTextMessage?.text ||
                     msg.message.imageMessage?.caption ||
                     msg.message.videoMessage?.caption ||
                     msg.message.documentMessage?.caption ||
                     '';

        const fromMe = msg.key.fromMe || false;
        const remoteJid = msg.key.remoteJid || '';
        const isGroup = remoteJid.endsWith('@g.us');
        const sender = msg.pushName || msg.key.participant || msg.key.remoteJid || 'Unknown';
        const msgTimestamp = timestamp || Math.floor(Date.now() / 1000);
        const messageId = msg.key.id || `${msgTimestamp}-${sender}`;
        const hasMedia = !!(msg.message.imageMessage || msg.message.videoMessage ||
                           msg.message.audioMessage || msg.message.documentMessage ||
                           msg.message.stickerMessage);

        if (fromMe) {
          // Self-sent messages (for notes group etc.)
          const groupId = remoteJid;
          console.log(`[MSG_CREATE] Self-sent to ${groupId}: ${body.substring(0, 80)}...`);
          pushRecentMessage('Me', body, msgTimestamp);

          fireHAEvent('whatsapp_message_create', {
            group_id: groupId,
            body: body,
            timestamp: msgTimestamp,
            message_id: messageId,
            from_me: true
          });
        } else {
          // Messages from others
          console.log(`[MSG] ${isGroup ? 'Group' : 'DM'} ${remoteJid}: ${body.substring(0, 80)}...`);
          pushRecentMessage(sender, body, msgTimestamp);

          fireHAEvent('whatsapp_message', {
            group_id: remoteJid,
            sender: sender,
            body: body,
            timestamp: msgTimestamp,
            message_id: messageId,
            is_group: isGroup,
            from_me: false,
            has_media: hasMedia
          });
        }
      }
    } catch (error) {
      console.error('Error handling messages.upsert:', error.message);
    }
  });

  // Message edits
  sock.ev.on('messages.update', async (updates) => {
    try {
      if (!clientReady) return;

      for (const update of updates) {
        // Detect edited messages
        if (update.update?.message) {
          const editedMsg = update.update.message;
          const newBody = editedMsg.conversation ||
                          editedMsg.extendedTextMessage?.text ||
                          editedMsg.protocolMessage?.editedMessage?.message?.conversation ||
                          editedMsg.protocolMessage?.editedMessage?.message?.extendedTextMessage?.text ||
                          '';

          if (newBody && update.key) {
            const groupId = update.key.remoteJid || '';
            const messageId = update.key.id || '';

            console.log(`[EDIT] ${groupId}: ${newBody.substring(0, 80)}...`);

            fireHAEvent('whatsapp_message_edit', {
              group_id: groupId,
              message_id: messageId,
              body: newBody,
              new_body: newBody,
              prev_body: '',
              from_me: update.key.fromMe || false,
              timestamp: Math.floor(Date.now() / 1000)
            });
          }
        }
      }
    } catch (error) {
      console.error('Error handling messages.update:', error.message);
    }
  });
}

// ────────────────────────────────────────────────────────────
// COMMAND QUEUE — true round-robin by app_id with delay
// ────────────────────────────────────────────────────────────
const appQueues = new Map();
let roundRobinOrder = [];
let roundRobinIndex = 0;
let processingCommand = false;

function getTotalQueueLength() {
  let total = 0;
  for (const q of appQueues.values()) total += q.length;
  return total;
}

async function handleCommand(eventType, eventData) {
  const requestId = eventData.request_id || `auto-${Date.now()}`;
  const appId = eventData.app_id || 'unknown';

  if (!appQueues.has(appId)) {
    appQueues.set(appId, []);
    roundRobinOrder.push(appId);
  }
  appQueues.get(appId).push({ eventType, eventData, requestId });

  console.log(`[CMD] Queued ${eventType} from [${appId}] (${requestId}) — total: ${getTotalQueueLength()}`);
  processCommandQueue();
}

async function processCommandQueue() {
  if (processingCommand) return;
  if (getTotalQueueLength() === 0) return;

  // Wait for client to be ready
  if (!clientReady) {
    console.log(`[CMD] Client not ready — deferring ${getTotalQueueLength()} queued commands`);
    setTimeout(processCommandQueue, 10000);
    return;
  }

  roundRobinOrder = roundRobinOrder.filter(id => appQueues.has(id) && appQueues.get(id).length > 0);
  if (roundRobinOrder.length === 0) return;

  roundRobinIndex = roundRobinIndex % roundRobinOrder.length;
  const appId = roundRobinOrder[roundRobinIndex];
  roundRobinIndex++;

  const queue = appQueues.get(appId);
  const { eventType, eventData, requestId } = queue.shift();

  if (queue.length === 0) {
    appQueues.delete(appId);
  }

  processingCommand = true;
  console.log(`[CMD] Processing ${eventType} from [${appId}] (${requestId})... [${getTotalQueueLength()} remaining]`);

  if (SAFE_MODE) {
    console.log(`[SAFE] Skipping command in safe mode: ${eventType}`);
    fireHAEvent('whatsapp_response', {
      request_id: requestId,
      command: eventType.replace('whatsapp_command_', ''),
      success: false,
      error: 'Safe mode is enabled — commands are disabled'
    });
    processingCommand = false;
    setTimeout(processCommandQueue, COMMAND_DELAY_MS);
    return;
  }

  try {
    switch (eventType) {
      case 'whatsapp_command_send': {
        if (!clientReady || !sock) throw new Error('WhatsApp not connected');
        const { target_id, message, quoted_message_id } = eventData;
        if (!target_id || !message) throw new Error('target_id and message required');

        const sendOptions = {};
        if (quoted_message_id) {
          const quotedMsg = findStoredMessage(target_id, quoted_message_id);
          if (quotedMsg) {
            sendOptions.quoted = quotedMsg;
          }
        }

        await retry(() => sock.sendMessage(target_id, { text: message }, sendOptions), `send-${target_id}`);
        console.log(`[CMD] ✅ Message sent to ${target_id}`);

        fireHAEvent('whatsapp_response', { request_id: requestId, command: 'send', success: true });
        break;
      }

      case 'whatsapp_command_fetch': {
        if (!clientReady || !sock) throw new Error('WhatsApp not connected');
        const { group_id, limit = 50 } = eventData;
        if (!group_id) throw new Error('group_id required');

        // Fetch messages using store — Baileys doesn't have a direct fetchMessages like wwebjs
        // We collect from our message store
        const fetchedMessages = [];
        for (const [, msg] of messageStore) {
          if (msg.key && msg.key.remoteJid === group_id) {
            const body = msg.message?.conversation ||
                         msg.message?.extendedTextMessage?.text ||
                         msg.message?.imageMessage?.caption ||
                         '';
            fetchedMessages.push({
              body: body,
              timestamp: msg.messageTimestamp ? Number(msg.messageTimestamp) : 0,
              sender: msg.pushName || msg.key.participant || 'Unknown',
              message_id: msg.key.id || null,
              from_me: msg.key.fromMe || false
            });
          }
        }

        // Sort by timestamp descending and limit
        fetchedMessages.sort((a, b) => b.timestamp - a.timestamp);
        const msgData = fetchedMessages.slice(0, Math.min(limit, 200));

        console.log(`[CMD] ✅ Fetched ${msgData.length} messages from ${group_id}`);
        fireHAEvent('whatsapp_response', { request_id: requestId, command: 'fetch', success: true, data: msgData });
        break;
      }

      case 'whatsapp_command_react': {
        if (!clientReady || !sock) throw new Error('WhatsApp not connected');
        const { message_id, chat_id, emoji } = eventData;
        if (!message_id || !chat_id || !emoji) throw new Error('message_id, chat_id, and emoji required');

        try {
          const storedMsg = findStoredMessage(chat_id, message_id);
          if (storedMsg) {
            await sock.sendMessage(chat_id, {
              react: { text: emoji, key: storedMsg.key }
            });
            console.log(`[CMD] ✅ Reacted ${emoji} to ${message_id}`);
          } else {
            // Construct key manually if not in store
            await sock.sendMessage(chat_id, {
              react: {
                text: emoji,
                key: {
                  remoteJid: chat_id,
                  id: message_id,
                  fromMe: false
                }
              }
            });
            console.log(`[CMD] ✅ Reacted ${emoji} to ${message_id} (key reconstructed)`);
          }
          fireHAEvent('whatsapp_response', { request_id: requestId, command: 'react', success: true });
        } catch (reactErr) {
          console.error(`[CMD] ❌ React failed: ${reactErr.message}`);
          fireHAEvent('whatsapp_response', { request_id: requestId, command: 'react', success: false, error: reactErr.message });
        }
        break;
      }

      case 'whatsapp_command_list_groups': {
        if (!clientReady || !sock) throw new Error('WhatsApp not connected');

        const groups = await retry(async () => {
          const participatingGroups = await sock.groupFetchAllParticipating();
          return Object.values(participatingGroups).map(g => ({
            id: g.id,
            name: g.subject || g.id
          }));
        }, 'list-groups');

        console.log(`[CMD] ✅ Listed ${groups.length} groups`);
        fireHAEvent('whatsapp_response', { request_id: requestId, command: 'list_groups', success: true, data: groups });
        break;
      }

      case 'whatsapp_command_status': {
        fireHAEvent('whatsapp_response', {
          request_id: requestId,
          command: 'status',
          success: true,
          data: {
            status: connectionStatus,
            client_ready: clientReady,
            last_heartbeat: lastHeartbeat,
            engine: 'baileys',
            timestamp: Date.now()
          }
        });
        break;
      }

      default:
        console.log(`[CMD] Unknown command: ${eventType}`);
    }
  } catch (err) {
    console.error(`[CMD] ❌ Error ${eventType}: ${err.message}`);
    fireHAEvent('whatsapp_response', {
      request_id: requestId,
      command: eventType.replace('whatsapp_command_', ''),
      success: false,
      error: err.message
    });
  } finally {
    processingCommand = false;
    setTimeout(processCommandQueue, COMMAND_DELAY_MS);
  }
}

// ────────────────────────────────────────────────────────────
// HA WEBSOCKET — subscribe to command events
// ────────────────────────────────────────────────────────────
let wsMessageId = 1;
let wsPingInterval = null;
let haWs = null;

function connectHAWebSocket() {
  if (!SUPERVISOR_TOKEN) {
    console.log('[WS] No SUPERVISOR_TOKEN — skipping WebSocket');
    return;
  }

  console.log('[WS] Connecting to HA WebSocket...');
  haWs = new WebSocket('ws://supervisor/core/websocket');

  haWs.on('open', () => {
    console.log('[WS] WebSocket connected');
    if (wsPingInterval) clearInterval(wsPingInterval);
    wsPingInterval = setInterval(() => {
      try { if (haWs && haWs.readyState === WebSocket.OPEN) haWs.ping(); } catch (_) {}
    }, 30000);
  });

  haWs.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());

      if (msg.type === 'auth_required') {
        haWs.send(JSON.stringify({ type: 'auth', access_token: SUPERVISOR_TOKEN }));
        return;
      }

      if (msg.type === 'auth_ok') {
        console.log('[WS] ✅ Authenticated with HA');
        const commands = [
          'whatsapp_command_send',
          'whatsapp_command_fetch',
          'whatsapp_command_react',
          'whatsapp_command_list_groups',
          'whatsapp_command_status'
        ];
        for (const evt of commands) {
          const id = wsMessageId++;
          haWs.send(JSON.stringify({ id, type: 'subscribe_events', event_type: evt }));
          console.log(`[WS] Subscribed to ${evt}`);
        }
        return;
      }

      if (msg.type === 'event' && msg.event) {
        const eventType = msg.event.event_type;
        const eventData = msg.event.data || {};
        if (eventType && eventType.startsWith('whatsapp_command_')) {
          handleCommand(eventType, eventData);
        }
        return;
      }

      if (msg.type === 'auth_invalid') {
        console.error('[WS] Auth failed:', msg.message);
      }
    } catch (err) {
      console.error('[WS] Error:', err.message);
    }
  });

  haWs.on('close', () => {
    console.log('[WS] Disconnected — reconnecting in 5s...');
    if (wsPingInterval) { clearInterval(wsPingInterval); wsPingInterval = null; }
    setTimeout(connectHAWebSocket, WS_RECONNECT_DELAY_MS);
  });

  haWs.on('error', (err) => {
    console.error('[WS] Error:', err.message);
  });
}

// ────────────────────────────────────────────────────────────
// HEARTBEAT
// ────────────────────────────────────────────────────────────
setInterval(() => {
  heartbeatCount++;
  lastHeartbeat = Date.now();
  fireHAEvent('whatsapp_status', {
    status: connectionStatus,
    heartbeat: heartbeatCount,
    timestamp: lastHeartbeat
  });
}, HEARTBEAT_INTERVAL_MS);

// ────────────────────────────────────────────────────────────
// INGRESS WEB UI
// ────────────────────────────────────────────────────────────
const app = express();
app.use(express.json());

app.get('/api/status', (req, res) => {
  res.json({
    status: connectionStatus,
    connected_number: connectedNumber,
    qr_data_url: currentQRDataUrl,
    recent_messages: recentMessages,
    command_queue_length: getTotalQueueLength(),
    command_queues: Object.fromEntries([...appQueues.entries()].map(([k, v]) => [k, v.length])),
    last_heartbeat: lastHeartbeat,
    heartbeat_count: heartbeatCount,
    engine: 'baileys',
  });
});

app.use(express.static(path.join(__dirname, 'public')));

app.listen(INGRESS_PORT, () => {
  console.log(`Ingress running on port ${INGRESS_PORT}`);
});

// ────────────────────────────────────────────────────────────
// STARTUP
// ────────────────────────────────────────────────────────────
function clearOldSession() {
  if (fs.existsSync(MIGRATION_FLAG)) return;

  console.log(`[Migration] Flag "${MIGRATION_FLAG}" not found — clearing session for fresh QR scan...`);
  // Clear old whatsapp-web.js sessions
  for (const dir of ['/data/.wwebjs_auth', '/data/session']) {
    try {
      if (fs.existsSync(dir)) {
        fs.rmSync(dir, { recursive: true, force: true });
        console.log(`[Migration] Cleared: ${dir}`);
      }
    } catch (err) {
      console.error(`[Migration] Failed to clear ${dir}: ${err.message}`);
    }
  }
  // Clear Baileys auth
  try {
    if (fs.existsSync(AUTH_DIR)) {
      fs.rmSync(AUTH_DIR, { recursive: true, force: true });
      console.log(`[Migration] Cleared: ${AUTH_DIR}`);
    }
  } catch (err) {
    console.error(`[Migration] Failed to clear ${AUTH_DIR}: ${err.message}`);
  }
  // Clean up old migration flags
  try {
    const dataFiles = fs.readdirSync('/data/');
    for (const f of dataFiles) {
      if (f.startsWith('.migrated_')) {
        fs.unlinkSync(`/data/${f}`);
        console.log(`[Migration] Removed old flag: ${f}`);
      }
    }
  } catch (_) {}
  try { fs.writeFileSync(MIGRATION_FLAG, new Date().toISOString()); } catch {}
  console.log('[Migration] Done — will prompt for fresh QR scan.');
}

async function main() {
  console.log(`[CONFIG] CONNECTED_NUMBER: ${CONNECTED_NUMBER || '(auto-detect)'}`);
  console.log(`[CONFIG] RESTART_HOURS: ${RESTART_HOURS || 'disabled'}`);
  console.log(`[CONFIG] TEST_MESSAGE: ${TEST_MESSAGE}`);
  console.log(`[CONFIG] SAFE_MODE: ${SAFE_MODE}`);
  console.log(`[CONFIG] MIGRATION_FLAG: ${MIGRATION_FLAG}`);
  console.log(`[CONFIG] Node.js: ${process.version}`);
  console.log(`[CONFIG] Platform: ${process.platform} ${process.arch}`);
  console.log('');
  console.log('╔══════════════════════════════════════════╗');
  console.log('║   WhatsApp Client for HA                  ║');
  console.log('║   v2.0.5 • Baileys • No Chromium          ║');
  console.log('╚══════════════════════════════════════════╝');
  console.log('');

  clearOldSession();

  // Connect to HA WebSocket for command events
  connectHAWebSocket();

  console.log('Initializing WhatsApp client (Baileys)...');
  try {
    await startBaileys();
    console.log('✅ Baileys socket created');
  } catch (err) {
    console.error('❌ Init failed:', err.message);
    console.error(err.stack);
    process.exit(1);
  }
}

main();

// Clean disconnect on exit signals
function handleExitSignal(signal) {
  console.log(`[EXIT] Received ${signal} — closing connection cleanly...`);
  clientReady = false;
  if (sock) {
    try { sock.end(); } catch (_) {}
  }
  setTimeout(() => {
    console.log('[EXIT] Exiting now.');
    process.exit(0);
  }, 2000);
}

process.on('SIGTERM', () => handleExitSignal('SIGTERM'));
process.on('SIGINT', () => handleExitSignal('SIGINT'));
