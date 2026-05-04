// /addons/whatsapp_client/client.js
// WhatsApp Client — event-driven bridge for HA addons
const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const QRCode = require('qrcode');
const axios = require('axios');
const express = require('express');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');

// ────────────────────────────────────────────────────────────
// TIMESTAMPED LOGGING
// ────────────────────────────────────────────────────────────
const _origLog = console.log;
const _origErr = console.error;
const _ts = () => new Date().toLocaleString('he-IL', { timeZone: 'Asia/Jerusalem', hour: '2-digit', minute: '2-digit', second: '2-digit', day: '2-digit', month: '2-digit' });
console.log = (...args) => _origLog(`[${_ts()}]`, ...args);
console.error = (...args) => _origErr(`[${_ts()}]`, ...args);

// ────────────────────────────────────────────────────────────
// ERROR RECOVERY
// ────────────────────────────────────────────────────────────
function isFatalLibraryError(err) {
  const msg = (err && err.message) ? err.message : String(err);
  return msg.includes('Execution context was destroyed') ||
         msg.includes('detached frame') ||
         msg.includes('target closed') ||
         msg.includes('session closed');
}

process.on('unhandledRejection', (reason) => {
  if (isFatalLibraryError(reason)) {
    if (clientReady) {
      console.log('[WARN] Navigation error (non-fatal, client is connected):', reason.message || reason);
      return;
    }
    console.error('[FATAL] Library crash during init — exiting for supervisor restart.');
    setTimeout(() => process.exit(1), 3000);
    return;
  }
  console.error('[FATAL] Unhandled Rejection:', reason);
});

process.on('uncaughtException', (err) => {
  if (isFatalLibraryError(err)) {
    if (clientReady) {
      console.log('[WARN] Navigation error (non-fatal, client is connected):', err.message);
      return;
    }
    console.error('[FATAL] Library crash during init — exiting for supervisor restart.');
    setTimeout(() => process.exit(1), 3000);
    return;
  }
  console.error('[FATAL] Uncaught Exception:', err);
});

// ────────────────────────────────────────────────────────────
// CONFIG
// ────────────────────────────────────────────────────────────
const SUPERVISOR_TOKEN = process.env.SUPERVISOR_TOKEN;
const INGRESS_PORT = 3001;
const COMMAND_DELAY_MS = 5000; // 5s between commands for stability
const HEARTBEAT_INTERVAL_MS = 2 * 60 * 1000; // 2 minutes
const WS_RECONNECT_DELAY_MS = 5000;
const RESTART_INTERVAL_MS = 12 * 60 * 60 * 1000; // 12 hours
const MIGRATION_FLAG = '/data/.migrated_v134';

let options = {};
try {
  options = JSON.parse(fs.readFileSync('/data/options.json', 'utf8'));
  console.log('Loaded options:', JSON.stringify(options));
} catch (_) {
  console.log('No options.json, using defaults.');
}

const TEST_MESSAGE = options.TEST_MESSAGE !== undefined ? options.TEST_MESSAGE : true;

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

async function retry(fn, label, maxRetries = 3, delayMs = 15000) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      console.error(`[${label}] Attempt ${attempt}/${maxRetries} failed: ${err.message}`);
      if (attempt === maxRetries) throw err;
      await delay(delayMs);
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
// WHATSAPP CLIENT
// ────────────────────────────────────────────────────────────
const client = new Client({
  authStrategy: new LocalAuth({ dataPath: '/data' }),
  puppeteer: {
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/chromium',
    protocolTimeout: 600000,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--no-first-run',
      '--no-zygote',
      '--disable-extensions',
    ],
  }
});

// --- QR Code ---
client.on('qr', async (qr) => {
  console.log('');
  console.log('╔══════════════════════════════════════════╗');
  console.log('║   SCAN THIS QR CODE WITH WHATSAPP        ║');
  console.log('╚══════════════════════════════════════════╝');
  qrcode.generate(qr, { small: true });
  console.log('');

  connectionStatus = 'qr_required';
  try {
    currentQRDataUrl = await QRCode.toDataURL(qr, { width: 300 });
  } catch (e) {
    console.error('Failed to generate QR data URL:', e.message);
  }

  fireHAEvent('whatsapp_status', { status: 'qr_required', timestamp: Date.now() });
});

// --- Auth ---
let authLogged = false;
client.on('authenticated', () => {
  if (!authLogged) {
    console.log('[Auth] ✅ Authenticated — session saved');
    authLogged = true;
  }
});

client.on('auth_failure', (msg) => {
  console.error('[Auth] ❌ Authentication failure:', msg);
});

client.on('loading_screen', (percent, message) => {
  console.log(`[Loading] ${percent}% — ${message}`);
  if (clientReady) {
    console.log('[Loading] WhatsApp Web is reloading — pausing commands until reconnected.');
    clientReady = false;
    connectionStatus = 'reconnecting';
  }
});

// --- Ready ---
client.on('ready', async () => {
  clientReady = true;
  connectionStatus = 'connected';
  currentQRDataUrl = null;

  const me = client.info.wid.user;
  console.log('');
  console.log('╔══════════════════════════════════════════╗');
  console.log(`║   ✅ CONNECTED as ${me}            ║`);
  console.log('╚══════════════════════════════════════════╝');
  console.log('');

  fireHAEvent('whatsapp_status', { status: 'connected', timestamp: Date.now() });

  // Send test message to self
  if (TEST_MESSAGE) {
    try {
      const testId = me + '@c.us';
      await client.sendMessage(testId, '✅ WhatsApp Client connected successfully!');
      console.log(`[TEST] ✅ Test message sent to self (${me})`);
    } catch (err) {
      console.error(`[TEST] ❌ Test message failed: ${err.message}`);
    }
  }
});

// --- Disconnected ---
client.on('disconnected', (reason) => {
  console.error('[Disconnected]', reason);
  clientReady = false;
  connectionStatus = 'disconnected';
  fireHAEvent('whatsapp_status', { status: 'disconnected', reason, timestamp: Date.now() });
});

// ────────────────────────────────────────────────────────────
// MESSAGE LISTENERS — fire HA events for each message
// ────────────────────────────────────────────────────────────

// Messages from others
client.on('message', async (msg) => {
  try {
    if (!clientReady) return;

    const sender = (msg._data && msg._data.notifyName) || msg.author || 'Unknown';
    const isGroup = msg.from && msg.from.endsWith('@g.us');
    const messageId = msg.id && msg.id._serialized ? msg.id._serialized : `${msg.timestamp}-${sender}`;

    console.log(`[MSG] ${isGroup ? 'Group' : 'DM'} ${msg.from}: ${(msg.body || '').substring(0, 80)}...`);
    pushRecentMessage(sender, msg.body, msg.timestamp);

    fireHAEvent('whatsapp_message', {
      group_id: msg.from,
      sender: sender,
      body: msg.body || '',
      timestamp: msg.timestamp,
      message_id: messageId,
      is_group: isGroup,
      from_me: false,
      has_media: msg.hasMedia || false
    });
  } catch (error) {
    console.error('Error handling message:', error.message);
  }
});

// Self-sent messages (for notes group etc.)
client.on('message_create', async (msg) => {
  try {
    if (!clientReady) return;
    if (!msg.fromMe) return;

    const groupId = msg.to || msg.from;
    const messageId = msg.id && msg.id._serialized ? msg.id._serialized : `${msg.timestamp}-self`;

    console.log(`[MSG_CREATE] Self-sent to ${groupId}: ${(msg.body || '').substring(0, 80)}...`);
    pushRecentMessage('Me', msg.body, msg.timestamp);

    fireHAEvent('whatsapp_message_create', {
      group_id: groupId,
      body: msg.body || '',
      timestamp: msg.timestamp,
      message_id: messageId,
      from_me: true
    });
  } catch (error) {
    console.error('Error handling message_create:', error.message);
  }
});

// Message edits
client.on('message_edit', async (msg, newBody, prevBody) => {
  try {
    if (!clientReady) return;

    const groupId = msg.from || msg.to || (msg._data && (msg._data.from || msg._data.to));
    const messageId = msg.id && msg.id._serialized ? msg.id._serialized : null;

    if (groupId && messageId) {
      console.log(`[EDIT] ${groupId}: ${(newBody || '').substring(0, 80)}...`);

      fireHAEvent('whatsapp_message_edit', {
        group_id: groupId,
        message_id: messageId,
        body: newBody || msg.body,
        new_body: newBody || msg.body,
        prev_body: prevBody || '',
        from_me: msg.fromMe || false,
        timestamp: Math.floor(Date.now() / 1000)
      });
    }
  } catch (error) {
    console.error('Error handling message_edit:', error.message);
  }
});

// ────────────────────────────────────────────────────────────
// COMMAND QUEUE — true round-robin by app_id with delay
// ────────────────────────────────────────────────────────────
const appQueues = new Map();  // app_id -> [{eventType, eventData, requestId}, ...]
let roundRobinOrder = [];     // rotating list of app_ids with pending commands
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

  // Reject new commands during restart drain
  if (restartScheduled) {
    console.log(`[CMD] ⏰ Rejected ${eventType} from [${appId}] — restart in progress`);
    fireHAEvent('whatsapp_response', {
      request_id: requestId,
      command: eventType.replace('whatsapp_command_', ''),
      success: false,
      error: 'Client is restarting, retry in ~60 seconds'
    });
    return;
  }

  // Add to the app's sub-queue
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

  // Clean up empty queues from the round-robin order
  roundRobinOrder = roundRobinOrder.filter(id => appQueues.has(id) && appQueues.get(id).length > 0);
  if (roundRobinOrder.length === 0) return;

  // Pick the next app in round-robin
  roundRobinIndex = roundRobinIndex % roundRobinOrder.length;
  const appId = roundRobinOrder[roundRobinIndex];
  roundRobinIndex++;

  const queue = appQueues.get(appId);
  const { eventType, eventData, requestId } = queue.shift();

  // Clean up if the queue is now empty
  if (queue.length === 0) {
    appQueues.delete(appId);
  }

  processingCommand = true;
  console.log(`[CMD] Processing ${eventType} from [${appId}] (${requestId})... [${getTotalQueueLength()} remaining]`);

  try {
    switch (eventType) {
      case 'whatsapp_command_send': {
        if (!clientReady) throw new Error('WhatsApp not connected');
        const { target_id, message, quoted_message_id } = eventData;
        if (!target_id || !message) throw new Error('target_id and message required');

        const chat = await retry(() => client.getChatById(target_id), `send-${target_id}`);
        const sendOptions = {};
        if (quoted_message_id) {
          sendOptions.quotedMessageId = quoted_message_id;
        }
        await chat.sendMessage(message, sendOptions);
        console.log(`[CMD] ✅ Message sent to ${target_id}`);

        fireHAEvent('whatsapp_response', { request_id: requestId, command: 'send', success: true });
        break;
      }

      case 'whatsapp_command_fetch': {
        if (!clientReady) throw new Error('WhatsApp not connected');
        const { group_id, limit = 50 } = eventData;
        if (!group_id) throw new Error('group_id required');

        const chat = await retry(() => client.getChatById(group_id), `fetch-${group_id}`);
        const messages = await chat.fetchMessages({ limit: Math.min(limit, 200) });

        const msgData = messages.map(m => ({
          body: m.body || '',
          timestamp: m.timestamp,
          sender: (m._data && m._data.notifyName) || m.author || 'Unknown',
          message_id: m.id && m.id._serialized ? m.id._serialized : null,
          from_me: m.fromMe || false
        }));

        console.log(`[CMD] ✅ Fetched ${msgData.length} messages from ${group_id}`);
        fireHAEvent('whatsapp_response', { request_id: requestId, command: 'fetch', success: true, data: msgData });
        break;
      }

      case 'whatsapp_command_react': {
        if (!clientReady) throw new Error('WhatsApp not connected');
        const { message_id, chat_id, emoji } = eventData;
        if (!message_id || !chat_id || !emoji) throw new Error('message_id, chat_id, and emoji required');

        try {
          const chat = await client.getChatById(chat_id);
          const messages = await chat.fetchMessages({ limit: 50 });
          const targetMsg = messages.find(m => m.id && m.id._serialized === message_id);

          if (targetMsg) {
            await targetMsg.react(emoji);
            console.log(`[CMD] ✅ Reacted ${emoji} to ${message_id}`);
          } else {
            console.log(`[CMD] ⚠️ React skipped — message not in recent 50`);
          }
        } catch (reactErr) {
          console.log(`[CMD] React failed (non-fatal): ${reactErr.message}`);
        }

        fireHAEvent('whatsapp_response', { request_id: requestId, command: 'react', success: true });
        break;
      }

      case 'whatsapp_command_list_groups': {
        if (!clientReady) throw new Error('WhatsApp not connected');

        const chats = await client.getChats();
        const groups = chats.filter(c => c.isGroup).map(c => ({ id: c.id._serialized, name: c.name }));

        console.log(`[CMD] ✅ Listed ${groups.length} groups`);
        fireHAEvent('whatsapp_response', { request_id: requestId, command: 'list_groups', success: true, data: groups });
        break;
      }

      case 'whatsapp_command_status': {
        fireHAEvent('whatsapp_response', {
          request_id: requestId,
          command: 'status',
          success: true,
          data: { status: connectionStatus, client_ready: clientReady, last_heartbeat: lastHeartbeat, timestamp: Date.now() }
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

    if (isFatalLibraryError(err)) {
      console.error('[CMD] Detached frame error — scheduling restart.');
      setTimeout(() => process.exit(1), 3000);
    }
  } finally {
    processingCommand = false;
    // Round-robin delay — wait before processing next command
    setTimeout(processCommandQueue, COMMAND_DELAY_MS);
  }
}

// ────────────────────────────────────────────────────────────
// HA WEBSOCKET — subscribe to command events
// ────────────────────────────────────────────────────────────
let wsMessageId = 1;
let haWs = null;

function connectHAWebSocket() {
  if (!SUPERVISOR_TOKEN) {
    console.log('[WS] No SUPERVISOR_TOKEN — skipping WebSocket');
    return;
  }

  console.log('[WS] Connecting to HA WebSocket...');
  haWs = new WebSocket('ws://supervisor/core/websocket');

  haWs.on('open', () => console.log('[WS] WebSocket connected'));

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
// SCHEDULED RESTART — restart every 12 hours for stability
// ────────────────────────────────────────────────────────────
let restartScheduled = false;

function scheduleRestart() {
  setTimeout(() => {
    restartScheduled = true;
    const uptime = Math.round(RESTART_INTERVAL_MS / 1000 / 60 / 60);
    console.log(`[RESTART] ⏰ ${uptime}h uptime reached — scheduling graceful restart...`);

    // Wait for the current command to finish, then exit
    const waitForDrain = setInterval(() => {
      if (!processingCommand && getTotalQueueLength() === 0) {
        clearInterval(waitForDrain);
        console.log('[RESTART] ✅ Queue drained — exiting for supervisor restart.');
        process.exit(0);
      } else {
        console.log(`[RESTART] Waiting for queue to drain... (${getTotalQueueLength()} remaining, processing: ${processingCommand})`);
      }
    }, 5000);

    // Safety net: force exit after 2 minutes even if queue doesn't drain
    setTimeout(() => {
      console.log('[RESTART] ⚠️ Force exit after 2min drain timeout.');
      process.exit(0);
    }, 2 * 60 * 1000);
  }, RESTART_INTERVAL_MS);

  const restartAt = new Date(Date.now() + RESTART_INTERVAL_MS).toLocaleString('he-IL', { timeZone: 'Asia/Jerusalem' });
  console.log(`[RESTART] Next scheduled restart at ${restartAt}`);
}

// ────────────────────────────────────────────────────────────
// INGRESS WEB UI
// ────────────────────────────────────────────────────────────
const app = express();
app.use(express.json());

app.get('/api/status', (req, res) => {
  let connectedNumber = null;
  try {
    if (clientReady && client.info && client.info.wid) {
      connectedNumber = client.info.wid.user;
    }
  } catch (_) {}

  res.json({
    status: connectionStatus,
    connected_number: connectedNumber,
    qr_data_url: currentQRDataUrl,
    recent_messages: recentMessages,
    command_queue_length: getTotalQueueLength(),
    command_queues: Object.fromEntries([...appQueues.entries()].map(([k, v]) => [k, v.length])),
    last_heartbeat: lastHeartbeat,
    heartbeat_count: heartbeatCount,
  });
});

app.use(express.static(path.join(__dirname, 'public')));

app.listen(INGRESS_PORT, () => {
  console.log(`Ingress running on port ${INGRESS_PORT}`);
});

// ────────────────────────────────────────────────────────────
// STARTUP
// ────────────────────────────────────────────────────────────
function cleanupChromium() {
  for (const p of ['/data/session/SingletonLock', '/data/session/SingletonSocket', '/data/session/SingletonCookie']) {
    try { fs.unlinkSync(p); console.log(`Removed stale lock: ${p}`); } catch {}
  }
}

function clearOldSession() {
  if (fs.existsSync(MIGRATION_FLAG)) return;

  console.log('[Migration] First run on v1.34 — clearing old session data...');
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
  try { fs.writeFileSync(MIGRATION_FLAG, new Date().toISOString()); } catch {}
  console.log('[Migration] Done — will prompt for fresh QR scan.');
}

async function main() {
  console.log('');
  console.log('╔══════════════════════════════════════════╗');
  console.log('║   WhatsApp Client for HA                  ║');
  console.log('║   v1.34 • Event-driven • Round-robin      ║');
  console.log('╚══════════════════════════════════════════╝');
  console.log('');

  clearOldSession();
  cleanupChromium();

  // Connect to HA WebSocket for command events
  connectHAWebSocket();

  // Schedule automatic restart for stability
  scheduleRestart();

  console.log('Initializing WhatsApp client...');
  try {
    await client.initialize();
    console.log('✅ Client initialized');
  } catch (err) {
    console.error('❌ Init failed:', err.message);
    console.error(err.stack);
    process.exit(1);
  }
}

main();
