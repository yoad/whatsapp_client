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
    console.error(reason);
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
    console.error(err);
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
const COMMAND_DELAY_MS = 3000; // 3s between commands
const COMMAND_TIMEOUT_MS = 60000; // 60s max per command execution
const HEARTBEAT_INTERVAL_MS = 2 * 60 * 1000; // 2 minutes
const WS_RECONNECT_DELAY_MS = 5000;
let options = {};
try {
  options = JSON.parse(fs.readFileSync('/data/options.json', 'utf8'));
  console.log('Loaded options:', JSON.stringify(options));
} catch (_) {
  console.log('No options.json, using defaults.');
}

const RESTART_HOURS = options.RESTART_HOURS !== undefined ? Number(options.RESTART_HOURS) : 7;
const RESTART_INTERVAL_MS = RESTART_HOURS > 0 ? RESTART_HOURS * 60 * 60 * 1000 : 0;
const MIGRATION_FLAG_NAME = options.MIGRATION_FLAG || '.migrated_v110';
const MIGRATION_FLAG = `/data/${MIGRATION_FLAG_NAME}`;

const TEST_MESSAGE = options.TEST_MESSAGE !== undefined ? options.TEST_MESSAGE : true;
const CONNECTED_NUMBER = options.CONNECTED_NUMBER || '972525628289';
const SAFE_MODE = options.SAFE_MODE !== undefined ? options.SAFE_MODE : false;

if (!SUPERVISOR_TOKEN) {
  console.error('WARNING: SUPERVISOR_TOKEN not available — HA integration will not work.');
}

console.log('[CONFIG] CONNECTED_NUMBER:', CONNECTED_NUMBER);
console.log('[CONFIG] RESTART_HOURS:', RESTART_HOURS);
console.log('[CONFIG] TEST_MESSAGE:', TEST_MESSAGE);
console.log('[CONFIG] MIGRATION_FLAG:', MIGRATION_FLAG_NAME);
console.log('[CONFIG] SAFE_MODE:', SAFE_MODE);
console.log('[CONFIG] Node.js:', process.version);
console.log('[CONFIG] Platform:', process.platform, process.arch);

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
  if (SAFE_MODE) {
    // console.log(`[SAFE MODE] Suppressed firing event: ${eventType}`);
    return;
  }
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
  webVersionCache: {
    type: 'remote',
    remotePath: 'https://raw.githubusercontent.com/wppconnect-team/wa-version/main/html/2.3000.1039561367-alpha.html',
  },
  puppeteer: {
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/chromium',
    protocolTimeout: 300000,
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
let qrCount = 0;
client.on('qr', async (qr) => {
  qrCount++;
  console.log('');
  console.log('╔══════════════════════════════════════════╗');
  console.log('║   SCAN THIS QR CODE WITH WHATSAPP        ║');
  console.log('╚══════════════════════════════════════════╝');
  qrcode.generate(qr, { small: true });
  console.log(`[QR] Code #${qrCount} generated at ${new Date().toISOString()}`);
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
});

// --- State Change ---
client.on('change_state', (state) => {
  console.log(`[State] WhatsApp state changed: ${state}`);
});

// --- Ready ---
client.on('ready', async () => {
  clientReady = true;
  connectionStatus = 'connected';
  currentQRDataUrl = null;

  const me = CONNECTED_NUMBER || client.info.wid.user;
  console.log('');
  console.log('╔══════════════════════════════════════════╗');
  console.log(`║   ✅ CONNECTED as ${me}            ║`);
  console.log('╚══════════════════════════════════════════╝');
  console.log('');

  // Diagnostic info
  try {
    const info = client.info;
    console.log(`[DEBUG] WID: ${info.wid._serialized}`);
    console.log(`[DEBUG] Platform: ${info.platform}`);
    console.log(`[DEBUG] Pushname: ${info.pushname}`);
    if (info.phone) {
      console.log(`[DEBUG] Phone: wa_version=${info.phone.wa_version}, os_version=${info.phone.os_version}, device_manufacturer=${info.phone.device_manufacturer}, device_model=${info.phone.device_model}`);
    }
  } catch (e) {
    console.log(`[DEBUG] Could not read client.info: ${e.message}`);
  }

  fireHAEvent('whatsapp_status', { status: 'connected', timestamp: Date.now() });

  // Send test message to self
  if (TEST_MESSAGE) {
    try {
      const testId = CONNECTED_NUMBER + '@c.us';
      await client.sendMessage(testId, '✅ WhatsApp Client connected successfully!');
      console.log(`[TEST] ✅ Test message sent to self (${me})`);
    } catch (err) {
      console.error(`[TEST] ❌ Test message failed: ${err.message}`);
    }
  }
});

// --- Disconnected ---
client.on('disconnected', (reason) => {
  const uptime = clientReady ? 'was connected' : 'never reached ready';
  console.error(`[Disconnected] Reason: ${reason} | Status: ${uptime} | QR codes shown: ${qrCount} | Auth: ${authLogged}`);
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

        const msgData = messages.map(m => {
          // Extract reactions if available
          let reactions = [];
          try {
            if (m._data && m._data.reactions && Array.isArray(m._data.reactions)) {
              reactions = m._data.reactions.map(r => r.id || r.emoji || r).filter(Boolean);
            }
          } catch (_) {}

          return {
            body: m.body || '',
            timestamp: m.timestamp,
            sender: (m._data && m._data.notifyName) || m.author || 'Unknown',
            message_id: m.id && m.id._serialized ? m.id._serialized : null,
            from_me: m.fromMe || false,
            reactions
          };
        });

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
// INGRESS WEB UI
// ────────────────────────────────────────────────────────────
const app = express();
app.use(express.json());

app.get('/api/status', (req, res) => {
  let connectedNumber = null;
  try {
    if (clientReady && client.info && client.info.wid) {
      connectedNumber = CONNECTED_NUMBER || client.info.wid.user;
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

  console.log('[RESET] New migration flag detected — performing hard reset...');

  // Remove old migration flag files
  try {
    const dataFiles = fs.readdirSync('/data');
    for (const f of dataFiles) {
      if (f.startsWith('.migrated_')) {
        const p = `/data/${f}`;
        try { fs.unlinkSync(p); console.log(`[RESET] Removed old flag: ${p}`); } catch {}
      }
    }
  } catch {}

  // Remove all session/auth data
  for (const dir of ['/data/.wwebjs_auth', '/data/session']) {
    try {
      if (fs.existsSync(dir)) {
        fs.rmSync(dir, { recursive: true, force: true });
        console.log(`[RESET] Cleared: ${dir}`);
      }
    } catch (err) {
      console.error(`[RESET] Failed to clear ${dir}: ${err.message}`);
    }
  }

  try { fs.writeFileSync(MIGRATION_FLAG, new Date().toISOString()); } catch {}
  console.log('[RESET] Done — will prompt for fresh QR scan.');
}

async function main() {
  console.log('');
  console.log('╔══════════════════════════════════════════╗');
  console.log('║   WhatsApp Client for HA                  ║');
  console.log('║   v1.1.0 • Event-driven • Round-robin     ║');
  console.log('╚══════════════════════════════════════════╝');
  console.log('');

  clearOldSession();
  cleanupChromium();

  // Connect to HA WebSocket for command events
  if (!SAFE_MODE) {
    connectHAWebSocket();
  } else {
    console.log('[SAFE MODE] Bypassing HA WebSocket connection.');
  }

  console.log('Initializing WhatsApp client...');
  console.log('[INIT] webVersionCache: remote (pinned version)');
  console.log('[INIT] authStrategy: LocalAuth (/data)');
  const initStart = Date.now();
  try {
    await client.initialize();
    const initMs = Date.now() - initStart;
    console.log(`✅ Client initialized (took ${Math.round(initMs / 1000)}s)`);
  } catch (err) {
    const initMs = Date.now() - initStart;
    console.error(`❌ Init failed after ${Math.round(initMs / 1000)}s: ${err.message}`);
    console.error(err.stack);
    process.exit(1);
  }

  // Schedule periodic restart (supervisor will bring us back)
  if (RESTART_INTERVAL_MS > 0) {
    console.log(`[RESTART] Scheduled automatic restart in ${RESTART_HOURS} hour(s)`);
    setTimeout(() => {
      const uptimeMin = Math.round(RESTART_INTERVAL_MS / 60000);
      const connectedNumber = CONNECTED_NUMBER || ((clientReady && client.info && client.info.wid) ? client.info.wid.user : 'N/A');
      console.log('');
      console.log('╔══════════════════════════════════════════╗');
      console.log('║   🔄 SCHEDULED RESTART                    ║');
      console.log('╚══════════════════════════════════════════╝');
      console.log(`[RESTART] Uptime: ${uptimeMin} minutes (${RESTART_HOURS}h)`);
      console.log(`[RESTART] Status: ${connectionStatus}`);
      console.log(`[RESTART] Connected number: ${connectedNumber}`);
      console.log(`[RESTART] Heartbeats sent: ${heartbeatCount}`);
      console.log(`[RESTART] Command queue depth: ${getTotalQueueLength()}`);
      console.log(`[RESTART] Exiting now — supervisor will restart...`);
      console.log('');
      process.exit(1);
    }, RESTART_INTERVAL_MS);
  } else {
    console.log('[RESTART] Periodic restart is disabled (RESTART_HOURS=0)');
  }
}

main();
