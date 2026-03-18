// /addons/whatsapp_client/client.js
// Stateless WhatsApp Client — exposes WhatsApp via HA events
const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const QRCode = require('qrcode');
const axios = require('axios');
const express = require('express');
const WebSocket = require('ws');
const path = require('path');

// --- Timestamped logging ---
const _origLog = console.log;
const _origErr = console.error;
const _ts = () => new Date().toLocaleString('he-IL', { timeZone: 'Asia/Jerusalem', hour: '2-digit', minute: '2-digit', second: '2-digit', day: '2-digit', month: '2-digit' });
console.log = (...args) => _origLog(`[${_ts()}]`, ...args);
console.error = (...args) => _origErr(`[${_ts()}]`, ...args);

// ────────────────────────────────────────────────────────────
// CONFIG
// ────────────────────────────────────────────────────────────
const SUPERVISOR_TOKEN = process.env.SUPERVISOR_TOKEN;
const INGRESS_PORT = 3001;
const HEARTBEAT_INTERVAL_MS = 2 * 60 * 1000; // 2 minutes
const WS_RECONNECT_DELAY_MS = 5000;

// Load addon options
const fs = require('fs');
let options = {};
try {
  options = JSON.parse(fs.readFileSync('/data/options.json', 'utf8'));
  console.log('Loaded options.json:', JSON.stringify(options));
} catch (err) {
  console.log('No options.json found, using defaults.');
}

const TEST_MESSAGE = options.TEST_MESSAGE !== undefined ? options.TEST_MESSAGE : trueno;

if (!SUPERVISOR_TOKEN) {
  console.error('WARNING: SUPERVISOR_TOKEN not available — HA event integration will not work.');
}

// ────────────────────────────────────────────────────────────
// STATE (in-memory only, no persistence)
// ────────────────────────────────────────────────────────────
let connectionStatus = 'initializing'; // initializing | qr_required | connected | disconnected
let currentQR = null;                  // raw QR string (for terminal + ingress page)
let currentQRDataUrl = null;           // QR as data URL (for ingress page)
let lastHeartbeat = null;
let readyHandled = false;
let clientReady = false;

// ────────────────────────────────────────────────────────────
// HELPERS
// ────────────────────────────────────────────────────────────
function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

async function retry(fn, label, maxRetries = 3, delayMs = 15000) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`[${label}] Attempt ${attempt}/${maxRetries}...`);
      return await fn();
    } catch (err) {
      console.error(`[${label}] Attempt ${attempt} failed: ${err.message}`);
      if (attempt === maxRetries) throw err;
      console.log(`[${label}] Waiting ${delayMs / 1000}s before retry...`);
      await delay(delayMs);
    }
  }
}

// ────────────────────────────────────────────────────────────
// HA EVENT HELPERS
// ────────────────────────────────────────────────────────────
async function fireHAEvent(eventType, eventData) {
  if (!SUPERVISOR_TOKEN) {
    console.log(`[HA] Would fire ${eventType} but no SUPERVISOR_TOKEN`);
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
    console.error(`[HA] Failed to fire event ${eventType}: ${err.message}`);
  }
}

// ────────────────────────────────────────────────────────────
// WHATSAPP CLIENT
// ────────────────────────────────────────────────────────────
const client = new Client({
  authStrategy: new LocalAuth({ dataPath: '/data' }),
  puppeteer: {
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/chromium',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--disable-extensions',
      '--disable-software-rasterizer',
      '--disable-background-networking',
      '--disable-default-apps',
      '--disable-sync',
      '--no-first-run',
      '--single-process',
      '--no-zygote',
    ],
    protocolTimeout: 300000,
  }
});

// --- QR Code ---
client.on('qr', async (qr) => {
  if (readyHandled) {
    console.log('QR received after already ready — ignoring.');
    return;
  }
  console.log('--- SCAN THIS QR CODE WITH WHATSAPP ---');
  qrcode.generate(qr, { small: true });
  console.log('------------------------------------');

  currentQR = qr;
  connectionStatus = 'qr_required';

  // Generate QR data URL for ingress page
  try {
    currentQRDataUrl = await QRCode.toDataURL(qr, { width: 300 });
  } catch (e) {
    console.error('Failed to generate QR data URL:', e.message);
  }

  // Fire status event
  fireHAEvent('whatsapp_status', {
    status: 'qr_required',
    timestamp: Date.now()
  });
});

// --- Ready ---
client.on('ready', async () => {
  if (readyHandled) {
    console.log('WhatsApp client fired ready again — ignoring.');
    return;
  }
  readyHandled = true;
  console.log('WhatsApp client is ready!');
  console.log('Waiting 30 seconds for WhatsApp to fully sync...');
  await delay(30000);

  clientReady = true;
  connectionStatus = 'connected';
  currentQR = null;
  currentQRDataUrl = null;

  console.log('WhatsApp sync complete — now forwarding messages.');

  fireHAEvent('whatsapp_status', {
    status: 'connected',
    timestamp: Date.now()
  });

  // Send test message if configured
  if (TEST_MESSAGE) {
    try {
      const testNumber = '972525628289@c.us';
      await client.sendMessage(testNumber, '✅ WhatsApp Client addon connected successfully!');
      console.log('[TEST] ✅ Test message sent to 972525628289');
    } catch (err) {
      console.error('[TEST] ❌ Test message failed:', err.message);
    }
  }
});

// --- Disconnected ---
client.on('disconnected', async (reason) => {
  console.error('WhatsApp client disconnected:', reason);
  clientReady = false;
  connectionStatus = 'disconnected';

  fireHAEvent('whatsapp_status', {
    status: 'disconnected',
    reason: reason,
    timestamp: Date.now()
  });

  console.log('Will attempt to reconnect in 30 seconds...');
  await delay(30000);
  try {
    await startClient();
  } catch (err) {
    console.error('Reconnection failed:', err.message);
    console.log('Will try again in 60 seconds...');
    setTimeout(async () => {
      try { await startClient(); } catch (e) {
        console.error('Reconnection still failing:', e.message);
      }
    }, 60000);
  }
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

    fireHAEvent('whatsapp_message', {
      group_id: msg.from,
      sender: sender,
      body: msg.body || '',
      timestamp: msg.timestamp,
      message_id: messageId,
      is_group: isGroup,
      from_me: false
    });
  } catch (error) {
    console.error('Error handling message:', error);
  }
});

// Self-sent messages (for notes group etc.)
client.on('message_create', async (msg) => {
  try {
    if (!clientReady) return;
    if (!msg.fromMe) return; // only self-sent

    const groupId = msg.to || msg.from;
    const messageId = msg.id && msg.id._serialized ? msg.id._serialized : `${msg.timestamp}-self`;

    console.log(`[MSG_CREATE] Self-sent to ${groupId}: ${(msg.body || '').substring(0, 80)}...`);

    fireHAEvent('whatsapp_message_create', {
      group_id: groupId,
      body: msg.body || '',
      timestamp: msg.timestamp,
      message_id: messageId,
      from_me: true
    });
  } catch (error) {
    console.error('Error handling message_create:', error);
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
        new_body: newBody || msg.body,
        prev_body: prevBody || '',
        timestamp: Math.floor(Date.now() / 1000)
      });
    }
  } catch (error) {
    console.error('Error handling message_edit:', error);
  }
});

// ────────────────────────────────────────────────────────────
// COMMAND HANDLERS — process commands from other addons
// ────────────────────────────────────────────────────────────
async function handleCommand(eventType, eventData) {
  const requestId = eventData.request_id || `auto-${Date.now()}`;

  console.log(`[CMD] Received ${eventType} (request_id: ${requestId})`);

  try {
    switch (eventType) {
      case 'whatsapp_command_send': {
        if (!clientReady) throw new Error('WhatsApp not connected');
        const { target_id, message } = eventData;
        if (!target_id || !message) throw new Error('target_id and message are required');

        const chat = await retry(() => client.getChatById(target_id), `send-${target_id}`);
        await chat.sendMessage(message);
        console.log(`[CMD] Message sent to ${target_id}`);

        fireHAEvent('whatsapp_response', {
          request_id: requestId,
          command: 'send',
          success: true
        });
        break;
      }

      case 'whatsapp_command_fetch': {
        if (!clientReady) throw new Error('WhatsApp not connected');
        const { group_id, limit = 50 } = eventData;
        if (!group_id) throw new Error('group_id is required');

        const chat = await retry(() => client.getChatById(group_id), `fetch-${group_id}`);
        const messages = await chat.fetchMessages({ limit: Math.min(limit, 200) });

        const msgData = messages.map(m => ({
          body: m.body || '',
          timestamp: m.timestamp,
          sender: (m._data && m._data.notifyName) || m.author || 'Unknown',
          message_id: m.id && m.id._serialized ? m.id._serialized : null,
          from_me: m.fromMe || false
        }));

        console.log(`[CMD] Fetched ${msgData.length} messages from ${group_id}`);

        fireHAEvent('whatsapp_response', {
          request_id: requestId,
          command: 'fetch',
          success: true,
          data: msgData
        });
        break;
      }

      case 'whatsapp_command_react': {
        if (!clientReady) throw new Error('WhatsApp not connected');
        const { message_id, chat_id, emoji } = eventData;
        if (!message_id || !chat_id || !emoji) throw new Error('message_id, chat_id, and emoji are required');

        // Fetch the specific message to react to it
        const chat = await retry(() => client.getChatById(chat_id), `react-${chat_id}`);
        const messages = await chat.fetchMessages({ limit: 50 });
        const targetMsg = messages.find(m => m.id && m.id._serialized === message_id);

        if (targetMsg) {
          await targetMsg.react(emoji);
          console.log(`[CMD] Reacted with ${emoji} to ${message_id}`);
        } else {
          throw new Error(`Message ${message_id} not found in recent messages`);
        }

        fireHAEvent('whatsapp_response', {
          request_id: requestId,
          command: 'react',
          success: true
        });
        break;
      }

      case 'whatsapp_command_list_groups': {
        if (!clientReady) throw new Error('WhatsApp not connected');

        const chats = await client.getChats();
        const groups = chats
          .filter(c => c.isGroup)
          .map(c => ({
            id: c.id._serialized,
            name: c.name
          }));

        console.log(`[CMD] Listed ${groups.length} groups`);

        fireHAEvent('whatsapp_response', {
          request_id: requestId,
          command: 'list_groups',
          success: true,
          data: groups
        });
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
            timestamp: Date.now()
          }
        });
        break;
      }

      default:
        console.log(`[CMD] Unknown command: ${eventType}`);
    }
  } catch (err) {
    console.error(`[CMD] Error handling ${eventType}: ${err.message}`);
    fireHAEvent('whatsapp_response', {
      request_id: requestId,
      command: eventType.replace('whatsapp_command_', ''),
      success: false,
      error: err.message
    });
  }
}

// ────────────────────────────────────────────────────────────
// HA WEBSOCKET — subscribe to command events in real-time
// ────────────────────────────────────────────────────────────
let wsMessageId = 1;
let haWs = null;
let wsSubscriptions = {}; // id -> event_type

function connectHAWebSocket() {
  if (!SUPERVISOR_TOKEN) {
    console.log('[WS] No SUPERVISOR_TOKEN — skipping WebSocket connection');
    return;
  }

  console.log('[WS] Connecting to HA WebSocket...');
  haWs = new WebSocket('ws://supervisor/core/websocket');

  haWs.on('open', () => {
    console.log('[WS] WebSocket connected');
  });

  haWs.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());

      // Step 1: HA sends auth_required
      if (msg.type === 'auth_required') {
        haWs.send(JSON.stringify({
          type: 'auth',
          access_token: SUPERVISOR_TOKEN
        }));
        return;
      }

      // Step 2: Auth OK — subscribe to command events
      if (msg.type === 'auth_ok') {
        console.log('[WS] Authenticated with HA');
        subscribeToCommandEvents();
        return;
      }

      // Step 3: Handle subscription results
      if (msg.type === 'result' && msg.success) {
        return; // subscription confirmed
      }

      // Step 4: Handle incoming events (subscribed)
      if (msg.type === 'event' && msg.event) {
        const eventType = msg.event.event_type;
        const eventData = msg.event.data || {};
        if (eventType && eventType.startsWith('whatsapp_command_')) {
          handleCommand(eventType, eventData);
        }
        return;
      }

      // Auth failed
      if (msg.type === 'auth_invalid') {
        console.error('[WS] Auth failed:', msg.message);
      }
    } catch (err) {
      console.error('[WS] Error processing message:', err.message);
    }
  });

  haWs.on('close', () => {
    console.log('[WS] WebSocket disconnected — reconnecting in 5s...');
    wsSubscriptions = {};
    setTimeout(connectHAWebSocket, WS_RECONNECT_DELAY_MS);
  });

  haWs.on('error', (err) => {
    console.error('[WS] WebSocket error:', err.message);
  });
}

function subscribeToCommandEvents() {
  const commandEvents = [
    'whatsapp_command_send',
    'whatsapp_command_fetch',
    'whatsapp_command_react',
    'whatsapp_command_list_groups',
    'whatsapp_command_status'
  ];

  for (const eventType of commandEvents) {
    const id = wsMessageId++;
    wsSubscriptions[id] = eventType;
    haWs.send(JSON.stringify({
      id: id,
      type: 'subscribe_events',
      event_type: eventType
    }));
    console.log(`[WS] Subscribed to ${eventType} (id=${id})`);
  }
}

// ────────────────────────────────────────────────────────────
// HEARTBEAT — fire status event periodically
// ────────────────────────────────────────────────────────────
let heartbeatCount = 0;

setInterval(() => {
  heartbeatCount++;
  lastHeartbeat = Date.now();
  const now = new Date().toLocaleString('he-IL', { timeZone: 'Asia/Jerusalem' });
  console.log(`[♥] Heartbeat #${heartbeatCount} at ${now} — status: ${connectionStatus}`);

  fireHAEvent('whatsapp_status', {
    status: connectionStatus,
    heartbeat: heartbeatCount,
    timestamp: lastHeartbeat
  });
}, HEARTBEAT_INTERVAL_MS);

// ────────────────────────────────────────────────────────────
// INGRESS WEB UI — simple status page
// ────────────────────────────────────────────────────────────
const app = express();

// Serve static files
app.use(express.static(path.join(__dirname, 'public')));

// Status API endpoint
app.get('/api/status', (req, res) => {
  res.json({
    status: connectionStatus,
    client_ready: clientReady,
    qr_data_url: currentQRDataUrl,
    last_heartbeat: lastHeartbeat,
    heartbeat_count: heartbeatCount,
    timestamp: Date.now()
  });
});

app.listen(INGRESS_PORT, () => {
  console.log(`Ingress status page running on port ${INGRESS_PORT}`);
});

// ────────────────────────────────────────────────────────────
// STARTUP
// ────────────────────────────────────────────────────────────
async function startClient(maxRetries = 5) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`[Startup] Initializing WhatsApp client (attempt ${attempt}/${maxRetries})...`);
      await client.initialize();
      console.log('[Startup] WhatsApp client initialized successfully');
      return;
    } catch (err) {
      console.error(`[Startup] Attempt ${attempt} failed: ${err.message}`);
      if (attempt === maxRetries) throw err;
      console.log(`[Startup] Retrying in 30 seconds...`);
      await delay(30000);
    }
  }
}

async function main() {
  console.log('');
  console.log('╔══════════════════════════════════════════╗');
  console.log('║       WhatsApp Client for HA             ║');
  console.log('║       Stateless • Event-driven           ║');
  console.log('╚══════════════════════════════════════════╝');
  console.log('');

  // Connect to HA WebSocket for command events
  connectHAWebSocket();

  // Keep trying to connect to WhatsApp — never give up
  while (true) {
    try {
      await startClient();
      return; // connected successfully
    } catch (err) {
      console.error('Startup failed:', err.message);
      console.log('Retrying in 60 seconds...');
      await delay(60000);
    }
  }
}

main();
