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

const TEST_MESSAGE = options.TEST_MESSAGE !== undefined ? options.TEST_MESSAGE : true;
const PHONE_NUMBER_FILE = '/data/phone_number.txt';

// Load phone number: config option takes priority, then saved file
function loadSavedPhoneNumber() {
  try {
    return fs.readFileSync(PHONE_NUMBER_FILE, 'utf8').trim();
  } catch (_) {
    return '';
  }
}

function savePhoneNumber(number) {
  try {
    fs.writeFileSync(PHONE_NUMBER_FILE, number, 'utf8');
    console.log(`[PAIR] Phone number saved for future restarts.`);
  } catch (err) {
    console.error(`[PAIR] Failed to save phone number: ${err.message}`);
  }
}

let phoneNumber = options.PHONE_NUMBER || loadSavedPhoneNumber();

if (phoneNumber) {
  console.log(`Phone number pairing enabled for: ${phoneNumber}`);
  savePhoneNumber(phoneNumber); // persist config value too
} else {
  console.log('No phone number configured — will use QR code authentication.');
}

if (!SUPERVISOR_TOKEN) {
  console.error('WARNING: SUPERVISOR_TOKEN not available — HA event integration will not work.');
}

// ────────────────────────────────────────────────────────────
// STATE (in-memory only, no persistence)
// ────────────────────────────────────────────────────────────
let connectionStatus = 'initializing'; // initializing | qr_required | pairing_code | connected | disconnected
let currentQR = null;                  // raw QR string (for terminal + ingress page)
let currentQRDataUrl = null;           // QR as data URL (for ingress page)
let currentPairingCode = null;         // 8-char pairing code (for phone number auth)
let authMethod = phoneNumber ? 'pairing_code' : 'qr'; // which auth method is in use
let lastHeartbeat = null;
let readyHandled = false;
let clientReady = false;
let recentMessages = [];  // last 3 messages for UI

function pushRecentMessage(sender, body, timestamp) {
  recentMessages.push({
    sender: sender,
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
const clientOptions = {
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
};

// Enable pairing code auth if phone number is configured
if (phoneNumber) {
  clientOptions.pairWithPhoneNumber = {
    phoneNumber: phoneNumber,
    showNotification: true,
    intervalMs: 180000  // refresh code every 3 minutes
  };
  console.log(`[Config] pairWithPhoneNumber set for: ${phoneNumber}`);
} else {
  console.log('[Config] No phone number — will use QR code flow');
}

console.log('[Config] Client options:', JSON.stringify({
  authStrategy: 'LocalAuth(/data)',
  pairWithPhoneNumber: clientOptions.pairWithPhoneNumber ? phoneNumber : 'none',
  puppeteer: 'chromium + ' + clientOptions.puppeteer.args.length + ' args'
}));

const client = new Client(clientOptions);
console.log('[Init] WhatsApp client instance created');

// --- All client events for logging ---
client.on('authenticated', () => {
  console.log('[Auth] ✅ Authenticated successfully — session is valid');
});

client.on('auth_failure', (msg) => {
  console.error('[Auth] ❌ Authentication failure:', msg);
});

client.on('loading_screen', (percent, message) => {
  console.log(`[Loading] ${percent}% — ${message}`);
});

client.on('change_state', (state) => {
  console.log(`[State] WhatsApp state changed to: ${state}`);
});

// --- Pairing Code (phone number auth) ---
client.on('code', (code) => {
  console.log(`[Pairing] 📱 Pairing code received (raw): ${code}`);
  if (readyHandled) {
    console.log('Pairing code received after already ready — ignoring.');
    return;
  }
  const formatted = code.substring(0, 4) + '-' + code.substring(4);
  console.log('╔══════════════════════════════════════════╗');
  console.log(`║   PAIRING CODE:  ${formatted}                ║`);
  console.log('╚══════════════════════════════════════════╝');
  console.log('Enter this code in WhatsApp on your phone:');
  console.log('Settings → Linked Devices → Link a Device → Link with phone number instead');

  currentPairingCode = formatted;
  connectionStatus = 'pairing_code';
  currentQR = null;
  currentQRDataUrl = null;

  fireHAEvent('whatsapp_status', {
    status: 'pairing_code',
    pairing_code: formatted,
    timestamp: Date.now()
  });
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
  currentPairingCode = null;

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
  currentPairingCode = null;

  console.log('WhatsApp sync complete — now forwarding messages.');

  fireHAEvent('whatsapp_status', {
    status: 'connected',
    timestamp: Date.now()
  });

  // Send test message if configured
  if (TEST_MESSAGE) {
    try {
      const myNumber = client.info.wid.user;
      const testId = myNumber + '@c.us';
      await client.sendMessage(testId, '✅ WhatsApp Client addon connected successfully!');
      console.log(`[TEST] ✅ Test message sent to ${myNumber}`);
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
// DETACHED FRAME RECOVERY
// ────────────────────────────────────────────────────────────
function isDetachedFrameError(err) {
  const msg = (err && err.message) ? err.message.toLowerCase() : '';
  return msg.includes('detached frame') ||
         msg.includes('execution context was destroyed') ||
         msg.includes('target closed') ||
         msg.includes('session closed');
}

let restartInProgress = false;

async function restartDueToDetachedFrame(source) {
  if (restartInProgress) return;
  restartInProgress = true;

  console.error(`[RECOVER] ⚠️  Detached frame detected (${source}) — exiting in 3s so supervisor can restart.`);
  fireHAEvent('whatsapp_status', { status: 'disconnected', reason: 'detached_frame', timestamp: Date.now() }).catch(() => {});

  // Give HA event time to fire, then hard-exit.
  // The HA supervisor automatically restarts the addon container.
  // WhatsApp auth is persisted to /data so no QR re-scan is needed.
  setTimeout(() => {
    console.error('[RECOVER] 🔄 Exiting now — supervisor will restart.');
    process.exit(1);
  }, 3000);
}

// Periodic health check — catches silent frame death before the next command fails.
setInterval(async () => {
  if (!clientReady) return;
  try {
    await client.getState();
  } catch (err) {
    if (isDetachedFrameError(err)) {
      console.error('[HEALTH] ❌ Health check caught detached frame:', err.message);
      restartDueToDetachedFrame('health-check');
    }
  }
}, 2 * 60 * 1000); // every 2 minutes

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

    pushRecentMessage('Me', msg.body, msg.timestamp);

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
        const { target_id, message, quoted_message_id } = eventData;
        if (!target_id || !message) throw new Error('target_id and message are required');

        const chat = await retry(() => client.getChatById(target_id), `send-${target_id}`);

        const sendOptions = {};
        if (quoted_message_id) {
          sendOptions.quotedMessageId = quoted_message_id;
          console.log(`[CMD] Sending as reply to ${quoted_message_id}`);
        }

        await chat.sendMessage(message, sendOptions);
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

        // Reactions are best-effort — WA often rejects them for older messages.
        // Use a single attempt (no retry) and never propagate the error.
        try {
          const chat = await client.getChatById(chat_id);
          const messages = await chat.fetchMessages({ limit: 50 });
          const targetMsg = messages.find(m => m.id && m.id._serialized === message_id);

          if (targetMsg) {
            await targetMsg.react(emoji);
            console.log(`[CMD] Reacted with ${emoji} to ${message_id}`);
          } else {
            console.log(`[CMD] React skipped — message ${message_id} not in recent 50`);
          }
        } catch (reactErr) {
          // Non-fatal: log and continue
          console.log(`[CMD] React failed (ignored): ${reactErr.message}`);
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

    // Detached frame = Puppeteer browser died silently — trigger a full restart
    if (isDetachedFrameError(err)) {
      console.error('[CMD] Detached frame error detected — scheduling process restart.');
      restartDueToDetachedFrame('command-error');
    }
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
app.use(express.json());

// Serve static files
app.use(express.static(path.join(__dirname, 'public')));

// Status API endpoint
app.get('/api/status', (req, res) => {
  // Get connected phone number from client info
  let connectedNumber = null;
  try {
    if (clientReady && client.info && client.info.wid) {
      connectedNumber = client.info.wid.user || client.info.wid._serialized;
    }
  } catch (_) {}

  res.json({
    status: connectionStatus,
    client_ready: clientReady,
    auth_method: authMethod,
    connected_number: connectedNumber,
    qr_data_url: currentQRDataUrl,
    pairing_code: currentPairingCode,
    recent_messages: recentMessages,
    last_heartbeat: lastHeartbeat,
    heartbeat_count: heartbeatCount,
    timestamp: Date.now()
  });
});

// Pair with phone number — triggered from the UI
app.post('/api/pair', async (req, res) => {
  const { phone_number } = req.body || {};
  if (!phone_number) {
    return res.status(400).json({ error: 'phone_number is required' });
  }

  // Strip any non-digit characters
  const cleaned = phone_number.replace(/\D/g, '');
  if (cleaned.length < 10 || cleaned.length > 15) {
    return res.status(400).json({ error: 'Invalid phone number format. Use international format without + (e.g. 972525628289)' });
  }

  if (clientReady) {
    return res.status(400).json({ error: 'Already connected — no pairing needed' });
  }

  // Save phone number and restart — the client will initialize with pairing code auth
  console.log(`[PAIR] Phone number ${cleaned} received from UI — saving and restarting...`);
  savePhoneNumber(cleaned);
  res.json({ success: true, restarting: true, message: 'Phone number saved. Restarting to generate pairing code...' });

  // Give the response time to send, then exit — Docker will restart us
  setTimeout(() => {
    console.log('[PAIR] Restarting process to apply phone number pairing...');
    process.exit(0);
  }, 1000);
});

app.listen(INGRESS_PORT, () => {
  console.log(`Ingress status page running on port ${INGRESS_PORT}`);
});

// ────────────────────────────────────────────────────────────
// STARTUP
// ────────────────────────────────────────────────────────────
async function killOrphanedChromium() {
  // Remove Chromium singleton lock so a fresh instance can start
  const lockPaths = [
    '/data/session/SingletonLock',
    '/data/session/SingletonSocket',
    '/data/session/SingletonCookie'
  ];
  for (const p of lockPaths) {
    try { require('fs').unlinkSync(p); console.log(`[Startup] Removed stale lock: ${p}`); } catch {}
  }
  // Kill any lingering chromium/chrome processes
  try {
    require('child_process').execSync('pkill -f "chromium\|chrome" 2>/dev/null || true', { shell: true });
    console.log('[Startup] Killed orphaned Chromium process(es).');
    await delay(1500); // let OS clean up
  } catch {}
}

async function startClient(maxRetries = 5) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      if (attempt > 1) {
        console.log('[Startup] Cleaning up before retry...');
        await killOrphanedChromium();
      }
      console.log(`[Startup] Initializing WhatsApp client (attempt ${attempt}/${maxRetries})...`);
      console.log('[Startup] Launching Chromium browser...');
      await client.initialize();
      console.log('[Startup] ✅ WhatsApp client initialized successfully');
      return;
    } catch (err) {
      console.error(`[Startup] ❌ Attempt ${attempt} failed: ${err.message}`);
      console.error(`[Startup] Stack: ${err.stack}`);
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

  console.log('[Main] Auth method:', authMethod);
  console.log('[Main] Phone number:', phoneNumber || '(none)');
  console.log('');

  // Connect to HA WebSocket for command events
  connectHAWebSocket();

  // Keep trying to connect to WhatsApp — never give up
  while (true) {
    try {
      await startClient();
      console.log('[Main] ✅ Client started successfully');
      return; // connected successfully
    } catch (err) {
      console.error('[Main] ❌ Startup failed:', err.message);
      console.log('[Main] Retrying in 60 seconds...');
      await delay(60000);
    }
  }
}

main();
