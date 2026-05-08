# WhatsApp Client for Home Assistant

A WhatsApp Web client that runs as a Home Assistant add-on. Bridges WhatsApp messaging to HA via events and accepts commands from other add-ons through a round-robin command queue.

Built on [whatsapp-web.js](https://github.com/pedroslopez/whatsapp-web.js) v1.34.7.

## Features

- **QR Code Authentication** — Scan-to-link via the ingress web UI or terminal logs
- **Session persistence** — Once linked, the session survives add-on restarts (stored in `/data/`)
- **Real-time message forwarding** — Incoming messages, self-sent messages, and message edits are forwarded as HA events
- **Command interface** — Other add-ons can send messages, fetch history, react to messages, and list groups via HA events
- **Round-robin command queue** — Commands from multiple apps are processed fairly with configurable delay between requests
- **Navigation error recovery** — Gracefully handles WhatsApp Web's internal page navigations without crashing
- **Heartbeat monitoring** — Periodic status events every 2 minutes
- **Ingress web UI** — Status dashboard accessible from the HA sidebar

## Authentication

1. Open the add-on's web UI from the HA sidebar (click **WhatsApp**)
2. A QR code will be displayed
3. On your phone: WhatsApp → **Settings** → **Linked Devices** → **Link a Device** → Scan the QR code

The QR code is also printed to the add-on's terminal logs.

## Configuration

| Option | Type | Default | Description |
|---|---|---|---|
| `TEST_MESSAGE` | `bool` | `true` | Send a test message to self on successful connection |
| `RESTART_HOURS` | `int` | `7` | Restart the add-on every X hours to keep WhatsApp Web stable. Set to `0` to disable |

## HA Events

### Emitted Events

| Event | Fields | Description |
|---|---|---|
| `whatsapp_status` | `status`, `timestamp`, `heartbeat?`, `reason?` | Connection status changes and heartbeats |
| `whatsapp_message` | `group_id`, `sender`, `body`, `timestamp`, `message_id`, `is_group`, `from_me`, `has_media` | Incoming messages from others |
| `whatsapp_message_create` | `group_id`, `body`, `timestamp`, `message_id`, `from_me` | Self-sent messages |
| `whatsapp_message_edit` | `group_id`, `message_id`, `body`, `new_body`, `prev_body`, `from_me`, `timestamp` | Edited messages |
| `whatsapp_response` | `request_id`, `command`, `success`, `data?`, `error?` | Responses to commands (correlated by `request_id`) |

### Command Events

Other add-ons fire these events to send commands to the client:

| Event | Required Fields | Optional Fields | Description |
|---|---|---|---|
| `whatsapp_command_send` | `target_id`, `message` | `quoted_message_id`, `app_id`, `request_id` | Send a message (supports replies) |
| `whatsapp_command_fetch` | `group_id` | `limit` (default 50), `app_id`, `request_id` | Fetch message history |
| `whatsapp_command_react` | `message_id`, `chat_id`, `emoji` | `app_id`, `request_id` | React to a message |
| `whatsapp_command_list_groups` | — | `app_id`, `request_id` | List all groups |
| `whatsapp_command_status` | — | `app_id`, `request_id` | Query connection status |

### `app_id` and Round-Robin

All command events should include an `app_id` field identifying the sending app:

```javascript
fireHAEvent('whatsapp_command_send', {
  app_id: 'bot',           // identifies the app for round-robin scheduling
  request_id: 'send-123',  // correlates with whatsapp_response
  target_id: '1234@g.us',
  message: 'Hello!'
});
```

The client maintains a **per-app command queue** and processes them in round-robin order with a 5-second delay between commands. This ensures fair scheduling when multiple apps send commands simultaneously:

```
bot:       [fetch-1] [fetch-2] [send-1]
notes:     [react-1]
translate: [send-1]  [react-1]

Processing order: bot→notes→translate→bot→translate→bot (5s gaps)
```

| App | `app_id` |
|---|---|
| Kindergarten Bot | `bot` |
| Personal Notes | `notes` |
| Translator | `translate` |

## Architecture

```
┌──────────────────┐     HA Events      ┌──────────────────────┐
│  WhatsApp Web    │ ───messages───────► │  Home Assistant       │
│  (Puppeteer +    │                     │  Event Bus            │
│   wwebjs 1.34.7) │ ◄──commands──────── │                      │
│                  │   (round-robin)     │  ┌─────────────────┐ │
│  LocalAuth       │                     │  │ Kindergarten Bot │ │
│  /data/          │                     │  │ (app_id: bot)    │ │
└──────────────────┘                     │  ├─────────────────┤ │
        │                                │  │ Personal Notes   │ │
        ▼                                │  │ (app_id: notes)  │ │
┌──────────────────┐                     │  ├─────────────────┤ │
│  Ingress Web UI  │                     │  │ Translator       │ │
│  :3001           │                     │  │ (app_id:translate)│ │
│  - Status page   │                     │  └─────────────────┘ │
│  - QR Code       │                     └──────────────────────┘
└──────────────────┘
```

## Error Recovery

### Navigation Errors (Execution context destroyed)

WhatsApp Web occasionally navigates internally, which destroys the Puppeteer execution context. This is handled at two levels:

1. **Library level (v1.34.7)** — The `framenavigated` event triggers automatic re-injection of the client library
2. **App level** — If a navigation error occurs while the client is connected, it is logged as a non-fatal warning and the process continues running

If a navigation error occurs during initialization (before connection), the process exits and the HA Supervisor restarts it automatically.

### Periodic Restart

WhatsApp Web's Puppeteer session can degrade over long uptimes (memory leaks, stale contexts). The client schedules an automatic restart after `RESTART_HOURS` hours (default: 7). When triggered, a full status snapshot is logged (uptime, connection status, connected number, heartbeat count, queue depth) and the process exits so the HA Supervisor brings it back up cleanly. Set `RESTART_HOURS` to `0` in the add-on configuration to disable this behaviour.

### Session Migration

On first run after upgrading from an older library version, the client automatically detects and clears incompatible session data (flagged by `/data/.migrated_v134`).

## Data Persistence

All persistent data is stored in `/data/`:

| File | Purpose |
|---|---|
| `/data/.wwebjs_auth/` | WhatsApp session data (managed by `LocalAuth`) |
| `/data/.migrated_v134` | Migration flag — prevents re-clearing session on restarts |
| `/data/options.json` | Add-on configuration (managed by HA) |

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/status` | Connection status, QR data URL, queue lengths, recent messages |

## Changelog

### v1.1.003
- Added configurable periodic restart (`RESTART_HOURS`, default 7h, `0` to disable)
- Full status snapshot logged on scheduled restart

### v0.1.0
- Upgraded `whatsapp-web.js` from v1.23.0 to v1.34.7
- Added round-robin command queue by `app_id` with 5s delay
- Added `whatsapp_command_react` and `whatsapp_command_list_groups` handlers
- Added `has_media` field to `whatsapp_message` events
- Added `body` and `from_me` fields to `whatsapp_message_edit` events
- Added navigation error recovery (non-fatal when connected)
- Added session migration for v1.23→v1.34 upgrade
- Removed phone number pairing (QR-only for simplicity)

### v0.0.1
- Initial release
