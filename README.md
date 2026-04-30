# WhatsApp Client for Home Assistant

A stateless WhatsApp Web client that runs as a Home Assistant add-on. Exposes WhatsApp messaging via HA events and accepts commands via HA event subscriptions.

## Features

- **Two authentication methods:**
  - **Phone Number Pairing** — Enter your phone number in the web UI and get an 8-digit code to link your device. No camera needed.
  - **QR Code** — Traditional scan-to-link via the ingress web UI or terminal logs.
- **Session persistence** — Once linked, the session survives add-on restarts. Re-authentication is only needed if you log out from your phone.
- **Real-time message forwarding** — Incoming messages, self-sent messages, and message edits are forwarded as HA events.
- **Command interface** — Other add-ons can send messages, fetch history, react to messages, and list groups via HA events.
- **Heartbeat monitoring** — Periodic status events for health checks.
- **Ingress web UI** — Status dashboard accessible from the HA sidebar.

## Authentication

### Option 1: Phone Number (Recommended)

The easiest method, especially for headless/Docker setups:

1. Open the add-on's web UI from the HA sidebar (click **WhatsApp**).
2. Enter your phone number in international format (e.g. `972525628289` for Israel, `12025550108` for US) — no `+` or spaces.
3. Click **Get Code**.
4. On your phone, open WhatsApp → **Settings** → **Linked Devices** → **Link a Device** → **Link with phone number instead** → Enter the 8-digit code shown.

The phone number is saved to `/data/phone_number.txt` so if re-authentication is ever needed (e.g. after a logout), it will automatically use pairing code again.

You can also pre-configure the phone number in the add-on config:

```yaml
PHONE_NUMBER: "972525628289"
```

### Option 2: QR Code

If no phone number is configured, the add-on falls back to QR code authentication:

1. Open the add-on's web UI — a QR code will be displayed.
2. On your phone, open WhatsApp → **Settings** → **Linked Devices** → **Link a Device** → Scan the QR code.

The QR code is also printed to the add-on's terminal logs.

## Configuration

| Option | Type | Default | Description |
|---|---|---|---|
| `TEST_MESSAGE` | `bool` | `true` | Send a test message on successful connection |
| `PHONE_NUMBER` | `str` | `""` | Phone number for pairing code auth (international format, no `+`) |

## HA Events

### Emitted Events

| Event | Description |
|---|---|
| `whatsapp_status` | Connection status changes and heartbeats |
| `whatsapp_message` | Incoming messages from others |
| `whatsapp_message_create` | Self-sent messages |
| `whatsapp_message_edit` | Edited messages |
| `whatsapp_response` | Responses to commands |

### Command Events (subscribe to these)

| Event | Required Fields | Description |
|---|---|---|
| `whatsapp_command_send` | `target_id`, `message` | Send a message |
| `whatsapp_command_fetch` | `group_id`, `limit?` | Fetch message history |
| `whatsapp_command_react` | `message_id`, `chat_id`, `emoji` | React to a message |
| `whatsapp_command_list_groups` | — | List all groups |
| `whatsapp_command_status` | — | Query connection status |

All command events accept an optional `request_id` field. The response is fired as a `whatsapp_response` event with the same `request_id`.

## Architecture

```
┌──────────────────┐     HA Events      ┌──────────────┐
│  WhatsApp Web    │ ──────────────────► │    Home      │
│  (Puppeteer)     │                     │  Assistant   │
│                  │ ◄────────────────── │              │
│  LocalAuth       │   WS Commands      │  Other       │
│  /data/          │                     │  Add-ons     │
└──────────────────┘                     └──────────────┘
        │
        ▼
┌──────────────────┐
│  Ingress Web UI  │
│  :3001           │
│  - Status page   │
│  - QR / Pairing  │
└──────────────────┘
```

## Data Persistence

All persistent data is stored in `/data/`:

| File | Purpose |
|---|---|
| `/data/.wwebjs_auth/` | WhatsApp session data (managed by `LocalAuth`) |
| `/data/phone_number.txt` | Saved phone number for pairing code auth |
| `/data/options.json` | Add-on configuration (managed by HA) |
