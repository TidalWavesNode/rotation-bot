# Rotation Bot

Rotation Bot is a Bittensor subnet rotation scanner and execution bot built around validator weight-change signals, liquidity filters, TaoStats confirmation, and controlled live execution.

Repo URL: `https://github.com/TidalWavesNode/rotation-bot.git`

## What it does

- Scans all tradeable subnets by default
- Scores candidates using validator delta, breadth, liquidity, reserve change, flow, and activity proxies
- Tracks staged signals with setup / trigger / decay logic
- Supports signals-only, confirm, and auto execution modes, including confirm-mode selection during setup
- Uses Bittensor SDK trade execution with explicit extrinsic success checks
- Reconciles entries and exits against pre/post on-chain snapshots
- Maintains a control panel for dashboard, portfolio, journal, settings, exports, logs, and reset operations

## Major improvements in this build

This build includes the following production-oriented upgrades:

1. Sequential capital allocation so multiple entries in one cycle do not oversubscribe free TAO.
2. Bittensor SDK execution reconciliation using pre/post wallet + stake snapshots.
3. Configurable default validator hotkey stored in the saved profile.
4. Richer runtime status fields for the dashboard, including last signal scan and PnL fields.
5. Reduced SQLite churn by batching trade execution writes inside a transaction where possible.
6. Wallet balance cache fallback to reduce sizing failures when live balance reads are temporarily unavailable.
7. Expanded menu-based settings surface for advanced trading controls.
8. Menu reset action that wipes trading history, signals, journal state, execution reconciliations, runtime state, and tracked positions while preserving install-time settings, wallet selection, and secrets.

## Main files

- `rotation_bot.py` — scanner, scoring engine, execution engine, reconciliation, runtime loop
- `rotation_menu.py` — dashboard and control panel
- `run.sh` — service entrypoint
- `install.sh` — installation bootstrap
- `rotationctl` — helper launcher

## Installation

```bash
git clone https://github.com/TidalWavesNode/rotation-bot.git
cd rotation-bot
chmod +x install.sh run.sh rotationctl update.sh
./install.sh
```

## Runtime

Systemd service:

```bash
sudo systemctl status rotation-bot
journalctl -u rotation-bot -f --no-pager
```

Menu:

```bash
./rotationctl
```

## Update

Use the repo updater to pull the latest code, refresh Python dependencies, preserve your existing `.env` and database, and restart the service:

```bash
./update.sh
```

Unlike `install.sh`, the updater does not rerun the guided setup flow.

## Environment

Typical `.env` values (preserved across `./update.sh`):

```env
ROTATION_DB_PATH=/root/rotation-bot/rotation_bot.db
ROTATION_POLL_SECONDS=120
ROTATION_NONINTERACTIVE=1
BT_WALLET_NAME=trading
BT_WALLET_PATH=/root/.bittensor/wallets
BT_HOTKEY_NAME=
ROTATION_WALLET_PASSWORD=
```

## Execution truth

This version validates `ExtrinsicResponse.success` before recording a trade as successful, and stores reconciliation details from observed on-chain deltas. Failed extrinsics remain failed and do not update tracked positions as completed trades.

## Reset behavior

The menu includes a reset option that clears trading-related records so the bot behaves like a fresh start, while preserving:

- saved profile / setup choices
- wallet selection
- secrets such as TaoStats key and Discord webhook

## Notes

- Default network is `finney`.
- Default validator alias is `tao.bot` with a configurable hotkey override in saved settings.
- The bot is designed to run non-interactively under systemd after initial setup.
