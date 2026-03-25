#!/usr/bin/env python3
from __future__ import annotations
import getpass
import os
import sys
from pathlib import Path

import rotation_bot as rb

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / 'rotation_bot.db'
ENV_PATH = ROOT / '.env'


def ask(prompt: str, default: str = '') -> str:
    suffix = f' [{default}]' if default else ''
    raw = input(f'{prompt}{suffix}: ').strip()
    return raw if raw else default


def ask_bool(prompt: str, default: bool = True) -> bool:
    hint = 'Y/n' if default else 'y/N'
    raw = input(f'{prompt} [{hint}]: ').strip().lower()
    if not raw:
        return default
    return raw in {'y', 'yes', '1', 'true'}


def choose(prompt: str, options: list[str], default_index: int = 0) -> str:
    print(f'\n{prompt}')
    for i, opt in enumerate(options, 1):
        d = ' (default)' if i - 1 == default_index else ''
        print(f'  {i}. {opt}{d}')
    while True:
        raw = input('Choose: ').strip()
        if not raw:
            return options[default_index]
        if raw.isdigit() and 1 <= int(raw) <= len(options):
            return options[int(raw) - 1]
        print('Invalid choice.')


def write_env(values: dict[str, str]) -> None:
    lines = [f'{k}={v}' for k, v in values.items() if v is not None]
    ENV_PATH.write_text('\n'.join(lines) + '\n')


def main() -> None:
    print('=' * 72)
    print('Rotation Bot Setup')
    print('=' * 72)
    print('This setup saves your choices, starts the systemd service, and opens the dashboard.')
    print('A Bittensor wallet is required.\\n')

    conn = rb.db_connect(str(DB_PATH))

    default_wallet_path = rb.DEFAULT_WALLET_DIR
    wallet_path = ask('Wallet path', default_wallet_path)
    discovered = rb.discover_wallets(wallet_path)
    if not discovered:
        print(f'\nNo wallets were found in: {wallet_path}')
        print('Create/import your Bittensor wallet first, then run ./install.sh again.')
        sys.exit(1)

    print('\nDetected wallets:')
    for idx, w in enumerate(discovered, 1):
        hk = w.hotkey_name or '(no hotkey)'
        print(f'  {idx}. wallet={w.wallet_name} hotkey={hk}')

    while True:
        raw = input('Select wallet number: ').strip()
        if raw.isdigit() and 1 <= int(raw) <= len(discovered):
            selection = discovered[int(raw) - 1]
            break
        print('Invalid selection.')

    rb.save_wallet_selection(conn, selection)

    style = choose('Trading style', ['Conservative', 'Balanced', 'Aggressive'], 1)
    mode = choose('Bot mode', ['Signals only', 'Confirm each trade', 'Fully automatic live trading'], 0)
    strategy = choose('Strategy mode', ['early_rotation_hunter', 'momentum_follower', 'capital_preserver'], 0)
    poll_seconds_raw = ask('Polling interval in seconds', str(rb.POLL_SECONDS))
    try:
        poll_seconds = max(30, int(poll_seconds_raw))
    except Exception:
        poll_seconds = rb.POLL_SECONDS

    if style == 'Conservative':
        profile = rb.conservative_profile()
    elif style == 'Aggressive':
        profile = rb.aggressive_profile()
    else:
        profile = rb.balanced_profile()
    profile.name = style
    profile.style = style.lower()
    profile.strategy_mode = strategy
    if mode == 'Signals only':
        profile.execution_mode = 'signals_only'
        profile.live_mode = False
    elif mode == 'Confirm each trade':
        profile.execution_mode = 'confirm'
        profile.live_mode = True
    else:
        profile.execution_mode = 'auto'
        profile.live_mode = True
    profile.wallet_name = selection.wallet_name
    profile.wallet_path = selection.wallet_path
    profile.hotkey_name = selection.hotkey_name

    print('\nSafety defaults from the chosen profile are preloaded. You can change them later.')
    if ask_bool('Adjust a few core safety settings now?', False):
        profile.reserve_buffer_tao = float(ask('Reserve buffer TAO', str(profile.reserve_buffer_tao)))
        profile.max_position_tao = float(ask('Max TAO per subnet', str(profile.max_position_tao)))
        if profile.position_sizing_mode == 'fixed':
            profile.fixed_entry_tao = float(ask('Fixed TAO per entry', str(profile.fixed_entry_tao)))
        else:
            profile.percent_of_free_tao = float(ask('Percent of free TAO per entry', str(profile.percent_of_free_tao)))
        profile.daily_max_loss_tao = float(ask('Max modeled daily loss TAO before pause', str(profile.daily_max_loss_tao)))
        profile.max_entry_impact500 = float(ask('Max entry impact500', str(profile.max_entry_impact500)))
        profile.block_entry_if_reserve_below = float(ask('Block entries if reserve below', str(profile.block_entry_if_reserve_below)))

    tao_key = ask('TaoStats API key (leave blank to skip)', '')
    discord_hook = ask('Discord webhook URL (leave blank to skip)', '')
    wallet_password = ''
    if profile.live_mode:
        wallet_password = getpass.getpass('Wallet password (leave blank if none; stored locally for the service): ')

    rb.save_profile(conn, profile)
    rb.save_secrets(conn, rb.AppSecrets(taostats_api_key=tao_key, discord_webhook_url=discord_hook))
    status_payload = {
        'state': 'configured',
        'last_heartbeat': rb.now_ts(),
        'profile': profile.name,
        'execution_mode': profile.execution_mode,
        'live_mode': profile.live_mode,
        'wallet': selection.wallet_name,
        'wallet_path': selection.wallet_path,
    }
    if hasattr(rb, 'save_runtime_status') and callable(getattr(rb, 'save_runtime_status')):
        rb.save_runtime_status(conn, status_payload)
    conn.close()

    env_values = {
        'ROTATION_DB_PATH': str(DB_PATH),
        'ROTATION_POLL_SECONDS': str(poll_seconds),
        'ROTATION_NONINTERACTIVE': '1',
        'BT_WALLET_NAME': selection.wallet_name,
        'BT_WALLET_PATH': selection.wallet_path,
        'BT_HOTKEY_NAME': selection.hotkey_name,
        'ROTATION_WALLET_PASSWORD': wallet_password,
    }
    write_env(env_values)

    print('\nSaved configuration to rotation_bot.db and .env')
    print('Setup complete.')


if __name__ == '__main__':
    main()
