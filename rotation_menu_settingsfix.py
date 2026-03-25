#!/usr/bin/env python3
from __future__ import annotations
import csv
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

DB_PATH = os.getenv('ROTATION_DB_PATH', '/root/rotation-bot/rotation_bot.db')
SERVICE = 'rotation-bot'


def connect() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def get_meta(conn: sqlite3.Connection, key: str, default: Any = None) -> Any:
    cur = conn.execute('SELECT value FROM meta WHERE key = ?', (key,))
    row = cur.fetchone()
    if not row:
        return default
    raw = row[0]
    try:
        return json.loads(raw)
    except Exception:
        return raw


def set_meta(conn: sqlite3.Connection, key: str, value: Any) -> None:
    raw = json.dumps(value)
    conn.execute(
        'INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
        (key, raw),
    )
    conn.commit()


def clear() -> None:
    os.system('clear')


def pause() -> None:
    input('\nPress ENTER to continue...')


def fmt_ts(ts: Any) -> str:
    if ts in (None, '', 0):
        return 'n/a'
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    except Exception:
        return str(ts)


def fmt_num(v: Any, places: int = 6) -> str:
    try:
        return f'{float(v):.{places}f}'
    except Exception:
        return 'n/a'


def fmt_signed(v: Any, places: int = 6) -> str:
    try:
        return f'{float(v):+.{places}f}'
    except Exception:
        return 'n/a'


def dashboard(conn: sqlite3.Connection) -> None:
    clear()
    rt = get_meta(conn, 'runtime_status', {}) or {}
    print('=' * 72)
    print('Rotation Bot Dashboard')
    print('=' * 72)
    print(f'DB Path:           {DB_PATH}')
    print('Runtime mode:      systemd service')
    print(f"Bot state:         {rt.get('state', 'n/a')}")
    print(f"Last heartbeat:    {fmt_ts(rt.get('last_heartbeat'))}")
    last_scan = rt.get('last_signal_scan') or rt.get('last_scan') or 'n/a'
    print(f'Last signal scan:  {last_scan}')
    profile = get_meta(conn, 'profile', {}) or {}
    print(f"Profile:           {profile.get('name', 'n/a')} / {profile.get('execution_mode', 'n/a')}")
    print(f"Wallet:            {rt.get('wallet', profile.get('wallet_name', 'n/a'))} @ {rt.get('wallet_path', profile.get('wallet_path', 'n/a'))}")
    print(f"Wallet free TAO:   {fmt_num(rt.get('wallet_free_tao'))}")
    print(f"Signal counts:     total={rt.get('signal_count', 'n/a')} entries={rt.get('entry_candidates', 'n/a')} exits={rt.get('exit_candidates', 'n/a')} watch={rt.get('watch_candidates', 'n/a')}")
    cur = conn.execute("SELECT COALESCE(SUM(CASE WHEN side='ENTRY' AND status='ok' THEN 1 ELSE 0 END),0), COALESCE(SUM(CASE WHEN side='EXIT' AND status='ok' THEN 1 ELSE 0 END),0) FROM trades")
    entries, exits = cur.fetchone()
    cur = conn.execute("SELECT COALESCE(SUM(realized_pnl_tao),0) FROM pnl_journal WHERE event LIKE 'EXIT%'")
    realized = cur.fetchone()[0]
    position_state = get_meta(conn, 'position_state', {}) or {}
    open_basis = sum(float((row or {}).get('amount_tao', 0) or 0) for row in position_state.values())
    open_staked = sum(float((row or {}).get('staked_tao', 0) or 0) for row in position_state.values())
    print(f'Open positions:    {len(position_state)}')
    print(f'Open basis TAO:    {open_basis:.6f}')
    print(f'Reconciled staked: {open_staked:.6f}')
    print(f'Entries logged:    {int(entries or 0)}')
    print(f'Exits logged:      {int(exits or 0)}')
    print(f'Realized PnL TAO:  {float(realized or 0):+.6f}')


def view_portfolio(conn: sqlite3.Connection) -> None:
    clear()
    state = get_meta(conn, 'position_state', {}) or {}
    mark = get_meta(conn, 'latest_mark_snapshot', {}) or {}
    print('=' * 72)
    print('Open Portfolio')
    print('=' * 72)
    if not state:
        print('No tracked positions.')
        return
    for netuid, row in state.items():
        basis = float((row or {}).get('amount_tao', 0) or 0)
        staked = float((row or {}).get('staked_tao', 0) or 0)
        entry_mark = float((row or {}).get('entry_mark', 1) or 1)
        current_mark = float(((mark.get(str(netuid), {}) or {}).get('mark', entry_mark)) or entry_mark)
        est_value = staked if staked > 0 else basis * (current_mark / max(entry_mark, 1e-9))
        upnl = est_value - basis
        print(f"SN{netuid} {(row or {}).get('name', '')}")
        if staked > 0:
            print(f"  basis={basis:.6f} | reconciled_staked={staked:.6f} | est_value={est_value:.6f} | est_uPnL={upnl:+.6f}")
        else:
            print(f"  basis={basis:.6f} | entry_mark={entry_mark:.6f} | current_mark={current_mark:.6f} | est_value={est_value:.6f} | est_uPnL={upnl:+.6f} [mark-based estimate]")


def view_trade_journal(conn: sqlite3.Connection) -> None:
    clear()
    print('=' * 72)
    print('Trade Journal (last 25)')
    print('=' * 72)
    cur = conn.execute('SELECT ts, side, netuid, name, amount_tao, status, detail FROM trades ORDER BY id DESC LIMIT 25')
    rows = cur.fetchall()
    if not rows:
        print('No journal entries yet.')
        return
    for ts, side, netuid, name, amount_tao, status, detail in rows:
        print(f"{fmt_ts(ts)} | {side:<5} SN{netuid:<3} {name:<24} amt={float(amount_tao):.6f} status={status}")
        if detail:
            print(f"  {str(detail)[:220]}")


def pnl_summary(conn: sqlite3.Connection) -> None:
    clear()
    print('=' * 72)
    print('PnL Summary')
    print('=' * 72)
    cur = conn.execute("SELECT COALESCE(SUM(realized_pnl_tao),0), COUNT(*) FROM pnl_journal WHERE event LIKE 'EXIT%'")
    realized, closed = cur.fetchone()
    position_state = get_meta(conn, 'position_state', {}) or {}
    mark = get_meta(conn, 'latest_mark_snapshot', {}) or {}
    basis = 0.0
    est_value = 0.0
    reconciled_value = 0.0
    for netuid, row in position_state.items():
        amt = float((row or {}).get('amount_tao', 0) or 0)
        staked = float((row or {}).get('staked_tao', 0) or 0)
        entry_mark = float((row or {}).get('entry_mark', 1) or 1)
        current_mark = float(((mark.get(str(netuid), {}) or {}).get('mark', entry_mark)) or entry_mark)
        basis += amt
        est_component = staked if staked > 0 else amt * (current_mark / max(entry_mark, 1e-9))
        est_value += est_component
        reconciled_value += staked
    unreal = est_value - basis
    cur = conn.execute("SELECT COUNT(*) FROM pnl_journal WHERE event LIKE 'EXIT%' AND realized_pnl_tao > 0")
    wins = cur.fetchone()[0]
    cur = conn.execute("SELECT COUNT(*) FROM pnl_journal WHERE event LIKE 'EXIT%' AND realized_pnl_tao < 0")
    losses = cur.fetchone()[0]
    win_rate = (wins / closed * 100.0) if closed else None
    avg = (float(realized or 0) / closed) if closed else 0.0
    print(f'Realized PnL TAO:     {float(realized or 0):+.6f}')
    print(f'Unrealized PnL TAO:   {unreal:+.6f}')
    print(f'Total PnL TAO:        {float(realized or 0) + unreal:+.6f}')
    print(f'Open basis TAO:       {basis:.6f}')
    print(f'Open est value TAO:   {est_value:.6f}')
    print(f'Reconciled staked TAO:{reconciled_value:.6f}')
    print(f'Closed exits:         {int(closed or 0)}')
    print(f'Wins / Losses:        {int(wins or 0)} / {int(losses or 0)}')
    print(f'Win rate:             {f"{win_rate:.1f}%" if win_rate is not None else "n/a"}')
    print(f'Avg realized / exit:  {avg:+.6f}')


def latest_signals(conn: sqlite3.Connection) -> None:
    clear()
    print('=' * 108)
    print('Latest Signals')
    print('=' * 108)
    cur = conn.execute(
        """SELECT netuid, name, classification, score, validator_delta, validator_cluster, reserve_now, reserve_delta, impact500, updated_at
           FROM history
           WHERE id IN (SELECT MAX(id) FROM history GROUP BY netuid)
           ORDER BY score DESC, netuid ASC
           LIMIT 25"""
    )
    rows = cur.fetchall()
    if not rows:
        print('No signal history yet.')
        return
    print('Subnet | Name                     | Class     | Score | ΔWeights | Breadth | Reserve    | ResΔ     | Impact500 | Updated')
    print('-' * 108)
    for netuid, name, classification, score, vd, vc, reserve_now, reserve_delta, impact500, updated_at in rows:
        print(f"SN{netuid:<4} | {str(name)[:24]:<24} | {classification:<9} | {float(score):.3f} | {float(vd):+7.4f} | {int(vc):<7} | {float(reserve_now):10.3f} | {float(reserve_delta):+8.3f} | {float(impact500):9.2f} | {updated_at}")


def export_reports(conn: sqlite3.Connection) -> None:
    clear()
    out_dir = Path(DB_PATH).resolve().parent / 'exports'
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    for table in ('trades', 'pnl_journal', 'history', 'execution_reconciliations'):
        cur = conn.execute(f'SELECT * FROM {table}')
        cols = [d[0] for d in cur.description]
        path = out_dir / f'{table}_{stamp}.csv'
        with open(path, 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(cur.fetchall())
        print(f'Wrote {path}')


def view_logs() -> None:
    subprocess.run(['journalctl', '-u', SERVICE, '-f'])


def service_cmd(cmd: str) -> subprocess.CompletedProcess:
    return subprocess.run(['sudo', 'systemctl', cmd, SERVICE], check=False, capture_output=True, text=True)


def service_is_active() -> bool:
    result = subprocess.run(['systemctl', 'is-active', SERVICE], check=False, capture_output=True, text=True)
    return result.returncode == 0 and result.stdout.strip() == 'active'


def stop_service_if_running() -> bool:
    was_running = service_is_active()
    if was_running:
        subprocess.run(['sudo', 'systemctl', 'stop', SERVICE], check=False)
        time.sleep(1.5)
    return was_running


def start_service_if_requested(was_running: bool) -> None:
    if was_running:
        subprocess.run(['sudo', 'systemctl', 'start', SERVICE], check=False)


def write_meta_safely(profile: Dict[str, Any], secrets: Dict[str, Any], restart_after: bool = False) -> bool:
    was_running = stop_service_if_running()
    conn = None
    try:
        conn = connect()
        conn.execute('PRAGMA busy_timeout = 5000')
        conn.execute('BEGIN IMMEDIATE')
        raw_profile = json.dumps(profile)
        raw_secrets = json.dumps(secrets)
        conn.execute(
            'INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
            ('profile', raw_profile),
        )
        conn.execute(
            'INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
            ('secrets', raw_secrets),
        )
        conn.commit()
    except sqlite3.OperationalError as exc:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        print(f'Failed to save settings: {exc}')
        if 'locked' in str(exc).lower():
            print('Database remained locked. Make sure no other process is using the DB.')
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        if restart_after:
            start_service_if_requested(was_running)
        return False
    except Exception as exc:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
        if restart_after:
            start_service_if_requested(was_running)
        print(f'Failed to save settings: {exc}')
        return False
    else:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        if restart_after:
            start_service_if_requested(was_running)
            print('Saved and restarted service.')
        else:
            print('Saved.')
        return True


def reset_trading_state(conn: sqlite3.Connection) -> None:
    clear()
    print('=' * 72)
    print('Reset Trading History')
    print('=' * 72)
    print('This will delete trade history, reconciliations, PnL journal, alerts, signals, snapshots, runtime status,')
    print('tracked positions, target portfolio, and other bot state.')
    print('It will keep your saved profile, wallet selection, and secrets.')
    confirm = input("Type RESET to continue: ").strip()
    if confirm != 'RESET':
        print('Cancelled.')
        time.sleep(1)
        return

    try:
        conn.close()
    except Exception:
        pass

    was_running = stop_service_if_running()
    wipe_conn = None
    try:
        wipe_conn = connect()
        wipe_conn.execute('PRAGMA busy_timeout = 5000')
        wipe_conn.execute('BEGIN IMMEDIATE')
        for table in (
            'trades', 'pnl_journal', 'execution_reconciliations', 'alerts',
            'history', 'validator_snapshots', 'subnet_snapshots'
        ):
            try:
                wipe_conn.execute(f'DELETE FROM {table}')
            except sqlite3.OperationalError:
                pass
        keys_to_remove = [
            'feature_history', 'watch_stages', 'latest_mark_snapshot', 'last_rankings',
            'target_portfolio', 'last_trade_times', 'position_state', 'behavior_bias',
            'runtime_status', 'realized_pnl_tao', 'simulated_pnl_tao', 'wallet_balance_cache'
        ]
        wipe_conn.executemany('DELETE FROM meta WHERE key = ?', [(k,) for k in keys_to_remove])
        wipe_conn.commit()
        print('Trading history and runtime state cleared.')
    except sqlite3.OperationalError as exc:
        if wipe_conn is not None:
            try:
                wipe_conn.rollback()
            except Exception:
                pass
        print(f'Reset failed: {exc}')
    finally:
        if wipe_conn is not None:
            try:
                wipe_conn.close()
            except Exception:
                pass
        start_service_if_requested(was_running)
        time.sleep(1)


def edit_saved_settings(conn: sqlite3.Connection) -> None:
    profile = get_meta(conn, 'profile', {}) or {}
    secrets = get_meta(conn, 'secrets', {}) or {}
    if not profile:
        print('No saved profile found in DB.')
        return
    fields = [
        ('execution_mode', 'Execution mode', str),
        ('live_mode', 'Live mode (true/false)', lambda x: x.strip().lower() in {'1','true','yes','y'}),
        ('default_validator_hotkey', 'Default validator hotkey', str),
        ('reserve_buffer_tao', 'Reserve buffer TAO', float),
        ('position_sizing_mode', 'Sizing mode (fixed/percent)', str),
        ('fixed_entry_tao', 'Fixed entry TAO', float),
        ('percent_of_free_tao', '% of free TAO per entry', float),
        ('max_position_tao', 'Max TAO per subnet', float),
        ('max_position_pct', 'Max % deployable per subnet', float),
        ('daily_max_tao', 'Daily max TAO', float),
        ('daily_max_loss_tao', 'Daily max loss TAO', float),
        ('max_trades_per_hour', 'Max trades per hour', int),
        ('portfolio_size', 'Portfolio size', int),
        ('max_entry_impact500', 'Max entry impact500', float),
        ('block_entry_if_impact1000_above', 'Block entry if impact1000 above', float),
        ('block_entry_if_reserve_below', 'Min reserve floor', float),
        ('min_entry_taostats_score', 'Min TaoStats score for entry', float),
        ('max_new_entries_per_cycle', 'Max new entries per cycle', int),
        ('max_exits_per_cycle', 'Max exits per cycle', int),
        ('trade_cooldown_seconds', 'Trade cooldown seconds', int),
        ('min_hold_time_seconds', 'Min hold time seconds', int),
        ('partial_exit_pct', 'Partial exit %', float),
    ]
    while True:
        clear()
        print('=' * 72)
        print('Edit Saved Settings')
        print('=' * 72)
        i = 1
        index_map: Dict[str, tuple[str, Any]] = {}
        for key, label, caster in fields:
            print(f'{i}. {label}: {profile.get(key)}')
            index_map[str(i)] = ('profile', (key, caster, label))
            i += 1
        print(f'{i}. TaoStats API key: {"set" if secrets.get("taostats_api_key") else "blank"}')
        index_map[str(i)] = ('secret', ('taostats_api_key', str, 'TaoStats API key'))
        i += 1
        print(f'{i}. Discord webhook URL: {"set" if secrets.get("discord_webhook_url") else "blank"}')
        index_map[str(i)] = ('secret', ('discord_webhook_url', str, 'Discord webhook URL'))
        i += 1
        print('S. Save and return')
        print('R. Save and restart service')
        print('0. Cancel')
        choice = input('Choose a setting: ').strip()
        if choice == '0':
            return
        if choice.lower() == 's':
            if write_meta_safely(profile, secrets, restart_after=False):
                time.sleep(1)
                return
            pause()
            continue
        if choice.lower() == 'r':
            if write_meta_safely(profile, secrets, restart_after=True):
                time.sleep(1)
                return
            pause()
            continue
        if choice not in index_map:
            continue
        kind, payload = index_map[choice]
        key, caster, label = payload
        current = profile.get(key) if kind == 'profile' else secrets.get(key)
        raw = input(f'{label} [{current}]: ').strip()
        if raw == '':
            continue
        try:
            value = caster(raw)
            if kind == 'profile':
                profile[key] = value
            else:
                secrets[key] = value
        except Exception:
            print('Invalid value.')
            time.sleep(1)


def main() -> None:
    while True:
        clear()
        print('=' * 72)
        print('Rotation Bot Control Panel')
        print('=' * 72)
        print('1. Dashboard')
        print('2. View portfolio')
        print('3. View trade journal')
        print('4. View PnL summary')
        print('5. View latest signals')
        print('6. Export CSV reports')
        print('7. View live logs')
        print('8. Start bot')
        print('9. Stop bot')
        print('10. Restart bot')
        print('11. Edit saved settings')
        print('12. Reset trading history/state')
        print('0. Exit')
        choice = input('\nSelect an option: ').strip()
        conn = connect()
        try:
            if choice == '1':
                dashboard(conn)
                pause()
            elif choice == '2':
                view_portfolio(conn)
                pause()
            elif choice == '3':
                view_trade_journal(conn)
                pause()
            elif choice == '4':
                pnl_summary(conn)
                pause()
            elif choice == '5':
                latest_signals(conn)
                pause()
            elif choice == '6':
                export_reports(conn)
                pause()
            elif choice == '7':
                conn.close()
                view_logs()
                continue
            elif choice == '8':
                service_cmd('start')
                pause()
            elif choice == '9':
                service_cmd('stop')
                pause()
            elif choice == '10':
                service_cmd('restart')
                pause()
            elif choice == '11':
                edit_saved_settings(conn)
            elif choice == '12':
                reset_trading_state(conn)
            elif choice == '0':
                break
        finally:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == '__main__':
    main()
