#!/usr/bin/env python3
from __future__ import annotations
import getpass
import json
import math
import os
import sqlite3
import sys
import subprocess
import time
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import requests
try:
    import bittensor as bt
except Exception as exc:
    print("Failed to import bittensor. Install it first:", file=sys.stderr)
    print("  pip install -U bittensor requests", file=sys.stderr)
    raise exc
# ============================================================
# CORE SETTINGS
# ============================================================
NETWORK = "finney"
MECHID = 0
SUBNETS: str | List[int] = "all"
POLL_SECONDS = int(os.getenv("ROTATION_POLL_SECONDS", "120"))
TOP_LOCAL_CONFIRM = 12
SQLITE_DB_PATH = os.getenv("ROTATION_DB_PATH", "rotation_bot.db")
MIN_VALIDATOR_DELTA = 0.01
MIN_CLUSTER = 2
BUY_SCORE_THRESHOLD = 0.78
WATCH_SCORE_THRESHOLD = 0.62
SELL_RISK_THRESHOLD = 0.22

HISTORY_WINDOW = 6
SETUP_SCORE_MIN = 0.50
TRIGGER_SCORE_MIN = 0.58
ENTRY_DECAY_MAX = 0.42
SOFT_EXIT_DECAY = 0.60
HARD_EXIT_DECAY = 0.72
WATCH_STAGE_ENTRY_MIN = 3
PERSISTENCE_MIN_POINTS = 2
WATCH_VALIDATORS: Dict[str, float] = {
    # "hotkey_ss58_here": 0.15,
}
ALPHA_EXIT_SIZES = [100, 500, 1000]
DEBUG = True
SEND_ERROR_MESSAGES = True
SEND_STARTUP_MESSAGE = True
# Composite score weights (local scan)
W_VALIDATOR_DELTA = 0.30
W_CLUSTER = 0.16
W_ELI = 0.16
W_WATCHLIST = 0.06
W_TAO_FLOW = 0.14
W_RESERVE_CHANGE = 0.10
W_TRADE_ACTIVITY = 0.08
# TaoStats confirmation weights
LOCAL_SCORE_WEIGHT = 0.82
TAOSTATS_SCORE_WEIGHT = 0.18
W_TS_LIQUIDITY = 0.34
W_TS_FLOW = 0.23
W_TS_GITHUB = 0.18
W_TS_PRICE = 0.10
W_TS_EMISSION = 0.10
W_TS_REGISTERED = 0.05
DEFAULT_WALLET_DIR = "/root/.bittensor/wallets"
DEFAULT_VALIDATOR_NAME = "tao.bot"
DEFAULT_VALIDATOR_HOTKEY = "5E2LP6EnZ54m3wS8s1yPvD5c3xo71kQroBw7aUVK32TKeZ5u"
AGGRESSIVE_FIELD_FALLBACKS = True
# Optional env overrides for non-interactive runs
BT_WALLET_NAME = os.getenv("BT_WALLET_NAME", "").strip()
BT_WALLET_PATH = os.getenv("BT_WALLET_PATH", DEFAULT_WALLET_DIR).strip() or DEFAULT_WALLET_DIR
BT_HOTKEY_NAME = os.getenv("BT_HOTKEY_NAME", "").strip()
ROTATION_WALLET_PASSWORD = os.getenv("ROTATION_WALLET_PASSWORD", "")
NONINTERACTIVE = (not sys.stdin.isatty()) or os.getenv("ROTATION_NONINTERACTIVE", "0").strip() in {"1", "true", "yes"}
# ============================================================
# DATA MODELS
# ============================================================
@dataclass
class AppSecrets:
    taostats_api_key: str = ""
    discord_webhook_url: str = ""
@dataclass
class WalletSelection:
    wallet_name: str
    wallet_path: str
    hotkey_name: str = ""
@dataclass
class Signal:
    netuid: int
    name: str
    classification: str
    score: float
    confidence: str
    validator_delta: float
    validator_cluster: int
    eli: float
    watchlist_score: float
    regime: str
    impact100: float
    impact500: float
    impact1000: float
    liquidity_risk: str
    suggested_size: str
    exit_signal: bool
    reserve_now: float
    reserve_delta: float
    tao_flow_proxy: float
    trade_activity_proxy: float
    updated_at: str
    detail: str
    local_score: float = 0.0
    taostats_score: float = 0.0
    taostats_notes: str = ""
    rank: int = 0
    previous_rank: Optional[int] = None
    rank_delta: Optional[int] = None
    previous_score: Optional[float] = None
    score_delta: Optional[float] = None
    action: str = "WATCH"
@dataclass
class TradeDecision:
    side: str  # ENTRY / EXIT
    netuid: int
    name: str
    amount_tao: float
    reason: str
    expected_score: float
    expected_impact500: float
@dataclass
class Profile:
    name: str
    style: str  # conservative / balanced / aggressive / custom
    strategy_mode: str  # early_rotation_hunter / momentum_follower / capital_preserver
    execution_mode: str  # signals_only / confirm / auto
    live_mode: bool
    position_sizing_mode: str  # fixed / percent
    fixed_entry_tao: float
    percent_of_free_tao: float
    reserve_buffer_tao: float
    max_position_tao: float
    max_position_pct: float
    exit_mode: str  # full / partial
    partial_exit_pct: float
    portfolio_size: int
    entry_score_min: float
    watch_score_min: float
    exit_score_max: float
    max_entry_impact500: float
    block_entry_if_impact1000_above: float
    block_entry_if_reserve_below: float
    min_entry_taostats_score: float
    max_new_entries_per_cycle: int
    max_exits_per_cycle: int
    trade_cooldown_seconds: int
    daily_max_tao: float
    daily_max_loss_tao: float
    max_trades_per_hour: int
    min_hold_time_seconds: int
    explain_trades: bool
    send_discord_updates: bool
    default_validator_hotkey: str = DEFAULT_VALIDATOR_HOTKEY
    wallet_name: str = ""
    wallet_path: str = DEFAULT_WALLET_DIR
    hotkey_name: str = ""
# ============================================================
# UTILITIES
# ============================================================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
def now_ts() -> int:
    return int(time.time())
def log(msg: str) -> None:
    if DEBUG:
        print(msg, flush=True)
def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default
def safe_list(value: Any) -> List[Any]:
    try:
        if hasattr(value, "tolist"):
            return value.tolist()
        return list(value)
    except Exception:
        return []
def truncate(text: str, max_len: int = 1800) -> str:
    return text if len(text) <= max_len else text[: max_len - 3] + "..."
def normalize(values: List[float]) -> List[float]:
    if not values:
        return []
    lo = min(values)
    hi = max(values)
    if math.isclose(lo, hi):
        return [0.5 for _ in values]
    return [(v - lo) / (hi - lo) for v in values]
def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))

def is_tradeable_netuid(netuid: int) -> bool:
    return int(netuid) != 0

def get_validator_hotkey(profile: Optional[Profile]) -> str:
    hotkey = getattr(profile, "default_validator_hotkey", "") if profile is not None else ""
    hotkey = (hotkey or "").strip()
    return hotkey or DEFAULT_VALIDATOR_HOTKEY

def logistic(x: float, center: float = 0.0, steepness: float = 1.0) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-steepness * (x - center)))
    except OverflowError:
        return 1.0 if x > center else 0.0
def scale_positive(x: float, softness: float = 1.0) -> float:
    if x <= 0:
        return 0.0
    return x / (x + softness)
def get_first(d: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default
def fmt_signed(value: Optional[float], places: int = 4) -> str:
    if value is None:
        return "n/a"
    return f"{value:+.{places}f}"
def fmt_rank_delta(value: Optional[int]) -> str:
    if value is None:
        return "new"
    if value > 0:
        return f"↑{value}"
    if value < 0:
        return f"↓{abs(value)}"
    return "—"
def score_to_confidence(score: float) -> str:
    if score >= 0.85:
        return "A"
    if score >= 0.75:
        return "A-"
    if score >= 0.65:
        return "B+"
    if score >= 0.55:
        return "B"
    if score >= 0.45:
        return "C"
    if score >= 0.35:
        return "D"
    return "F"
def score_to_classification(
    score: float,
    validator_cluster: int,
    validator_delta: float,
    tao_flow_proxy: float,
    *,
    setup_score: float = 0.0,
    trigger_score: float = 0.0,
    decay_score: float = 0.0,
    watch_stage: int = 0,
) -> str:
    if decay_score >= HARD_EXIT_DECAY:
        return "SELL-RISK"
    if (
        score >= BUY_SCORE_THRESHOLD
        and watch_stage >= WATCH_STAGE_ENTRY_MIN
        and trigger_score >= TRIGGER_SCORE_MIN
        and setup_score >= SETUP_SCORE_MIN
        and decay_score <= ENTRY_DECAY_MAX
        and validator_cluster >= 1
        and validator_delta > 0
    ):
        return "BUY"
    if setup_score >= SETUP_SCORE_MIN or score >= WATCH_SCORE_THRESHOLD or watch_stage > 0:
        return "WATCH"
    if score <= SELL_RISK_THRESHOLD and (validator_cluster == 0 or validator_delta <= 0 or tao_flow_proxy < 0):
        return "SELL-RISK"
    return "NEUTRAL"


def estimate_regime(
    validator_cluster: int,
    validator_delta: float,
    eli: float,
    tao_flow_proxy: float,
    reserve_delta: float,
) -> str:
    if validator_cluster >= 2 and validator_delta > 0 and reserve_delta > 0 and tao_flow_proxy > 0:
        return "WEIGHT ROTATION"
    if validator_cluster >= 1 and reserve_delta > 0:
        return "EARLY MOMENTUM"
    if tao_flow_proxy < 0 and reserve_delta < 0:
        return "DISTRIBUTION"
    if eli < 0.25 and validator_cluster == 0:
        return "DEAD-LIQUIDITY"
    return "RANGE"


def risk_from_impact(impact500: float) -> str:
    if impact500 >= 8.0:
        return "High"
    if impact500 >= 4.0:
        return "Medium"
    return "Low"
def suggested_size_from_impact(impact100: float, impact500: float, impact1000: float, classification: str) -> str:
    if classification == "SELL-RISK":
        return "Reduce / Exit"
    if impact500 <= 3.0:
        return "500–1000 TAO"
    if impact500 <= 6.0:
        return "250–500 TAO"
    if impact1000 <= 12.0:
        return "150–300 TAO"
    return "≤150 TAO"
def post_discord(webhook_url: str, content: str) -> None:
    if not webhook_url:
        return
    payload = {"content": truncate(content)}
    r = requests.post(webhook_url, json=payload, timeout=20)
    r.raise_for_status()
def prompt_choice(prompt: str, options: List[str], default_index: int = 0) -> str:
    if NONINTERACTIVE:
        return options[default_index]
    while True:
        print(prompt, flush=True)
        for idx, opt in enumerate(options, start=1):
            suffix = " (default)" if idx - 1 == default_index else ""
            print(f"  {idx}. {opt}{suffix}", flush=True)
        raw = input("Choose: ").strip()
        if raw == "":
            return options[default_index]
        if raw.isdigit():
            idx = int(raw) - 1
            if 0 <= idx < len(options):
                return options[idx]
        print("Invalid choice.\n", flush=True)
def prompt_bool(prompt: str, default: bool) -> bool:
    if NONINTERACTIVE:
        return default
    hint = "Y/n" if default else "y/N"
    raw = input(f"{prompt} [{hint}]: ").strip().lower()
    if raw == "":
        return default
    return raw in {"y", "yes", "true", "1"}
def prompt_float(prompt: str, default: float) -> float:
    if NONINTERACTIVE:
        return default
    while True:
        raw = input(f"{prompt} [{default}]: ").strip()
        if raw == "":
            return default
        try:
            return float(raw)
        except Exception:
            print("Please enter a number.", flush=True)
def prompt_int(prompt: str, default: int) -> int:
    if NONINTERACTIVE:
        return default
    while True:
        raw = input(f"{prompt} [{default}]: ").strip()
        if raw == "":
            return default
        try:
            return int(raw)
        except Exception:
            print("Please enter an integer.", flush=True)
# ============================================================
# SQLITE
# ============================================================
def db_connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS validator_snapshots (
            netuid INTEGER NOT NULL,
            mechid INTEGER NOT NULL,
            uid INTEGER NOT NULL,
            hotkey TEXT NOT NULL,
            weights_json TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (netuid, mechid, uid)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS subnet_snapshots (
            netuid INTEGER NOT NULL,
            mechid INTEGER NOT NULL,
            reserve_now REAL NOT NULL,
            emission_now REAL NOT NULL,
            impact100 REAL NOT NULL,
            impact500 REAL NOT NULL,
            impact1000 REAL NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (netuid, mechid)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            alert_key TEXT PRIMARY KEY,
            sent_at INTEGER NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            netuid INTEGER NOT NULL,
            name TEXT NOT NULL,
            classification TEXT NOT NULL,
            score REAL NOT NULL,
            validator_delta REAL NOT NULL,
            validator_cluster INTEGER NOT NULL,
            eli REAL NOT NULL,
            reserve_now REAL NOT NULL,
            reserve_delta REAL NOT NULL,
            tao_flow_proxy REAL NOT NULL,
            trade_activity_proxy REAL NOT NULL,
            impact100 REAL NOT NULL,
            impact500 REAL NOT NULL,
            impact1000 REAL NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            side TEXT NOT NULL,
            netuid INTEGER NOT NULL,
            name TEXT NOT NULL,
            amount_tao REAL NOT NULL,
            status TEXT NOT NULL,
            detail TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pnl_journal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            netuid INTEGER NOT NULL,
            name TEXT NOT NULL,
            event TEXT NOT NULL,
            amount_tao REAL NOT NULL,
            cost_basis_tao REAL NOT NULL,
            est_value_tao REAL NOT NULL,
            realized_pnl_tao REAL NOT NULL,
            remaining_cost_basis_tao REAL NOT NULL,
            remaining_est_value_tao REAL NOT NULL,
            detail TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS execution_reconciliations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            side TEXT NOT NULL,
            netuid INTEGER NOT NULL,
            name TEXT NOT NULL,
            requested_amount_tao REAL NOT NULL,
            actual_wallet_delta_tao REAL,
            actual_stake_delta_tao REAL,
            chain_success INTEGER NOT NULL,
            detail TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn
def get_meta(conn: sqlite3.Connection, key: str) -> Optional[str]:
    cur = conn.execute("SELECT value FROM meta WHERE key = ?", (key,))
    row = cur.fetchone()
    return None if row is None else str(row[0])
def set_meta(conn: sqlite3.Connection, key: str, value: Any) -> None:
    if not isinstance(value, (str, int, float, bytes, type(None))):
        value = json.dumps(value)
    conn.execute(
        """
        INSERT INTO meta (key, value)
        VALUES (?, ?)
        ON CONFLICT(key)
        DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )
    if not conn.in_transaction:
        conn.commit()
def get_json_meta(conn: sqlite3.Connection, key: str, default: Any) -> Any:
    raw = get_meta(conn, key)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default
def save_json_meta(conn: sqlite3.Connection, key: str, value: Any) -> None:
    set_meta(conn, key, json.dumps(value))

def load_feature_history(conn: sqlite3.Connection) -> Dict[str, List[Dict[str, Any]]]:
    data = get_json_meta(conn, "feature_history", {})
    return data if isinstance(data, dict) else {}


def save_feature_history(conn: sqlite3.Connection, data: Dict[str, List[Dict[str, Any]]]) -> None:
    save_json_meta(conn, "feature_history", data)


def load_watchlist_state(conn: sqlite3.Connection) -> Dict[str, int]:
    data = get_json_meta(conn, "watch_stages", {})
    if not isinstance(data, dict):
        return {}
    out: Dict[str, int] = {}
    for key, value in data.items():
        try:
            out[str(key)] = int(value)
        except Exception:
            continue
    return out


def save_watchlist_state(conn: sqlite3.Connection, data: Dict[str, int]) -> None:
    save_json_meta(conn, "watch_stages", data)


def load_latest_mark_snapshot(conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    data = get_json_meta(conn, "latest_mark_snapshot", {})
    return data if isinstance(data, dict) else {}


def save_latest_mark_snapshot(conn: sqlite3.Connection, data: Dict[str, Dict[str, Any]]) -> None:
    save_json_meta(conn, "latest_mark_snapshot", data)


def signal_mark_value(sig: Signal) -> float:
    return max(safe_float(getattr(sig, "reserve_now", 0.0), 0.0), 1e-9)


def latest_mark_value_for_netuid(conn: sqlite3.Connection, netuid: int, fallback: float = 1.0) -> float:
    snap = load_latest_mark_snapshot(conn).get(str(netuid), {})
    mark = safe_float(snap.get("mark"), fallback)
    return mark if mark > 0 else fallback


def estimate_position_value_from_row(conn: sqlite3.Connection, netuid: int, row: Dict[str, Any]) -> float:
    alpha_held = safe_float(row.get("alpha_held"), safe_float(row.get("staked_tao"), None))
    basis = safe_float(row.get("tao_basis"), safe_float(row.get("amount_tao"), 0.0))
    entry_mark = max(safe_float(row.get("entry_mark"), 0.0), 1e-9)
    current_mark = latest_mark_value_for_netuid(conn, netuid, fallback=entry_mark if entry_mark > 0 else 1.0)
    if alpha_held is not None and alpha_held > 0:
        return round(alpha_held * current_mark, 6)
    if basis <= 0:
        return 0.0
    return round(basis * (current_mark / max(entry_mark, 1e-9)), 6)


def compute_portfolio_unrealized_pnl(conn: sqlite3.Connection) -> Tuple[float, float, float]:
    state = load_position_state(conn)
    total_basis = 0.0
    total_value = 0.0
    for netuid, row in state.items():
        basis = safe_float(row.get("tao_basis"), safe_float(row.get("amount_tao"), 0.0))
        total_basis += basis
        total_value += estimate_position_value_from_row(conn, int(netuid), row)
    return round(total_basis, 6), round(total_value, 6), round(total_value - total_basis, 6)


def cumulative_realized_pnl_tao(conn: sqlite3.Connection) -> float:
    raw = get_meta(conn, "realized_pnl_tao")
    return safe_float(raw, 0.0)


def add_realized_pnl_tao(conn: sqlite3.Connection, delta: float) -> None:
    total = cumulative_realized_pnl_tao(conn) + delta
    set_meta(conn, "realized_pnl_tao", f"{total:.12f}")
    set_meta(conn, "simulated_pnl_tao", f"{total:.12f}")


def insert_pnl_journal(
    conn: sqlite3.Connection,
    netuid: int,
    name: str,
    event: str,
    amount_tao: float,
    cost_basis_tao: float,
    est_value_tao: float,
    realized_pnl_tao: float,
    remaining_cost_basis_tao: float,
    remaining_est_value_tao: float,
    detail: str,
) -> None:
    conn.execute(
        """
        INSERT INTO pnl_journal (
            ts, netuid, name, event, amount_tao, cost_basis_tao, est_value_tao,
            realized_pnl_tao, remaining_cost_basis_tao, remaining_est_value_tao, detail
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            now_ts(), netuid, name, event, amount_tao, cost_basis_tao, est_value_tao,
            realized_pnl_tao, remaining_cost_basis_tao, remaining_est_value_tao, detail,
        ),
    )
    conn.commit()


def insert_execution_reconciliation(
    conn: sqlite3.Connection,
    side: str,
    netuid: int,
    name: str,
    requested_amount_tao: float,
    actual_wallet_delta_tao: Optional[float],
    actual_stake_delta_tao: Optional[float],
    chain_success: bool,
    detail: Dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO execution_reconciliations (
            ts, side, netuid, name, requested_amount_tao, actual_wallet_delta_tao,
            actual_stake_delta_tao, chain_success, detail
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            now_ts(), side, netuid, name, requested_amount_tao,
            None if actual_wallet_delta_tao is None else round(actual_wallet_delta_tao, 12),
            None if actual_stake_delta_tao is None else round(actual_stake_delta_tao, 12),
            1 if chain_success else 0,
            json.dumps(detail, default=str),
        ),
    )
    conn.commit()


def _mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _ratio(values: List[bool]) -> float:
    return (sum(1 for v in values if v) / len(values)) if values else 0.0


def _history_slice(history: List[Dict[str, Any]], points: int = HISTORY_WINDOW - 1) -> List[Dict[str, Any]]:
    return history[-points:] if history else []


def build_feature_snapshot(sig: Signal) -> Dict[str, Any]:
    return {
        "ts": now_ts(),
        "validator_delta": sig.validator_delta,
        "validator_cluster": sig.validator_cluster,
        "reserve_delta": sig.reserve_delta,
        "tao_flow_proxy": sig.tao_flow_proxy,
        "trade_activity_proxy": sig.trade_activity_proxy,
        "impact500": sig.impact500,
        "score": sig.score,
        "rank": sig.rank,
        "setup_score": sig.setup_score,
        "trigger_score": sig.trigger_score,
        "decay_score": sig.decay_score,
    }


def classify_lifecycle(sig: Signal) -> str:
    if sig.decay_score >= HARD_EXIT_DECAY:
        return "EXIT CANDIDATE"
    if sig.decay_score >= SOFT_EXIT_DECAY:
        return "DECAYING"
    if sig.trigger_score >= TRIGGER_SCORE_MIN and sig.watch_stage >= WATCH_STAGE_ENTRY_MIN:
        return "ACTIVE MOMENTUM"
    if sig.setup_score >= SETUP_SCORE_MIN:
        return "BUILDING"
    return "SCANNING"


def enrich_signals_with_rotation_metrics(
    signals: List[Signal],
    feature_history: Dict[str, List[Dict[str, Any]]],
    watch_state: Dict[str, int],
    profile: Profile,
) -> None:
    for sig in signals:
        history = _history_slice(feature_history.get(str(sig.netuid), []))
        prev_weight_deltas = [safe_float(x.get("validator_delta"), 0.0) for x in history]
        prev_clusters = [safe_float(x.get("validator_cluster"), 0.0) for x in history]
        prev_reserve_deltas = [safe_float(x.get("reserve_delta"), 0.0) for x in history]
        prev_impacts = [safe_float(x.get("impact500"), 0.0) for x in history]

        avg_weight_delta = _mean(prev_weight_deltas)
        avg_cluster = _mean(prev_clusters)
        avg_reserve_delta = _mean(prev_reserve_deltas)
        avg_impact = _mean(prev_impacts)

        sig.weight_slope = sig.validator_delta - avg_weight_delta
        sig.cluster_slope = sig.validator_cluster - avg_cluster
        sig.reserve_slope = sig.reserve_delta - avg_reserve_delta
        sig.impact_slope = sig.impact500 - avg_impact

        positive_weight_changes = [safe_float(x.get("validator_delta"), 0.0) > 0 for x in history] + [sig.validator_delta > 0]
        positive_breadth = [safe_float(x.get("validator_cluster"), 0.0) > 0 for x in history] + [sig.validator_cluster > 0]
        positive_flow = [
            (safe_float(x.get("reserve_delta"), 0.0) > 0) or (safe_float(x.get("tao_flow_proxy"), 0.0) > 0)
            for x in history
        ] + [(sig.reserve_delta > 0) or (sig.tao_flow_proxy > 0)]

        sig.breadth_persistence = _ratio(positive_breadth)
        sig.flow_persistence = _ratio(positive_flow)
        sig.signal_persistence = sum(1 for v in positive_weight_changes if v)

        reserve_softness = max(1.0, sig.reserve_now * 0.002)
        activity_softness = max(1.0, sig.reserve_now * 0.003)
        early_capacity = clamp01(1.0 - (sig.impact500 / max(profile.max_entry_impact500 * 1.35, 1.0)))
        rank_boost = scale_positive(float(max(sig.rank_delta or 0, 0)), softness=2.0)
        score_boost = scale_positive(max(sig.score_delta or 0.0, 0.0), softness=0.06)
        ts_support = sig.taostats_score if sig.taostats_score > 0 else 0.5
        weight_level = scale_positive(sig.validator_delta, softness=max(MIN_VALIDATOR_DELTA, 0.01))
        breadth_level = scale_positive(float(sig.validator_cluster), softness=2.0)
        weight_accel = scale_positive(sig.weight_slope, softness=max(MIN_VALIDATOR_DELTA, 0.01))
        reserve_level = scale_positive(sig.reserve_delta, softness=reserve_softness)
        flow_level = scale_positive(sig.tao_flow_proxy, softness=reserve_softness)
        activity_level = scale_positive(sig.trade_activity_proxy, softness=activity_softness)

        sig.setup_score = clamp01(
            0.20 * weight_level
            + 0.16 * breadth_level
            + 0.14 * reserve_level
            + 0.08 * flow_level
            + 0.10 * activity_level
            + 0.14 * sig.breadth_persistence
            + 0.10 * weight_accel
            + 0.08 * early_capacity
        )

        impact_exhaustion = scale_positive(max(sig.impact_slope, 0.0), softness=1.5)
        sig.trigger_score = clamp01(
            0.26 * sig.score
            + 0.18 * sig.setup_score
            + 0.16 * sig.breadth_persistence
            + 0.10 * sig.flow_persistence
            + 0.08 * rank_boost
            + 0.08 * score_boost
            + 0.08 * ts_support
            + 0.06 * early_capacity
            - 0.08 * impact_exhaustion
        )

        weight_rollover = scale_positive(max(avg_weight_delta - sig.validator_delta, 0.0), softness=max(MIN_VALIDATOR_DELTA, 0.01))
        breadth_rollover = scale_positive(max(avg_cluster - sig.validator_cluster, 0.0), softness=1.0)
        reserve_rollover = scale_positive(max(avg_reserve_delta - sig.reserve_delta, 0.0), softness=reserve_softness)
        neg_reserve = scale_positive(-sig.reserve_delta, softness=reserve_softness)
        neg_score = scale_positive(-(sig.score_delta or 0.0), softness=0.05)
        overcrowded = scale_positive(max(sig.impact500 - profile.max_entry_impact500, 0.0), softness=2.0)

        sig.decay_score = clamp01(
            0.24 * weight_rollover
            + 0.18 * breadth_rollover
            + 0.18 * reserve_rollover
            + 0.12 * neg_reserve
            + 0.08 * (1.0 - sig.breadth_persistence)
            + 0.08 * impact_exhaustion
            + 0.07 * overcrowded
            + 0.05 * neg_score
        )

        sig.conviction_score = clamp01(
            0.42 * sig.trigger_score
            + 0.26 * sig.setup_score
            + 0.18 * sig.score
            + 0.08 * ts_support
            + 0.06 * score_boost
            - 0.20 * sig.decay_score
        )

        prev_stage = int(watch_state.get(str(sig.netuid), 0))
        stage = prev_stage
        if sig.setup_score >= SETUP_SCORE_MIN:
            stage = max(stage, 1)
        if sig.setup_score >= SETUP_SCORE_MIN and sig.signal_persistence >= PERSISTENCE_MIN_POINTS:
            stage = max(stage, 2)
        if sig.trigger_score >= TRIGGER_SCORE_MIN and sig.decay_score <= ENTRY_DECAY_MAX:
            stage = max(stage, WATCH_STAGE_ENTRY_MIN)
        if sig.conviction_score >= profile.entry_score_min and sig.decay_score <= ENTRY_DECAY_MAX:
            stage = max(stage, 4)

        if sig.decay_score >= HARD_EXIT_DECAY:
            stage = 0
        elif sig.decay_score >= SOFT_EXIT_DECAY:
            stage = max(1, stage - 1)
        elif sig.setup_score < 0.35 and sig.trigger_score < 0.35:
            stage = max(0, stage - 1)

        sig.watch_stage = stage
        sig.entry_ready = (
            sig.watch_stage >= WATCH_STAGE_ENTRY_MIN
            and sig.setup_score >= SETUP_SCORE_MIN
            and sig.trigger_score >= TRIGGER_SCORE_MIN
            and sig.decay_score <= ENTRY_DECAY_MAX
        )
        sig.lifecycle = classify_lifecycle(sig)
        sig.score = sig.conviction_score
        sig.confidence = score_to_confidence(sig.score)
def insert_history(conn: sqlite3.Connection, sig: Signal) -> None:
    conn.execute(
        """
        INSERT INTO history (
            netuid, name, classification, score,
            validator_delta, validator_cluster, eli,
            reserve_now, reserve_delta, tao_flow_proxy, trade_activity_proxy,
            impact100, impact500, impact1000, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            sig.netuid,
            sig.name,
            sig.classification,
            sig.score,
            sig.validator_delta,
            sig.validator_cluster,
            sig.eli,
            sig.reserve_now,
            sig.reserve_delta,
            sig.tao_flow_proxy,
            sig.trade_activity_proxy,
            sig.impact100,
            sig.impact500,
            sig.impact1000,
            sig.updated_at,
        ),
    )
    if not conn.in_transaction:
        conn.commit()
def insert_trade(conn: sqlite3.Connection, side: str, netuid: int, name: str, amount_tao: float, status: str, detail: str) -> None:
    conn.execute(
        """
        INSERT INTO trades (ts, side, netuid, name, amount_tao, status, detail)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (now_ts(), side, netuid, name, amount_tao, status, detail),
    )
    conn.commit()
def was_alerted_recently(conn: sqlite3.Connection, alert_key: str, cooldown: int) -> bool:
    cur = conn.execute("SELECT sent_at FROM alerts WHERE alert_key = ?", (alert_key,))
    row = cur.fetchone()
    if not row:
        return False
    return (now_ts() - int(row[0])) < cooldown
def mark_alerted(conn: sqlite3.Connection, alert_key: str) -> None:
    conn.execute(
        """
        INSERT INTO alerts (alert_key, sent_at)
        VALUES (?, ?)
        ON CONFLICT(alert_key)
        DO UPDATE SET sent_at = excluded.sent_at
        """,
        (alert_key, now_ts()),
    )
    conn.commit()
# ============================================================
# SETTINGS PERSISTENCE
# ============================================================
def load_saved_wallet_selection(conn: sqlite3.Connection) -> Optional[WalletSelection]:
    raw = get_json_meta(conn, "wallet_selection", None)
    if not raw:
        return None
    try:
        return WalletSelection(
            wallet_name=str(raw["wallet_name"]),
            wallet_path=str(raw["wallet_path"]),
            hotkey_name=str(raw.get("hotkey_name", "")),
        )
    except Exception:
        return None
def save_wallet_selection(conn: sqlite3.Connection, sel: WalletSelection) -> None:
    save_json_meta(
        conn,
        "wallet_selection",
        {"wallet_name": sel.wallet_name, "wallet_path": sel.wallet_path, "hotkey_name": sel.hotkey_name},
    )
def load_profile(conn: sqlite3.Connection) -> Optional[Profile]:
    raw = get_json_meta(conn, "profile", None)
    if not raw:
        return None
    try:
        return Profile(**raw)
    except Exception:
        return None
def save_profile(conn: sqlite3.Connection, profile: Profile) -> None:
    save_json_meta(conn, "profile", asdict(profile))
def load_secrets(conn: sqlite3.Connection) -> AppSecrets:
    raw = get_json_meta(conn, "secrets", {})
    if not isinstance(raw, dict):
        raw = {}
    return AppSecrets(
        taostats_api_key=str(raw.get("taostats_api_key", "")),
        discord_webhook_url=str(raw.get("discord_webhook_url", "")),
    )
def save_secrets(conn: sqlite3.Connection, secrets: AppSecrets) -> None:
    save_json_meta(conn, "secrets", asdict(secrets))
def load_prev_rankings(conn: sqlite3.Connection) -> Dict[int, Dict[str, Any]]:
    raw = get_json_meta(conn, "last_rankings", {})
    out: Dict[int, Dict[str, Any]] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            try:
                out[int(k)] = v
            except Exception:
                continue
    return out
def save_current_rankings(conn: sqlite3.Connection, signals: List[Signal]) -> None:
    payload: Dict[str, Dict[str, Any]] = {}
    for s in signals:
        payload[str(s.netuid)] = {
            "rank": s.rank,
            "score": s.score,
            "classification": s.classification,
            "name": s.name,
        }
    save_json_meta(conn, "last_rankings", payload)
def load_prev_target_portfolio(conn: sqlite3.Connection) -> List[int]:
    raw = get_json_meta(conn, "target_portfolio", [])
    try:
        return [int(x) for x in raw]
    except Exception:
        return []
def save_target_portfolio(conn: sqlite3.Connection, netuids: List[int]) -> None:
    save_json_meta(conn, "target_portfolio", [int(x) for x in netuids])
def load_last_trade_times(conn: sqlite3.Connection) -> Dict[str, int]:
    raw = get_json_meta(conn, "last_trade_times", {})
    if isinstance(raw, dict):
        return {str(k): int(v) for k, v in raw.items()}
    return {}
def save_last_trade_times(conn: sqlite3.Connection, data: Dict[str, int]) -> None:
    save_json_meta(conn, "last_trade_times", data)
def load_position_state(conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    raw = get_json_meta(conn, "position_state", {})
    return raw if isinstance(raw, dict) else {}
def save_position_state(conn: sqlite3.Connection, data: Dict[str, Dict[str, Any]]) -> None:
    save_json_meta(conn, "position_state", data)

def reconcile_position_state_from_chain(
    conn: sqlite3.Connection,
    subtensor: Any,
    wallet: Optional[Any],
    profile: Profile,
    netuids: Optional[List[int]] = None,
) -> Dict[str, Any]:
    state = load_position_state(conn)
    if wallet is None:
        return {"updated": 0, "removed": 0, "checked": 0, "error": "no wallet selected"}
    validator_hotkey = get_validator_hotkey(profile)
    if not validator_hotkey:
        return {"updated": 0, "removed": 0, "checked": 0, "error": "no validator hotkey configured"}

    if netuids is None:
        target_netuids = sorted({int(k) for k in state.keys() if str(k).isdigit()})
    else:
        target_netuids = sorted({int(n) for n in netuids})

    updated = 0
    removed = 0
    checked = 0
    for netuid in target_netuids:
        checked += 1
        key = str(netuid)
        cur = dict(state.get(key, {}) or {})
        actual_alpha = get_wallet_stake_tao_on_subnet(subtensor, wallet, validator_hotkey, netuid)
        if actual_alpha is None:
            continue
        actual_alpha = max(0.0, float(actual_alpha))
        current_mark = latest_mark_value_for_netuid(conn, netuid, fallback=max(safe_float(cur.get("entry_mark"), 0.0), 1e-9))
        if actual_alpha <= 1e-12:
            if key in state:
                state.pop(key, None)
                removed += 1
            continue
        prev_alpha = safe_float(cur.get("alpha_held"), safe_float(cur.get("staked_tao"), 0.0))
        cur["alpha_held"] = round(actual_alpha, 6)
        cur["staked_tao"] = round(actual_alpha, 6)
        cur["current_mark"] = round(current_mark, 6)
        cur["last_mark"] = round(current_mark, 6)
        cur.setdefault("name", cur.get("name") or f"SN{netuid}")
        basis = safe_float(cur.get("tao_basis"), safe_float(cur.get("amount_tao"), 0.0))
        if basis <= 0:
            estimated_basis = actual_alpha * current_mark
            cur["tao_basis"] = round(estimated_basis, 6)
            cur["amount_tao"] = round(estimated_basis, 6)
        state[key] = cur
        if abs(actual_alpha - prev_alpha) > 1e-9:
            updated += 1

    save_position_state(conn, state)
    return {"updated": updated, "removed": removed, "checked": checked}
def load_behavior_bias(conn: sqlite3.Connection) -> Dict[str, Any]:
    raw = get_json_meta(conn, "behavior_bias", {"avoid_high_impact": 0, "prefer_conservative": 0})
    return raw if isinstance(raw, dict) else {"avoid_high_impact": 0, "prefer_conservative": 0}
def save_behavior_bias(conn: sqlite3.Connection, data: Dict[str, Any]) -> None:
    save_json_meta(conn, "behavior_bias", data)
# ============================================================
# SNAPSHOT STORAGE
# ============================================================
def load_prev_rows(conn: sqlite3.Connection, netuid: int, mechid: int) -> Dict[int, Tuple[str, List[float]]]:
    cur = conn.execute(
        """
        SELECT uid, hotkey, weights_json
        FROM validator_snapshots
        WHERE netuid = ? AND mechid = ?
        """,
        (netuid, mechid),
    )
    rows: Dict[int, Tuple[str, List[float]]] = {}
    for uid, hotkey, weights_json in cur.fetchall():
        try:
            rows[int(uid)] = (str(hotkey), json.loads(weights_json))
        except Exception:
            continue
    return rows
def save_current_rows(conn: sqlite3.Connection, netuid: int, mechid: int, rows: Dict[int, Tuple[str, List[float]]]) -> None:
    t = now_ts()
    for uid, (hotkey, weights) in rows.items():
        conn.execute(
            """
            INSERT INTO validator_snapshots (netuid, mechid, uid, hotkey, weights_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(netuid, mechid, uid)
            DO UPDATE SET
                hotkey = excluded.hotkey,
                weights_json = excluded.weights_json,
                updated_at = excluded.updated_at
            """,
            (netuid, mechid, uid, hotkey, json.dumps(weights), t),
        )
    if not conn.in_transaction:
        conn.commit()
def load_prev_subnet_snapshot(conn: sqlite3.Connection, netuid: int, mechid: int) -> Optional[Tuple[float, float, float, float, float]]:
    cur = conn.execute(
        """
        SELECT reserve_now, emission_now, impact100, impact500, impact1000
        FROM subnet_snapshots
        WHERE netuid = ? AND mechid = ?
        """,
        (netuid, mechid),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return tuple(float(x) for x in row)  # type: ignore
def save_subnet_snapshot(
    conn: sqlite3.Connection,
    netuid: int,
    mechid: int,
    reserve_now: float,
    emission_now: float,
    impact100: float,
    impact500: float,
    impact1000: float,
) -> None:
    conn.execute(
        """
        INSERT INTO subnet_snapshots (
            netuid, mechid, reserve_now, emission_now, impact100, impact500, impact1000, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(netuid, mechid)
        DO UPDATE SET
            reserve_now = excluded.reserve_now,
            emission_now = excluded.emission_now,
            impact100 = excluded.impact100,
            impact500 = excluded.impact500,
            impact1000 = excluded.impact1000,
            updated_at = excluded.updated_at
        """,
        (netuid, mechid, reserve_now, emission_now, impact100, impact500, impact1000, now_ts()),
    )
    conn.commit()
# ============================================================
# PROFILES
# ============================================================
def conservative_profile() -> Profile:
    return Profile(
        name="Conservative",
        style="conservative",
        strategy_mode="capital_preserver",
        execution_mode="signals_only",
        live_mode=False,
        position_sizing_mode="percent",
        fixed_entry_tao=5.0,
        percent_of_free_tao=5.0,
        reserve_buffer_tao=40.0,
        max_position_tao=15.0,
        max_position_pct=20.0,
        exit_mode="partial",
        partial_exit_pct=50.0,
        portfolio_size=3,
        entry_score_min=0.84,
        watch_score_min=0.72,
        exit_score_max=0.50,
        max_entry_impact500=3.5,
        block_entry_if_impact1000_above=8.0,
        block_entry_if_reserve_below=4000.0,
        min_entry_taostats_score=0.50,
        max_new_entries_per_cycle=1,
        max_exits_per_cycle=2,
        trade_cooldown_seconds=7200,
        daily_max_tao=25.0,
        daily_max_loss_tao=10.0,
        max_trades_per_hour=1,
        min_hold_time_seconds=7200,
        explain_trades=True,
        send_discord_updates=True,
        default_validator_hotkey=DEFAULT_VALIDATOR_HOTKEY,
    )
def balanced_profile() -> Profile:
    return Profile(
        name="Balanced",
        style="balanced",
        strategy_mode="early_rotation_hunter",
        execution_mode="signals_only",
        live_mode=False,
        position_sizing_mode="percent",
        fixed_entry_tao=10.0,
        percent_of_free_tao=10.0,
        reserve_buffer_tao=20.0,
        max_position_tao=25.0,
        max_position_pct=25.0,
        exit_mode="partial",
        partial_exit_pct=50.0,
        portfolio_size=5,
        entry_score_min=0.80,
        watch_score_min=0.68,
        exit_score_max=0.42,
        max_entry_impact500=5.0,
        block_entry_if_impact1000_above=12.0,
        block_entry_if_reserve_below=2500.0,
        min_entry_taostats_score=0.45,
        max_new_entries_per_cycle=2,
        max_exits_per_cycle=2,
        trade_cooldown_seconds=3600,
        daily_max_tao=75.0,
        daily_max_loss_tao=20.0,
        max_trades_per_hour=2,
        min_hold_time_seconds=3600,
        explain_trades=True,
        send_discord_updates=True,
        default_validator_hotkey=DEFAULT_VALIDATOR_HOTKEY,
    )
def aggressive_profile() -> Profile:
    return Profile(
        name="Aggressive",
        style="aggressive",
        strategy_mode="momentum_follower",
        execution_mode="signals_only",
        live_mode=False,
        position_sizing_mode="percent",
        fixed_entry_tao=15.0,
        percent_of_free_tao=20.0,
        reserve_buffer_tao=10.0,
        max_position_tao=50.0,
        max_position_pct=35.0,
        exit_mode="full",
        partial_exit_pct=100.0,
        portfolio_size=8,
        entry_score_min=0.76,
        watch_score_min=0.64,
        exit_score_max=0.38,
        max_entry_impact500=6.0,
        block_entry_if_impact1000_above=15.0,
        block_entry_if_reserve_below=1500.0,
        min_entry_taostats_score=0.40,
        max_new_entries_per_cycle=3,
        max_exits_per_cycle=3,
        trade_cooldown_seconds=1800,
        daily_max_tao=150.0,
        daily_max_loss_tao=35.0,
        max_trades_per_hour=4,
        min_hold_time_seconds=1800,
        explain_trades=True,
        send_discord_updates=True,
        default_validator_hotkey=DEFAULT_VALIDATOR_HOTKEY,
    )
# ============================================================
# WALLET DISCOVERY / SELECTION
# ============================================================
def discover_wallets(wallet_dir: str) -> List[WalletSelection]:
    root = Path(wallet_dir)
    results: List[WalletSelection] = []
    if not root.exists() or not root.is_dir():
        return results
    for child in sorted(root.iterdir(), key=lambda p: p.name.lower()):
        if not child.is_dir() or child.name.startswith("."):
            continue
        hotkeys_dir = child / "hotkeys"
        hotkeys: List[str] = []
        if hotkeys_dir.exists() and hotkeys_dir.is_dir():
            for hk in sorted(hotkeys_dir.iterdir(), key=lambda p: p.name.lower()):
                if hk.is_file():
                    hotkeys.append(hk.name)
        if hotkeys:
            for hk in hotkeys:
                results.append(WalletSelection(wallet_name=child.name, wallet_path=str(root), hotkey_name=hk))
        else:
            results.append(WalletSelection(wallet_name=child.name, wallet_path=str(root), hotkey_name=""))
    return results
def print_discovered_wallets(discovered: List[WalletSelection], wallet_dir: str) -> None:
    if not discovered:
        print(f"[wallet] no wallets found in {wallet_dir}", flush=True)
        return
    print(f"[wallet] detected wallets in {wallet_dir}:", flush=True)
    for idx, w in enumerate(discovered, start=1):
        hk = w.hotkey_name if w.hotkey_name else "(no hotkey)"
        print(f"  {idx}. wallet={w.wallet_name} hotkey={hk}", flush=True)
def resolve_wallet_selection(conn: sqlite3.Connection) -> Optional[WalletSelection]:
    discovered = discover_wallets(BT_WALLET_PATH)
    print_discovered_wallets(discovered, BT_WALLET_PATH)
    if BT_WALLET_NAME:
        candidate = WalletSelection(wallet_name=BT_WALLET_NAME, wallet_path=BT_WALLET_PATH, hotkey_name=BT_HOTKEY_NAME)
        for item in discovered:
            if (
                item.wallet_name == candidate.wallet_name
                and item.wallet_path == candidate.wallet_path
                and (candidate.hotkey_name == "" or item.hotkey_name == candidate.hotkey_name or item.hotkey_name == "")
            ):
                print(f"[wallet] using env-selected wallet={candidate.wallet_name} hotkey={candidate.hotkey_name or '(no hotkey)'}", flush=True)
                save_wallet_selection(conn, candidate)
                return candidate
    saved = load_saved_wallet_selection(conn)
    if saved:
        if NONINTERACTIVE:
            print(f"[wallet] using saved wallet={saved.wallet_name} hotkey={saved.hotkey_name or '(no hotkey)'}", flush=True)
            return saved
        for item in discovered:
            if item.wallet_name == saved.wallet_name and item.wallet_path == saved.wallet_path and item.hotkey_name == saved.hotkey_name:
                if prompt_bool(f'Use saved wallet "{saved.wallet_name}" hotkey "{saved.hotkey_name or "(no hotkey)"}"?', True):
                    return saved
                break
    if not discovered:
        return None
    if len(discovered) == 1 or NONINTERACTIVE:
        choice = discovered[0]
        print(f"[wallet] auto-selected wallet={choice.wallet_name} hotkey={choice.hotkey_name or '(no hotkey)'}", flush=True)
        save_wallet_selection(conn, choice)
        return choice
    while True:
        raw = input("Select wallet number for signing trades (blank to stay signals-only): ").strip()
        if raw == "":
            return None
        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(discovered):
                choice = discovered[idx - 1]
                confirm = input(f'Use wallet "{choice.wallet_name}" hotkey "{choice.hotkey_name or "(no hotkey)"}"? [y/N]: ').strip().lower()
                if confirm == "y":
                    save_wallet_selection(conn, choice)
                    return choice
        print("Invalid selection.", flush=True)
def maybe_get_wallet(selection: Optional[WalletSelection], profile: Optional[Profile] = None) -> Optional[Any]:
    wallet_name = ""
    wallet_path = DEFAULT_WALLET_DIR
    hotkey_name = ""

    if selection is not None:
        wallet_name = (selection.wallet_name or "").strip()
        wallet_path = (selection.wallet_path or DEFAULT_WALLET_DIR).strip() or DEFAULT_WALLET_DIR
        hotkey_name = (selection.hotkey_name or "").strip()
    elif profile is not None:
        wallet_name = (profile.wallet_name or "").strip()
        wallet_path = (profile.wallet_path or DEFAULT_WALLET_DIR).strip() or DEFAULT_WALLET_DIR
        hotkey_name = (profile.hotkey_name or "").strip()
    else:
        wallet_name = (BT_WALLET_NAME or "").strip()
        wallet_path = (BT_WALLET_PATH or DEFAULT_WALLET_DIR).strip() or DEFAULT_WALLET_DIR
        hotkey_name = (BT_HOTKEY_NAME or "").strip()

    if not wallet_name:
        log("[wallet] no wallet name available for wallet construction")
        return None

    constructors: List[Any] = []
    if hasattr(bt, "wallet"):
        constructors.append(bt.wallet)
    if hasattr(bt, "Wallet"):
        constructors.append(bt.Wallet)

    attempts: List[Tuple[Any, Dict[str, Any]]] = []
    for ctor in constructors:
        if hotkey_name:
            attempts.append((ctor, {"name": wallet_name, "path": wallet_path, "hotkey": hotkey_name}))
            attempts.append((ctor, {"name": wallet_name, "path": wallet_path, "hotkey_name": hotkey_name}))
        attempts.append((ctor, {"name": wallet_name, "path": wallet_path}))
        attempts.append((ctor, {"path": wallet_path, "name": wallet_name}))

    for ctor, kwargs in attempts:
        try:
            wallet = ctor(**kwargs)
            if wallet is not None:
                log(
                    f"[wallet] initialized wallet name={wallet_name} "
                    f"path={wallet_path} hotkey={hotkey_name or '(none)'} "
                    f"via {getattr(ctor, '__name__', str(ctor))} kwargs={kwargs}"
                )
                return wallet
        except TypeError as exc:
            log(
                f"[wallet] constructor signature mismatch via "
                f"{getattr(ctor, '__name__', str(ctor))} kwargs={kwargs}: {exc}"
            )
        except Exception as exc:
            log(
                f"[wallet] wallet init failed via "
                f"{getattr(ctor, '__name__', str(ctor))} kwargs={kwargs}: {type(exc).__name__}: {exc}"
            )

    log(
        f"[wallet] failed to initialize wallet after all attempts "
        f"name={wallet_name} path={wallet_path} hotkey={hotkey_name or '(none)'}"
    )
    return None
def prompt_wallet_password_if_needed(profile: Profile, selection: Optional[WalletSelection]) -> Optional[str]:
    if selection is None or not profile.live_mode or profile.execution_mode == "signals_only":
        return None
    if NONINTERACTIVE:
        return ROTATION_WALLET_PASSWORD or None
    prompt = (
        f'Enter password for wallet "{selection.wallet_name}"'
        + (f' hotkey "{selection.hotkey_name}"' if selection.hotkey_name else "")
        + " (press Enter if no password; kept in memory only): "
    )
    try:
        pw = getpass.getpass(prompt)
    except Exception:
        return None
    return pw or None
# ============================================================
# SIMPLIFIED ONBOARDING
# ============================================================
def setup_secrets(conn: sqlite3.Connection) -> AppSecrets:
    saved = load_secrets(conn)
    use_saved = False
    if saved.taostats_api_key or saved.discord_webhook_url:
        use_saved = prompt_bool("Use saved TaoStats API key / Discord webhook?", True)
    if use_saved:
        return saved
    print("\n=== Secrets Setup ===", flush=True)
    tao_key = input("Enter TaoStats API key (blank to skip): ").strip()
    discord_hook = input("Enter Discord webhook URL (blank to skip): ").strip()
    secrets = AppSecrets(taostats_api_key=tao_key, discord_webhook_url=discord_hook)
    if prompt_bool("Save these for future runs?", True):
        save_secrets(conn, secrets)
    return secrets
def run_profile_wizard(conn: sqlite3.Connection, selection: Optional[WalletSelection]) -> Profile:
    existing = load_profile(conn)
    if existing and (NONINTERACTIVE or prompt_bool(f'Use saved profile "{existing.name}"?', True)):
        if selection:
            existing.wallet_name = selection.wallet_name
            existing.wallet_path = selection.wallet_path
            existing.hotkey_name = selection.hotkey_name
            save_profile(conn, existing)
        return existing
    print("\n=== Trading Setup ===", flush=True)
    execution_choice = prompt_choice(
        "Trading mode:",
        [
            "Signals only",
            "Confirm each live trade",
            "Fully automatic live trading",
        ],
        default_index=0,
    )
    style_choice = prompt_choice(
        "Trading style:",
        [
            "Conservative",
            "Balanced",
            "Aggressive",
            "Custom",
        ],
        default_index=1,
    )
    if style_choice == "Conservative":
        profile = conservative_profile()
    elif style_choice == "Aggressive":
        profile = aggressive_profile()
    else:
        profile = balanced_profile()
    if style_choice == "Custom":
        profile.name = input("Profile name [Custom]: ").strip() or "Custom"
    else:
        profile.name = style_choice
    if execution_choice == "Signals only":
        profile.execution_mode = "signals_only"
        profile.live_mode = False
    elif execution_choice == "Confirm each live trade":
        profile.execution_mode = "confirm"
        profile.live_mode = True
    else:
        profile.execution_mode = "auto"
        profile.live_mode = True
    profile.strategy_mode = prompt_choice(
        "Strategy mode:",
        ["early_rotation_hunter", "momentum_follower", "capital_preserver"],
        default_index={"Conservative": 2, "Balanced": 0, "Aggressive": 1, "Custom": 0}[style_choice],
    )
    print("\n=== Position Sizing ===", flush=True)
    profile.position_sizing_mode = prompt_choice(
        "Position sizing mode:",
        ["fixed", "percent"],
        default_index=1 if profile.position_sizing_mode == "percent" else 0,
    )
    if profile.position_sizing_mode == "fixed":
        profile.fixed_entry_tao = prompt_float("Fixed TAO per ENTRY", profile.fixed_entry_tao)
    else:
        profile.percent_of_free_tao = prompt_float("Percent of free TAO per ENTRY", profile.percent_of_free_tao)
    print("\n=== Safety Limits ===", flush=True)
    profile.reserve_buffer_tao = prompt_float("Minimum free TAO reserve buffer", profile.reserve_buffer_tao)
    profile.max_position_tao = prompt_float("Maximum TAO per subnet", profile.max_position_tao)
    profile.max_position_pct = prompt_float("Maximum % of deployable TAO per subnet", profile.max_position_pct)
    profile.daily_max_tao = prompt_float("Max TAO traded per day", profile.daily_max_tao)
    profile.daily_max_loss_tao = prompt_float("Max loss per day in TAO before pause", profile.daily_max_loss_tao)
    profile.max_trades_per_hour = prompt_int("Max trades per hour", profile.max_trades_per_hour)
    profile.exit_mode = prompt_choice("Exit mode:", ["full", "partial"], default_index=0 if profile.exit_mode == "full" else 1)
    if profile.exit_mode == "partial":
        profile.partial_exit_pct = prompt_float("Partial exit percent", profile.partial_exit_pct)
    profile.portfolio_size = prompt_int("Portfolio size", profile.portfolio_size)
    profile.explain_trades = prompt_bool("Explain trades?", profile.explain_trades)
    profile.send_discord_updates = prompt_bool("Send Discord updates?", profile.send_discord_updates)
    profile.default_validator_hotkey = (input(f"Default validator hotkey [{profile.default_validator_hotkey or DEFAULT_VALIDATOR_HOTKEY}]: ").strip() or profile.default_validator_hotkey or DEFAULT_VALIDATOR_HOTKEY)
    if selection:
        profile.wallet_name = selection.wallet_name
        profile.wallet_path = selection.wallet_path
        profile.hotkey_name = selection.hotkey_name
    print("\n=== Review ===", flush=True)
    print(f"Wallet: {profile.wallet_name or 'none selected'}", flush=True)
    print(f"Mode: {execution_choice}", flush=True)
    print(f"Style: {profile.name}", flush=True)
    if profile.position_sizing_mode == "fixed":
        print(f"Sizing: fixed {profile.fixed_entry_tao} TAO", flush=True)
    else:
        print(f"Sizing: {profile.percent_of_free_tao}% of free TAO", flush=True)
    print(f"Reserve buffer: {profile.reserve_buffer_tao} TAO", flush=True)
    print(f"Validator hotkey: {profile.default_validator_hotkey}", flush=True)
    print(f"Max per subnet: {profile.max_position_tao} TAO", flush=True)
    print(f"Daily max traded: {profile.daily_max_tao} TAO", flush=True)
    print(f"Default validator hotkey: {profile.default_validator_hotkey}", flush=True)
    if not prompt_bool("Start bot with these settings?", True):
        print("Aborted by user.", flush=True)
        sys.exit(0)
    save_profile(conn, profile)
    print(
        f"[startup] default_validator={DEFAULT_VALIDATOR_NAME} "
        f"hotkey={get_validator_hotkey(profile)}",
        flush=True,
    )
    return profile
# ============================================================
# BITTENSOR HELPERS
# ============================================================
def get_subtensor() -> Any:
    if hasattr(bt, "subtensor"):
        return bt.subtensor(network=NETWORK)
    if hasattr(bt, "Subtensor"):
        return bt.Subtensor(network=NETWORK)
    raise RuntimeError("Could not find a Subtensor constructor in installed bittensor package.")
def get_all_subnets(subtensor: Any) -> List[int]:
    method_names = ["get_all_subnet_netuids", "get_subnets", "all_subnets"]
    for name in method_names:
        if hasattr(subtensor, name):
            try:
                result = getattr(subtensor, name)()
                vals = sorted(set(int(x) for x in safe_list(result)))
                if vals:
                    return vals
            except Exception:
                pass
    return list(range(0, 129))
def get_selected_subnets(subtensor: Any) -> List[int]:
    if SUBNETS == "all":
        vals = get_all_subnets(subtensor)
    elif isinstance(SUBNETS, list):
        vals = sorted(set(int(x) for x in SUBNETS))
    else:
        raise ValueError("SUBNETS must be 'all' or a Python list of ints.")
    return [x for x in vals if is_tradeable_netuid(int(x))]
def get_metagraph(subtensor: Any, netuid: int, mechid: int) -> Any:
    if hasattr(subtensor, "metagraph"):
        try:
            return subtensor.metagraph(netuid=netuid, mechid=mechid, lite=False)
        except TypeError:
            try:
                return subtensor.metagraph(netuid=netuid, lite=False)
            except Exception:
                pass
        except Exception:
            pass
    try:
        from bittensor.core.metagraph import Metagraph  # type: ignore
        try:
            return Metagraph(netuid=netuid, network=NETWORK, sync=True, lite=False, mechid=mechid)
        except TypeError:
            return Metagraph(netuid=netuid, network=NETWORK, sync=True, lite=False)
    except Exception as exc:
        raise RuntimeError(f"Unable to fetch metagraph for netuid {netuid}: {exc}") from exc
def get_subnet_obj(subtensor: Any, netuid: int) -> Optional[Any]:
    for name in ("subnet", "get_subnet"):
        if hasattr(subtensor, name):
            try:
                return getattr(subtensor, name)(netuid=netuid)
            except Exception:
                continue
    return None
def get_subnet_name(metagraph: Any, subnet_obj: Any, netuid: int) -> str:
    for obj in (subnet_obj, metagraph):
        if obj is None:
            continue
        for field in ("name", "subnet_name"):
            if hasattr(obj, field):
                val = getattr(obj, field)
                if val:
                    return str(val)
    return f"Subnet {netuid}"
def get_hotkeys(metagraph: Any) -> List[str]:
    hotkeys = getattr(metagraph, "hotkeys", None)
    if hotkeys is None:
        return []
    return [str(x) for x in safe_list(hotkeys)]
def get_validator_permit(metagraph: Any) -> List[float]:
    return [safe_float(x) for x in safe_list(getattr(metagraph, "validator_permit", []))]
def get_weights_matrix(metagraph: Any) -> List[List[float]]:
    raw = getattr(metagraph, "weights", None)
    if raw is None:
        return []
    matrix = []
    for row in safe_list(raw):
        matrix.append([safe_float(x) for x in safe_list(row)])
    return matrix
def compute_validator_attention(
    metagraph: Any,
    prev_rows: Dict[int, Tuple[str, List[float]]],
) -> Tuple[float, int, Dict[int, Tuple[str, List[float]]], float]:
    hotkeys = get_hotkeys(metagraph)
    permits = get_validator_permit(metagraph)
    weights = get_weights_matrix(metagraph)
    current_rows: Dict[int, Tuple[str, List[float]]] = {}
    total_positive_weight_change = 0.0
    validators_increasing_weights = 0
    watchlist_score = 0.0

    count = min(len(hotkeys), len(permits), len(weights))
    for uid in range(count):
        if permits[uid] < 0.5:
            continue
        hotkey = hotkeys[uid]
        row = weights[uid]
        current_rows[uid] = (hotkey, row)
        prev = prev_rows.get(uid)
        if prev is None:
            continue
        prev_hotkey, prev_row = prev
        n = min(len(prev_row), len(row))
        row_positive_delta = 0.0
        for i in range(n):
            delta = row[i] - prev_row[i]
            if delta > 0:
                row_positive_delta += delta
        if row_positive_delta > 0:
            validators_increasing_weights += 1
            total_positive_weight_change += row_positive_delta
            boost = WATCH_VALIDATORS.get(hotkey) or WATCH_VALIDATORS.get(prev_hotkey) or 0.0
            watchlist_score += boost

    return total_positive_weight_change, validators_increasing_weights, current_rows, watchlist_score


def get_emissions_proxy(metagraph: Any, subnet_obj: Any) -> float:
    candidate_fields = [
        "emission", "emissions", "alpha_out_emission", "tao_in_emission",
        "subnet_emission", "tempo_emission",
    ]
    for obj in (subnet_obj, metagraph):
        if obj is None:
            continue
        for field in candidate_fields:
            if hasattr(obj, field):
                value = getattr(obj, field)
                if isinstance(value, (list, tuple)) or hasattr(value, "tolist"):
                    vals = [safe_float(x) for x in safe_list(value)]
                    total = sum(vals)
                    if total > 0:
                        return total
                else:
                    v = safe_float(value)
                    if v > 0:
                        return v
    return 1.0
def get_liquidity_proxy(subnet_obj: Any, metagraph: Any) -> float:
    candidate_fields = [
        "tao_in", "tao_reserve", "tao_pool", "reserve_tao", "tao_liquidity",
        "liquidity", "alpha_in", "alpha_reserve", "reserve_alpha", "moving_price",
    ]
    for obj in (subnet_obj, metagraph):
        if obj is None:
            continue
        for field in candidate_fields:
            if hasattr(obj, field):
                v = safe_float(getattr(obj, field))
                if v > 0:
                    return v
    if AGGRESSIVE_FIELD_FALLBACKS and subnet_obj is not None:
        for attr in dir(subnet_obj):
            if "reserve" in attr.lower() or "liquid" in attr.lower() or "pool" in attr.lower():
                try:
                    v = safe_float(getattr(subnet_obj, attr))
                    if v > 0:
                        return v
                except Exception:
                    continue
    return 1.0
def estimate_alpha_exit_impacts(subtensor: Any, netuid: int) -> Tuple[float, float, float]:
    subnet_obj = get_subnet_obj(subtensor, netuid)
    if subnet_obj is None:
        return (0.0, 0.0, 0.0)
    results = []
    for amount in ALPHA_EXIT_SIZES:
        pct = None
        try:
            alpha_amount = bt.Balance.from_tao(amount).set_unit(netuid)
            val = subnet_obj.alpha_to_tao_with_slippage(alpha_amount, percentage=True)
            pct = safe_float(val)
        except Exception:
            pass
        if pct is None:
            try:
                alpha_amount = bt.Balance.from_tao(amount).set_unit(netuid)
                val = subnet_obj.alpha_to_tao_with_slippage(alpha_amount)
                pct = safe_float(val)
            except Exception:
                pass
        results.append(max(0.0, pct if pct is not None else 0.0))
    while len(results) < 3:
        results.append(0.0)
    return (results[0], results[1], results[2])
def compute_live_flow_proxies(
    prev_snapshot: Optional[Tuple[float, float, float, float, float]],
    reserve_now: float,
    emission_now: float,
    impact100: float,
    impact500: float,
    impact1000: float,
) -> Tuple[float, float, float]:
    if prev_snapshot is None:
        return 0.0, 0.0, 0.0
    prev_reserve, prev_emission, _prev_i100, prev_i500, _prev_i1000 = prev_snapshot
    reserve_delta = reserve_now - prev_reserve
    tao_flow_proxy = reserve_delta
    impact_delta = abs(impact500 - prev_i500)
    emission_delta = abs(emission_now - prev_emission)
    trade_activity_proxy = abs(reserve_delta) + (impact_delta * max(reserve_now, 1.0) * 0.01) + emission_delta
    return reserve_delta, tao_flow_proxy, trade_activity_proxy
# ============================================================
# TAOSTATS HELPERS
# ============================================================
class TaoStatsClient:
    def __init__(self, api_key: str):
        self.api_key = api_key.strip().strip('"').strip("'")
        self.base = "https://api.taostats.io"
    def _get(self, path: str) -> Any:
        url = f"{self.base}{path}"
        headers = {
            "accept": "application/json",
            "user-agent": "rotation-bot-live-scan/5.0",
            "authorization": self.api_key,
        }
        log(f"[taostats] GET {url}")
        r = requests.get(url, headers=headers, timeout=(3.5, 8.0))
        log(f"[taostats] {r.status_code} {url}")
        if r.status_code == 401:
            body = (r.text or "")[:300]
            raise RuntimeError(f"401 unauthorized for {url} body={body}")
        r.raise_for_status()
        return r.json()
    def get_subnets_latest(self) -> Any:
        return self._get("/api/subnet/latest/v1")
    def get_dev_activity_latest(self) -> Any:
        return self._get("/api/dev_activity/latest/v1")
    def get_tao_flow(self) -> Any:
        try:
            return self._get("/api/dtao/tao_flow/v1")
        except Exception as exc:
            log(f"[taostats] tao flow unavailable: {type(exc).__name__}: {exc}")
            return []
def ensure_list(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("data", "results", "items", "subnets"):
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
        return [payload]
    return []
def index_by_netuid(payload: Any) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    for row in ensure_list(payload):
        raw_netuid = get_first(row, ["netuid", "subnet_id", "id", "uid"], None)
        if raw_netuid is None:
            continue
        try:
            out[int(raw_netuid)] = row
        except Exception:
            continue
    return out
def test_taostats_auth(secrets: AppSecrets) -> None:
    if not secrets.taostats_api_key:
        print("[taostats] disabled - no API key found", flush=True)
        return
    key_prefix = secrets.taostats_api_key[:12] if secrets.taostats_api_key else "missing"
    print(f"[taostats] auth test starting key_prefix={key_prefix} key_len={len(secrets.taostats_api_key)}", flush=True)
    try:
        client = TaoStatsClient(secrets.taostats_api_key)
        payload = client.get_subnets_latest()
        row_count = len(ensure_list(payload))
        print(f"[taostats] auth OK subnet_rows={row_count}", flush=True)
    except Exception as exc:
        print(f"[taostats] auth FAILED: {type(exc).__name__}: {exc}", flush=True)
def fetch_taostats_indices(secrets: AppSecrets) -> Tuple[Dict[int, Dict[str, Any]], Dict[int, Dict[str, Any]], Dict[int, Dict[str, Any]]]:
    if not secrets.taostats_api_key:
        return {}, {}, {}
    client = TaoStatsClient(secrets.taostats_api_key)
    try:
        log("[taostats] fetching subnet snapshot...")
        subnets = index_by_netuid(client.get_subnets_latest())
    except Exception as exc:
        log(f"[taostats] subnet fetch failed: {type(exc).__name__}: {exc}")
        subnets = {}
    try:
        log("[taostats] fetching dev activity snapshot...")
        github = index_by_netuid(client.get_dev_activity_latest())
    except Exception as exc:
        log(f"[taostats] dev activity fetch failed: {type(exc).__name__}: {exc}")
        github = {}
    try:
        log("[taostats] fetching tao flow snapshot...")
        flow = index_by_netuid(client.get_tao_flow())
    except Exception as exc:
        log(f"[taostats] tao flow fetch failed: {type(exc).__name__}: {exc}")
        flow = {}
    return subnets, github, flow
def taostats_confirmation_for_signal(
    sig: Signal,
    subnet_row: Optional[Dict[str, Any]],
    github_row: Optional[Dict[str, Any]],
    flow_row: Optional[Dict[str, Any]],
) -> Tuple[float, str]:
    subnet_row = subnet_row or {}
    github_row = github_row or {}
    flow_row = flow_row or {}
    liquidity = safe_float(get_first(subnet_row, ["liquidity", "reserve", "tao_reserve", "alpha_reserve", "market_cap", "mcap"], 0.0))
    emission = safe_float(get_first(subnet_row, ["emission", "tao_emission", "alpha_emission", "emissions", "daily_emission"], 0.0))
    price = safe_float(get_first(subnet_row, ["price", "alpha_price", "last_price", "spot_price"], 0.0))
    registered = get_first(subnet_row, ["is_registered", "registered", "active", "is_active"], True)
    if isinstance(registered, str):
        registered = registered.lower() in {"1", "true", "yes", "active"}
    github_activity = safe_float(get_first(github_row, ["activity_score", "score", "events_30d", "events", "commit_count", "commits_30d"], 0.0))
    ts_flow = safe_float(get_first(flow_row, ["net_flow", "flow", "tao_flow", "value", "net_tao_flow"], 0.0))
    s_liquidity = scale_positive(liquidity, softness=5000.0)
    s_flow = logistic(ts_flow, center=0.0, steepness=0.02)
    s_github = scale_positive(github_activity, softness=10.0)
    s_price = scale_positive(price, softness=1.0)
    s_emission = scale_positive(emission, softness=1.0)
    s_registered = 1.0 if registered else 0.0
    score = clamp01(
        W_TS_LIQUIDITY * s_liquidity
        + W_TS_FLOW * s_flow
        + W_TS_GITHUB * s_github
        + W_TS_PRICE * s_price
        + W_TS_EMISSION * s_emission
        + W_TS_REGISTERED * s_registered
    )
    notes = (
        f"liq={liquidity:.2f} "
        f"flow={ts_flow:+.2f} "
        f"github={github_activity:.2f} "
        f"price={price:.4f} "
        f"emission={emission:.4f} "
        f"registered={registered}"
    )
    return score, notes
def apply_taostats_confirmation(signals: List[Signal], secrets: AppSecrets) -> None:
    if not secrets.taostats_api_key or not signals:
        return
    confirm_count = min(TOP_LOCAL_CONFIRM, len(signals))
    log(f"[taostats] confirming top {confirm_count} local candidates")
    subnet_idx, github_idx, flow_idx = fetch_taostats_indices(secrets)
    if not subnet_idx and not github_idx and not flow_idx:
        log("[taostats] no data returned, keeping local scores only")
        return
    for sig in signals[:confirm_count]:
        try:
            ts_score, notes = taostats_confirmation_for_signal(sig, subnet_idx.get(sig.netuid), github_idx.get(sig.netuid), flow_idx.get(sig.netuid))
            old_score = sig.score
            sig.taostats_score = ts_score
            sig.taostats_notes = notes
            sig.score = clamp01((LOCAL_SCORE_WEIGHT * sig.local_score) + (TAOSTATS_SCORE_WEIGHT * ts_score))
            sig.classification = score_to_classification(sig.score, sig.validator_cluster, sig.validator_delta, sig.tao_flow_proxy)
            sig.confidence = score_to_confidence(sig.score)
            log(f"[taostats] SN{sig.netuid} local={old_score:.4f} ts={ts_score:.4f} final={sig.score:.4f} | {notes}")
        except Exception as exc:
            log(f"[taostats] SN{sig.netuid} confirmation failed: {type(exc).__name__}: {exc}")
    signals.sort(key=lambda s: s.score, reverse=True)
# ============================================================
# EXPLAINABILITY / ACTIONS
# ============================================================
def annotate_rank_deltas(signals: List[Signal], prev_rankings: Dict[int, Dict[str, Any]]) -> None:
    for idx, sig in enumerate(signals, start=1):
        sig.rank = idx
        prev = prev_rankings.get(sig.netuid)
        if prev:
            try:
                sig.previous_rank = int(prev.get("rank"))
            except Exception:
                sig.previous_rank = None
            try:
                sig.previous_score = float(prev.get("score"))
            except Exception:
                sig.previous_score = None
            if sig.previous_rank is not None:
                sig.rank_delta = sig.previous_rank - sig.rank
            if sig.previous_score is not None:
                sig.score_delta = sig.score - sig.previous_score
def profile_adjustments_from_bias(profile: Profile, bias: Dict[str, Any]) -> Profile:
    p = Profile(**asdict(profile))
    if bias.get("avoid_high_impact", 0) >= 2:
        p.max_entry_impact500 = min(p.max_entry_impact500, 4.0)
    if bias.get("prefer_conservative", 0) >= 2:
        p.percent_of_free_tao = min(p.percent_of_free_tao, 8.0)
        p.max_position_pct = min(p.max_position_pct, 20.0)
    return p
def is_entry_eligible(sig: Signal, profile: Profile) -> bool:
    if sig.classification != "BUY":
        return False
    if not sig.entry_ready:
        return False
    if sig.score < profile.entry_score_min:
        return False
    if sig.trigger_score < TRIGGER_SCORE_MIN:
        return False
    if sig.setup_score < SETUP_SCORE_MIN:
        return False
    if sig.decay_score > ENTRY_DECAY_MAX:
        return False
    if sig.impact500 > profile.max_entry_impact500:
        return False
    if sig.impact1000 > profile.block_entry_if_impact1000_above:
        return False
    if sig.reserve_now < profile.block_entry_if_reserve_below:
        return False
    if sig.taostats_score > 0 and sig.taostats_score < profile.min_entry_taostats_score:
        return False
    return True


def is_watch_candidate(sig: Signal, profile: Profile) -> bool:
    return (
        sig.rank <= profile.portfolio_size + 5
        and (
            sig.watch_stage > 0
            or sig.setup_score >= SETUP_SCORE_MIN
            or sig.trigger_score >= TRIGGER_SCORE_MIN * 0.9
            or sig.score >= profile.watch_score_min
        )
    )


def should_force_exit(sig: Signal, profile: Profile) -> bool:
    if sig.decay_score >= HARD_EXIT_DECAY:
        return True
    if sig.score <= profile.exit_score_max:
        return True
    if sig.impact500 >= max(8.0, profile.max_entry_impact500 * 1.5) and sig.trigger_score < TRIGGER_SCORE_MIN:
        return True
    if sig.validator_cluster == 0 and sig.validator_delta <= 0 and sig.breadth_persistence < 0.34:
        return True
    if sig.reserve_delta < 0 and sig.flow_persistence < 0.34:
        return True
    return False


def derive_target_portfolio(signals: List[Signal], prev_target: List[int], profile: Profile) -> List[int]:
    ranked = sorted(signals, key=lambda s: (s.score, s.trigger_score, s.setup_score), reverse=True)
    target: List[int] = []

    for sig in ranked:
        if is_entry_eligible(sig, profile):
            target.append(sig.netuid)
        if len(target) >= profile.portfolio_size:
            break

    if len(target) < profile.portfolio_size:
        prev_lookup = {s.netuid: s for s in ranked}
        for netuid in prev_target:
            sig = prev_lookup.get(netuid)
            if sig is None or netuid in target:
                continue
            if should_force_exit(sig, profile):
                continue
            if sig.decay_score < SOFT_EXIT_DECAY and (sig.trigger_score >= 0.45 or sig.setup_score >= 0.45):
                target.append(netuid)
            if len(target) >= profile.portfolio_size:
                break

    if len(target) < profile.portfolio_size:
        for sig in ranked:
            if sig.netuid in target:
                continue
            if is_watch_candidate(sig, profile) and sig.decay_score < SOFT_EXIT_DECAY:
                target.append(sig.netuid)
            if len(target) >= profile.portfolio_size:
                break

    return target[: profile.portfolio_size]


def annotate_portfolio_actions(
    signals: List[Signal],
    prev_target: List[int],
    current_target: List[int],
    profile: Profile,
) -> Tuple[List[Signal], List[Signal], List[Signal], List[Signal]]:
    prev_set = set(prev_target)
    curr_set = set(current_target)
    entries: List[Signal] = []
    holds: List[Signal] = []
    exits: List[Signal] = []
    watch: List[Signal] = []

    by_netuid = {s.netuid: s for s in signals}
    for sig in signals:
        if sig.netuid in curr_set:
            if sig.netuid in prev_set:
                sig.action = "HOLD"
                holds.append(sig)
            else:
                sig.action = "ENTRY"
                entries.append(sig)
        else:
            if sig.netuid in prev_set or sig.decay_score >= SOFT_EXIT_DECAY:
                sig.action = "EXIT"
                exits.append(sig)
            else:
                sig.action = "WATCH" if is_watch_candidate(sig, profile) else "AVOID"
                if sig.action == "WATCH":
                    watch.append(sig)

    for netuid in prev_target:
        if netuid not in curr_set and netuid not in by_netuid:
            exits.append(
                Signal(
                    netuid=netuid,
                    name=f"Subnet {netuid}",
                    classification="EXIT",
                    score=0.0,
                    confidence="F",
                    validator_delta=0.0,
                    validator_cluster=0,
                    eli=0.0,
                    watchlist_score=0.0,
                    regime="UNKNOWN",
                    impact100=0.0,
                    impact500=0.0,
                    impact1000=0.0,
                    liquidity_risk="Unknown",
                    suggested_size="Reduce / Exit",
                    exit_signal=True,
                    reserve_now=0.0,
                    reserve_delta=0.0,
                    tao_flow_proxy=0.0,
                    trade_activity_proxy=0.0,
                    updated_at=utc_now_iso(),
                    detail="missing from latest scan",
                    action="EXIT",
                    lifecycle="EXIT CANDIDATE",
                )
            )
    return entries, holds, exits, watch


def explain_signal(sig: Signal) -> str:
    reasons: List[str] = []
    if sig.validator_delta > 0:
        reasons.append(f"validator weight increases {sig.validator_delta:+.4f}")
    if sig.validator_cluster > 0:
        reasons.append(f"{sig.validator_cluster} validators increased subnet weights")
    if sig.setup_score > 0:
        reasons.append(f"setup {sig.setup_score:.2f}")
    if sig.trigger_score > 0:
        reasons.append(f"trigger {sig.trigger_score:.2f}")
    if sig.breadth_persistence > 0:
        reasons.append(f"breadth persistence {sig.breadth_persistence:.2f}")
    if sig.tao_flow_proxy > 0:
        reasons.append(f"positive flow {sig.tao_flow_proxy:+.2f}")
    if sig.reserve_delta > 0:
        reasons.append(f"reserve growing {sig.reserve_delta:+.2f}")
    if sig.decay_score > 0:
        reasons.append(f"decay {sig.decay_score:.2f}")
    if sig.taostats_score > 0:
        reasons.append(f"TaoStats confirmation {sig.taostats_score:.2f}")
    if not reasons:
        reasons.append("relative ranking improvement")
    return "; ".join(reasons)




def btcli_wallet_balance_tao(wallet_name: str, wallet_path: str) -> Optional[float]:
    wallet_name = (wallet_name or "").strip()
    wallet_path = (wallet_path or DEFAULT_WALLET_DIR).strip() or DEFAULT_WALLET_DIR
    if not wallet_name:
        return None
    candidates = ["btcli", os.path.expanduser("~/.local/bin/btcli"), "/usr/local/bin/btcli"]
    import json as _json
    import re as _re
    for cmd in candidates:
        json_cmds = [
            [cmd, "w", "balance", "--wallet-name", wallet_name, "--wallet-path", wallet_path, "--json-output"],
            [cmd, "wallet", "balance", "--wallet-name", wallet_name, "--wallet-path", wallet_path, "--json-output"],
            [cmd, "w", "balance", "--all", "--json-output"],
            [cmd, "wallet", "balance", "--all", "--json-output"],
        ]
        for argv in json_cmds:
            try:
                proc = subprocess.run(argv, capture_output=True, text=True, timeout=30)
                if proc.returncode != 0 or not proc.stdout.strip():
                    continue
                data = _json.loads(proc.stdout)
                if isinstance(data, dict):
                    balances = data.get("balances")
                    if isinstance(balances, dict):
                        if wallet_name in balances and isinstance(balances[wallet_name], dict):
                            free = safe_float(balances[wallet_name].get("free"), None)
                            if free is not None:
                                return free
                        for _, info in balances.items():
                            if not isinstance(info, dict):
                                continue
                            free = safe_float(info.get("free"), None)
                            if free is not None:
                                return free
                    free = safe_float(data.get("free"), None)
                    if free is not None:
                        return free
            except Exception:
                pass
        text_cmds = [
            [cmd, "w", "balance", "--all"],
            [cmd, "wallet", "balance", "--all"],
        ]
        for argv in text_cmds:
            try:
                proc = subprocess.run(argv, capture_output=True, text=True, timeout=30)
                if proc.returncode != 0 or not proc.stdout.strip():
                    continue
                for line in proc.stdout.splitlines():
                    if wallet_name not in line:
                        continue
                    cleaned = (line.replace("τ", " ").replace("‎", " ").replace("‏", " ")
                               .replace("┃", " ").replace("│", " "))
                    m = _re.search(rf"\b{_re.escape(wallet_name)}\b.*?([-+]?\d+(?:\.\d+)?)\s+([-+]?\d+(?:\.\d+)?)\s+([-+]?\d+(?:\.\d+)?)", cleaned)
                    if m:
                        return safe_float(m.group(1), None)
            except Exception:
                pass
    return None


def _balance_like_to_tao(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    for attr in ("tao", "amount", "value"):
        parsed = safe_float(getattr(value, attr, None), None)
        if parsed is not None:
            return parsed
    if isinstance(value, dict):
        for key in ("tao", "stake", "amount", "value", "alpha"):
            parsed = safe_float(value.get(key), None)
            if parsed is not None:
                return parsed
    parsed = safe_float(str(value).replace("τ", " ").replace("α", " ").strip(), None)
    return parsed


def _wallet_coldkey_ss58(wallet: Optional[Any]) -> Optional[str]:
    if wallet is None:
        return None
    for attr in ("coldkeypub", "coldkeypub_file", "coldkey"):
        maybe = getattr(wallet, attr, None)
        if maybe is None:
            continue
        for addr_attr in ("ss58_address", "ss58", "address"):
            val = getattr(maybe, addr_attr, None)
            if val:
                return str(val)
    return None


def _wallet_hotkey_ss58(wallet: Optional[Any]) -> Optional[str]:
    # Do not touch wallet.hotkey here.
    # Delegation/removal only needs the coldkey wallet plus the validator hotkey,
    # and accessing wallet.hotkey can force a local hotkey keyfile lookup that
    # breaks coldkey-only staking wallets.
    return None


def _object_field(obj: Any, *names: str) -> Any:
    if obj is None:
        return None
    if isinstance(obj, dict):
        for name in names:
            if name in obj:
                return obj.get(name)
        return None
    for name in names:
        if hasattr(obj, name):
            return getattr(obj, name)
    return None


def _match_stake_record(item: Any, hotkey_ss58: str, netuid: int) -> bool:
    item_hotkey = _object_field(item, "hotkey_ss58", "hotkey", "hotkey_ss58_address", "delegate_hotkey")
    item_netuid = _object_field(item, "netuid", "subnet", "subnet_id")
    if item_hotkey and str(item_hotkey) != str(hotkey_ss58):
        return False
    if item_netuid is not None:
        try:
            if int(item_netuid) != int(netuid):
                return False
        except Exception:
            return False
    return True


def _stake_item_value(item: Any) -> Optional[float]:
    for key in ("stake", "amount", "tao", "value", "alpha"):
        parsed = _balance_like_to_tao(_object_field(item, key))
        if parsed is not None:
            return parsed
    return _balance_like_to_tao(item)


def get_wallet_stake_tao_on_subnet(subtensor: Any, wallet: Optional[Any], hotkey_ss58: str, netuid: int) -> Optional[float]:
    coldkey_ss58 = _wallet_coldkey_ss58(wallet)
    if not coldkey_ss58:
        return None
    attempts: List[Tuple[str, Dict[str, Any]]] = []
    attempts.extend([
        ("get_stake_for_coldkey_and_hotkey_on_subnet", {"coldkey_ss58": coldkey_ss58, "hotkey_ss58": hotkey_ss58, "netuid": netuid}),
        ("get_stake_for_coldkey_and_hotkey", {"coldkey_ss58": coldkey_ss58, "hotkey_ss58": hotkey_ss58, "netuid": netuid}),
        ("get_stake", {"coldkey_ss58": coldkey_ss58, "hotkey_ss58": hotkey_ss58, "netuid": netuid}),
        ("get_stake_for_coldkey", {"coldkey_ss58": coldkey_ss58, "netuid": netuid}),
        ("get_stake_info_for_coldkey", {"coldkey_ss58": coldkey_ss58}),
        ("get_stake_info_for_coldkeys", {"coldkey_ss58s": [coldkey_ss58]}),
    ])
    for name, kwargs in attempts:
        fn = getattr(subtensor, name, None)
        if not callable(fn):
            continue
        try:
            result = fn(**kwargs)
        except TypeError:
            continue
        except Exception:
            continue
        if isinstance(result, (list, tuple)):
            matched_values = []
            for item in result:
                if _match_stake_record(item, hotkey_ss58, netuid):
                    value = _stake_item_value(item)
                    if value is not None:
                        matched_values.append(value)
            if matched_values:
                return float(sum(matched_values))
            continue
        if isinstance(result, dict) and any(k in result for k in ("stakes", "items", "data")):
            for container_key in ("stakes", "items", "data"):
                container = result.get(container_key)
                if not isinstance(container, (list, tuple)):
                    continue
                matched_values = []
                for item in container:
                    if _match_stake_record(item, hotkey_ss58, netuid):
                        value = _stake_item_value(item)
                        if value is not None:
                            matched_values.append(value)
                if matched_values:
                    return float(sum(matched_values))
            continue
        parsed = _stake_item_value(result)
        if parsed is not None:
            return parsed
    return None


def capture_chain_execution_snapshot(subtensor: Any, wallet: Optional[Any], hotkey_ss58: str, netuid: int) -> Dict[str, Any]:
    alpha_held = get_wallet_stake_tao_on_subnet(subtensor, wallet, hotkey_ss58, netuid)
    return {
        "captured_at": utc_now_iso(),
        "wallet_free_tao": best_effort_wallet_balance_tao(wallet, subtensor),
        "stake_tao": alpha_held,
        "alpha_held": alpha_held,
        "coldkey_ss58": _wallet_coldkey_ss58(wallet),
        "delegate_hotkey": hotkey_ss58,
        "netuid": netuid,
    }


def parse_extrinsic_success(result: Any) -> Tuple[Optional[bool], Optional[str]]:
    if result is None:
        return None, None
    for attr in ("success", "is_success", "ok"):
        val = getattr(result, attr, None)
        if isinstance(val, bool):
            msg = getattr(result, "message", None) or getattr(result, "error_message", None)
            return val, None if msg is None else str(msg)
    text = str(result)
    lower = text.lower()
    if "success: false" in lower:
        return False, text
    if "success: true" in lower:
        return True, text
    if "extrinsicresponse" in lower and "failed" in lower:
        return False, text
    return None, text


def reconcile_execution_with_chain(
    subtensor: Any,
    wallet: Optional[Any],
    hotkey_ss58: str,
    netuid: int,
    side: str,
    requested_amount_tao: float,
    before: Optional[Dict[str, Any]],
    chain_success: bool,
    max_attempts: int = 4,
    sleep_seconds: float = 1.25,
) -> Dict[str, Any]:
    before = before or capture_chain_execution_snapshot(subtensor, wallet, hotkey_ss58, netuid)
    after = before
    expected_dir = 1.0 if side.upper() == "ENTRY" else -1.0
    observed = False
    for attempt in range(max_attempts):
        if attempt > 0:
            time.sleep(sleep_seconds)
        after = capture_chain_execution_snapshot(subtensor, wallet, hotkey_ss58, netuid)
        stake_before = safe_float(before.get("stake_tao"), None)
        stake_after = safe_float(after.get("stake_tao"), None)
        free_before = safe_float(before.get("wallet_free_tao"), None)
        free_after = safe_float(after.get("wallet_free_tao"), None)
        stake_delta = None if stake_before is None or stake_after is None else round(stake_after - stake_before, 12)
        wallet_delta = None if free_before is None or free_after is None else round(free_after - free_before, 12)
        if side.upper() == "ENTRY":
            observed = (stake_delta is not None and stake_delta > 1e-9) or (wallet_delta is not None and wallet_delta < -1e-9)
        else:
            observed = (stake_delta is not None and stake_delta < -1e-9) or (wallet_delta is not None and wallet_delta > 1e-9)
        if observed or not chain_success:
            break
    stake_before = safe_float(before.get("stake_tao"), None)
    stake_after = safe_float(after.get("stake_tao"), None)
    free_before = safe_float(before.get("wallet_free_tao"), None)
    free_after = safe_float(after.get("wallet_free_tao"), None)
    stake_delta = None if stake_before is None or stake_after is None else round(stake_after - stake_before, 12)
    wallet_delta = None if free_before is None or free_after is None else round(free_after - free_before, 12)
    actual_wallet_debit = None if wallet_delta is None else round(max(0.0, -wallet_delta), 12)
    actual_wallet_credit = None if wallet_delta is None else round(max(0.0, wallet_delta), 12)
    actual_stake_delta_abs = None if stake_delta is None else round(abs(stake_delta), 12)
    fee_estimate = None
    if side.upper() == "ENTRY" and actual_wallet_debit is not None and actual_stake_delta_abs is not None:
        fee_estimate = round(max(0.0, actual_wallet_debit - actual_stake_delta_abs), 12)
    elif side.upper() == "EXIT" and actual_stake_delta_abs is not None and actual_wallet_credit is not None:
        fee_estimate = round(max(0.0, actual_stake_delta_abs - actual_wallet_credit), 12)
    return {
        "before": before,
        "after": after,
        "wallet_delta_tao": wallet_delta,
        "stake_delta_tao": stake_delta,
        "actual_wallet_debit_tao": actual_wallet_debit,
        "actual_wallet_credit_tao": actual_wallet_credit,
        "actual_stake_delta_tao": actual_stake_delta_abs,
        "fee_estimate_tao": fee_estimate,
        "observed_chain_change": observed,
        "chain_success": chain_success,
        "expected_side": side.upper(),
        "requested_amount_tao": requested_amount_tao,
    }


def load_wallet_balance_cache(conn: sqlite3.Connection) -> Dict[str, Any]:
    raw = get_json_meta(conn, "wallet_balance_cache", {})
    return raw if isinstance(raw, dict) else {}

def save_wallet_balance_cache(conn: sqlite3.Connection, balance_tao: Optional[float]) -> None:
    if balance_tao is None:
        return
    save_json_meta(conn, "wallet_balance_cache", {"balance_tao": float(balance_tao), "ts": now_ts()})

def best_effort_wallet_balance_tao_cached(conn: sqlite3.Connection, wallet: Optional[Any], subtensor: Any, max_age_seconds: int = 300) -> Optional[float]:
    balance = best_effort_wallet_balance_tao(wallet, subtensor)
    if balance is not None:
        save_wallet_balance_cache(conn, balance)
        return balance
    cache = load_wallet_balance_cache(conn)
    ts = int(cache.get("ts", 0) or 0)
    if ts and (now_ts() - ts) <= max_age_seconds:
        return safe_float(cache.get("balance_tao"), None)
    return None

def best_effort_wallet_balance_tao(wallet: Optional[Any], subtensor: Any) -> Optional[float]:
    if wallet is None:
        return btcli_wallet_balance_tao(BT_WALLET_NAME, BT_WALLET_PATH)
    candidates: List[Tuple[Any, str]] = [
        (wallet, "get_balance"),
        (wallet, "balance"),
        (subtensor, "get_balance"),
        (subtensor, "balance"),
    ]
    for obj, name in candidates:
        fn = getattr(obj, name, None)
        if callable(fn):
            try:
                if obj is wallet:
                    result = fn()
                else:
                    ss58 = None
                    for attr in ("coldkeypub", "coldkeypub_file", "coldkey"):
                        maybe = getattr(wallet, attr, None)
                        if maybe is None:
                            continue
                        ss58 = getattr(maybe, "ss58_address", None) or getattr(maybe, "ss58", None) or getattr(maybe, "address", None)
                        if ss58:
                            break
                    if ss58:
                        result = fn(ss58)
                    else:
                        continue
                parsed = safe_float(getattr(result, "tao", result), None)
                if parsed is not None:
                    return parsed
            except Exception:
                continue
    wallet_name = getattr(wallet, "name", None) or BT_WALLET_NAME
    wallet_path = getattr(wallet, "path", None) or BT_WALLET_PATH
    return btcli_wallet_balance_tao(str(wallet_name or ""), str(wallet_path or DEFAULT_WALLET_DIR))
def update_position_state_on_entry(conn: sqlite3.Connection, decision: TradeDecision, result: Optional[Dict[str, Any]] = None) -> None:
    state = load_position_state(conn)
    key = str(decision.netuid)
    cur = state.get(key, {})
    reconciliation = result.get("reconciliation", {}) if isinstance(result, dict) else {}
    actual_spend = safe_float(reconciliation.get("actual_wallet_debit_tao"), decision.amount_tao)
    actual_alpha = safe_float(reconciliation.get("actual_stake_delta_tao"), 0.0)
    actual_spend = max(0.0, actual_spend)
    actual_alpha = max(0.0, actual_alpha)
    prev_basis = safe_float(cur.get("tao_basis"), safe_float(cur.get("amount_tao"), 0.0))
    prev_alpha = safe_float(cur.get("alpha_held"), safe_float(cur.get("staked_tao"), 0.0))
    new_basis = prev_basis + actual_spend
    new_alpha = prev_alpha + actual_alpha
    blended_entry_mark = (new_basis / max(new_alpha, 1e-9)) if new_alpha > 0 else max(safe_float(cur.get("entry_mark"), 0.0), 1e-9)
    current_mark = latest_mark_value_for_netuid(conn, decision.netuid, fallback=blended_entry_mark if blended_entry_mark > 0 else 1.0)
    cur["amount_tao"] = round(new_basis, 6)
    cur["tao_basis"] = round(new_basis, 6)
    cur["alpha_held"] = round(new_alpha, 6)
    cur["staked_tao"] = round(new_alpha, 6)
    cur["entry_mark"] = blended_entry_mark
    cur["last_mark"] = current_mark
    cur["entered_at"] = int(cur.get("entered_at", now_ts()))
    cur["name"] = decision.name
    cur["realized_pnl_tao"] = safe_float(cur.get("realized_pnl_tao"), 0.0)
    cur["last_reconciled_at"] = now_ts()
    state[key] = cur
    save_position_state(conn, state)
    est_value = round(new_alpha * current_mark, 6)
    detail = {
        "reason": decision.reason,
        "requested_amount_tao": decision.amount_tao,
        "actual_wallet_debit_tao": actual_spend,
        "actual_alpha_bought": actual_alpha,
        "fee_estimate_tao": safe_float(reconciliation.get("fee_estimate_tao"), 0.0),
    }
    insert_pnl_journal(
        conn,
        decision.netuid,
        decision.name,
        "ENTRY",
        actual_spend,
        actual_spend,
        est_value,
        0.0,
        new_basis,
        est_value,
        json.dumps(detail, default=str),
    )


def update_position_state_on_exit(conn: sqlite3.Connection, decision: TradeDecision, full_exit: bool, result: Optional[Dict[str, Any]] = None) -> None:
    state = load_position_state(conn)
    key = str(decision.netuid)
    cur = state.get(key)
    if not cur:
        return
    reconciliation = result.get("reconciliation", {}) if isinstance(result, dict) else {}
    prev_basis = safe_float(cur.get("tao_basis"), safe_float(cur.get("amount_tao"), 0.0))
    prev_alpha = safe_float(cur.get("alpha_held"), safe_float(cur.get("staked_tao"), 0.0))
    if prev_basis <= 0 and prev_alpha <= 0:
        state.pop(key, None)
        save_position_state(conn, state)
        return
    actual_credit = safe_float(reconciliation.get("actual_wallet_credit_tao"), 0.0)
    actual_alpha_sold = safe_float(reconciliation.get("actual_stake_delta_tao"), decision.amount_tao)
    actual_credit = max(0.0, actual_credit)
    actual_alpha_sold = max(0.0, actual_alpha_sold)
    ratio = 1.0 if full_exit else clamp01(actual_alpha_sold / max(prev_alpha, 1e-9))
    basis_removed = prev_basis if ratio >= 0.999999 else round(prev_basis * ratio, 12)
    realized = round(actual_credit - basis_removed, 6)
    remaining_basis = max(0.0, prev_basis - basis_removed)
    remaining_alpha = 0.0 if full_exit else max(0.0, prev_alpha - actual_alpha_sold)
    current_mark = latest_mark_value_for_netuid(conn, decision.netuid, fallback=max(safe_float(cur.get("entry_mark"), 0.0), 1e-9))
    cur["realized_pnl_tao"] = round(safe_float(cur.get("realized_pnl_tao"), 0.0) + realized, 6)
    cur["last_mark"] = current_mark
    cur["last_reconciled_at"] = now_ts()
    if remaining_basis <= 1e-9 or remaining_alpha <= 1e-9:
        state.pop(key, None)
    else:
        cur["amount_tao"] = round(remaining_basis, 6)
        cur["tao_basis"] = round(remaining_basis, 6)
        cur["alpha_held"] = round(remaining_alpha, 6)
        cur["staked_tao"] = round(remaining_alpha, 6)
        state[key] = cur
    save_position_state(conn, state)
    add_realized_pnl_tao(conn, realized)
    remaining_est_value = round(remaining_alpha * current_mark, 6)
    detail = {
        "reason": decision.reason,
        "requested_alpha_amount": decision.amount_tao,
        "actual_wallet_credit_tao": actual_credit,
        "actual_alpha_sold": actual_alpha_sold,
        "fee_estimate_tao": safe_float(reconciliation.get("fee_estimate_tao"), 0.0),
    }
    insert_pnl_journal(
        conn,
        decision.netuid,
        decision.name,
        "EXIT_FULL" if remaining_basis <= 1e-9 or remaining_alpha <= 1e-9 else "EXIT_PARTIAL",
        actual_alpha_sold if actual_alpha_sold > 0 else decision.amount_tao,
        basis_removed,
        actual_credit,
        realized,
        remaining_basis,
        remaining_est_value,
        json.dumps(detail, default=str),
    )


def current_positions_total_tao(conn: sqlite3.Connection) -> float:
    state = load_position_state(conn)
    total = 0.0
    for netuid_str, row in state.items():
        try:
            netuid = int(netuid_str)
        except Exception:
            netuid = safe_int((row or {}).get("netuid"), 0)
        total += estimate_position_value_from_row(conn, netuid, row)
    return round(total, 6)
def position_amount_for_netuid(conn: sqlite3.Connection, netuid: int) -> float:
    state = load_position_state(conn)
    row = state.get(str(netuid), {})
    return safe_float(row.get("alpha_held"), safe_float(row.get("staked_tao"), 0.0))

def has_open_position(conn: sqlite3.Connection, netuid: int) -> bool:
    return position_amount_for_netuid(conn, netuid) > 0
# ============================================================
# SIGNAL ENGINE
# ============================================================
def build_signals(
    subtensor: Any,
    conn: sqlite3.Connection,
    profile: Profile,
    secrets: AppSecrets,
) -> Tuple[List[Signal], List[Signal], List[Signal], List[Signal], List[Signal], List[int]]:
    netuids = get_selected_subnets(subtensor)
    raw_rows = []
    staged_validator_rows: Dict[int, Dict[int, Tuple[str, List[float]]]] = {}
    staged_subnet_rows: Dict[int, Tuple[float, float, float, float, float]] = {}

    log(f"[scan] scanning {len(netuids)} subnets")
    for pos, netuid in enumerate(netuids, start=1):
        try:
            log(f"[scan] ({pos}/{len(netuids)}) SN{netuid} fetching metagraph...")
            metagraph = get_metagraph(subtensor, netuid, MECHID)
            subnet_obj = get_subnet_obj(subtensor, netuid)

            prev_validator_rows = load_prev_rows(conn, netuid, MECHID)
            prev_subnet_snapshot = load_prev_subnet_snapshot(conn, netuid, MECHID)

            validator_delta, cluster, current_validator_rows, watchlist_score = compute_validator_attention(metagraph, prev_validator_rows)
            emission_now = get_emissions_proxy(metagraph, subnet_obj)
            reserve_now = get_liquidity_proxy(subnet_obj, metagraph)
            eli = emission_now / max(reserve_now, 1e-9)
            impact100, impact500, impact1000 = estimate_alpha_exit_impacts(subtensor, netuid)
            reserve_delta, tao_flow_proxy, trade_activity_proxy = compute_live_flow_proxies(
                prev_subnet_snapshot, reserve_now, emission_now, impact100, impact500, impact1000
            )

            log(
                f"SN{netuid} Δweights={validator_delta:.6f} breadth={cluster} "
                f"ELI={eli:.6f} reserve={reserve_now:.6f} reserveΔ={reserve_delta:+.6f} "
                f"flow={tao_flow_proxy:+.6f} activity={trade_activity_proxy:.6f} impact500={impact500:.2f}%"
            )

            raw_rows.append(
                {
                    "netuid": netuid,
                    "name": get_subnet_name(metagraph, subnet_obj, netuid),
                    "validator_delta": validator_delta,
                    "cluster": cluster,
                    "eli": eli,
                    "watchlist_score": watchlist_score,
                    "impact100": impact100,
                    "impact500": impact500,
                    "impact1000": impact1000,
                    "reserve_now": reserve_now,
                    "reserve_delta": reserve_delta,
                    "tao_flow_proxy": tao_flow_proxy,
                    "trade_activity_proxy": trade_activity_proxy,
                }
            )
            staged_validator_rows[netuid] = current_validator_rows
            staged_subnet_rows[netuid] = (reserve_now, emission_now, impact100, impact500, impact1000)
        except Exception as exc:
            log(f"SN{netuid} error: {type(exc).__name__}: {exc}")
            if SEND_ERROR_MESSAGES:
                try:
                    post_discord(secrets.discord_webhook_url, f"⚠️ SN{netuid} error: `{type(exc).__name__}: {exc}`")
                except Exception:
                    pass
            continue

    if not raw_rows:
        return [], [], [], [], [], []

    norm_validator = normalize([r["validator_delta"] for r in raw_rows])
    norm_cluster = normalize([float(r["cluster"]) for r in raw_rows])
    norm_eli = normalize([r["eli"] for r in raw_rows])
    norm_watch = normalize([r["watchlist_score"] for r in raw_rows])
    norm_tao_flow = normalize([max(r["tao_flow_proxy"], 0.0) for r in raw_rows])
    norm_reserve_change = normalize([max(r["reserve_delta"], 0.0) for r in raw_rows])
    norm_trade_activity = normalize([r["trade_activity_proxy"] for r in raw_rows])

    signals: List[Signal] = []
    now_iso = utc_now_iso()
    for i, row in enumerate(raw_rows):
        local_score = clamp01(
            W_VALIDATOR_DELTA * norm_validator[i]
            + W_CLUSTER * norm_cluster[i]
            + W_ELI * norm_eli[i]
            + W_WATCHLIST * norm_watch[i]
            + W_TAO_FLOW * norm_tao_flow[i]
            + W_RESERVE_CHANGE * norm_reserve_change[i]
            + W_TRADE_ACTIVITY * norm_trade_activity[i]
        )
        classification = score_to_classification(local_score, row["cluster"], row["validator_delta"], row["tao_flow_proxy"])
        confidence = score_to_confidence(local_score)
        regime = estimate_regime(row["cluster"], row["validator_delta"], row["eli"], row["tao_flow_proxy"], row["reserve_delta"])
        liquidity_risk = risk_from_impact(row["impact500"])
        suggested_size = suggested_size_from_impact(row["impact100"], row["impact500"], row["impact1000"], classification)
        sig = Signal(
            netuid=row["netuid"],
            name=row["name"],
            classification=classification,
            score=local_score,
            confidence=confidence,
            validator_delta=row["validator_delta"],
            validator_cluster=row["cluster"],
            eli=row["eli"],
            watchlist_score=row["watchlist_score"],
            regime=regime,
            impact100=row["impact100"],
            impact500=row["impact500"],
            impact1000=row["impact1000"],
            liquidity_risk=liquidity_risk,
            suggested_size=suggested_size,
            exit_signal=classification == "SELL-RISK",
            reserve_now=row["reserve_now"],
            reserve_delta=row["reserve_delta"],
            tao_flow_proxy=row["tao_flow_proxy"],
            trade_activity_proxy=row["trade_activity_proxy"],
            updated_at=now_iso,
            detail=(
                f"Δweights={row['validator_delta']:.6f} | breadth={row['cluster']} | "
                f"ELI={row['eli']:.6f} | reserveΔ={row['reserve_delta']:+.6f} | "
                f"flow={row['tao_flow_proxy']:+.6f} | activity={row['trade_activity_proxy']:.6f}"
            ),
            local_score=local_score,
        )
        signals.append(sig)

    signals.sort(key=lambda s: s.score, reverse=True)
    apply_taostats_confirmation(signals, secrets)

    prev_rankings = load_prev_rankings(conn)
    prev_target = load_prev_target_portfolio(conn)
    feature_history = load_feature_history(conn)
    watch_state = load_watchlist_state(conn)

    annotate_rank_deltas(signals, prev_rankings)
    enrich_signals_with_rotation_metrics(signals, feature_history, watch_state, profile)

    for sig in signals:
        sig.classification = score_to_classification(
            sig.score,
            sig.validator_cluster,
            sig.validator_delta,
            sig.tao_flow_proxy,
            setup_score=sig.setup_score,
            trigger_score=sig.trigger_score,
            decay_score=sig.decay_score,
            watch_stage=sig.watch_stage,
        )
        sig.exit_signal = sig.classification == "SELL-RISK" or sig.decay_score >= SOFT_EXIT_DECAY
        sig.suggested_size = suggested_size_from_impact(sig.impact100, sig.impact500, sig.impact1000, sig.classification)
        sig.detail = (
            f"Δweights={sig.validator_delta:.6f} | breadth={sig.validator_cluster} | "
            f"setup={sig.setup_score:.3f} | trigger={sig.trigger_score:.3f} | decay={sig.decay_score:.3f} | "
            f"flow={sig.tao_flow_proxy:+.6f} | reserveΔ={sig.reserve_delta:+.6f} | lifecycle={sig.lifecycle}"
        )

    signals.sort(key=lambda s: (s.score, s.trigger_score, s.setup_score, -s.decay_score), reverse=True)
    annotate_rank_deltas(signals, prev_rankings)

    current_target = [n for n in derive_target_portfolio(signals, prev_target, profile) if is_tradeable_netuid(n)]
    entries, holds, exits, watch = annotate_portfolio_actions(signals, prev_target, current_target, profile)
    signals = [s for s in signals if is_tradeable_netuid(s.netuid)]
    entries = [s for s in entries if is_tradeable_netuid(s.netuid)]
    holds = [s for s in holds if is_tradeable_netuid(s.netuid)]
    exits = [s for s in exits if is_tradeable_netuid(s.netuid)]
    watch = [s for s in watch if is_tradeable_netuid(s.netuid)]

    for sig in signals:
        insert_history(conn, sig)
        netuid_key = str(sig.netuid)
        existing = feature_history.get(netuid_key, [])
        existing.append(build_feature_snapshot(sig))
        feature_history[netuid_key] = existing[-HISTORY_WINDOW:]

    for netuid, rows in staged_validator_rows.items():
        save_current_rows(conn, netuid, MECHID, rows)
    for netuid, snap in staged_subnet_rows.items():
        reserve_now, emission_now, impact100, impact500, impact1000 = snap
        save_subnet_snapshot(conn, netuid, MECHID, reserve_now, emission_now, impact100, impact500, impact1000)

    save_feature_history(conn, feature_history)
    save_watchlist_state(conn, {str(sig.netuid): sig.watch_stage for sig in signals if sig.watch_stage > 0})
    save_current_rankings(conn, signals)
    save_target_portfolio(conn, current_target)
    return signals, entries, holds, exits, watch, current_target
# ============================================================
# SAFETY / EXECUTION LOGIC
# ============================================================
def trade_key(side: str, netuid: int) -> str:
    return f"{side}:{netuid}"
def trade_allowed_now(conn: sqlite3.Connection, side: str, netuid: int, cooldown_seconds: int) -> bool:
    data = load_last_trade_times(conn)
    last_ts = int(data.get(trade_key(side, netuid), 0))
    return (now_ts() - last_ts) >= cooldown_seconds
def mark_trade_time(conn: sqlite3.Connection, side: str, netuid: int) -> None:
    data = load_last_trade_times(conn)
    data[trade_key(side, netuid)] = now_ts()
    save_last_trade_times(conn, data)
def trades_in_last_hour(conn: sqlite3.Connection) -> int:
    cutoff = now_ts() - 3600
    cur = conn.execute("SELECT COUNT(*) FROM trades WHERE ts >= ? AND status = 'ok'", (cutoff,))
    row = cur.fetchone()
    return int(row[0]) if row else 0
def traded_tao_today(conn: sqlite3.Connection) -> float:
    cutoff = now_ts() - 86400
    total = 0.0
    try:
        cur = conn.execute(
            "SELECT COALESCE(SUM(ABS(actual_wallet_delta_tao)), 0) FROM execution_reconciliations WHERE ts >= ? AND chain_success = 1",
            (cutoff,),
        )
        row = cur.fetchone()
        total = safe_float(row[0], 0.0) if row else 0.0
        if total > 0:
            return round(total, 6)
    except Exception:
        pass

    cur = conn.execute("SELECT side, amount_tao, detail FROM trades WHERE ts >= ? AND status = 'ok'", (cutoff,))
    for side, amount_value, detail in cur.fetchall():
        payload = {}
        try:
            payload = json.loads(detail) if detail else {}
            if not isinstance(payload, dict):
                payload = {}
        except Exception:
            payload = {}
        if str(side).upper() == "ENTRY":
            total += safe_float(payload.get("actual_wallet_debit_tao"), safe_float(amount_value, 0.0))
        else:
            total += safe_float(payload.get("actual_wallet_credit_tao"), 0.0)
    return round(total, 6)
def simulated_pnl_tao(conn: sqlite3.Connection) -> float:
    raw = get_meta(conn, "simulated_pnl_tao")
    return safe_float(raw, 0.0)
def enough_hold_time(conn: sqlite3.Connection, netuid: int, min_hold_time_seconds: int) -> bool:
    state = load_position_state(conn)
    row = state.get(str(netuid))
    if not row:
        return True
    entered_at = int(row.get("entered_at", 0))
    return (now_ts() - entered_at) >= min_hold_time_seconds
def compute_entry_amount(
    profile: Profile,
    wallet_free_tao: Optional[float],
    conn: sqlite3.Connection,
    netuid: int,
    sig: Optional[Signal] = None,
) -> float:
    if wallet_free_tao is None:
        wallet_free_tao = 0.0
    deployable = max(0.0, wallet_free_tao - profile.reserve_buffer_tao)

    if profile.position_sizing_mode == "fixed":
        amount = profile.fixed_entry_tao
    else:
        amount = deployable * (profile.percent_of_free_tao / 100.0)

    amount = min(amount, profile.max_position_tao)
    amount = min(amount, deployable * (profile.max_position_pct / 100.0))

    state = load_position_state(conn)
    row = state.get(str(netuid), {})
    current_pos_value = estimate_position_value_from_row(conn, netuid, row) if row else 0.0
    remaining_cap = max(0.0, profile.max_position_tao - current_pos_value)
    amount = min(amount, remaining_cap)

    if sig is not None:
        conviction_mult = 0.45 + (0.85 * sig.score)
        setup_mult = 0.80 + (0.30 * sig.setup_score)
        trigger_mult = 0.80 + (0.35 * sig.trigger_score)
        impact_penalty = clamp01(1.0 - max(0.0, sig.impact500 - 1.5) / max(profile.max_entry_impact500, 1.0))
        stage_mult = {0: 0.0, 1: 0.35, 2: 0.55, 3: 0.80, 4: 1.00}.get(sig.watch_stage, 1.0)
        amount *= conviction_mult * setup_mult * trigger_mult * max(0.35, impact_penalty) * stage_mult

    return max(0.0, round(amount, 6))


def compute_exit_amount(profile: Profile, conn: sqlite3.Connection, netuid: int, sig: Optional[Signal] = None) -> float:
    current_pos = position_amount_for_netuid(conn, netuid)
    if current_pos <= 0:
        return 0.0
    if sig is not None:
        if sig.decay_score >= HARD_EXIT_DECAY:
            return round(current_pos, 6)
        if sig.decay_score >= SOFT_EXIT_DECAY:
            return round(current_pos * max(profile.partial_exit_pct / 100.0, 0.5), 6)
    if profile.exit_mode == "full":
        return round(current_pos, 6)
    return round(current_pos * (profile.partial_exit_pct / 100.0), 6)


def make_trade_decisions(
    conn: sqlite3.Connection,
    profile: Profile,
    wallet_free_tao: Optional[float],
    entries: List[Signal],
    exits: List[Signal],
) -> Tuple[List[TradeDecision], List[TradeDecision]]:
    entry_decisions: List[TradeDecision] = []
    exit_decisions: List[TradeDecision] = []

    remaining_deployable = None if wallet_free_tao is None else max(0.0, wallet_free_tao - profile.reserve_buffer_tao)
    ranked_entries = sorted(entries, key=lambda s: (s.score, s.trigger_score, s.setup_score, -s.impact500), reverse=True)
    for sig in ranked_entries[: profile.max_new_entries_per_cycle]:
        effective_free_tao = None if remaining_deployable is None else remaining_deployable + profile.reserve_buffer_tao
        amt = compute_entry_amount(profile, effective_free_tao, conn, sig.netuid, sig=sig)
        if remaining_deployable is not None:
            amt = min(amt, max(0.0, remaining_deployable))
        if amt <= 0:
            continue
        entry_decisions.append(
            TradeDecision(
                side="ENTRY",
                netuid=sig.netuid,
                name=sig.name,
                amount_tao=amt,
                reason=(
                    f"entry-ready rotation | lifecycle={sig.lifecycle} | stage={sig.watch_stage} | "
                    f"setup={sig.setup_score:.3f} trigger={sig.trigger_score:.3f} decay={sig.decay_score:.3f} | "
                    f"{explain_signal(sig)}"
                ),
                expected_score=sig.score,
                expected_impact500=sig.impact500,
            )
        )
        if remaining_deployable is not None:
            remaining_deployable = max(0.0, remaining_deployable - amt)

    real_exits = [sig for sig in exits if has_open_position(conn, sig.netuid)]
    for sig in real_exits[: profile.max_exits_per_cycle]:
        if not enough_hold_time(conn, sig.netuid, profile.min_hold_time_seconds):
            continue
        amt = compute_exit_amount(profile, conn, sig.netuid, sig=sig)
        if amt <= 0:
            continue
        exit_decisions.append(
            TradeDecision(
                side="EXIT",
                netuid=sig.netuid,
                name=sig.name,
                amount_tao=amt,
                reason=(
                    f"momentum decay / target removal | lifecycle={sig.lifecycle} | stage={sig.watch_stage} | "
                    f"setup={sig.setup_score:.3f} trigger={sig.trigger_score:.3f} decay={sig.decay_score:.3f}"
                ),
                expected_score=sig.score,
                expected_impact500=sig.impact500,
            )
        )

    return entry_decisions, exit_decisions
def unlock_wallet_if_possible(wallet: Any, password: Optional[str]) -> None:
    if wallet is None or not password:
        return
    attempted = False
    for attr in ("unlock_coldkey", "unlock", "coldkey_file"):
        try:
            obj = getattr(wallet, attr, None)
            if obj is None:
                continue
            attempted = True
            if callable(obj):
                try:
                    obj(password=password)
                    log("[wallet] wallet unlock helper succeeded")
                    return
                except TypeError:
                    pass
            decrypt = getattr(obj, "decrypt", None)
            if callable(decrypt):
                decrypt(password)
                log("[wallet] coldkey decrypt helper succeeded")
                return
            keypair = getattr(obj, "keypair", None)
            if callable(keypair):
                keypair(password=password)
                log("[wallet] keypair unlock helper succeeded")
                return
        except Exception:
            continue
    if attempted:
        log("[wallet] unlock helper unavailable/failed; SDK may prompt on live transaction")
    else:
        log("[wallet] no unlock helper found; SDK may prompt on live transaction")
def _call_subtensor_method(subtensor: Any, candidate_names: List[str], **kwargs: Any) -> Any:
    for name in candidate_names:
        fn = getattr(subtensor, name, None)
        if callable(fn):
            return fn(**kwargs)
    raise RuntimeError(f"No supported subtensor method found in: {candidate_names}")
def execute_entry(
    profile: Profile,
    subtensor: Any,
    wallet: Optional[Any],
    wallet_password: Optional[str],
    decision: TradeDecision,
) -> Dict[str, Any]:
    if not profile.live_mode or profile.execution_mode == "signals_only":
        return {
            "ok": True,
            "dry_run": True,
            "side": "ENTRY",
            "netuid": decision.netuid,
            "name": decision.name,
            "amount_tao": decision.amount_tao,
            "delegate_hotkey": get_validator_hotkey(profile),
            "reason": decision.reason,
        }
    if wallet is None:
        return {"ok": False, "side": "ENTRY", "netuid": decision.netuid, "error": "no wallet selected"}
    try:
        unlock_wallet_if_possible(wallet, wallet_password)
        amount = bt.Balance.from_tao(decision.amount_tao)
        validator_hotkey = get_validator_hotkey(profile)
        before = capture_chain_execution_snapshot(subtensor, wallet, validator_hotkey, decision.netuid)
        result = _call_subtensor_method(
            subtensor,
            candidate_names=["add_stake", "stake", "add_stake_multiple"],
            wallet=wallet,
            hotkey_ss58=validator_hotkey,
            netuid=decision.netuid,
            amount=amount,
        )
        chain_success, chain_message = parse_extrinsic_success(result)
        chain_success = True if chain_success is None else bool(chain_success)
        reconciliation = reconcile_execution_with_chain(
            subtensor, wallet, validator_hotkey, decision.netuid,
            side="ENTRY", requested_amount_tao=decision.amount_tao,
            before=before, chain_success=chain_success,
        )
        return {
            "ok": chain_success,
            "transport_ok": True,
            "chain_success": chain_success,
            "dry_run": False,
            "side": "ENTRY",
            "netuid": decision.netuid,
            "amount_tao": decision.amount_tao,
            "delegate_hotkey": get_validator_hotkey(profile),
            "message": chain_message,
            "result": str(result),
            "reconciliation": reconciliation,
        }
    except Exception as exc:
        return {
            "ok": False,
            "transport_ok": False,
            "chain_success": False,
            "dry_run": False,
            "side": "ENTRY",
            "netuid": decision.netuid,
            "error": f"{type(exc).__name__}: {exc}",
        }

def execute_exit(
    profile: Profile,
    subtensor: Any,
    wallet: Optional[Any],
    wallet_password: Optional[str],
    decision: TradeDecision,
) -> Dict[str, Any]:
    if not profile.live_mode or profile.execution_mode == "signals_only":
        return {
            "ok": True,
            "dry_run": True,
            "side": "EXIT",
            "netuid": decision.netuid,
            "name": decision.name,
            "amount_tao": decision.amount_tao,
            "delegate_hotkey": get_validator_hotkey(profile),
            "reason": decision.reason,
        }
    if wallet is None:
        return {"ok": False, "side": "EXIT", "netuid": decision.netuid, "error": "no wallet selected"}
    try:
        unlock_wallet_if_possible(wallet, wallet_password)
        amount = bt.Balance.from_tao(decision.amount_tao).set_unit(decision.netuid)
        validator_hotkey = get_validator_hotkey(profile)
        before = capture_chain_execution_snapshot(subtensor, wallet, validator_hotkey, decision.netuid)
        result = _call_subtensor_method(
            subtensor,
            candidate_names=["remove_stake", "unstake", "remove_stake_multiple"],
            wallet=wallet,
            hotkey_ss58=validator_hotkey,
            netuid=decision.netuid,
            amount=amount,
        )
        chain_success, chain_message = parse_extrinsic_success(result)
        chain_success = True if chain_success is None else bool(chain_success)
        reconciliation = reconcile_execution_with_chain(
            subtensor, wallet, validator_hotkey, decision.netuid,
            side="EXIT", requested_amount_tao=decision.amount_tao,
            before=before, chain_success=chain_success,
        )
        return {
            "ok": chain_success,
            "transport_ok": True,
            "chain_success": chain_success,
            "dry_run": False,
            "side": "EXIT",
            "netuid": decision.netuid,
            "amount_tao": decision.amount_tao,
            "amount_alpha": decision.amount_tao,
            "delegate_hotkey": get_validator_hotkey(profile),
            "message": chain_message,
            "result": str(result),
            "reconciliation": reconciliation,
        }
    except Exception as exc:
        return {
            "ok": False,
            "transport_ok": False,
            "chain_success": False,
            "dry_run": False,
            "side": "EXIT",
            "netuid": decision.netuid,
            "error": f"{type(exc).__name__}: {exc}",
        }

def confirm_trade_interactively(decision: TradeDecision, profile: Optional[Profile] = None) -> str:
    validator_hotkey = get_validator_hotkey(profile)
    while True:
        raw = input(
            f"{decision.side} {decision.name} SN{decision.netuid} "
            f"{decision.amount_tao:.6f} TAO via {validator_hotkey}. "
            "Confirm? [y]es/[n]o/[a]lways: "
        ).strip().lower()
        if raw in {"y", "yes"}:
            return "yes"
        if raw in {"n", "no"}:
            return "no"
        if raw in {"a", "always"}:
            return "always"
        print("Please enter y, n, or a.", flush=True)

def maybe_execute_trades(
    conn: sqlite3.Connection,
    profile: Profile,
    subtensor: Any,
    wallet: Optional[Any],
    wallet_password: Optional[str],
    entry_decisions: List[TradeDecision],
    exit_decisions: List[TradeDecision],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    entry_results: List[Dict[str, Any]] = []
    exit_results: List[Dict[str, Any]] = []
    session_flags = {"auto_confirm": profile.execution_mode == "auto"}
    started_txn = False
    if not conn.in_transaction:
        conn.execute("BEGIN")
        started_txn = True
    if trades_in_last_hour(conn) >= profile.max_trades_per_hour:
        reason = "max trades per hour reached"
        for d in entry_decisions:
            entry_results.append({"ok": False, "side": "ENTRY", "netuid": d.netuid, "error": reason})
        for d in exit_decisions:
            exit_results.append({"ok": False, "side": "EXIT", "netuid": d.netuid, "error": reason})
        if started_txn and conn.in_transaction:
            conn.commit()
        return entry_results, exit_results, session_flags
    if traded_tao_today(conn) >= profile.daily_max_tao:
        reason = "daily max TAO reached"
        for d in entry_decisions:
            entry_results.append({"ok": False, "side": "ENTRY", "netuid": d.netuid, "error": reason})
        for d in exit_decisions:
            exit_results.append({"ok": False, "side": "EXIT", "netuid": d.netuid, "error": reason})
        if started_txn and conn.in_transaction:
            conn.commit()
        return entry_results, exit_results, session_flags
    if simulated_pnl_tao(conn) <= -abs(profile.daily_max_loss_tao):
        reason = "daily max loss reached"
        for d in entry_decisions:
            entry_results.append({"ok": False, "side": "ENTRY", "netuid": d.netuid, "error": reason})
        for d in exit_decisions:
            exit_results.append({"ok": False, "side": "EXIT", "netuid": d.netuid, "error": reason})
        if started_txn and conn.in_transaction:
            conn.commit()
        return entry_results, exit_results, session_flags
    for d in entry_decisions:
        if not trade_allowed_now(conn, "ENTRY", d.netuid, profile.trade_cooldown_seconds):
            entry_results.append({"ok": False, "side": "ENTRY", "netuid": d.netuid, "error": "cooldown active"})
            continue
        if profile.execution_mode == "confirm" and not session_flags["auto_confirm"]:
            action = confirm_trade_interactively(d, profile)
            if action == "no":
                entry_results.append({"ok": False, "side": "ENTRY", "netuid": d.netuid, "error": "user skipped"})
                bias = load_behavior_bias(conn)
                if d.expected_impact500 > 4.0:
                    bias["avoid_high_impact"] = int(bias.get("avoid_high_impact", 0)) + 1
                save_behavior_bias(conn, bias)
                insert_trade(conn, "ENTRY", d.netuid, d.name, d.amount_tao, "skipped", "user skipped")
                continue
            if action == "always":
                session_flags["auto_confirm"] = True
        result = execute_entry(profile, subtensor, wallet, wallet_password, d)
        entry_results.append(result)
        status = "dry_run" if result.get("dry_run") else ("ok" if result.get("ok") else "failed")
        insert_trade(conn, "ENTRY", d.netuid, d.name, d.amount_tao, status, json.dumps(result, default=str))
        if not result.get("dry_run"):
            insert_execution_reconciliation(
                conn,
                "ENTRY",
                d.netuid,
                d.name,
                d.amount_tao,
                safe_float(result.get("reconciliation", {}).get("wallet_delta_tao"), None),
                safe_float(result.get("reconciliation", {}).get("stake_delta_tao"), None),
                bool(result.get("chain_success", result.get("ok"))),
                result,
            )
        if result.get("ok") and not result.get("dry_run"):
            mark_trade_time(conn, "ENTRY", d.netuid)
            update_position_state_on_entry(conn, d, result=result)
    if exit_decisions and wallet is not None:
        reconcile_position_state_from_chain(conn, subtensor, wallet, profile, [d.netuid for d in exit_decisions])
        refreshed_exit_decisions: List[TradeDecision] = []
        for d in exit_decisions:
            refreshed_amt = compute_exit_amount(profile, conn, d.netuid)
            if refreshed_amt > 0:
                d.amount_tao = refreshed_amt
                refreshed_exit_decisions.append(d)
        exit_decisions = refreshed_exit_decisions

    for d in exit_decisions:
        if not trade_allowed_now(conn, "EXIT", d.netuid, profile.trade_cooldown_seconds):
            exit_results.append({"ok": False, "side": "EXIT", "netuid": d.netuid, "error": "cooldown active"})
            continue
        if profile.execution_mode == "confirm" and not session_flags["auto_confirm"]:
            action = confirm_trade_interactively(d, profile)
            if action == "no":
                exit_results.append({"ok": False, "side": "EXIT", "netuid": d.netuid, "error": "user skipped"})
                bias = load_behavior_bias(conn)
                bias["prefer_conservative"] = int(bias.get("prefer_conservative", 0)) + 1
                save_behavior_bias(conn, bias)
                insert_trade(conn, "EXIT", d.netuid, d.name, d.amount_tao, "skipped", "user skipped")
                continue
            if action == "always":
                session_flags["auto_confirm"] = True
        result = execute_exit(profile, subtensor, wallet, wallet_password, d)
        exit_results.append(result)
        status = "dry_run" if result.get("dry_run") else ("ok" if result.get("ok") else "failed")
        insert_trade(conn, "EXIT", d.netuid, d.name, d.amount_tao, status, json.dumps(result, default=str))
        if not result.get("dry_run"):
            insert_execution_reconciliation(
                conn,
                "EXIT",
                d.netuid,
                d.name,
                d.amount_tao,
                safe_float(result.get("reconciliation", {}).get("wallet_delta_tao"), None),
                safe_float(result.get("reconciliation", {}).get("stake_delta_tao"), None),
                bool(result.get("chain_success", result.get("ok"))),
                result,
            )
        if result.get("ok") and not result.get("dry_run"):
            mark_trade_time(conn, "EXIT", d.netuid)
            full_exit = safe_float(result.get("reconciliation", {}).get("actual_stake_delta_tao"), d.amount_tao) >= position_amount_for_netuid(conn, d.netuid)
            update_position_state_on_exit(conn, d, full_exit=full_exit, result=result)
    if started_txn and conn.in_transaction:
        conn.commit()
    return entry_results, exit_results, session_flags
# ============================================================
# DISCORD / CONSOLE UI
# ============================================================
def format_signal_entry_candidate(sig: Signal, profile: Profile) -> str:
    ts_text = f" | ts=`{sig.taostats_score:.2f}`" if sig.taostats_score > 0 else ""
    explain = f"\nReason: {explain_signal(sig)}" if profile.explain_trades else ""
    return (
        "🟡 **Signal Entry Candidate**\n\n"
        f"**SN{sig.netuid} {sig.name}**\n"
        f"score=`{sig.score:.3f}`{ts_text} | stage=`{sig.watch_stage}` | lifecycle=`{sig.lifecycle}`\n"
        f"setup=`{sig.setup_score:.3f}` | trigger=`{sig.trigger_score:.3f}` | decay=`{sig.decay_score:.3f}`\n"
        f"Δweights=`{sig.validator_delta:+.4f}` | breadth=`{sig.validator_cluster}` | rank=`{sig.rank}` | rankΔ=`{fmt_rank_delta(sig.rank_delta)}`\n"
        f"impact500=`{sig.impact500:.2f}%` | reserve=`{sig.reserve_now:.2f}` | flow=`{sig.tao_flow_proxy:+.2f}`\n"
        f"Validator: `{get_validator_hotkey(profile)}`{explain}"
    )


def format_entry_executed(result: Dict[str, Any], decision: Optional[TradeDecision] = None, sig: Optional[Signal] = None) -> str:
    tao_spent = safe_float(result.get("amount_tao"), safe_float(getattr(decision, "amount_tao", 0.0), 0.0))
    lines = ["✅ **Entry Executed**", ""]
    netuid = result.get("netuid", getattr(decision, "netuid", "?"))
    name = result.get("name") or (decision.name if decision else "")
    title = f"Bought SN{netuid} {name}".strip()
    lines.append(f"**{title}**")
    lines.append(f"TAO spent: `{tao_spent:.6f}`")
    if result.get("alpha_received") is not None:
        lines.append(f"Alpha received: `{safe_float(result.get('alpha_received'), 0.0):.6f}`")
    elif sig is not None:
        est_alpha = tao_spent / max(signal_mark_value(sig), 1e-9)
        lines.append(f"Estimated α received: `{est_alpha:.6f}`")
    lines.append(f"Entry cost basis: `{tao_spent:.6f}` TAO")
    if sig is not None:
        lines.append(f"score=`{sig.score:.3f}` | stage=`{sig.watch_stage}` | lifecycle=`{sig.lifecycle}`")
        lines.append(f"impact500=`{sig.impact500:.2f}%` | reserve=`{sig.reserve_now:.2f}` | flow=`{sig.tao_flow_proxy:+.2f}`")
    lines.append(f"Validator: `{result.get('delegate_hotkey', DEFAULT_VALIDATOR_NAME)}`")
    return "\n".join(lines)


def format_exit_executed(result: Dict[str, Any], decision: Optional[TradeDecision] = None, sig: Optional[Signal] = None) -> str:
    alpha_amt = safe_float(result.get("amount_alpha"), safe_float(result.get("amount_tao"), safe_float(getattr(decision, "amount_tao", 0.0), 0.0)))
    tao_recv = safe_float(result.get("tao_received"), safe_float(result.get("actual_wallet_credit_tao"), 0.0))
    lines = ["✅ **Exit Executed**", ""]
    netuid = result.get("netuid", getattr(decision, "netuid", "?"))
    name = result.get("name") or (decision.name if decision else "")
    title = f"Sold SN{netuid} {name}".strip()
    lines.append(f"**{title}**")
    lines.append(f"Alpha sold: `{alpha_amt:.6f}`")
    if tao_recv > 0:
        lines.append(f"TAO received: `{tao_recv:.6f}`")
    if result.get("realized_pnl_tao") is not None:
        lines.append(f"Realized PnL: `{safe_float(result.get('realized_pnl_tao'), 0.0):+.6f}` TAO")
    if sig is not None:
        lines.append(f"score=`{sig.score:.3f}` | stage=`{sig.watch_stage}` | lifecycle=`{sig.lifecycle}`")
        lines.append(f"impact500=`{sig.impact500:.2f}%` | decay=`{sig.decay_score:.3f}`")
    lines.append(f"Validator: `{result.get('delegate_hotkey', DEFAULT_VALIDATOR_NAME)}`")
    return "\n".join(lines)
def build_portfolio_update(
    profile: Profile,
    current_target: List[int],
    signals: List[Signal],
    entries: List[Signal],
    exits: List[Signal],
    wallet_free_tao: Optional[float],
    conn: sqlite3.Connection,
) -> str:
    by_netuid = {s.netuid: s for s in signals}
    current_names = [f"SN{n}" for n in current_target]
    entry_names = [f"SN{s.netuid}" for s in entries]
    exit_names = [f"SN{s.netuid}" for s in exits]
    watch_names = [f"SN{s.netuid}" for s in signals if s.action == "WATCH"][:5]
    lines = ["📊 **Portfolio Update**"]
    lines.append(f"Held / Target: {', '.join(current_names) if current_names else 'none'}")
    if entry_names:
        lines.append(f"Entry Candidates: {', '.join(entry_names)}")
    if exit_names:
        lines.append(f"Exit Candidates: {', '.join(exit_names)}")
    if watch_names:
        lines.append(f"Watch: {', '.join(watch_names)}")
    if wallet_free_tao is not None:
        lines.append(f"Free TAO: {wallet_free_tao:.6f}")
    basis, est_value, unrealized = compute_portfolio_unrealized_pnl(conn)
    lines.append(f"Allocated TAO: {current_positions_total_tao(conn):.6f}")
    lines.append(f"Open Basis: {basis:.6f} | Open Est. Value: {est_value:.6f} | uPnL: {unrealized:+.6f}")
    lines.append(f"Realized PnL: {cumulative_realized_pnl_tao(conn):+.6f}")
    lines.append(f"Profile: {profile.name} / {profile.execution_mode}")
    lines.append(f"Validator: {get_validator_hotkey(profile)}")
    if current_target:
        top = by_netuid.get(current_target[0])
        if top:
            lines.append(f"Top Conviction: SN{top.netuid} score={top.score:.3f} stage={top.watch_stage} life={top.lifecycle}")
    return "\n".join(lines)

def save_runtime_status(conn: sqlite3.Connection, status: Dict[str, Any]) -> None:
    save_json_meta(conn, "runtime_status", status)

def print_wallet_state(profile: Profile, wallet_free_tao: Optional[float], conn: sqlite3.Connection) -> None:
    print("\n=== WALLET STATE ===", flush=True)
    if wallet_free_tao is None:
        print("Free TAO: unknown", flush=True)
    else:
        print(f"Free TAO: {wallet_free_tao:.6f}", flush=True)
    allocated = current_positions_total_tao(conn)
    print(f"Staked / allocated TAO: {allocated:.6f}", flush=True)
    if wallet_free_tao is not None:
        print(f"Approx total TAO: {wallet_free_tao + allocated:.6f}", flush=True)
        print(f"Available for new entries after reserve: {max(0.0, wallet_free_tao - profile.reserve_buffer_tao):.6f}", flush=True)
def print_positions(conn: sqlite3.Connection) -> None:
    print("\n=== DEPLOYMENT ===", flush=True)
    state = load_position_state(conn)
    if not state:
        print("No tracked positions.", flush=True)
        return
    snap = load_latest_mark_snapshot(conn)
    for netuid, row in state.items():
        basis = safe_float(row.get("tao_basis"), safe_float(row.get("amount_tao"), 0.0))
        alpha_held = safe_float(row.get("alpha_held"), safe_float(row.get("staked_tao"), 0.0))
        entry_mark = max(safe_float(row.get("entry_mark"), 0.0), 1e-9)
        current_mark = safe_float((snap.get(str(netuid), {}) or {}).get("mark"), entry_mark)
        est_value = estimate_position_value_from_row(conn, int(netuid), row)
        unrealized = est_value - basis
        roi = ((est_value / basis) - 1.0) * 100.0 if basis > 0 else 0.0
        print(f"SN{netuid} {row.get('name', '')}", flush=True)
        print(
            f"  basis={basis:.6f} TAO | alpha_held={alpha_held:.6f} | current_mark={current_mark:.6f} TAO/alpha | "
            f"est_value={est_value:.6f} TAO | est_uPnL={unrealized:+.6f} TAO ({roi:+.2f}%)",
            flush=True,
        )

def print_performance(conn: sqlite3.Connection) -> None:
    print("\n=== PERFORMANCE ===", flush=True)
    cur = conn.execute("SELECT COUNT(*) FROM trades WHERE side = 'ENTRY' AND status IN ('ok','dry_run')")
    row = cur.fetchone()
    entries = int(row[0]) if row else 0
    cur = conn.execute("SELECT COUNT(*) FROM trades WHERE side = 'EXIT' AND status IN ('ok','dry_run')")
    row = cur.fetchone()
    exits = int(row[0]) if row else 0
    total_basis, total_value, unrealized = compute_portfolio_unrealized_pnl(conn)
    realized = cumulative_realized_pnl_tao(conn)
    total_pnl = realized + unrealized
    print(f"Entries executed: {entries}", flush=True)
    print(f"Exits executed: {exits}", flush=True)
    print(f"Traded last 24h: {traded_tao_today(conn):.6f} TAO", flush=True)
    print(f"Open basis: {total_basis:.6f} TAO", flush=True)
    print(f"Open est. value: {total_value:.6f} TAO", flush=True)
    print(f"Realized PnL: {realized:+.6f} TAO", flush=True)
    print(f"Unrealized PnL: {unrealized:+.6f} TAO", flush=True)
    print(f"Total PnL: {total_pnl:+.6f} TAO", flush=True)

    cur = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(realized_pnl_tao),0) FROM pnl_journal WHERE event LIKE 'EXIT%'"
    )
    row = cur.fetchone()
    exit_events = int(row[0]) if row else 0
    realized_sum = safe_float(row[1], 0.0) if row else 0.0
    cur = conn.execute(
        "SELECT COUNT(*) FROM pnl_journal WHERE event LIKE 'EXIT%' AND realized_pnl_tao > 0"
    )
    win_count = int(cur.fetchone()[0])
    cur = conn.execute(
        "SELECT COUNT(*) FROM pnl_journal WHERE event LIKE 'EXIT%' AND realized_pnl_tao < 0"
    )
    loss_count = int(cur.fetchone()[0])
    if exit_events > 0:
        win_rate = (win_count / exit_events) * 100.0
        avg_realized = realized_sum / exit_events
        print(f"Closed trade win rate: {win_rate:.1f}% ({win_count}W/{loss_count}L)", flush=True)
        print(f"Avg realized PnL per exit: {avg_realized:+.6f} TAO", flush=True)

    print("\n=== TRADE JOURNAL (last 10) ===", flush=True)
    cur = conn.execute(
        """
        SELECT ts, netuid, name, event, amount_tao, cost_basis_tao, est_value_tao,
               realized_pnl_tao, remaining_cost_basis_tao, remaining_est_value_tao
        FROM pnl_journal
        ORDER BY ts DESC, id DESC
        LIMIT 10
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("No journal entries.", flush=True)
    else:
        for ts, netuid, name, event, amount_tao, cost_basis_tao, est_value_tao, realized_pnl_tao, rem_basis, rem_value in rows:
            stamp = datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            print(
                f"{stamp} | {event} SN{netuid} {name} | {'alpha' if str(event).startswith('EXIT') else 'tao'}={safe_float(amount_tao):.6f} | "
                f"basis={safe_float(cost_basis_tao):.6f} | est={safe_float(est_value_tao):.6f} | "
                f"realized={safe_float(realized_pnl_tao):+.6f} | rem_basis={safe_float(rem_basis):.6f} | rem_est={safe_float(rem_value):.6f}",
                flush=True,
            )


def print_rotation_summary(
    profile: Profile,
    signals: List[Signal],
    entries: List[Signal],
    holds: List[Signal],
    exits: List[Signal],
    current_target: List[int],
) -> None:
    print("\n=== TARGET PORTFOLIO ===", flush=True)
    target_set = set(current_target)
    for sig in signals[:20]:
        if sig.netuid in target_set:
            print(
                f"* SN{sig.netuid} {sig.name} action={sig.action} lifecycle={sig.lifecycle} "
                f"score={sig.score:.3f} setup={sig.setup_score:.3f} trigger={sig.trigger_score:.3f} "
                f"decay={sig.decay_score:.3f} stage={sig.watch_stage} rank={sig.rank} "
                f"rankΔ={fmt_rank_delta(sig.rank_delta)} scoreΔ={fmt_signed(sig.score_delta, 3)}",
                flush=True,
            )

    if entries:
        print("\n=== ENTRIES ===", flush=True)
        for sig in entries:
            explain = f" | {explain_signal(sig)}" if profile.explain_trades else ""
            print(
                f"+ SN{sig.netuid} {sig.name} score={sig.score:.3f} setup={sig.setup_score:.3f} "
                f"trigger={sig.trigger_score:.3f} decay={sig.decay_score:.3f} "
                f"Δw={sig.validator_delta:+.4f} breadth={sig.validator_cluster} impact500={sig.impact500:.2f}%{explain}",
                flush=True,
            )

    if exits:
        print("\n=== EXITS ===", flush=True)
        for sig in exits:
            print(
                f"- SN{sig.netuid} {sig.name} score={sig.score:.3f} setup={sig.setup_score:.3f} "
                f"trigger={sig.trigger_score:.3f} decay={sig.decay_score:.3f} "
                f"Δw={sig.validator_delta:+.4f} breadth={sig.validator_cluster} impact500={sig.impact500:.2f}%",
                flush=True,
            )

    print("\n=== FINAL RANKING ===", flush=True)
    for sig in signals[: max(profile.portfolio_size * 4, 20)]:
        print(
            f"{sig.rank:>2}. SN{sig.netuid:<4} {sig.name[:24]:<24} "
            f"{sig.classification:<9} action={sig.action:<5} stage={sig.watch_stage:<1} "
            f"score={sig.score:.3f} setup={sig.setup_score:.3f} trigger={sig.trigger_score:.3f} "
            f"decay={sig.decay_score:.3f} rankΔ={fmt_rank_delta(sig.rank_delta):<3} "
            f"scoreΔ={fmt_signed(sig.score_delta, 3):<8} ts={sig.taostats_score:.3f} "
            f"life={sig.lifecycle}",
            flush=True,
        )


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    print(f"[startup] network={NETWORK} mechid={MECHID} subnets={SUBNETS}", flush=True)
    print(f"[startup] polling every {POLL_SECONDS}s", flush=True)
    conn = db_connect(SQLITE_DB_PATH)
    subtensor = get_subtensor()
    selection = resolve_wallet_selection(conn)
    if selection is None and BT_WALLET_NAME:
        selection = WalletSelection(wallet_name=BT_WALLET_NAME, wallet_path=BT_WALLET_PATH, hotkey_name=BT_HOTKEY_NAME)
        save_wallet_selection(conn, selection)
    secrets = setup_secrets(conn)
    profile = run_profile_wizard(conn, selection)
    profile = profile_adjustments_from_bias(profile, load_behavior_bias(conn))
    save_profile(conn, profile)
    wallet = maybe_get_wallet(selection, profile)
    wallet_password = prompt_wallet_password_if_needed(profile, selection)
    if profile.live_mode and profile.execution_mode != "signals_only":
        if wallet is None:
            print(
                f"[wallet] live trading enabled but wallet init failed "
                f"(selection={selection.wallet_name if selection else profile.wallet_name or 'none'})",
                flush=True,
            )
        else:
            print(
                f"[wallet] live trading wallet ready: "
                f"{selection.wallet_name if selection else profile.wallet_name} "
                f"@ {selection.wallet_path if selection else profile.wallet_path}",
                flush=True,
            )
    print(
        f"[startup] profile={profile.name} style={profile.style} strategy={profile.strategy_mode} "
        f"mode={profile.execution_mode} live={profile.live_mode}",
        flush=True,
    )
    print(
        f"[startup] taostats_enabled={bool(secrets.taostats_api_key)} "
        f"discord_enabled={bool(secrets.discord_webhook_url)}",
        flush=True,
    )
    test_taostats_auth(secrets)
    if SEND_STARTUP_MESSAGE:
        try:
            post_discord(
                secrets.discord_webhook_url,
                "✅ **Rotation bot started**\n"
                f"profile=`{profile.name}` | strategy=`{profile.strategy_mode}` | mode=`{profile.execution_mode}` | "
                f"live=`{profile.live_mode}` | validator_hotkey=`{get_validator_hotkey(profile)}`"
            )
        except Exception as exc:
            print(f"[warn] startup Discord post failed: {exc}", flush=True)
    while True:
        started = time.time()
        try:
            wallet_free_tao = best_effort_wallet_balance_tao_cached(conn, wallet, subtensor)
            print(f"[{datetime.utcnow().isoformat()}] Fetching signals...", flush=True)
            signals, entries, holds, exits, watch, current_target = build_signals(subtensor, conn, profile, secrets)
            print(f"[{datetime.utcnow().isoformat()}] Got {len(signals)} signals", flush=True)
            print_wallet_state(profile, wallet_free_tao, conn)
            print_positions(conn)
            print_performance(conn)
            if signals:
                print_rotation_summary(profile, signals, entries, holds, exits, current_target)

            total_basis, total_value, unrealized = compute_portfolio_unrealized_pnl(conn)
            status_payload = {
                "state": "running",
                "last_heartbeat": now_ts(),
                "last_signal_scan": utc_now_iso(),
                "profile": profile.name,
                "execution_mode": profile.execution_mode,
                "live_mode": profile.live_mode,
                "wallet": selection.wallet_name if selection else profile.wallet_name,
                "wallet_path": selection.wallet_path if selection else profile.wallet_path,
                "wallet_free_tao": wallet_free_tao,
                "signal_count": len(signals),
                "entry_candidates": len(entries),
                "exit_candidates": len(exits),
                "watch_candidates": len(watch),
                "open_positions": len(load_position_state(conn)),
                "open_basis_tao": total_basis,
                "open_est_value_tao": total_value,
                "unrealized_pnl_tao": unrealized,
                "realized_pnl_tao": cumulative_realized_pnl_tao(conn),
                "default_validator_hotkey": get_validator_hotkey(profile),
            }
            save_runtime_status(conn, status_payload)
            if wallet is not None:
                tracked_netuids = [int(k) for k in load_position_state(conn).keys() if str(k).isdigit()]
                candidate_netuids = [s.netuid for s in entries] + [s.netuid for s in exits]
                reconcile_netuids = sorted({int(n) for n in (tracked_netuids + candidate_netuids) if str(n).isdigit()})
                reconcile_position_state_from_chain(conn, subtensor, wallet, profile, reconcile_netuids)

            entry_decisions, exit_decisions = make_trade_decisions(conn, profile, wallet_free_tao, entries, exits)
            if profile.live_mode and profile.execution_mode != "signals_only" and wallet is None:
                wallet = maybe_get_wallet(selection, profile)
            if profile.live_mode and profile.execution_mode != "signals_only" and wallet is None:
                print("[wallet] skipping trade execution because wallet is unavailable", flush=True)
                status_payload["last_error"] = "wallet unavailable"
                save_runtime_status(conn, status_payload)
                entry_results, exit_results, _session = [], [], None
            else:
                entry_results, exit_results, _session = maybe_execute_trades(
                    conn, profile, subtensor, wallet, wallet_password, entry_decisions, exit_decisions
                )
                successful_trade_netuids = sorted({
                    int(r.get("netuid"))
                    for r in (entry_results + exit_results)
                    if r.get("ok") and not r.get("dry_run") and r.get("netuid") is not None
                })
                if successful_trade_netuids:
                    reconcile_position_state_from_chain(conn, subtensor, wallet, profile, successful_trade_netuids)
            for result in entry_results + exit_results:
                log(f"[trade] {json.dumps(result, default=str)}")
            changed = bool(entries or exits)
            by_netuid = {s.netuid: s for s in signals}
            entry_results_ok = [r for r in entry_results if r.get("ok") and not r.get("dry_run")]
            exit_results_ok = [r for r in exit_results if r.get("ok") and not r.get("dry_run")]
            if changed and profile.send_discord_updates:
                summary_key = f"portfolio:{','.join(str(x) for x in current_target)}"
                if not was_alerted_recently(conn, summary_key, 1800):
                    post_discord(
                        secrets.discord_webhook_url,
                        build_portfolio_update(profile, current_target, signals, entries, exits, wallet_free_tao, conn),
                    )
                    mark_alerted(conn, summary_key)
            if profile.send_discord_updates:
                for result, decision in zip(entry_results, entry_decisions):
                    if not (result.get("ok") and not result.get("dry_run")):
                        continue
                    sig = by_netuid.get(decision.netuid)
                    key = f"entry_exec:{decision.netuid}:{now_ts()}"
                    post_discord(secrets.discord_webhook_url, format_entry_executed(result, decision, sig))
                    mark_alerted(conn, key)
                for result, decision in zip(exit_results, exit_decisions):
                    if not (result.get("ok") and not result.get("dry_run")):
                        continue
                    sig = by_netuid.get(decision.netuid)
                    key = f"exit_exec:{decision.netuid}:{now_ts()}"
                    post_discord(secrets.discord_webhook_url, format_exit_executed(result, decision, sig))
                    mark_alerted(conn, key)
        except KeyboardInterrupt:
            print("Exiting.", flush=True)
            break
        except Exception as exc:
            traceback.print_exc()
            if SEND_ERROR_MESSAGES:
                try:
                    post_discord(
                        secrets.discord_webhook_url,
                        "⚠️ **Rotation bot fatal loop error**\n"
                        f"`{type(exc).__name__}: {str(exc)[:1200]}`"
                    )
                except Exception:
                    pass
        elapsed = time.time() - started
        sleep_for = max(1, POLL_SECONDS - int(elapsed))
        time.sleep(sleep_for)
def run_reconcile_only() -> int:
    conn = db_connect(SQLITE_DB_PATH)
    try:
        subtensor = get_subtensor()
        selection = resolve_wallet_selection(conn)
        if selection is None and BT_WALLET_NAME:
            selection = WalletSelection(wallet_name=BT_WALLET_NAME, wallet_path=BT_WALLET_PATH, hotkey_name=BT_HOTKEY_NAME)
        profile = load_profile(conn) or Profile()
        wallet = maybe_get_wallet(selection, profile)
        result = reconcile_position_state_from_chain(conn, subtensor, wallet, profile)
        print(json.dumps(result, default=str))
        return 0 if not result.get("error") else 1
    finally:
        conn.close()


if __name__ == "__main__":
    if "--reconcile-only" in sys.argv:
        raise SystemExit(run_reconcile_only())
    main()
