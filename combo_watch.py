#!/usr/bin/env python3
"""
CS2 kombo-watch
Overvåger ALLE nye kombo bets (combos) på Polymarket via Polygon-blockchainen
og poster i Discord, når et kombo bet:
  - har mindst ét CS2-ben, og
  - har en samlet mulig udbetaling på mindst MIN_PAYOUT USD (standard 1000).

Sådan virker det:
  1. Læser ERC-1155 transfer-events fra Polymarkets PositionManager-kontrakt.
  2. Finder kombo-positioner (module id 3) på YES-siden, der lander hos en bruger.
     1 kombo-andel udbetaler 1 USD, så antal andele = mulig udbetaling.
  3. Slår kombo'ens ben op i Polymarkets gratis Data API.
  4. Hvis mindst ét ben er CS2 -> besked i Discord.

Kun standardbiblioteket bruges (ingen pip install nødvendig).
"""
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

# ---------- Indstillinger (sættes i GitHub Secrets / workflow) ----------
RPC_URL = os.environ.get("POLYGON_RPC_URL", "").strip()
WEBHOOK = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
MIN_PAYOUT = float(os.environ.get("MIN_PAYOUT") or 1000)
DRY_RUN = os.environ.get("DRY_RUN", "false").strip().lower() == "true"
LOOKBACK = int(os.environ.get("LOOKBACK_BLOCKS") or 0)
LOG_CHUNK = int(os.environ.get("LOG_CHUNK") or 10)  # Alchemy gratis: max 10 blokke pr. opslag

STATE_FILE = "state.json"
FIRST_RUN_BLOCKS = 300          # ca. 10 min tilbage ved allerførste kørsel
MAX_BLOCKS_PER_RUN = 20000      # sikkerhed: max ca. 11 timer pr. kørsel
PENDING_MAX_AGE = 2 * 60 * 60   # prøv at finde benene i op til 2 timer
SEEN_KEEP = 2000

DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

# Polymarket-kontrakter på Polygon (fra docs.polymarket.com/resources/contracts)
POSITION_MANAGER = "0x006f54f7f9a22e0000cc2ab60031000000ae9fef"
IGNORE_TO = {
    "0x0000000000000000000000000000000000000000",
    POSITION_MANAGER,
    "0xe3333700ca9d93003f00f0f71f8515005f6c00aa",  # Exchange v2
    "0x12121212006e4cd160d18e3f00711da5c3372600",  # Router
    "0x1000008dd9001b968442c1000017eae6e0da00ba",  # BinaryModule
    "0x200000900045e3b6259600682756002200028933",  # NegRiskModule
    "0x30000034706c7d8e12009dab006be20000c031a8",  # CombinatorialModule
    "0xa1200000d0002264c9a1698e001292d00e1b00af",  # AutoRedeemer
    "0xe111180000d2663c0091e4f400237545b87b996b",  # CTF Exchange
    "0xe2222d279d744050d28e00520010520000310f59",  # Neg Risk CTF Exchange
}

# Standard ERC-1155 events
TOPIC_SINGLE = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
TOPIC_BATCH = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"

COMBO_MODULE_ID = 3   # de øverste 8 bits i et position ID
YES_OUTCOME = 0       # de nederste 8 bits: 0 = YES
DECIMALS = 10 ** 6    # pUSD / andele har 6 decimaler

CS2_RE = re.compile(r"(^|[^a-z0-9])(cs2|counter[\s-]?strike|cs:?go)([^a-z0-9]|$)", re.I)

# Tier 1-turneringer. Et CS2-ben fra en af disse tæller IKKE.
# Botten poster kun, hvis mindst ét CS2-ben er fra en anden turnering (tier 2-4).
# Tilføj/fjern navne her, hvis en turnering bliver sorteret forkert.
TIER1_EVENTS = [
    r"\biem\b", r"intel extreme masters",
    r"\bblast\b",
    r"\bpgl\b",
    r"esl pro league",
    r"\bmajor\b",
    r"esports world cup", r"\bewc\b",
    r"fissure playground",
    r"thunderpick world championship",
]
TIER1_RE = re.compile("|".join(TIER1_EVENTS), re.I)


# ---------- HTTP ----------
def http_json(url, payload=None, retries=4):
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {
        "User-Agent": "cs2-kombo-watch/1.0",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    for attempt in range(retries):
        req = urllib.request.Request(url, data=data, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                body = r.read()
                return json.loads(body) if body else None
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                wait = int(e.headers.get("Retry-After") or 2 * (attempt + 1))
                time.sleep(min(wait, 30))
                continue
            raise
        except urllib.error.URLError:
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
                continue
            raise


def rpc(method, params):
    res = http_json(RPC_URL, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    if res is None or "error" in res:
        raise RuntimeError(f"RPC-fejl i {method}: {res and res.get('error')}")
    return res["result"]


# ---------- Blockchain ----------
def get_logs(frm, to):
    return rpc("eth_getLogs", [{
        "address": POSITION_MANAGER,
        "fromBlock": hex(frm),
        "toBlock": hex(to),
        "topics": [[TOPIC_SINGLE, TOPIC_BATCH]],
    }])


def scan(frm, to):
    """Henter logs i bidder; gør bidderne mindre hvis udbyderen brokker sig."""
    logs, chunk, cur = [], LOG_CHUNK, frm
    while cur <= to:
        end = min(cur + chunk - 1, to)
        try:
            logs += get_logs(cur, end)
            cur = end + 1
        except Exception as e:  # noqa: BLE001
            if chunk > 10:
                chunk = max(10, chunk // 4)
                print(f"  (getLogs fejlede, prøver mindre bidder: {chunk}) {e}")
                continue
            raise
    return logs


def topic_addr(t):
    return "0x" + t[-40:].lower()


def words(data):
    d = data[2:] if data.startswith("0x") else data
    return [int(d[i:i + 64], 16) for i in range(0, len(d), 64)]


def decode_transfers(log):
    """Giver (from, to, position_id, amount) for et TransferSingle/TransferBatch-log."""
    t = log["topics"]
    if len(t) < 4:
        return []
    frm, to = topic_addr(t[2]), topic_addr(t[3])
    w = words(log["data"])
    if t[0].lower() == TOPIC_SINGLE:
        pairs = [(w[0], w[1])]
    else:
        oi, ov = w[0] // 32, w[1] // 32
        n, m = w[oi], w[ov]
        pairs = list(zip(w[oi + 1:oi + 1 + n], w[ov + 1:ov + 1 + m]))
    return [(frm, to, pid, val) for pid, val in pairs]


def module_id(pid):
    return pid >> 248


def outcome_index(pid):
    return pid & 0xFF


# ---------- Polymarket Data API ----------
def to_int(x):
    if x is None:
        return None
    s = str(x).strip()
    try:
        return int(s, 16) if s.lower().startswith("0x") else int(s)
    except ValueError:
        return None


def find_combo(wallet, pid):
    """Finder kombo'ens række (med ben) i brugerens combo-aktivitet."""
    res = http_json(f"{DATA_API}/v2/activity/combos?user={wallet}&limit=200") or {}
    rows = [r for r in (res.get("data") or []) if to_int(r.get("combo_position_id")) == pid]
    if not rows:
        return None
    buys = [r for r in rows if str(r.get("type", "")).upper() != "REDEEM"]
    return (buys or rows)[0]


def profile_name(wallet):
    try:
        p = http_json(f"{GAMMA_API}/public-profile?address={wallet}") or {}
        return p.get("name") or p.get("pseudonym")
    except Exception:  # noqa: BLE001
        return None


def leg_text(leg):
    m = leg.get("market") or {}
    ev = m.get("event") or {}
    return " ".join(str(x) for x in (
        m.get("slug"), m.get("question"), m.get("title"), m.get("subcategory"),
        ev.get("event_slug"), ev.get("event_title"),
    ) if x)


def is_cs2_leg(leg):
    return bool(CS2_RE.search(leg_text(leg)))


def is_tier1_leg(leg):
    m = leg.get("market") or {}
    ev = m.get("event") or {}
    text = " ".join(str(x) for x in (ev.get("event_title"), m.get("question"), ev.get("event_slug")) if x)
    return bool(TIER1_RE.search(text.replace("-", " ")))


def is_lower_tier_cs2_leg(leg):
    return is_cs2_leg(leg) and not is_tier1_leg(leg)


# ---------- Discord ----------
STATUS_ICON = {"OPEN": "⏳", "RESOLVED_WIN": "✅", "RESOLVED_LOSS": "❌"}


def build_message(c, row):
    wallet = c["to"]
    name = profile_name(wallet) or f"{wallet[:6]}…{wallet[-4:]}"
    payout = c["payout"]
    prices = [leg.get("leg_current_price") for leg in row.get("legs") or []]

    lines = []
    for leg in row.get("legs") or []:
        m = leg.get("market") or {}
        ev = m.get("event") or {}
        icon = STATUS_ICON.get(str(leg.get("leg_status", "")).upper(), "•")
        tag = ("🏆 " if is_tier1_leg(leg) else "🎮 ") if is_cs2_leg(leg) else ""
        title = ev.get("event_title") or m.get("question") or m.get("title") or "?"
        pick = leg.get("leg_outcome_label") or m.get("outcome") or "?"
        price = leg.get("leg_current_price")
        price_txt = f" ({price:.2f})" if isinstance(price, (int, float)) else ""
        lines.append(f"{icon} {tag}**{title}** → {pick}{price_txt}")
    desc = "\n".join(lines)[:3900] or "(ingen ben fundet)"

    fields = [
        {"name": "Konto", "value": f"[{name}](https://polymarket.com/profile/{wallet})", "inline": True},
        {"name": "Mulig udbetaling", "value": f"${payout:,.0f}", "inline": True},
    ]
    if prices and all(isinstance(p, (int, float)) and p > 0 for p in prices):
        prob = 1.0
        for p in prices:
            prob *= p
        fields.append({"name": "Odds lige nu (ca.)", "value": f"{1 / prob:.2f}", "inline": True})
    fields.append({"name": "Ben", "value": str(len(row.get("legs") or [])), "inline": True})
    fields.append({"name": "Transaktion", "value": f"[Polygonscan](https://polygonscan.com/tx/{c['tx']})", "inline": True})

    return {
        "username": "CS2 Kombo-Watch",
        "embeds": [{
            "title": f"🎯 CS2 kombo bet – ${payout:,.0f} mulig udbetaling",
            "description": desc,
            "color": 0xF5A623,
            "fields": fields,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(c["ts"])),
        }],
    }


def post(msg):
    if DRY_RUN or not WEBHOOK:
        print("  [TEST – ikke sendt]\n" + json.dumps(msg, ensure_ascii=False, indent=2))
        return
    http_json(WEBHOOK, msg)
    time.sleep(1)  # undgå Discords rate limit


# ---------- State ----------
def load_state():
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=1)


# ---------- Main ----------
def main():
    if not RPC_URL:
        sys.exit("Mangler POLYGON_RPC_URL (sæt den som GitHub Secret).")

    state = load_state()
    seen = set(state.get("seen") or [])
    pending = state.get("pending") or []

    latest = int(rpc("eth_blockNumber", []), 16) - 5
    if LOOKBACK:
        start = latest - LOOKBACK
    elif state.get("last_block"):
        start = state["last_block"] + 1
    else:
        start = latest - FIRST_RUN_BLOCKS
    start = max(start, latest - MAX_BLOCKS_PER_RUN)

    print(f"Scanner blok {start} → {latest} ({latest - start + 1} blokke), min. udbetaling ${MIN_PAYOUT:,.0f}")
    logs = scan(start, latest) if start <= latest else []

    stats = {"logs": len(logs), "combo_transfers": 0, "combo_yes_to_user": 0, "over_limit": 0}
    pending_keys = {p["key"] for p in pending}
    for log in logs:
        for frm, to, pid, val in decode_transfers(log):
            if module_id(pid) != COMBO_MODULE_ID:
                continue
            stats["combo_transfers"] += 1
            if outcome_index(pid) != YES_OUTCOME or to in IGNORE_TO:
                continue
            stats["combo_yes_to_user"] += 1
            payout = val / DECIMALS
            if payout < MIN_PAYOUT:
                continue
            stats["over_limit"] += 1
            key = f"{log['transactionHash']}-{int(log['logIndex'], 16)}-{pid}"
            if key in seen or key in pending_keys:
                continue
            pending.append({
                "key": key, "to": to, "pid": str(pid), "payout": payout,
                "tx": log["transactionHash"], "ts": int(time.time()),
            })
            pending_keys.add(key)

    alerts, still = 0, []
    for c in pending:
        try:
            row = find_combo(c["to"], int(c["pid"]))
        except Exception as e:  # noqa: BLE001
            print(f"  Data API-fejl for {c['to']}: {e}")
            still.append(c)
            continue
        if row is None:
            if time.time() - c["ts"] < PENDING_MAX_AGE:
                still.append(c)   # Data API er ikke opdateret endnu – prøv igen næste gang
            else:
                print(f"  Opgiver {c['key']} (ben ikke fundet efter 2 timer)")
                seen.add(c["key"])
            continue
        seen.add(c["key"])
        if any(is_lower_tier_cs2_leg(leg) for leg in row.get("legs") or []):
            post(build_message(c, row))
            alerts += 1

    print(f"Status: {stats} | ventende: {len(still)} | Discord-beskeder: {alerts}")

    if not DRY_RUN:
        state["last_block"] = latest
        state["pending"] = still
        state["seen"] = list(seen)[-SEEN_KEEP:]
        save_state(state)


if __name__ == "__main__":
    # Kører i ring i RUN_SECONDS (fx 5,7 timer) med 60 sek. pause, ellers kun én gang
    run_for = int(os.environ.get("RUN_SECONDS") or 0)
    t_end = time.time() + run_for
    while True:
        t0 = time.time()
        try:
            main()
        except Exception as e:  # noqa: BLE001
            print("Fejl i runden (prøver igen om lidt):", e)
        if time.time() >= t_end:
            break
        time.sleep(max(0, 60 - (time.time() - t0)))
