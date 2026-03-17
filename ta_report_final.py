# =============================================================================
# ALPHA BANK — CROSS ASSET TECHNICAL VISTA
# ta_report_final.py — Full Architecture Overhaul (v6)
# Cells 1–8 + Cell 7A (Claude API Integration)
#
# Bloomberg BQuant notebook — generates monthly institutional cross-asset
# technical analysis report. Fetches OHLCV via BQL, computes indicators,
# generates charts, assembles self-contained HTML converted to PDF.
# =============================================================================


# ═══════════════════════════════════════════════════════════════════════════════
# CELL 1 — Imports
# ═══════════════════════════════════════════════════════════════════════════════
import bql
import pandas as pd
import numpy as np
import json, os, math, io, base64, warnings, traceback, time, re as _re
from datetime import datetime, timedelta

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.patches as mpatches
import matplotlib.ticker as mticker
from matplotlib.lines import Line2D

warnings.filterwarnings("ignore")
bq = bql.Service()
print("CELL 1 OK — BQL connected")


# ═══════════════════════════════════════════════════════════════════════════════
# CELL 2 — Configuration
# ═══════════════════════════════════════════════════════════════════════════════
ASSETS = {
    "EURUSD": {"ticker": "EURUSD Curncy", "name": "EUR/USD",    "type": "fx"},
    "GBPUSD": {"ticker": "GBPUSD Curncy", "name": "GBP/USD",    "type": "fx"},
    "USDJPY": {"ticker": "USDJPY Curncy", "name": "USD/JPY",    "type": "fx"},
    "NKY":    {"ticker": "NKY Index",     "name": "Nikkei 225", "type": "index"},
    "SPX":    {"ticker": "SPX Index",     "name": "S&P 500",    "type": "index"},
    "NDX":    {"ticker": "NDX Index",     "name": "Nasdaq 100", "type": "index"},
    "GOLD":   {"ticker": "XAU Curncy",    "name": "Gold",       "type": "commodity"},
    "BTC":    {"ticker": "XBTUSD Curncy", "name": "Bitcoin",    "type": "crypto"},
    "WTI":    {"ticker": "CL1 Comdty",   "name": "WTI Crude",  "type": "commodity"},
}

TODAY        = datetime.today()

# Fetch window: 3 calendar years back. This guarantees >200 trading day
# warm-up for SMA200 across all asset types including BTC (365d/yr).
FETCH_START  = (TODAY - timedelta(days=3 * 365)).strftime("%Y-%m-%d")

# Chart display window: exactly 1 year of daily candles
CHART_START  = (TODAY - timedelta(days=365)).strftime("%Y-%m-%d")

END_DATE     = TODAY.strftime("%Y-%m-%d")
START_DATE   = FETCH_START   # backwards-compat alias used in BQL range calls
REPORT_DATE  = TODAY.strftime("%B %Y")

# Minimum required rows per asset type after fetch
MIN_ROWS = {"fx": 240, "index": 220, "commodity": 220, "crypto": 340}

# Fibonacci lookback per asset type
FIB_LOOKBACK = {
    "fx":        350,   # slow-moving, needs longer history
    "index":     300,
    "commodity": 300,
    "crypto":    180,   # BTC cycles are faster
}

# Pattern detection windows per pattern family
PATTERN_WINDOWS = {
    "reversal":      120,   # H&S, Double Top/Bottom need more history
    "continuation":   60,   # Flags, Wedges, Channels
    "triangle":       80,   # Triangles need enough bars to show convergence
}

# Logo: try multiple filename variants, fall back to inline reconstruction
_LOGO_SVG_RAW = None
for _try in ["alphaBank_logo%20(1).svg",
             "alphaBank_logo (1).svg",
             "alphaBank_logo.svg",
             "alpha_bank_logo.svg"]:
    try:
        with open(_try, "r", encoding="utf-8") as _f:
            _LOGO_SVG_RAW = _f.read().strip()
        print("Logo loaded: " + _try)
        break
    except FileNotFoundError:
        continue
    except Exception as _le:
        print("Logo error (" + _try + "): " + str(_le))
        continue
if not _LOGO_SVG_RAW:
    print("No logo file found — using inline fallback")

# ── Claude API config — set key here; leave blank to use deterministic template fallback ──
CLAUDE_API_KEY = ""
CLAUDE_MODEL   = "claude-sonnet-4-6"
CLAUDE_TEMP    = 0.3

print("CELL 2 OK \u2014 FETCH_START: " + FETCH_START
      + " | CHART_START: " + CHART_START + " | END: " + END_DATE
      + " | Claude: " + ("configured" if CLAUDE_API_KEY else "template fallback"))


# ═══════════════════════════════════════════════════════════════════════════════
# CELL 3 — Data Fetch + Indicators
# ═══════════════════════════════════════════════════════════════════════════════

# ── Custom exceptions ─────────────────────────────────────────────────────────
class BQLFetchError(Exception):
    """Raised when BQL fetch fails after all retry attempts."""
    pass

class DataValidationError(Exception):
    """Raised when fetched data fails validation checks."""
    pass


def fetch_ohlcv(ticker, max_retries=3):
    """Fetch OHLCV data from Bloomberg via BQL with retry logic."""
    last_error = None
    for attempt in range(max_retries):
        try:
            # Per-field BQL fetch — rationale:
            #   close (px_last) is most critical: all indicators depend on it.
            #   open/high/low needed for candlestick rendering and ATR/ADX computation.
            #   volume is last: FX assets suppress it downstream (has_meaningful_volume=False).
            #   Order matches col_names list below for index-based column renaming on merge.
            fields = {
                "px_open":   bq.data.px_open(dates=bq.func.range(START_DATE, END_DATE), per="D", fill="prev"),
                "px_high":   bq.data.px_high(dates=bq.func.range(START_DATE, END_DATE), per="D", fill="prev"),
                "px_low":    bq.data.px_low(dates=bq.func.range(START_DATE, END_DATE), per="D", fill="prev"),
                "px_last":   bq.data.px_last(dates=bq.func.range(START_DATE, END_DATE), per="D", fill="prev"),
                "px_volume": bq.data.px_volume(dates=bq.func.range(START_DATE, END_DATE), per="D", fill="prev"),
            }
            req = bql.Request(ticker, fields)
            res = bq.execute(req)

            # Per-field merge into single DataFrame
            df = None
            col_names = ["open", "high", "low", "close", "volume"]
            for i, item in enumerate(res):
                tmp = item.df().reset_index()
                # Find date column
                date_col = None
                val_col = None
                for c in tmp.columns:
                    lc = c.lower().replace(" ", "").replace("()", "").replace("_", "")
                    if "date" in lc:
                        date_col = c
                    elif lc not in ("id", "ticker", "security") and "date" not in lc:
                        val_col = c
                if date_col is None or val_col is None:
                    continue
                tmp = tmp.rename(columns={date_col: "date", val_col: col_names[i]})
                tmp["date"] = pd.to_datetime(tmp["date"]).dt.strftime("%Y-%m-%d")
                tmp = tmp[["date", col_names[i]]].copy()
                if df is None:
                    df = tmp
                else:
                    df = df.merge(tmp, on="date", how="outer")

            if df is None or df.empty:
                raise ValueError("Empty result set for " + ticker)

            df["ticker"] = ticker
            for col in ("open", "high", "low", "close", "volume"):
                if col not in df.columns:
                    df[col] = np.nan
            df = df[["ticker", "date", "open", "high", "low", "close", "volume"]].copy()
            df = df.sort_values("date").reset_index(drop=True)
            df = df.dropna(subset=["close"]).reset_index(drop=True)
            return df   # clean return on success

        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                time.sleep(2)

    raise BQLFetchError(
        f"Failed after {max_retries} attempts: {ticker} — {last_error}"
    )


def validate_ohlcv(df, asset_key, asset_type):
    """Validate fetched OHLCV data quality — raises DataValidationError on failure."""
    # Check 1: Row count
    min_rows = MIN_ROWS.get(asset_type, 220)
    if len(df) < min_rows:
        raise DataValidationError(
            f"{asset_key}: Insufficient rows ({len(df)} < {min_rows})"
        )

    # Check 2: No NaN in last 10 bars of close
    tail10 = df.tail(10)
    if tail10["close"].isna().any():
        raise DataValidationError(f"{asset_key}: NaN in last 10 close values")

    # Check 3: No NaN in last 10 bars of open, high, low
    for col in ("open", "high", "low"):
        if tail10[col].isna().any():
            raise DataValidationError(
                f"{asset_key}: NaN in last 10 {col} values"
            )

    # Check 4: Last close > 0
    last_close = float(df.iloc[-1]["close"])
    if last_close <= 0:
        raise DataValidationError(f"{asset_key}: Last close <= 0 ({last_close})")

    # Check 5: No single-bar close move > threshold
    threshold = 0.25 if asset_key == "BTC" else 0.20
    closes = df["close"].values.astype(float)
    for i in range(1, len(closes)):
        if closes[i - 1] > 0:
            move = abs(closes[i] / closes[i - 1] - 1)
            if move > threshold:
                raise DataValidationError(
                    f"{asset_key}: Spike at index {i} — "
                    f"{move:.1%} move exceeds {threshold:.0%} threshold"
                )

    # Check 6: Date column monotonically increasing (no duplicates)
    dates = df["date"].values
    for i in range(1, len(dates)):
        if dates[i] <= dates[i - 1]:
            raise DataValidationError(
                f"{asset_key}: Non-monotonic dates at index {i} "
                f"({dates[i-1]} >= {dates[i]})"
            )


def detect_stale_fills(df, ticker):
    """Scan for runs of identical consecutive closes > 5 bars (stale fill detection)."""
    closes = df["close"].values.astype(float)
    dates = df["date"].values
    warnings_list = []
    run_start = 0
    for i in range(1, len(closes)):
        if closes[i] == closes[run_start]:
            run_len = i - run_start + 1
            if run_len > 5 and (i == len(closes) - 1 or closes[i + 1] != closes[run_start]):
                warnings_list.append(
                    f"WARN: {ticker} has {run_len}-bar stale fill starting {dates[run_start]}"
                )
        else:
            run_start = i
    return warnings_list


def _find_swing_points_for_fib(df_slice, is_uptrend, asset_type="index", window=15):
    """Find (fib_anchor_low, fib_anchor_high) for trend-aware Fibonacci."""
    highs = df_slice['high'].values.astype(float)
    lows  = df_slice['low'].values.astype(float)
    n     = len(highs)

    swing_highs = []
    swing_lows  = []
    for i in range(window, n - 1):
        ws = max(0, i - window); we = min(n, i + window + 1)
        if highs[i] == highs[ws:we].max():
            swing_highs.append((i, float(highs[i])))
        if lows[i] == lows[ws:we].min():
            swing_lows.append((i, float(lows[i])))

    if not swing_highs:
        swing_highs = [(n - 1, float(highs.max()))]
    if not swing_lows:
        swing_lows  = [(0, float(lows.min()))]

    last_sh_idx, last_sh_val = swing_highs[-1]
    last_sl_idx, last_sl_val = swing_lows[-1]

    if is_uptrend:
        anchor_high = last_sh_val
        pre = [(i, v) for i, v in swing_lows if i < last_sh_idx]
        anchor_low = pre[-1][1] if pre else last_sl_val
    else:
        anchor_low  = last_sl_val
        pre = [(i, v) for i, v in swing_highs if i < last_sl_idx]
        anchor_high = pre[-1][1] if pre else last_sh_val

    if anchor_high <= anchor_low:
        anchor_high = float(highs.max())
        anchor_low  = float(lows.min())
    return anchor_low, anchor_high


def _compute_weekly_indicators(df):
    """Resample daily OHLCV to weekly and recompute key indicators on weekly bars."""
    df_w = df.copy()
    df_w["date_dt"] = pd.to_datetime(df_w["date"])
    df_w = df_w.set_index("date_dt")

    weekly = df_w.resample("W-FRI").agg({
        "open": "first", "high": "max", "low": "min",
        "close": "last", "volume": "sum"
    }).dropna(subset=["close"])

    if len(weekly) < 20:
        return None   # insufficient weekly bars

    wc = pd.Series(weekly["close"].values.astype(float))

    # Weekly RSI(9)
    delta = wc.diff()
    gain  = delta.clip(lower=0).rolling(9).mean()
    loss  = (-delta.clip(upper=0)).rolling(9).mean()
    w_rsi = (100 - 100 / (1 + gain / loss.replace(0, np.nan))).values

    # Weekly MACD (12/26/9)
    ema12 = wc.ewm(span=12, adjust=False).mean()
    ema26 = wc.ewm(span=26, adjust=False).mean()
    w_macd     = (ema12 - ema26).values
    w_macd_sig = pd.Series(w_macd).ewm(span=9, adjust=False).mean().values

    # Weekly BB (20/2)
    sma20 = wc.rolling(20).mean()
    std20 = wc.rolling(20).std()
    w_bb_upper = (sma20 + 2 * std20).values
    w_bb_lower = (sma20 - 2 * std20).values
    w_bb_pct   = ((wc - pd.Series(w_bb_lower)) /
                  (pd.Series(w_bb_upper) - pd.Series(w_bb_lower) + 1e-9)).values

    # Weekly Stochastic (14/3/3)
    wh = pd.Series(weekly["high"].values.astype(float))
    wl = pd.Series(weekly["low"].values.astype(float))
    low14  = wl.rolling(14).min()
    high14 = wh.rolling(14).max()
    stk = 100 * (wc - low14) / (high14 - low14 + 1e-9)
    w_stoch_k = stk.rolling(3).mean().values
    w_stoch_d = pd.Series(w_stoch_k).rolling(3).mean().values

    # ADX (14) on weekly bars
    wpc = wc.shift(1)
    wtr = pd.concat([wh - wl, (wh - wpc).abs(), (wl - wpc).abs()], axis=1).max(axis=1)
    dmp = wh.diff().clip(lower=0); dmn = (-wl.diff()).clip(lower=0)
    dmp = dmp.where(dmp > dmn, 0); dmn = dmn.where(dmn > dmp, 0)
    watr = wtr.ewm(span=14, adjust=False).mean()
    dip = 100 * dmp.ewm(span=14, adjust=False).mean() / (watr + 1e-9)
    din = 100 * dmn.ewm(span=14, adjust=False).mean() / (watr + 1e-9)
    dx  = 100 * (dip - din).abs() / (dip + din + 1e-9)
    w_adx = dx.ewm(span=14, adjust=False).mean().values

    last = -1
    def _safe(arr, idx):
        """Safe extraction from array."""
        v = arr[idx]
        return float(v) if not math.isnan(v) else None

    return {
        "w_rsi":      _safe(w_rsi, last),
        "w_macd":     _safe(w_macd, last),
        "w_macd_sig": _safe(w_macd_sig, last),
        "w_bb_pct":   _safe(w_bb_pct, last),
        "w_stoch_k":  _safe(w_stoch_k, last),
        "w_stoch_d":  _safe(w_stoch_d, last),
        "w_adx":      _safe(w_adx, last),
        "w_di_plus":  _safe(dip.values, last),   # weekly DI+ for correct adx_sig() call
        "w_di_minus": _safe(din.values, last),   # weekly DI- for correct adx_sig() call
    }


def compute_indicators(df, asset_type="index"):
    """Compute all technical indicators on the full-buffer DataFrame."""
    df = df.copy()
    c, h, lo = (df[x].values.astype(float) for x in ("close", "high", "low"))
    cs, hs, ls = pd.Series(c), pd.Series(h), pd.Series(lo)

    # ── Moving Averages ───────────────────────────────────────────────────────
    df["sma21"]  = cs.rolling(21).mean().values
    df["sma55"]  = cs.rolling(55).mean().values    # Fix 8: SMA55 replaces SMA50
    df["sma200"] = cs.rolling(200).mean().values

    # ── RSI(9) ────────────────────────────────────────────────────────────────
    delta = cs.diff()
    gain  = delta.clip(lower=0).rolling(9).mean()
    loss  = (-delta.clip(upper=0)).rolling(9).mean()
    df["rsi"] = (100 - 100 / (1 + gain / loss.replace(0, np.nan))).values

    # ── MACD (12/26/9) ────────────────────────────────────────────────────────
    ema12 = cs.ewm(span=12, adjust=False).mean()
    ema26 = cs.ewm(span=26, adjust=False).mean()
    ml = ema12 - ema26; ms = ml.ewm(span=9, adjust=False).mean()
    df["macd"] = ml.values; df["macd_signal"] = ms.values; df["macd_hist"] = (ml - ms).values

    # ── Bollinger Bands (20/2) ────────────────────────────────────────────────
    sma20 = cs.rolling(20).mean(); std20 = cs.rolling(20).std()
    df["bb_upper"] = (sma20 + 2 * std20).values
    df["bb_lower"] = (sma20 - 2 * std20).values
    df["bb_pct"]   = ((cs - df["bb_lower"]) / (df["bb_upper"] - df["bb_lower"] + 1e-9)).values

    # ── ADX (14) — preserve DI+ and DI- (Fix 9) ──────────────────────────────
    pc  = cs.shift(1)
    tr  = pd.concat([hs - ls, (hs - pc).abs(), (ls - pc).abs()], axis=1).max(axis=1)
    dmp = hs.diff().clip(lower=0); dmn = (-ls.diff()).clip(lower=0)
    dmp = dmp.where(dmp > dmn, 0); dmn = dmn.where(dmn > dmp, 0)
    atr = tr.ewm(span=14, adjust=False).mean()
    dip = 100 * dmp.ewm(span=14, adjust=False).mean() / (atr + 1e-9)
    din = 100 * dmn.ewm(span=14, adjust=False).mean() / (atr + 1e-9)
    dx  = 100 * (dip - din).abs() / (dip + din + 1e-9)
    df["adx"]      = dx.ewm(span=14, adjust=False).mean().values
    df["di_plus"]  = dip.values    # bullish directional indicator
    df["di_minus"] = din.values    # bearish directional indicator
    df["atr"]      = atr.values    # Average True Range (14) — for volatility-normalised thresholds

    # ── Stochastic (14/3/3) ───────────────────────────────────────────────────
    low14 = ls.rolling(14).min(); high14 = hs.rolling(14).max()
    stk   = 100 * (cs - low14) / (high14 - low14 + 1e-9)
    df["stoch_k"] = stk.rolling(3).mean().values
    df["stoch_d"] = pd.Series(df["stoch_k"]).rolling(3).mean().values

    # ── Fibonacci retracement levels (swing-based, trend-aware) ───────────────
    _last_close  = float(cs.iloc[-1])
    _sma200_last = float(df["sma200"].iloc[-1]) if not math.isnan(float(df["sma200"].iloc[-1])) else _last_close
    _is_uptrend  = _last_close > _sma200_last
    _fib_lookback = FIB_LOOKBACK.get(asset_type, 300)
    _fib_slice   = df.tail(_fib_lookback) if len(df) > _fib_lookback else df
    _fib_low, _fib_high = _find_swing_points_for_fib(
        _fib_slice, _is_uptrend, asset_type=asset_type, window=15
    )
    _rng = _fib_high - _fib_low
    if _rng <= 0:
        _fib_high = float(hs.max()); _fib_low = float(ls.min()); _rng = _fib_high - _fib_low
    df["fib_100"]  = _fib_high
    df["fib_78_6"] = _fib_high - 0.214 * _rng
    df["fib_61_8"] = _fib_high - 0.382 * _rng
    df["fib_50"]   = _fib_high - 0.500 * _rng
    df["fib_38_2"] = _fib_high - 0.618 * _rng
    df["fib_23_6"] = _fib_high - 0.764 * _rng
    df["fib_0"]    = _fib_low

    # ── True weekly indicators (Fix 7) ────────────────────────────────────────
    # Resample OHLCV to weekly (W-FRI) FIRST, then recompute indicators
    # on the weekly bars. Never carry daily indicators via .last().
    weekly = _compute_weekly_indicators(df)
    if weekly:
        for wk, wv in weekly.items():
            df[wk] = wv   # store as scalar columns

    return df


# ── Smoke test — validate all 9 assets ────────────────────────────────────────
for _sk, _sm in ASSETS.items():
    _df_test = fetch_ohlcv(_sm["ticker"])
    _df_test = compute_indicators(_df_test, asset_type=_sm["type"])
    print(f"  {_sm['name']}: rows={len(_df_test)}"
          f"  SMA55={round(float(_df_test.iloc[-1]['sma55']), 4) if not math.isnan(float(_df_test.iloc[-1]['sma55'])) else 'NaN'}"
          f"  RSI={round(float(_df_test.iloc[-1]['rsi']), 1) if not math.isnan(float(_df_test.iloc[-1]['rsi'])) else 'NaN'}")
del _df_test, _sk, _sm
print("CELL 3 OK — all 9 assets validated")


# ═══════════════════════════════════════════════════════════════════════════════
# CELL 4 — Signals, Stats, Momentum, Patterns
# ═══════════════════════════════════════════════════════════════════════════════

def _sf(v, default=0.0):
    """Safe float conversion — handles NaN/Inf gracefully."""
    try:
        f = float(v)
        return default if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return default

def rsi_sig(v):
    """RSI signal classification."""
    if v > 70: return "Overbought"
    if v > 55: return "Bullish"
    if v > 45: return "Neutral"
    if v > 30: return "Bearish"
    return "Oversold"

def macd_sig(m, s):
    """MACD signal classification."""
    if m > s and m > 0: return "Bullish"
    if m > s:           return "Mildly Bullish"
    if m < 0:           return "Bearish"
    return "Mildly Bearish"

def adx_sig(adx, di_plus, di_minus):
    """ADX signal with directional component (Fix 9)."""
    direction = "Bullish" if di_plus > di_minus else "Bearish"
    if adx >= 25: return f"Strong {direction} Trend"
    if adx >= 20: return f"Weak {direction} Trend"
    return "No Trend"

def bb_sig(p):
    """Bollinger Band position signal."""
    if p > 0.8: return "Overbought"
    if p > 0.5: return "Bullish"
    if p > 0.2: return "Neutral"
    if p > 0.0: return "Bearish"
    return "Oversold"

def sma_sig(price, sma):
    """SMA position signal."""
    if sma == 0: return "Neutral"
    d = (price / sma - 1) * 100
    if d >  3: return "Bullish"
    if d >  0: return "Mildly Bullish"
    if d > -3: return "Mildly Bearish"
    return "Bearish"

def stoch_sig(k, d):
    """Stochastic oscillator signal."""
    if k > 80 and d > 80: return "Overbought"
    if k > 50 and k > d:  return "Bullish"
    if k > 50:            return "Neutral"
    if k < 20 and d < 20: return "Oversold"
    return "Bearish"


# ── Weighted Bias Scoring (Fix 12) ────────────────────────────────────────────

def compute_bias_score(close, sma21, sma55, sma200,
                       macd, macd_signal, rsi,
                       di_plus, di_minus, adx,
                       price_roc_20, divergence=None,
                       stoch_k=50.0, stoch_d=50.0):
    """Weighted bias score. Max raw score ~110, clamped to 100.
    Components: trend structure (40) + momentum (30) + directional (30) + oscillator confluence (10).
    """
    score = 0

    # COMPONENT 1 — Trend structure (40 pts total)
    if close > sma200:
        score += 20
    if close > sma55:
        score += 12
    if sma21 > sma55:
        score += 8

    # COMPONENT 2 — Momentum (30 pts total)
    if   rsi > 65: score += 15
    elif rsi > 55: score += 10
    elif rsi > 50: score += 5
    elif rsi > 45: score += 0
    elif rsi > 35: score -= 5
    else:          score -= 10

    if   price_roc_20 > 3.0:  score += 15
    elif price_roc_20 > 1.0:  score += 8
    elif price_roc_20 > 0:    score += 3
    elif price_roc_20 > -1.0: score -= 3
    elif price_roc_20 > -3.0: score -= 8
    else:                      score -= 15

    # COMPONENT 3 — Directional confirmation (30 pts total)
    if macd > macd_signal and macd > 0: score += 12
    elif macd > macd_signal:            score += 6
    elif macd < 0:                      score -= 6

    if   di_plus > di_minus and adx >= 25: score += 10   # Strong bullish trend (mirrors adx_sig threshold)
    elif di_plus > di_minus and adx >= 20: score += 5    # Weak bullish trend: partial bonus
    elif di_plus > di_minus:               score += 2    # No trend, slight bullish lean
    elif di_minus > di_plus and adx >= 25: score -= 8    # Strong bearish trend
    elif di_minus > di_plus and adx >= 20: score -= 4    # Weak bearish trend: partial penalty

    if   sma55 > sma200: score += 8
    elif sma55 < sma200: score -= 4

    # COMPONENT 4 — Oscillator confluence (+10 pts max)
    # Stochastic: independent momentum confirmation (not redundant with RSI)
    if   stoch_k > 50 and stoch_k > stoch_d: score += 5   # bullish K cross
    elif stoch_k < 20:                        score -= 5   # oversold pressure

    # RSI Divergence: confirmed divergence shifts bias despite price trend
    if divergence:
        div_lbl = divergence[0] if isinstance(divergence, tuple) else str(divergence)
        if "Bullish Div" in div_lbl:   score += 5
        elif "Bearish Div" in div_lbl: score -= 5

    return max(0, min(100, score))


def overall_bias(score):
    """Map 0-100 weighted score to bias label."""
    if   score >= 72: return "Bullish"
    elif score >= 58: return "Mildly Bullish"
    elif score >= 42: return "Neutral"
    elif score >= 28: return "Mildly Bearish"
    else:             return "Bearish"


def _fmt(v, d=4):
    """Format a numeric value for display."""
    if v is None: return "—"
    try: return "{:,.{}f}".format(float(v), d)
    except: return str(v)


# ── R² helper for pattern confidence ──────────────────────────────────────────

def _r_squared(x, y):
    """Compute R² for a linear regression fit."""
    if len(x) < 3:
        return 0.0
    coeffs = np.polyfit(x, y, 1)
    predicted = coeffs[0] * x + coeffs[1]
    ss_res = np.sum((y - predicted) ** 2)
    ss_tot = np.sum((y - y.mean()) ** 2)
    return 1 - ss_res / (ss_tot + 1e-9)


# ── Trendline confidence from R² — used by triangle + continuation patterns ───
def _tl_conf(r2):
    """Map R² of a fitted trendline to confidence tier (High / Medium / Low)."""
    if r2 > 0.85: return "High"
    if r2 > 0.70: return "Medium"
    if r2 > 0.55: return "Low"
    return None


# ── RSI Divergence — swing-based (Fix 16) ─────────────────────────────────────

def _detect_rsi_divergence(df, lookback=60):
    """Proper divergence detection using confirmed swing highs/lows."""
    closes = df["close"].values.astype(float)[-lookback:]
    rsi    = df["rsi"].values.astype(float)[-lookback:]
    n      = len(closes)

    # Find swing highs (bearish divergence check)
    swing_high_idxs = []
    for i in range(8, n - 1):
        window_slice = closes[max(0, i - 8):min(n, i + 9)]
        if closes[i] == window_slice.max():
            swing_high_idxs.append(i)

    swing_low_idxs = []
    for i in range(8, n - 1):
        window_slice = closes[max(0, i - 8):min(n, i + 9)]
        if closes[i] == window_slice.min():
            swing_low_idxs.append(i)

    # Bearish divergence: price higher high, RSI lower high
    if len(swing_high_idxs) >= 2:
        i1, i2 = swing_high_idxs[-2], swing_high_idxs[-1]
        price_move = (closes[i2] - closes[i1]) / (closes[i1] + 1e-9)
        if price_move > 0.015:
            if closes[i2] > closes[i1] and rsi[i2] < rsi[i1] - 3:
                return ("Bearish Div.", "bear")

    # Bullish divergence: price lower low, RSI higher low
    if len(swing_low_idxs) >= 2:
        i1, i2 = swing_low_idxs[-2], swing_low_idxs[-1]
        price_move = (closes[i1] - closes[i2]) / (closes[i1] + 1e-9)
        if price_move > 0.015:
            if closes[i2] < closes[i1] and rsi[i2] > rsi[i1] + 3:
                return ("Bullish Div.", "bull")

    return ("None", "neut")


# ── Pattern Strength Tiers ────────────────────────────────────────────────────
# Patterns are ranked by priority: lower number = higher structural significance.
# When multiple patterns are detected on the same asset, the highest-priority
# (lowest number) pattern wins; confidence (High > Medium > Low) is the tiebreaker.
#
# RATIONALE:
#   Tier 0 — Reversal: strongest signal — identifies a likely trend change.
#             Requires the most bars to form; highest false-positive cost.
#   Tier 1 — Triangle: structural compression — imminent directional resolution.
#             Direction ambiguous until breakout, hence below reversal.
#   Tier 2 — Continuation: confirms existing trend only.
#             Lowest conviction; trend could still fail.
_PATTERN_TIERS = {
    "Head & Shoulders":         0,  "Inverse Head & Shoulders": 0,
    "Double Top":               0,  "Double Bottom":            0,
    "Ascending Triangle":       1,  "Descending Triangle":      1,
    "Rising Wedge":             2,  "Falling Wedge":            2,
    "Rising Channel":           2,  "Falling Channel":          2,
    "Bull Flag":                2,  "Bear Flag":                2,
}


# ── Chart Pattern Detection (Fix 13, 14, 15) ─────────────────────────────────

def _detect_chart_pattern(df, stats, asset_type="index"):
    """Detect dominant chart pattern — purely geometric, no score gating."""
    best_pattern = None
    best_confidence = "Low"
    best_priority = 99   # lower = higher priority: 0=reversal, 1=triangle, 2=continuation

    def _check_reversal(df_window):
        """Check reversal patterns on given window."""
        closes = df_window['close'].values.astype(float)
        highs  = df_window['high'].values.astype(float)
        lows   = df_window['low'].values.astype(float)
        n      = len(closes)
        if n < 30:
            return None

        r1s = _fmt(stats['resistances'][0]) if stats['resistances'][0] else "resistance"
        s1s = _fmt(stats['supports'][0]) if stats['supports'][0] else "support"

        t1 = n // 3; t2 = 2 * n // 3
        mh1, mh2, mh3 = highs[:t1].max(), highs[t1:t2].max(), highs[t2:].max()
        ml1, ml2, ml3 = lows[:t1].min(),  lows[t1:t2].min(),  lows[t2:].min()

        # Head & Shoulders — no score gating
        if (mh2 > mh1 * 1.015 and mh2 > mh3 * 1.015 and
                abs(mh1 - mh3) / (mh2 + 1e-9) < 0.06):
            symmetry = abs(mh1 - mh3) / (mh2 + 1e-9)
            if symmetry < 0.02:
                conf = "High"
            elif symmetry < 0.04:
                conf = "Medium"
            else:
                conf = "Low"
            return ("Head & Shoulders",
                    f"A Head & Shoulders reversal structure is visible — a dominant head flanked by two "
                    f"lower shoulders. A confirmed close below the neckline near {s1s} would validate "
                    f"the pattern and signal a potential trend reversal.",
                    conf, 0)

        # Inverse H&S
        if (ml2 < ml1 * 0.985 and ml2 < ml3 * 0.985 and
                abs(ml1 - ml3) / (abs(ml2) + 1e-9) < 0.06):
            symmetry = abs(ml1 - ml3) / (abs(ml2) + 1e-9)
            if symmetry < 0.02:
                conf = "High"
            elif symmetry < 0.04:
                conf = "Medium"
            else:
                conf = "Low"
            return ("Inverse Head & Shoulders",
                    f"An Inverse Head & Shoulders is developing — three troughs with the central "
                    f"trough deepest, a classic bullish reversal. A breakout above {r1s} would confirm.",
                    conf, 0)

        # Double Top
        if (abs(mh1 - mh3) / (mh1 + 1e-9) < 0.03 and mh2 < mh1 * 0.985):
            tol = abs(mh1 - mh3) / (mh1 + 1e-9)
            if tol < 0.02:
                conf = "High"
            elif tol < 0.04:
                conf = "Medium"
            else:
                conf = "Low"
            return ("Double Top",
                    f"A Double Top (M-pattern) is forming — price tested the same resistance zone "
                    f"twice without breaking through. The neckline near {s1s} is the critical level.",
                    conf, 0)

        # Double Bottom
        if (abs(ml1 - ml3) / (abs(ml1) + 1e-9) < 0.03 and ml2 > ml1 * 1.015):
            tol = abs(ml1 - ml3) / (abs(ml1) + 1e-9)
            if tol < 0.02:
                conf = "High"
            elif tol < 0.04:
                conf = "Medium"
            else:
                conf = "Low"
            return ("Double Bottom",
                    f"A Double Bottom (W-pattern) has formed — sellers failed twice to sustain "
                    f"downside from the same support zone. A push above {r1s} would confirm reversal.",
                    conf, 0)

        return None

    def _check_triangle(df_window):
        """Check triangle patterns on given window."""
        closes = df_window['close'].values.astype(float)
        highs  = df_window['high'].values.astype(float)
        lows   = df_window['low'].values.astype(float)
        n      = len(closes)
        if n < 30:
            return None

        r1s = _fmt(stats['resistances'][0]) if stats['resistances'][0] else "resistance"
        s1s = _fmt(stats['supports'][0]) if stats['supports'][0] else "support"

        x = np.arange(n, dtype=float)
        mh_slope, _ = np.polyfit(x, highs, 1)
        ml_slope, _ = np.polyfit(x, lows, 1)
        mean_px = highs.mean()
        r2_h = _r_squared(x, highs)
        r2_l = _r_squared(x, lows)
        r2_min = min(r2_h, r2_l)

        highs_std_r = highs.std() / (highs.mean() + 1e-9)
        lows_std_r  = lows.std() / (lows.mean() + 1e-9)

        # Ascending Triangle
        if (abs(mh_slope) < 0.0003 * mean_px and ml_slope > 0 and highs_std_r < 0.025):
            conf = _tl_conf(r2_min)
            if conf:
                return ("Ascending Triangle",
                        f"An Ascending Triangle — price making higher lows while pressing against "
                        f"flat resistance near {r1s}. A volume-confirmed breakout above resistance "
                        f"would activate the bullish pattern.",
                        conf, 1)

        # Descending Triangle
        if (abs(ml_slope) < 0.0003 * mean_px and mh_slope < 0 and lows_std_r < 0.025):
            conf = _tl_conf(r2_min)
            if conf:
                return ("Descending Triangle",
                        f"A Descending Triangle — lower highs pressing on flat support near {s1s}. "
                        f"A decisive close below support would confirm the breakdown.",
                        conf, 1)

        return None

    def _check_continuation(df_window):
        """Check continuation patterns on given window."""
        closes = df_window['close'].values.astype(float)
        highs  = df_window['high'].values.astype(float)
        lows   = df_window['low'].values.astype(float)
        n      = len(closes)
        if n < 30:
            return None

        r1s = _fmt(stats['resistances'][0]) if stats['resistances'][0] else "resistance"
        s1s = _fmt(stats['supports'][0]) if stats['supports'][0] else "support"

        x = np.arange(n, dtype=float)
        mh_slope, _ = np.polyfit(x, highs, 1)
        ml_slope, _ = np.polyfit(x, lows, 1)
        mean_px = highs.mean()
        r2_h = _r_squared(x, highs)
        r2_l = _r_squared(x, lows)
        r2_min = min(r2_h, r2_l)

        # Rising Wedge
        if (mh_slope > 0 and ml_slope > 0 and ml_slope > mh_slope * 1.2):
            conf = _tl_conf(r2_min)
            if conf:
                return ("Rising Wedge",
                        f"A Rising Wedge is in play — both trendlines rising but converging. "
                        f"This pattern typically resolves to the downside. A break below {s1s} "
                        f"would confirm the bearish resolution.",
                        conf, 2)

        # Falling Wedge
        if (mh_slope < 0 and ml_slope < 0 and mh_slope < ml_slope * 1.2):
            conf = _tl_conf(r2_min)
            if conf:
                return ("Falling Wedge",
                        f"A Falling Wedge is developing — both trendlines declining but converging. "
                        f"A confirmed move above {r1s} would trigger the bullish measured target.",
                        conf, 2)

        # Rising Channel
        if (mh_slope > 0 and ml_slope > 0 and
                abs(mh_slope - ml_slope) / (mean_px + 1e-9) < 0.0005):
            conf = _tl_conf(r2_min)
            if conf:
                return ("Rising Channel",
                        f"Price is trending within a Rising Channel. Support near {s1s} is "
                        f"the key level to hold; a break of the lower trendline would end the phase.",
                        conf, 2)

        # Falling Channel
        if (mh_slope < 0 and ml_slope < 0 and
                abs(mh_slope - ml_slope) / (mean_px + 1e-9) < 0.0005):
            conf = _tl_conf(r2_min)
            if conf:
                return ("Falling Channel",
                        f"A Falling Channel structure is in force — price trending lower between "
                        f"parallel declining trendlines. Rallies to {r1s} likely face resistance.",
                        conf, 2)

        # Bull / Bear Flag
        half = n // 2
        first_move = (closes[half] - closes[0]) / (abs(closes[0]) + 1e-9)
        cons_range = (closes[half:].max() - closes[half:].min()) / (abs(closes[half:].mean()) + 1e-9)

        if first_move > 0.04 and cons_range < first_move * 0.5:
            conf = _tl_conf(r2_min) or "Low"
            return ("Bull Flag",
                    f"A Bull Flag has formed following a sharp advance. The tight consolidation "
                    f"suggests healthy digestion. A breakout above {r1s} would confirm continuation.",
                    conf, 2)

        if first_move < -0.04 and cons_range < abs(first_move) * 0.5:
            conf = _tl_conf(r2_min) or "Low"
            return ("Bear Flag",
                    f"A Bear Flag structure — a sharp decline followed by brief consolidation. "
                    f"A break below {s1s} would confirm bearish continuation.",
                    conf, 2)

        return None

    # Run detection on appropriate windows for each pattern family
    n_bars = len(df)
    for family, check_fn, priority in [
        ("reversal", _check_reversal, 0),
        ("triangle", _check_triangle, 1),
        ("continuation", _check_continuation, 2),
    ]:
        window = PATTERN_WINDOWS.get(family, 60)
        df_win = df.tail(min(window, n_bars))
        result = check_fn(df_win)
        if result:
            pat_name, pat_desc, pat_conf, pat_prio = result
            # Keep highest confidence, tiebreak: reversal > triangle > continuation
            conf_rank = {"High": 0, "Medium": 1, "Low": 2}
            if best_pattern is None:
                best_pattern = (pat_name, pat_desc)
                best_confidence = pat_conf
                best_priority = pat_prio
            elif (conf_rank.get(pat_conf, 3) < conf_rank.get(best_confidence, 3) or
                  (conf_rank.get(pat_conf, 3) == conf_rank.get(best_confidence, 3) and pat_prio < best_priority)):
                best_pattern = (pat_name, pat_desc)
                best_confidence = pat_conf
                best_priority = pat_prio

    if best_pattern:
        return best_pattern[0], best_pattern[1], best_confidence

    # Fallback — score-based pattern (always "Low" confidence)
    bias = stats.get("overall_bias", "Neutral")
    r1s = _fmt(stats['resistances'][0]) if stats['resistances'][0] else "resistance"
    s1s = _fmt(stats['supports'][0]) if stats['supports'][0] else "support"
    fibs = stats.get("fib_levels", {})
    f38 = _fmt(fibs.get("38.2%", 0))
    f50 = _fmt(fibs.get("50%", 0))

    fallback_map = {
        "Bullish":        ("Bullish Continuation", f"Broad bullish alignment. Pullbacks toward {f38} may offer re-entry."),
        "Mildly Bullish": ("Bullish Momentum",     f"Bullish structure intact. Watch {f50} for intraday support."),
        "Neutral":        ("Range Consolidation",   f"No decisive bias. Price between {s1s} and {r1s}. A closing break defines the next leg."),
        "Mildly Bearish": ("Bearish Pressure",      f"Momentum and trend aligned lower. Reclaim of SMA55 needed to neutralise."),
        "Bearish":        ("Distribution / Breakdown", f"Full bearish alignment. Rallies toward {r1s} likely to face selling pressure."),
    }
    fb = fallback_map.get(bias, ("Range Consolidation", "Mixed signals."))
    return fb[0], fb[1], "Low"


# ── S/R Levels — Multi-Source Ranking (Fix 17) ────────────────────────────────

def _compute_sr_levels(df, close, fibs, asset_type):
    """Compute S/R levels from three sources, rank by strength."""
    scored = {}   # price → score

    # Source 1: Fibonacci levels
    fib_weights = {
        "61.8%": 3, "50%": 3, "38.2%": 3,
        "78.6%": 2, "23.6%": 2, "100%": 1, "0%": 1,
    }
    for lbl, val in fibs.items():
        if val is None or math.isnan(float(val)):
            continue
        val = round(float(val), 6)
        scored[val] = scored.get(val, 0) + fib_weights.get(lbl, 1)

    # Source 2: Swing highs and lows (last 252 bars)
    h_arr = df["high"].tail(252).values.astype(float)
    l_arr = df["low"].tail(252).values.astype(float)
    n = len(h_arr)
    sw = 12
    for i in range(sw, n - 1):
        if h_arr[i] == h_arr[max(0, i - sw):min(n, i + sw + 1)].max():
            v = round(float(h_arr[i]), 6)
            scored[v] = scored.get(v, 0) + 3
        if l_arr[i] == l_arr[max(0, i - sw):min(n, i + sw + 1)].min():
            v = round(float(l_arr[i]), 6)
            scored[v] = scored.get(v, 0) + 3

    # Source 3: Round number levels
    if   close >= 10000: magnitudes = [1000, 500]
    elif close >= 1000:  magnitudes = [100, 50]
    elif close >= 100:   magnitudes = [10, 5]
    elif close >= 10:    magnitudes = [1, 0.5]
    elif close >= 1:     magnitudes = [0.1, 0.05]
    else:                magnitudes = [0.01, 0.005]

    for mag in magnitudes:
        lo = math.floor(close * 0.85 / mag) * mag
        hi = math.ceil(close * 1.15 / mag) * mag
        val = lo
        while val <= hi:
            v = round(float(val), 6)
            scored[v] = scored.get(v, 0) + 2
            val = round(val + mag, 10)

    # Apply proximity bonus
    for val in list(scored.keys()):
        dist_pct = abs(val - close) / (close + 1e-9)
        if   dist_pct < 0.015: scored[val] += 2
        elif dist_pct < 0.030: scored[val] += 1

    # Apply confluence bonus
    vals = sorted(scored.keys())
    for i, v1 in enumerate(vals):
        for v2 in vals[i + 1:]:
            if abs(v1 - v2) / (v1 + 1e-9) < 0.005:
                scored[v1] += 3
                scored[v2] += 3

    # Deduplicate
    def dedup(candidates):
        """Merge levels within 0.5% (keep highest score)."""
        result = []
        for val, sc in sorted(candidates, key=lambda x: -x[1]):
            if all(abs(val - prev_val) / (prev_val + 1e-9) > 0.005
                   for prev_val, _ in result):
                result.append((val, sc))
        return result

    res_raw = dedup([(v, s) for v, s in scored.items() if v > close])
    sup_raw = dedup([(v, s) for v, s in scored.items() if v <= close])

    res_sorted = sorted(res_raw, key=lambda x: (-x[1], x[0]))
    sup_sorted = sorted(sup_raw, key=lambda x: (-x[1], -x[0]))

    def top3(lst):
        """Extract top 3 (price, score) tuples."""
        out = [(v, sc) for v, sc in lst[:3]]
        while len(out) < 3:
            out.append((None, 0))
        return out

    return top3(res_sorted), top3(sup_sorted)


# ── Market Structure Intelligence (Fix 18, 20) ───────────────────────────────

def _compute_market_structure(df, stats, divergence=None):
    """Compute market structure signals for the Intelligence Matrix."""
    c_arr  = df['close'].values.astype(float)
    close  = stats['last']
    sma55  = stats['sma55']
    sma200 = stats['sma200']
    adx    = stats['adx_val']
    rsi    = stats['rsi_val']
    bb_p   = stats['bb_pct_val']

    # Trend Phase (Wyckoff-inspired) — uses SMA55 instead of SMA50
    sma55_arr = df['sma55'].values.astype(float)
    sma55_slope = float(sma55_arr[-1] - sma55_arr[-20]) if len(sma55_arr) >= 20 else 0
    if   close > sma55 > sma200 and sma55_slope > 0 and adx >= 20: tp = ("Markup",       "bull")
    elif close > sma55 > sma200 and adx < 20:                       tp = ("Accumulation", "neut")
    elif close < sma55 < sma200 and sma55_slope < 0 and adx >= 20: tp = ("Markdown",     "bear")
    elif close < sma55 < sma200 and adx < 20:                       tp = ("Distribution", "neut")
    else:                                                            tp = ("Transition",   "neut")

    # Volatility Regime (BB Width)
    bbu = df['bb_upper'].values.astype(float)
    bbl = df['bb_lower'].values.astype(float)
    bbm = (bbu + bbl) / 2
    bw  = (bbu - bbl) / (bbm + 1e-9)
    curr_bw = float(bw[-1]) if not math.isnan(bw[-1]) else 0
    avg_bw  = float(np.nanmean(bw[-60:])) if len(bw) >= 60 else float(np.nanmean(bw))
    if   curr_bw > avg_bw * 1.5:  vr = ("Elevated",   "bear")
    elif curr_bw < avg_bw * 0.65: vr = ("Compressed", "bull")
    else:                          vr = ("Normal",     "neut")

    # MACD Crossover
    macd_a  = df['macd'].values.astype(float)
    msig_a  = df['macd_signal'].values.astype(float)
    macd_cross = None
    for i in range(max(1, len(macd_a) - 5), len(macd_a)):
        if not (math.isnan(macd_a[i]) or math.isnan(msig_a[i]) or
                math.isnan(macd_a[i-1]) or math.isnan(msig_a[i-1])):
            if macd_a[i] > msig_a[i] and macd_a[i-1] <= msig_a[i-1]:
                macd_cross = "Bullish Cross \u26a1"
            elif macd_a[i] < msig_a[i] and macd_a[i-1] >= msig_a[i-1]:
                macd_cross = "Bearish Cross \u26a1"
    if macd_cross is None:
        macd_cross = "Above Signal" if macd_a[-1] > msig_a[-1] else "Below Signal"
    mc_cls = "bull" if "Bull" in macd_cross else "bear" if "Bear" in macd_cross else "neut"

    # SMA55/200 Cross — 63-bar lookback with bars_ago (Fix 18)
    sma200_arr = df['sma200'].values.astype(float)
    ma_cross_55_200_lbl = None
    ma_cross_55_200_ago = None
    for i in range(max(1, len(sma55_arr) - 63), len(sma55_arr)):
        if all(not math.isnan(v) for v in [sma55_arr[i], sma200_arr[i], sma55_arr[i-1], sma200_arr[i-1]]):
            if sma55_arr[i] > sma200_arr[i] and sma55_arr[i-1] <= sma200_arr[i-1]:
                ma_cross_55_200_lbl = "Golden Cross"
                ma_cross_55_200_ago = len(sma55_arr) - 1 - i
            elif sma55_arr[i] < sma200_arr[i] and sma55_arr[i-1] >= sma200_arr[i-1]:
                ma_cross_55_200_lbl = "Death Cross"
                ma_cross_55_200_ago = len(sma55_arr) - 1 - i
    if ma_cross_55_200_lbl is None:
        ma_cross_55_200_lbl = "SMA55 > SMA200" if sma55 > sma200 else "SMA55 < SMA200"
    mac_cls = "bull" if "Gold" in (ma_cross_55_200_lbl or "") or ">" in (ma_cross_55_200_lbl or "") else \
              "bear" if "Death" in (ma_cross_55_200_lbl or "") or "<" in (ma_cross_55_200_lbl or "") else "neut"

    # SMA21/55 Cross — 63-bar lookback with bars_ago
    sma21_arr = df['sma21'].values.astype(float)
    ma_cross_21_55_lbl = None
    ma_cross_21_55_ago = None
    sma21_v = stats.get('sma21', 0)
    for i in range(max(1, len(sma21_arr) - 63), len(sma21_arr)):
        if all(not math.isnan(v) for v in [sma21_arr[i], sma55_arr[i], sma21_arr[i-1], sma55_arr[i-1]]):
            if sma21_arr[i] > sma55_arr[i] and sma21_arr[i-1] <= sma55_arr[i-1]:
                ma_cross_21_55_lbl = "Bullish Cross"
                ma_cross_21_55_ago = len(sma21_arr) - 1 - i
            elif sma21_arr[i] < sma55_arr[i] and sma21_arr[i-1] >= sma55_arr[i-1]:
                ma_cross_21_55_lbl = "Bearish Cross"
                ma_cross_21_55_ago = len(sma21_arr) - 1 - i
    if ma_cross_21_55_lbl is None:
        ma_cross_21_55_lbl = "SMA21 > SMA55" if sma21_v > sma55 else "SMA21 < SMA55"
    mac21_cls = "bull" if "Bullish" in (ma_cross_21_55_lbl or "") or ">" in (ma_cross_21_55_lbl or "") else \
                "bear" if "Bearish" in (ma_cross_21_55_lbl or "") or "<" in (ma_cross_21_55_lbl or "") else "neut"

    # RSI Divergence — use pre-computed value from compute_stats() to avoid double computation
    if divergence is None:
        divergence = _detect_rsi_divergence(df)

    # Gap from SMAs
    gap55  = round((close / sma55  - 1) * 100, 2) if sma55  else 0
    gap200 = round((close / sma200 - 1) * 100, 2) if sma200 else 0

    # BB Position label
    if   bb_p > 0.80: bb_lbl = ("Near Upper Band",  "bear")
    elif bb_p > 0.55: bb_lbl = ("Upper Half",        "bull")
    elif bb_p > 0.45: bb_lbl = ("Midpoint",          "neut")
    elif bb_p > 0.20: bb_lbl = ("Lower Half",        "bear")
    else:             bb_lbl = ("Near Lower Band",   "bull")

    return {
        "trend_phase":      tp,
        "vol_regime":       vr,
        "macd_cross":       (macd_cross, mc_cls),
        "ma_cross_55_200":  (ma_cross_55_200_lbl, mac_cls, ma_cross_55_200_ago),
        "ma_cross_21_55":   (ma_cross_21_55_lbl, mac21_cls, ma_cross_21_55_ago),
        "divergence":       divergence,
        "bb_position":      bb_lbl,
        "gap_sma55":        gap55,
        "gap_sma200":       gap200,
    }


# ── Momentum Rows ─────────────────────────────────────────────────────────────

def compute_momentum(df, has_volume=True):
    """Compute multi-period momentum rows from OHLCV data."""
    close = _sf(df.iloc[-1]["close"], 0)

    def pct(n):
        """Period return."""
        sub = df[df["date"] <= (TODAY - timedelta(days=n)).strftime("%Y-%m-%d")]
        b = _sf(sub.iloc[-1]["close"], close) if len(sub) else close
        try:    return round((close / b - 1) * 100, 2) if b != 0 else 0.0
        except: return 0.0

    if has_volume:
        avg20v = df["volume"].tail(20).replace(0, np.nan).mean()
        lv     = _sf(df.iloc[-1]["volume"], None)
        vr     = round(lv / float(avg20v), 2) if (lv and avg20v and float(avg20v) != 0) else None
    else:
        vr = None

    sub20 = df.tail(21)
    if len(sub20) >= 5:
        lr = np.log(sub20["close"].values / np.roll(sub20["close"].values, 1))[1:]
        av = round(float(np.std(lr)) * math.sqrt(252) * 100, 1)
    else:
        av = None

    def p_rsi(n):
        """Period RSI."""
        sub = df.tail(n).copy()
        if len(sub) < 12: return None
        c2  = sub["close"].values.astype(float)
        dlt = pd.Series(c2).diff()
        g   = dlt.clip(lower=0).rolling(9).mean()
        l   = (-dlt.clip(upper=0)).rolling(9).mean()
        r   = (100 - 100 / (1 + g / l.replace(0, np.nan))).values
        v   = r[-1]
        return round(v, 1) if not math.isnan(v) else None

    ytd_df = df[df["date"] >= str(TODAY.year) + "-01-01"]
    ytd    = round((close / _sf(ytd_df.iloc[0]["close"], close) - 1) * 100, 2) if len(ytd_df) else 0.0

    return [
        {"label": "1 Week",   "ret": pct(7),   "prsi": p_rsi(30),  "vr": None, "av": av},
        {"label": "1 Month",  "ret": pct(30),  "prsi": p_rsi(60),  "vr": vr,   "av": None},
        {"label": "3 Months", "ret": pct(90),  "prsi": p_rsi(90),  "vr": None, "av": None},
        {"label": "6 Months", "ret": pct(180), "prsi": p_rsi(130), "vr": None, "av": None},
        {"label": "YTD",      "ret": ytd,      "prsi": None,       "vr": None, "av": None},
        {"label": "1 Year",   "ret": pct(365), "prsi": None,       "vr": None, "av": None},
    ]


# ── Master Stats Function ────────────────────────────────────────────────────

def compute_stats(df, name, asset_type="index"):
    """Compute all technical signals, key levels, momentum, and patterns."""
    L      = df.iloc[-1]
    close  = _sf(L["close"],  0)
    sma21  = _sf(L["sma21"],  close)
    sma55  = _sf(L["sma55"],  close)
    sma200 = _sf(L["sma200"], close)
    rsi_v  = _sf(L["rsi"],    50)
    macd_v = _sf(L["macd"],   0)
    mac_s  = _sf(L["macd_signal"], 0)
    adx_v  = _sf(L["adx"],    20)
    dip_v  = _sf(L["di_plus"], 50)
    din_v  = _sf(L["di_minus"], 50)
    bb_p   = _sf(L["bb_pct"],  0.5)
    stk    = _sf(L["stoch_k"], 50)
    std    = _sf(L["stoch_d"], 50)

    has_volume = asset_type != "fx"

    def spct(a, b):
        """Safe percent change."""
        try:    return round((a / b - 1) * 100, 2) if b != 0 else 0.0
        except: return 0.0

    def cn(n):
        """Close n calendar days ago."""
        sub = df[df["date"] <= (TODAY - timedelta(days=n)).strftime("%Y-%m-%d")]
        return _sf(sub.iloc[-1]["close"], close) if len(sub) else close

    mtd = spct(close, cn(30)); m12 = spct(close, cn(365))
    ytd_df = df[df["date"] >= str(TODAY.year) + "-01-01"]
    ytd    = spct(close, _sf(ytd_df.iloc[0]["close"], close)) if len(ytd_df) else 0.0

    # Price ROC 20 for bias score
    price_roc_20 = round((close / _sf(df.iloc[-21]["close"], close) - 1) * 100, 2) if len(df) > 21 else 0.0

    # Fibonacci key levels
    fibs = {
        "0%":    round(_sf(L["fib_0"],    close), 4),
        "23.6%": round(_sf(L["fib_23_6"], close), 4),
        "38.2%": round(_sf(L["fib_38_2"], close), 4),
        "50%":   round(_sf(L["fib_50"],   close), 4),
        "61.8%": round(_sf(L["fib_61_8"], close), 4),
        "78.6%": round(_sf(L["fib_78_6"], close), 4),
        "100%":  round(_sf(L["fib_100"],  close), 4),
    }

    # S/R from multi-source ranking (Fix 17)
    res_scored, sup_scored = _compute_sr_levels(df, close, fibs, asset_type)
    res = [r[0] for r in res_scored]   # prices only
    sup = [s[0] for s in sup_scored]
    res_scores = [r[1] for r in res_scored]
    sup_scores = [s[1] for s in sup_scored]

    # Pre-compute divergence for both bias score and market structure (avoids double computation)
    _divergence = _detect_rsi_divergence(df)

    # Weighted bias score — includes oscillator confluence (divergence + stochastic)
    bias_score_raw = compute_bias_score(
        close, sma21, sma55, sma200,
        macd_v, mac_s, rsi_v,
        dip_v, din_v, adx_v,
        price_roc_20,
        divergence=_divergence,
        stoch_k=stk, stoch_d=std
    )
    bias_label = overall_bias(bias_score_raw)

    # Chart pattern detection (Fix 13-15)
    pat, pdesc, pconf = _detect_chart_pattern(df, {
        'resistances': [round(r, 4) if r else None for r in res],
        'supports':    [round(s, 4) if s else None for s in sup],
        'fib_levels':  fibs,
        'sma55':       round(sma55, 4),
        'rsi_val':     round(rsi_v, 1),
        'adx_val':     round(adx_v, 1),
        'overall_bias': bias_label,
    }, asset_type)

    # 52-week high/low from OHLCV
    year_ago = (TODAY - timedelta(days=365)).strftime("%Y-%m-%d")
    df_52    = df[df["date"] >= year_ago]
    stats_52w = {
        "high52_calc": round(float(df_52["high"].max()), 4) if len(df_52) else None,
        "low52_calc":  round(float(df_52["low"].min()),  4) if len(df_52) else None,
    }

    # Market structure intelligence — pass pre-computed divergence to avoid recomputation
    ms = _compute_market_structure(df, {
        "last": round(close, 4), "sma21": round(sma21, 4),
        "sma55": round(sma55, 4), "sma200": round(sma200, 4),
        "adx_val": round(adx_v, 1), "rsi_val": round(rsi_v, 1),
        "bb_pct_val": round(bb_p, 2),
        "stoch_k": round(stk, 1), "stoch_d": round(std, 1),
    }, divergence=_divergence)

    # Weekly indicators
    weekly = {}
    for wk in ("w_rsi", "w_macd", "w_macd_sig", "w_bb_pct", "w_stoch_k", "w_stoch_d", "w_adx", "w_di_plus", "w_di_minus"):
        if wk in df.columns:
            val = df[wk].iloc[-1]
            weekly[wk] = float(val) if not (isinstance(val, float) and math.isnan(val)) else None

    # Build signals dict with true weekly indicators (Fix 31)
    signals = {
        "RSI (9)":         {
            "daily":  rsi_sig(rsi_v),
            "weekly": rsi_sig(_sf(weekly.get("w_rsi"), 50)) if weekly.get("w_rsi") is not None else "N/A",
        },
        "MACD (12/26/9)":  {
            "daily":  macd_sig(macd_v, mac_s),
            "weekly": macd_sig(_sf(weekly.get("w_macd"), 0), _sf(weekly.get("w_macd_sig"), 0)) if weekly.get("w_macd") is not None else "N/A",
        },
        "ADX (14)":        {
            "daily":  adx_sig(adx_v, dip_v, din_v),
            "weekly": (adx_sig(
                           _sf(weekly.get("w_adx"),     20),
                           _sf(weekly.get("w_di_plus"),  50),
                           _sf(weekly.get("w_di_minus"), 50)
                       ) if weekly.get("w_adx") is not None else "N/A"),
        },
        "Bollinger Bands": {
            "daily":  bb_sig(bb_p),
            "weekly": bb_sig(_sf(weekly.get("w_bb_pct"), 0.5)) if weekly.get("w_bb_pct") is not None else "N/A",
        },
        "Stochastic":      {
            "daily":  stoch_sig(stk, std),
            "weekly": stoch_sig(_sf(weekly.get("w_stoch_k"), 50), _sf(weekly.get("w_stoch_d"), 50)) if weekly.get("w_stoch_k") is not None else "N/A",
        },
    }

    return {
        "last": round(close, 4), "mtd": mtd, "ytd": ytd, "12m": m12,
        "sma21":  round(sma21,  4),
        "sma55":  round(sma55,  4),
        "sma200": round(sma200, 4),
        "rsi_val":    round(rsi_v, 1), "adx_val": round(adx_v, 1),
        "di_plus":    round(dip_v, 1), "di_minus": round(din_v, 1),
        "bb_pct_val": round(bb_p,  2),
        "stoch_k":    round(stk,   1), "stoch_d": round(std,   1),
        "signals": signals,
        "overall_bias": bias_label,
        "bias_score_raw": bias_score_raw,
        "pattern": pat, "pattern_desc": pdesc, "pattern_confidence": pconf,
        "resistances":     [round(r, 4) if r else None for r in res],
        "supports":        [round(s, 4) if s else None for s in sup],
        "resistance_scores": res_scores,
        "support_scores":    sup_scores,
        "fib_levels":      fibs,
        "momentum_rows":   compute_momentum(df, has_volume=has_volume),
        "stats_52w":       stats_52w,
        "market_structure": ms,
        "weekly_indicators": weekly,
        "price_roc_20":    price_roc_20,
    }


print("CELL 4 OK \u2014 signals, stats, pattern + S/R functions loaded")


# ═══════════════════════════════════════════════════════════════════════════════
# CELL 5 — Chart Generator (1-year daily candles)
# ═══════════════════════════════════════════════════════════════════════════════

def _accent_band(asset_type, report_date):
    """Generate decorative accent band HTML with inline SVG candlestick pattern."""
    # Generate inline SVG candlestick pattern (repeating ~64 candlesticks across 1280px)
    candles_svg = ""
    for i in range(64):
        x = i * 20
        is_bull = (i % 3 != 0)  # alternating pattern
        body_h = 10 if is_bull else 6
        body_y = 9 if is_bull else 11
        candles_svg += (
            f'<rect x="{x+8}" y="{body_y}" width="4" height="{body_h}" '
            f'fill="white" opacity="0.15"/>'
            f'<line x1="{x+10}" y1="4" x2="{x+10}" y2="{body_y}" '
            f'stroke="white" stroke-width="0.8" opacity="0.12"/>'
            f'<line x1="{x+10}" y1="{body_y+body_h}" x2="{x+10}" y2="24" '
            f'stroke="white" stroke-width="0.8" opacity="0.12"/>'
        )

    return (
        '<div class="accent-band">'
        '<svg class="band-svg-overlay" viewBox="0 0 1280 28" preserveAspectRatio="none"'
        ' style="width:100%">'
        + candles_svg + '</svg>'
        '</div>'
    )


def make_chart_b64(df_full, name, stats, asset_type="index"):
    """Render 1-year daily candle chart as base64 PNG at 160 DPI."""

    # Trim to 1-year chart window — SMAs already warm from 3yr buffer
    df_plot = df_full[df_full["date"] >= CHART_START].copy().reset_index(drop=True)
    n  = len(df_plot)
    xs = np.arange(n)

    lmin = float(df_plot["low"].min())
    lmax = float(df_plot["high"].max())
    price_range = lmax - lmin if lmax != lmin else lmax * 0.01

    # Figure + GridSpec
    fig = plt.figure(figsize=(24, 11), facecolor="white")
    gs  = gridspec.GridSpec(
        2, 1,
        height_ratios=[3.2, 1],
        hspace=0.04,
        left=0.025, right=0.975,
        top=0.96,   bottom=0.10,
    )
    ax1 = fig.add_subplot(gs[0])
    ax2 = fig.add_subplot(gs[1])

    ax1.set_facecolor("#fafbfc")
    ax2.set_facecolor("#fafbfc")

    # ── X-axis monthly labels (Fix 21) ────────────────────────────────────────
    month_ticks     = []   # (x_pos, label)
    quarter_gridlines = []

    seen_ym = set()
    for i, row in df_plot.iterrows():
        d_str = row["date"]
        yr    = int(d_str[:4])
        mth   = int(d_str[5:7])
        ym    = (yr, mth)
        if ym in seen_ym:
            continue
        seen_ym.add(ym)

        if mth == 1:
            month_ticks.append((i, f"Jan '{str(yr)[2:]}"))
            quarter_gridlines.append(i)
        else:
            abbr = {2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                    7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}.get(mth, "")
            month_ticks.append((i, abbr))
            if mth in (4, 7, 10):
                quarter_gridlines.append(i)

    # Quarterly gridlines
    for xg in quarter_gridlines:
        ax1.axvline(xg, color="#e5e7eb", lw=0.8, zorder=1)
        ax2.axvline(xg, color="#e5e7eb", lw=0.8, zorder=1)

    ax1.grid(axis="y", color="#e5e7eb", lw=0.5, zorder=0)

    # ── Candlesticks ──────────────────────────────────────────────────────────
    cw = float(np.clip(260 / max(n, 1) * 0.65, 0.35, 0.80))
    BULL_COL = "#16a34a"
    BEAR_COL = "#dc2626"

    for i in range(n):
        row = df_plot.iloc[i]
        op  = _sf(row.get("open"),  None)
        hi  = _sf(row.get("high"),  None)
        lo  = _sf(row.get("low"),   None)
        cl  = _sf(row.get("close"), None)
        if None in (op, hi, lo, cl):
            continue
        bullish = cl >= op
        col     = BULL_COL if bullish else BEAR_COL
        ax1.plot([i, i], [lo, hi], color=col, lw=0.6, zorder=3)
        body_bot = min(op, cl)
        body_h   = max(abs(cl - op), price_range * 0.0002)
        ax1.add_patch(mpatches.Rectangle(
            (i - cw / 2, body_bot), cw, body_h,
            facecolor=col, edgecolor="none", zorder=4,
        ))

    # ── SMA lines — triple-pass glow (Fix 20) ────────────────────────────────
    def _draw_sma_triple(ax, xs, y_vals, base_color, highlight_color, lw_main=2.0):
        """Draw one SMA with glow + main + highlight passes."""
        mask = ~np.isnan(y_vals.astype(float))
        if mask.sum() < 2:
            return
        x_seg = xs[mask].astype(float)
        y_seg = y_vals[mask].astype(float)
        ax.plot(x_seg, y_seg, color=base_color, lw=5.0, alpha=0.08,
                zorder=5, solid_capstyle="round")
        ax.plot(x_seg, y_seg, color=base_color, lw=lw_main, alpha=1.0,
                zorder=6, solid_capstyle="round")
        ax.plot(x_seg, y_seg, color=highlight_color, lw=0.8, alpha=0.50,
                zorder=7, solid_capstyle="round")

    # SMA21 — purple
    if "sma21" in df_plot.columns:
        _draw_sma_triple(ax1, xs, df_plot["sma21"].values,
                         base_color="#a855f7", highlight_color="#e9d5ff", lw_main=1.4)

    # SMA55 — orange
    if "sma55" in df_plot.columns:
        _draw_sma_triple(ax1, xs, df_plot["sma55"].values,
                         base_color="#F97316", highlight_color="#FED7AA", lw_main=2.0)

    # SMA200 — blue
    if "sma200" in df_plot.columns:
        _draw_sma_triple(ax1, xs, df_plot["sma200"].values,
                         base_color="#0EA5E9", highlight_color="#BAE6FD", lw_main=2.0)

    # ── Title & legend ────────────────────────────────────────────────────────
    ax1.set_title(name, fontsize=13, fontweight="heavy", color="#374151", pad=6, loc="center")
    legend_handles = [
        Line2D([0], [0], color="#a855f7", lw=1.4, label="SMA (21)"),
        Line2D([0], [0], color="#F97316", lw=2.5, label="SMA (55)"),
        Line2D([0], [0], color="#0EA5E9", lw=2.5, label="SMA (200)"),
    ]
    ax1.legend(handles=legend_handles, loc="upper left", fontsize=9,
               framealpha=0.90, edgecolor="#e5e7eb", fancybox=False)

    # ax1 axes styling
    ax1.set_xlim(-0.5, n + 8)
    ax1.set_ylim(lmin * 0.992, lmax * 1.008)
    ax1.yaxis.set_tick_params(labelsize=10, colors="#374151")
    ax1.tick_params(axis="x", bottom=False, labelbottom=False)
    ax1.tick_params(axis="y", left=True, labelsize=10, colors="#374151")
    for sp in ("top", "right"):
        ax1.spines[sp].set_visible(False)
    for sp in ("left", "bottom"):
        ax1.spines[sp].set_color("#d1d5db")

    # ── RSI panel (label "RSI (9)" — Fix 23) ─────────────────────────────────
    rsi_vals = df_plot["rsi"].values.astype(float) if "rsi" in df_plot.columns else np.full(n, 50.0)
    _rsi_finite = np.isfinite(rsi_vals)
    ax2.fill_between(xs, rsi_vals, 0, alpha=0.15, color="#7c3aed", zorder=1,
                     where=_rsi_finite)
    ax2.fill_between(xs, rsi_vals, 70, where=(rsi_vals >= 70) & _rsi_finite,
                     alpha=0.20, color="#ef4444", zorder=2, interpolate=True)
    ax2.fill_between(xs, rsi_vals, 30, where=(rsi_vals <= 30) & _rsi_finite,
                     alpha=0.20, color="#16a34a", zorder=2, interpolate=True)
    ax2.plot(xs[_rsi_finite], rsi_vals[_rsi_finite], color="#7c3aed", lw=1.5, zorder=3)
    ax2.axhline(70, color="#ef4444", lw=0.9, ls=(0, (4, 3)), alpha=0.90, zorder=4)
    ax2.axhline(50, color="#d1d5db", lw=0.5, ls=":", alpha=0.80, zorder=4)
    ax2.axhline(30, color="#16a34a", lw=0.9, ls=(0, (4, 3)), alpha=0.90, zorder=4)
    ax2.text(n + 1.0, 70, "OB", color="#ef4444", fontsize=7.5,
             va="center", fontweight="bold", clip_on=False)
    ax2.text(n + 1.0, 30, "OS", color="#16a34a", fontsize=7.5,
             va="center", fontweight="bold", clip_on=False)

    # RSI panel label
    ax2.set_ylabel("RSI (9)", fontsize=9, color="#6b7280", labelpad=8)

    ax2.set_xlim(-0.5, n + 8)
    ax2.set_ylim(0, 100)
    ax2.set_yticks([30, 50, 70])
    ax2.set_yticklabels(["30", "50", "70"], fontsize=10, color="#374151")
    ax2.tick_params(axis="y", labelsize=10, colors="#374151")
    ax2.grid(axis="y", color="#e5e7eb", lw=0.5, zorder=0)
    for sp in ("top", "right"):
        ax2.spines[sp].set_visible(False)
    for sp in ("left", "bottom"):
        ax2.spines[sp].set_color("#d1d5db")

    # X-axis monthly ticks on ax2
    mt_positions = [x for x, _ in month_ticks]
    mt_labels    = [lbl for _, lbl in month_ticks]
    ax2.set_xticks(mt_positions, minor=False)
    ax2.set_xticklabels(mt_labels, fontsize=9, color="#374151", ha="center", fontweight="semibold")
    ax2.tick_params(axis="x", which="major", length=4, width=0.8, color="#9ca3af", pad=3)

    # ── Encode to base64 PNG @ 160 DPI (Fix 22) ──────────────────────────────
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=160, bbox_inches="tight",
                facecolor="white", edgecolor="none")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


print("CELL 5 OK — chart generator ready")


# ═══════════════════════════════════════════════════════════════════════════════
# CELL 6 — Main Loop
# ═══════════════════════════════════════════════════════════════════════════════

run_log = {
    "report_date": REPORT_DATE,
    "run_timestamp": datetime.now().isoformat(),
    "assets": {},
    "summary": {},
}

def _log_asset_failure(key, meta, e, elapsed, exc_kind, print_traceback=False):
    """Write failure records to run_log and report_data; print status line."""
    print(f"    FAILED ({exc_kind}): {e}")
    if print_traceback:
        traceback.print_exc()
    run_log["assets"][key] = {
        "status": "failed", "rows_fetched": 0,
        "bias": "", "pattern": "", "pattern_confidence": "",
        "data_warnings": [], "error": str(e),
        "runtime_seconds": elapsed,
    }
    report_data[key] = {
        "meta": meta, "stats": None, "chart_b64": None,
        "data_warnings": [], "error": str(e),
    }


report_data = {}
for _k, _m in ASSETS.items():
    _t0 = time.time()
    print(f"  {_m['name']} ...")
    try:
        # 1. Fetch with retry
        _raw = fetch_ohlcv(_m["ticker"])

        # 2. Validate
        validate_ohlcv(_raw, _k, _m["type"])

        # 3. Detect stale fills
        _stale_warnings = detect_stale_fills(_raw, _m["ticker"])
        for _sw in _stale_warnings:
            print("    " + _sw)

        # 4. Compute indicators (pass asset_type)
        _full = compute_indicators(_raw, asset_type=_m["type"])

        # ── Cross-asset date alignment note ───────────────────────────────────
        # Each asset's DataFrame retains its own native calendar. All indicators
        # are computed on each asset's own date series independently. No code
        # path should cross-reference DataFrames across assets.

        # 5. Compute stats (pass asset_type)
        _st = compute_stats(_full, _m["name"], asset_type=_m["type"])

        # 6. Build performance dict from OHLCV momentum rows
        _mom = _st["momentum_rows"]
        _perf = {
            "r1w":    _mom[0]["ret"],
            "r1m":    _mom[1]["ret"],
            "r3m":    _mom[2]["ret"],
            "r6m":    _mom[3]["ret"],
            "rytd":   _mom[4]["ret"],
            "r1y":    _mom[5]["ret"],
            "high52": _st["stats_52w"].get("high52_calc"),
            "low52":  _st["stats_52w"].get("low52_calc"),
        }
        _st["bql_perf"] = _perf

        # 7. FX volume suppression flag
        _st["has_meaningful_volume"] = _m["type"] != "fx"

        # 8. Generate chart (pass asset_type)
        _ch = make_chart_b64(_full, _m["name"], _st, asset_type=_m["type"])

        _elapsed = round(time.time() - _t0, 1)
        _status = "warned" if _stale_warnings else "ok"

        report_data[_k] = {
            "meta": _m, "stats": _st, "chart_b64": _ch,
            "data_warnings": _stale_warnings,
        }

        run_log["assets"][_k] = {
            "status": _status,
            "rows_fetched": len(_raw),
            "bias": _st["overall_bias"],
            "pattern": _st["pattern"],
            "pattern_confidence": _st["pattern_confidence"],
            "data_warnings": _stale_warnings,
            "error": None,
            "runtime_seconds": _elapsed,
        }

        print(f"    OK  rows={len(_raw)}  bias={_st['overall_bias']}"
              f"  pattern={_st['pattern']} ({_st['pattern_confidence']})"
              f"  {_elapsed}s")

    except BQLFetchError as _e:
        _log_asset_failure(_k, _m, _e, round(time.time() - _t0, 1), "BQLFetchError")

    except DataValidationError as _e:
        _log_asset_failure(_k, _m, _e, round(time.time() - _t0, 1), "DataValidationError")

    except Exception as _e:
        _log_asset_failure(_k, _m, _e, round(time.time() - _t0, 1), "Exception", print_traceback=True)

# Print summary table
print("\n" + "=" * 60)
print("RUN SUMMARY")
print("=" * 60)
for _rk, _rv in run_log["assets"].items():
    print(f"  {_rk:8s}  {_rv['status']:8s}  {_rv.get('bias',''):16s}  "
          f"{_rv.get('pattern',''):24s}  {_rv['runtime_seconds']:.1f}s")
print("=" * 60)
print(f"CELL 6 OK — {sum(1 for v in report_data.values() if v.get('stats'))}"
      f"/{len(ASSETS)} assets loaded")


# ═══════════════════════════════════════════════════════════════════════════════
# CELL 7A — Claude API Integration
# ═══════════════════════════════════════════════════════════════════════════════
# CLAUDE_API_KEY / CLAUDE_MODEL / CLAUDE_TEMP are defined in Cell 2 (config section).

_CLAUDE_SYSTEM = (
    "You are the institutional TA narrator for Alpha Bank Cross Asset Technical Vista.\n"
    "STRICT RULES:\n"
    "1. Third-person institutional voice. No I or we.\n"
    "2. Max 3 concise bullets per asset, each under 45 words.\n"
    "3. Never invent price levels not present in the brief.\n"
    "4. Never contradict the bias supplied in the brief.\n"
    "5. No emojis or informal language.\n"
    "6. Reference pattern, S/R levels, and at least one momentum indicator.\n"
    "7. Pattern text must reference pattern_confidence.\n"
    "8. Outlook must reference both R1 and S1.\n"
    "9. Return ONLY valid JSON with keys: title, bullet1, bullet2, bullet3, pattern_text, outlook."
)


def build_claude_brief(key, asset):
    """Build structured brief dict for Claude API."""
    s    = asset["stats"]
    meta = asset["meta"]
    ms   = s.get("market_structure", {})
    return {
        "asset_key":          key,
        "asset_name":         meta["name"],
        "asset_type":         meta["type"],
        "report_date":        REPORT_DATE,
        "last":               s["last"],
        "bias":               s["overall_bias"],
        "bias_score":         round(s.get("bias_score_raw", 50), 1),
        "rsi":                s["rsi_val"],
        "adx":                s["adx_val"],
        "di_plus":            s.get("di_plus", 0),
        "di_minus":           s.get("di_minus", 0),
        "macd_signal":        s["signals"].get("MACD (12/26/9)", {}).get("daily", ""),
        "bb_pct":             round(s["bb_pct_val"] * 100, 1),
        "stoch_k":            s["stoch_k"],
        "sma21":              s["sma21"],
        "sma55":              s["sma55"],
        "sma200":             s["sma200"],
        "pattern":            s["pattern"],
        "pattern_confidence": s.get("pattern_confidence", "Medium"),
        "R1":                 s["resistances"][0],
        "R2":                 s["resistances"][1],
        "S1":                 s["supports"][0],
        "S2":                 s["supports"][1],
        "trend_phase":        ms.get("trend_phase", ("", ""))[0],
        "vol_regime":         ms.get("vol_regime", ("", ""))[0],
        "ma_cross_55_200":    ms.get("ma_cross_55_200", ("", "", None))[0],
        "ma_cross_21_55":     ms.get("ma_cross_21_55", ("", "", None))[0],
        "divergence":         ms.get("divergence", ("None", ""))[0],
        "mtd":                s["mtd"],
        "ytd":                s["ytd"],
        "price_roc_20":       s.get("price_roc_20", 0),
    }


def _call_claude(brief_dict, max_retries=2):
    """Call Claude API and return parsed JSON dict."""
    try:
        import anthropic
    except ImportError:
        raise RuntimeError("anthropic package not installed")
    import re as _re2
    client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)
    brief_str = json.dumps(brief_dict, indent=2)
    for attempt in range(max_retries + 1):
        try:
            msg = client.messages.create(
                model=CLAUDE_MODEL, max_tokens=512, temperature=CLAUDE_TEMP,
                system=_CLAUDE_SYSTEM,
                messages=[{"role": "user", "content": brief_str}],
            )
            raw = msg.content[0].text.strip()
            raw = _re2.sub(r"^```(?:json)?\s*", "", raw)
            raw = _re2.sub(r"\s*```$", "", raw)
            result = json.loads(raw)
            required = {"title", "bullet1", "bullet2", "bullet3", "pattern_text", "outlook"}
            if not required.issubset(result.keys()):
                raise ValueError("Missing keys: " + str(required - result.keys()))
            return result
        except Exception as _e:
            if attempt < max_retries:
                time.sleep(2)
            else:
                raise


def _validate_claude_output(result, brief):
    """Validate Claude output — check for bias contradictions."""
    warnings_list = []
    bias  = brief.get("bias", "")
    title = result.get("title", "").lower()
    if "Bear" in bias and any(w in title for w in ["bullish", "upward", "positive"]):
        warnings_list.append("title contradicts bearish bias")
    if "Bull" in bias and any(w in title for w in ["bearish", "downward", "negative"]):
        warnings_list.append("title contradicts bullish bias")
    return warnings_list


def _template_prose_fallback(brief):
    """Deterministic template fallback — no API needed."""
    name     = brief.get("asset_name", "Asset")
    bias     = brief.get("bias", "Neutral")
    last     = brief.get("last", 0)
    sma55_v  = brief.get("sma55", last)
    sma200_v = brief.get("sma200", last)
    above55  = "above" if last > sma55_v  else "below"
    above200 = "above" if last > sma200_v else "below"
    r1       = _fmt(brief.get("R1"))
    s1       = _fmt(brief.get("S1"))
    pat      = brief.get("pattern", "Range Consolidation")
    pconf    = brief.get("pattern_confidence", "Medium")
    rsi_v    = brief.get("rsi", 50)
    adx_v    = brief.get("adx", 20)
    trend_ph = brief.get("trend_phase", "Transition")
    div      = brief.get("divergence", "None")
    mac55    = brief.get("ma_cross_55_200", "")

    golden_death = ""
    if "Golden Cross" in (mac55 or ""):
        golden_death = (" A Golden Cross (SMA55/SMA200) has recently formed"
                        " \u2014 a classically bullish structural signal.")
    elif "Death Cross" in (mac55 or ""):
        golden_death = (" A Death Cross (SMA55/SMA200) has formed"
                        " \u2014 a bearish structural deterioration signal.")

    div_note = ""
    if "Bullish Div" in (div or ""):
        div_note = " Bullish RSI divergence detected \u2014 price made a lower low while RSI held higher."
    elif "Bearish Div" in (div or ""):
        div_note = " Bearish RSI divergence \u2014 momentum failing to confirm price highs."

    adx_str = "strong" if adx_v >= 25 else "moderate" if adx_v >= 20 else "weak"

    b1 = (name + " is trading <strong>" + above55 + " its 55-day SMA</strong> and "
          "<strong>" + above200 + " its 200-day SMA</strong>, placing the asset in a "
          "<strong>" + trend_ph + "</strong> market phase." + golden_death)

    b2 = ("RSI(9) at <strong>" + str(rsi_v) + "</strong>. ADX at " + str(round(adx_v, 0))
          + " signals " + adx_str + " directional conviction." + div_note)

    bias_dir  = "bullish" if "Bullish" in bias else "bearish"
    key_level = r1 if "Bullish" in bias else s1
    move_dir  = "above" if "Bullish" in bias else "below"

    b3 = ("<strong>R1 " + r1 + "</strong> is the immediate resistance ceiling; "
          "<strong>S1 " + s1 + "</strong> is key support. Dominant structure: "
          "<strong>" + pat + "</strong>. A confirmed " + bias_dir + " close "
          + move_dir + " " + key_level + " would confirm the next directional leg.")

    pattern_text = ("Pattern: <strong>" + pat + "</strong> (" + pconf + " confidence). "
                    "RSI(9) at " + str(rsi_v) + ", ADX " + str(round(adx_v, 0))
                    + " \u2014 " + adx_str + " conviction.")

    _phase_word = {
        "Uptrend":    "Momentum Builds",
        "Downtrend":  "Selling Pressure Mounts",
        "Transition": "At a Crossroads",
        "Ranging":    "Range-Bound",
    }.get(trend_ph, "Under Pressure")

    if "Bullish" in bias:
        _narrative_title = name + ": " + _phase_word + " \u2014 " + pat + " Pattern Active"
    elif "Bearish" in bias:
        _narrative_title = name + ": " + _phase_word + " \u2014 " + pat + " Signals Caution"
    else:
        _narrative_title = name + ": " + _phase_word + " as " + pat + " Develops"

    if bias == "Bullish":
        outlook = (
            "The technical picture for " + name + " remains constructive. "
            "Price is trading " + above55 + " its 55-day SMA and " + above200
            + " its 200-day SMA, consistent with a " + trend_ph + " phase." + golden_death + " "
            "RSI(9) at " + str(rsi_v) + " keeps momentum in constructive territory." + div_note + " "
            "A sustained close above " + r1 + " would open the next leg higher; "
            "a break below " + s1 + " would signal a near-term pullback and warrant defensive repositioning."
        )
    elif bias == "Mildly Bullish":
        outlook = (
            "Conditions for " + name + " are cautiously positive, with price " + above55
            + " the 55-day SMA in a " + trend_ph + " market phase." + golden_death + " "
            "RSI(9) at " + str(rsi_v) + " and ADX at " + str(round(adx_v)) + " suggest " + adx_str
            + " directional conviction \u2014 a strengthening trend would add conviction to the bull case." + div_note + " "
            "Watch " + r1 + " as the next resistance hurdle; a decisive close above it confirms upside continuation. "
            "Key support at " + s1 + " must hold on any pullback to preserve the constructive bias."
        )
    elif bias == "Neutral":
        outlook = (
            name + " is currently range-bound, trading " + above55 + " its 55-day SMA in a "
            + trend_ph + " phase with no clear directional conviction. "
            "RSI(9) at " + str(rsi_v) + " and ADX at " + str(round(adx_v))
            + " reflect subdued momentum \u2014 neither bulls nor bears have asserted control." + div_note + " "
            "A closing break above " + r1 + " would tip the balance toward the bulls and invite trend-following longs. "
            "Conversely, a confirmed close below " + s1 + " would shift the outlook bearish and expose lower supports."
        )
    elif bias == "Mildly Bearish":
        outlook = (
            name + " shows early signs of technical deterioration, trading " + above55
            + " its 55-day SMA in a " + trend_ph + " phase." + golden_death + " "
            "RSI(9) at " + str(rsi_v) + " is drifting into weaker territory; ADX at " + str(round(adx_v))
            + " indicates " + adx_str + " trend strength." + div_note + " "
            "Resistance at " + r1 + " is expected to cap near-term relief rallies. "
            "A close below " + s1 + " would confirm the corrective impulse and likely extend the move lower."
        )
    else:
        outlook = (
            "Technical indicators for " + name + " broadly align to the downside. "
            "Price is trading " + above55 + " its 55-day SMA and " + above200
            + " its 200-day SMA in a " + trend_ph + " phase." + golden_death + " "
            "RSI(9) at " + str(rsi_v) + " reflects weak momentum; ADX at " + str(round(adx_v))
            + " confirms " + adx_str + " bearish conviction." + div_note + " "
            "Rallies toward " + r1 + " are expected to attract selling pressure. "
            "Monitor " + s1 + " as the next downside trigger \u2014 a sustained breach would open the path to lower technical targets."
        )

    return {
        "title":        _narrative_title,
        "bullet1":      b1,
        "bullet2":      b2,
        "bullet3":      b3,
        "pattern_text": pattern_text,
        "outlook":      outlook,
    }


# ── Execute: populate claude_prose dict ───────────────────────────────────────
claude_prose = {}
_t7a_start = time.time()

for _k, _a in report_data.items():
    if _a.get("stats") is None:
        claude_prose[_k] = _template_prose_fallback({
            "asset_name": _a["meta"]["name"], "asset_type": _a["meta"]["type"],
            "bias": "Neutral", "last": 0, "rsi": 50, "adx": 20,
            "sma55": 0, "sma200": 0, "R1": None, "S1": None,
            "pattern": "N/A", "pattern_confidence": "Low",
            "trend_phase": "Transition", "divergence": "None",
            "ma_cross_55_200": "", "report_date": REPORT_DATE,
        })
        run_log["assets"][_k]["prose_source"] = "skip"
        continue
    _brief = build_claude_brief(_k, _a)
    if CLAUDE_API_KEY:
        try:
            _prose = _call_claude(_brief)
            _pw    = _validate_claude_output(_prose, _brief)
            claude_prose[_k] = _prose
            run_log["assets"][_k]["prose_source"]   = "claude"
            run_log["assets"][_k]["prose_warnings"] = _pw
            _warn_str = (" [WARN: " + str(_pw) + "]") if _pw else ""
            print("  " + _k + ": Claude prose OK" + _warn_str)
        except Exception as _ce:
            print("  " + _k + ": Claude failed (" + str(_ce) + ") - using template fallback")
            claude_prose[_k] = _template_prose_fallback(_brief)
            run_log["assets"][_k]["prose_source"]   = "fallback"
            run_log["assets"][_k]["prose_warnings"] = [str(_ce)]
    else:
        claude_prose[_k] = _template_prose_fallback(_brief)
        run_log["assets"][_k]["prose_source"]   = "template"
        run_log["assets"][_k]["prose_warnings"] = []

_n_claude   = sum(1 for v in run_log["assets"].values() if v.get("prose_source") == "claude")
_n_template = sum(1 for v in run_log["assets"].values() if v.get("prose_source") in ("template", "fallback"))
print(f"CELL 7A OK \u2014 prose generated for {len(claude_prose)} assets"
      f"  (Claude: {_n_claude}  template: {_n_template})"
      f"  {round(time.time() - _t7a_start, 1)}s")


# ═══════════════════════════════════════════════════════════════════════════════
# CELL 7 — HTML Builders
# ═══════════════════════════════════════════════════════════════════════════════

# ── Logo ──────────────────────────────────────────────────────────────────────
def _get_logo(variant="dark", height="42px"):
    """Render Alpha Bank SVG logo for the given variant and size."""
    sz = "52px" if variant in ("cover-corner",) else height
    if _LOGO_SVG_RAW:
        svg = _re.sub(r"<\?xml[^?]*\?>", "", _LOGO_SVG_RAW)
        svg = _re.sub(r"<!DOCTYPE[^>]*>",  "", svg).strip()
        if variant in ("white", "cover-corner"):
            for pair in [('#11366B', '#FFFFFF'), ('#1A1A18', '#FFFFFF'), ('#000000', '#FFFFFF'),
                         ("'#11366B'", "'#FFFFFF'"), ("'#1A1A18'", "'#FFFFFF'"), ("'#000000'", "'#FFFFFF'")]:
                svg = svg.replace('fill="' + pair[0] + '"', 'fill="' + pair[1] + '"')
                svg = svg.replace("fill='" + pair[0] + "'", "fill='" + pair[1] + "'")

        def _clean_svg_root(m):
            """Strip hardcoded w/h from SVG root and inject controlled style."""
            tag = m.group(0)
            tag = _re.sub(r'''\s+width\s*=\s*["'][^"']*["']''',  "", tag)
            tag = _re.sub(r'''\s+height\s*=\s*["'][^"']*["']''', "", tag)
            tag = _re.sub(r'''\s+style\s*=\s*["'][^"']*["']''',  "", tag)
            style_attr = ' style="height:' + sz + ';width:auto;display:block;flex-shrink:0"'
            tag = _re.sub(r"\s*/?>$", style_attr + ">", tag)
            return tag

        svg = _re.sub(r"<svg\b[^>]*>", _clean_svg_root, svg, count=1, flags=_re.DOTALL)
        return svg

    stroke = "white" if variant in ("white", "cover-corner") else "#11366B"
    text   = "white" if variant in ("white", "cover-corner") else "#11366B"
    return (
        '<svg viewBox="0 0 155 50" xmlns="http://www.w3.org/2000/svg"'
        ' style="height:' + sz + ';width:auto;display:block;flex-shrink:0">'
        '<circle cx="25" cy="25" r="19" fill="none" stroke="' + stroke + '" stroke-width="2.0"/>'
        '<line x1="25" y1="6" x2="25" y2="44" stroke="' + stroke + '" stroke-width="1.5"/>'
        '<line x1="6" y1="25" x2="44" y2="25" stroke="' + stroke + '" stroke-width="1.5"/>'
        '<text x="52" y="22" font-family="Arial,sans-serif" font-size="13"'
        ' font-weight="700" fill="' + text + '" letter-spacing="2">ALPHA</text>'
        '<text x="52" y="38" font-family="Arial,sans-serif" font-size="13"'
        ' font-weight="700" fill="' + text + '" letter-spacing="2">BANK</text>'
        '</svg>'
    )


# ── Badge helpers ─────────────────────────────────────────────────────────────
_SIG_CLASS = {
    "Bullish": "sb-bull", "Mildly Bullish": "sb-bull",
    "Neutral": "sb-neut",
    "Mildly Bearish": "sb-bear", "Bearish": "sb-bear",
    "Overbought": "sb-bear", "Oversold": "sb-bull",
    "Strong Trend": "sb-bull", "Weak Trend": "sb-neut", "No Trend": "sb-bear",
    "N/A": "sb-neut",
}

def _badge(sig):
    """Render a sentiment badge span."""
    c = _SIG_CLASS.get(sig, "sb-neut")
    return '<span class="sentiment-badge ' + c + '">' + sig + "</span>"

def _pct(v):
    """Format percentage with sign."""
    if v is None: return "\u2014"
    return ("+" if v >= 0 else "") + "{:.2f}%".format(v)

def _pct_col(v):
    """Green for positive, red for negative."""
    return "#15803d" if (v or 0) >= 0 else "#b91c1c"


# ── Strength dots helper (Fix 32) ─────────────────────────────────────────────
def _strength_dots(score):
    """Render filled/empty dots for S/R strength (3 = high, 2 = med, 1 = low)."""
    if score is None:
        return ""
    filled = min(max(int(round(score)), 1), 3)
    dots   = ""
    for i in range(3):
        color = "#ef4444" if i < filled else "#e5e7eb"
        dots += '<span style="color:' + color + ';font-size:9px">●</span>'
    label = {3: "High", 2: "Med", 1: "Low"}.get(filled, "")
    return '<span style="font-size:9px;margin-left:3px;color:#6b7280">' + label + "</span>" + dots


# ── Performance Scorecard — 2-col: Period + Return only (Fix 31) ──────────────
def _perf_scorecard_html(s):
    """Multi-period performance scorecard. Period + Return columns only (no Signal)."""
    perf = s.get("bql_perf", {})
    rows_data = [
        ("1 Week",   perf.get("r1w")),
        ("1 Month",  perf.get("r1m")),
        ("3 Months", perf.get("r3m")),
        ("6 Months", perf.get("r6m")),
        ("YTD",      perf.get("rytd")),
        ("1 Year",   perf.get("r1y")),
    ]
    rows_html = ""
    for i, (label, val) in enumerate(rows_data):
        bg      = "white" if i % 2 == 0 else "#f9fafb"
        ret_str = (("+" if val >= 0 else "") + "{:.2f}%".format(val)
                   if val is not None else "\u2014")
        col = ("#15803d" if val is not None and val >= 0
               else "#b91c1c" if val is not None else "#6b7280")
        rows_html += (
            '<tr style="background:' + bg + '">'
            '<td style="padding:2px 8px;font-size:10.5px;font-weight:500">' + label + "</td>"
            '<td style="padding:2px 8px;text-align:right;font-size:10.5px;'
            'font-variant-numeric:tabular-nums;font-weight:700;color:' + col + '">'
            + ret_str + "</td></tr>"
        )
    h52  = perf.get("high52")
    l52  = perf.get("low52")
    close = s["last"]
    if h52:
        gap = round((close / h52 - 1) * 100, 2)
        col = "#b91c1c" if gap < 0 else "#15803d"
        rows_html += (
            '<tr style="background:#fff7f0;border-top:1px solid #fed7aa">'
            '<td style="padding:2px 8px;font-size:10.5px;font-weight:500;color:#92400e">52W High</td>'
            '<td style="padding:2px 8px;text-align:right;font-size:10px;font-weight:700">'
            + _fmt(h52) + " <span style=\"color:" + col + ";font-size:9px\">"
            + ("+" if gap >= 0 else "") + str(gap) + "%</span></td></tr>"
        )
    if l52:
        gap = round((close / l52 - 1) * 100, 2)
        rows_html += (
            '<tr style="background:#f0fdf4">'
            '<td style="padding:2px 8px;font-size:10.5px;font-weight:500;color:#166534">52W Low</td>'
            '<td style="padding:2px 8px;text-align:right;font-size:10px;font-weight:700">'
            + _fmt(l52) + " <span style=\"color:#15803d;font-size:9px\">"
            + "+" + str(gap) + "%</span></td></tr>"
        )
    return (
        '<div style="margin-top:6px">'
        '<h2 style="font-size:10.5px;font-weight:700;color:#11366B;margin-bottom:3px;'
        'text-transform:uppercase;letter-spacing:.04em">Performance Scorecard</h2>'
        '<div class="tbl-wrap">'
        '<table style="width:100%;border-collapse:collapse;font-size:10.5px">'
        '<thead><tr style="background:#11366B">'
        '<th style="padding:4px 8px;color:rgba(255,255,255,.92);font-size:10px;'
        'text-align:left;font-weight:600">Period</th>'
        '<th style="padding:4px 8px;color:rgba(255,255,255,.92);font-size:10px;'
        'text-align:right;font-weight:600">Return</th>'
        "</tr></thead>"
        "<tbody>" + rows_html + "</tbody>"
        "</table></div></div>"
    )


# ── Market Structure Intelligence Matrix (Fix 34-35) ─────────────────────────
def _market_structure_html(s):
    """Market structure intelligence panel with true weekly signals and bars_ago labels."""
    ms = s.get("market_structure", {})
    if not ms:
        return ""

    def msbadge(lbl, cls):
        """Market-structure badge."""
        css = {"bull": "sb-bull", "bear": "sb-bear", "neut": "sb-neut"}.get(cls, "sb-neut")
        return '<span class="sentiment-badge ' + css + '">' + lbl + "</span>"

    def pct_cell(v):
        """Pct value with colour."""
        col = "#15803d" if v >= 0 else "#b91c1c"
        return ('<span style="font-size:10.5px;font-weight:700;color:' + col + '">'
                + ("+" if v >= 0 else "") + str(v) + "%</span>")

    def _ago_suffix(n):
        """Format bars-ago suffix string."""
        if n is None:
            return ""
        return " <span style=\"font-size:9px;color:#9ca3af\">(" + str(n) + "d ago)</span>"

    tp   = ms.get("trend_phase",    ("\u2014", "neut"))
    vr   = ms.get("vol_regime",     ("\u2014", "neut"))
    mc   = ms.get("macd_cross",     ("\u2014", "neut"))
    mac  = ms.get("ma_cross_55_200", ("\u2014", "neut", None))
    mac2 = ms.get("ma_cross_21_55",  ("\u2014", "neut", None))
    div  = ms.get("divergence",     ("None", "neut"))
    bbp  = ms.get("bb_position",    ("\u2014", "neut"))
    g55  = ms.get("gap_sma55",  0)
    g200 = ms.get("gap_sma200", 0)

    # Weekly signals (Fix 34)
    weekly = s.get("weekly_indicators", {})
    def _w_badge(wkey, sig_fn, *args):
        """Weekly badge from true weekly indicator or N/A."""
        val = weekly.get(wkey)
        if val is None:
            return '<span class="sentiment-badge sb-neut">N/A</span>'
        return msbadge(sig_fn(val, *args), "neut")

    rows = [
        ("Trend Phase",            msbadge(tp[0], tp[1])),
        ("Volatility Regime",      msbadge(vr[0], vr[1])),
        ("MACD Signal",            msbadge(mc[0], mc[1])),
        ("MA Cross 55/200",        msbadge(mac[0],  mac[1]) + _ago_suffix(mac[2] if len(mac) > 2 else None)),
        ("MA Cross 21/55",         msbadge(mac2[0], mac2[1]) + _ago_suffix(mac2[2] if len(mac2) > 2 else None)),
        ("RSI Divergence",         msbadge(div[0], div[1])),
        ("BB Position",            msbadge(bbp[0], bbp[1])),
        ("Gap vs SMA 55",          pct_cell(g55)),
        ("Gap vs SMA 200",         pct_cell(g200)),
    ]
    rows_html = ""
    for i, (label, val_html) in enumerate(rows):
        bg = "white" if i % 2 == 0 else "#f9fafb"
        rows_html += (
            '<tr style="background:' + bg + '">'
            '<td style="padding:1px 6px;font-size:10px;font-weight:500;color:#374151">'
            + label + "</td>"
            '<td style="padding:1px 6px;text-align:right">' + val_html + "</td>"
            "</tr>"
        )
    return (
        '<div style="margin-top:6px">'
        '<h2 style="font-size:10.5px;font-weight:700;color:#11366B;margin-bottom:3px;'
        'text-transform:uppercase;letter-spacing:.04em">Market Structure Intelligence</h2>'
        '<div class="tbl-wrap">'
        '<table style="width:100%;border-collapse:collapse">'
        '<thead><tr style="background:#11366B">'
        '<th style="padding:4px 8px;color:rgba(255,255,255,.92);font-size:10px;'
        'text-align:left;font-weight:600">Signal</th>'
        '<th style="padding:4px 8px;color:rgba(255,255,255,.92);font-size:10px;'
        'text-align:right;font-weight:600">Reading</th>'
        "</tr></thead>"
        "<tbody>" + rows_html + "</tbody>"
        "</table></div></div>"
    )


# ── Footer ────────────────────────────────────────────────────────────────────
def _footer(pnum):
    """Render slide footer."""
    return (
        '<div class="footer">'
        "<span>Source: Global Markets Analysis, Bloomberg</span>"
        "<span>" + str(pnum) + "</span></div>"
    )


# ── Sentiment phrase ───────────────────────────────────────────────────────────
def _sentiment_phrase(bias):
    """Return institutional sentiment phrase for given bias."""
    return {
        "Bullish":        "Constructive, Upward Momentum",
        "Mildly Bullish": "Cautiously Positive Tone",
        "Neutral":        "Directional Ambiguity \u2014 Range-Bound",
        "Mildly Bearish": "Corrective Pressure Building",
        "Bearish":        "Downward Sentiment Prevailing",
    }.get(bias, "Mixed Technical Signals")


# ── Page 1 ────────────────────────────────────────────────────────────────────
def _page1(key, asset, pnum):
    """Render Page 1 slide — chart + accent band + narrative bullets."""
    s    = asset["stats"]
    ch   = asset.get("chart_b64")
    name = asset["meta"]["name"]
    atype = asset["meta"]["type"]
    prose = claude_prose.get(key, {})

    # Use Claude/template prose if available, else fall back to _bullets logic
    if prose:
        b1 = prose.get("bullet1", "")
        b2 = prose.get("bullet2", "")
        b3 = prose.get("bullet3", "")
        title_str = prose.get("title", name + ": " + s["pattern"])
    else:
        b1 = b2 = b3 = ""
        title_str = name + ": " + s["pattern"]

    mc = _pct_col(s["mtd"]); yc = _pct_col(s["ytd"]); m12c = _pct_col(s["12m"])

    def pill(lbl, val, col=None):
        """Render a stat pill."""
        vc = (' style="font-weight:700;color:' + col + '"' if col else ' style="font-weight:700"')
        return (
            '<span class="stat-pill">'
            '<span class="pill-lbl">' + lbl + "</span>"
            "<span" + vc + ">" + val + "</span></span>"
        )

    _roc = s.get("price_roc_20", 0) or 0
    pills = (
        pill("Last",    _fmt(s["last"]))
        + pill("MTD",   _pct(s["mtd"]),  mc)
        + pill("YTD",   _pct(s["ytd"]),  yc)
        + pill("12M",   _pct(s["12m"]),  m12c)
        + pill("ROC(20)", _pct(_roc), "#15803d" if _roc >= 0 else "#b91c1c")
        + pill("RSI (9)", str(s["rsi_val"]))
    )

    chart_img = (
        '<img src="data:image/png;base64,' + ch
        + '" style="width:100%;height:100%;object-fit:contain;display:block"/>'
        if ch else
        '<div style="text-align:center;padding:40px 0;color:#9ca3af;font-size:13px">'
        "Chart unavailable</div>"
    )

    # Accent band (Fix 27)
    accent = _accent_band(atype, REPORT_DATE)

    return (
        '<div class="slide-content" id="s-' + key + '-1">'
        '<div class="p1-hdr">'
        "<div>"
        '<div style="display:flex;align-items:baseline;gap:12px;flex-wrap:wrap">'
        '<h1 class="slide-title">' + title_str + "</h1>"
        '<span style="font-size:11px;font-weight:600;color:#64748b;'
        'letter-spacing:.02em;white-space:nowrap">'
        + _sentiment_phrase(s["overall_bias"]) + "</span>"
        "</div>"
        '<div class="pills-row">' + pills + "</div>"
        "</div>"
        "<div>" + _get_logo("dark", "40px") + "</div>"
        "</div>"
        + accent
        + '<div class="divider"></div>'
        '<div style="flex:0 0 370px;min-height:0;overflow:hidden;margin-bottom:6px">'
        + chart_img + "</div>"
        '<div class="analysis">'
        '<div class="ai"><span class="bullet">&#9658;</span><p>' + b1 + "</p></div>"
        '<div class="ai"><span class="bullet">&#9658;</span><p>' + b2 + "</p></div>"
        '<div class="ai"><span class="bullet">&#9658;</span><p>' + b3 + "</p></div>"
        "</div>"
        + _footer(pnum) + "</div>"
    )


# ── Fibonacci key levels table for S/R panel ──────────────────────────────────
def _fib_table_html(s):
    """Compact Fibonacci level band (61.8%, 50%, 38.2%) for the S/R panel on Page 2."""
    fibs  = s.get("fib_levels", {})
    close = s.get("last", 0) or 1
    if not fibs:
        return ""
    key_levels = [
        ("61.8%", fibs.get("61.8%"), "#92400e"),
        ("50%",   fibs.get("50%"),   "#7c3aed"),
        ("38.2%", fibs.get("38.2%"), "#f97316"),
    ]
    rows_html = ""
    for lbl, val, col in key_levels:
        if val is None:
            continue
        rel     = round((float(val) / close - 1) * 100, 2)
        rel_str = ("+" if rel >= 0 else "") + f"{rel:.2f}%"
        rel_col = "#15803d" if rel >= 0 else "#b91c1c"
        rows_html += (
            f'<tr>'
            f'<td style="padding:1px 5px;font-size:9.5px;color:{col};font-weight:700">{lbl}</td>'
            f'<td style="padding:1px 5px;font-size:9.5px;text-align:right;font-weight:600">{_fmt(val)}</td>'
            f'<td style="padding:1px 5px;font-size:9.5px;text-align:right;color:{rel_col}">{rel_str}</td>'
            f'</tr>'
        )
    if not rows_html:
        return ""
    return (
        '<div style="margin-top:4px;border-top:1px solid #e5e7eb;padding-top:3px">'
        '<p style="font-size:9px;font-weight:700;color:#6b7280;text-transform:uppercase;'
        'letter-spacing:.04em;margin-bottom:2px">Key Fib Levels</p>'
        '<table style="width:100%;border-collapse:collapse">'
        + rows_html +
        '</table></div>'
    )


# ── Page 2 ────────────────────────────────────────────────────────────────────
def _page2(key, asset, pnum):
    """Render Page 2 slide — indicator matrix + S/R levels + pattern + outlook."""
    s    = asset["stats"]
    name = asset["meta"]["name"]
    bias = s["overall_bias"]
    prose = claude_prose.get(key, {})

    # Indicator signal rows (Fix 36: RSI (9) key)
    sig_rows = ""
    for i, (ind, tf) in enumerate(s["signals"].items()):
        bg = "white" if i % 2 == 0 else "#f9fafb"
        daily_v  = tf.get("daily",  "")
        weekly_v = tf.get("weekly", "")
        # N/A weekly gets grey badge
        w_badge = (_badge(weekly_v) if weekly_v != "N/A"
                   else '<span class="sentiment-badge sb-neut">N/A</span>')
        sig_rows += (
            '<tr style="background:' + bg + '">'
            '<td style="padding:3px 8px;font-size:10.5px;font-weight:500">' + ind + "</td>"
            '<td class="tc" style="padding:3px 8px">' + _badge(daily_v) + "</td>"
            '<td class="tc" style="padding:3px 8px">' + w_badge + "</td>"
            "</tr>"
        )

    # Resistance rows — always render 3, show "—" for None
    res_rows = ""
    _ress = list(s["resistances"]) + [None, None, None]
    for i in range(3):
        r = _ress[i]
        val_str = _fmt(r) if r is not None else "&mdash;"
        res_rows += (
            '<div class="level-item level-res">'
            '<span>R' + str(i + 1) + ":</span>"
            '<span class="lv">' + val_str + "</span>"
            "</div>"
        )

    # Support rows — always render 3, show "—" for None
    sup_rows = ""
    _sups = list(s["supports"]) + [None, None, None]
    for i in range(3):
        sv = _sups[i]
        val_str = _fmt(sv) if sv is not None else "&mdash;"
        sup_rows += (
            '<div class="level-item level-sup">'
            '<span>S' + str(i + 1) + ":</span>"
            '<span class="lv">' + val_str + "</span>"
            "</div>"
        )

    # Pattern text from prose
    pat_text = prose.get("pattern_text", s.get("pattern_desc", ""))
    if not pat_text:
        pconf = s.get("pattern_confidence", "Medium")
        pat_text = ("Pattern: <strong>" + s["pattern"] + "</strong> (" + pconf + " confidence). "
                    "RSI(9) at " + str(s["rsi_val"]) + ", ADX at " + str(s["adx_val"]) + ".")

    # Total Outlook — from prose or shared template fallback
    outlook = prose.get("outlook", "")
    if not outlook:
        ms = s.get("market_structure", {})
        _fb = _template_prose_fallback({
            "asset_name": name, "asset_type": asset["meta"]["type"],
            "bias": bias,
            "last": s["last"], "sma55": s["sma55"], "sma200": s["sma200"],
            "rsi": s["rsi_val"], "adx": s["adx_val"],
            "R1": s["resistances"][0] if s["resistances"] else None,
            "S1": s["supports"][0] if s["supports"] else None,
            "pattern": s["pattern"],
            "pattern_confidence": s.get("pattern_confidence", "Medium"),
            "trend_phase": ms.get("trend_phase", ("Transition",))[0] if ms.get("trend_phase") else "Transition",
            "divergence": ms.get("divergence", ("None",))[0] if ms.get("divergence") else "None",
            "ma_cross_55_200": ms.get("ma_cross_55_200", ("",))[0] if ms.get("ma_cross_55_200") else "",
            "report_date": REPORT_DATE,
        })
        outlook = _fb.get("outlook", "")

    return (
        '<div class="slide-content" id="s-' + key + '-2">'
        '<div class="p2-hdr">'
        '<h1 class="slide-title">' + name + ": Technical Analysis Summary</h1>"
        "<div>" + _get_logo("dark", "40px") + "</div>"
        "</div>"
        '<div class="divider"></div>'
        '<div class="p2-body">'

        # Left panel (Fix 37: no overflow-y)
        '<div class="left-panel">'
        '<h2 class="section-title">Indicator Sentiment Matrix</h2>'
        '<div class="tbl-wrap"><table>'
        "<thead><tr>"
        "<th>Indicator</th>"
        '<th class="tc">Daily</th>'
        '<th class="tc">Weekly</th>'
        "</tr></thead>"
        "<tbody>" + sig_rows + "</tbody>"
        "</table></div>"
        '<div class="legend">'
        '<div class="li"><span class="lb lb-bull"></span><span>Bullish</span></div>'
        '<div class="li"><span class="lb lb-neut"></span><span>Neutral</span></div>'
        '<div class="li"><span class="lb lb-bear"></span><span>Bearish</span></div>'
        "</div>"
        + _perf_scorecard_html(s)
        + _market_structure_html(s)
        + "</div>"

        # Right panel
        '<div class="right-panel">'

        '<div class="ibox box-pat">'
        '<h3>Pattern Breakdown</h3>'
        '<div style="font-size:11.5px;font-weight:700;color:#1e40af;margin-bottom:4px">'
        + s["pattern"]
        + ' <span style="font-size:9.5px;font-weight:600;color:#6b7280;background:#f1f5f9;'
        'padding:1px 5px;border-radius:3px;border:1px solid #e2e8f0">'
        + s.get("pattern_confidence", "Medium") + "</span></div>"
        '<p class="dtext">' + pat_text + "</p>"
        "</div>"

        '<div class="ibox box-lvl">'
        '<h3>Key Resistance &amp; Support Levels</h3>'
        '<div class="lvl-sec">'
        '<p class="lvl-title res-title">RESISTANCE</p>' + res_rows + "</div>"
        '<div class="lvl-sec" style="border-top:1px solid #d1d5db;padding-top:4px">'
        '<p class="lvl-title sup-title">SUPPORT</p>' + sup_rows + "</div>"
        + _fib_table_html(s) +
        "</div>"

        # Total Outlook — plain text, no header badge
        '<div class="ibox box-outlook">'
        "<h3>Total Outlook</h3>"
        '<p class="dtext">' + outlook + "</p>"
        "</div>"

        "</div>"  # right-panel
        "</div>"  # p2-body
        + _footer(pnum) + "</div>"
    )


# ── Warning slide (for failed assets) ─────────────────────────────────────────
def _warning_slide(key, asset, pnum):
    """Render a placeholder slide for assets that failed to load."""
    name  = asset["meta"]["name"]
    err   = asset.get("error", "Unknown error")
    return (
        '<div class="slide-content" id="s-' + key + '-1">'
        '<div class="p1-hdr">'
        '<h1 class="slide-title" style="color:#b91c1c">' + name + ": Data Unavailable</h1>"
        "<div>" + _get_logo("dark", "40px") + "</div>"
        "</div>"
        '<div class="divider"></div>'
        '<div style="flex:1;display:flex;align-items:center;justify-content:center;'
        'flex-direction:column;gap:12px">'
        '<div style="font-size:48px">\u26a0\ufe0f</div>'
        '<div style="font-size:14px;color:#374151;font-weight:600">'
        "Data could not be loaded for this asset</div>"
        '<div style="font-size:11.5px;color:#6b7280;max-width:500px;text-align:center">'
        + str(err) + "</div>"
        "</div>"
        + _footer(pnum) + "</div>"
    )


# ── Cover ─────────────────────────────────────────────────────────────────────
def _cover():
    """Render cover slide — white background, bank-style design."""
    names = " &middot; ".join(a["meta"]["name"] for a in report_data.values())

    # Drafting compass + ruler illustration — geometric, linear, no loops.
    # Compass pivot at (310, 210); two legs spread open; arc mid-draw.
    # Ruler sits across the bottom of the art area.
    # All elements: stroke navy, no fill (except ruler body faint tint).
    # Nothing extends above y=120 — logo safe zone preserved.
    _art_div = (
        '<div style="position:absolute;top:0;right:0;width:620px;height:720px;'
        'overflow:hidden;pointer-events:none;z-index:0">'
        '<svg viewBox="0 0 620 720" width="620" height="720" '
        'xmlns="http://www.w3.org/2000/svg" fill="none" stroke="#11366B" '
        'stroke-linecap="round" stroke-linejoin="round">'

        # — Compass hinge (pivot circle) —
        '<circle cx="310" cy="210" r="5" stroke-width="1.5" opacity="0.45"/>'

        # — Left leg: hinge → sharp point (needle tip) —
        '<line x1="310" y1="210" x2="185" y2="490" stroke-width="1.5" opacity="0.45"/>'
        # Diamond needle tip at (185, 490)
        '<polygon points="185,482 189,490 185,498 181,490" stroke-width="1.3" opacity="0.45"/>'

        # — Right leg: hinge → pencil tip —
        '<line x1="310" y1="210" x2="440" y2="490" stroke-width="1.5" opacity="0.45"/>'
        # Pencil tip triangle at (440, 490)
        '<polygon points="433,482 447,482 440,500" stroke-width="1.3" opacity="0.45"/>'
        # Pencil ferrule line
        '<line x1="434" y1="484" x2="446" y2="484" stroke-width="1.0" opacity="0.35"/>'

        # — Cross-piece (hinge bar connecting legs at ~y=295) —
        # Left leg at y=295: parametric t=(295-210)/(490-210)=85/280≈0.304
        #   x = 310 + (185-310)*0.304 = 310 - 38 = 272
        # Right leg at y=295: x = 310 + (440-310)*0.304 = 310 + 39.5 ≈ 350
        '<line x1="272" y1="295" x2="350" y2="295" stroke-width="1.4" opacity="0.40"/>'
        # Small pivot caps at the leg intersections
        '<circle cx="272" cy="295" r="3" stroke-width="1.2" opacity="0.38"/>'
        '<circle cx="350" cy="295" r="3" stroke-width="1.2" opacity="0.38"/>'

        # — Arc drawn by pencil tip (compass mid-draw) —
        # Center (310,210), radius ≈ 293 (distance to pencil tip at 440,490)
        # Arc sweeps ~35° from pencil tip clockwise toward needle tip zone
        # pencil tip angle: atan2(490-210, 440-310)=atan2(280,130)≈65° → start ~65°
        # end ~100° (leftward) — SVG arc: large-arc=0 sweep=1
        '<path d="M 440,490 A 293,293 0 0 1 258,497" stroke-width="1.6" opacity="0.50"/>'

        # — Ruler (bottom of art area, y=612–636) —
        # Body rectangle
        '<rect x="40" y="612" width="540" height="24" rx="2" '
        'fill="#11366B" fill-opacity="0.04" stroke-width="1.4" opacity="0.42"/>'
        # Major ticks (7 divisions, every 77px starting at x=40)
        '<line x1="117" y1="612" x2="117" y2="600" stroke-width="1.3" opacity="0.42"/>'
        '<line x1="194" y1="612" x2="194" y2="600" stroke-width="1.3" opacity="0.42"/>'
        '<line x1="271" y1="612" x2="271" y2="600" stroke-width="1.3" opacity="0.42"/>'
        '<line x1="348" y1="612" x2="348" y2="600" stroke-width="1.3" opacity="0.42"/>'
        '<line x1="425" y1="612" x2="425" y2="600" stroke-width="1.3" opacity="0.42"/>'
        '<line x1="502" y1="612" x2="502" y2="600" stroke-width="1.3" opacity="0.42"/>'
        # Minor ticks (every ~19px between major ticks — 4 minor per division)
        + "".join(
            f'<line x1="{40 + 77*i + 19*j}" y1="612" '
            f'x2="{40 + 77*i + 19*j}" y2="607" stroke-width="0.9" opacity="0.30"/>'
            for i in range(7) for j in range(1, 4)
        ) +
        # Major tick numbers 1–7
        '<text x="117" y="598" text-anchor="middle" font-size="8" opacity="0.40" '
        'fill="#11366B" stroke="none">1</text>'
        '<text x="194" y="598" text-anchor="middle" font-size="8" opacity="0.40" '
        'fill="#11366B" stroke="none">2</text>'
        '<text x="271" y="598" text-anchor="middle" font-size="8" opacity="0.40" '
        'fill="#11366B" stroke="none">3</text>'
        '<text x="348" y="598" text-anchor="middle" font-size="8" opacity="0.40" '
        'fill="#11366B" stroke="none">4</text>'
        '<text x="425" y="598" text-anchor="middle" font-size="8" opacity="0.40" '
        'fill="#11366B" stroke="none">5</text>'
        '<text x="502" y="598" text-anchor="middle" font-size="8" opacity="0.40" '
        'fill="#11366B" stroke="none">6</text>'

        '</svg></div>'
    )

    return (
        '<div class="slide-content cover-slide" id="s-cover">'
        + _art_div +
        '<div style="position:absolute;top:20px;right:40px;z-index:2">'
        + _get_logo("dark", "40px") +
        "</div>"
        '<div class="cover-inner">'
        '<div style="width:50px;height:3px;background:#11366B;margin-bottom:18px"></div>'
        '<div style="font-size:11px;letter-spacing:.16em;text-transform:uppercase;'
        'color:#6b7280;margin-bottom:11px;font-weight:600">'
        "Global Markets Analysis</div>"
        "<h1 style=\"font-family:Georgia,'Times New Roman',serif;font-size:50px;"
        'font-weight:700;color:#11366B;line-height:1.1;margin-bottom:16px">'
        "Cross Asset<br>Technical Vista</h1>"
        '<div style="font-size:15px;color:#4b5563;font-weight:400;'
        'letter-spacing:.02em;margin-bottom:32px">' + REPORT_DATE + "</div>"
        '<div style="width:50px;height:3px;background:#11366B;margin-bottom:14px"></div>'
        '<p style="font-size:11.5px;color:#6b7280;line-height:1.9">' + names + "</p>"
        "</div></div>"
    )


def _build_summary_text(key, s, p):
    """Build a unique, data-rich 2-sentence asset summary from live stats."""
    if p.get("outlook"):
        return p["outlook"]
    last   = s.get("last", 0)
    rsi    = s.get("rsi_val", 50)
    adx    = s.get("adx_val", 20)
    bias   = s.get("overall_bias", "Neutral")
    pat    = s.get("pattern", "Range Consolidation")
    r1     = _fmt((s.get("resistances") or [None])[0])
    s1     = _fmt((s.get("supports") or [None])[0])
    sma55  = s.get("sma55", last)
    above  = "above" if last > sma55 else "below"
    adx_q  = "strong" if adx >= 25 else "moderate" if adx >= 18 else "weak"
    rsi_q  = "overbought" if rsi >= 70 else "oversold" if rsi <= 30 else f"at {rsi}"
    ms     = s.get("market_structure", {})
    div_lbl = ((ms.get("divergence") or ("None",))[0]
               if ms.get("divergence") else "None")
    div_note = (" Bullish RSI divergence supports recovery." if "Bullish Div" in div_lbl
                else " Bearish RSI divergence adds downside risk." if "Bearish Div" in div_lbl
                else "")
    return (
        f"{bias} \u2014 {pat} with {adx_q} trend conviction (ADX {round(adx)}). "
        f"Price {above} SMA55; RSI(9) {rsi_q}.{div_note} "
        f"Watch R1 {r1} / S1 {s1} for the next directional trigger."
    )


def _summary_slide(pnum):
    """Render final summary slide — one row per asset with bias + outlook sentence."""
    rows = ""
    for _k, _a in report_data.items():
        _s    = _a.get("stats") or {}
        _p    = claude_prose.get(_k, {})
        _bias = _s.get("overall_bias", "N/A")
        _bc   = "#15803d" if "Bullish" in _bias else "#b91c1c" if "Bearish" in _bias else "#92400e"
        _name = ASSETS[_k]["name"] if isinstance(ASSETS.get(_k), dict) else _a["meta"]["name"]
        _out  = _build_summary_text(_k, _s, _p)
        rows += (
            f'<tr>'
            f'<td style="font-weight:700;padding:12px 14px;white-space:nowrap;'
            f'font-size:14px;vertical-align:top">'
            f'{_name}</td>'
            f'<td style="padding:12px 14px;vertical-align:top">'
            f'<span style="color:{_bc};font-weight:600;font-size:13.5px">{_bias}</span></td>'
            f'<td style="padding:12px 14px;font-size:13px;line-height:1.5;vertical-align:top">'
            f'{_out}</td>'
            f'</tr>'
        )
    table = (
        '<table style="width:100%;border-collapse:collapse">'
        '<thead><tr style="background:#11366B">'
        '<th style="text-align:left;padding:9px 14px;color:white;font-size:12px;width:120px">'
        'Asset</th>'
        '<th style="text-align:left;padding:9px 14px;color:white;font-size:12px;width:170px">'
        'Bias</th>'
        '<th style="text-align:left;padding:9px 14px;color:white;font-size:12px">'
        'Outlook Summary</th>'
        '</tr></thead>'
        '<tbody>' + rows + '</tbody>'
        '</table>'
    )
    return (
        '<div class="slide-content" id="s-summary">'
        '<div class="p2-hdr">'
        f'<h1 class="slide-title">Cross-Asset Outlook Summary \u2014 {REPORT_DATE}</h1>'
        '<div>' + _get_logo("dark", "40px") + '</div>'
        '</div>'
        '<div class="divider"></div>'
        '<div style="flex:1;min-height:0;overflow:hidden;padding:8px 0">'
        + table +
        '</div>'
        + _footer(pnum) + '</div>'
    )


# ── Assemble all slides ───────────────────────────────────────────────────────
_slides = [_cover()]
_ids    = ["s-cover"]
_pnum   = 2

for _k, _a in report_data.items():
    if _a.get("stats") is None:
        # Warning slide (1 page for failed asset)
        _slides.append(_warning_slide(_k, _a, _pnum))
        _ids.append("s-" + _k + "-1")
        _pnum += 1
    else:
        _slides.append(_page1(_k, _a, _pnum)); _ids.append("s-" + _k + "-1"); _pnum += 1
        _slides.append(_page2(_k, _a, _pnum)); _ids.append("s-" + _k + "-2"); _pnum += 1

_slides.append(_summary_slide(_pnum)); _ids.append("s-summary"); _pnum += 1

_slides_html = "\n".join(_slides)
_ids_js      = json.dumps(_ids)
_assets_js   = json.dumps([{"key": k, "name": v["meta"]["name"]} for k, v in report_data.items()])

print("CELL 7 OK \u2014 " + str(len(_ids)) + " slides assembled")


# ═══════════════════════════════════════════════════════════════════════════════
# CELL 8 — Final Assembly: CSS + JS + HTML → versioned file + smoke test
# ═══════════════════════════════════════════════════════════════════════════════

_CSS = """
* { margin: 0; padding: 0; box-sizing: border-box; }

body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto,
               'Helvetica Neue', Arial, sans-serif;
  background: #f3f4f6;
  padding: 20px;
}
.container { max-width: 1400px; margin: 0 auto; }

/* Slide wrapper — scrollable in browser, paginated in print */
.slide-container {
  width: 1280px; height: auto; margin: 0 auto; overflow: visible;
}

/* Slide base */
.slide-content {
  width: 1280px; height: 720px; padding: 14px 24px 10px;
  display: flex; flex-direction: column; overflow: hidden;
  background: white; margin-bottom: 20px;
  box-shadow: 0 20px 25px -5px rgba(0,0,0,.12), 0 8px 10px -6px rgba(0,0,0,.06);
}

/* Page 1 header */
.p1-hdr {
  display: flex; justify-content: space-between; align-items: flex-start;
  margin-bottom: 4px; flex-shrink: 0;
}
.slide-title {
  font-size: 19px; font-weight: 700; color: #11366B; line-height: 1.2;
  max-width: 870px;
}
.divider { border-top: 2px solid #d1d5db; margin-bottom: 6px; flex-shrink: 0; }

/* Accent band */
.accent-band {
  display: flex; align-items: center; justify-content: space-between;
  margin-bottom: 4px; flex-shrink: 0;
  overflow: hidden; border-radius: 4px;
}

/* Stat pills */
.pills-row { display: flex; gap: 5px; flex-wrap: wrap; margin-top: 4px; }
.stat-pill {
  display: inline-flex; align-items: center; gap: 4px;
  background: #f9fafb; border: 1px solid #d1d5db; border-radius: 6px;
  padding: 2px 8px; font-size: 11px; white-space: nowrap;
}
.pill-lbl { color: #6b7280; font-weight: 400; margin-right: 1px; }

/* Analysis bullets */
.analysis { display: flex; flex-direction: column; gap: 3px; flex-shrink: 0; }
.ai { display: flex; align-items: flex-start; gap: 7px; }
.bullet { color: #11366B; font-size: 11px; flex-shrink: 0; margin-top: 3px; }
.analysis p { font-size: 13px; line-height: 1.55; color: #374151; }

/* Footer */
.footer {
  display: flex; justify-content: space-between; align-items: center;
  margin-top: auto; padding-top: 4px; border-top: 1px solid #d1d5db;
  font-size: 11px; color: #6b7280; flex-shrink: 0;
}

/* Page 2 */
.p2-hdr {
  display: flex; justify-content: space-between; align-items: center;
  margin-bottom: 5px; flex-shrink: 0;
}
/* Fix 37: remove overflow-y from left-panel */
.p2-body { display: flex; gap: 12px; flex: 1; min-height: 0; overflow: hidden; }
.left-panel  { flex: 1; min-width: 0; overflow: hidden; }
.right-panel {
  width: 294px; flex-shrink: 0; display: flex; flex-direction: column;
  gap: 6px; min-height: 0; overflow: hidden;
}
.section-title {
  font-size: 11.5px; font-weight: 700; color: #11366B;
  margin-bottom: 4px; letter-spacing:.02em; text-transform:uppercase;
}

/* Tables */
.tbl-wrap {
  border: 1px solid #d1d5db; border-radius: 8px; overflow: hidden;
  box-shadow: 0 1px 3px rgba(0,0,0,.07);
}
table { width: 100%; border-collapse: collapse; }
thead { background: #11366B; color: white; }
th { padding: 4px 8px; text-align: left; font-size: 10.5px; font-weight: 600; }
th.tc, td.tc { text-align: center; }
tbody tr:nth-child(even) { background: #f9fafb; }
tbody tr:nth-child(odd)  { background: white; }
/* Fix 37: reduce row padding for no-scroll */
td { padding: 1px 6px; font-size: 10.5px; border-bottom: 1px solid #e5e7eb; }

/* Sentiment badges */
.sentiment-badge {
  display: inline-block; padding: 1px 6px; border-radius: 9999px;
  font-size: 10px; font-weight: 600; border: 1px solid;
}
.sb-bull { background: #dcfce7; color: #166534; border-color: #86efac; }
.sb-bear { background: #fee2e2; color: #991b1b; border-color: #fca5a5; }
.sb-neut { background: #fefce8; color: #854d0e; border-color: #fde047; }

/* Legend */
.legend { display: flex; gap: 10px; margin-top: 4px; font-size: 10.5px; }
.li { display: flex; align-items: center; gap: 6px; }
.lb { width: 12px; height: 12px; border-radius: 2px; border: 1px solid; }
.lb-bull { background: #dcfce7; border-color: #86efac; }
.lb-neut { background: #fefce8; border-color: #fde047; }
.lb-bear { background: #fee2e2; border-color: #fca5a5; }

/* Right panel boxes */
.ibox {
  border: 2px solid; border-radius: 8px; padding: 7px 9px;
  flex: 1; min-height: 0; overflow: hidden;
  box-shadow: 0 1px 3px rgba(0,0,0,.07);
}
.ibox h3 { font-size: 11px; font-weight: 700; color: #11366B; margin-bottom: 4px; }
.box-pat     { background: #eff6ff; border-color: #11366B; }
.box-lvl     { background: #f9fafb; border-color: #d1d5db; }
.box-outlook { background: #f8fafc; border-color: #cbd5e1; }

.dtext { font-size: 11px; color: #374151; line-height: 1.42; }

/* Level items */
.lvl-sec { margin-bottom: 4px; }
.lvl-title {
  font-size: 10px; font-weight: 700; text-transform: uppercase;
  letter-spacing: .5px; margin-bottom: 2px;
}
.res-title { color: #b91c1c; }
.sup-title { color: #15803d; }
.level-item {
  display: flex; justify-content: space-between; align-items: center;
  font-size: 11px; padding: 2px 7px; margin-bottom: 2px; border-radius: 4px;
}
.level-res { background: #fee2e2; }
.level-sup { background: #dcfce7; }
.lv { font-weight: 600; }

/* Cover */
.cover-slide {
  background: #ffffff !important;
  position: relative !important;
  justify-content: center !important;
  padding: 0 !important;
}
.cover-inner {
  flex: 1; display: flex; flex-direction: column;
  align-items: flex-start; justify-content: center; padding: 52px 64px;
  position: relative; z-index: 1;
}

@media print {
  body { background: white; padding: 0; margin: 0; }
  .container { max-width: none; margin: 0; padding: 0; }
  .slide-container { width: 100%; height: auto; overflow: visible; margin: 0; }
  .slide-content {
    page-break-after: always; break-after: page; page-break-inside: avoid;
    width: 100%; height: 100vh; overflow: hidden;
    padding: 20px 28px 12px; margin-bottom: 0; box-shadow: none;
  }
}
"""

# ── Versioned filename (Fix 39) ────────────────────────────────────────────────
_base_fn = "Alpha_Bank_TA_Report_" + TODAY.strftime("%Y%m") + ".html"
_out_path = os.path.join(os.getcwd(), _base_fn)
if os.path.exists(_out_path):
    for _v in range(2, 20):
        _vpath = _out_path.replace(".html", "_v" + str(_v) + ".html")
        if not os.path.exists(_vpath):
            _out_path = _vpath
            break

_HTML = (
    "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n"
    "<meta charset=\"UTF-8\">\n"
    "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1.0\">\n"
    "<title>Alpha Bank \u2014 Cross Asset Technical Vista \u2014 " + REPORT_DATE + "</title>\n"
    "<style>" + _CSS + "</style>\n"
    "</head>\n<body>\n"
    "<div class=\"container\">\n"

    "<div class=\"slide-container\">\n"
    + _slides_html
    + "\n</div>\n"
    "</div>\n"
    "</body>\n</html>\n"
)

# ── Write file ─────────────────────────────────────────────────────────────────
with open(_out_path, "w", encoding="utf-8") as _f:
    _f.write(_HTML)

# ── Smoke test (Fix 40) ────────────────────────────────────────────────────────
_sz_kb = os.path.getsize(_out_path) // 1024
assert _sz_kb > 100, "SMOKE TEST FAILED: output file < 100 KB (" + str(_sz_kb) + " KB)"
assert len(_ids) == len(_slides), ("SMOKE TEST FAILED: slide count mismatch "
                                   + str(len(_slides)) + " slides vs " + str(len(_ids)) + " IDs")
print("Smoke test PASSED: " + str(_sz_kb) + " KB | " + str(len(_ids)) + " slides")

# ── PDF export via WeasyPrint (Fix 38) ────────────────────────────────────────
_pdf_path = _out_path.replace(".html", ".pdf")
try:
    from weasyprint import HTML as _WP_HTML
    _WP_HTML(filename=_out_path).write_pdf(_pdf_path)
    _pdf_kb = os.path.getsize(_pdf_path) // 1024
    print("PDF saved: " + _pdf_path + " (" + str(_pdf_kb) + " KB)")
except ImportError:
    print("WeasyPrint not installed — PDF not generated. Open HTML in Chrome and use Ctrl+P.")
except Exception as _we:
    print("WeasyPrint error: " + str(_we) + " — HTML file is the deliverable.")

# ── Finalize run log (Fix 41) ──────────────────────────────────────────────────
_n_ok     = sum(1 for v in run_log["assets"].values() if v["status"] in ("ok", "warned"))
_n_failed = sum(1 for v in run_log["assets"].values() if v["status"] == "failed")
_total_rt = sum(v.get("runtime_seconds", 0) for v in run_log["assets"].values())

run_log["summary"] = {
    "report_date":       REPORT_DATE,
    "assets_ok":         _n_ok,
    "assets_failed":     _n_failed,
    "assets_warned":     sum(1 for v in run_log["assets"].values() if v["status"] == "warned"),
    "slide_count":       len(_ids),
    "output_file":       _out_path,
    "output_size_kb":    _sz_kb,
    "total_runtime_sec": round(_total_rt, 1),
    "prose_sources":     {k: v.get("prose_source", "") for k, v in run_log["assets"].items()},
}

_log_fn = "TA_Report_RunLog_" + TODAY.strftime("%Y%m") + ".json"
with open(_log_fn, "w", encoding="utf-8") as _lf:
    json.dump(run_log, _lf, indent=2, default=str)

print(f"Run log saved: {_log_fn}")
print("")
print("=" * 60)
print(f"CELL 8 OK \u2014 Report generation complete")
print(f"  Output : {_out_path}")
print(f"  Size   : {_sz_kb} KB")
print(f"  Slides : {len(_ids)} ({_n_ok} assets OK, {_n_failed} failed)")
print(f"  Runtime: {round(_total_rt, 1)}s")
print("=" * 60)
print("")
print("NEXT STEPS:")
print(f"  1. Download {_base_fn} from BQuant file browser")
print("  2. Open in Chrome / Edge")
print("  3. Ctrl+P \u2192 Save as PDF (if WeasyPrint unavailable)")
