# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Alpha Bank — Cross Asset Technical Vista** is a Bloomberg BQuant Python script that generates an institutional monthly cross-asset TA report. It fetches OHLCV data via BQL for 9 global assets, computes technical indicators, detects chart patterns, generates matplotlib charts, optionally calls the Claude API for narrative prose, and outputs a self-contained HTML slide deck (optionally converted to PDF via WeasyPrint).

The script runs inside Bloomberg's BQuant notebook environment and is designed to be executed top-to-bottom as a single Python file (all 8 cells + Cell 7A are concatenated).

## Git Workflow

**Commit and push frequently** — after completing any meaningful unit of work (a cell, a function, a bug fix), commit and push to GitHub so progress is never lost.

```bash
git add -p                          # stage only relevant changes
git commit -m "feat(cell3): add true weekly indicator computation"
git push
```

**Commit message conventions:**
- `feat(cellN): <what was added>` — new functionality
- `fix(cellN): <what was corrected>` — bug fixes
- `refactor: <what changed>` — restructuring without behavior change
- `chore: <what was updated>` — config, dependencies, tooling

Push after every cell is completed. Never accumulate more than one cell's worth of uncommitted changes. If a long-running task is interrupted, the last push is the recovery point.

## Running & Checking

**Syntax check (no BQL required):**
```bash
python -m py_compile ta_report_final.py
```

**Full run (requires Bloomberg BQuant + BQL):**
Execute the file top-to-bottom in a BQuant notebook. Each cell prints `CELL N OK` on success.

**Claude API prose (optional):**
Set `CLAUDE_API_KEY` in Cell 7A before running. Leave empty to use the deterministic template fallback.

**Output files written to the current working directory:**
- `Alpha_Bank_TA_Report_YYYYMM[_vN].html` — interactive HTML deck
- `Alpha_Bank_TA_Report_YYYYMM[_vN].pdf` — PDF via WeasyPrint (if installed)
- `TA_Report_RunLog_YYYYMM.json` — per-asset status, timings, prose sources

## Architecture

The file is structured as 9 sequential cells, each building on the previous:

| Cell | Role |
|------|------|
| 1 | Imports; `bq = bql.Service()` |
| 2 | Config: `ASSETS`, date windows, `MIN_ROWS`, `FIB_LOOKBACK`, `PATTERN_WINDOWS`, logo loading |
| 3 | `fetch_ohlcv()` → `validate_ohlcv()` → `detect_stale_fills()` → `compute_indicators()` → `_compute_weekly_indicators()` |
| 4 | Signal helpers, `compute_bias_score()`, `_detect_chart_pattern()`, `_compute_sr_levels()`, `_compute_market_structure()`, `compute_stats()` |
| 5 | `make_chart_b64()` — 1-year daily matplotlib chart → Base64 PNG; `_accent_band()` |
| 6 | Main loop over 9 assets; per-asset exception handling; `run_log` dict |
| 7A | Claude API integration: `build_claude_brief()` → `_call_claude()` → `_validate_claude_output()`; `_template_prose_fallback()` fallback; populates `claude_prose` dict |
| 7 | HTML builders: `_page1()`, `_page2()`, `_warning_slide()`, `_cover()`, `_perf_scorecard_html()`, `_market_structure_html()` |
| 8 | CSS + JS assembly; versioned filename; WeasyPrint PDF; smoke tests; run log JSON write |

**Data flow:**
```
BQL API → fetch_ohlcv() → validate_ohlcv() → compute_indicators()
       → compute_stats() → make_chart_b64()
       → build_claude_brief() → _call_claude() (or fallback)
       → _page1() / _page2() → HTML + CSS + JS → .html / .pdf
```

## Key Invariants

These must be preserved across any edits:

- **RSI period = 9** (not 14) everywhere — `rolling(9)` in `compute_indicators()`, label `"RSI (9)"` in signals dict
- **SMAs**: `sma21`, `sma55`, `sma200` — no `sma50` column anywhere
- **`adx_sig(adx, di_plus, di_minus)`** takes 3 arguments (returns directional bias)
- **Signals dict key**: `"RSI (9)"` (used as dict key in `compute_stats()` and referenced in `_bullets()` / HTML builders)
- **Slide dimensions**: 1280×720px hardcoded in CSS and `make_chart_b64()`
- **Brand color**: `#11366B` (Alpha Bank navy)
- **Badge classes**: `sb-bull` / `sb-bear` / `sb-neut` — used throughout CSS and HTML builders
- **BQL fetch pattern**: per-field merge (individual `bq.data.*` fields joined) — never `bql.combined_df(res)`
- **Cell order**: Cell 5 must precede Cell 6 in the file (chart function must exist before main loop calls it)
- **MACD**: 12/26/9; **BB**: 20/2; **Stochastic**: 14/3/3; **ADX**: 14; **Fib levels**: 0%, 23.6%, 38.2%, 50%, 61.8%, 78.6%, 100%

## Asset Configuration

9 assets across 4 types — tickers and types must not change:

```python
ASSETS = {
    "EURUSD": {"ticker": "EURUSD Curncy", "type": "fx"},
    "GBPUSD": {"ticker": "GBPUSD Curncy", "type": "fx"},
    "USDJPY": {"ticker": "USDJPY Curncy", "type": "fx"},
    "NKY":    {"ticker": "NKY Index",     "type": "index"},
    "SPX":    {"ticker": "SPX Index",     "type": "index"},
    "NDX":    {"ticker": "NDX Index",     "type": "index"},
    "GOLD":   {"ticker": "XAU Curncy",    "type": "commodity"},
    "BTC":    {"ticker": "XBTUSD Curncy", "type": "crypto"},
    "WTI":    {"ticker": "CL1 Comdty",   "type": "commodity"},
}
```

Asset type controls: `MIN_ROWS`, `FIB_LOOKBACK`, FX volume suppression (`has_meaningful_volume=False`), chart styling.

## SMA Colors (Chart)

- SMA21 = `#a855f7` (purple), lw=1.4
- SMA55 = `#F97316` (orange), lw=2.0
- SMA200 = `#0EA5E9` (blue), lw=2.0

All drawn with triple-pass glow technique (outer glow → main line → highlight).

## `compute_stats()` Return Dict

The central dict passed to chart builder and all HTML builders. Key fields:

```python
{
    "last", "mtd", "ytd", "12m",
    "sma21", "sma55", "sma200",
    "rsi_val", "adx_val", "di_plus", "di_minus",
    "bb_pct_val", "stoch_k", "stoch_d",
    "signals": {                          # keys: "RSI (9)", "MACD (12/26/9)", "ADX (14)",
        "RSI (9)": {"daily": ..., "weekly": ...},   # "Bollinger Bands", "Stochastic"
        ...
    },
    "overall_bias",                       # "Bullish"/"Mildly Bullish"/"Neutral"/...
    "bias_score_raw",                     # 0–100 float
    "pattern", "pattern_desc", "pattern_confidence",
    "resistances": [R1, R2, R3],          # may contain None
    "supports":    [S1, S2, S3],
    "resistance_scores", "support_scores",
    "fib_levels": {"0%":..., "23.6%":..., ...},
    "momentum_rows",                      # list of dicts for multi-period table
    "stats_52w": {"high52_calc", "low52_calc"},
    "market_structure": {
        "trend_phase", "vol_regime", "macd_cross",
        "ma_cross_55_200",                # tuple: (label, class, bars_ago)
        "ma_cross_21_55",
        "divergence", "bb_position",
        "gap_sma55", "gap_sma200",
    },
    "weekly_indicators": {"w_rsi", "w_macd", "w_macd_sig", "w_bb_pct", "w_stoch_k", "w_stoch_d", "w_adx"},
    "price_roc_20",
}
```

## Error Handling Pattern

Cell 6 wraps each asset in try/except for three specific exception types:

```python
except BQLFetchError:   # BQL connectivity / data fetch failure
except DataValidationError:  # validate_ohlcv() quality checks
except Exception:       # catch-all with traceback.print_exc()
```

Failed assets produce a `_warning_slide()` placeholder and are logged in `run_log["assets"][key]` with `status="failed"`.

## Claude API (Cell 7A)

- Set `CLAUDE_API_KEY = ""` → template fallback (no API call)
- `_call_claude()` returns a dict with exactly: `title`, `bullet1`, `bullet2`, `bullet3`, `pattern_text`, `outlook`
- `_validate_claude_output()` checks for bias contradictions only (does not reject on warnings)
- `claude_prose[key]` is consumed by `_page1()` and `_page2()` in Cell 7
- `run_log["assets"][key]["prose_source"]` records `"claude"` / `"fallback"` / `"template"` / `"skip"`
