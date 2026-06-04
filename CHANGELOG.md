# Changelog

All notable changes to PyEventBT will be documented in this file.

## [0.0.13] - 2026-06-04

Fixes a redundant MT5 query in the live data provider that, in a rare timing window, could skip a bar and duplicate the next one. In `Mt5LiveDataProvider.update_bars`, a new bar was detected and recorded using one `get_latest_bar` call, but the bar actually delivered to the event queue came from a *second* `get_latest_bar` call. If a timeframe boundary fell between the two calls (a sub-millisecond window), the second call returned a different, newer bar than the one recorded in `last_bar_tf_datetime` — causing the gated bar to be skipped and the newer bar to be re-emitted on the following cycle. The fix reuses the already-fetched bar, which also removes a redundant IPC round-trip per symbol/timeframe on every update cycle. Backtesting is unaffected.

### Bug Fixes

- Reuse the already-fetched bar in `Mt5LiveDataProvider.update_bars` instead of calling `get_latest_bar` a second time to build the event. This eliminates a race where a timeframe boundary between the two calls could skip the detected bar and duplicate the next one, and halves the MT5 IPC calls made per symbol/timeframe on each update.

**Full Changelog**: [v0.0.12...v0.0.13](https://github.com/marticastany/pyeventbt/compare/v0.0.12...v0.0.13)

## [0.0.12] - 2026-05-29

Fixes a failed stop-loss/take-profit modification in the MT5 live execution engine connector when a caller passes a non-`float` numeric (e.g. a `Decimal`) for `new_sl`/`new_tp`. `mt5.order_send()` only accepts native Python floats for the `sl`/`tp` request fields, so a `Decimal` made the send return `None` with last error `(-2, 'Invalid "sl" argument')` and the modification was silently dropped. This complements the 0.0.11 fix, which handled the `None` result gracefully but did not address why the send was failing. The backtest simulator accepts `Decimal`, so the issue only surfaced in live trading.

### Bug Fixes

- Cast `new_sl`/`new_tp` to `float` when building the `TRADE_ACTION_SLTP` request in `update_position_sl_tp`, so callers passing `Decimal` (or other numerics) no longer trigger `(-2, 'Invalid "sl" argument')` from `mt5.order_send()`. The order-placement paths already cast their `sl`/`tp`; this brings the SL/TP-modification path in line.

**Full Changelog**: [v0.0.11...v0.0.12](https://github.com/marticastany/pyeventbt/compare/v0.0.11...v0.0.12)

## [0.0.11] - 2026-05-27

Fixes a crash in the MT5 live execution engine connector when `mt5.order_send()` returns `None`. The connector correctly detected the failure but then attempted to read `.retcode` and `.request.symbol` on the `None` result while formatting the warning log, raising an `AttributeError` that propagated up through `run_live` and stopped the strategy.

### Bug Fixes

- Fixed `AttributeError` in `update_position_sl_tp` and `cancel_pending_order` when `mt5.order_send()` returns `None` (e.g. on transient trade-server disconnects). Both branches now log `mt5.last_error()` instead of dereferencing the missing result.
- Added an early `return None` in `place_pending_order` and `cancel_pending_order` when the order send returns `None`, so the subsequent `result._asdict()` call no longer crashes after a failed send.

**Full Changelog**: [v0.0.10...v0.0.11](https://github.com/marticastany/pyeventbt/compare/v0.0.10...v0.0.11)

## [0.0.10] - 2026-05-22

This release expands the list of supported symbols, enhances the hook system with a new `ON_FILL_EVENT` hook and richer callback signatures, and addresses a correctness issue in the backtesting simulator that prevented some stop loss modifications on open positions.

### Highlights

**Stop loss modification on open positions**

The backtesting simulator now validates stop loss modifications against the current bar's close price rather than the previous bar's close. This previously caused the simulator to reject stop loss changes that were valid at the current price, leading to a mismatch between backtest behavior and how MT5 handles the same operation in live trading.

### Features

- Added `BTCUSD` and `ETHUSD` to the list of supported symbols (by [Meherett](https://github.com/meherett)).
- **Enhanced Hook System:** Introduces an `ON_FILL_EVENT` hook and updates existing `ON_SIGNAL_EVENT` and `ON_ORDER_EVENT` hooks to pass the relevant event object directly to their callbacks. This provides more context and data to custom hook functions, while maintaining backward compatibility for existing callbacks (by [Meherett](https://github.com/meherett)).

### Bug Fixes

- Fixed a problem where, in some cases, the backtesting simulator was not allowing to modify stop losses on open positions because it was checking for the position open price instead of the current one.

**Full Changelog**: [v0.0.9...v0.0.10](https://github.com/marticastany/pyeventbt/compare/v0.0.9...v0.0.10)

## [0.0.9] - 2026-04-16

Fixes a multi-symbol synchronization bug in the data connectors that caused symbols to process bars at the wrong speed when they appeared more than once in `tradeable_symbol_list`.

### Bug Fixes

- Deduplicate `tradeable_symbol_list` in both CSV and MT5 live data connectors to prevent a symbol's data generator from advancing multiple times per cycle. When a symbol appeared more than once in the list, its generator was called N times per `update_bars()` cycle, causing that symbol to process bars at Nx speed and desynchronize from other symbols. A warning is now logged when duplicates are removed.

**Full Changelog**: [v0.0.8...v0.0.9](https://github.com/marticastany/pyeventbt/compare/v0.0.8...v0.0.9)

## [0.0.8] - 2026-04-14

Improves position sizing precision by ensuring risk-based volume calculations always round down, so the actual risk taken never exceeds the configured risk percentage.

### Bug Fixes

- Fixed position sizing rounding in `MT5RiskPctSizing` to always round down to the nearest volume step, ensuring the actual risk never exceeds the configured risk percentage. Previously used Python's `round()` (banker's rounding), which could round up and overshoot the intended risk.

**Full Changelog**: [v0.0.7...v0.0.8](https://github.com/marticastany/pyeventbt/compare/v0.0.7...v0.0.8)

## [0.0.7] - 2026-04-10

This release focuses on correctness of the backtesting simulator, particularly for multi-symbol strategies. It also includes several quality-of-life fixes around state management and error reporting.

### Highlights

**Multi-symbol equity calculation**

The simulator now computes equity by summing floating PnL across all open positions, regardless of which symbol's bar is being processed. Previously, equity only reflected the PnL of the current bar's symbol, which caused incorrect free margin, margin call checks, and historical equity curves when trading multiple symbols simultaneously. This aligns the simulator with how the MT5 platform calculates equity in live trading.

**SharedData reset between backtests**

Running sequential backtests (e.g. in optimization loops) could leak simulator state between runs — symbol visibility flags, error codes, and account info from a previous backtest would carry over. SharedData is now reset to clean defaults at the start of each backtest call.

### Features

- Added `CHANGELOG.md` with automated GitHub release notes extraction on each release.

### Bug Fixes

- Fixed multi-symbol equity calculation in backtesting simulator to account for floating PnL across all open positions, matching live MT5 behavior.
- Replaced mutable default arguments in Strategy method signatures (`custom_signal_engine`, `configure_predefined_signal_engine`, `backtest`, `run_live`) with `None` defaults.
- Consolidated FX symbol list into a single `ALL_FX_SYMBOLS` constant in `utils.py`, replacing 4 duplicated tuples. Added missing `USDMXN` and `EURMXN` pairs to the connector and sizing engine, which previously applied incorrect CFD margin/commission formulas for those pairs.
- Fixed logger name in `utils.py` from `"PyEventBT"` to `"pyeventbt"` so log messages from utility functions are captured by the framework's logger handlers.
- Replaced bare `IndexError` in currency conversion lookups with a descriptive `ValueError` indicating the unsupported currency pair.
- Added `SharedData` reset at the start of each backtest to prevent simulator state from leaking between sequential backtest runs.

**Full Changelog**: [v0.0.6...v0.0.7](https://github.com/marticastany/pyeventbt/compare/v0.0.6...v0.0.7)

## [0.0.6] - 2025-12-17

Adds three new technical indicators along with fixes to the example strategies, all contributed by [kevin-bruton](https://github.com/kevin-bruton).

### Features

- Added TRIX, DeMarker and RVI indicators
- Updated README examples

### Bug Fixes

- Fixed strategy_id in Quantdle MA crossover example
- Fixed example strategies

**Full Changelog**: [v0.0.5...v0.0.6](https://github.com/marticastany/pyeventbt/compare/v0.0.5...v0.0.6)

## [0.0.5] - 2025-12-07

Adds backtest result export and improves sizing precision.

### Features

- Added CSV export for backtest results
- Pointed homepage to dedicated website (pyeventbt.com)

### Bug Fixes

- Ensured precision in MT5 sizing calculations
- Fixed README example syntax

**Full Changelog**: [v0.0.4...v0.0.5](https://github.com/marticastany/pyeventbt/compare/v0.0.4...v0.0.5)

## [0.0.4] - 2025-12-06

### Bug Fixes

- Handled MT5 import errors gracefully for non-Windows platforms

**Full Changelog**: [v0.0.3...v0.0.4](https://github.com/marticastany/pyeventbt/compare/v0.0.3...v0.0.4)

## [0.0.3] - 2025-12-06

### Features

- Added CLI application with `info` command
- Read version from package metadata

**Full Changelog**: [v0.0.2...v0.0.3](https://github.com/marticastany/pyeventbt/compare/v0.0.2...v0.0.3)

## [0.0.2] - 2025-12-06

Initial full framework release with all core modules: event-driven backtesting and live trading engine, MT5 simulator wrapper and broker connectors, data provider with CSV backtesting and MT5 live data support, signal/sizing/risk engine services, portfolio and position management, trade archiver, technical indicators (ATR, SMA, EMA, KAMA), hook and schedule services, and the Strategy class with decorator-based API. Includes example strategies for MA crossover, Bollinger Bands breakout, and Quantdle MA crossover.

**Full Changelog**: [v0.0.1...v0.0.2](https://github.com/marticastany/pyeventbt/compare/v0.0.1...v0.0.2)

## [0.0.1] - 2025-10-08

Initial project structure, CI workflow, and Poetry configuration.

# Changelog Format

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
