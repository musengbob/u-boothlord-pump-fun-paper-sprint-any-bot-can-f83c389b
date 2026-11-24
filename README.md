# SprintNet — Pump.fun Paper Sprint Bot

[![make.fun](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b.svg)](https://make.fun/c/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![rank](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/musengbob/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/rank.svg)](https://make.fun/c/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![reward](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/musengbob/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/reward.svg)](https://make.fun/c/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)

Paper-trading bot for the [Pump.fun Paper Sprint: Any Bot Can Win](https://make.fun/c/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b) bounty on [make.fun](https://make.fun). One `main.ts` (Node 22), zero dependencies, fully deterministic — it paper-trades pump.fun bonding curves on a 6-hour replay for net PnL. No real funds, no entry fee.

## Strategy: wide net, strict filter, fast exits

On a 6-hour window almost every new coin dies and a handful run hard. So the
bot fishes wide (many small tickets), enters only launches whose first seconds
show real dispersed buy pressure, and lets the rare runner ride behind a
trailing stop instead of capping it with a fixed take-profit. Losers are cut
fast; the 5-slot position cap turns over quickly.

**Entry** — a coin 3–150 slots old, real SOL 5–50, ≥ 8 buys from ≥ 8 distinct
non-creator wallets in the last 60 slots, net inflow ≥ 3 SOL and accelerating
(newer half beats older half), price up 10–50% from the window start and
within 2% of the window high, creator has not sold, no single sell ≥ 1.5 SOL.
One ticket of 0.02 SOL per coin, no adds.

**Exit (whole position)** — stop-loss −25% vs cost, trailing stop −20% from
the peak once up ≥ 15%, max hold 2400 slots, creator sells, a ≥ 1.5 SOL
single sell, 30-slot net flow < −1 SOL, or no trade for 600 slots.

**Guardrail pacing** — the run must clear 20 distinct coins (≥ 0.01 SOL round
trips). If the coin count lags the pace of the 6-hour window, entry thresholds
relax in two steps so the bot still banks the minimum instead of sitting on
idle cash.

## Local backtest (fresh kit dev window, kit pilot sim engine)

| Metric | Value |
|---|---|
| Net PnL | −0.079162 SOL |
| Fills / coins / trips | 94 / 46 / 47 |
| Winning trips | 8 |
| Max drawdown | 0.79% |
| Net PnL at 2x fees | −0.111075 SOL |
| Determinism | repeat run identical (−0.079162 SOL) |

Guardrails met: 46 distinct coins (≥ 20 required), max drawdown 0.79%
(≤ 50% allowed), 0.02 SOL tickets (≥ 0.01 SOL round trips).

The dev window is brutally negative-sum after fees (kit pilots: momentum
−9.489, sniper −0.475, reversion −3.995 SOL) — this bot's edge is losing the
least while keeping live exposure to the rare runner that decides the final
6-hour window. Full-window grade via the kit's own grader:

```sh
node cli/makefun.mjs grade-local --kit <kit dir> --bot <this repo> --driver process
```

## Layout

- `main.ts` — entrypoint (`node /bot/main.ts`)
- `strategy.ts` — the SprintNet strategy (all logic lives here)
- `vendor/makefun-bot.ts` — the kit's `agent-fun/replay/v2` shim, vendored byte for byte
- `.makefun/owner.json` — claim file naming the payout wallet

## License

MIT — see [LICENSE](LICENSE).
