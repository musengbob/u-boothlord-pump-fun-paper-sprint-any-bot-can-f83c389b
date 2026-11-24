// SprintNet — wide-net early momentum for the pump.fun paper sprint.
//
// Idea: on a 6-hour window almost every new coin dies and a handful run hard.
// So the bot fishes wide (many small tickets), enters only launches whose first
// seconds show real dispersed buy pressure, and lets the rare runner ride
// behind a trailing stop instead of capping it with a fixed take-profit.
// Losers are cut fast; the 5-slot position cap turns over quickly.
//
// Entry: a coin 3-150 slots old, real SOL 5-50, >= 8 buys from >= 8 distinct
// non-creator wallets in the last 60 slots, net inflow >= 3 SOL and
// accelerating (newer half beats older half), price up 10-50% from the window
// start and within 2% of the window high, creator has not sold, no single
// sell >= 1.5 SOL. One ticket of 0.02 SOL per coin, no adds: on the dev
// window every add and every bigger ticket only deepened the bleed, and the
// strict filter is what keeps the trip count (and the fee drag) down.
//
// Exit (whole position): stop-loss -25% vs cost, trailing stop -20% from the
// peak once up >= 15%, max hold 2400 slots, creator sells, a >= 1.5 SOL
// single sell, 30-slot net flow < -1 SOL, or no trade for 600 slots.
//
// Guardrail pacing: the run must clear 20 distinct coins. If the coin count
// lags the pace of the 6-hour window, entry thresholds relax in two steps so
// the bot still banks the minimum instead of sitting on idle cash.
//
// Deterministic by construction: no clock, no environment, no randomness;
// iteration only over insertion-ordered maps fed by the batch stream.

import type { Batch, Bot, BotState, OrderInput, Pubkey } from './vendor/makefun-bot.ts';

export interface SprintParams {
  /** Entry lookback in slots. */
  windowSlots: number;
  /** Youngest / oldest age (slots since CreateEvent) for a first buy. */
  minAgeSlots: number;
  maxAgeSlots: number;
  /** Real SOL reserve window at entry. */
  minRealSol: number;
  maxRealSol: number;
  /** Buys / distinct non-creator buyers inside the window. */
  minWindowBuys: number;
  minWindowBuyers: number;
  /** Net SOL inflow (buys minus sells) inside the window. */
  minNetInflowSol: number;
  /** Price rise vs window start, as a fraction. */
  minRise: number;
  maxRise: number;
  /** Price now must be at least this share of the window high. */
  nearHigh: number;
  /** One single sell of this size (SOL) vetoes entry / forces exit. */
  bigSellSol: number;
  /** First ticket and one add-on ticket, SOL. */
  firstTicketSol: number;
  addTicketSol: number;
  /** Max SOL invested in one coin. */
  maxPerCoinSol: number;
  /** Add only once price is this far above the last buy's decision price. */
  addStepUp: number;
  /** Buy slippage (bps); sells always go out wide open. */
  buySlippageBps: number;
  /** Stop-loss vs cost, as a fraction. */
  stopLoss: number;
  /** Trailing stop arms once value >= cost * (1 + trailArm). */
  trailArm: number;
  /** Exit once value falls this far below its peak. */
  trailGiveback: number;
  /** Max hold in slots. */
  maxHoldSlots: number;
  /** Exit when no trade hits the coin for this many slots. */
  staleSlots: number;
  /** Exit when net flow over flowWindowSlots falls below -exitFlowSol. */
  flowWindowSlots: number;
  exitFlowSol: number;
  /** Slots to wait after an exit before the same coin may be bought again. */
  cooldownSlots: number;
  /** Most first entries per coin over the run. */
  maxEntriesPerCoin: number;
  /** Pacing: target coins / fills across paceSeconds of block time. */
  targetCoins: number;
  targetFills: number;
  paceSeconds: number;
}

export const DEFAULT_PARAMS: SprintParams = {
  windowSlots: 60,
  minAgeSlots: 3,
  maxAgeSlots: 150,
  minRealSol: 5,
  maxRealSol: 50,
  minWindowBuys: 8,
  minWindowBuyers: 8,
  minNetInflowSol: 3,
  minRise: 0.1,
  maxRise: 0.5,
  nearHigh: 0.98,
  bigSellSol: 1.5,
  firstTicketSol: 0.02,
  addTicketSol: 0,
  maxPerCoinSol: 0.02,
  addStepUp: 0.25,
  buySlippageBps: 1500,
  stopLoss: 0.25,
  trailArm: 0.15,
  trailGiveback: 0.2,
  maxHoldSlots: 2400,
  staleSlots: 600,
  flowWindowSlots: 30,
  exitFlowSol: 1,
  cooldownSlots: 120,
  maxEntriesPerCoin: 2,
  targetCoins: 30,
  targetFills: 120,
  paceSeconds: 21600,
};

const LAMPORTS_PER_SOL = 1e9;
/** Curve fee on a sell: protocol 95 bps + creator 30 bps. */
const CURVE_FEE = 0.0125;

interface Tick {
  slot: number;
  buy: boolean;
  sol: number;
  user: string;
  price: number;
}

interface Watch {
  createdSlot: number;
  creator: string;
  ticks: Tick[];
  devSold: boolean;
  lastTradeSlot: number;
  entries: number;
  lastExitSlot: number;
  /** Price (vq/vt) the last buy on this coin was decided at. */
  lastBuyPrice: number;
  /** SOL sent on buys that filled for the current position. */
  filledSol: number;
}

interface Open {
  openedSlot: number;
  peakValueSol: number;
  lastBuyPrice: number;
  exiting: boolean;
}

export function createStrategy(p: SprintParams = DEFAULT_PARAMS): Bot {
  const watches = new Map<Pubkey, Watch>();
  const open = new Map<Pubkey, Open>();
  const myCoins = new Set<Pubkey>();
  let myFills = 0;
  let firstBlockTime: number | null = null;
  let lastBlockTime: number | null = null;
  const keepSlots = Math.max(p.windowSlots, p.flowWindowSlots) + 10;

  const watchOf = (mint: Pubkey): Watch => {
    let w = watches.get(mint);
    if (w === undefined) {
      w = {
        createdSlot: -1,
        creator: '',
        ticks: [],
        devSold: false,
        lastTradeSlot: -1,
        entries: 0,
        lastExitSlot: -1e12,
        lastBuyPrice: 0,
        filledSol: 0,
      };
      watches.set(mint, w);
    }
    return w;
  };

  /** Net SOL flow over the last `span` slots, plus window stats. */
  const windowStats = (w: Watch, now: number) => {
    const from = now - p.windowSlots;
    const mid = now - p.windowSlots / 2;
    let buys = 0;
    let buySol = 0;
    let sellSol = 0;
    let netOld = 0;
    let netNew = 0;
    let startPrice = -1;
    let high = 0;
    let last = 0;
    let biggestSell = 0;
    const buyers = new Set<string>();
    for (const t of w.ticks) {
      if (t.slot <= from) {
        startPrice = t.price;
        continue;
      }
      if (startPrice < 0) startPrice = t.price;
      const signed = t.buy ? t.sol : -t.sol;
      if (t.slot <= mid) netOld += signed;
      else netNew += signed;
      if (t.buy) {
        buys++;
        buySol += t.sol;
        buyers.add(t.user);
      } else {
        sellSol += t.sol;
        if (t.sol > biggestSell) biggestSell = t.sol;
      }
      if (t.price > high) high = t.price;
      last = t.price;
    }
    return { buys, buyers: buyers.size, net: buySol - sellSol, netOld, netNew, startPrice, high, last, biggestSell };
  };

  const flowOver = (w: Watch, now: number, span: number): number => {
    let net = 0;
    for (let i = w.ticks.length - 1; i >= 0; i--) {
      const t = w.ticks[i]!;
      if (t.slot <= now - span) break;
      net += t.buy ? t.sol : -t.sol;
    }
    return net;
  };

  /** What selling `tokens` against (vq, vt) pays, in SOL, after curve + tx fees. */
  const sellValueSol = (tokens: bigint, vq: bigint, vt: bigint, txLamports: bigint): number => {
    if (tokens <= 0n || vt <= 0n) return 0;
    const out = Number((tokens * vq) / (vt + tokens)) / LAMPORTS_PER_SOL;
    return out * (1 - CURVE_FEE) - Number(txLamports) / LAMPORTS_PER_SOL;
  };

  return {
    onBatch(state: BotState, batch: Batch): OrderInput[] {
      const now = batch.slot;
      const tx = state.txFeeLamports();
      const traded = new Set<Pubkey>();

      for (const f of batch.fills) {
        if (f.status === 'filled' && !f.client_id.startsWith('_')) {
          myFills++;
          myCoins.add(f.mint);
          if (f.side === 'buy') {
            const w = watches.get(f.mint);
            if (w !== undefined) w.filledSol += Number(f.quote_lamports) / LAMPORTS_PER_SOL;
          }
        }
      }

      for (const e of batch.events) {
        if (e.block_time !== null) {
          if (firstBlockTime === null) firstBlockTime = e.block_time;
          if (lastBlockTime === null || e.block_time > lastBlockTime) lastBlockTime = e.block_time;
        }
        if (e.event === 'CreateEvent') {
          const w = watchOf(e.mint);
          w.createdSlot = e.slot;
          w.creator = e.creator;
          w.lastTradeSlot = e.slot;
        } else if (e.event === 'TradeEvent') {
          const w = watchOf(e.mint);
          const price = Number(e.virtual_quote_reserves) / Number(e.virtual_token_reserves);
          w.ticks.push({ slot: e.slot, buy: e.is_buy, sol: Number(e.sol_amount) / LAMPORTS_PER_SOL, user: e.user, price });
          w.lastTradeSlot = e.slot;
          traded.add(e.mint);
          if (!e.is_buy && e.user === w.creator) w.devSold = true;
        } else {
          const w = watches.get(e.mint);
          if (w !== undefined) w.lastTradeSlot = e.slot;
        }
      }
      for (const mint of traded) {
        const w = watches.get(mint)!;
        let cut = 0;
        while (cut < w.ticks.length && w.ticks[cut]!.slot <= now - keepSlots) cut++;
        if (cut > 0) w.ticks.splice(0, cut);
      }

      const marks = new Map(batch.marks.map((m) => [m.mint, m] as const));
      const pendingByMint = new Map<Pubkey, { buy: boolean; sell: boolean }>();
      for (const po of state.pending.values()) {
        const rec = pendingByMint.get(po.order.mint) ?? { buy: false, sell: false };
        if (po.order.side === 'buy') rec.buy = true;
        else rec.sell = true;
        pendingByMint.set(po.order.mint, rec);
      }

      // Pacing: how far through the window are we, and are coins lagging?
      let relaxed = false;
      let desperate = false;
      if (firstBlockTime !== null && lastBlockTime !== null) {
        const pace = Math.min(1, (lastBlockTime - firstBlockTime) / p.paceSeconds);
        const expected = Math.ceil(p.targetCoins * pace);
        if (myCoins.size < expected) relaxed = true;
        if (pace > 0.5 && myCoins.size < 20) desperate = true;
      }

      const orders: OrderInput[] = [];
      const cap = state.hello.caps.max_orders_per_batch;
      const txSol = Number(tx) / LAMPORTS_PER_SOL;

      // ---- Exits first. ----
      for (const [mint, o] of open) {
        const pos = state.positions.get(mint);
        const curve = state.curves.get(mint);
        const pend = pendingByMint.get(mint);
        if (pos === undefined) {
          if (pend === undefined) {
            open.delete(mint);
            const w = watchOf(mint);
            w.lastExitSlot = now;
            w.filledSol = 0;
          }
          continue;
        }
        if (curve === undefined || curve.complete) continue;
        const mk = marks.get(mint);
        const vq = mk ? BigInt(mk.virtual_quote_reserves) : curve.virtual_quote_reserves;
        const vt = mk ? BigInt(mk.virtual_token_reserves) : curve.virtual_token_reserves;
        const value = sellValueSol(pos.token_amount, vq, vt, tx);
        const cost = Number(pos.cost_lamports) / LAMPORTS_PER_SOL;
        if (value > o.peakValueSol) o.peakValueSol = value;
        const w = watchOf(mint);
        let exit = o.exiting;
        if (!exit && cost > 0 && value < cost * (1 - p.stopLoss)) exit = true;
        if (!exit && cost > 0 && o.peakValueSol >= cost * (1 + p.trailArm) && value < o.peakValueSol * (1 - p.trailGiveback)) exit = true;
        if (!exit && now - o.openedSlot > p.maxHoldSlots) exit = true;
        if (!exit && w.devSold) exit = true;
        if (!exit && windowStats(w, now).biggestSell >= p.bigSellSol) exit = true;
        if (!exit && flowOver(w, now, p.flowWindowSlots) < -p.exitFlowSol) exit = true;
        if (!exit && now - w.lastTradeSlot > p.staleSlots) exit = true;
        if (!exit) continue;
        o.exiting = true;
        if (pend?.sell) continue;
        if (orders.length >= cap) break;
        orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
      }

      // ---- Entries and one add per coin. ----
      let free = Number(state.freeCashLamports()) / LAMPORTS_PER_SOL;
      let slotsLeft = state.hello.caps.max_open_positions - state.committedPositionCount();
      for (const mint of traded) {
        if (orders.length >= cap) break;
        const curve = state.curves.get(mint);
        if (curve === undefined || curve.complete) continue;
        if (pendingByMint.has(mint)) continue;
        const w = watches.get(mint)!;
        const price = Number(curve.virtual_quote_reserves) / Number(curve.virtual_token_reserves);
        const realSol = Number(curve.real_quote_reserves) / LAMPORTS_PER_SOL;
        const held = open.get(mint);

        if (held !== undefined) {
          // Add once into strength: price stepped up, inflow still positive.
          if (held.exiting || w.filledSol >= p.maxPerCoinSol - 0.005) continue;
          if (price < held.lastBuyPrice * (1 + p.addStepUp)) continue;
          if (flowOver(w, now, p.windowSlots) < p.minNetInflowSol / 2) continue;
          const capSol = Number(state.maxBuyLamports(mint)) / LAMPORTS_PER_SOL;
          let amount = Math.min(capSol, p.addTicketSol, p.maxPerCoinSol - w.filledSol);
          if (amount > free - txSol) amount = free - txSol;
          if (amount < 0.01) continue;
          orders.push({ side: 'buy', mint, quote_lamports: BigInt(Math.floor(amount * LAMPORTS_PER_SOL)), max_slippage_bps: p.buySlippageBps });
          free -= amount + txSol;
          held.lastBuyPrice = price;
          w.lastBuyPrice = price;
          continue;
        }

        if (slotsLeft <= 0) continue;
        if (w.entries >= p.maxEntriesPerCoin || now - w.lastExitSlot < p.cooldownSlots) continue;
        const age = w.createdSlot < 0 ? 1e9 : now - w.createdSlot;
        const maxAge = desperate ? 600 : p.maxAgeSlots;
        if (age < p.minAgeSlots || age > maxAge) continue;
        if (realSol < (desperate ? 2 : p.minRealSol) || realSol > p.maxRealSol) continue;
        if (w.devSold) continue;
        const s = windowStats(w, now);
        const minBuys = desperate ? 2 : relaxed ? 3 : p.minWindowBuys;
        const minBuyers = desperate ? 2 : relaxed ? 2 : p.minWindowBuyers;
        const minNet = desperate ? 0.1 : relaxed ? 0.3 : p.minNetInflowSol;
        const minRise = desperate ? 0 : relaxed ? 0.03 : p.minRise;
        if (s.buys < minBuys || s.buyers < minBuyers) continue;
        if (s.net < minNet) continue;
        if (!desperate && s.netNew <= 0) continue;
        if (!desperate && s.netNew < s.netOld) continue;
        if (s.startPrice <= 0) continue;
        const rise = s.last / s.startPrice - 1;
        if (rise < minRise || rise > p.maxRise) continue;
        if (!desperate && s.last < s.high * p.nearHigh) continue;
        if (s.biggestSell >= p.bigSellSol) continue;

        const capSol = Number(state.maxBuyLamports(mint)) / LAMPORTS_PER_SOL;
        let amount = Math.min(capSol, p.firstTicketSol);
        if (amount > free - txSol) amount = free - txSol;
        if (amount < 0.01) continue;
        orders.push({ side: 'buy', mint, quote_lamports: BigInt(Math.floor(amount * LAMPORTS_PER_SOL)), max_slippage_bps: p.buySlippageBps });
        free -= amount + txSol;
        slotsLeft--;
        open.set(mint, { openedSlot: now, peakValueSol: 0, lastBuyPrice: price, exiting: false });
        w.entries++;
        w.lastBuyPrice = price;
      }
      return orders;
    },
  };
}
