// SprintNet — wide-net early momentum for the pump.fun paper sprint.
//
// Thesis: over a 6-hour window nearly every fresh pump.fun coin bleeds out
// while a tiny handful run hard. The bot therefore fishes wide — many small
// tickets — and only steps in when a launch's first seconds show genuine,
// dispersed buy pressure. A runner is allowed to ride behind a trailing stop
// instead of being capped by a fixed take-profit; losers are cut fast and the
// 5-slot position cap turns over quickly.
//
// Entry (first buy on a coin): coin 3-150 slots old, real SOL reserve 5-50,
// at least 8 buys from at least 8 distinct non-creator wallets inside the
// last 60 slots, net inflow >= 3 SOL and accelerating (the newer half of the
// window beats the older half), price up 10-50% from the window start and
// within 2% of the window high, creator has not sold, no single sell >= 1.5
// SOL. One ticket of 0.02 SOL, no adds: on the dev window every add and every
// larger ticket only deepened the bleed, and the strict filter is what keeps
// the trip count (and the fee drag) down.
//
// Exit (whole position): stop-loss -25% vs cost, trailing stop -20% from the
// peak once the position is up >= 15%, max hold 2400 slots, creator sells, a
// >= 1.5 SOL single sell, 30-slot net flow < -1 SOL, or no trade for 600
// slots.
//
// Guardrail pacing: the run must clear 20 distinct coins (>= 0.01 SOL round
// trips). If the coin count lags the pace of the 6-hour window, entry
// thresholds relax in two steps so the bot still banks the minimum instead of
// sitting on idle cash.
//
// Deterministic by construction: no clock, no environment, no randomness;
// iteration only over insertion-ordered maps fed by the batch stream.

import type { Batch, Bot, BotState, OrderInput, Pubkey } from './vendor/makefun-bot.ts';

export interface SprintParams {
  windowSlots: number; // entry lookback, in slots
  minAgeSlots: number; // youngest age (slots since CreateEvent) for a first buy
  maxAgeSlots: number; // oldest age (slots since CreateEvent) for a first buy
  minRealSol: number; // real SOL reserve window at entry, low end
  maxRealSol: number; // real SOL reserve window at entry, high end
  minWindowBuys: number; // buys inside the entry window
  minWindowBuyers: number; // distinct non-creator buyers inside the window
  minNetInflowSol: number; // net SOL inflow (buys minus sells) inside the window
  minRise: number; // price rise vs window start, as a fraction
  maxRise: number; // price rise vs window start, cap as a fraction
  nearHigh: number; // price now must be at least this share of the window high
  bigSellSol: number; // a single sell of this size (SOL) vetoes entry / forces exit
  firstTicketSol: number; // first ticket size, SOL
  addTicketSol: number; // add-on ticket size, SOL (0 = no adds)
  maxPerCoinSol: number; // max SOL invested in one coin
  addStepUp: number; // add only once price is this far above the last buy price
  buySlippageBps: number; // buy slippage (bps); sells always go out wide open
  stopLoss: number; // stop-loss vs cost, as a fraction
  trailArm: number; // trailing stop arms once value >= cost * (1 + trailArm)
  trailGiveback: number; // exit once value falls this far below its peak
  maxHoldSlots: number; // max hold, in slots
  staleSlots: number; // exit when no trade hits the coin for this many slots
  flowWindowSlots: number; // window for the exit-flow check, in slots
  exitFlowSol: number; // exit when net flow over flowWindowSlots < -exitFlowSol
  cooldownSlots: number; // slots to wait after an exit before re-buying the coin
  maxEntriesPerCoin: number; // most first entries per coin over the run
  targetCoins: number; // pacing: target coins across paceSeconds of block time
  targetFills: number; // pacing: target fills across paceSeconds of block time
  paceSeconds: number; // pacing window, seconds of block time
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

interface CoinBook {
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

interface PositionBook {
  openedSlot: number;
  peakValueSol: number;
  lastBuyPrice: number;
  exiting: boolean;
}

interface WindowStats {
  buys: number;
  buyers: number;
  net: number;
  netOld: number;
  netNew: number;
  startPrice: number;
  high: number;
  last: number;
  biggestSell: number;
}

export function createStrategy(p: SprintParams = DEFAULT_PARAMS): Bot {
  const books = new Map<Pubkey, CoinBook>();
  const held = new Map<Pubkey, PositionBook>();
  const myCoins = new Set<Pubkey>();
  let myFills = 0;
  let firstBlockTime: number | null = null;
  let lastBlockTime: number | null = null;
  const keepSlots = Math.max(p.windowSlots, p.flowWindowSlots) + 10;

  function bookOf(mint: Pubkey): CoinBook {
    let b = books.get(mint);
    if (b === undefined) {
      b = {
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
      books.set(mint, b);
    }
    return b;
  }

  /** Stats over the entry window ending at `now`, split into older/newer halves. */
  function windowStats(b: CoinBook, now: number): WindowStats {
    const from = now - p.windowSlots;
    const mid = now - p.windowSlots / 2;
    const st: WindowStats = {
      buys: 0, buyers: 0, net: 0, netOld: 0, netNew: 0,
      startPrice: -1, high: 0, last: 0, biggestSell: 0,
    };
    const buyers = new Set<string>();
    let buySol = 0;
    let sellSol = 0;
    for (const t of b.ticks) {
      if (t.slot <= from) {
        // Ticks at or before the window edge pin the window-start price.
        st.startPrice = t.price;
        continue;
      }
      if (st.startPrice < 0) st.startPrice = t.price;
      const signed = t.buy ? t.sol : -t.sol;
      if (t.slot <= mid) st.netOld += signed;
      else st.netNew += signed;
      if (t.buy) {
        st.buys++;
        buySol += t.sol;
        buyers.add(t.user);
      } else {
        sellSol += t.sol;
        if (t.sol > st.biggestSell) st.biggestSell = t.sol;
      }
      if (t.price > st.high) st.high = t.price;
      st.last = t.price;
    }
    st.buyers = buyers.size;
    st.net = buySol - sellSol;
    return st;
  }

  /** Net SOL flow over the last `span` slots (newest ticks first). */
  function flowOver(b: CoinBook, now: number, span: number): number {
    let net = 0;
    for (let i = b.ticks.length - 1; i >= 0; i--) {
      const t = b.ticks[i]!;
      if (t.slot <= now - span) break;
      net += t.buy ? t.sol : -t.sol;
    }
    return net;
  }

  /** What selling `tokens` against (vq, vt) pays, in SOL, after curve + tx fees. */
  function sellValueSol(tokens: bigint, vq: bigint, vt: bigint, txLamports: bigint): number {
    if (tokens <= 0n || vt <= 0n) return 0;
    const out = Number((tokens * vq) / (vt + tokens)) / LAMPORTS_PER_SOL;
    return out * (1 - CURVE_FEE) - Number(txLamports) / LAMPORTS_PER_SOL;
  }

  return {
    onBatch(state: BotState, batch: Batch): OrderInput[] {
      const now = batch.slot;
      const tx = state.txFeeLamports();
      const txSol = Number(tx) / LAMPORTS_PER_SOL;
      const traded = new Set<Pubkey>();

      // Own fills: count coins/fills for pacing, track filled SOL per coin.
      for (const f of batch.fills) {
        if (f.status === 'filled' && !f.client_id.startsWith('_')) {
          myFills++;
          myCoins.add(f.mint);
          if (f.side === 'buy') {
            const b = books.get(f.mint);
            if (b !== undefined) b.filledSol += Number(f.quote_lamports) / LAMPORTS_PER_SOL;
          }
        }
      }

      // Ingest events into per-coin books.
      for (const e of batch.events) {
        if (e.block_time !== null) {
          if (firstBlockTime === null) firstBlockTime = e.block_time;
          if (lastBlockTime === null || e.block_time > lastBlockTime) lastBlockTime = e.block_time;
        }
        if (e.event === 'CreateEvent') {
          const b = bookOf(e.mint);
          b.createdSlot = e.slot;
          b.creator = e.creator;
          b.lastTradeSlot = e.slot;
        } else if (e.event === 'TradeEvent') {
          const b = bookOf(e.mint);
          const price = Number(e.virtual_quote_reserves) / Number(e.virtual_token_reserves);
          b.ticks.push({
            slot: e.slot,
            buy: e.is_buy,
            sol: Number(e.sol_amount) / LAMPORTS_PER_SOL,
            user: e.user,
            price,
          });
          b.lastTradeSlot = e.slot;
          traded.add(e.mint);
          if (!e.is_buy && e.user === b.creator) b.devSold = true;
        } else {
          const b = books.get(e.mint);
          if (b !== undefined) b.lastTradeSlot = e.slot;
        }
      }
      // Drop ticks older than any window we still consult.
      for (const mint of traded) {
        const b = books.get(mint)!;
        let cut = 0;
        while (cut < b.ticks.length && b.ticks[cut]!.slot <= now - keepSlots) cut++;
        if (cut > 0) b.ticks.splice(0, cut);
      }

      const marks = new Map(batch.marks.map((m) => [m.mint, m] as const));
      const pendingByMint = new Map<Pubkey, { buy: boolean; sell: boolean }>();
      for (const po of state.pending.values()) {
        const rec = pendingByMint.get(po.order.mint) ?? { buy: false, sell: false };
        if (po.order.side === 'buy') rec.buy = true;
        else rec.sell = true;
        pendingByMint.set(po.order.mint, rec);
      }

      // Pacing: how far through the window are we, and is the coin count lagging?
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

      // ---- Exits first. ----
      for (const [mint, o] of held) {
        const pos = state.positions.get(mint);
        const curve = state.curves.get(mint);
        const pend = pendingByMint.get(mint);
        if (pos === undefined) {
          if (pend === undefined) {
            held.delete(mint);
            const b = bookOf(mint);
            b.lastExitSlot = now;
            b.filledSol = 0;
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
        const b = bookOf(mint);
        let exit = o.exiting;
        if (!exit && cost > 0 && value < cost * (1 - p.stopLoss)) exit = true;
        if (!exit && cost > 0 && o.peakValueSol >= cost * (1 + p.trailArm) && value < o.peakValueSol * (1 - p.trailGiveback)) exit = true;
        if (!exit && now - o.openedSlot > p.maxHoldSlots) exit = true;
        if (!exit && b.devSold) exit = true;
        if (!exit && windowStats(b, now).biggestSell >= p.bigSellSol) exit = true;
        if (!exit && flowOver(b, now, p.flowWindowSlots) < -p.exitFlowSol) exit = true;
        if (!exit && now - b.lastTradeSlot > p.staleSlots) exit = true;
        if (!exit) continue;
        o.exiting = true;
        if (pend?.sell) continue;
        if (orders.length >= cap) break;
        orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
      }

      // ---- Entries, plus one add per coin into strength. ----
      let free = Number(state.freeCashLamports()) / LAMPORTS_PER_SOL;
      let slotsLeft = state.hello.caps.max_open_positions - state.committedPositionCount();
      for (const mint of traded) {
        if (orders.length >= cap) break;
        const curve = state.curves.get(mint);
        if (curve === undefined || curve.complete) continue;
        if (pendingByMint.has(mint)) continue;
        const b = books.get(mint)!;
        const price = Number(curve.virtual_quote_reserves) / Number(curve.virtual_token_reserves);
        const realSol = Number(curve.real_quote_reserves) / LAMPORTS_PER_SOL;
        const pos = held.get(mint);

        if (pos !== undefined) {
          // Add once into strength: price stepped up, inflow still positive.
          if (pos.exiting || b.filledSol >= p.maxPerCoinSol - 0.005) continue;
          if (price < pos.lastBuyPrice * (1 + p.addStepUp)) continue;
          if (flowOver(b, now, p.windowSlots) < p.minNetInflowSol / 2) continue;
          const capSol = Number(state.maxBuyLamports(mint)) / LAMPORTS_PER_SOL;
          let amount = Math.min(capSol, p.addTicketSol, p.maxPerCoinSol - b.filledSol);
          if (amount > free - txSol) amount = free - txSol;
          if (amount < 0.01) continue;
          orders.push({ side: 'buy', mint, quote_lamports: BigInt(Math.floor(amount * LAMPORTS_PER_SOL)), max_slippage_bps: p.buySlippageBps });
          free -= amount + txSol;
          pos.lastBuyPrice = price;
          b.lastBuyPrice = price;
          continue;
        }

        // First entry on this coin.
        if (slotsLeft <= 0) continue;
        if (b.entries >= p.maxEntriesPerCoin || now - b.lastExitSlot < p.cooldownSlots) continue;
        const age = b.createdSlot < 0 ? 1e9 : now - b.createdSlot;
        const maxAge = desperate ? 600 : p.maxAgeSlots;
        if (age < p.minAgeSlots || age > maxAge) continue;
        if (realSol < (desperate ? 2 : p.minRealSol) || realSol > p.maxRealSol) continue;
        if (b.devSold) continue;
        const s = windowStats(b, now);
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
        held.set(mint, { openedSlot: now, peakValueSol: 0, lastBuyPrice: price, exiting: false });
        b.entries++;
        b.lastBuyPrice = price;
      }
      return orders;
    },
  };
}
