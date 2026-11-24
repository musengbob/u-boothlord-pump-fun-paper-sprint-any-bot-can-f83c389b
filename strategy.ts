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
// last 60 slots, net inflow >= 4 SOL and accelerating (the newer half of the
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
//
// Speed: the entry window's stats are maintained INCREMENTALLY. Each coin
// keeps running aggregates (buys, distinct buyers, net inflow split into
// older/newer halves, window high, biggest sell) that are updated in O(1)
// amortized per trade at ingest, and the window slides forward by advancing
// two pointers (first in-window tick, first newer-half tick) — a tick is
// added to, moved between, or removed from the aggregates exactly once over
// its life, so no per-batch rescan of any tick list ever happens. Window
// stats are snapshotted once per batch per coin and shared by the exit and
// entry checks. Dead coins are pruned from the book map. Decisions are
// byte-identical to the rescanning version: same parameters, same thresholds,
// same orders.

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
  minNetInflowSol: 4,
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
/** Prune a book once its coin has not traded for this many slots (and it is
 *  not held and has no pending order). Far above the oldest age any entry
 *  check can accept (maxAgeSlots 150, 600 when desperate), so a pruned coin
 *  could not have been entered anyway: its recreated book has age 1e9. */
const PRUNE_AFTER_SLOTS = 10000;
/** How often (batches) the dead-book sweep runs. */
const PRUNE_EVERY_BATCHES = 256;
/** Compact a coin's tick array once its dead prefix reaches this length. */
const COMPACT_TICKS_AT = 1024;

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
  /** Ticks in slot order (the harness feeds slots strictly increasing, and a
   *  batch's events all carry its slot). `head` is the first retained tick;
   *  ticks before it were pruned and are dead. */
  ticks: Tick[];
  head: number;
  /** First tick index inside the entry window as of aggNow. */
  winStart: number;
  /** First tick index in the window's newer half as of aggNow. */
  midIdx: number;
  /** Slot the running aggregates were last advanced to. */
  aggNow: number;
  /** Running aggregates over the entry window (ticks winStart..end). */
  buys: number;
  buySol: number;
  sellSol: number;
  netOld: number;
  netNew: number;
  /** Distinct buyers inside the window, with their buy counts. */
  buyerCounts: Map<string, number>;
  /** Monotonic deques of tick indices, decreasing by price / sell size, for
   *  the window high and the biggest sell. Heads index the live front. */
  highD: number[];
  highHead: number;
  sellD: number[];
  sellHead: number;
  /** Window stats snapshotted once per batch (shared by exit + entry). */
  cachedStats: WindowStats | null;
  cachedNow: number;
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
  let batchNo = 0;
  const keepSlots = Math.max(p.windowSlots, p.flowWindowSlots) + 10;

  function bookOf(mint: Pubkey): CoinBook {
    let b = books.get(mint);
    if (b === undefined) {
      b = {
        createdSlot: -1,
        creator: '',
        ticks: [],
        head: 0,
        winStart: 0,
        midIdx: 0,
        aggNow: -1,
        buys: 0,
        buySol: 0,
        sellSol: 0,
        netOld: 0,
        netNew: 0,
        buyerCounts: new Map<string, number>(),
        highD: [],
        highHead: 0,
        sellD: [],
        sellHead: 0,
        cachedStats: null,
        cachedNow: -1,
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

  /**
   * Slide the coin's running window aggregates forward to `now`: drop ticks
   * that left the 60-slot entry window (from the aggregates and from the
   * monotonic deque fronts) and move ticks that crossed the window's
   * midpoint from the newer half to the older half. Each tick crosses each
   * boundary once, so this is O(1) amortized per tick over the run.
   */
  function advanceTo(b: CoinBook, now: number): void {
    if (now <= b.aggNow) return;
    const from = now - p.windowSlots;
    const mid = now - p.windowSlots / 2;
    const ticks = b.ticks;
    // Midpoint first: move ticks that crossed it from the newer half to the
    // older half, so every tick the window loop below drops is already in
    // the older half (slot <= from < mid) and netOld is the right bucket.
    while (b.midIdx < ticks.length && ticks[b.midIdx]!.slot <= mid) {
      const t = ticks[b.midIdx]!;
      const signed = t.buy ? t.sol : -t.sol;
      b.netNew -= signed;
      b.netOld += signed;
      b.midIdx++;
    }
    // Then drop ticks that left the window entirely.
    while (b.winStart < ticks.length && ticks[b.winStart]!.slot <= from) {
      const t = ticks[b.winStart]!;
      const signed = t.buy ? t.sol : -t.sol;
      b.netOld -= signed;
      if (t.buy) {
        b.buys--;
        b.buySol -= t.sol;
        const c = (b.buyerCounts.get(t.user) ?? 0) - 1;
        if (c <= 0) b.buyerCounts.delete(t.user);
        else b.buyerCounts.set(t.user, c);
      } else {
        b.sellSol -= t.sol;
      }
      b.winStart++;
    }
    while (b.highHead < b.highD.length && b.highD[b.highHead]! < b.winStart) b.highHead++;
    while (b.sellHead < b.sellD.length && b.sellD[b.sellHead]! < b.winStart) b.sellHead++;
    b.aggNow = now;
  }

  /**
   * Record one trade tick, updating the running window aggregates for `now`
   * in O(1) amortized. Ticks arrive in slot order and a batch's events all
   * carry its slot, so a fresh tick is always inside the window, in the
   * newer half; the older-half / pre-window branches exist only so the
   * accounting stays exact if an old tick ever arrives.
   */
  function addTick(b: CoinBook, t: Tick, now: number): void {
    advanceTo(b, now);
    // A new tick changes the aggregates, so any snapshot taken at this
    // `now` is stale; the next statsOf recomputes it.
    b.cachedStats = null;
    b.cachedNow = -1;
    const idx = b.ticks.length;
    b.ticks.push(t);
    if (t.slot <= now - p.windowSlots) {
      // Pre-window: it pins the window-start price only, never the sums.
      // Every earlier tick is pre-window too (slot order), so both
      // boundaries sit at idx and move past this tick.
      b.winStart = idx + 1;
      b.midIdx = idx + 1;
      return;
    }
    if (t.buy) {
      b.buys++;
      b.buySol += t.sol;
      b.buyerCounts.set(t.user, (b.buyerCounts.get(t.user) ?? 0) + 1);
    } else {
      b.sellSol += t.sol;
    }
    if (t.slot <= now - p.windowSlots / 2) {
      b.netOld += t.buy ? t.sol : -t.sol;
      b.midIdx = idx + 1;
    } else {
      b.netNew += t.buy ? t.sol : -t.sol;
    }
    while (b.highD.length > b.highHead && b.ticks[b.highD[b.highD.length - 1]!]!.price <= t.price) b.highD.pop();
    b.highD.push(idx);
    if (!t.buy) {
      while (b.sellD.length > b.sellHead && b.ticks[b.sellD[b.sellD.length - 1]!]!.sol <= t.sol) b.sellD.pop();
      b.sellD.push(idx);
    }
  }

  /**
   * Stats over the entry window ending at `now`, split into older/newer
   * halves — the same values the old rescanning windowStats computed, read
   * off the running aggregates. Snapshotted once per batch per coin: the
   * exit and entry checks in one batch share the object. Net flow over the
   * last 30 slots is netNew, over the last 60 slots is net.
   */
  function statsOf(b: CoinBook, now: number): WindowStats {
    if (b.cachedNow === now && b.cachedStats !== null) return b.cachedStats;
    advanceTo(b, now);
    const n = b.ticks.length;
    const st: WindowStats = {
      buys: b.buys,
      buyers: b.buyerCounts.size,
      net: b.buySol - b.sellSol,
      netOld: b.netOld,
      netNew: b.netNew,
      // Window-start price: the newest retained tick at or before the
      // window edge, else the oldest in-window tick's price.
      startPrice: b.winStart > b.head
        ? b.ticks[b.winStart - 1]!.price
        : b.winStart < n
          ? b.ticks[b.winStart]!.price
          : -1,
      high: b.highHead < b.highD.length ? b.ticks[b.highD[b.highHead]!]!.price : 0,
      last: n > b.winStart ? b.ticks[n - 1]!.price : 0,
      biggestSell: b.sellHead < b.sellD.length ? b.ticks[b.sellD[b.sellHead]!]!.sol : 0,
    };
    b.cachedStats = st;
    b.cachedNow = now;
    return st;
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
      batchNo++;

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
          addTick(b, {
            slot: e.slot,
            buy: e.is_buy,
            sol: Number(e.sol_amount) / LAMPORTS_PER_SOL,
            user: e.user,
            price,
          }, now);
          b.lastTradeSlot = e.slot;
          traded.add(e.mint);
          if (!e.is_buy && e.user === b.creator) b.devSold = true;
        } else {
          const b = books.get(e.mint);
          if (b !== undefined) b.lastTradeSlot = e.slot;
        }
      }
      // Drop ticks older than any window we still consult (prune head only;
      // the aggregates already exclude them).
      for (const mint of traded) {
        const b = books.get(mint)!;
        while (b.head < b.ticks.length && b.ticks[b.head]!.slot <= now - keepSlots) b.head++;
        if (b.head >= COMPACT_TICKS_AT) {
          const cut = b.head;
          while (b.highHead < b.highD.length && b.highD[b.highHead]! < cut) b.highHead++;
          while (b.sellHead < b.sellD.length && b.sellD[b.sellHead]! < cut) b.sellHead++;
          b.ticks = b.ticks.slice(cut);
          b.winStart -= cut;
          b.midIdx -= cut;
          b.head = 0;
          if (b.highHead > 0) { b.highD = b.highD.slice(b.highHead); b.highHead = 0; }
          if (b.sellHead > 0) { b.sellD = b.sellD.slice(b.sellHead); b.sellHead = 0; }
          for (let i = 0; i < b.highD.length; i++) b.highD[i]! -= cut;
          for (let i = 0; i < b.sellD.length; i++) b.sellD[i]! -= cut;
        }
      }

      const marks = new Map(batch.marks.map((m) => [m.mint, m] as const));
      const pendingByMint = new Map<Pubkey, { buy: boolean; sell: boolean }>();
      for (const po of state.pending.values()) {
        const rec = pendingByMint.get(po.order.mint) ?? { buy: false, sell: false };
        if (po.order.side === 'buy') rec.buy = true;
        else rec.sell = true;
        pendingByMint.set(po.order.mint, rec);
      }

      // Prune dead books: a coin that has not traded for a long time, is not
      // held and has no pending order can never be entered again (its age
      // alone blocks it), so dropping its book changes no decision.
      if (batchNo % PRUNE_EVERY_BATCHES === 0) {
        for (const [mint, b] of books) {
          if (held.has(mint)) continue;
          if (pendingByMint.has(mint)) continue;
          if (now - b.lastTradeSlot <= PRUNE_AFTER_SLOTS) continue;
          books.delete(mint);
        }
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
        if (!exit && statsOf(b, now).biggestSell >= p.bigSellSol) exit = true;
        if (!exit && statsOf(b, now).netNew < -p.exitFlowSol) exit = true;
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
          if (statsOf(b, now).net < p.minNetInflowSol / 2) continue;
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
        const s = statsOf(b, now);
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
