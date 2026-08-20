from __future__ import annotations

import asyncio
import csv
import html
import json
import math
import re
import time
import xml.etree.ElementTree as ET
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import aiohttp

KRAKEN_WS = "wss://ws.kraken.com/v2"
KRAKEN_PAIRS = "https://api.kraken.com/0/public/AssetPairs"
KRAKEN_OHLC = "https://api.kraken.com/0/public/OHLC"
BINANCE_24H = "https://api.binance.com/api/v3/ticker/24hr"
BINANCE_KLINES = "https://api.binance.com/api/v3/klines"
COINBASE_PRODUCTS = "https://api.exchange.coinbase.com/products"
COINBASE_WS = "wss://advanced-trade-ws.coinbase.com"

TAG_RE = re.compile(r"<[^>]+>")
WORD_RE = re.compile(r"[^a-z0-9]+")

DEFAULT_ALIASES = {
    "BTC": ["bitcoin", "btc"], "ETH": ["ethereum", "ether", "eth"],
    "LINK": ["chainlink"], "SOL": ["solana"], "XRP": ["ripple", "xrp"],
    "ADA": ["cardano"], "DOGE": ["dogecoin"], "AVAX": ["avalanche"],
    "DOT": ["polkadot"], "UNI": ["uniswap"], "AAVE": ["aave"],
    "ONDO": ["ondo finance", "ondo"], "SHIB": ["shiba inu"],
    "LTC": ["litecoin"], "BCH": ["bitcoin cash"], "TRX": ["tron"],
    "SUI": ["sui"], "APT": ["aptos"], "ARB": ["arbitrum"],
    "OP": ["optimism"], "NEAR": ["near protocol"], "ATOM": ["cosmos"],
    "INJ": ["injective"], "TAO": ["bittensor"], "HYPE": ["hyperliquid"],
    "XMR": ["monero"], "PEPE": ["pepe"], "RENDER": ["render network", "render token"],
}

POSITIVE_CATALYST_WORDS = {
    "listing": 26, "listed": 24, "launch": 18, "integration": 18,
    "partnership": 16, "partner": 12, "approval": 24, "approved": 24,
    "etf": 22, "mainnet": 20, "upgrade": 14, "buyback": 24,
    "burn": 16, "adoption": 18, "institutional": 16, "treasury": 14,
    "staking": 12, "record": 12, "expands": 12, "support": 10,
}
NEGATIVE_CATALYST_WORDS = {
    "hack": 30, "exploit": 30, "delist": 28, "delisting": 28,
    "investigation": 22, "lawsuit": 22, "unlock": 16, "dilution": 24,
    "offering": 18, "breach": 26, "outage": 18, "suspends": 20,
    "suspended": 20, "warning": 12, "charges": 20,
}


@dataclass
class Signal:
    symbol: str
    base: str
    price: float
    bid: float
    ask: float
    spread_pct: float
    chg_5m: float | None
    chg_15m: float | None
    chg_1h: float | None
    chg_4h: float | None
    chg_24h: float
    chg_7d: float | None
    chg_30d: float | None
    volume_24h_eur: float
    volume_accel_15m: float | None
    volume_accel_5m: float | None
    trade_accel_5m: float | None
    taker_buy_ratio_5m: float | None
    compression_ratio: float | None
    room_to_high_pct: float
    btc_relative_15m: float | None
    higher_low: bool | None
    breakout_pct: float | None
    breakout_age_min: float | None
    rsi_14: float | None
    atr_pct: float | None
    support: float | None
    resistance: float | None
    reward_risk: float | None
    cross_exchange_lead_5m: float | None
    phase: str
    pre_momentum_score: int
    technical_score: int
    consensus_score: int
    catalyst_score: int
    confidence: int
    status: str
    reasons: list[str]
    risks: list[str]
    data_quality: str
    timestamp: str
    binance_available: bool
    binance_price: float | None
    binance_chg_5m: float | None
    binance_chg_15m: float | None
    coinbase_available: bool
    coinbase_price: float | None
    exchange_confirmations: int
    source_count: int
    catalyst_title: str | None
    catalyst_source: str | None
    catalyst_age_min: float | None
    catalyst_url: str | None
    catalyst_sentiment: str | None


class Scanner:
    def __init__(self, cfg: dict[str, Any], root: Path):
        self.cfg = cfg
        self.root = root
        self.tickers: dict[str, dict[str, Any]] = {}
        self.pair_map: dict[str, str] = {}
        self.base_to_kraken: dict[str, str] = {}
        self.price_hist = defaultdict(lambda: deque(maxlen=30000))
        self.connected = False
        self.last_error = ""
        self._logged: dict[tuple[str, str], float] = {}
        self.candles_1m: dict[str, list[dict[str, float]]] = {}
        self.candles_60m: dict[str, list[dict[str, float]]] = {}
        self.enriched_at: dict[str, float] = {}
        self._session: aiohttp.ClientSession | None = None
        self._sem = asyncio.Semaphore(int(cfg.get("enrichment_concurrency", 5)))

        self.binance: dict[str, dict[str, Any]] = {}
        self.binance_connected = False
        self.binance_error = ""
        self.binance_updated = 0.0

        self.coinbase: dict[str, dict[str, Any]] = {}
        self.coinbase_connected = False
        self.coinbase_error = ""
        self.coinbase_updated = 0.0
        self.coinbase_products: set[str] = set()

        self.news_items: deque[dict[str, Any]] = deque(maxlen=700)
        self.catalysts: dict[str, dict[str, Any]] = {}
        self.news_connected = False
        self.news_error = ""
        self.news_updated = 0.0

        self.aliases = {k: list(v) for k, v in DEFAULT_ALIASES.items()}
        for k, v in cfg.get("asset_aliases", {}).items():
            self.aliases[str(k).upper()] = [str(x).lower() for x in v]

    @staticmethod
    def _base(sym: str) -> str:
        return sym.split("/")[0].upper() if "/" in sym else sym.upper()

    async def discover(self, session: aiohttp.ClientSession) -> list[str]:
        async with session.get(KRAKEN_PAIRS, timeout=20) as r:
            j = await r.json()
        out = []
        q = self.cfg.get("quote_currency", "EUR")
        for k, v in j.get("result", {}).items():
            ws = v.get("wsname")
            if ws and ws.endswith("/" + q) and ".d" not in ws.lower():
                out.append(ws)
                self.pair_map[ws] = v.get("altname") or k
                self.base_to_kraken[self._base(ws)] = ws
        return sorted(set(out))

    async def run(self):
        while True:
            tasks: list[asyncio.Task] = []
            try:
                timeout = aiohttp.ClientTimeout(total=25)
                async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": "VoltTrade/0.6-premomentum"}) as session:
                    self._session = session
                    symbols = await self.discover(session)
                    tasks = [
                        asyncio.create_task(self.enrichment_loop()),
                        asyncio.create_task(self.binance_loop()),
                        asyncio.create_task(self.coinbase_loop()),
                        asyncio.create_task(self.news_loop()),
                    ]
                    async with session.ws_connect(KRAKEN_WS, heartbeat=20, receive_timeout=90) as ws:
                        self.connected = True
                        self.last_error = ""
                        for i in range(0, len(symbols), 100):
                            await ws.send_json({"method": "subscribe", "params": {"channel": "ticker", "symbol": symbols[i:i + 100], "snapshot": True}})
                        async for msg in ws:
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                continue
                            data = json.loads(msg.data)
                            if data.get("channel") != "ticker":
                                continue
                            for t in data.get("data", []):
                                self.ingest(t)
            except Exception as e:
                self.connected = False
                self.last_error = f"{type(e).__name__}: {e}"
                await asyncio.sleep(4)
            finally:
                for t in tasks:
                    t.cancel()

    def ingest(self, t: dict[str, Any]):
        sym = t.get("symbol")
        p = float(t.get("last") or 0)
        now = time.time()
        if not sym or p <= 0:
            return
        self.tickers[sym] = t
        self.price_hist[sym].append((now, p))
        cutoff = now - self.cfg.get("history_minutes", 360) * 60
        while self.price_hist[sym] and self.price_hist[sym][0][0] < cutoff:
            self.price_hist[sym].popleft()

    async def binance_loop(self):
        while True:
            try:
                assert self._session
                async with self._session.get(BINANCE_24H, timeout=20) as r:
                    arr = await r.json()
                allowed = set(self.base_to_kraken)
                rows = []
                for x in arr if isinstance(arr, list) else []:
                    s = str(x.get("symbol", ""))
                    if not s.endswith("USDT"):
                        continue
                    base = s[:-4].upper()
                    if base not in allowed:
                        continue
                    try:
                        p = float(x.get("lastPrice") or 0)
                        c24 = float(x.get("priceChangePercent") or 0)
                        qv = float(x.get("quoteVolume") or 0)
                    except Exception:
                        continue
                    if p <= 0 or qv < self.cfg.get("binance_min_quote_volume_usdt", 1_000_000):
                        continue
                    # Discovery must not be biased toward already-pumped assets.
                    if c24 < self.cfg.get("premomentum_24h_floor_pct", -4) or c24 > self.cfg.get("premomentum_discovery_24h_ceiling_pct", 10):
                        continue
                    priority = math.log10(max(qv, 1)) - abs(c24) * 0.10
                    rows.append((priority, base, s, p, c24, qv))
                rows.sort(reverse=True)
                rows = rows[:int(self.cfg.get("binance_candidates", 90))]
                for _, base, s, p, c24, qv in rows:
                    d = self.binance.get(base, {})
                    d.update({"symbol": s, "price": p, "chg24": c24, "quote_volume": qv, "updated": time.time()})
                    self.binance[base] = d

                for _, base, s, _, _, _ in rows[:int(self.cfg.get("binance_deep_candidates", 65))]:
                    try:
                        async with self._sem:
                            async with self._session.get(BINANCE_KLINES, params={"symbol": s, "interval": "1m", "limit": "120"}, timeout=15) as r:
                                k = await r.json()
                        if not isinstance(k, list):
                            continue
                        candles = []
                        for x in k:
                            if not isinstance(x, list) or len(x) < 10:
                                continue
                            candles.append({
                                "ts": float(x[0]) / 1000,
                                "open": float(x[1]), "high": float(x[2]), "low": float(x[3]), "close": float(x[4]),
                                "volume": float(x[5]), "count": float(x[8]), "taker_buy_volume": float(x[9]),
                            })
                        if len(candles) >= 90:
                            d = self.binance.setdefault(base, {})
                            d.update(self._candle_metrics(candles))
                            d["candles"] = candles
                    except Exception:
                        pass
                self.binance_connected = True
                self.binance_error = ""
                self.binance_updated = time.time()
            except Exception as e:
                self.binance_connected = False
                self.binance_error = f"{type(e).__name__}: {e}"
            await asyncio.sleep(float(self.cfg.get("binance_refresh_seconds", 25)))

    async def coinbase_loop(self):
        while True:
            try:
                assert self._session
                if not self.coinbase_products:
                    async with self._session.get(COINBASE_PRODUCTS, timeout=20) as r:
                        products = await r.json()
                    if isinstance(products, list):
                        self.coinbase_products = {
                            x.get("id") for x in products
                            if x.get("id") and x.get("quote_currency") in ("USD", "USDC") and x.get("status") in ("online", None)
                        }
                ids = []
                for base in self.base_to_kraken:
                    for q in ("USD", "USDC"):
                        pid = f"{base}-{q}"
                        if pid in self.coinbase_products:
                            ids.append(pid)
                            break
                ids.sort(key=lambda pid: (pid.split("-")[0] not in self.binance, pid))
                ids = ids[:350]
                async with self._session.ws_connect(COINBASE_WS, heartbeat=20, receive_timeout=90) as ws:
                    self.coinbase_connected = True
                    self.coinbase_error = ""
                    await ws.send_json({"type": "subscribe", "channel": "heartbeats"})
                    for i in range(0, len(ids), 100):
                        await ws.send_json({"type": "subscribe", "product_ids": ids[i:i + 100], "channel": "ticker_batch"})
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        j = json.loads(msg.data)
                        if j.get("channel") != "ticker_batch":
                            continue
                        for ev in j.get("events", []):
                            for t in ev.get("tickers", []):
                                pid = t.get("product_id", "")
                                base = pid.split("-")[0].upper()
                                try:
                                    self.coinbase[base] = {
                                        "product_id": pid,
                                        "price": float(t.get("price") or 0),
                                        "chg24": float(t.get("price_percent_chg_24_h") or 0),
                                        "volume24": float(t.get("volume_24_h") or 0),
                                        "updated": time.time(),
                                    }
                                    self.coinbase_updated = time.time()
                                except Exception:
                                    pass
            except Exception as e:
                self.coinbase_connected = False
                self.coinbase_error = f"{type(e).__name__}: {e}"
                await asyncio.sleep(float(self.cfg.get("coinbase_reconnect_seconds", 10)))

    # ---------- Catalyst / news engine ----------
    @staticmethod
    def _strip_html(s: str) -> str:
        return html.unescape(TAG_RE.sub(" ", s or "")).strip()

    @staticmethod
    def _parse_date(raw: str | None) -> float:
        if not raw:
            return time.time()
        try:
            return parsedate_to_datetime(raw).timestamp()
        except Exception:
            pass
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
        except Exception:
            return time.time()

    @classmethod
    def _parse_feed(cls, raw: bytes, source: str, tier: str) -> list[dict[str, Any]]:
        root = ET.fromstring(raw)
        out = []
        for node in list(root.findall(".//item")) + list(root.findall(".//{*}entry")):
            def txt(names: list[str]) -> str:
                for name in names:
                    x = node.find(name)
                    if x is None:
                        x = node.find("{*}" + name)
                    if x is not None and x.text:
                        return x.text.strip()
                return ""
            title = cls._strip_html(txt(["title"]))
            summary = cls._strip_html(txt(["description", "summary", "content"]))
            link = txt(["link"])
            if not link:
                lx = node.find("{*}link")
                if lx is not None:
                    link = lx.attrib.get("href", "")
            published = txt(["pubDate", "published", "updated", "date"])
            if title:
                out.append({"title": title, "summary": summary[:500], "url": link, "published_ts": cls._parse_date(published), "source": source, "tier": tier})
        return out

    @staticmethod
    def _classify_catalyst(item: dict[str, Any]) -> dict[str, Any]:
        text = (item.get("title", "") + " " + item.get("summary", "")).lower()
        pos = max([w for k, w in POSITIVE_CATALYST_WORDS.items() if k in text] or [0])
        neg = max([w for k, w in NEGATIVE_CATALYST_WORDS.items() if k in text] or [0])
        tier_score = {"A": 35, "B": 28, "C": 18}.get(item.get("tier", "B"), 24)
        age_min = max(0.0, (time.time() - float(item.get("published_ts") or time.time())) / 60)
        if age_min <= 10:
            freshness = 30
        elif age_min <= 30:
            freshness = 24
        elif age_min <= 90:
            freshness = 16
        elif age_min <= 240:
            freshness = 8
        else:
            freshness = 0
        sentiment = "negative" if neg > pos else ("positive" if pos > 0 else "neutral")
        impact = max(pos, neg)
        score = min(100, tier_score + freshness + impact)
        return {**item, "age_min": age_min, "sentiment": sentiment, "score": score, "impact": impact}

    def _asset_mentions(self, base: str, text: str) -> bool:
        low = text.lower()
        aliases = self.aliases.get(base, [])
        for a in aliases:
            if a and re.search(r"(?<![a-z0-9])" + re.escape(a.lower()) + r"(?![a-z0-9])", low):
                return True
        # Symbol-only matching only for less ambiguous tickers and uppercase occurrences.
        if len(base) >= 4 and base not in {"LINK", "NEAR", "ATOM", "RENDER"}:
            return re.search(r"(?<![A-Z0-9])" + re.escape(base) + r"(?![A-Z0-9])", text) is not None
        return False

    def _rebuild_catalyst_cache(self):
        now = time.time()
        recent = [x for x in self.news_items if now - x.get("published_ts", now) <= self.cfg.get("news_max_age_hours", 12) * 3600]
        result: dict[str, dict[str, Any]] = {}
        for base in self.base_to_kraken:
            best = None
            for raw in recent:
                text = raw.get("title", "") + " " + raw.get("summary", "")
                if not self._asset_mentions(base, text):
                    continue
                c = self._classify_catalyst(raw)
                # Neutral stories remain context, but should not dominate actual catalysts.
                adjusted = c["score"] - (18 if c["sentiment"] == "neutral" else 0)
                if best is None or adjusted > best["_adjusted"]:
                    best = {**c, "_adjusted": adjusted}
            if best:
                best.pop("_adjusted", None)
                result[base] = best
        self.catalysts = result

    async def news_loop(self):
        await asyncio.sleep(2)
        while True:
            errors = []
            try:
                assert self._session
                fresh: list[dict[str, Any]] = []
                sources = self.cfg.get("news_sources", [])
                for src in sources:
                    try:
                        async with self._session.get(src["url"], timeout=18) as r:
                            raw = await r.read()
                        if r.status >= 400:
                            raise RuntimeError(f"HTTP {r.status}")
                        fresh.extend(self._parse_feed(raw, src.get("name", "News"), src.get("tier", "B")))
                    except Exception as e:
                        errors.append(f"{src.get('name','?')}: {type(e).__name__}")
                # Deduplicate by title+source and keep newest first.
                merged = {(x.get("source"), x.get("title")): x for x in list(self.news_items) + fresh}
                ordered = sorted(merged.values(), key=lambda x: x.get("published_ts", 0), reverse=True)[:700]
                self.news_items = deque(ordered, maxlen=700)
                self._rebuild_catalyst_cache()
                self.news_connected = len(fresh) > 0
                self.news_error = "; ".join(errors[:4])
                self.news_updated = time.time()
            except Exception as e:
                self.news_connected = False
                self.news_error = f"{type(e).__name__}: {e}"
            await asyncio.sleep(float(self.cfg.get("news_refresh_seconds", 90)))

    # ---------- Kraken enrichment ----------
    async def enrichment_loop(self):
        await asyncio.sleep(3)
        while True:
            try:
                shortlist = self._shortlist()
                tasks = [asyncio.create_task(self._enrich_symbol(s, deep=(i < int(self.cfg.get("deep_candidates", 28))))) for i, s in enumerate(shortlist)]
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                self.last_error = f"enrichment: {type(e).__name__}: {e}"
            await asyncio.sleep(float(self.cfg.get("enrichment_interval_seconds", 60)))

    def _shortlist(self) -> list[str]:
        rows = []
        for sym, t in self.tickers.items():
            p = float(t.get("last") or 0)
            bid = float(t.get("bid") or p)
            ask = float(t.get("ask") or p)
            if p <= 0:
                continue
            spread = (ask - bid) / p * 100 if p else 999
            c24 = float(t.get("change_pct") or 0)
            v = float(t.get("volume") or 0)
            eur = v * p
            if spread > self.cfg.get("prefilter_max_spread_pct", 1.2) or eur < self.cfg.get("min_24h_volume_eur", 75000):
                continue
            if c24 < self.cfg.get("prefilter_24h_min_pct", -4) or c24 > self.cfg.get("prefilter_24h_max_pct", 12):
                continue
            base = self._base(sym)
            bx = self.binance.get(base, {})
            # Prioritize activity acceleration even when price has barely moved.
            boost = 0.0
            if (bx.get("vacc5") or 0) >= 1.4:
                boost += 7
            if (bx.get("tacc5") or 0) >= 1.4:
                boost += 6
            if (bx.get("taker_buy_ratio5") or 0) >= 0.57:
                boost += 4
            if base in self.catalysts:
                boost += self.catalysts[base].get("score", 0) / 12
            priority = math.log10(max(eur, 1)) - spread * 2 + boost - max(0, c24 - 5) * 0.5
            rows.append((priority, sym))
        rows.sort(reverse=True)
        return [s for _, s in rows[:int(self.cfg.get("enrich_candidates", 70))]]

    async def _fetch_ohlc(self, sym: str, interval: int) -> list[dict[str, float]]:
        if not self._session:
            return []
        pair = self.pair_map.get(sym)
        if not pair:
            return []
        async with self._sem:
            async with self._session.get(KRAKEN_OHLC, params={"pair": pair, "interval": str(interval)}, timeout=20) as r:
                j = await r.json()
        if j.get("error"):
            return []
        arr = next((v for k, v in j.get("result", {}).items() if k != "last" and isinstance(v, list)), None)
        if not arr:
            return []
        out = []
        for x in arr:
            if len(x) < 8:
                continue
            out.append({"ts": float(x[0]), "open": float(x[1]), "high": float(x[2]), "low": float(x[3]), "close": float(x[4]), "vwap": float(x[5]), "volume": float(x[6]), "count": float(x[7])})
        return out

    async def _enrich_symbol(self, sym: str, deep: bool = False):
        c1 = await self._fetch_ohlc(sym, 1)
        if c1:
            self.candles_1m[sym] = c1
        if deep:
            c60 = await self._fetch_ohlc(sym, 60)
            if c60:
                self.candles_60m[sym] = c60
        if c1 or deep:
            self.enriched_at[sym] = time.time()

    # ---------- Metrics ----------
    @staticmethod
    def _pct(a, b):
        return None if b in (None, 0) else (a / b - 1) * 100

    @staticmethod
    def _clamp(x, a, b):
        return max(a, min(b, x))

    @staticmethod
    def _block_ratio(values: list[float], recent_n: int, block_n: int, blocks: int = 6) -> float | None:
        if len(values) < recent_n + block_n * blocks:
            return None
        recent = sum(values[-recent_n:])
        bases = []
        end = len(values) - recent_n
        for i in range(blocks):
            b = end - i * block_n
            a = b - block_n
            if a < 0:
                break
            bases.append(sum(values[a:b]) * (recent_n / block_n))
        base = sum(bases) / len(bases) if bases else 0
        return recent / base if base > 0 else None

    @classmethod
    def _candle_metrics(cls, a: list[dict[str, float]]) -> dict[str, Any]:
        def pct(m):
            return None if len(a) <= m or a[-m - 1]["close"] == 0 else (a[-1]["close"] / a[-m - 1]["close"] - 1) * 100

        vols = [x.get("volume", 0.0) for x in a]
        counts = [x.get("count", 0.0) for x in a]
        vacc15 = cls._block_ratio(vols, 15, 15, 4)
        vacc5 = cls._block_ratio(vols, 5, 5, 6)
        tacc5 = cls._block_ratio(counts, 5, 5, 6) if any(counts) else None

        recent5 = a[-5:]
        total_v = sum(x.get("volume", 0.0) for x in recent5)
        buy_v = sum(x.get("taker_buy_volume", 0.0) for x in recent5)
        taker_ratio = buy_v / total_v if total_v > 0 and any("taker_buy_volume" in x for x in recent5) else None

        compression = None
        if len(a) >= 75:
            r15 = max(x["high"] for x in a[-15:]) - min(x["low"] for x in a[-15:])
            r45 = max(x["high"] for x in a[-60:-15]) - min(x["low"] for x in a[-60:-15])
            compression = r15 / r45 if r45 > 0 else None

        near_res = None
        if len(a) >= 45:
            prior_high = max(x["high"] for x in a[-45:-5])
            near_res = (a[-1]["close"] / prior_high - 1) * 100 if prior_high else None

        return {
            "chg5": pct(5), "chg15": pct(15), "chg1h": pct(60),
            "vacc": vacc15, "vacc5": vacc5, "tacc5": tacc5,
            "taker_buy_ratio5": taker_ratio, "compression": compression,
            "near_resistance_pct": near_res,
        }

    def _old_live(self, hist, secs):
        target = time.time() - secs
        best = None
        for ts, p in hist:
            if ts <= target:
                best = p
            else:
                break
        return best

    def _price_ago(self, sym, minutes):
        arr = self.candles_1m.get(sym, [])
        if len(arr) > minutes:
            return arr[-minutes - 1]["close"]
        return self._old_live(self.price_hist[sym], minutes * 60)

    def _hourly_ago(self, sym, hours):
        a = self.candles_60m.get(sym, [])
        return a[-hours - 1]["close"] if len(a) > hours else None

    def _volume_accel(self, sym):
        a = self.candles_1m.get(sym, [])
        if len(a) < 76:
            return None
        return self._candle_metrics(a)["vacc"]

    def _rsi(self, closes, period=14):
        if len(closes) < period + 1:
            return None
        gains, loss = [], []
        for a, b in zip(closes[-period - 1:-1], closes[-period:]):
            d = b - a
            gains.append(max(d, 0))
            loss.append(max(-d, 0))
        ag = sum(gains) / period
        al = sum(loss) / period
        if al == 0:
            return 100.0
        return 100 - (100 / (1 + ag / al))

    def _atr_pct(self, a, period=14):
        if len(a) < period + 1:
            return None
        trs = []
        for i in range(len(a) - period, len(a)):
            cur, prev = a[i], a[i - 1]["close"]
            trs.append(max(cur["high"] - cur["low"], abs(cur["high"] - prev), abs(cur["low"] - prev)))
        p = a[-1]["close"]
        return (sum(trs) / period) / p * 100 if p else None

    def _breakout_age(self, a: list[dict[str, float]]) -> float | None:
        if len(a) < 70:
            return None
        found = None
        start = max(45, len(a) - 35)
        for i in range(start, len(a)):
            prev = a[max(0, i - 40):i]
            if len(prev) < 25:
                continue
            level = max(x["high"] for x in prev)
            if a[i]["close"] > level * 1.001:
                found = a[i]["ts"]
                break
        return max(0, (time.time() - found) / 60) if found else None

    def _structure(self, sym, p):
        a = self.candles_1m.get(sym, [])
        if len(a) < 70:
            return (None,) * 7
        prior15, recent15 = a[-30:-15], a[-15:]
        higher_low = min(x["low"] for x in recent15) > min(x["low"] for x in prior15)
        base = a[-65:-5]
        resistance = max(x["high"] for x in base)
        support = min(x["low"] for x in a[-30:])
        breakout = (p / resistance - 1) * 100 if resistance else None
        closes = [x["close"] for x in a]
        return higher_low, breakout, support, resistance, self._rsi(closes[-40:], 14), self._atr_pct(a[-40:], 14), self._breakout_age(a)

    @classmethod
    def _score_premomentum(cls, *, chg15: float | None, chg24: float, vacc5: float | None,
                           tacc5: float | None, taker_ratio: float | None, compression: float | None,
                           near_resistance_pct: float | None, cross_lead5: float | None,
                           higher_low: bool | None, catalyst: dict[str, Any] | None) -> tuple[int, list[str], list[str]]:
        """Leading-signal score. Intentionally rewards activity before price expansion."""
        s = 0
        reasons: list[str] = []
        risks: list[str] = []

        # Price still quiet is a feature here, not a bug.
        if chg15 is not None and -0.45 <= chg15 <= 1.20:
            s += 10
            reasons.append("price still quiet")
        elif chg15 is not None and chg15 > 2.5:
            risks.append("price already moving")

        if -2 <= chg24 <= 6:
            s += 5
        elif chg24 > 10:
            risks.append("24h move already extended")

        if vacc5 is not None:
            if vacc5 >= 2.2:
                s += 18; reasons.append(f"5m volume acceleration {vacc5:.1f}x")
            elif vacc5 >= 1.45:
                s += 13; reasons.append(f"5m volume building {vacc5:.1f}x")
            elif vacc5 < 0.75:
                risks.append("short-term volume fading")

        if tacc5 is not None:
            if tacc5 >= 2.0:
                s += 15; reasons.append(f"trade activity acceleration {tacc5:.1f}x")
            elif tacc5 >= 1.4:
                s += 10; reasons.append(f"trade activity building {tacc5:.1f}x")

        if taker_ratio is not None:
            if taker_ratio >= 0.62:
                s += 12; reasons.append(f"aggressive buys {taker_ratio*100:.0f}%")
            elif taker_ratio >= 0.56:
                s += 8; reasons.append(f"buy pressure {taker_ratio*100:.0f}%")
            elif taker_ratio < 0.44:
                risks.append("sell-side aggression")

        if compression is not None:
            if compression <= 0.55:
                s += 11; reasons.append("volatility compression")
            elif compression <= 0.75:
                s += 7; reasons.append("range tightening")

        if near_resistance_pct is not None and -1.0 <= near_resistance_pct <= 0.15:
            s += 7; reasons.append("pressure below resistance")

        if cross_lead5 is not None:
            if 0.20 <= cross_lead5 <= 1.50:
                s += 8; reasons.append("Binance leading Kraken")
            elif cross_lead5 > 2.5:
                risks.append("lead already too large")

        if higher_low is True:
            s += 5; reasons.append("higher low forming")

        if catalyst:
            cscore = int(catalyst.get("score") or 0)
            sentiment = catalyst.get("sentiment")
            age = float(catalyst.get("age_min") or 999)
            if sentiment == "positive" and age <= 240:
                add = round(min(24, cscore * 0.24))
                s += add
                reasons.append(f"fresh positive catalyst {age:.0f}m")
            elif sentiment == "negative" and age <= 360:
                s -= min(35, round(cscore * 0.35))
                risks.append(f"negative catalyst {age:.0f}m")

        return round(cls._clamp(s, 0, 100)), reasons, risks

    def analyze(self, sym: str) -> Signal:
        t = self.tickers[sym]
        base = self._base(sym)
        p = float(t.get("last") or 0)
        bid = float(t.get("bid") or p)
        ask = float(t.get("ask") or p)
        spread = (ask - bid) / p * 100 if p else 999
        c5 = self._pct(p, self._price_ago(sym, 5))
        c15 = self._pct(p, self._price_ago(sym, 15))
        c1h = self._pct(p, self._price_ago(sym, 60))
        c4h = self._pct(p, self._price_ago(sym, 240))
        c24 = float(t.get("change_pct") or 0)
        c7 = self._pct(p, self._hourly_ago(sym, 24 * 7))
        c30 = self._pct(p, self._hourly_ago(sym, 24 * 30 - 1))
        high = float(t.get("high") or p)
        room = (high / p - 1) * 100 if p else 0
        v24 = float(t.get("volume") or 0)
        veur = v24 * p
        vacc = self._volume_accel(sym)

        btc15 = None
        if sym != "BTC/EUR" and "BTC/EUR" in self.tickers:
            bp = float(self.tickers["BTC/EUR"].get("last") or 0)
            bc = self._pct(bp, self._price_ago("BTC/EUR", 15))
            if c15 is not None and bc is not None:
                btc15 = c15 - bc

        hl, breakout, support, resistance, rsi, atr, bo_age = self._structure(sym, p)
        bx = self.binance.get(base, {})
        cx = self.coinbase.get(base, {})
        catalyst = self.catalysts.get(base)

        b5, b15 = bx.get("chg5"), bx.get("chg15")
        bv15, bv5 = bx.get("vacc"), bx.get("vacc5")
        bt5, buy5, compression = bx.get("tacc5"), bx.get("taker_buy_ratio5"), bx.get("compression")
        near_res = bx.get("near_resistance_pct")
        cross_lead5 = (b5 - c5) if b5 is not None and c5 is not None else None

        fresh = sym in self.candles_1m and time.time() - self.enriched_at.get(sym, 0) < 240
        deep = sym in self.candles_60m
        quality = "FULL" if fresh and deep else ("INTRADAY" if fresh else "TICKER_ONLY")

        # Technical score remains a confirmation layer, no longer the first trigger.
        technical = 0
        tech_reasons: list[str] = []
        risks: list[str] = []
        if c5 is not None and 0.10 <= c5 <= 1.5:
            technical += 5
        if c15 is not None and 0.20 <= c15 <= 2.2:
            technical += 8
        if c1h is not None and 0.30 <= c1h <= 4.0:
            technical += 5
        if vacc is not None:
            if vacc >= 2.0:
                technical += 18; tech_reasons.append(f"15m volume {vacc:.1f}x")
            elif vacc >= 1.4:
                technical += 12
        if hl:
            technical += 8
        if breakout is not None:
            if -0.8 <= breakout < 0:
                technical += 8; tech_reasons.append("testing resistance")
            elif 0 <= breakout <= 1.2:
                technical += 10; tech_reasons.append("fresh breakout")
            elif breakout > 2.5:
                risks.append("breakout already extended")
        if btc15 is not None and btc15 >= 0.3:
            technical += 8
        if spread <= 0.35 and veur >= 150000:
            technical += 8
        elif spread <= 0.75:
            technical += 3
        if room >= 3:
            technical += 5

        rr = None
        if support and resistance and support < p:
            risk_amt = p - support
            target = max(high, resistance * (1 + self.cfg.get("target_extension_pct", 3.0) / 100))
            reward = max(0, target - p)
            rr = reward / risk_amt if risk_amt > 0 else None
            if rr >= 1.7:
                technical += 4
        technical = min(100, technical)

        confirmations = 1
        source_count = 1
        consensus = 0
        if bx:
            source_count += 1
            if b15 is not None and c15 is not None and abs(b15 - c15) <= 1.5:
                confirmations += 1; consensus += 28
            if (bv5 or 0) >= 1.4:
                consensus += 18
            if (bt5 or 0) >= 1.4:
                consensus += 14
        if cx:
            source_count += 1
            cbc24 = cx.get("chg24")
            if cbc24 is not None and abs(cbc24 - c24) <= 4.0:
                confirmations += 1; consensus += 22
        if confirmations >= 3:
            consensus += 8
        consensus = min(100, consensus)

        pre, pre_reasons, pre_risks = self._score_premomentum(
            chg15=c15, chg24=c24, vacc5=bv5, tacc5=bt5, taker_ratio=buy5,
            compression=compression, near_resistance_pct=near_res,
            cross_lead5=cross_lead5, higher_low=hl, catalyst=catalyst,
        )
        risks.extend(pre_risks)
        reasons = pre_reasons + tech_reasons

        catalyst_score = int(catalyst.get("score") or 0) if catalyst else 0
        catalyst_sentiment = catalyst.get("sentiment") if catalyst else None
        catalyst_age = float(catalyst.get("age_min")) if catalyst else None

        # Phase/status: PRE is deliberately allowed while price is still flat.
        phase = "BASE"
        if pre >= self.cfg.get("pre_score_alert", 68) and (c15 is None or c15 <= self.cfg.get("premomentum_max_15m_pct", 1.4)):
            phase = "PRE_MOMENTUM"
        elif c15 is not None and c15 > 0.4 and max(bv5 or 0, vacc or 0) >= 1.25:
            phase = "IGNITION"
        if hl and breakout is not None and -0.8 <= breakout < 0.4 and pre >= 50:
            phase = "SETUP"
        if breakout is not None and 0 <= breakout <= 1.2 and technical >= 55:
            phase = "MOMENTUM"
        if c24 > self.cfg.get("late_24h_pct", 12) or (c15 is not None and c15 > self.cfg.get("late_15m_pct", 3.5)) or (breakout is not None and breakout > 3.0) or (rsi is not None and rsi > 80):
            phase = "LATE"

        hard_reject = (
            spread > self.cfg.get("max_spread_pct", 1.5)
            or veur < self.cfg.get("hard_min_24h_volume_eur", 30000)
            or c24 < self.cfg.get("hard_24h_loss_pct", -10)
            or (catalyst_sentiment == "negative" and catalyst_score >= self.cfg.get("negative_catalyst_reject_score", 78) and (catalyst_age or 999) <= 360)
        )

        if quality == "TICKER_ONLY":
            status = "DATA"
        elif hard_reject:
            status = "REJECT"
        elif phase == "LATE":
            status = "LATE"
        else:
            pre_ready = (
                pre >= self.cfg.get("pre_score_alert", 68)
                and (c15 is None or c15 <= self.cfg.get("premomentum_max_15m_pct", 1.4))
                and c24 <= self.cfg.get("premomentum_max_24h_pct", 6.0)
                and spread <= self.cfg.get("premomentum_max_spread_pct", 0.75)
            )
            entry_window = (
                technical >= self.cfg.get("entry_technical_min", 65)
                and consensus >= self.cfg.get("entry_consensus_min", 50)
                and pre >= self.cfg.get("entry_pre_min", 55)
                and (breakout is None or breakout <= 1.5)
                and (bo_age is None or bo_age <= self.cfg.get("breakout_age_max_entry_min", 25))
            )
            setup = pre >= self.cfg.get("pre_score_setup", 55) and technical >= 40
            momentum = technical >= 55 and c15 is not None and c15 > 1.0
            if pre_ready:
                status = "PRE"
            elif entry_window:
                status = "ENTRY_WINDOW"
            elif setup:
                status = "SETUP"
            elif momentum:
                status = "MOMENTUM"
            elif pre >= 40 or technical >= 40:
                status = "WATCH"
            else:
                status = "IGNORE"

        confidence = round(self._clamp(pre * 0.48 + technical * 0.24 + consensus * 0.18 + catalyst_score * 0.10 - len(risks) * 3, 0, 100))
        if status == "WATCH":
            risks.insert(0, "watch only — leading conditions incomplete")
        if status == "SETUP":
            risks.insert(0, "setup forming — not yet an entry window")
        if status == "PRE":
            reasons.insert(0, "pre-momentum conditions detected before price expansion")
        if status == "DATA":
            risks.insert(0, "intraday history still warming")

        return Signal(
            symbol=sym, base=base, price=p, bid=bid, ask=ask, spread_pct=spread,
            chg_5m=c5, chg_15m=c15, chg_1h=c1h, chg_4h=c4h, chg_24h=c24,
            chg_7d=c7, chg_30d=c30, volume_24h_eur=veur, volume_accel_15m=vacc,
            volume_accel_5m=bv5, trade_accel_5m=bt5, taker_buy_ratio_5m=buy5,
            compression_ratio=compression, room_to_high_pct=room, btc_relative_15m=btc15,
            higher_low=hl, breakout_pct=breakout, breakout_age_min=bo_age,
            rsi_14=rsi, atr_pct=atr, support=support, resistance=resistance,
            reward_risk=rr, cross_exchange_lead_5m=cross_lead5, phase=phase,
            pre_momentum_score=pre, technical_score=technical, consensus_score=consensus,
            catalyst_score=catalyst_score, confidence=confidence, status=status,
            reasons=reasons, risks=risks, data_quality=quality,
            timestamp=time.strftime("%Y-%m-%d %H:%M:%S %z"),
            binance_available=bool(bx), binance_price=bx.get("price"),
            binance_chg_5m=b5, binance_chg_15m=b15,
            coinbase_available=bool(cx), coinbase_price=cx.get("price"),
            exchange_confirmations=confirmations, source_count=source_count,
            catalyst_title=catalyst.get("title") if catalyst else None,
            catalyst_source=catalyst.get("source") if catalyst else None,
            catalyst_age_min=catalyst_age, catalyst_url=catalyst.get("url") if catalyst else None,
            catalyst_sentiment=catalyst_sentiment,
        )

    def snapshot(self):
        rows = [self.analyze(s) for s in list(self.tickers)]
        rank = {"PRE": 0, "ENTRY_WINDOW": 1, "SETUP": 2, "MOMENTUM": 3, "WATCH": 4, "LATE": 5, "REJECT": 6, "DATA": 7, "IGNORE": 8}
        rows.sort(key=lambda x: (rank.get(x.status, 9), -x.pre_momentum_score, -x.confidence, -x.technical_score))
        self._log(rows)
        return [asdict(x) for x in rows]

    def detail(self, sym):
        if sym not in self.tickers:
            return None
        return {"signal": asdict(self.analyze(sym)), "candles": self.candles_1m.get(sym, [])[-180:]}

    def news_snapshot(self, limit=80):
        now = time.time()
        out = []
        for x in list(self.news_items)[:limit]:
            c = self._classify_catalyst(x)
            if now - x.get("published_ts", now) <= self.cfg.get("news_max_age_hours", 12) * 3600:
                out.append(c)
        return out

    def source_health(self):
        now = time.time()
        return {
            "kraken": {"connected": self.connected, "age": 0 if self.connected else None, "error": self.last_error},
            "binance": {"connected": self.binance_connected and now - self.binance_updated < 90, "age": round(now - self.binance_updated, 1) if self.binance_updated else None, "error": self.binance_error},
            "coinbase": {"connected": self.coinbase_connected and now - self.coinbase_updated < 90, "age": round(now - self.coinbase_updated, 1) if self.coinbase_updated else None, "error": self.coinbase_error},
            "news": {"connected": self.news_connected and now - self.news_updated < 240, "age": round(now - self.news_updated, 1) if self.news_updated else None, "error": self.news_error, "items": len(self.news_items), "matched": len(self.catalysts)},
        }

    def _log(self, rows):
        path = self.root / "data" / "signals.csv"
        path.parent.mkdir(exist_ok=True)
        exists = path.exists()
        now = time.time()
        with path.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if not exists:
                w.writerow(["time", "symbol", "status", "phase", "pre", "technical", "consensus", "catalyst", "confidence", "price", "5m", "15m", "24h", "vol5", "trade5", "buy_ratio5", "compression", "spread", "catalyst_title"])
            for s in rows:
                if s.status not in ("PRE", "ENTRY_WINDOW", "SETUP"):
                    continue
                key = (s.symbol, s.status)
                if now - self._logged.get(key, 0) < 300:
                    continue
                self._logged[key] = now
                w.writerow([
                    s.timestamp, s.symbol, s.status, s.phase, s.pre_momentum_score,
                    s.technical_score, s.consensus_score, s.catalyst_score, s.confidence,
                    s.price, s.chg_5m, s.chg_15m, s.chg_24h, s.volume_accel_5m,
                    s.trade_accel_5m, s.taker_buy_ratio_5m, s.compression_ratio,
                    s.spread_pct, s.catalyst_title or "",
                ])
