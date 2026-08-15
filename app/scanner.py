from __future__ import annotations
import asyncio, csv, json, math, time
from collections import defaultdict, deque
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any
import aiohttp

KRAKEN_WS = "wss://ws.kraken.com/v2"
KRAKEN_PAIRS = "https://api.kraken.com/0/public/AssetPairs"

@dataclass
class Signal:
    symbol: str
    price: float
    bid: float
    ask: float
    spread_pct: float
    chg_5m: float | None
    chg_15m: float | None
    chg_1h: float | None
    chg_24h: float
    volume_24h: float
    volume_accel_15m: float | None
    room_to_high_pct: float
    btc_relative_15m: float | None
    score: int
    status: str
    reasons: list[str]
    timestamp: str

class Scanner:
    def __init__(self, cfg: dict[str, Any], root: Path):
        self.cfg=cfg; self.root=root
        self.tickers: dict[str, dict[str, Any]] = {}
        self.price_hist=defaultdict(lambda: deque(maxlen=20000))
        self.vol_hist=defaultdict(lambda: deque(maxlen=20000))
        self.connected=False; self.last_error=""; self.started=time.time()
        self._logged: dict[tuple[str,str], float] = {}

    async def discover(self, session: aiohttp.ClientSession) -> list[str]:
        async with session.get(KRAKEN_PAIRS, timeout=20) as r:
            j=await r.json()
        out=[]
        q=self.cfg.get("quote_currency","EUR")
        for v in j.get("result",{}).values():
            ws=v.get("wsname")
            if ws and ws.endswith("/"+q) and ".d" not in ws.lower(): out.append(ws)
        return sorted(set(out))

    async def run(self):
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    symbols=await self.discover(session)
                    async with session.ws_connect(KRAKEN_WS, heartbeat=20, receive_timeout=90) as ws:
                        self.connected=True; self.last_error=""
                        for i in range(0,len(symbols),100):
                            await ws.send_json({"method":"subscribe","params":{"channel":"ticker","symbol":symbols[i:i+100],"snapshot":True}})
                        async for msg in ws:
                            if msg.type != aiohttp.WSMsgType.TEXT: continue
                            data=json.loads(msg.data)
                            if data.get("channel")!="ticker": continue
                            for t in data.get("data",[]): self.ingest(t)
            except Exception as e:
                self.connected=False; self.last_error=f"{type(e).__name__}: {e}"
                await asyncio.sleep(4)

    def ingest(self,t):
        sym=t.get("symbol"); p=float(t.get("last") or 0); now=time.time()
        if not sym or p<=0:return
        self.tickers[sym]=t
        self.price_hist[sym].append((now,p))
        self.vol_hist[sym].append((now,float(t.get("volume") or 0)))
        cutoff=now-self.cfg.get("history_minutes",90)*60
        while self.price_hist[sym] and self.price_hist[sym][0][0]<cutoff:self.price_hist[sym].popleft()
        while self.vol_hist[sym] and self.vol_hist[sym][0][0]<cutoff:self.vol_hist[sym].popleft()

    def _old(self, hist, secs, idx=1):
        target=time.time()-secs; best=None
        for row in hist:
            if row[0] <= target: best=row[idx]
            else: break
        return best

    @staticmethod
    def _pct(a,b):
        return None if not b else (a/b-1)*100

    def analyze(self,sym:str)->Signal:
        t=self.tickers[sym]; p=float(t.get("last") or 0); bid=float(t.get("bid") or p); ask=float(t.get("ask") or p)
        spread=(ask-bid)/p*100 if p else 999
        ph=self.price_hist[sym]; vh=self.vol_hist[sym]
        c5=self._pct(p,self._old(ph,300)); c15=self._pct(p,self._old(ph,900)); c1h=self._pct(p,self._old(ph,3600))
        c24=float(t.get("change_pct") or 0); high=float(t.get("high") or p); room=(high/p-1)*100 if p else 0
        v24=float(t.get("volume") or 0); v15old=self._old(vh,900); vacc=None
        if v15old is not None and v24>0:
            delta=max(0,v24-v15old); expected=max(v24/96,1e-12); vacc=delta/expected
        btc15=None
        if sym!="BTC/EUR" and "BTC/EUR" in self.tickers:
            bp=float(self.tickers["BTC/EUR"].get("last") or 0); bo=self._old(self.price_hist["BTC/EUR"],900)
            bc=self._pct(bp,bo)
            if c15 is not None and bc is not None: btc15=c15-bc
        reasons=[]; score=0
        # price momentum 20
        if c15 is not None and self.cfg["early_15m_min_pct"]<=c15<=self.cfg["early_15m_max_pct"]: score+=12; reasons.append("15m momentum in early zone")
        elif c15 is not None and 0<c15<self.cfg["early_15m_min_pct"]: score+=5
        if c1h is not None and self.cfg["early_1h_min_pct"]<=c1h<=self.cfg["early_1h_max_pct"]: score+=8; reasons.append("1h momentum constructive")
        # volume 25
        if vacc is not None:
            if vacc>=self.cfg["volume_accel_min"]: score+=25; reasons.append(f"15m volume acceleration {vacc:.1f}x")
            elif vacc>=1.2: score+=14
        # structure 20: rising short windows / higher low proxy
        if c5 is not None and c15 is not None and c5>0 and c15>0: score+=10
        if c15 is not None and c1h is not None and c15>0 and c1h>0: score+=10; reasons.append("multi-timeframe higher momentum")
        # relative strength 15
        if btc15 is not None:
            if btc15>=1.0: score+=15; reasons.append("outperforming BTC")
            elif btc15>0: score+=8
        # order flow proxy 10: bid/ask spread + last near ask
        if spread<=0.25: score+=10
        elif spread<=0.6: score+=6
        # resistance room 10
        if room>=self.cfg["min_room_to_24h_high_pct"]: score+=10; reasons.append(f"{room:.1f}% room to 24h high")
        elif room>=1.5: score+=5
        extended = c24>self.cfg["max_24h_pct"] or (c15 is not None and c15>self.cfg["early_15m_max_pct"]*1.6)
        reject = spread>self.cfg["max_spread_pct"] or c24<-8
        if reject: status="REJECT"
        elif extended: status="EXTENDED"
        elif score>=self.cfg["score_strong"]: status="STRONG"
        elif score>=self.cfg["score_early"]: status="EARLY"
        elif score>=self.cfg["score_watch"]: status="WATCH"
        else: status="IGNORE"
        return Signal(sym,p,bid,ask,spread,c5,c15,c1h,c24,v24,vacc,room,btc15,min(100,score),status,reasons,time.strftime('%Y-%m-%d %H:%M:%S %z'))

    def snapshot(self):
        rows=[self.analyze(s) for s in list(self.tickers)]
        rank={"STRONG":0,"EARLY":1,"WATCH":2,"EXTENDED":3,"REJECT":4,"IGNORE":5}
        rows.sort(key=lambda x:(rank.get(x.status,9),-x.score,-(x.chg_15m or -999)))
        self._log(rows)
        return [asdict(x) for x in rows]

    def _log(self, rows):
        path=self.root/"data"/"signals.csv"; path.parent.mkdir(exist_ok=True)
        exists=path.exists(); now=time.time()
        with path.open("a",newline="",encoding="utf-8") as f:
            w=csv.writer(f)
            if not exists:w.writerow(["time","symbol","status","score","price","5m","15m","1h","24h","spread","room","vol_accel"])
            for s in rows:
                if s.score<self.cfg.get("log_min_score",65) or s.status not in ("EARLY","STRONG"): continue
                key=(s.symbol,s.status)
                if now-self._logged.get(key,0)<300: continue
                self._logged[key]=now
                w.writerow([s.timestamp,s.symbol,s.status,s.score,s.price,s.chg_5m,s.chg_15m,s.chg_1h,s.chg_24h,s.spread_pct,s.room_to_high_pct,s.volume_accel_15m])
