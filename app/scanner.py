from __future__ import annotations
import asyncio, csv, json, math, time
from collections import defaultdict, deque
from dataclasses import dataclass, asdict
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
    volume_24h: float
    volume_24h_eur: float
    volume_accel_15m: float | None
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
    phase: str
    score: int
    consensus_score: int
    confidence: int
    status: str
    reasons: list[str]
    risks: list[str]
    data_quality: str
    enriched_at: str | None
    timestamp: str
    binance_available: bool
    binance_price: float | None
    binance_chg_5m: float | None
    binance_chg_15m: float | None
    binance_chg_1h: float | None
    binance_chg_24h: float | None
    binance_volume_accel: float | None
    coinbase_available: bool
    coinbase_price: float | None
    coinbase_chg_24h: float | None
    exchange_confirmations: int
    source_count: int

class Scanner:
    def __init__(self, cfg: dict[str, Any], root: Path):
        self.cfg=cfg; self.root=root
        self.tickers: dict[str, dict[str, Any]] = {}
        self.pair_map: dict[str, str] = {}
        self.base_to_kraken: dict[str,str] = {}
        self.price_hist=defaultdict(lambda: deque(maxlen=30000))
        self.connected=False; self.last_error=""; self.started=time.time()
        self._logged: dict[tuple[str,str], float] = {}
        self.candles_1m: dict[str, list[dict[str,float]]] = {}
        self.candles_60m: dict[str, list[dict[str,float]]] = {}
        self.enriched_at: dict[str, float] = {}
        self._session: aiohttp.ClientSession | None = None
        self._sem = asyncio.Semaphore(int(cfg.get("enrichment_concurrency",5)))
        self.binance: dict[str,dict[str,Any]] = {}
        self.binance_connected=False; self.binance_error=""; self.binance_updated=0.0
        self.coinbase: dict[str,dict[str,Any]] = {}
        self.coinbase_connected=False; self.coinbase_error=""; self.coinbase_updated=0.0
        self.coinbase_products:set[str]=set()

    @staticmethod
    def _base(sym:str)->str:
        return sym.split('/')[0].upper() if '/' in sym else sym.upper()

    async def discover(self, session: aiohttp.ClientSession) -> list[str]:
        async with session.get(KRAKEN_PAIRS, timeout=20) as r:
            j=await r.json()
        out=[]; q=self.cfg.get("quote_currency","EUR")
        for k,v in j.get("result",{}).items():
            ws=v.get("wsname")
            if ws and ws.endswith("/"+q) and ".d" not in ws.lower():
                out.append(ws); self.pair_map[ws]=v.get("altname") or k; self.base_to_kraken[self._base(ws)]=ws
        return sorted(set(out))

    async def run(self):
        while True:
            try:
                timeout=aiohttp.ClientTimeout(total=25)
                async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent":"VoltTrade/0.5"}) as session:
                    self._session=session
                    symbols=await self.discover(session)
                    tasks=[asyncio.create_task(self.enrichment_loop()),asyncio.create_task(self.binance_loop()),asyncio.create_task(self.coinbase_loop())]
                    async with session.ws_connect(KRAKEN_WS, heartbeat=20, receive_timeout=90) as ws:
                        self.connected=True; self.last_error=""
                        for i in range(0,len(symbols),100):
                            await ws.send_json({"method":"subscribe","params":{"channel":"ticker","symbol":symbols[i:i+100],"snapshot":True}})
                        async for msg in ws:
                            if msg.type != aiohttp.WSMsgType.TEXT: continue
                            data=json.loads(msg.data)
                            if data.get("channel")!="ticker": continue
                            for t in data.get("data",[]): self.ingest(t)
                    for t in tasks:t.cancel()
            except Exception as e:
                self.connected=False; self.last_error=f"{type(e).__name__}: {e}"; await asyncio.sleep(4)

    def ingest(self,t):
        sym=t.get("symbol"); p=float(t.get("last") or 0); now=time.time()
        if not sym or p<=0:return
        self.tickers[sym]=t; self.price_hist[sym].append((now,p))
        cutoff=now-self.cfg.get("history_minutes",360)*60
        while self.price_hist[sym] and self.price_hist[sym][0][0]<cutoff:self.price_hist[sym].popleft()

    async def binance_loop(self):
        while True:
            try:
                assert self._session
                async with self._session.get(BINANCE_24H, timeout=20) as r:
                    arr=await r.json()
                allowed=set(self.base_to_kraken)
                rows=[]
                for x in arr if isinstance(arr,list) else []:
                    s=str(x.get("symbol", ""))
                    if not s.endswith("USDT"):continue
                    base=s[:-4].upper()
                    if base not in allowed:continue
                    try:
                        p=float(x.get("lastPrice") or 0); c24=float(x.get("priceChangePercent") or 0); qv=float(x.get("quoteVolume") or 0)
                    except: continue
                    if p<=0 or qv<self.cfg.get("binance_min_quote_volume_usdt",1_000_000):continue
                    if c24 < -3 or c24 > self.cfg.get("max_24h_pct",12): continue
                    pri=(max(-2,min(c24,8))*1.4)+math.log10(max(qv,1))
                    rows.append((pri,base,s,p,c24,qv))
                rows.sort(reverse=True)
                rows=rows[:int(self.cfg.get("binance_candidates",70))]
                for _,base,s,p,c24,qv in rows:
                    d=self.binance.get(base,{})
                    d.update({"symbol":s,"price":p,"chg24":c24,"quote_volume":qv,"updated":time.time()})
                    self.binance[base]=d
                # Fetch 1m history for early candidates; direct exchange history avoids startup warm-up.
                for _,base,s,_,_,_ in rows[:45]:
                    try:
                        async with self._sem:
                            async with self._session.get(BINANCE_KLINES,params={"symbol":s,"interval":"1m","limit":"90"},timeout=15) as r:
                                k=await r.json()
                        if not isinstance(k,list):continue
                        candles=[{"ts":float(x[0])/1000,"open":float(x[1]),"high":float(x[2]),"low":float(x[3]),"close":float(x[4]),"volume":float(x[5])} for x in k if isinstance(x,list) and len(x)>5]
                        if len(candles)>=61:
                            d=self.binance.setdefault(base,{})
                            d.update(self._candle_metrics(candles)); d["candles"]=candles
                    except Exception: pass
                self.binance_connected=True; self.binance_error=""; self.binance_updated=time.time()
            except Exception as e:
                self.binance_connected=False; self.binance_error=f"{type(e).__name__}: {e}"
            await asyncio.sleep(float(self.cfg.get("binance_refresh_seconds",25)))

    async def coinbase_loop(self):
        while True:
            try:
                assert self._session
                if not self.coinbase_products:
                    async with self._session.get(COINBASE_PRODUCTS,timeout=20) as r:
                        products=await r.json()
                    if isinstance(products,list):
                        self.coinbase_products={x.get("id") for x in products if x.get("id") and x.get("quote_currency") in ("USD","USDC") and x.get("status") in ("online",None)}
                ids=[]
                for base in self.base_to_kraken:
                    for q in ("USD","USDC"):
                        pid=f"{base}-{q}"
                        if pid in self.coinbase_products:
                            ids.append(pid);break
                # Keep payload sane; prioritize Kraken-listed assets that also appear in Binance shortlist.
                ids.sort(key=lambda pid:(pid.split('-')[0] not in self.binance,pid))
                ids=ids[:350]
                async with self._session.ws_connect(COINBASE_WS,heartbeat=20,receive_timeout=90) as ws:
                    self.coinbase_connected=True; self.coinbase_error=""
                    await ws.send_json({"type":"subscribe","channel":"heartbeats"})
                    for i in range(0,len(ids),100):
                        await ws.send_json({"type":"subscribe","product_ids":ids[i:i+100],"channel":"ticker_batch"})
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:continue
                        j=json.loads(msg.data)
                        if j.get("channel")!="ticker_batch":continue
                        for ev in j.get("events",[]):
                            for t in ev.get("tickers",[]):
                                pid=t.get("product_id",""); base=pid.split('-')[0].upper()
                                try:
                                    self.coinbase[base]={"product_id":pid,"price":float(t.get("price") or 0),"chg24":float(t.get("price_percent_chg_24_h") or 0),"volume24":float(t.get("volume_24_h") or 0),"updated":time.time()}
                                    self.coinbase_updated=time.time()
                                except:pass
            except Exception as e:
                self.coinbase_connected=False; self.coinbase_error=f"{type(e).__name__}: {e}"
                await asyncio.sleep(float(self.cfg.get("coinbase_reconnect_seconds",10)))

    async def enrichment_loop(self):
        await asyncio.sleep(3)
        while True:
            try:
                shortlist=self._shortlist()
                tasks=[asyncio.create_task(self._enrich_symbol(s, deep=(i < int(self.cfg.get("deep_candidates",24))))) for i,s in enumerate(shortlist)]
                if tasks: await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:self.last_error=f"enrichment: {type(e).__name__}: {e}"
            await asyncio.sleep(float(self.cfg.get("enrichment_interval_seconds",60)))

    def _shortlist(self)->list[str]:
        rows=[]
        for sym,t in self.tickers.items():
            p=float(t.get("last") or 0); bid=float(t.get("bid") or p); ask=float(t.get("ask") or p)
            if p<=0:continue
            spread=(ask-bid)/p*100 if p else 999; c24=float(t.get("change_pct") or 0); v=float(t.get("volume") or 0); eur=v*p
            if spread>self.cfg.get("prefilter_max_spread_pct",1.2) or eur<self.cfg.get("min_24h_volume_eur",75000):continue
            if c24<self.cfg.get("prefilter_24h_min_pct",-3) or c24>self.cfg.get("prefilter_24h_max_pct",14):continue
            base=self._base(sym); bx=self.binance.get(base,{})
            global_boost=0
            if bx.get("chg15") is not None and 0.25<=bx["chg15"]<=2.8:global_boost+=5
            if bx.get("vacc") is not None and bx["vacc"]>=1.4:global_boost+=4
            priority=(max(-2,min(c24,8))*1.7)+math.log10(max(eur,1))-spread*3+global_boost
            rows.append((priority,sym))
        rows.sort(reverse=True); return [s for _,s in rows[:int(self.cfg.get("enrich_candidates",55))]]

    async def _fetch_ohlc(self,sym:str,interval:int)->list[dict[str,float]]:
        if not self._session:return []
        pair=self.pair_map.get(sym)
        if not pair:return []
        async with self._sem:
            async with self._session.get(KRAKEN_OHLC,params={"pair":pair,"interval":str(interval)},timeout=20) as r:j=await r.json()
        if j.get("error"):return []
        arr=next((v for k,v in j.get("result",{}).items() if k!="last" and isinstance(v,list)),None)
        if not arr:return []
        out=[]
        for x in arr:
            if len(x)<8:continue
            out.append({"ts":float(x[0]),"open":float(x[1]),"high":float(x[2]),"low":float(x[3]),"close":float(x[4]),"vwap":float(x[5]),"volume":float(x[6]),"count":float(x[7])})
        return out

    async def _enrich_symbol(self,sym:str,deep:bool=False):
        c1=await self._fetch_ohlc(sym,1)
        if c1:self.candles_1m[sym]=c1
        if deep:
            c60=await self._fetch_ohlc(sym,60)
            if c60:self.candles_60m[sym]=c60
        if c1 or deep:self.enriched_at[sym]=time.time()

    @staticmethod
    def _pct(a,b):return None if b in (None,0) else (a/b-1)*100
    @staticmethod
    def _clamp(x,a,b):return max(a,min(b,x))
    @staticmethod
    def _candle_metrics(a:list[dict[str,float]])->dict[str,Any]:
        def pct(m):
            return None if len(a)<=m or a[-m-1]["close"]==0 else (a[-1]["close"]/a[-m-1]["close"]-1)*100
        recent=sum(x["volume"] for x in a[-15:]); blocks=[]
        for end in (30,45,60,75):
            if len(a)>=end:blocks.append(sum(x["volume"] for x in a[-end:-end+15]))
        base=sum(blocks)/len(blocks) if blocks else 0
        return {"chg5":pct(5),"chg15":pct(15),"chg1h":pct(60),"vacc":recent/base if base>0 else None}

    def _old_live(self,hist,secs):
        target=time.time()-secs;best=None
        for ts,p in hist:
            if ts<=target:best=p
            else:break
        return best
    def _price_ago(self,sym,minutes):
        arr=self.candles_1m.get(sym,[])
        if len(arr)>minutes:return arr[-minutes-1]["close"]
        return self._old_live(self.price_hist[sym],minutes*60)
    def _hourly_ago(self,sym,hours):
        a=self.candles_60m.get(sym,[]);return a[-hours-1]["close"] if len(a)>hours else None
    def _volume_accel(self,sym):
        a=self.candles_1m.get(sym,[])
        if len(a)<76:return None
        return self._candle_metrics(a)["vacc"]
    def _rsi(self,closes,period=14):
        if len(closes)<period+1:return None
        gains=[];loss=[]
        for a,b in zip(closes[-period-1:-1],closes[-period:]):
            d=b-a;gains.append(max(d,0));loss.append(max(-d,0))
        ag=sum(gains)/period;al=sum(loss)/period
        if al==0:return 100.0
        rs=ag/al;return 100-(100/(1+rs))
    def _atr_pct(self,a,period=14):
        if len(a)<period+1:return None
        trs=[]
        for i in range(len(a)-period,len(a)):
            cur=a[i];prev=a[i-1]["close"];trs.append(max(cur["high"]-cur["low"],abs(cur["high"]-prev),abs(cur["low"]-prev)))
        p=a[-1]["close"];return (sum(trs)/period)/p*100 if p else None

    def _breakout_age(self,a:list[dict[str,float]])->float|None:
        if len(a)<70:return None
        # Rolling breakout: first close in last 35m above the previous 40m high by 0.10%.
        found=None
        start=max(45,len(a)-35)
        for i in range(start,len(a)):
            prev=a[max(0,i-40):i]
            if len(prev)<25:continue
            level=max(x["high"] for x in prev)
            if a[i]["close"]>level*1.001:
                found=a[i]["ts"];break
        return max(0,(time.time()-found)/60) if found else None

    def _structure(self,sym,p):
        a=self.candles_1m.get(sym,[])
        if len(a)<70:return (None,None,None,None,None,None,None)
        prior15=a[-30:-15];recent15=a[-15:]
        higher_low=min(x["low"] for x in recent15)>min(x["low"] for x in prior15)
        base=a[-65:-5];resistance=max(x["high"] for x in base);support=min(x["low"] for x in a[-30:])
        breakout=(p/resistance-1)*100 if resistance else None
        closes=[x["close"] for x in a];rsi=self._rsi(closes[-40:],14);atr=self._atr_pct(a[-40:],14);age=self._breakout_age(a)
        return higher_low,breakout,support,resistance,rsi,atr,age

    def analyze(self,sym:str)->Signal:
        t=self.tickers[sym];base=self._base(sym);p=float(t.get("last") or 0);bid=float(t.get("bid") or p);ask=float(t.get("ask") or p)
        spread=(ask-bid)/p*100 if p else 999
        c5=self._pct(p,self._price_ago(sym,5));c15=self._pct(p,self._price_ago(sym,15));c1h=self._pct(p,self._price_ago(sym,60));c4h=self._pct(p,self._price_ago(sym,240))
        c24=float(t.get("change_pct") or 0);c7=self._pct(p,self._hourly_ago(sym,24*7));c30=self._pct(p,self._hourly_ago(sym,24*30-1))
        high=float(t.get("high") or p);room=(high/p-1)*100 if p else 0;v24=float(t.get("volume") or 0);veur=v24*p;vacc=self._volume_accel(sym)
        btc15=None
        if sym!="BTC/EUR" and "BTC/EUR" in self.tickers:
            bp=float(self.tickers["BTC/EUR"].get("last") or 0);bo=self._price_ago("BTC/EUR",15);bc=self._pct(bp,bo)
            if c15 is not None and bc is not None:btc15=c15-bc
        hl,breakout,support,resistance,rsi,atr,bo_age=self._structure(sym,p)
        bx=self.binance.get(base,{});cx=self.coinbase.get(base,{})
        b5=bx.get("chg5");b15=bx.get("chg15");b1h=bx.get("chg1h");b24=bx.get("chg24");bv=bx.get("vacc")
        reasons=[];risks=[];score=0
        fresh=sym in self.candles_1m and time.time()-self.enriched_at.get(sym,0)<240;deep=sym in self.candles_60m
        quality="FULL" if fresh and deep else ("INTRADAY" if fresh else "TICKER_ONLY")
        if c5 is not None and 0.15<=c5<=1.8:score+=5;reasons.append("5m acceleration")
        if c15 is not None and self.cfg["early_15m_min_pct"]<=c15<=self.cfg["early_15m_max_pct"]:score+=9;reasons.append("15m early momentum")
        if c1h is not None and self.cfg["early_1h_min_pct"]<=c1h<=self.cfg["early_1h_max_pct"]:score+=6;reasons.append("1h trend supports")
        if vacc is not None:
            if vacc>=2.2:score+=25;reasons.append(f"volume {vacc:.1f}x")
            elif vacc>=1.6:score+=18;reasons.append(f"volume {vacc:.1f}x")
            elif vacc>=1.2:score+=8
            elif vacc<0.8:risks.append("volume fading")
        if hl is True:score+=8;reasons.append("higher low")
        if breakout is not None:
            if 0<=breakout<=1.2:score+=12;reasons.append("fresh base breakout")
            elif -0.8<=breakout<0:score+=5;reasons.append("testing resistance")
            elif breakout>2.5:risks.append("breakout already extended")
        if btc15 is not None:
            if btc15>=1.0:score+=15;reasons.append("outperforming BTC")
            elif btc15>=0.3:score+=9
            elif btc15<0:risks.append("underperforming BTC")
        if spread<=0.20 and veur>=500000:score+=10
        elif spread<=0.40 and veur>=150000:score+=7
        elif spread<=0.75:score+=3
        else:risks.append("wide spread")
        if veur<self.cfg.get("min_24h_volume_eur",75000):risks.append("low liquidity")
        rr=None
        if support and resistance and support<p:
            risk=p-support;target=max(high,resistance*(1+self.cfg.get("target_extension_pct",3.0)/100));reward=max(0,target-p);rr=reward/risk if risk>0 else None
        if room>=5:score+=7
        elif room>=2.5:score+=4
        if rr is not None and rr>=2.0:score+=3;reasons.append(f"R/R {rr:.1f}")
        elif rr is not None and rr<1.2:risks.append("poor risk/reward")

        # Multi-exchange consensus: independent confirmation rather than averaging prices across quote currencies.
        confirmations=1;source_count=1;cons=0
        if bx:
            source_count+=1
            if b15 is not None and c15 is not None:
                if b15>0.25 and c15>0.25:confirmations+=1;cons+=28;reasons.append("Binance confirms acceleration")
                elif b15<0<c15:risks.append("Binance divergence")
            if bv is not None and bv>=1.5:cons+=14;reasons.append("Binance volume expanding")
            if b24 is not None and b24>self.cfg["max_24h_pct"]:risks.append("Binance already extended")
        if cx:
            source_count+=1
            cbc24=cx.get("chg24")
            if cbc24 is not None and ((cbc24>=0 and c24>=0) or (cbc24>c24-1.5)):
                confirmations+=1;cons+=20;reasons.append("Coinbase confirms direction")
            elif cbc24 is not None and cbc24<-2 and c24>0:risks.append("Coinbase divergence")
        # Cross-exchange short-term coherence.
        if b15 is not None and c15 is not None:
            diff=abs(b15-c15)
            if diff<=1.0:cons+=16
            elif diff>3:risks.append("cross-exchange mismatch")
        if bo_age is not None:
            if bo_age<=10:cons+=16;reasons.append(f"breakout age {bo_age:.0f}m")
            elif bo_age<=25:cons+=10;reasons.append(f"breakout age {bo_age:.0f}m")
            elif bo_age>60:risks.append("breakout is old")
        if confirmations>=3:cons+=6
        consensus=min(100,cons)

        phase="BASE"
        if c15 is not None and c15>0.4 and (vacc or 0)>=1.2:phase="IGNITION"
        if hl and breakout is not None and -0.8<=breakout<0.4:phase="PULLBACK"
        if hl and breakout is not None and 0<=breakout<=1.2 and (vacc or 0)>=1.5:phase="CONFIRMED"
        if c24>self.cfg["max_24h_pct"] or (breakout is not None and breakout>2.5) or (rsi is not None and rsi>78):phase="EXTENDED"
        hard_reject=spread>self.cfg["max_spread_pct"] or veur<self.cfg.get("hard_min_24h_volume_eur",30000) or c24<self.cfg.get("hard_24h_loss_pct",-10)
        if quality=="TICKER_ONLY":status="DATA"
        elif hard_reject:status="REJECT"
        elif phase=="EXTENDED":status="EXTENDED"
        else:
            entry_ready=(score>=self.cfg["score_entry"] and consensus>=self.cfg.get("consensus_entry_min",65) and confirmations>=2 and phase in ("PULLBACK","CONFIRMED") and (vacc or 0)>=self.cfg["entry_volume_accel_min"] and (rr is None or rr>=self.cfg["entry_min_rr"]) and spread<=self.cfg["entry_max_spread_pct"] and (bo_age is None or bo_age<=self.cfg.get("breakout_age_max_entry_min",25)))
            setup=(score>=self.cfg["score_setup"] and consensus>=self.cfg.get("consensus_setup_min",48) and phase in ("IGNITION","PULLBACK","CONFIRMED") and (bo_age is None or bo_age<=self.cfg.get("breakout_age_max_setup_min",45)))
            if entry_ready:status="ENTRY"
            elif setup:status="SETUP"
            elif score>=self.cfg["score_watch"]:status="WATCH"
            else:status="IGNORE"
        confidence=round(self._clamp(score*0.62+consensus*0.38+(4 if quality=="FULL" else 0)-len(risks)*4,0,100))
        if status=="WATCH":risks.insert(0,"watch only — no entry confirmation")
        if status=="SETUP":risks.insert(0,"setup developing — wait for entry confirmation")
        if status=="DATA":risks.insert(0,"intraday history still warming")
        return Signal(sym,base,p,bid,ask,spread,c5,c15,c1h,c4h,c24,c7,c30,v24,veur,vacc,room,btc15,hl,breakout,bo_age,rsi,atr,support,resistance,rr,phase,min(100,score),consensus,confidence,status,reasons,risks,quality,time.strftime('%H:%M:%S',time.localtime(self.enriched_at[sym])) if sym in self.enriched_at else None,time.strftime('%Y-%m-%d %H:%M:%S %z'),bool(bx),bx.get("price"),b5,b15,b1h,b24,bv,bool(cx),cx.get("price"),cx.get("chg24"),confirmations,source_count)

    def snapshot(self):
        rows=[self.analyze(s) for s in list(self.tickers)]
        rank={"ENTRY":0,"SETUP":1,"WATCH":2,"EXTENDED":3,"REJECT":4,"DATA":5,"IGNORE":6}
        rows.sort(key=lambda x:(rank.get(x.status,9),-x.consensus_score,-x.score,-(x.chg_15m or -999)))
        self._log(rows);return [asdict(x) for x in rows]

    def detail(self,sym):
        if sym not in self.tickers:return None
        return {"signal":asdict(self.analyze(sym)),"candles":self.candles_1m.get(sym,[])[-180:]}

    def source_health(self):
        now=time.time()
        return {
            "kraken":{"connected":self.connected,"age":0 if self.connected else None,"error":self.last_error},
            "binance":{"connected":self.binance_connected and now-self.binance_updated<90,"age":round(now-self.binance_updated,1) if self.binance_updated else None,"error":self.binance_error},
            "coinbase":{"connected":self.coinbase_connected and now-self.coinbase_updated<90,"age":round(now-self.coinbase_updated,1) if self.coinbase_updated else None,"error":self.coinbase_error}
        }

    def _log(self,rows):
        path=self.root/"data"/"signals.csv";path.parent.mkdir(exist_ok=True);exists=path.exists();now=time.time()
        with path.open("a",newline="",encoding="utf-8") as f:
            w=csv.writer(f)
            if not exists:w.writerow(["time","symbol","status","phase","score","consensus","confidence","price","5m","15m","1h","4h","24h","spread","volume_eur","vol_accel","breakout_age","rr","confirmations"])
            for s in rows:
                if s.status not in ("ENTRY","SETUP"):continue
                key=(s.symbol,s.status)
                if now-self._logged.get(key,0)<300:continue
                self._logged[key]=now
                w.writerow([s.timestamp,s.symbol,s.status,s.phase,s.score,s.consensus_score,s.confidence,s.price,s.chg_5m,s.chg_15m,s.chg_1h,s.chg_4h,s.chg_24h,s.spread_pct,s.volume_24h_eur,s.volume_accel_15m,s.breakout_age_min,s.reward_risk,s.exchange_confirmations])
