import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'app'))
from scanner import Scanner


def candles(flat=True, hot=True):
    out=[]
    p=10.0
    for i in range(120):
        if flat:
            drift=(i%7-3)*0.0002
        else:
            drift=0.003
        o=p
        p=p*(1+drift)
        v=100.0
        count=50.0
        buy=48.0
        if hot and i>=115:
            v=260.0
            count=130.0
            buy=175.0
        out.append({'ts':i*60,'open':o,'high':max(o,p)*1.0005,'low':min(o,p)*0.9995,'close':p,'volume':v,'count':count,'taker_buy_volume':buy})
    return out

class TestPreMomentum(unittest.TestCase):
    def test_activity_before_price_scores_high(self):
        m=Scanner._candle_metrics(candles(flat=True,hot=True))
        score,reasons,risks=Scanner._score_premomentum(
            chg15=0.25,chg24=1.0,vacc5=m['vacc5'],tacc5=m['tacc5'],
            taker_ratio=m['taker_buy_ratio5'],compression=0.45,
            near_resistance_pct=-0.35,cross_lead5=0.4,higher_low=True,
            catalyst={'score':82,'sentiment':'positive','age_min':8}
        )
        self.assertGreaterEqual(score,80)
        self.assertTrue(any('price still quiet' in r for r in reasons))
        self.assertFalse(any('already' in r for r in risks))

    def test_quiet_market_without_activity_is_low(self):
        m=Scanner._candle_metrics(candles(flat=True,hot=False))
        score,_,_=Scanner._score_premomentum(
            chg15=0.1,chg24=0.5,vacc5=m['vacc5'],tacc5=m['tacc5'],
            taker_ratio=0.50,compression=0.9,near_resistance_pct=-2.0,
            cross_lead5=0.0,higher_low=False,catalyst=None)
        self.assertLess(score,35)

    def test_negative_catalyst_penalizes(self):
        base,_r,_k=Scanner._score_premomentum(
            chg15=0.2,chg24=1.0,vacc5=2.0,tacc5=2.0,taker_ratio=0.62,
            compression=0.5,near_resistance_pct=-0.3,cross_lead5=0.4,
            higher_low=True,catalyst=None)
        bad,_r,risks=Scanner._score_premomentum(
            chg15=0.2,chg24=1.0,vacc5=2.0,tacc5=2.0,taker_ratio=0.62,
            compression=0.5,near_resistance_pct=-0.3,cross_lead5=0.4,
            higher_low=True,catalyst={'score':90,'sentiment':'negative','age_min':5})
        self.assertLess(bad,base)
        self.assertTrue(any('negative catalyst' in x for x in risks))

    def test_price_already_moving_is_flagged(self):
        score,_r,risks=Scanner._score_premomentum(
            chg15=4.2,chg24=13.0,vacc5=2.5,tacc5=2.2,taker_ratio=0.67,
            compression=0.8,near_resistance_pct=2.0,cross_lead5=3.0,
            higher_low=True,catalyst={'score':80,'sentiment':'positive','age_min':10})
        self.assertTrue(any('price already moving' in x for x in risks))
        self.assertTrue(any('24h move already extended' in x for x in risks))

    def test_rss_catalyst_parser(self):
        raw=b"""<?xml version="1.0"?><rss><channel><item><title>Chainlink launches major institutional integration</title><link>https://example.test/a</link><pubDate>Thu, 20 Aug 2026 12:00:00 GMT</pubDate><description>New integration announced.</description></item></channel></rss>"""
        rows=Scanner._parse_feed(raw,"TestSource","A")
        self.assertEqual(len(rows),1)
        c=Scanner._classify_catalyst(rows[0])
        self.assertEqual(c["sentiment"],"positive")
        self.assertGreater(c["score"],40)

if __name__=='__main__':
    unittest.main()
