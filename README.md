# VoltTrade Core v0.2-web

VoltTrade on **early-momentum scanner**, mitte "top gainers" leht. Eesmärk on märgata liikumist enne, kui tulemus on juba käes.

## Põhimõtted
- Kraken EUR paarid, avalik turuinfo; API võtit pole vaja.
- Kraken WebSocket v2 ticker live-hind, bid/ask, 24h muutus, high, volume.
- 5m / 15m / 1h muutused tekivad live-ajaloost pärast käivitamist.
- Early-zone: 15m +0.8…4%, 1h +1…6%, 24h alla +12%.
- 15m mahu kiirendus: siht >=1.8x võrreldes 24h keskmise 15m tempoga.
- Eelistab BTC suhtelist tugevust, normaalset spreadi ning vähemalt 3% ruumi 24h tipuni.
- Juba liiga kaugele liikunud vara -> EXTENDED, mitte ostusignaal.

## Score 0–100
- Volume 25
- Price momentum 20
- Structure 20
- BTC relative strength 15
- Order-flow/spread proxy 10
- Resistance room 10

Staatused: IGNORE <50, WATCH >=50, EARLY >=65, STRONG >=80; lisaks EXTENDED ja REJECT.

## Käivitamine Windowsis
Topeltklõps `run.bat`. Seejärel ava `http://localhost:8080`.

## Docker
```bash
docker build -t volttrade .
docker run --rm -p 8080:8080 volttrade
```

## Oluline
See versioon **ei tee automaatseid tehinguid**. See on scanner/otsustuskiht, mida saab valideerida €1 live-testidega. `data/signals.csv` logib EARLY/STRONG signaale, et hiljem mõõta 24h/48h/72h tulemust, hit rate'i, expectancy't ja drawdown'i.

Kraken Consumeri konto-spetsiifilist ostetavust ei saa avalikust market feedist garanteerida; enne testostu tuleb token Krakeni äpis üle kontrollida.
