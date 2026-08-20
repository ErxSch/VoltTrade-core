import asyncio, json, os
from pathlib import Path
from aiohttp import web
from scanner import Scanner

ROOT = Path(__file__).resolve().parents[1]
CFG = json.loads((ROOT / 'config.json').read_text(encoding='utf-8'))
scanner = Scanner(CFG, ROOT)

async def index(req):
    return web.FileResponse(ROOT / 'app/static/index.html')

async def scan(req):
    rows = scanner.snapshot()
    visible = [x for x in rows if x['status'] != 'IGNORE'][:220]
    return web.json_response({
        'connected': scanner.connected,
        'error': scanner.last_error,
        'count': len(scanner.tickers),
        'rows': visible,
        'warming': sum(1 for x in visible if x['status'] == 'DATA'),
        'sources': scanner.source_health(),
        'catalysts': len(scanner.catalysts),
    })

async def detail(req):
    d = scanner.detail(req.query.get('symbol', ''))
    if not d:
        raise web.HTTPNotFound(text='Unknown symbol')
    return web.json_response(d)

async def news(req):
    return web.json_response({'items': scanner.news_snapshot()})

async def health(req):
    return web.json_response({
        'ok': True,
        'version': '0.6-premomentum',
        'pairs': len(scanner.tickers),
        'enriched': len(scanner.candles_1m),
        'sources': scanner.source_health(),
    })

async def on_start(app):
    app['task'] = asyncio.create_task(scanner.run())

async def on_stop(app):
    app['task'].cancel()

app = web.Application()
app.router.add_get('/', index)
app.router.add_get('/api/scan', scan)
app.router.add_get('/api/detail', detail)
app.router.add_get('/api/news', news)
app.router.add_get('/health', health)
app.router.add_static('/static', ROOT / 'app/static')
app.on_startup.append(on_start)
app.on_cleanup.append(on_stop)

if __name__ == '__main__':
    web.run_app(app, host='0.0.0.0', port=int(os.getenv('PORT', '8080')))
