"""Redis-backed queue ticket demo for a Redis course project."""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from redis.asyncio import Redis


REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
COUNTER_KEY = "queue:ticket-counter"
WAITING_KEY = "queue:waiting"
CURRENT_KEY = "queue:current"
EVENTS_KEY = "queue:events"
CHANNEL = "queue:updates"

redis_client: Redis | None = None


def now_text() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


# One atomic Redis operation: allocate a number, put it in the sorted set,
# append an audit event to the stream, then notify live subscribers.
ISSUE_TICKET_LUA = """
local n = redis.call('INCR', KEYS[1])
local ticket = string.format('A%04d', n)
local created_at = ARGV[1]
redis.call('ZADD', KEYS[2], n, ticket)
redis.call('XADD', KEYS[3], 'MAXLEN', '~', 1000, '*',
  'action', 'take', 'ticket', ticket, 'time', created_at)
redis.call('PUBLISH', KEYS[4], cjson.encode({
  action = 'take', ticket = ticket, time = created_at
}))
return ticket
"""


CALL_NEXT_LUA = """
local current = redis.call('GET', KEYS[1])
if current then
  return {'BUSY', current}
end
local item = redis.call('ZPOPMIN', KEYS[2], 1)
if #item == 0 then
  return {'EMPTY', ''}
end
local ticket = item[1]
local called_at = ARGV[1]
redis.call('SET', KEYS[1], ticket)
redis.call('XADD', KEYS[3], 'MAXLEN', '~', 1000, '*',
  'action', 'call', 'ticket', ticket, 'time', called_at)
redis.call('PUBLISH', KEYS[4], cjson.encode({
  action = 'call', ticket = ticket, time = called_at
}))
return {'OK', ticket}
"""


FINISH_LUA = """
local ticket = redis.call('GET', KEYS[1])
if not ticket then
  return {'EMPTY', ''}
end
local finished_at = ARGV[1]
redis.call('DEL', KEYS[1])
redis.call('XADD', KEYS[2], 'MAXLEN', '~', 1000, '*',
  'action', 'finish', 'ticket', ticket, 'time', finished_at)
redis.call('PUBLISH', KEYS[3], cjson.encode({
  action = 'finish', ticket = ticket, time = finished_at
}))
return {'OK', ticket}
"""


@asynccontextmanager
async def lifespan(_: FastAPI):
    global redis_client
    redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
    await redis_client.ping()
    yield
    await redis_client.aclose()


app = FastAPI(title="Redis 門市叫號系統", version="1.0.0", lifespan=lifespan)


def rdb() -> Redis:
    if redis_client is None:
        raise RuntimeError("Redis 尚未連線")
    return redis_client


def action_label(action: str) -> str:
    return {"take": "取號", "call": "叫號", "finish": "完成服務"}.get(action, action)


async def read_state() -> dict[str, Any]:
    r = rdb()
    current = await r.get(CURRENT_KEY)
    waiting = await r.zrange(WAITING_KEY, 0, 19)
    raw_events = await r.xrevrange(EVENTS_KEY, count=10)
    events = [
        {
            "id": event_id,
            "action": fields.get("action", ""),
            "label": action_label(fields.get("action", "")),
            "ticket": fields.get("ticket", ""),
            "time": fields.get("time", ""),
        }
        for event_id, fields in raw_events
    ]
    return {
        "current": current,
        "waiting": waiting,
        "waiting_count": await r.zcard(WAITING_KEY),
        "events": events,
        "event_count": await r.xlen(EVENTS_KEY),
    }


@app.get("/", response_class=HTMLResponse)
async def customer_page() -> str:
    return PAGE.replace("__PAGE_MODE__", "customer")


@app.get("/counter", response_class=HTMLResponse)
async def counter_page() -> str:
    return PAGE.replace("__PAGE_MODE__", "counter")


@app.get("/api/state")
async def api_state() -> dict[str, Any]:
    return await read_state()


@app.post("/api/tickets")
async def take_ticket() -> dict[str, str]:
    ticket = await rdb().eval(
        ISSUE_TICKET_LUA,
        4,
        COUNTER_KEY,
        WAITING_KEY,
        EVENTS_KEY,
        CHANNEL,
        now_text(),
    )
    return {"ticket": ticket}


@app.post("/api/call-next")
async def call_next() -> dict[str, str]:
    result = await rdb().eval(
        CALL_NEXT_LUA, 4, CURRENT_KEY, WAITING_KEY, EVENTS_KEY, CHANNEL, now_text()
    )
    status, ticket = result
    if status == "BUSY":
        raise HTTPException(409, f"請先完成目前號碼 {ticket} 的服務")
    if status == "EMPTY":
        raise HTTPException(409, "目前沒有候位號碼")
    return {"ticket": ticket}


@app.post("/api/finish")
async def finish_current() -> dict[str, str]:
    status, ticket = await rdb().eval(FINISH_LUA, 3, CURRENT_KEY, EVENTS_KEY, CHANNEL, now_text())
    if status == "EMPTY":
        raise HTTPException(409, "目前沒有服務中的號碼")
    return {"ticket": ticket}


@app.get("/events")
async def server_events() -> StreamingResponse:
    async def stream():
        pubsub = rdb().pubsub()
        await pubsub.subscribe(CHANNEL)
        try:
            yield "event: ready\ndata: {}\n\n"
            while True:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                if message and message.get("type") == "message":
                    payload = message["data"]
                    yield f"data: {payload}\n\n"
                else:
                    yield ": keepalive\n\n"
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            raise
        finally:
            await pubsub.unsubscribe(CHANNEL)
            await pubsub.aclose()

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/health")
async def health() -> JSONResponse:
    try:
        await rdb().ping()
        return JSONResponse({"status": "ok", "redis": "connected"})
    except Exception as exc:  # Report a useful health-check error to the demo operator.
        return JSONResponse({"status": "error", "detail": str(exc)}, status_code=503)


PAGE = r"""<!doctype html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Redis 門市叫號系統</title>
  <style>
    :root { color-scheme: light; --ink:#172033; --muted:#68738a; --blue:#3157d5; --line:#e5e9f2; }
    * { box-sizing:border-box; }
    body { margin:0; background:#f4f6fb; color:var(--ink); font:16px/1.5 system-ui,"Noto Sans TC",sans-serif; }
    header { background:#14234b; color:white; padding:22px max(24px,calc((100vw - 1100px)/2)); display:flex; align-items:center; justify-content:space-between; gap:16px; }
    header h1 { margin:0; font-size:22px; }
    nav { display:flex; gap:10px; }
    nav a { color:#dce5ff; text-decoration:none; padding:7px 11px; border:1px solid #56658c; border-radius:9px; }
    main { max-width:1100px; margin:28px auto; padding:0 20px; }
    .grid { display:grid; grid-template-columns:1.2fr .8fr; gap:18px; }
    .page-intro { margin:0 0 18px; padding:17px 20px; background:#eaf0ff; border-left:5px solid #3157d5; border-radius:12px; }
    .page-intro h2 { margin:0 0 3px; font-size:22px; }
    .page-intro p { margin:0; color:var(--muted); }
    .role-tag { display:inline-block; margin-bottom:5px; color:#3157d5; font-size:11px; font-weight:800; letter-spacing:.12em; }
    .card { background:white; border:1px solid var(--line); border-radius:16px; padding:22px; box-shadow:0 8px 28px #1b2b4b0a; }
    .eyebrow { color:var(--muted); font-size:13px; letter-spacing:.04em; }
    .current { font-size:clamp(54px,9vw,96px); font-weight:800; color:var(--blue); line-height:1.1; margin:10px 0; }
    .count { font-size:36px; font-weight:750; margin:8px 0; }
    .muted { color:var(--muted); }
    button { border:0; border-radius:11px; color:white; background:var(--blue); padding:12px 18px; font:inherit; font-weight:700; cursor:pointer; }
    button.secondary { background:#17233e; }
    button:disabled { opacity:.48; cursor:not-allowed; }
    .actions { display:flex; flex-wrap:wrap; gap:10px; margin-top:17px; }
    .actions[hidden] { display:none !important; }
    .ticket { display:inline-block; background:#edf1ff; color:#2747b1; border-radius:9px; padding:5px 10px; font-weight:750; margin:3px; }
    .list { display:flex; flex-wrap:wrap; margin-top:12px; }
    table { border-collapse:collapse; width:100%; }
    th,td { text-align:left; border-bottom:1px solid var(--line); padding:9px 5px; font-size:14px; }
    th { color:var(--muted); font-weight:600; }
    .status { color:#157347; font-size:13px; }
    body[data-mode="counter"] header { background:#173b2b; }
    body[data-mode="counter"] .page-intro { background:#eaf7ef; border-color:#198754; }
    body[data-mode="counter"] .role-tag { color:#157347; }
    body[data-mode="counter"] .current { color:#157347; }
    body[data-mode="counter"] #counter-actions button { min-width:190px; min-height:54px; font-size:17px; }
    body[data-mode="customer"] #customer-actions button { min-width:190px; min-height:54px; font-size:17px; }
    .message { min-height:24px; margin-top:12px; color:#3157d5; font-weight:650; }
    footer { text-align:center; color:var(--muted); font-size:12px; padding:22px; }
    @media(max-width:760px) { .grid { grid-template-columns:1fr; } header { align-items:flex-start; flex-direction:column; } }
  </style>
</head>
<body data-mode="__PAGE_MODE__">
  <header>
    <h1>Redis 門市叫號系統</h1>
    <nav><a href="/">顧客取號</a><a href="/counter">櫃台控制</a></nav>
  </header>
  <main>
    <section class="page-intro">
      <span class="role-tag" id="role-tag">CUSTOMER</span>
      <h2 id="page-title">顧客取號</h2>
      <p id="page-description">領取候位號碼，並即時查看目前叫號與等待人數。</p>
    </section>
    <div class="grid">
      <section class="card">
        <div class="eyebrow">目前服務號碼</div>
        <div class="current" id="current">—</div>
        <div class="muted" id="current-caption">尚未叫號</div>
        <div class="actions" id="customer-actions">
          <button onclick="act('/api/tickets','已取號')">我要取號</button>
        </div>
        <div class="actions" id="counter-actions" hidden>
          <button onclick="act('/api/call-next','已叫下一號')">叫下一號</button>
          <button class="secondary" onclick="act('/api/finish','服務完成')">完成目前服務</button>
        </div>
        <div class="message" id="message" role="status"></div>
        <div class="eyebrow" style="margin-top:18px">等待人數</div>
        <div class="count" id="waiting-count">0</div>
        <div class="muted">目前候位號碼（最多顯示前 20 位）</div>
        <div class="list" id="waiting"></div>
      </section>
      <section class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;gap:10px">
          <strong>最近叫號紀錄</strong><span class="status" id="connection">連線中…</span>
        </div>
        <p class="muted">Redis Streams 累計事件：<b id="event-count">0</b></p>
        <table><thead><tr><th>時間</th><th>動作</th><th>號碼</th></tr></thead><tbody id="events"></tbody></table>
      </section>
    </div>
  </main>
  <footer>Redis Sorted Set 候位 · Streams 事件紀錄 · Pub/Sub 即時通知</footer>
  <script>
    const mode = document.body.dataset.mode;
    document.getElementById('customer-actions').hidden = mode !== 'customer';
    document.getElementById('counter-actions').hidden = mode !== 'counter';
    if (mode === 'counter') {
      document.getElementById('role-tag').textContent = 'STAFF CONTROL';
      document.getElementById('page-title').textContent = '櫃台叫號控制台';
      document.getElementById('page-description').textContent = '叫下一號、完成服務，並即時查看候位狀態與操作紀錄。';
      document.querySelector('header h1').textContent = '櫃台控制台';
      document.title = '櫃台控制台｜Redis 門市叫號系統';
    } else {
      document.getElementById('role-tag').textContent = 'CUSTOMER';
      document.getElementById('page-title').textContent = '顧客取號';
      document.getElementById('page-description').textContent = '領取候位號碼，並即時查看目前叫號與等待人數。';
      document.querySelector('header h1').textContent = '顧客取號';
      document.title = '顧客取號｜Redis 門市叫號系統';
    }
    const labels = {take:'取號', call:'叫號', finish:'完成服務'};
    async function refresh() {
      try {
        const response = await fetch('/api/state');
        const state = await response.json();
        document.getElementById('current').textContent = state.current || '—';
        document.getElementById('current-caption').textContent = state.current ? '請目前號碼至櫃台' : '尚未叫號';
        document.getElementById('waiting-count').textContent = state.waiting_count;
        document.getElementById('event-count').textContent = state.event_count;
        document.getElementById('waiting').innerHTML = state.waiting.length
          ? state.waiting.map(ticket => `<span class="ticket">${ticket}</span>`).join('')
          : '<span class="muted">目前沒有候位號碼</span>';
        document.getElementById('events').innerHTML = state.events.length
          ? state.events.map(item => `<tr><td>${item.time}</td><td>${item.label}</td><td><b>${item.ticket}</b></td></tr>`).join('')
          : '<tr><td colspan="3" class="muted">尚無紀錄</td></tr>';
      } catch (_) {
        document.getElementById('connection').textContent = '連線中斷';
      }
    }
    async function act(path, successText) {
      const message = document.getElementById('message');
      try {
        const response = await fetch(path, {method:'POST'});
        const result = await response.json();
        if (!response.ok) throw new Error(result.detail || '操作失敗');
        message.textContent = `${successText}${result.ticket ? `：${result.ticket}` : ''}`;
        await refresh();
      } catch (error) { message.textContent = error.message; }
    }
    const events = new EventSource('/events');
    events.onopen = () => document.getElementById('connection').textContent = '即時連線正常';
    events.onmessage = () => refresh();
    events.onerror = () => document.getElementById('connection').textContent = '重新連線中…';
    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>"""
