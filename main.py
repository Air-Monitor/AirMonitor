import os
import asyncio
import json
from datetime import timezone
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from telethon import TelegramClient, events
from telethon.sessions import StringSession

app = FastAPI()
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION = os.environ["TELEGRAM_SESSION"]
CHANNEL = os.environ.get("CHANNEL", "@kyiv_airdef")
ALERTS_API_TOKEN = os.environ.get("ALERTS_API_TOKEN", "").strip()
KYIV = ZoneInfo("Europe/Kyiv")
messages = []
subscribers = set()
telegram_client = None
alerts_cache = []
alerts_updated_at = None

def format_message(message):
    dt = message.date
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(KYIV)
    return {"id": message.id, "date": dt.strftime("%d.%m.%Y %H:%M:%S"), "text": message.message or ""}

async def telegram_loop():
    global telegram_client
    while True:
        try:
            telegram_client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)
            await telegram_client.start()
            entity = await telegram_client.get_entity(CHANNEL)
            history = await telegram_client.get_messages(entity, limit=50)
            messages.clear()
            for msg in reversed(history):
                if msg.message:
                    messages.append(format_message(msg))
            @telegram_client.on(events.NewMessage(chats=entity))
            async def new_message(event):
                if event.message and event.message.message:
                    item = format_message(event.message)
                    messages.append(item)
                    del messages[:-50]
                    for q in list(subscribers):
                        try:
                            await q.put(item)
                        except Exception:
                            subscribers.discard(q)
            print(f"Telegram connected: {CHANNEL}", flush=True)
            await telegram_client.run_until_disconnected()
        except Exception as e:
            print(f"Telegram error: {e}", flush=True)
            try:
                if telegram_client:
                    await telegram_client.disconnect()
            except Exception:
                pass
            telegram_client = None
            await asyncio.sleep(10)

async def alerts_loop():
    global alerts_cache, alerts_updated_at
    if not ALERTS_API_TOKEN:
        print("ALERTS_API_TOKEN not set; air-alert block disabled.", flush=True)
        return
    url = "https://api.alerts.in.ua/v1/alerts/active.json"
    while True:
        try:
            headers = {"Authorization": f"Bearer {ALERTS_API_TOKEN}"}
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get(url, headers=headers)
                r.raise_for_status()
                data = r.json()
            result = []
            for a in data.get("alerts", []):
                if a.get("alert_type") != "air_raid":
                    continue
                if a.get("location_oblast") != "Київська область":
                    continue
                result.append({
                    "id": a.get("id"),
                    "location_title": a.get("location_title"),
                    "location_type": a.get("location_type"),
                    "location_raion": a.get("location_raion"),
                    "started_at": a.get("started_at"),
                    "updated_at": a.get("updated_at"),
                    "alert_level": a.get("alert_level"),
                })
            alerts_cache = result
            from datetime import datetime
            alerts_updated_at = datetime.now(KYIV).isoformat()
        except Exception as e:
            print(f"Alerts API error: {e}", flush=True)
        await asyncio.sleep(30)

@app.on_event("startup")
async def startup():
    asyncio.create_task(telegram_loop())
    asyncio.create_task(alerts_loop())

@app.get("/")
async def index():
    return FileResponse("AirMonitor_iPhone.html")

@app.get("/messages")
async def get_messages(limit: int = Query(50, ge=1, le=100)):
    return JSONResponse(messages[-limit:])

@app.get("/history")
async def get_history(offset_id: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=100)):
    if telegram_client is None or not telegram_client.is_connected():
        return JSONResponse([])
    try:
        entity = await telegram_client.get_entity(CHANNEL)
        kwargs = {"limit": limit}
        if offset_id:
            kwargs["offset_id"] = offset_id
        result = []
        async for msg in telegram_client.iter_messages(entity, **kwargs):
            if msg.message:
                result.append(format_message(msg))
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=503)

@app.get("/alerts")
async def get_alerts():
    return JSONResponse({"enabled": bool(ALERTS_API_TOKEN), "updated_at": alerts_updated_at, "alerts": alerts_cache})

@app.get("/stream")
async def stream():
    q = asyncio.Queue()
    subscribers.add(q)
    async def generator():
        try:
            while True:
                item = await q.get()
                yield f"data: {json.dumps(item, ensure_ascii=False)}\n\n"
        except asyncio.CancelledError:
            raise
        finally:
            subscribers.discard(q)
    return StreamingResponse(generator(), media_type="text/event-stream", headers={"Cache-Control":"no-cache","Connection":"keep-alive","X-Accel-Buffering":"no"})
