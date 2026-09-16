import os
import asyncio
import json
from datetime import timezone
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from telethon import TelegramClient, events
from telethon.sessions import StringSession

app = FastAPI()
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION = os.environ["TELEGRAM_SESSION"]

MESSAGE_CHANNEL = os.environ.get("CHANNEL", "@kyiv_airdef")
ALERT_CHANNEL = os.environ.get("ALERT_CHANNEL", "@kyivoda")

KYIV = ZoneInfo("Europe/Kyiv")
messages = []
subscribers = set()
telegram_client = None

alerts = {}
alert_history = []

def format_message(message):
    dt = message.date
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(KYIV)
    return {
        "id": message.id,
        "date": dt.strftime("%d.%m.%Y %H:%M:%S"),
        "text": message.message or ""
    }

def parse_alert(message):
    text = (message.message or "").strip()
    low = text.lower()

    if "повітряна тривога" not in low:
        return None

    is_end = "відбій" in low
    is_start = not is_end

    # We intentionally keep this to public alert status only.
    # No movement/route/target information is extracted.
    if "київська область" in low or "київщина" in low:
        place = "Київська область"
    else:
        district_markers = [
            "район -", "район —", "район –", "громада -",
            "громада —", "громада –"
        ]
        place = "Київська область"
        for marker in district_markers:
            pos = low.find(marker)
            if pos > 0:
                start = max(0, pos - 60)
                fragment = text[start:pos + len(marker)].strip()
                # Prefer the text before the marker, keeping it short.
                words = fragment.split()
                if len(words) >= 2:
                    place = " ".join(words[-4:]).strip("—-–: ")
                break

    dt = message.date
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(KYIV)

    return {
        "id": message.id,
        "date": dt.strftime("%d.%m.%Y %H:%M:%S"),
        "place": place,
        "active": is_start,
        "text": text
    }

async def telegram_loop():
    global telegram_client

    while True:
        try:
            telegram_client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)
            await telegram_client.start()

            msg_entity = await telegram_client.get_entity(MESSAGE_CHANNEL)
            alert_entity = await telegram_client.get_entity(ALERT_CHANNEL)

            history = await telegram_client.get_messages(msg_entity, limit=50)
            messages.clear()
            for msg in reversed(history):
                if msg.message:
                    messages.append(format_message(msg))

            # Build current alert state from recent official Kyiv OVA posts.
            alerts.clear()
            alert_history.clear()
            alert_msgs = await telegram_client.get_messages(alert_entity, limit=100)
            for msg in reversed(alert_msgs):
                item = parse_alert(msg)
                if not item:
                    continue
                alert_history.append(item)
                alerts[item["place"]] = item["active"]

            @telegram_client.on(events.NewMessage(chats=msg_entity))
            async def new_message(event):
                if event.message and event.message.message:
                    item = format_message(event.message)
                    messages.append(item)
                    del messages[:-500]
                    for q in list(subscribers):
                        try:
                            await q.put(item)
                        except Exception:
                            subscribers.discard(q)

            @telegram_client.on(events.NewMessage(chats=alert_entity))
            async def new_alert(event):
                item = parse_alert(event.message)
                if not item:
                    return
                alerts[item["place"]] = item["active"]
                alert_history.append(item)
                del alert_history[:-100]

            print(
                f"Telegram connected: {MESSAGE_CHANNEL}; alerts: {ALERT_CHANNEL}",
                flush=True
            )
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

@app.on_event("startup")
async def startup():
    asyncio.create_task(telegram_loop())

@app.get("/")
async def index():
    return FileResponse("AirMonitor_iPhone.html")

@app.get("/messages")
async def get_messages(limit: int = Query(50, ge=1, le=100)):
    return JSONResponse(messages[-limit:])

@app.get("/history")
async def get_history(
    offset_id: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100)
):
    if telegram_client is None or not telegram_client.is_connected():
        return JSONResponse([])

    try:
        entity = await telegram_client.get_entity(MESSAGE_CHANNEL)
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
    active = [
        {"place": place, "active": state}
        for place, state in alerts.items()
        if state
    ]
    return JSONResponse({
        "enabled": True,
        "source": ALERT_CHANNEL,
        "alerts": active,
        "recent": alert_history[-20:]
    })

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

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )
