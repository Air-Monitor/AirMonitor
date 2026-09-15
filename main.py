import os
import asyncio
from datetime import timezone
from zoneinfo import ZoneInfo

from fastapi import FastAPI
from fastapi.responses import FileResponse, StreamingResponse
from telethon import TelegramClient, events
from telethon.sessions import StringSession

app = FastAPI()

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION = os.environ["TELEGRAM_SESSION"]
CHANNEL = os.environ.get("CHANNEL", "@kyiv_airdef")
KYIV = ZoneInfo("Europe/Kyiv")

messages = []
subscribers = set()
telegram_client = None

def format_message(message):
    dt = message.date
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(KYIV)
    return {
        "date": dt.strftime("%d.%m.%Y %H:%M:%S"),
        "text": message.message or ""
    }

async def telegram_loop():
    global telegram_client
    while True:
        try:
            telegram_client = TelegramClient(
                StringSession(SESSION), API_ID, API_HASH
            )
            await telegram_client.start()
            entity = await telegram_client.get_entity(CHANNEL)

            history = await telegram_client.get_messages(entity, limit=20)
            messages.clear()
            for msg in reversed(history):
                messages.append(format_message(msg))

            @telegram_client.on(events.NewMessage(chats=entity))
            async def new_message(event):
                item = format_message(event.message)
                messages.append(item)
                del messages[:-20]
                dead = set()
                for q in list(subscribers):
                    try:
                        await q.put(item)
                    except Exception:
                        dead.add(q)
                subscribers.difference_update(dead)

            print(f"Telegram подключен: {CHANNEL}", flush=True)
            await telegram_client.run_until_disconnected()
        except Exception as e:
            print(f"Telegram ошибка: {e}", flush=True)
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
async def get_messages():
    return messages[-20:]

@app.get("/stream")
async def stream():
    q = asyncio.Queue()
    subscribers.add(q)

    async def generator():
        try:
            while True:
                item = await q.get()
                import json
                yield f"data: {json.dumps(item, ensure_ascii=False)}\n\n"
        finally:
            subscribers.discard(q)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )
