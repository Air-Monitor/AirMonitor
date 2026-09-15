import os
import asyncio
import json
from collections import deque
from pathlib import Path
from zoneinfo import ZoneInfo

from fastapi import FastAPI
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from telethon import TelegramClient, events
from telethon.sessions import StringSession

BASE_DIR = Path(__file__).resolve().parent
CHANNEL = os.getenv("CHANNEL", "@kyiv_airdef")
PORT = int(os.getenv("PORT", "10000"))

API_ID = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()
SESSION_STRING = os.getenv("TELEGRAM_SESSION", "").strip()

app = FastAPI()
messages = deque(maxlen=20)
subscribers = set()
telegram_task = None

KYIV_TZ = ZoneInfo("Europe/Kyiv")

def add_message(text, when):
    item = {
        "date": when.astimezone(KYIV_TZ).isoformat(),
        "text": text,
    }
    messages.append(item)
    dead = []
    for q in list(subscribers):
        try:
            q.put_nowait(item)
        except Exception:
            dead.append(q)
    for q in dead:
        subscribers.discard(q)

async def telegram_worker():
    if not API_ID or not API_HASH or not SESSION_STRING:
        print("ERROR: API_ID, API_HASH and TELEGRAM_SESSION must be set.")
        return

    while True:
        client = None
        try:
            client = TelegramClient(StringSession(SESSION_STRING), int(API_ID), API_HASH)
            await client.connect()
            if not await client.is_user_authorized():
                print("ERROR: Telegram session is not authorized.")
                return

            entity = await client.get_entity(CHANNEL)
            history = await client.get_messages(entity, limit=20)
            messages.clear()
            for m in reversed(history):
                if m.message:
                    add_message(m.message, m.date)

            print("Telegram connected:", CHANNEL)

            @client.on(events.NewMessage(chats=entity))
            async def handler(event):
                if event.message and event.message.message:
                    add_message(event.message.message, event.message.date)

            await client.run_until_disconnected()

        except Exception as e:
            print("Telegram connection error; retrying in 10 sec:", repr(e))
            await asyncio.sleep(10)
        finally:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass

@app.on_event("startup")
async def startup():
    global telegram_task
    telegram_task = asyncio.create_task(telegram_worker())

@app.get("/")
async def index():
    return FileResponse(BASE_DIR / "AirMonitor_iPhone.html")

@app.get("/messages")
async def get_messages():
    return JSONResponse(list(messages))

@app.get("/stream")
async def stream():
    q = asyncio.Queue()
    subscribers.add(q)

    async def generator():
        try:
            while True:
                item = await q.get()
                yield "data:" + json.dumps(item, ensure_ascii=False) + "\n\n"
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
            "X-Accel-Buffering": "no",
        },
    )
