import asyncio
import websockets
import time
import secrets
import json
import os
from collections import defaultdict, deque

from dataclasses import dataclass, field
from typing import Dict, Deque

import websockets
from websockets.server import WebSocketServerProtocol

# ---------------- Config ---------------- #
HOST = "0.0.0.0"
PORT = 2025
ROOM_HISTORY_SIZE = 200

# ---------------- Data Structures ---------------- #
@dataclass

class Client:
    ws: WebSocketServerProtocol
    username: str
    room: str
    id: str = field(default_factory=lambda: secrets.token_hex(8))

# Active rooms: room -> dict of client_id -> Client
rooms: Dict[str, Dict[str, Client]] = defaultdict(dict)
# Room histories: room -> deque of messages
room_history: Dict[str, Deque[dict]] = defaultdict(lambda: deque(maxlen=ROOM_HISTORY_SIZE))


# ---------------- Helper Functions ---------------- #
def now_ms() -> int:
    return int(time.time() * 1000)


async def send(ws, payload: dict):
    """Send JSON to one client."""
    await ws.send(json.dumps(payload))


async def broadcast(room: str, payload: dict):
    """Broadcast JSON to all clients in a room."""
    if room not in rooms:
        return
    message = json.dumps(payload)
    await asyncio.gather(
        *[c.ws.send(message) for c in rooms[room].values()],
        return_exceptions=True
    )


def add_history(room: str, msg: dict):
    """Store message in room history."""
    room_history[room].append(msg)


async def send_history(ws, room: str):
    """Send stored history to one client."""
    history = list(room_history[room])
    if history:
        await send(ws, {"type": "history", "room": room, "messages": history})


# ---------------- Action Handlers ---------------- #
async def handle_join(client: Client, data: dict):
    room = data.get("room")
    username = data.get("username")

    # Leave old room if joined
    if client.room:
        rooms[client.room].pop(client.id, None)

    client.room = room
    client.username = username
    rooms[room][client.id] = client   # store by id

    # Send confirmation + history
    await send(client.ws, {"type": "joined", "room": room, "username": username})
    await send_history(client.ws, room)

    # Broadcast join system message
    sysmsg = {
        "type": "system",
        "room": room,
        "text": f"{username} joined the room.",
        "ts": now_ms(),
    }
    add_history(room, sysmsg)
    await broadcast(room, sysmsg)


async def handle_message(client: Client, data: dict):
    if not client.room:
        return
    msg = {
        "type": "message",
        "room": client.room,
        "from": client.username,
        "text": data.get("text"),
        "ts": now_ms(),
    }
    add_history(client.room, msg)
    await broadcast(client.room, msg)


async def handle_leave(client: Client):
    if not client.room:
        return
    room = client.room
    rooms[room].pop(client.id, None)
    sysmsg = {
        "type": "system",
        "room": room,
        "text": f"{client.username} left the room.",
        "ts": now_ms(),
    }
    add_history(room, sysmsg)
    await broadcast(room, sysmsg)
    client.room = ""


# ---------------- Client Connection Handler ---------------- #
async def client_handler(ws: WebSocketServerProtocol):
    client = Client(ws=ws, username="", room="")
    try:
        async for raw in ws:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            action = data.get("action")

            if action == "join":
                await handle_join(client, data)
            elif action == "message":
                await handle_message(client, data)
            elif action == "leave":
                await handle_leave(client)
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if client.room:
            await handle_leave(client)


# ---------------- Main Server ---------------- #
async def main():
    async with websockets.serve(client_handler, HOST, PORT):
        print(f"Chat server running on ws://{HOST}:{PORT}")
        await asyncio.Future()  # Run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Server stopped.")
