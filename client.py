import asyncio
import json
import websockets
import sys

SERVER_URL = "ws://localhost:2025"

# Optional: predefine available rooms
AVAILABLE_ROOMS = ["Room 01", "Room 02", "Room 03", "Room 04"]


async def receiver(ws):
    """Listen for messages from the server."""
    try:
        async for msg in ws:
            try:
                data = json.loads(msg)
            except json.JSONDecodeError:
                print("Received non-JSON:", msg)
                continue

            t = data.get("type")
            if t == "system":
                print(f"[SYSTEM][{data['room']}] {data['text']}")
            elif t == "message":
                print(f"[{data['room']}] {data['from']}: {data['text']}")
            elif t == "joined":
                print(f"Joined room '{data['room']}' as {data['username']}")
            elif t == "history":
                print(f"Chat history for {data['room']}:")
                for h in data["messages"]:
                    if h["type"] == "message":
                        print(f"[{h['room']}] {h['from']}: {h['text']}")
                    else:
                        print(f"[SYSTEM][{h['room']}] {h['text']}")
            else:
                print("📩", data)
    except websockets.exceptions.ConnectionClosed:
        print("Connection closed.")


async def sender(ws, username, room):
    """Read user input and send to server."""
    # Join the room first
    await ws.send(json.dumps({"action": "join", "username": username, "room": room}))

    loop = asyncio.get_event_loop()
    while True:
        text = await loop.run_in_executor(None, sys.stdin.readline)
        text = text.strip()
        if not text:
            continue

        if text.lower() in {"/quit", "/exit"}:
            await ws.send(json.dumps({"action": "leave"}))
            await ws.close()
            break
        else:
            await ws.send(json.dumps({"action": "message", "text": text}))


async def main():
    if len(sys.argv) < 3:
        print(f"Usage: python {sys.argv[0]} <username> <room>")
        print(f"Available rooms: {', '.join(AVAILABLE_ROOMS)}")
        return

    username = sys.argv[1]
    room = sys.argv[2]

    if room not in AVAILABLE_ROOMS:
        print(f"Invalid room. Choose one of: {', '.join(AVAILABLE_ROOMS)}")
        return

    async with websockets.connect(SERVER_URL) as ws:
        await asyncio.gather(receiver(ws), sender(ws, username, room))


if __name__ == "__main__":
    asyncio.run(main())
