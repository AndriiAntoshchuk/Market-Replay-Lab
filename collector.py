import asyncio
import websockets
import json
from pathlib import Path
from datetime import datetime as dt, timezone as tz

# Custom exception used to rotate the output file when UTC date changes.
class DateChangedError(Exception):
    pass

# Coinbase public WebSocket endpoint.
uri = "wss://ws-feed.exchange.coinbase.com"

# Subscribe to ETH-USD Level 2 batched order book updates.
subscription_request = {
    "type": "subscribe",
    "product_ids": ["ETH-USD"],
    "channels": [
        "level2_batch",
    ],
}

# Always use UTC so filenames and timestamps are consistent.
def get_now():
    return dt.now(tz.utc)

def get_date() -> str:
    return get_now().strftime("%Y-%m-%d")

def get_time() -> str:
    return get_now().strftime("%H-%M-%S")

def create_new_dir(path):
    Path(path).mkdir(parents=True, exist_ok=True)


# Rename the current file when a session ends, so the final filename
# reflects both the start and end time of the capture.
def rename_file(filepath):
    if Path(filepath).exists():
        Path(filepath).rename(f"{filepath[:-6]}-{get_time()}.jsonl")

async def main():
    while True:

        # Start a new capture session.
        start_date = get_date()
        start_time = get_time()
        dirpath = f"data/raw/{start_date}"
        filepath = f"{dirpath}/data_{start_time}.jsonl"
        counter = 0
        try:
            print(f"Connecting to server...")
            async with websockets.connect(uri) as ws:
                print(f"Connected to server")
                print(f"Sending subscription request...")
                await ws.send(json.dumps(subscription_request))
                print(f"Subscription request is sent")
                print(f"Starting to read and store messages")
                create_new_dir(dirpath)

                # Store raw messages as JSONL so they can be replayed later line by line.
                with open(filepath, "a") as file:
                    while True:
                        msg = await ws.recv()
                        record = {
                            "timestamp":get_now().isoformat(),
                            "message":json.loads(msg)
                        }
                        file.write(json.dumps(record) + '\n')
                        counter += 1

                        # Flush periodically so data is not stuck in memory too long,
                        if(counter % 50 == 0):
                            print(f"Message number {counter} was received")
                            file.flush()

                        # Force file rotation when the UTC date changes.
                        if(start_date != get_date()):
                            raise DateChangedError("Date changed")
                        
        except DateChangedError as e:
            print(f"{get_now()}:\nDateChangedError was found: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")     
        finally:
            # Always rename the file when leaving the current session
            rename_file(filepath)                

try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("Stopped by user")
