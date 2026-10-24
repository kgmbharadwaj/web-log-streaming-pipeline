"""Reads web logs from Kafka and loads them into SQLite in batches.
Usage:  python consumer.py            (runs until Ctrl+C)
        python consumer.py --idle 15  (stops after 15s with no new messages)
"""
import argparse, json, time
from db import init_db, DB_PATH

TOPIC = "web-logs"
COLS = ("event_id", "ts", "ip", "method", "path", "status", "response_ms", "user_agent")
REQUIRED = ("event_id", "ts", "ip", "method", "path", "status", "response_ms")


def parse(raw: bytes):
    """Return a row tuple, or None if the message is malformed."""
    try:
        d = json.loads(raw)
        if any(d.get(k) is None for k in REQUIRED):
            return None
        return tuple(d.get(c) for c in COLS)
    except (ValueError, TypeError):
        return None


def flush(conn, consumer, batch):
    if not batch:
        return 0
    conn.executemany(
        f"INSERT OR IGNORE INTO web_logs ({','.join(COLS)}) VALUES ({','.join('?' * len(COLS))})", batch
    )
    conn.commit()
    consumer.commit(asynchronous=False)  # commit offsets only after the DB write succeeded
    return len(batch)


def main():
    from confluent_kafka import Consumer

    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", default="localhost:9092")
    ap.add_argument("--db", default=DB_PATH)
    ap.add_argument("--batch", type=int, default=100)
    ap.add_argument("--idle", type=int, default=0, help="exit after N idle seconds (0 = never)")
    args = ap.parse_args()

    conn = init_db(args.db)
    consumer = Consumer({
        "bootstrap.servers": args.broker,
        "group.id": "weblog-loader",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])

    batch, total, bad = [], 0, 0
    last_msg = time.time()
    print(f"Consuming '{TOPIC}' -> {args.db}. Ctrl+C to stop.")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if batch:
                    total += flush(conn, consumer, batch); batch = []
                    print(f"loaded {total} rows")
                if args.idle and time.time() - last_msg > args.idle:
                    break
                continue
            if msg.error():
                print("Kafka error:", msg.error())
                continue
            last_msg = time.time()
            row = parse(msg.value())
            if row is None:
                bad += 1
                continue
            batch.append(row)
            if len(batch) >= args.batch:
                total += flush(conn, consumer, batch); batch = []
                print(f"loaded {total} rows")
    except KeyboardInterrupt:
        pass
    finally:
        total += flush(conn, consumer, batch)
        consumer.close()
        conn.close()
        print(f"Stopped. Loaded {total} rows, skipped {bad} malformed messages.")


if __name__ == "__main__":
    main()
