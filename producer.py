"""Simulates web server logs and streams them into Kafka.
Usage:  python producer.py --rate 20 --count 500
"""
import argparse, json, random, time, uuid
from datetime import datetime, timezone

TOPIC = "web-logs"
PATHS = ["/", "/login", "/products", "/cart", "/checkout", "/api/orders", "/search"]
# chance that a request to this path fails (4xx/5xx)
ERROR_RATE = {"/": 0.02, "/login": 0.08, "/products": 0.03, "/cart": 0.05,
              "/checkout": 0.15, "/api/orders": 0.12, "/search": 0.04}
AGENTS = ["Chrome/126", "Firefox/127", "Safari/17", "Edge/126", "curl/8.4"]
IPS = [f"192.168.{random.randint(0, 3)}.{random.randint(1, 60)}" for _ in range(40)]


def generate_log() -> dict:
    path = random.choice(PATHS)
    if random.random() < ERROR_RATE[path]:
        status = random.choice([400, 403, 404, 404, 500, 502, 503])
    else:
        status = random.choices([200, 201, 301, 304], weights=[80, 5, 7, 8])[0]
    slow = path in ("/checkout", "/api/orders", "/search")
    return {
        "event_id": str(uuid.uuid4()),
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "ip": random.choice(IPS),
        "method": "POST" if path in ("/login", "/checkout", "/api/orders") else "GET",
        "path": path,
        "status": status,
        "response_ms": max(5, int(random.gauss(400 if slow else 120, 60))) if status < 500 else random.randint(800, 3000),
        "user_agent": random.choice(AGENTS),
    }


def main():
    from confluent_kafka import Producer  # imported here so generate_log() can be tested without Kafka

    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=10, help="messages per second")
    ap.add_argument("--count", type=int, default=0, help="stop after N messages (0 = run forever)")
    ap.add_argument("--broker", default="localhost:9092")
    args = ap.parse_args()

    producer = Producer({"bootstrap.servers": args.broker})
    sent = 0
    print(f"Producing to '{TOPIC}' at {args.rate}/s. Ctrl+C to stop.")
    try:
        while args.count == 0 or sent < args.count:
            log = generate_log()
            producer.produce(TOPIC, key=log["ip"], value=json.dumps(log))
            producer.poll(0)
            sent += 1
            if sent % 50 == 0:
                print(f"sent {sent}")
            time.sleep(1 / args.rate)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(10)
        print(f"Done. Sent {sent} messages.")


if __name__ == "__main__":
    main()
