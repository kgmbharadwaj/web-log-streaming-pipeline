# Web Log Streaming Pipeline
Python producer -> Apache Kafka (Docker) -> Python consumer -> SQL database (SQLite) -> SQL analytics + Bash validation.

```
producer.py  --->  Kafka topic "web-logs"  --->  consumer.py  --->  weblogs.db
                                                                       |-- queries.sql / run_queries.py
                                                                       |-- validate.sh
```

## Run
    pip install -r requirements.txt
    docker compose up -d                   # start Kafka (wait ~15 s)
    python consumer.py --idle 20           # terminal 1
    python producer.py --rate 20 --count 500   # terminal 2
    python run_queries.py                  # traffic + error-rate analysis
    bash validate.sh                       # data-quality checks (Git Bash / WSL on Windows)
    docker compose down

## Design notes
- Consumer commits Kafka offsets only after the DB write succeeds (at-least-once delivery).
- `event_id` is UNIQUE and inserts use INSERT OR IGNORE, so replays never create duplicates.
- Malformed messages are counted and skipped, not crashed on.
