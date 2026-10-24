"""Runs every query in queries.sql and prints the results as tables.
Usage: python run_queries.py
"""
import sqlite3
from db import DB_PATH


def main():
    conn = sqlite3.connect(DB_PATH)
    blocks = [b.strip() for b in open("queries.sql", encoding="utf-8").read().split(";") if b.strip()]
    for block in blocks:
        lines = block.splitlines()
        title = lines[0].lstrip("- ").strip() if lines[0].startswith("--") else "Query"
        cur = conn.execute(block)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        print(f"\n=== {title} ===")
        widths = [max(len(str(c)), *(len(str(r[i])) for r in rows)) if rows else len(str(c)) for i, c in enumerate(cols)]
        print("  ".join(str(c).ljust(w) for c, w in zip(cols, widths)))
        print("  ".join("-" * w for w in widths))
        for r in rows:
            print("  ".join(str(v).ljust(w) for v, w in zip(r, widths)))
    conn.close()


if __name__ == "__main__":
    main()
