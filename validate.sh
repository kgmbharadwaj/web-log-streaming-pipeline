#!/usr/bin/env bash
# Automatic data-quality checks on the loaded web logs.
# Usage: bash validate.sh        (exit code 0 = all passed, 1 = something failed)
DB="${1:-weblogs.db}"
FAILED=0

# Use the sqlite3 CLI if installed, otherwise fall back to Python's sqlite3 module.
q() {
  if command -v sqlite3 >/dev/null 2>&1; then
    sqlite3 "$DB" "$1"
  else
    python -c "import sqlite3,sys; print(sqlite3.connect(sys.argv[1]).execute(sys.argv[2]).fetchone()[0])" "$DB" "$1"
  fi
}

check() {  # check "description" "SQL returning a count" "expectation: zero|positive"
  local desc="$1" sql="$2" mode="$3" n
  n=$(q "$sql" | tr -d '\r')
  if { [ "$mode" = "zero" ] && [ "$n" -eq 0 ]; } || { [ "$mode" = "positive" ] && [ "$n" -gt 0 ]; }; then
    echo "PASS  $desc ($n)"
  else
    echo "FAIL  $desc ($n)"; FAILED=1
  fi
}

[ -f "$DB" ] || { echo "FAIL  database $DB not found"; exit 1; }

check "table has rows"                       "SELECT COUNT(*) FROM web_logs"                                       positive
check "no NULLs in key columns"              "SELECT COUNT(*) FROM web_logs WHERE ts IS NULL OR ip IS NULL OR path IS NULL OR status IS NULL" zero
check "status codes within 100-599"          "SELECT COUNT(*) FROM web_logs WHERE status < 100 OR status > 599"    zero
check "no negative response times"           "SELECT COUNT(*) FROM web_logs WHERE response_ms < 0"                 zero
check "no duplicate event IDs"               "SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM web_logs"            zero
check "HTTP methods are valid"               "SELECT COUNT(*) FROM web_logs WHERE method NOT IN ('GET','POST','PUT','DELETE','PATCH')" zero

if [ "$FAILED" -eq 0 ]; then echo "All checks passed."; else echo "Some checks FAILED."; fi
exit $FAILED
