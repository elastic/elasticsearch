#!/bin/bash
# Print headline coverage per layer and merged, from the CSV reports.
set -euo pipefail
shopt -s nullglob
REPORT="$1"
printf '%-18s %-22s %s\n' "LAYER" "LINE" "BRANCH"
for d in "$REPORT"/*/; do
  name=$(basename "$d")
  csv="$d/coverage.csv"
  [[ -f "$csv" ]] || continue
  python3 - "$name" "$csv" <<'PY'
import csv, sys
name, path = sys.argv[1], sys.argv[2]
rows = list(csv.DictReader(open(path)))
lc = sum(int(r['LINE_COVERED']) for r in rows); lm = sum(int(r['LINE_MISSED']) for r in rows)
bc = sum(int(r['BRANCH_COVERED']) for r in rows); bm = sum(int(r['BRANCH_MISSED']) for r in rows)
lt, bt = lc+lm, bc+bm
print(f"{name:<18} {lc:>6}/{lt:<7} {100*lc/max(lt,1):5.1f}%   {bc:>6}/{bm+bc:<7} {100*bc/max(bt,1):5.1f}%")
PY
done
