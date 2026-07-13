#!/bin/bash

ITERS=$1; shift
PASS=0; FAIL=0
for i in $(seq 1 "$ITERS"); do
  echo "--- Iteration $i/$ITERS"
  if "$@"; then
    PASS=$((PASS + 1))
  else
    FAIL=$((FAIL + 1))
  fi
done
echo "Results: $PASS/$((PASS + FAIL)) passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
