#!/bin/bash

# Repeatedly runs a REST test command to surface flakiness. Each Gradle invocation
# overwrites the JUnit XML in place, so after every iteration we relocate
# the freshly written TEST-*.xml into a per-iteration subtree.
# This lets the flakiness analyzer see every re-run instead of only the last.
# The preserved paths keep the `build/test-results/` segment so they still match
# the analyzer's XML walker and the pipeline artifact glob (`**/build/test-results/**/TEST-*.xml`).

ITERS=$1; shift
SNAPSHOT_ROOT="flakiness-iters"
# Only reports written after the loop starts are ours to relocate. Without this,
# a `find` from the repo root (the cwd for local runs) would also sweep unrelated
# build/test-results reports already present in a developer's checkout. CI runs on
# a fresh checkout, so nothing pre-exists there.
MARKER="$(mktemp)"
PASS=0; FAIL=0
for i in $(seq 1 "$ITERS"); do
  echo "--- Iteration $i/$ITERS"
  if "$@"; then
    PASS=$((PASS + 1))
  else
    FAIL=$((FAIL + 1))
  fi
  # Relocate this iteration's JUnit XML so the next iteration cannot overwrite it.
  # `-newer` limits the move to reports written during this run; the snapshot root
  # (already-preserved reports) and heavy VCS/build-tool dirs are pruned for speed.
  # `mv` preserves mtime, keeping the local analyzer's mtime filter correct.
  while IFS= read -r -d '' xml; do
    dest="$SNAPSHOT_ROOT/iter-$i/${xml#./}"
    mkdir -p "$(dirname "$dest")"
    mv "$xml" "$dest"
  done < <(find . \( -name .git -o -name .gradle -o -name node_modules -o -name "$SNAPSHOT_ROOT" \) -prune -o \
                  -newer "$MARKER" -path '*/build/test-results/*' -name 'TEST-*.xml' -type f -print0)
done
rm -f "$MARKER"
echo "Results: $PASS/$((PASS + FAIL)) passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
