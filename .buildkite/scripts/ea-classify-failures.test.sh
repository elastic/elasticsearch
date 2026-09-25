#!/bin/bash
#
# Fixture test for ea-classify-failures.sh. Stubs the two data-fetching
# functions (fetch_ea_failures, count_baseline_failures) so the parsing and
# classification logic can be exercised without hitting ci-stats.
#
# Run from anywhere:
#   .buildkite/scripts/ea-classify-failures.test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=ea-classify-failures.sh
source "$SCRIPT_DIR/ea-classify-failures.sh"

failures=0
expect() {
  local label="$1" haystack="$2" needle="$3"
  if grep -qF -- "$needle" <<< "$haystack"; then
    echo "  ok: $label"
  else
    echo "  FAIL: $label" >&2
    echo "    expected to find: $needle" >&2
    failures=$((failures + 1))
  fi
}

# --- parse_test_pairs ---
echo "test: parse_test_pairs extracts unique class/name pairs"
fixture=$(cat <<'EOF'
Failures: 3 total, showing 3 most recent

[2026-05-08 09:19:14] branch=main  tags=[CI, elasticsearch-periodic-java-ea, main]
  Class:  com.example.AlphaTests
  Test:   testInherited
  Task:   :foo:test

[2026-05-08 09:19:13] branch=main  tags=[CI, elasticsearch-periodic-java-ea, main]
  Class:  com.example.AlphaTests
  Test:   testInherited
  Task:   :foo:test

[2026-05-08 09:19:12] branch=main  tags=[CI, elasticsearch-periodic-java-ea, main]
  Class:  com.example.BetaTests
  Test:   testEaSpecific
  Task:   :bar:test
EOF
)
parsed=$(printf '%s\n' "$fixture" | parse_test_pairs)
expect "AlphaTests pair present" "$parsed" $'com.example.AlphaTests\ttestInherited'
expect "BetaTests pair present"  "$parsed" $'com.example.BetaTests\ttestEaSpecific'
got_count=$(printf '%s\n' "$parsed" | wc -l | tr -d ' ')
if [[ "$got_count" != "2" ]]; then
  echo "  FAIL: expected 2 unique pairs after dedup, got $got_count" >&2
  failures=$((failures + 1))
else
  echo "  ok: dedups to 2 unique pairs"
fi

# --- main: classification end-to-end with stubbed I/O ---
echo "test: main classifies into candidates vs inherited buckets"

# These stubs are invoked indirectly by main; suppress "never invoked" warnings.
# shellcheck disable=SC2317,SC2329
fetch_ea_failures() {
  cat <<'EOF'
Failures: 2 total, showing 2 most recent

[2026-05-08 09:19:14] branch=main  tags=[CI, elasticsearch-periodic-java-ea, main]
  Class:  com.example.AlphaTests
  Test:   testInherited
  Task:   :foo:test

[2026-05-08 09:19:13] branch=main  tags=[CI, elasticsearch-periodic-java-ea, main]
  Class:  com.example.BetaTests
  Test:   testEaSpecific
  Task:   :bar:test
EOF
}

# AlphaTests has non-EA history (=> inherited); BetaTests has none (=> candidate).
# shellcheck disable=SC2317,SC2329
count_baseline_failures() {
  local class="$1"
  case "$class" in
    *AlphaTests) echo 5 ;;
    *BetaTests)  echo 0 ;;
    *)           echo 0 ;;
  esac
}

# Unset BUILDKITE_BUILD_ID so main prints the body to stdout instead of trying
# to call buildkite-agent annotate.
unset BUILDKITE_BUILD_ID
output=$(main 2>/dev/null)

expect "shows 1 candidate count"           "$output" "EA-specific candidates (1)"
expect "shows 1 inherited count"           "$output" "Inherited from main (1)"
expect "BetaTests listed as candidate"     "$output" "BetaTests.testEaSpecific\` — 0 non-EA fails"
expect "AlphaTests listed as inherited"    "$output" "AlphaTests.testInherited\` — 5 non-EA fails"
expect "candidates section before details" "$output" "### EA-specific candidates"

# --- main: empty fetch yields no report ---
echo "test: main exits cleanly when there are no EA failures"
# shellcheck disable=SC2317,SC2329
fetch_ea_failures() { echo "Failures: 0 total, showing 0 most recent"; }
output=$(main 2>/dev/null)
if [[ -n "$output" ]]; then
  echo "  FAIL: expected no stdout when there are no EA failures, got:" >&2
  echo "$output" >&2
  failures=$((failures + 1))
else
  echo "  ok: empty stdout when no EA failures"
fi

echo
if [[ "$failures" -gt 0 ]]; then
  echo "$failures assertion(s) failed" >&2
  exit 1
fi
echo "all tests passed"
