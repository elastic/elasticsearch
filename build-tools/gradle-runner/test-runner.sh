#!/bin/bash
#
# End-to-end test for the GradleRunner.
#
# Exercises two scenarios:
#   1. Normal build (no preemption) — verifies task-status.json is written and
#      preemption artifacts are absent.
#   2. Preemption build — verifies task-status.json, .preemption-marker.json,
#      and /tmp/gradle-preemption-exit-local are all written with correct content,
#      and the runner exits with the preemption exit code.
#
# Usage:
#   ./build-tools/gradle-runner/test-runner.sh [--project-dir <dir>]
#
# The script runs from the repository root by default. Pass --project-dir to
# override (the directory must contain a Gradle wrapper).

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

PROJECT_DIR="."
while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-dir) PROJECT_DIR="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

PROJECT_DIR="$(cd "$PROJECT_DIR" && pwd)"
RUNNER_JAR="build-tools/gradle-runner/build/libs/gradle-runner.jar"
TASK_STATUS="$PROJECT_DIR/build/task-status.json"
PREEMPTION_MARKER="$PROJECT_DIR/build/.preemption-marker.json"
PREEMPTION_EXIT_FILE="/tmp/gradle-preemption-exit-local"

PASS=0
FAIL=0

pass() {
  echo -e "  ${GREEN}✓${NC} $1"
  PASS=$((PASS + 1))
}

fail() {
  echo -e "  ${RED}✗${NC} $1"
  FAIL=$((FAIL + 1))
}

assert_file_exists() {
  if [[ -f "$1" ]]; then
    pass "$2"
  else
    fail "$2 (file not found: $1)"
  fi
}

assert_file_not_exists() {
  if [[ ! -f "$1" ]]; then
    pass "$2"
  else
    fail "$2 (file unexpectedly exists: $1)"
  fi
}

assert_file_contains() {
  if grep -q "$2" "$1" 2>/dev/null; then
    pass "$3"
  else
    fail "$3 (pattern '$2' not found in $1)"
  fi
}

assert_file_not_contains() {
  if ! grep -q "$2" "$1" 2>/dev/null; then
    pass "$3"
  else
    fail "$3 (pattern '$2' unexpectedly found in $1)"
  fi
}

cleanup() {
  echo "Cleaning up output files..."
  rm -f "$TASK_STATUS" "$PREEMPTION_MARKER" "$PREEMPTION_EXIT_FILE"
}

# ===========================================================================
echo -e "${YELLOW}=== GradleRunner End-to-End Test ===${NC}"
echo ""

# --- Setup -----------------------------------------------------------------
echo -e "${YELLOW}--- Setup${NC}"

echo "Stopping Gradle daemons..."
"$PROJECT_DIR/gradlew" --stop 2>&1 | sed 's/^/  /'

cleanup

echo "Building gradle-runner.jar..."
"$PROJECT_DIR/gradlew" --no-daemon :build-tools:gradle-runner:jar 2>&1 | tail -5 | sed 's/^/  /'

if [[ ! -f "$PROJECT_DIR/$RUNNER_JAR" ]]; then
  echo -e "${RED}FATAL: gradle-runner.jar not found after build${NC}"
  exit 1
fi
echo ""

# ===========================================================================
# Scenario 1: Normal build (no preemption)
# ===========================================================================
echo -e "${YELLOW}--- Scenario 1: Normal build (no preemption)${NC}"

cleanup
"$PROJECT_DIR/gradlew" --stop >/dev/null 2>&1

java -jar "$PROJECT_DIR/$RUNNER_JAR" \
  --project-dir "$PROJECT_DIR" \
  -- help \
  2>&1 | sed 's/^/  /'

EXIT_CODE=$?

echo ""
echo "Assertions:"

# Exit code should be 0
if [[ "$EXIT_CODE" -eq 0 ]]; then
  pass "Exit code is 0"
else
  fail "Exit code is $EXIT_CODE (expected 0)"
fi

# task-status.json should exist and have correct structure
assert_file_exists "$TASK_STATUS" "task-status.json exists"
assert_file_contains "$TASK_STATUS" '"tasks"' "task-status.json contains tasks array"
assert_file_contains "$TASK_STATUS" '"tests"' "task-status.json contains tests array"
assert_file_contains "$TASK_STATUS" '"cancelled" : false' "task-status.json shows cancelled=false"
assert_file_contains "$TASK_STATUS" '"preemptedAt" : null' "task-status.json shows preemptedAt=null"
assert_file_contains "$TASK_STATUS" '"outcome"' "task-status.json contains task outcomes"

# Preemption artifacts should NOT exist
assert_file_not_exists "$PREEMPTION_MARKER" "No preemption marker file"
assert_file_not_exists "$PREEMPTION_EXIT_FILE" "No preemption exit file"

echo ""

# ===========================================================================
# Scenario 2: Preemption build
# ===========================================================================
echo -e "${YELLOW}--- Scenario 2: Preemption build (simulated after 5s)${NC}"

cleanup
"$PROJECT_DIR/gradlew" --stop >/dev/null 2>&1

# Run with preemption simulation; the runner should exit with code 47.
# Use `set +e` so the non-zero exit doesn't kill the script.
set +e
GCP_PREEMPTION_WATCHDOG=true \
GCP_PREEMPTION_SIMULATE_AFTER_SECONDS=5 \
  java -jar "$PROJECT_DIR/$RUNNER_JAR" \
    --project-dir "$PROJECT_DIR" \
    -- help \
    2>&1 | sed 's/^/  /'

EXIT_CODE=$?
set -e

echo ""
echo "Assertions:"

# Exit code should be 47
if [[ "$EXIT_CODE" -eq 47 ]]; then
  pass "Exit code is 47 (preemption)"
else
  fail "Exit code is $EXIT_CODE (expected 47)"
fi

# --- task-status.json ---
assert_file_exists "$TASK_STATUS" "task-status.json exists"
assert_file_contains "$TASK_STATUS" '"cancelled" : true' "task-status.json shows cancelled=true"
assert_file_contains "$TASK_STATUS" '"preemptedAt" : "' "task-status.json has preemptedAt timestamp"
assert_file_contains "$TASK_STATUS" '"tasks"' "task-status.json contains tasks array"
assert_file_contains "$TASK_STATUS" '"tests"' "task-status.json contains tests array"

# Tasks should have INTERRUPTED or normal outcomes, not raw FAILED (preemption failures are INTERRUPTED)
assert_file_not_contains "$TASK_STATUS" '"outcome" : "FAILED"' "No tasks show FAILED (preemption uses INTERRUPTED)"

# --- .preemption-marker.json ---
assert_file_exists "$PREEMPTION_MARKER" "Preemption marker file exists"
assert_file_contains "$PREEMPTION_MARKER" '"preempted": true' "Marker file has preempted=true"
assert_file_contains "$PREEMPTION_MARKER" '"preemptedAt":' "Marker file has preemptedAt timestamp"

# --- /tmp/gradle-preemption-exit-local ---
assert_file_exists "$PREEMPTION_EXIT_FILE" "Preemption exit file exists"
PREEMPTION_EXIT_CONTENT=$(cat "$PREEMPTION_EXIT_FILE")
if [[ "$PREEMPTION_EXIT_CONTENT" == "47" ]]; then
  pass "Preemption exit file contains 47"
else
  fail "Preemption exit file contains '$PREEMPTION_EXIT_CONTENT' (expected '47')"
fi

echo ""

# ===========================================================================
# Summary
# ===========================================================================
TOTAL=$((PASS + FAIL))
echo -e "${YELLOW}=== Results: ${PASS}/${TOTAL} passed ===${NC}"
if [[ "$FAIL" -gt 0 ]]; then
  echo -e "${RED}${FAIL} assertion(s) failed${NC}"
  exit 1
else
  echo -e "${GREEN}All assertions passed${NC}"
  exit 0
fi
