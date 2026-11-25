#!/bin/bash
# ---------------------------------------------------------------------------
# Local end-to-end test for the smart-retry and preemption infrastructure.
#
# Runs a Gradle build, simulates a retry cycle using the TypeScript pipeline,
# and prints a summary — all without Buildkite.
#
# Usage:
#   ./test-local-flow.sh <gradle-command> [options]
#
# Examples:
#   # Simple retry of a failing test suite:
#   ./test-local-flow.sh "./gradlew :server:test --tests 'org.elasticsearch.common.StringsTests'"
#
#   # Simulate preemption after 5 seconds:
#   ./test-local-flow.sh --preempt 5 "./gradlew :server:test"
#
#   # Skip run 1 and reuse an existing task-status.json:
#   ./test-local-flow.sh --skip-run1 --status build/task-status.json "./gradlew :server:test"
#
# Options:
#   --preempt <seconds>   Simulate GCP preemption after N seconds
#   --skip-run1           Skip the first build; reuse existing task-status.json
#   --status <path>       Path to existing task-status.json (implies --skip-run1)
#   --skip-run2           Only do run 1 + transform, don't execute the retry
#   --seed <seed>         Test seed to pass through to .failed-test-history.json
#   --help                Show this help
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
NODE_FLAGS="--experimental-strip-types --no-warnings=ExperimentalWarning"

PREEMPT_SECONDS=""
SKIP_RUN1=false
SKIP_RUN2=false
STATUS_FILE=""
TEST_SEED=""
GRADLE_CMD=""

# --- Parse arguments ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --preempt)
      PREEMPT_SECONDS="$2"
      shift 2
      ;;
    --skip-run1)
      SKIP_RUN1=true
      shift
      ;;
    --status)
      STATUS_FILE="$2"
      SKIP_RUN1=true
      shift 2
      ;;
    --skip-run2)
      SKIP_RUN2=true
      shift
      ;;
    --seed)
      TEST_SEED="$2"
      shift 2
      ;;
    --help)
      head -30 "$0" | grep '^#' | sed 's/^# \?//'
      exit 0
      ;;
    *)
      GRADLE_CMD="$1"
      shift
      ;;
  esac
done

if [[ -z "$GRADLE_CMD" ]]; then
  echo "Error: no Gradle command provided."
  echo "Usage: $0 [options] <gradle-command>"
  echo "Run with --help for details."
  exit 1
fi

TASK_STATUS="$REPO_ROOT/build/task-status.json"
MULTI_RUN="$REPO_ROOT/build/task-status-multi.json"
HISTORY_FILE="$REPO_ROOT/.failed-test-history.json"

# --- Cleanup from previous runs ---
cleanup() {
  rm -f "$MULTI_RUN"
  rm -f "$HISTORY_FILE"
}

echo ""
echo "========================================================================"
echo "  Smart Retry — Local Test Flow"
echo "========================================================================"
echo "  Gradle command:  $GRADLE_CMD"
[[ -n "$PREEMPT_SECONDS" ]] && echo "  Preemption:      after ${PREEMPT_SECONDS}s"
echo "  Repo root:       $REPO_ROOT"
echo "========================================================================"
echo ""

# ============================== RUN 1 ======================================

if [[ "$SKIP_RUN1" == true ]]; then
  if [[ -n "$STATUS_FILE" ]]; then
    TASK_STATUS="$STATUS_FILE"
  fi
  echo "--- Skipping Run 1, using existing task-status: $TASK_STATUS ---"
  if [[ ! -f "$TASK_STATUS" ]]; then
    echo "Error: $TASK_STATUS does not exist"
    exit 1
  fi
else
  cleanup

  echo "--- Run 1: Initial build ---"
  echo ""

  EXTRA_ENV=""
  if [[ -n "$PREEMPT_SECONDS" ]]; then
    EXTRA_ENV="GCP_PREEMPTION_WATCHDOG=true GCP_PREEMPTION_SIMULATE_AFTER_SECONDS=$PREEMPT_SECONDS"
    echo "(preemption will fire after ${PREEMPT_SECONDS}s)"
  fi

  set +e
  if [[ -n "$EXTRA_ENV" ]]; then
    env $EXTRA_ENV bash -c "cd '$REPO_ROOT' && $GRADLE_CMD" 2>&1
  else
    bash -c "cd '$REPO_ROOT' && $GRADLE_CMD" 2>&1
  fi
  RUN1_EXIT=$?
  set -e

  echo ""
  echo "--- Run 1 exited with code $RUN1_EXIT ---"

  if [[ ! -f "$TASK_STATUS" ]]; then
    echo "Error: Run 1 did not produce $TASK_STATUS"
    exit 1
  fi
fi

echo ""

# ============================== MERGE ======================================

echo "--- Merging task-status into multi-run format ---"
node $NODE_FLAGS "$SCRIPT_DIR/finalize-task-status.ts" "$TASK_STATUS" "$MULTI_RUN" "$MULTI_RUN"
echo "  Wrote $MULTI_RUN"
echo ""

# ============================== SUMMARY ====================================

echo "--- Run 1 Summary ---"
echo ""
node $NODE_FLAGS "$SCRIPT_DIR/summarize-task-status.ts" "$MULTI_RUN"
echo ""

# ============================== TRANSFORM ==================================

echo "--- Transforming to .failed-test-history.json ---"
node $NODE_FLAGS "$SCRIPT_DIR/transform-task-status.ts" "$MULTI_RUN" "$HISTORY_FILE" "$TEST_SEED"
echo ""

if [[ "$SKIP_RUN2" == true ]]; then
  echo "--- Skipping Run 2 (--skip-run2) ---"
  echo ""
  echo "Files produced:"
  echo "  Multi-run status:  $MULTI_RUN"
  echo "  Test history:      $HISTORY_FILE"
  exit 0
fi

# ============================== RUN 2 ======================================

echo "--- Run 2: Retry build (smart-retry active) ---"
echo ""

set +e
if [[ -n "$EXTRA_ENV" ]]; then
  env $EXTRA_ENV bash -c "cd '$REPO_ROOT' && $GRADLE_CMD" 2>&1
else
  bash -c "cd '$REPO_ROOT' && $GRADLE_CMD" 2>&1
fi
RUN2_EXIT=$?
set -e

echo ""
echo "--- Run 2 exited with code $RUN2_EXIT ---"
echo ""

# Merge run 2 into the multi-run artifact
if [[ -f "$TASK_STATUS" ]]; then
  echo "--- Merging Run 2 task-status ---"
  node $NODE_FLAGS "$SCRIPT_DIR/finalize-task-status.ts" "$TASK_STATUS" "$MULTI_RUN" "$MULTI_RUN"
  echo ""

  echo "--- Final Summary (both runs) ---"
  echo ""
  node $NODE_FLAGS "$SCRIPT_DIR/summarize-task-status.ts" "$MULTI_RUN"
fi

echo ""
echo "Files produced:"
echo "  Multi-run status:  $MULTI_RUN"
echo "  Test history:      $HISTORY_FILE"
echo ""

# Cleanup the history file so it doesn't affect future normal builds
rm -f "$HISTORY_FILE"
echo "Cleaned up $HISTORY_FILE"
