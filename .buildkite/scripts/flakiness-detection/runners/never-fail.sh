#!/bin/bash

# Runs one flakiness batch command so that the Buildkite step ALWAYS exits 0, annotating any failure
# instead of propagating it. Buildkite's GitHub commit-status integration mirrors step.state and ignores
# the soft_failed flag, so a soft_fail step that fails still shows a red check on the PR; exiting 0 and
# annotating is the only way to report failures without turning the PR red.
#
# The command is not an argument. It arrives in FLAKINESS_CMD_<n>, indexed by BUILDKITE_PARALLEL_JOB.
# `parallelism: N` makes N jobs from ONE step definition: all N run the identical command string and
# differ only by that variable, so per-batch values have nowhere to live but env vars.
#
# Usage:
#   never-fail.sh --context <step-key> --inner-timeout-minutes <m> [--kind <test-kind>]
#
# Passing --kind enables per-job outcome recording. The analyze step omits it: it wants the never-fail
# behaviour but must not emit a batch outcome of its own.
#
# Deliberately no `set -e`: the whole point is to observe a failing command's exit code rather than die
# with it.

CONTEXT=""
INNER_TIMEOUT_MINUTES=""
KIND=""
WRAPPED_CMD_FILE=""
rc=0

parse_arguments() {
  while [ $# -gt 0 ]; do
    case "$1" in
      --context) CONTEXT="$2"; shift 2 ;;
      --inner-timeout-minutes) INNER_TIMEOUT_MINUTES="$2"; shift 2 ;;
      --kind) KIND="$2"; shift 2 ;;
      *) echo "never-fail.sh: unknown argument '$1'" >&2; exit 2 ;;
    esac
  done
}

# Fail loudly for a malformed direct invocation. Once the wrapped command starts, the never-fail contract
# takes over and nothing below here may exit non-zero.
validate_arguments() {
  [ -n "$CONTEXT" ] || { echo "never-fail.sh: --context is required" >&2; exit 2; }
  [ -n "$INNER_TIMEOUT_MINUTES" ] || { echo "never-fail.sh: --inner-timeout-minutes is required" >&2; exit 2; }
  if [ -n "$KIND" ]; then
    for var in FLAKINESS_STATUS_DIR FLAKINESS_JOB_STATUS_PREFIX FLAKINESS_TASK_STATUS_PREFIX FLAKINESS_TASK_STATUS_FILE; do
      [ -n "${!var}" ] || { echo "never-fail.sh: $var must be set when --kind is given" >&2; exit 2; }
    done
  fi
}

read_wrapped_command() {
  local batch cmd_var
  batch="${BUILDKITE_PARALLEL_JOB:-0}"
  cmd_var="FLAKINESS_CMD_$batch"
  WRAPPED_CMD_FILE=$(mktemp)
  trap 'rm -f "$WRAPPED_CMD_FILE"' EXIT
  printf '%s\n' "${!cmd_var}" > "$WRAPPED_CMD_FILE"
}

# --foreground stops `timeout` putting the command in a fresh process group. Without it the gradle CLI
# prints BUILD SUCCESSFUL and then never exits: the develocity scan plugin's shutdown path stalls until
# the inner timeout fires, ~36min per step. Diagnosed in #150209 by a four-variant matrix, which also
# ruled out the gradle daemon (--no-daemon did not help).
run_wrapped_command() {
  timeout --foreground --signal=TERM --kill-after=30s "${INNER_TIMEOUT_MINUTES}m" bash "$WRAPPED_CMD_FILE"
  rc=$?
}

# 124 is timeout's own SIGTERM exit, 137 the SIGKILL after --kill-after elapsed.
annotate_failure() {
  local msg=""
  if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ]; then
    msg="timed out after ${INNER_TIMEOUT_MINUTES}m (rc=$rc)"
  elif [ "$rc" -ne 0 ]; then
    msg="exited with $rc"
  else
    return
  fi
  buildkite-agent annotate --style warning --context "$CONTEXT-failures" \
    --append "[$BUILDKITE_LABEL] (job $BUILDKITE_JOB_ID) $msg - see job log"
}

# Every ES test JVM runs with -XX:+HeapDumpOnOutOfMemoryError and a heapdump path under buildDir
# (ElasticsearchTestBasePlugin), so a leftover .hprof means a JVM-heap OutOfMemoryError, which exits rc=1
# via Gradle rather than the rc=137 SIGKILL a kernel OOM-kill gives. Detected from the file rather than
# the log so we never touch the wrapped command's stdout. analyze.ts turns it into the `oom` infraSubtype.
detect_oom() {
  if [ -n "$(find . -type f -path '*/build/heapdump/*.hprof' -print -quit 2>/dev/null)" ]; then
    echo "oom"
  fi
}

# A task rejected by `onlyIf` (bwc's bwc_tests_enabled, the distro arch check) or with no source is
# reported SKIPPED: zero tests, exit 0, indistinguishable from a hang by rc alone. gradle-runner records
# every task's outcome here, and analyze.ts reads the verdict for this batch's own task paths. Only
# COPIED, never parsed, so this script stays uncoupled from the report's exact spacing.
copy_task_status() {
  cp "$FLAKINESS_TASK_STATUS_FILE" \
    "$FLAKINESS_STATUS_DIR/$FLAKINESS_TASK_STATUS_PREFIX$BUILDKITE_JOB_ID.json" 2>/dev/null || true
}

# Best-effort by design: `|| true` here and `exit 0` in main mean observability can never fail a batch.
write_outcome() {
  local batch tp_var duration oom
  batch="${BUILDKITE_PARALLEL_JOB:-0}"
  tp_var="FLAKINESS_TASK_PATHS_$batch"
  duration=$(( _fd_end - _fd_start ))
  oom=$(detect_oom)
  mkdir -p "$FLAKINESS_STATUS_DIR"
  copy_task_status
  printf '{"jobId":"%s","stepKey":"%s","kind":"%s","rc":%s,"durationSec":%s,"infraSubtype":"%s","taskPaths":%s}' \
    "$BUILDKITE_JOB_ID" "$CONTEXT" "$KIND" "$rc" "$duration" "$oom" "${!tp_var:-[]}" \
    > "$FLAKINESS_STATUS_DIR/$FLAKINESS_JOB_STATUS_PREFIX$BUILDKITE_JOB_ID.json" || true
}

parse_arguments "$@"
validate_arguments
read_wrapped_command

# Wall clock is captured around the run only when it will be reported, matching the previous behaviour.
if [ -n "$KIND" ]; then
  _fd_start=$(date +%s)
  run_wrapped_command
  _fd_end=$(date +%s)
else
  run_wrapped_command
fi

annotate_failure

if [ -n "$KIND" ]; then
  write_outcome
fi

exit 0
