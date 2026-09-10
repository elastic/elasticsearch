#!/bin/bash

# The flakiness orchestration step: resolve -> compile -> scan, sequentially on ONE gradle agent, because
# scan reads the build/classes output compile produced and separate Buildkite steps share no workspace.
#
# Which phase failed decides how the run is reported, so the exit codes are not interchangeable:
#   * compile non-zero is the SOLE build_failed signal. It writes the buildFailed plan and the precompile
#     marker, then exits rc. The separate generate step depends on this one with allow_failure and turns
#     those markers into the single build_failed record.
#   * resolve or scan non-zero is a resolver/infra defect, NOT build_failed: no marker, exit rc.
#
# Configuration arrives in the pipeline-level env block so that domain.ts stays the one place these names
# and task lists are written:
#   FLAKINESS_REFS_ARTIFACT, FLAKINESS_PLAN_ARTIFACT, FLAKINESS_PRECOMPILE_ARTIFACT,
#   FLAKINESS_TARGETS_DIR, FLAKINESS_TARGETS_ARCHIVE, FLAKINESS_COMPILE_TASKS,
#   FLAKINESS_RESOLVE_INNER_TIMEOUT, FLAKINESS_COMPILE_INNER_TIMEOUT, FLAKINESS_SCAN_INNER_TIMEOUT
#
# The inner timeouts fire a grace period before Buildkite's outer timeout_in_minutes so this script gets to
# classify the failure rather than being SIGKILLed by the agent. --foreground keeps each gradle CLI in this
# script's process group; without it `timeout` setpgid()s its child, the CLI loses the controlling-TTY
# plumbing the develocity scan plugin relies on, and the JVM hangs ~36min after BUILD SUCCESSFUL.
# Diagnosed on build #2 of elasticsearch-flakiness-detection-manual.

for var in FLAKINESS_REFS_ARTIFACT FLAKINESS_PLAN_ARTIFACT FLAKINESS_PRECOMPILE_ARTIFACT \
           FLAKINESS_TARGETS_DIR FLAKINESS_TARGETS_ARCHIVE FLAKINESS_COMPILE_TASKS \
           FLAKINESS_RESOLVE_INNER_TIMEOUT FLAKINESS_COMPILE_INNER_TIMEOUT FLAKINESS_SCAN_INNER_TIMEOUT; do
  [ -n "${!var}" ] || { echo "orchestrate.sh: $var must be set" >&2; exit 2; }
done

# Deliberately unquoted where it is used below: this must word-split into its four arguments. Quoting it
# would pass the whole string as a single argv element and `timeout` would not recognise it.
TIMEOUT="timeout --foreground --signal=TERM --kill-after=30s"

# refs are produced by the bootstrap step on a different agent, so fetch them onto this one.
buildkite-agent artifact download "$FLAKINESS_REFS_ARTIFACT" . || true
set +e

# --- resolve ---
# UNQUALIFIED on purpose: every project that registered the task runs it and self-selects on whether a ref
# lands in its own source sets. The configuration cache stays ON - each project's model reaches the task as
# an @Input, which survives the configuration/execution boundary.
$TIMEOUT "${FLAKINESS_RESOLVE_INNER_TIMEOUT}m" .ci/scripts/run-gradle.sh -Pflakiness.resolve flakinessResolveProject
rc=$?
if [ "$rc" -ne 0 ]; then
  echo "flakiness resolve failed (rc=$rc): resolver/infra defect, not a PR build failure."
  exit $rc
fi

# One tarball, not a *.json glob: every project writes a file whether or not it owns a ref, so a glob would
# mean ~450 uploads per build of pure debugging detail.
tar -czf "$FLAKINESS_TARGETS_ARCHIVE" -C "$FLAKINESS_TARGETS_DIR" . 2>/dev/null || true

# --- compile (every test source set in the repo) ---
# UNQUALIFIED, so the whole repo compiles rather than only the projects that owned a ref - that is what lets
# scan resolve an abstract base against subclasses in other projects.
#
# The guard is the second of two gates (pr.ts is the first, and coarser), and it exists because a PR
# touching only src/main/java still produces refs that resolve to nothing runnable. "refIndex" is the
# marker: it appears in a per-project file exactly when that project resolved a target, which keeps this a
# single-token grep instead of parsing JSON in shell.
if grep -qs '"refIndex"' "$FLAKINESS_TARGETS_DIR"/*.json; then
  $TIMEOUT "${FLAKINESS_COMPILE_INNER_TIMEOUT}m" .ci/scripts/run-gradle.sh $FLAKINESS_COMPILE_TASKS
  rc=$?
  if [ "$rc" -ne 0 ]; then
    printf '{"buildFailed":true,"reason":"precompile","entries":[]}' > "$FLAKINESS_PLAN_ARTIFACT"
    printf '{"outcome":"build_failed","reason":"precompile"}' > "$FLAKINESS_PRECOMPILE_ARTIFACT"
    exit $rc
  fi
else
  echo "resolve produced no runnable targets; skipping the repo-wide test compile."
fi

# --- scan ---
# Runs even when compile was skipped: scan is what reports refs no project could claim (a muted-tests entry
# naming a deleted class). Worth its ~9s - it is the only signal for those refs.
$TIMEOUT "${FLAKINESS_SCAN_INNER_TIMEOUT}m" .ci/scripts/run-gradle.sh -Pflakiness.resolve flakinessScan
rc=$?
if [ "$rc" -ne 0 ]; then
  echo "flakiness scan failed (rc=$rc): resolver/infra defect, not a PR build failure."
  exit $rc
fi

# --- happy path: scan wrote the plan; the separate generate step uploads batch + analyze ---
exit 0
