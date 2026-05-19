#!/bin/bash

# =============================================================================
# Smart retry test filtering
# =============================================================================
# When smart retries are enabled, download the task-status.json.gz artifact from
# the original job and use it to filter this retry to only run tests/tasks that
# did not complete successfully.
#
# Expected environment (set by pre-command before sourcing):
#   BUILD_JSON       – Buildkite build JSON (may be empty)
#   ORIGIN_JOB_ID    – retried job's source ID (may be empty/null)
#   TESTS_SEED       – test seed from the original job (may be empty)
#   BUILDKITE_*      – standard Buildkite env vars
#   BUILDKITE_API_TOKEN – API token for Buildkite
#
# Outputs (available to caller after sourcing):
#   .failed-test-history.json – written to $PWD when smart retry succeeds
#   SMART_RETRY_STATUS        – enabled | disabled | failed
#   SMART_RETRY_DETAILS       – human-readable status detail
#   FILTERED_WORK_UNITS       – number of failed work units (if enabled)
#   EXECUTED_TASKS_COUNT      – number of executed test tasks (if available)
#   Buildkite metadata keys:  smart-retry-status, smart-retry-details,
#                              smart-retry-work-units,
#                              smart-retry-executed-tasks
# =============================================================================

echo "--- [Smart Retry] Resolving previously failed tests"
SMART_RETRY_STATUS="disabled"
SMART_RETRY_DETAILS=""

if [[ -z "$BUILD_JSON" ]]; then
  BUILD_JSON=$(curl --retry 3 --retry-delay 2 --retry-max-time 60 --retry-connrefused --connect-timeout 10 --max-time 30 -H "Authorization: Bearer $BUILDKITE_API_TOKEN" -X GET "https://api.buildkite.com/v2/organizations/elastic/pipelines/${BUILDKITE_PIPELINE_SLUG}/builds/${BUILDKITE_BUILD_NUMBER}?include_retried_jobs=true" 2>/dev/null) || BUILD_JSON=""
fi

if [[ -n "$BUILD_JSON" ]]; then
  if [[ -z "$ORIGIN_JOB_ID" ]] || [[ "$ORIGIN_JOB_ID" == "null" ]]; then
    ORIGIN_JOB_ID=$(printf '%s\n' "$BUILD_JSON" | jq -r --arg jobId "$BUILDKITE_JOB_ID" ' .jobs[] | select(.id == $jobId) | .retry_source.job_id' 2>/dev/null) || ORIGIN_JOB_ID=""
  fi

  if [[ -n "$ORIGIN_JOB_ID" ]] && [[ "$ORIGIN_JOB_ID" != "null" ]]; then
    TASK_STATUS_FILE=$(mktemp .task-status.XXXXXX)
    trap "rm -f '$TASK_STATUS_FILE'" EXIT

    if buildkite-agent artifact download task-status.json.gz . --step "$ORIGIN_JOB_ID" 2>/dev/null \
       && [[ -f task-status.json.gz ]] \
       && gunzip -f task-status.json.gz \
       && [[ -f task-status.json ]]; then
      mv task-status.json "$TASK_STATUS_FILE"
    else
      rm -f task-status.json.gz task-status.json
      echo "[Smart Retry] Failed to download task-status.json.gz from origin job $ORIGIN_JOB_ID"
      echo "[Smart Retry] will be disabled - all tests will run."
      SMART_RETRY_STATUS="failed"
      SMART_RETRY_DETAILS="Failed to download task-status artifact"
    fi

    if [[ "$SMART_RETRY_STATUS" != "failed" ]] && [[ -s "$TASK_STATUS_FILE" ]]; then
      # Transform task-status.json into .failed-test-history.json
      #
      # task-status.json has:
      #   tasks[]: {path, outcome} where outcome is SUCCESS|UP_TO_DATE|FROM_CACHE|FAILED|SKIPPED|INTERRUPTED|NOT_RUN
      #   tests[]: {taskPath, className, methodName, result} where result is SUCCESS|FAILURE|SKIPPED
      #   cancelled: boolean
      #
      # .failed-test-history.json needs:
      #   workUnits[]: tasks with individual test failures, containing the failed test classes/methods
      #   executedTestTasks[]: all test task paths that actually ran (for skip-if-passed logic)
      #   failedTestTasks[]: test tasks that failed at the Gradle level without individual test failures
      #   testseed: the test seed from the original run
      if jq --arg testseed "${TESTS_SEED:-}" '
        (.tests | map(.taskPath) | unique) as $testTaskPaths |
        [.tasks[] | select(.outcome == "FAILED") | .path] as $failedTaskPaths |
        ([.tests[] | select(.result == "FAILURE") | .taskPath] | unique) as $tasksWithTestFailures |
        ([.tests[] | select(.result == "FAILURE")] | group_by(.taskPath) | map(
          . as $taskTests |
          {
            name: $taskTests[0].taskPath,
            outcome: "failed",
            tests: ($taskTests | group_by(.className) | map({
              name: .[0].className,
              outcome: { overall: "failed", own: "passed", children: "failed" },
              children: [.[] | {
                name: .methodName,
                outcome: { overall: "failed" },
                children: []
              }]
            }))
          }
        )) as $workUnits |
        [$failedTaskPaths[] | select(. as $p | $tasksWithTestFailures | index($p) | not) | select(. as $p | $testTaskPaths | index($p))] as $failedTestTasks |
        {
          workUnits: $workUnits,
          testseed: $testseed,
          executedTestTasks: $testTaskPaths,
          failedTestTasks: $failedTestTasks
        }
      ' "$TASK_STATUS_FILE" > .failed-test-history.json; then

        chmod 600 .failed-test-history.json

        FILTERED_WORK_UNITS=$(jq -r '.workUnits | length' .failed-test-history.json 2>/dev/null || echo "0")
        EXECUTED_TASKS_COUNT=$(jq -r '.executedTestTasks | length' .failed-test-history.json 2>/dev/null || echo "0")
        FAILED_TEST_TASKS_COUNT=$(jq -r '.failedTestTasks | length' .failed-test-history.json 2>/dev/null || echo "0")

        echo "[Smart Retry] Fetched $EXECUTED_TASKS_COUNT executed test tasks from previous run"
        if [[ "$FAILED_TEST_TASKS_COUNT" -gt 0 ]]; then
          echo "[Smart Retry] Fetched $FAILED_TEST_TASKS_COUNT failed tasks from previous run"
        fi

        if [[ "$FILTERED_WORK_UNITS" -eq 0 ]] && [[ "$FAILED_TEST_TASKS_COUNT" -eq 0 ]]; then
          rm -f .failed-test-history.json
          SMART_RETRY_STATUS="disabled"
          SMART_RETRY_DETAILS="Previous failure was not caused by test or task failures — rerunning all tests"
          echo "[Smart Retry] Disabled: previous build had no test or task failures"
          echo "[Smart Retry] All tests will run."
        else
          SMART_RETRY_STATUS="enabled"
          SMART_RETRY_DETAILS="Filtering to $FILTERED_WORK_UNITS work units with test failures, $FAILED_TEST_TASKS_COUNT tasks with non-test failures"

          ORIGIN_JOB_NAME=$(printf '%s\n' "$BUILD_JSON" | jq -r --arg jobId "$ORIGIN_JOB_ID" '.jobs[] | select(.id == $jobId) | .name' 2>/dev/null)
          if [ -z "$ORIGIN_JOB_NAME" ] || [ "$ORIGIN_JOB_NAME" = "null" ]; then
            ORIGIN_JOB_NAME="previous attempt"
          fi

          echo "[Smart Retry] Enabled: $FILTERED_WORK_UNITS work units with test failures, $FAILED_TEST_TASKS_COUNT tasks with non-test failures"

          cat << EOF | buildkite-agent annotate --style info --context "smart-retry-$BUILDKITE_JOB_ID"
Rerunning failed build job [$ORIGIN_JOB_NAME]

**Gradle Tasks with Test Failures:** $FILTERED_WORK_UNITS
**Gradle Tasks with Non-Test Failures:** $FAILED_TEST_TASKS_COUNT
**Executed Test Tasks in Previous Run:** ${EXECUTED_TASKS_COUNT:-unknown}

This retry will rerun failed tests, rerun all tests for tasks that failed at the Gradle level (e.g. resource leaks), skip confirmed-passed tasks, and run all tests for tasks not executed in the previous run.
EOF
        fi
      else
        echo "[Smart Retry] Failed to transform task-status.json into retry filter"
        echo "[Smart Retry] will be disabled - all tests will run."
        SMART_RETRY_STATUS="failed"
        SMART_RETRY_DETAILS="Failed to transform task status data"
      fi
    fi

    rm -f "$TASK_STATUS_FILE"
    trap - EXIT
  else
    echo "[Smart Retry] Configuration Issue"
    echo "[Smart Retry] Could not find origin job ID for retry."
    echo "[Smart Retry] will be disabled for this run."
    SMART_RETRY_STATUS="failed"
    SMART_RETRY_DETAILS="No origin job ID found"
  fi
else
  echo "[Smart Retry] API Error"
  echo "[Smart Retry] Failed to fetch build information from Buildkite API"
  echo "[Smart Retry] will be disabled - all tests will run."
  SMART_RETRY_STATUS="failed"
  SMART_RETRY_DETAILS="Buildkite API request failed"
fi

# Store metadata for tracking and analysis
buildkite-agent meta-data set "smart-retry-status" "$SMART_RETRY_STATUS" 2>/dev/null || true
if [[ -n "$SMART_RETRY_DETAILS" ]]; then
  buildkite-agent meta-data set "smart-retry-details" "$SMART_RETRY_DETAILS" 2>/dev/null || true
fi
if [[ -n "${FILTERED_WORK_UNITS:-}" ]]; then
  buildkite-agent meta-data set "smart-retry-work-units" "$FILTERED_WORK_UNITS" 2>/dev/null || true
fi
if [[ -n "${EXECUTED_TASKS_COUNT:-}" ]]; then
  buildkite-agent meta-data set "smart-retry-executed-tasks" "$EXECUTED_TASKS_COUNT" 2>/dev/null || true
fi
if [[ -n "${FAILED_TEST_TASKS_COUNT:-}" ]] && [[ "$FAILED_TEST_TASKS_COUNT" -gt 0 ]]; then
  buildkite-agent meta-data set "smart-retry-failed-tasks" "$FAILED_TEST_TASKS_COUNT" 2>/dev/null || true
fi
if [[ "$SMART_RETRY_STATUS" == "disabled" ]]; then
  buildkite-agent meta-data set "smart-retry-disabled-reason" "no-test-or-task-failures" 2>/dev/null || true
fi
