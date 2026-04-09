#!/bin/bash

# =============================================================================
# Smart retry test filtering
# =============================================================================
# When smart retries are enabled, fetch the list of failed tests from the
# original job and filter this retry to only run those tests.
#
# Expected environment (set by pre-command before sourcing):
#   BUILD_JSON       – Buildkite build JSON (may be empty)
#   ORIGIN_JOB_ID    – retried job's source ID (may be empty/null)
#   BUILD_SCAN_ID    – Develocity build scan ID (may be empty/null)
#   BUILD_SCAN_URL   – Develocity build scan URL (may be empty/null)
#   TESTS_SEED       – test seed from the original job (may be empty)
#   BUILDKITE_*      – standard Buildkite env vars
#   DEVELOCITY_API_ACCESS_KEY – API token for Develocity
#   BUILDKITE_API_TOKEN       – API token for Buildkite
#
# Outputs (available to caller after sourcing):
#   .failed-test-history.json – written to $PWD when smart retry succeeds
#   SMART_RETRY_STATUS        – enabled | disabled | failed
#   SMART_RETRY_DETAILS       – human-readable status detail
#   FILTERED_WORK_UNITS       – number of failed work units (if enabled)
#   EXECUTED_TASKS_COUNT      – number of executed test tasks (if available)
#   Buildkite metadata keys:  smart-retry-status, smart-retry-details,
#                              origin-build-scan, smart-retry-work-units,
#                              smart-retry-executed-tasks
# =============================================================================

echo "--- [Smart Retry] Resolving previously failed tests"
SMART_RETRY_STATUS="disabled"
SMART_RETRY_DETAILS=""

# Check if we already have the build info from seed retrieval above
if [[ -z "$BUILD_JSON" ]]; then
  # Fetch build info if not already available (shouldn't happen, but be safe)
  BUILD_JSON=$(curl --retry 3 --retry-delay 2 --retry-max-time 60 --retry-connrefused --connect-timeout 10 --max-time 30 -H "Authorization: Bearer $BUILDKITE_API_TOKEN" -X GET "https://api.buildkite.com/v2/organizations/elastic/pipelines/${BUILDKITE_PIPELINE_SLUG}/builds/${BUILDKITE_BUILD_NUMBER}?include_retried_jobs=true" 2>/dev/null) || BUILD_JSON=""
fi

if [[ -n "$BUILD_JSON" ]]; then
  # Get origin job ID if not already available
  if [[ -z "$ORIGIN_JOB_ID" ]] || [[ "$ORIGIN_JOB_ID" == "null" ]]; then
    ORIGIN_JOB_ID=$(printf '%s\n' "$BUILD_JSON" | jq -r --arg jobId "$BUILDKITE_JOB_ID" ' .jobs[] | select(.id == $jobId) | .retry_source.job_id' 2>/dev/null) || ORIGIN_JOB_ID=""
  fi

  if [[ -n "$ORIGIN_JOB_ID" ]] && [[ "$ORIGIN_JOB_ID" != "null" ]]; then
    # Get build scan ID if not already available
    if [[ -z "$BUILD_SCAN_ID" ]] || [[ "$BUILD_SCAN_ID" == "null" ]]; then
      BUILD_SCAN_ID=$(printf '%s\n' "$BUILD_JSON" | jq -r --arg job_id "$ORIGIN_JOB_ID" '.meta_data["build-scan-id-" + $job_id]' 2>/dev/null) || BUILD_SCAN_ID=""
      BUILD_SCAN_URL=$(printf '%s\n' "$BUILD_JSON" | jq -r --arg job_id "$ORIGIN_JOB_ID" '.meta_data["build-scan-" + $job_id]' 2>/dev/null) || BUILD_SCAN_URL=""
    fi

    if [[ -n "$BUILD_SCAN_ID" ]] && [[ "$BUILD_SCAN_ID" != "null" ]]; then
      # Validate BUILD_SCAN_ID format to prevent injection attacks
      if [[ ! "$BUILD_SCAN_ID" =~ ^[a-zA-Z0-9_-]+$ ]]; then
        echo "[Smart Retry] Configuration Issue"
        echo "Invalid build scan ID format: $BUILD_SCAN_ID"
        echo "[Smart Retry] will be disabled for this run."
        SMART_RETRY_STATUS="failed"
        SMART_RETRY_DETAILS="Invalid build scan ID format"
      else
        DEVELOCITY_BASE_URL="${DEVELOCITY_BASE_URL:-https://gradle-enterprise.elastic.co}"
        DEVELOCITY_FAILED_TEST_API_URL="${DEVELOCITY_BASE_URL}/api/tests/build/${BUILD_SCAN_ID}?testOutcomes=failed"
        DEVELOCITY_TEST_PERF_API_URL="${DEVELOCITY_BASE_URL}/api/builds/${BUILD_SCAN_ID}/gradle-test-performance"

        # Add random delay to prevent API rate limiting from parallel retries
        sleep $((RANDOM % 5))

        # Fetch both API responses in parallel to reduce retry startup latency.
        # The two Develocity calls are independent; we start them concurrently
        # and wait for both before processing results.
        PERF_RESPONSE_FILE=$(mktemp .test-perf-response.XXXXXX)
        curl --compressed --request GET \
          --url "$DEVELOCITY_TEST_PERF_API_URL" \
          --max-filesize 10485760 \
          --max-time 30 \
          --retry 3 \
          --retry-delay 2 \
          --retry-max-time 60 \
          --retry-connrefused \
          --connect-timeout 10 \
          --header 'accept: application/json' \
          --header "authorization: Bearer $DEVELOCITY_API_ACCESS_KEY" \
          --header 'content-type: application/json' 2>/dev/null > "$PERF_RESPONSE_FILE" &
        PERF_PID=$!

        if curl --compressed --request GET \
          --url "$DEVELOCITY_FAILED_TEST_API_URL" \
          --max-filesize 10485760 \
          --max-time 30 \
          --retry 3 \
          --retry-delay 2 \
          --retry-max-time 60 \
          --retry-connrefused \
          --connect-timeout 10 \
          --header 'accept: application/json' \
          --header "authorization: Bearer $DEVELOCITY_API_ACCESS_KEY" \
          --header 'content-type: application/json' 2>/dev/null | jq --arg testseed "${TESTS_SEED:-}" '. + {testseed: $testseed}' &> .failed-test-history.json; then

          # Wait for the parallel test-performance fetch to complete.
          # This enables three-state logic: distinguishing "confirmed passed" tasks
          # from "never executed" tasks (e.g. when Gradle stopped at first failure).
          # If this call fails, executedTestTasks will be null, triggering safe fallback.
          if wait "$PERF_PID" && [[ -s "$PERF_RESPONSE_FILE" ]]; then

            # Extract taskPath values from testTasks array and merge into the JSON
            EXECUTED_TASK_PATHS=$(jq '[.testTasks[].taskPath]' "$PERF_RESPONSE_FILE" 2>/dev/null)
            if [[ -n "$EXECUTED_TASK_PATHS" ]] && [[ "$EXECUTED_TASK_PATHS" != "null" ]]; then
              (umask 077 && jq --argjson executed "$EXECUTED_TASK_PATHS" '. + {executedTestTasks: $executed}' .failed-test-history.json > .failed-test-history.json.tmp) \
                && [[ -s .failed-test-history.json.tmp ]] \
                && mv .failed-test-history.json.tmp .failed-test-history.json \
                || { rm -f .failed-test-history.json.tmp; echo "[Smart Retry] Warning: Failed to merge executed test tasks into report"; }
              EXECUTED_TASKS_COUNT=$(printf '%s\n' "$EXECUTED_TASK_PATHS" | jq 'length' 2>/dev/null || echo "0")
              echo "[Smart Retry] Fetched $EXECUTED_TASKS_COUNT executed test tasks from previous run"
            else
              echo "[Smart Retry] Warning: Could not parse executed test tasks from gradle-test-performance API"
              echo "[Smart Retry] Falling back to safe mode (unrecognized tasks will run all tests)"
            fi
          else
            echo "[Smart Retry] Warning: Failed to fetch executed test tasks from Develocity API"
            echo "[Smart Retry] Falling back to safe mode (unrecognized tasks will run all tests)"
          fi
          rm -f "$PERF_RESPONSE_FILE"

          # Set secure file permissions
          chmod 600 .failed-test-history.json

          # Count filtered tests for visibility
          FILTERED_WORK_UNITS=$(jq -r '.workUnits | length' .failed-test-history.json 2>/dev/null || echo "0")
          SMART_RETRY_STATUS="enabled"
          SMART_RETRY_DETAILS="Filtering to $FILTERED_WORK_UNITS work units with failures"

          # Get the origin job name for better annotation labels
          ORIGIN_JOB_NAME=$(printf '%s\n' "$BUILD_JSON" | jq -r --arg jobId "$ORIGIN_JOB_ID" '.jobs[] | select(.id == $jobId) | .name' 2>/dev/null)
          if [ -z "$ORIGIN_JOB_NAME" ] || [ "$ORIGIN_JOB_NAME" = "null" ]; then
            ORIGIN_JOB_NAME="previous attempt"
          fi

          echo "[Smart Retry] Enabled: filtering to $FILTERED_WORK_UNITS work units"

          # Create Buildkite annotation for visibility
          # Use unique context per job to support multiple retries
          cat << EOF | buildkite-agent annotate --style info --context "smart-retry-$BUILDKITE_JOB_ID"
Rerunning failed build job [$ORIGIN_JOB_NAME]($BUILD_SCAN_URL)

**Gradle Tasks with Failures:** $FILTERED_WORK_UNITS
**Executed Test Tasks in Previous Run:** ${EXECUTED_TASKS_COUNT:-unknown}

This retry will rerun failed tests, skip confirmed-passed tasks, and run all tests for tasks that were not executed in the previous run.
EOF
        else
          # First API call failed; clean up the parallel perf fetch
          wait "$PERF_PID" 2>/dev/null || true
          rm -f "$PERF_RESPONSE_FILE"
          echo "[Smart Retry] API Error"
          echo "[Smart Retry] Failed to fetch failed tests from Develocity API"
          echo "[Smart Retry] will be disabled - all tests will run."
          SMART_RETRY_STATUS="failed"
          SMART_RETRY_DETAILS="API request failed"
        fi
      fi
    else
      echo "[Smart Retry] Configuration Issue"
      echo "[Smart Retry] Could not find build scan ID in metadata."
      echo "[Smart Retry] will be disabled for this run."
      SMART_RETRY_STATUS="failed"
      SMART_RETRY_DETAILS="No build scan ID in metadata"
    fi
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
if [[ -n "$BUILD_SCAN_URL" ]]; then
  buildkite-agent meta-data set "origin-build-scan" "$BUILD_SCAN_URL" 2>/dev/null || true
fi
# Effectiveness metrics: track how many tests are being filtered per retry
if [[ -n "${FILTERED_WORK_UNITS:-}" ]]; then
  buildkite-agent meta-data set "smart-retry-work-units" "$FILTERED_WORK_UNITS" 2>/dev/null || true
fi
if [[ -n "${EXECUTED_TASKS_COUNT:-}" ]]; then
  buildkite-agent meta-data set "smart-retry-executed-tasks" "$EXECUTED_TASKS_COUNT" 2>/dev/null || true
fi
