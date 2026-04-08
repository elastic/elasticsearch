@ECHO OFF
REM =============================================================================
REM Smart retry test filtering
REM =============================================================================
REM When smart retries are enabled, fetch the list of failed tests from the
REM original job and filter this retry to only run those tests.
REM
REM Expected environment (set by pre-command.bat before calling):
REM   ORIGIN_JOB_ID       - retried job's source ID (may be empty/null)
REM   BUILD_SCAN_ID       - Develocity build scan ID (may be empty/null)
REM   BUILD_SCAN_URL      - Develocity build scan URL (may be empty/null)
REM   TESTS_SEED          - test seed from the original job (may be empty)
REM   .build-info.json    - Buildkite build JSON file (may not exist)
REM   BUILDKITE_*         - standard Buildkite env vars
REM   DEVELOCITY_API_ACCESS_KEY - API token for Develocity
REM   BUILDKITE_API_TOKEN       - API token for Buildkite
REM
REM Outputs (available to caller after call):
REM   .failed-test-history.json - written to %cd% when smart retry succeeds
REM   SMART_RETRY_STATUS        - enabled | disabled | failed
REM   SMART_RETRY_DETAILS       - human-readable status detail
REM   FILTERED_WORK_UNITS       - number of failed work units (if enabled)
REM   EXECUTED_TASKS_COUNT      - number of executed test tasks (if available)
REM   Buildkite metadata keys:  smart-retry-status, smart-retry-details,
REM                              origin-build-scan, smart-retry-work-units,
REM                              smart-retry-executed-tasks
REM =============================================================================

echo --- [Smart Retry] Resolving previously failed tests
set SMART_RETRY_STATUS=disabled
set SMART_RETRY_DETAILS=

REM Check if we need to fetch build info (should already exist from Part 1)
if not exist .build-info.json (
  curl --retry 3 --retry-delay 2 --retry-max-time 60 --retry-connrefused --connect-timeout 10 --max-time 30 -H "Authorization: Bearer %BUILDKITE_API_TOKEN%" -X GET "https://api.buildkite.com/v2/organizations/elastic/pipelines/%BUILDKITE_PIPELINE_SLUG%/builds/%BUILDKITE_BUILD_NUMBER%?include_retried_jobs=true" -o .build-info.json 2>nul
)

if exist .build-info.json (
  REM Get origin job ID if not already set
  if not defined ORIGIN_JOB_ID (
    for /f "delims=" %%i in ('jq -r --arg jobId "%BUILDKITE_JOB_ID%" ".jobs[] | select(.id == $jobId) | .retry_source.job_id" .build-info.json 2^>nul') do set ORIGIN_JOB_ID=%%i
  )

  if defined ORIGIN_JOB_ID (
    if not "!ORIGIN_JOB_ID!"=="null" (
      REM Get build scan ID if not already set
      if not defined BUILD_SCAN_ID (
        for /f "delims=" %%i in ('jq -r --arg job_id "!ORIGIN_JOB_ID!" ".meta_data[\"build-scan-id-\" + $job_id]" .build-info.json 2^>nul') do set BUILD_SCAN_ID=%%i
        for /f "delims=" %%i in ('jq -r --arg job_id "!ORIGIN_JOB_ID!" ".meta_data[\"build-scan-\" + $job_id]" .build-info.json 2^>nul') do set BUILD_SCAN_URL=%%i
      )

      if defined BUILD_SCAN_ID (
        if not "!BUILD_SCAN_ID!"=="null" (

          REM Validate using PowerShell via environment variable (avoids delayed-expansion injection)
          powershell -NoProfile -Command "exit -not ($env:BUILD_SCAN_ID -match '^[a-zA-Z0-9_\-]+$')"
          if errorlevel 1 (
            echo [Smart Retry] Configuration Issue
            echo Invalid build scan ID format: !BUILD_SCAN_ID!
            echo [Smart Retry] will be disabled for this run.
            set SMART_RETRY_STATUS=failed
            set SMART_RETRY_DETAILS=Invalid build scan ID format
          ) else (
            REM Set Develocity API URL
            if not defined DEVELOCITY_BASE_URL set DEVELOCITY_BASE_URL=https://gradle-enterprise.elastic.co
            set DEVELOCITY_FAILED_TEST_API_URL=!DEVELOCITY_BASE_URL!/api/tests/build/!BUILD_SCAN_ID!?testOutcomes=failed
            set DEVELOCITY_TEST_PERF_API_URL=!DEVELOCITY_BASE_URL!/api/builds/!BUILD_SCAN_ID!/gradle-test-performance

            REM Add random delay to prevent API rate limiting (0-4 seconds)
            set /a "delay=%RANDOM% %% 5"
            timeout /t !delay! /nobreak >nul 2>&1

            REM Fetch failed tests from Develocity API (curl will auto-decompress gzip with --compressed)
            REM Write to temp file first: cmd.exe truncates redirect targets before the pipeline runs,
            REM so a direct redirect would leave an empty file on curl/jq failure.
            curl --compressed --request GET --url "!DEVELOCITY_FAILED_TEST_API_URL!" --max-filesize 10485760 --max-time 30 --retry 3 --retry-delay 2 --retry-max-time 60 --retry-connrefused --connect-timeout 10 --header "accept: application/json" --header "authorization: Bearer %DEVELOCITY_API_ACCESS_KEY%" --header "content-type: application/json" 2>nul | jq --arg testseed "!TESTS_SEED!" ". + {testseed: $testseed}" > .failed-test-history.json.dl 2>nul

            REM Validate the downloaded file is non-empty before using it
            set HISTORY_DL_SIZE=0
            if exist .failed-test-history.json.dl (
              for %%A in (.failed-test-history.json.dl) do set HISTORY_DL_SIZE=%%~zA
            )
            if !HISTORY_DL_SIZE! GTR 0 (
              move /y .failed-test-history.json.dl .failed-test-history.json >nul 2>&1
            ) else (
              del .failed-test-history.json.dl 2>nul
            )

            if exist .failed-test-history.json (
              REM Fetch executed test tasks from gradle-test-performance endpoint
              REM This enables three-state logic: distinguishing confirmed-passed from never-executed tasks
              curl --compressed --request GET --url "!DEVELOCITY_TEST_PERF_API_URL!" --max-filesize 10485760 --max-time 30 --retry 3 --retry-delay 2 --retry-max-time 60 --retry-connrefused --connect-timeout 10 --header "accept: application/json" --header "authorization: Bearer %DEVELOCITY_API_ACCESS_KEY%" --header "content-type: application/json" 2>nul > .test-perf-response.json.dl 2>nul

              REM Validate the test-perf response is non-empty before using it
              set PERF_DL_SIZE=0
              if exist .test-perf-response.json.dl (
                for %%A in (.test-perf-response.json.dl) do set PERF_DL_SIZE=%%~zA
              )
              if !PERF_DL_SIZE! GTR 0 (
                move /y .test-perf-response.json.dl .test-perf-response.json >nul 2>&1
              ) else (
                del .test-perf-response.json.dl 2>nul
              )

              if exist .test-perf-response.json (
                REM Extract taskPath values and merge into the history JSON
                for /f "delims=" %%i in ('jq -c "[.testTasks[].taskPath]" .test-perf-response.json 2^>nul') do set EXECUTED_TASK_PATHS=%%i
                if defined EXECUTED_TASK_PATHS (
                  if not "!EXECUTED_TASK_PATHS!"=="null" (
                    jq --argjson executed "!EXECUTED_TASK_PATHS!" ". + {executedTestTasks: $executed}" .failed-test-history.json > .failed-test-history.json.tmp 2>nul
                    if exist .failed-test-history.json.tmp (
                      REM Validate tmp file is non-empty before replacing the original
                      for %%A in (.failed-test-history.json.tmp) do set TMP_SIZE=%%~zA
                      if !TMP_SIZE! GTR 0 (
                        move /y .failed-test-history.json.tmp .failed-test-history.json >nul 2>&1
                        for /f "delims=" %%i in ('jq ".executedTestTasks | length" .failed-test-history.json 2^>nul') do set EXECUTED_TASKS_COUNT=%%i
                        if not defined EXECUTED_TASKS_COUNT set EXECUTED_TASKS_COUNT=0
                        echo [Smart Retry] Fetched !EXECUTED_TASKS_COUNT! executed test tasks from previous run
                      ) else (
                        del .failed-test-history.json.tmp 2>nul
                        echo [Smart Retry] Warning: jq produced empty output when merging executed test tasks
                        echo [Smart Retry] Falling back to safe mode
                      )
                    ) else (
                      echo [Smart Retry] Warning: Could not merge executed test tasks into history JSON
                      echo [Smart Retry] Falling back to safe mode
                    )
                  ) else (
                    echo [Smart Retry] Warning: Could not parse executed test tasks from gradle-test-performance API
                    echo [Smart Retry] Falling back to safe mode
                  )
                ) else (
                  echo [Smart Retry] Warning: Could not parse executed test tasks from gradle-test-performance API
                  echo [Smart Retry] Falling back to safe mode
                )
                del .test-perf-response.json 2>nul
              ) else (
                echo [Smart Retry] Warning: Failed to fetch executed test tasks from Develocity API
                echo [Smart Retry] Falling back to safe mode
              )
              REM Set restrictive file permissions (owner only)
              icacls .failed-test-history.json /inheritance:r /grant:r "%USERNAME%:(R,W)" >nul 2>&1

              REM Count filtered tests for visibility
              for /f "delims=" %%i in ('jq -r ".workUnits | length" .failed-test-history.json 2^>nul') do set FILTERED_WORK_UNITS=%%i
              if not defined FILTERED_WORK_UNITS set FILTERED_WORK_UNITS=0

              set SMART_RETRY_STATUS=enabled
              set SMART_RETRY_DETAILS=Filtering to !FILTERED_WORK_UNITS! work units with failures

              REM Get the origin job name for better annotation labels
              for /f "delims=" %%i in ('jq -r --arg jobId "!ORIGIN_JOB_ID!" ".jobs[] | select(.id == $jobId) | .name" .build-info.json 2^>nul') do set ORIGIN_JOB_NAME=%%i
              if not defined ORIGIN_JOB_NAME set ORIGIN_JOB_NAME=previous attempt
              if "!ORIGIN_JOB_NAME!"=="null" set ORIGIN_JOB_NAME=previous attempt

              echo [Smart Retry] Enabled: filtering to !FILTERED_WORK_UNITS! work units

              REM Create Buildkite annotation for visibility
              echo Rerunning failed build job [!ORIGIN_JOB_NAME!]^(!BUILD_SCAN_URL!^) > .smart-retry-annotation.txt
              echo. >> .smart-retry-annotation.txt
              echo **Gradle Tasks with Failures:** !FILTERED_WORK_UNITS! >> .smart-retry-annotation.txt
              if defined EXECUTED_TASKS_COUNT (
                echo **Executed Test Tasks in Previous Run:** !EXECUTED_TASKS_COUNT! >> .smart-retry-annotation.txt
              ) else (
                echo **Executed Test Tasks in Previous Run:** unknown >> .smart-retry-annotation.txt
              )
              echo. >> .smart-retry-annotation.txt
              echo This retry will rerun failed tests, skip confirmed-passed tasks, and run all tests for tasks that were not executed in the previous run. >> .smart-retry-annotation.txt
              buildkite-agent annotate --style info --context "smart-retry-!BUILDKITE_JOB_ID!" < .smart-retry-annotation.txt
              del .smart-retry-annotation.txt 2>nul
            ) else (
              echo [Smart Retry] API Error
              echo [Smart Retry] Failed to fetch failed tests from Develocity API
              echo [Smart Retry] will be disabled - all tests will run.
              set SMART_RETRY_STATUS=failed
              set SMART_RETRY_DETAILS=API request failed
            )
          )
        ) else (
          echo [Smart Retry] Configuration Issue
          echo [Smart Retry] Could not find build scan ID in metadata.
          echo [Smart Retry] will be disabled for this run.
          set SMART_RETRY_STATUS=failed
          set SMART_RETRY_DETAILS=No build scan ID in metadata
        )
      ) else (
        echo [Smart Retry] Configuration Issue
        echo [Smart Retry] Could not find build scan ID in metadata.
        echo [Smart Retry] will be disabled for this run.
        set SMART_RETRY_STATUS=failed
        set SMART_RETRY_DETAILS=No build scan ID in metadata
      )
    ) else (
      echo [Smart Retry] Configuration Issue
      echo [Smart Retry] Could not find origin job ID for retry.
      echo [Smart Retry] will be disabled for this run.
      set SMART_RETRY_STATUS=failed
      set SMART_RETRY_DETAILS=No origin job ID found
    )
  ) else (
    echo [Smart Retry] Configuration Issue
    echo [Smart Retry] Could not find origin job ID for retry.
    echo [Smart Retry] will be disabled for this run.
    set SMART_RETRY_STATUS=failed
    set SMART_RETRY_DETAILS=No origin job ID found
  )

  REM Clean up temporary build info file and any leftover temp files
  del .build-info.json 2>nul
  del .test-perf-response.json 2>nul
  del .test-perf-response.json.dl 2>nul
  del .failed-test-history.json.tmp 2>nul
  del .failed-test-history.json.dl 2>nul
) else (
  echo [Smart Retry] API Error
  echo [Smart Retry] Failed to fetch build information from Buildkite API
  echo [Smart Retry] will be disabled - all tests will run.
  set SMART_RETRY_STATUS=failed
  set SMART_RETRY_DETAILS=Buildkite API request failed
)

REM Store metadata for tracking and analysis
buildkite-agent meta-data set "smart-retry-status" "!SMART_RETRY_STATUS!" 2>nul
if defined SMART_RETRY_DETAILS (
  buildkite-agent meta-data set "smart-retry-details" "!SMART_RETRY_DETAILS!" 2>nul
)
if defined BUILD_SCAN_URL (
  buildkite-agent meta-data set "origin-build-scan" "!BUILD_SCAN_URL!" 2>nul
)
REM Effectiveness metrics: track how many tests are being filtered per retry
if defined FILTERED_WORK_UNITS (
  buildkite-agent meta-data set "smart-retry-work-units" "!FILTERED_WORK_UNITS!" 2>nul
)
if defined EXECUTED_TASKS_COUNT (
  buildkite-agent meta-data set "smart-retry-executed-tasks" "!EXECUTED_TASKS_COUNT!" 2>nul
)

exit /b 0
