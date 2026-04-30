@ECHO OFF
SETLOCAL EnableDelayedExpansion

FOR /F "tokens=* eol=#" %%i in ('type .ci\java-versions.properties') do set %%i

SET JAVA_HOME=%USERPROFILE%\.java\%ES_BUILD_JAVA%
SET JAVA16_HOME=%USERPROFILE%\.java\openjdk16

SET GRADLEW=./gradlew --parallel --no-daemon --scan --build-cache --no-watch-fs -Dorg.elasticsearch.build.cache.url=https://gradle-enterprise.elastic.co/cache/
SET GRADLEW_BAT=./gradlew.bat --parallel --no-daemon --scan --build-cache --no-watch-fs -Dorg.elasticsearch.build.cache.url=https://gradle-enterprise.elastic.co/cache/

(if not exist "%USERPROFILE%/.gradle" mkdir "%USERPROFILE%/.gradle") && (echo. >> "%USERPROFILE%/.gradle/gradle.properties" && echo org.gradle.daemon=false >> "%USERPROFILE%/.gradle/gradle.properties")

set WORKSPACE=%cd%
set BUILD_NUMBER=%BUILDKITE_BUILD_NUMBER%
set COMPOSE_HTTP_TIMEOUT=120
set JOB_BRANCH=%BUILDKITE_BRANCH%

set GH_TOKEN=%VAULT_GITHUB_TOKEN%

REM Set credentials needed for gradle build cache and smart retries
for /f "delims=" %%i in ('vault read -field^=username secret/ci/elastic-elasticsearch/migrated/gradle-build-cache') do set GRADLE_BUILD_CACHE_USERNAME=%%i
for /f "delims=" %%i in ('vault read -field^=password secret/ci/elastic-elasticsearch/migrated/gradle-build-cache') do set GRADLE_BUILD_CACHE_PASSWORD=%%i
for /f "delims=" %%i in ('vault read -field^=accesskey secret/ci/elastic-elasticsearch/migrated/gradle-build-cache') do set DEVELOCITY_API_ACCESS_KEY=%%i
set DEVELOCITY_ACCESS_KEY=gradle-enterprise.elastic.co=!DEVELOCITY_API_ACCESS_KEY!

for /f "delims=" %%i in ('vault read -field^=token secret/ci/elastic-elasticsearch/buildkite-api-token') do set BUILDKITE_API_TOKEN=%%i

bash.exe -c "nohup bash .buildkite/scripts/setup-monitoring.sh </dev/null >/dev/null 2>&1 &"

REM =============================================================================
REM PART 1: Test seed retrieval for ANY retry
REM =============================================================================
REM When a job is retried, retrieve and use
REM the same test seed from the original job to ensure reproducible test failures.
REM =============================================================================
set ORIGIN_JOB_ID=
set BUILD_SCAN_ID=
set BUILD_SCAN_URL=
set TESTS_SEED=

if defined BUILDKITE_RETRY_COUNT (
  if %BUILDKITE_RETRY_COUNT% GTR 0 (
    echo --- Retrieving test seed from original job

    REM Fetch build information from Buildkite API
    curl --max-time 30 -H "Authorization: Bearer %BUILDKITE_API_TOKEN%" -X GET "https://api.buildkite.com/v2/organizations/elastic/pipelines/%BUILDKITE_PIPELINE_SLUG%/builds/%BUILDKITE_BUILD_NUMBER%?include_retried_jobs=true" -o .build-info.json 2>nul

    if exist .build-info.json (
      REM Extract origin job ID
      for /f "delims=" %%i in ('jq -r --arg jobId "%BUILDKITE_JOB_ID%" ".jobs[] | select(.id == $jobId) | .retry_source.job_id" .build-info.json 2^>nul') do set ORIGIN_JOB_ID=%%i

      if defined ORIGIN_JOB_ID (
        if not "!ORIGIN_JOB_ID!"=="null" (
          REM Export the original job ID so the build scan can link back to it
          set BUILDKITE_RETRY_SOURCE_JOB_ID=!ORIGIN_JOB_ID!

          REM Extract retry type (automatic/manual) from the current job's API data
          for /f "delims=" %%i in ('jq -r --arg jobId "%BUILDKITE_JOB_ID%" ".jobs[] | select(.id == $jobId) | .retry_type // empty" .build-info.json 2^>nul') do set BUILDKITE_RETRY_TYPE=%%i

          REM Retrieve test seed directly from Buildkite metadata
          for /f "delims=" %%i in ('jq -r --arg job_id "!ORIGIN_JOB_ID!" ".meta_data[\"tests-seed-\" + $job_id]" .build-info.json 2^>nul') do set TESTS_SEED=%%i

          REM Retrieve build scan URL and ID for smart retry (reused in Part 2)
          for /f "delims=" %%i in ('jq -r --arg job_id "!ORIGIN_JOB_ID!" ".meta_data[\"build-scan-\" + $job_id]" .build-info.json 2^>nul') do set BUILD_SCAN_URL=%%i
          for /f "delims=" %%i in ('jq -r --arg job_id "!ORIGIN_JOB_ID!" ".meta_data[\"build-scan-id-\" + $job_id]" .build-info.json 2^>nul') do set BUILD_SCAN_ID=%%i

          if defined TESTS_SEED (
            if not "!TESTS_SEED!"=="null" (
              echo Using test seed !TESTS_SEED! from original job !ORIGIN_JOB_ID!
            ) else (
              echo Warning: Could not find test seed in metadata for original job.
              echo Tests will use a new random seed.
              set TESTS_SEED=
            )
          ) else (
            echo Warning: Could not find test seed in metadata for original job.
            echo Tests will use a new random seed.
          )
        ) else (
          echo Warning: Could not find origin job ID for retry.
          echo Tests will use a new random seed.
        )
      ) else (
        echo Warning: Could not find origin job ID for retry.
        echo Tests will use a new random seed.
      )
    ) else (
      echo Warning: Failed to fetch build information from Buildkite API
      echo Tests will use a new random seed.
    )
  )
)

REM =============================================================================
REM PART 2: Smart retry test filtering
REM =============================================================================
REM Extracted to .buildkite\scripts\smart-retry.bat for single responsibility.
REM The script is called (not executed separately) so it shares the SETLOCAL
REM environment — it can read ORIGIN_JOB_ID, BUILD_SCAN_ID, BUILD_SCAN_URL,
REM TESTS_SEED, and .build-info.json set by PART 1 above, and set
REM SMART_RETRY_STATUS, FILTERED_WORK_UNITS, etc. back in this scope.
REM =============================================================================
if defined BUILDKITE_RETRY_COUNT (
  if %BUILDKITE_RETRY_COUNT% GTR 0 (
    call .buildkite\scripts\smart-retry.bat
  )
)

REM Capture variables before ENDLOCAL so they can be restored to parent scope
set "_WORKSPACE=%WORKSPACE%"
set "_GRADLEW=%GRADLEW%"
set "_GRADLEW_BAT=%GRADLEW_BAT%"
set "_BUILD_NUMBER=%BUILD_NUMBER%"
set "_JOB_BRANCH=%JOB_BRANCH%"
set "_GH_TOKEN=%GH_TOKEN%"
set "_GRADLE_BUILD_CACHE_USERNAME=%GRADLE_BUILD_CACHE_USERNAME%"
set "_GRADLE_BUILD_CACHE_PASSWORD=%GRADLE_BUILD_CACHE_PASSWORD%"
set "_DEVELOCITY_ACCESS_KEY=%DEVELOCITY_ACCESS_KEY%"
set "_DEVELOCITY_API_ACCESS_KEY=%DEVELOCITY_API_ACCESS_KEY%"
set "_BUILDKITE_API_TOKEN=%BUILDKITE_API_TOKEN%"
set "_JAVA_HOME=%JAVA_HOME%"
set "_JAVA16_HOME=%JAVA16_HOME%"
set "_TESTS_SEED=%TESTS_SEED%"
set "_BUILDKITE_RETRY_SOURCE_JOB_ID=%BUILDKITE_RETRY_SOURCE_JOB_ID%"
set "_BUILDKITE_RETRY_TYPE=%BUILDKITE_RETRY_TYPE%"

REM End local scope and restore critical variables to parent environment
REM This ensures bash scripts can access WORKSPACE, GRADLEW, and other variables
ENDLOCAL && set "WORKSPACE=%_WORKSPACE%" && set "GRADLEW=%_GRADLEW%" && set "GRADLEW_BAT=%_GRADLEW_BAT%" && set "BUILD_NUMBER=%_BUILD_NUMBER%" && set "JOB_BRANCH=%_JOB_BRANCH%" && set "GH_TOKEN=%_GH_TOKEN%" && set "GRADLE_BUILD_CACHE_USERNAME=%_GRADLE_BUILD_CACHE_USERNAME%" && set "GRADLE_BUILD_CACHE_PASSWORD=%_GRADLE_BUILD_CACHE_PASSWORD%" && set "DEVELOCITY_ACCESS_KEY=%_DEVELOCITY_ACCESS_KEY%" && set "DEVELOCITY_API_ACCESS_KEY=%_DEVELOCITY_API_ACCESS_KEY%" && set "BUILDKITE_API_TOKEN=%_BUILDKITE_API_TOKEN%" && set "JAVA_HOME=%_JAVA_HOME%" && set "JAVA16_HOME=%_JAVA16_HOME%" && set "TESTS_SEED=%_TESTS_SEED%" && set "BUILDKITE_RETRY_SOURCE_JOB_ID=%_BUILDKITE_RETRY_SOURCE_JOB_ID%" && set "BUILDKITE_RETRY_TYPE=%_BUILDKITE_RETRY_TYPE%"

bash.exe -c "bash .buildkite/scripts/get-latest-test-mutes.sh"

exit /b 0