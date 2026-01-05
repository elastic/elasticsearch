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

REM Smart retries implementation
if "%SMART_RETRIES%"=="true" (
  if defined BUILDKITE_RETRY_COUNT (
    if %BUILDKITE_RETRY_COUNT% GTR 0 (
      echo --- Resolving previously failed tests
      set SMART_RETRY_STATUS=disabled
      set SMART_RETRY_DETAILS=

      REM Fetch build information from Buildkite API
      curl --max-time 30 -H "Authorization: Bearer %BUILDKITE_API_TOKEN%" -X GET "https://api.buildkite.com/v2/organizations/elastic/pipelines/%BUILDKITE_PIPELINE_SLUG%/builds/%BUILDKITE_BUILD_NUMBER%?include_retried_jobs=true" -o .build-info.json 2>nul

      if exist .build-info.json (
        REM Extract origin job ID
        for /f "delims=" %%i in ('jq -r --arg jobId "%BUILDKITE_JOB_ID%" ".jobs[] | select(.id == $jobId) | .retry_source.job_id" .build-info.json 2^>nul') do set ORIGIN_JOB_ID=%%i

        if defined ORIGIN_JOB_ID (
          if not "!ORIGIN_JOB_ID!"=="null" (
            REM Extract build scan ID directly (new way)
            for /f "delims=" %%i in ('jq -r --arg job_id "!ORIGIN_JOB_ID!" ".meta_data[\"build-scan-id-\" + $job_id]" .build-info.json 2^>nul') do set BUILD_SCAN_ID=%%i

            REM Retrieve build scan URL for annotation
            for /f "delims=" %%i in ('jq -r --arg job_id "!ORIGIN_JOB_ID!" ".meta_data[\"build-scan-\" + $job_id]" .build-info.json 2^>nul') do set BUILD_SCAN_URL=%%i

            if defined BUILD_SCAN_ID (
              if not "!BUILD_SCAN_ID!"=="null" (

                REM Validate using PowerShell (more reliable)
                powershell -NoProfile -Command "exit -not ('!BUILD_SCAN_ID!' -match '^[a-zA-Z0-9_\-]+$')"
                if errorlevel 1 (
                  echo Smart Retry Configuration Issue
                  echo Invalid build scan ID format: !BUILD_SCAN_ID!
                  echo Smart retry will be disabled for this run.
                  set SMART_RETRY_STATUS=failed
                  set SMART_RETRY_DETAILS=Invalid build scan ID format
                ) else (
                  REM Set Develocity API URL
                  if not defined DEVELOCITY_BASE_URL set DEVELOCITY_BASE_URL=https://gradle-enterprise.elastic.co
                  set DEVELOCITY_FAILED_TEST_API_URL=!DEVELOCITY_BASE_URL!/api/tests/build/!BUILD_SCAN_ID!?testOutcomes=failed

                  REM Add random delay to prevent API rate limiting (0-4 seconds)
                  set /a "delay=%RANDOM% %% 5"
                  timeout /t !delay! /nobreak >nul 2>&1

                  REM Fetch failed tests from Develocity API (curl will auto-decompress gzip with --compressed)
                  curl --compressed --request GET --url "!DEVELOCITY_FAILED_TEST_API_URL!" --max-filesize 10485760 --max-time 30 --header "accept: application/json" --header "authorization: Bearer %DEVELOCITY_API_ACCESS_KEY%" --header "content-type: application/json" 2>nul | jq "." > .failed-test-history.json 2>nul

                  if exist .failed-test-history.json (
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

                    echo ✓ Smart retry enabled: filtering to !FILTERED_WORK_UNITS! work units

                    REM Create Buildkite annotation for visibility
                    echo Rerunning failed build job [!ORIGIN_JOB_NAME!]^(!BUILD_SCAN_URL!^) > .smart-retry-annotation.txt
                    echo. >> .smart-retry-annotation.txt
                    echo **Gradle Tasks with Failures:** !FILTERED_WORK_UNITS! >> .smart-retry-annotation.txt
                    echo. >> .smart-retry-annotation.txt
                    echo This retry will skip test tasks that had no failures in the previous run. >> .smart-retry-annotation.txt
                    buildkite-agent annotate --style info --context "smart-retry-!BUILDKITE_JOB_ID!" < .smart-retry-annotation.txt
                    del .smart-retry-annotation.txt 2>nul
                  ) else (
                    echo Smart Retry API Error
                    echo Failed to fetch failed tests from Develocity API
                    echo Smart retry will be disabled - all tests will run.
                    set SMART_RETRY_STATUS=failed
                    set SMART_RETRY_DETAILS=API request failed
                  )
                )
              ) else (
                echo Smart Retry Configuration Issue
                echo Could not find build scan ID in metadata.
                echo Smart retry will be disabled for this run.
                set SMART_RETRY_STATUS=failed
                set SMART_RETRY_DETAILS=No build scan ID in metadata
              )
            ) else (
              echo Smart Retry Configuration Issue
              echo Could not find build scan ID in metadata.
              echo Smart retry will be disabled for this run.
              set SMART_RETRY_STATUS=failed
              set SMART_RETRY_DETAILS=No build scan ID in metadata
            )
          ) else (
            echo Smart Retry Configuration Issue
            echo Could not find origin job ID for retry.
            echo Smart retry will be disabled for this run.
            set SMART_RETRY_STATUS=failed
            set SMART_RETRY_DETAILS=No origin job ID found
          )
        ) else (
          echo Smart Retry Configuration Issue
          echo Could not find origin job ID for retry.
          echo Smart retry will be disabled for this run.
          set SMART_RETRY_STATUS=failed
          set SMART_RETRY_DETAILS=No origin job ID found
        )

        REM Clean up temporary build info file
        del .build-info.json 2>nul
      ) else (
        echo Smart Retry API Error
        echo Failed to fetch build information from Buildkite API
        echo Smart retry will be disabled - all tests will run.
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
    )
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

REM End local scope and restore critical variables to parent environment
REM This ensures bash scripts can access WORKSPACE, GRADLEW, and other variables
ENDLOCAL && set "WORKSPACE=%_WORKSPACE%" && set "GRADLEW=%_GRADLEW%" && set "GRADLEW_BAT=%_GRADLEW_BAT%" && set "BUILD_NUMBER=%_BUILD_NUMBER%" && set "JOB_BRANCH=%_JOB_BRANCH%" && set "GH_TOKEN=%_GH_TOKEN%" && set "GRADLE_BUILD_CACHE_USERNAME=%_GRADLE_BUILD_CACHE_USERNAME%" && set "GRADLE_BUILD_CACHE_PASSWORD=%_GRADLE_BUILD_CACHE_PASSWORD%" && set "DEVELOCITY_ACCESS_KEY=%_DEVELOCITY_ACCESS_KEY%" && set "DEVELOCITY_API_ACCESS_KEY=%_DEVELOCITY_API_ACCESS_KEY%" && set "BUILDKITE_API_TOKEN=%_BUILDKITE_API_TOKEN%" && set "JAVA_HOME=%_JAVA_HOME%" && set "JAVA16_HOME=%_JAVA16_HOME%"

bash.exe -c "bash .buildkite/scripts/get-latest-test-mutes.sh"

exit /b 0
