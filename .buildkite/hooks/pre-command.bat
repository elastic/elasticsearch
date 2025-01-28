@ECHO OFF

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

set GRADLE_BUILD_CACHE_USERNAME=vault read -field=username secret/ci/elastic-elasticsearch/migrated/gradle-build-cache
set GRADLE_BUILD_CACHE_PASSWORD=vault read -field=password secret/ci/elastic-elasticsearch/migrated/gradle-build-cache

bash.exe -c "nohup bash .buildkite/scripts/setup-monitoring.sh </dev/null >/dev/null 2>&1 &"
bash.exe -c "bash .buildkite/scripts/get-latest-test-mutes.sh"

exit /b 0
