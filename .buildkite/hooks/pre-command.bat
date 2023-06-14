@ECHO OFF

FOR /F "tokens=* eol=#" %%i in ('type .ci/java-versions.properties') do SET %%i

SET JAVA_HOME="%USERPROFILE%\\.java\\%ES_BUILD_JAVA%"
SET JAVA11_HOME="%USERPROFILE%\\.java\\java11"
SET JAVA16_HOME="%USERPROFILE%\\.java\\openjdk16"

SET GRADLEW_BAT=./gradlew.bat --parallel --scan --build-cache --no-watch-fs -Dorg.elasticsearch.build.cache.url=https://gradle-enterprise.elastic.co/cache/

set WORKSPACE="%cd%"
set BUILD_NUMBER="%BUILDKITE_BUILD_NUMBER%"
set COMPOSE_HTTP_TIMEOUT="120"
set JOB_BRANCH="%BUILDKITE_BRANCH%"

set GRADLE_BUILD_CACHE_USERNAME=vault read -field=username secret/ci/elastic-elasticsearch/migrated/gradle-build-cache
