@ECHO OFF

FOR /F "tokens=* eol=#" %%i in ('type .ci/java-versions.properties') do SET %%i

SET JAVA_HOME="%USERPROFILE%\\.java\\%ES_BUILD_JAVA%"
SET JAVA11_HOME="%USERPROFILE%\\.java\\java11"
SET JAVA16_HOME="%USERPROFILE%\\.java\\openjdk16"

set GRADLE_BUILD_CACHE_USERNAME=vault read -field=username secret/ci/elastic-elasticsearch/migrated/gradle-build-cache
