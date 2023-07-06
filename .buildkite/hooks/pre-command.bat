@REM @ECHO OFF

@REM FOR /F "tokens=* eol=#" %%i in ('type .ci\java-versions.properties') do set %%i

@REM SET JAVA_HOME=%USERPROFILE%\.java\%ES_BUILD_JAVA%
@REM SET JAVA11_HOME=%USERPROFILE%\.java\java11
@REM SET JAVA16_HOME=%USERPROFILE%\.java\openjdk16

@REM SET GRADLEW_BAT=./gradlew.bat --parallel --build-cache --no-watch-fs -Dorg.elasticsearch.build.cache.url=https://gradle-enterprise.elastic.co/cache/

@REM set WORKSPACE=%cd%
@REM set BUILD_NUMBER=%BUILDKITE_BUILD_NUMBER%
@REM set COMPOSE_HTTP_TIMEOUT=120
@REM set JOB_BRANCH=%BUILDKITE_BRANCH%

@REM set GRADLE_BUILD_CACHE_USERNAME=vault read -field=username secret/ci/elastic-elasticsearch/migrated/gradle-build-cache
@REM set GRADLE_BUILD_CACHE_PASSWORD=vault read -field=password secret/ci/elastic-elasticsearch/migrated/gradle-build-cache

@REM powershell -Command "Invoke-WebRequest https://download.java.net/java/GA/jdk17.0.2/dfd4a8d0985749f896bed50d7138ee7f/8/GPL/openjdk-17.0.2_windows-x64_bin.zip -OutFile openjdk-17.0.2_windows-x64_bin.zip"
@REM powershell -Command "Expand-Archive openjdk-17.0.2_windows-x64_bin.zip"

@REM md "%JAVA_HOME%"
@REM rem move openjdk-17.0.2_windows-x64_bin\jdk-17.0.2 "%JAVA_HOME%"
@REM robocopy openjdk-17.0.2_windows-x64_bin\jdk-17.0.2\ %JAVA_HOME% /E /MOV /NFL /NDL

@REM exit /b 0
