@echo off

if DEFINED JAVA_HOME goto cont

:err
ECHO JAVA_HOME environment variable must be set! 1>&2
EXIT /B 1

:cont
set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set ES_HOME=%%~dpfI

REM check in case a user was using this mechanism
if "%ES_CLASSPATH%" == "" (
set ES_CLASSPATH=!ES_HOME!/lib/elasticsearch-${project.version}.jar;!ES_HOME!/lib/*
) else (
ECHO Error: Don't modify the classpath with ES_CLASSPATH, Best is to add 1>&2
ECHO additional elements via the plugin mechanism, or if code must really be 1>&2
ECHO added to the main classpath, add jars to lib\, unsupported 1>&2
EXIT /B 1
)
set ES_PARAMS=-Delasticsearch -Des.path.home="%ES_HOME%"
