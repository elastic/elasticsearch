@echo off

if NOT DEFINED JAVA_HOME IF EXIST %ProgramData%\Oracle\java\javapath\java.exe (
    for /f "tokens=2 delims=[]" %%a in ('dir %ProgramData%\Oracle\java\javapath\java.exe') do @set JAVA_EXE=%%a
)
if DEFINED JAVA_EXE set JAVA_HOME=%JAVA_EXE:\bin\java.exe=%
if DEFINED JAVA_EXE (
    ECHO Using JAVA_HOME=%JAVA_HOME% retrieved from %ProgramData%\Oracle\java\javapath\java.exe
)
set JAVA_EXE=
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
