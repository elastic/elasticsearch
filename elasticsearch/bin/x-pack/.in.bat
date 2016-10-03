@echo off

rem Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
rem or more contributor license agreements. Licensed under the Elastic License;
rem you may not use this file except in compliance with the Elastic License.

REM .in.bat <java main class> [args,..]

SETLOCAL

if NOT DEFINED JAVA_HOME goto err

set JAVA_CMD=%1
if "%JAVA_CMD%" == "" goto err_java_cmd

REM fix args
for /f "usebackq tokens=1*" %%i in (`echo %*`) DO @ set params=%%j
SHIFT

set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..\..") do set ES_HOME=%%~dpfI

REM ***** JAVA options *****

if "%ES_MIN_MEM%" == "" (
set ES_MIN_MEM=256m
)

if "%ES_MAX_MEM%" == "" (
set ES_MAX_MEM=1g
)

if NOT "%ES_HEAP_SIZE%" == "" (
set ES_MIN_MEM=%ES_HEAP_SIZE%
set ES_MAX_MEM=%ES_HEAP_SIZE%
)

set JAVA_OPTS=%JAVA_OPTS% -Xms%ES_MIN_MEM% -Xmx%ES_MAX_MEM%

if NOT "%ES_HEAP_NEWSIZE%" == "" (
  set JAVA_OPTS=%JAVA_OPTS% -Xmn%ES_HEAP_NEWSIZE%
)

if NOT "%ES_DIRECT_SIZE%" == "" (
  set JAVA_OPTS=%JAVA_OPTS% -XX:MaxDirectMemorySize=%ES_DIRECT_SIZE%
)

set JAVA_OPTS=%JAVA_OPTS% -Xss256k

REM Enable aggressive optimizations in the JVM
REM    - Disabled by default as it might cause the JVM to crash
REM set JAVA_OPTS=%JAVA_OPTS% -XX:+AggressiveOpts

set JAVA_OPTS=%JAVA_OPTS% -XX:+UseParNewGC
set JAVA_OPTS=%JAVA_OPTS% -XX:+UseConcMarkSweepGC

set JAVA_OPTS=%JAVA_OPTS% -XX:CMSInitiatingOccupancyFraction=75
set JAVA_OPTS=%JAVA_OPTS% -XX:+UseCMSInitiatingOccupancyOnly

REM When running under Java 7
REM JAVA_OPTS=%JAVA_OPTS% -XX:+UseCondCardMark

REM GC logging options -- uncomment to enable
REM JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCDetails
REM JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCTimeStamps
REM JAVA_OPTS=%JAVA_OPTS% -XX:+PrintClassHistogram
REM JAVA_OPTS=%JAVA_OPTS% -XX:+PrintTenuringDistribution
REM JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCApplicationStoppedTime
REM JAVA_OPTS=%JAVA_OPTS% -Xloggc:/var/log/elasticsearch/gc.log

REM Causes the JVM to dump its heap on OutOfMemory.
set JAVA_OPTS=%JAVA_OPTS% -XX:+HeapDumpOnOutOfMemoryError
REM The path to the heap dump location, note directory must exists and have enough
REM space for a full heap dump.
REM JAVA_OPTS=%JAVA_OPTS% -XX:HeapDumpPath=$ES_HOME/logs/heapdump.hprof

REM Disables explicit GC
set JAVA_OPTS=%JAVA_OPTS% -XX:+DisableExplicitGC
REM Avoid empty elements in classpath to make JarHell happy
if "%ES_CLASSPATH%" == "" (
  set ES_CLASSPATH=%ES_HOME%/lib/*;%ES_HOME%/plugins/x-pack/*
) else (
  set ES_CLASSPATH=%ES_CLASSPATH%;%ES_HOME%/lib/*;%ES_HOME%/plugins/x-pack/*
)
set ES_PARAMS=-Des.path.home="%ES_HOME%"

SET HOSTNAME=%COMPUTERNAME%

"%JAVA_HOME%\bin\java" %JAVA_OPTS% %ES_JAVA_OPTS% %ES_PARAMS% -cp "%ES_CLASSPATH%" %JAVA_CMD% %PARAMS%
goto finally

:err
echo JAVA_HOME environment variable must be set!
ENDLOCAL
EXIT /B 1
:err_java_cmd
echo Can not call .in.bat without specifying a main java class
ENDLOCAL
EXIT /B 1

:finally
ENDLOCAL
