@echo off

if DEFINED JAVA_HOME goto cont

:err
echo JAVA_HOME environment variable must be set!
pause
exit

:cont
set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set ES_HOME=%%~dpfI


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

REM min and max heap sizes should be set to the same value to avoid
REM stop-the-world GC pauses during resize, and so that we can lock the
REM heap in memory on startup to prevent any of it from being swapped
REM out.
set JAVA_OPTS=%JAVA_OPTS% -Xms%ES_MIN_MEM% -Xmx%ES_MAX_MEM%

REM new generation
if NOT "%ES_HEAP_NEWSIZE%" == "" (
set JAVA_OPTS=%JAVA_OPTS% -Xmn%ES_HEAP_NEWSIZE%
)

REM max direct memory
if NOT "%ES_DIRECT_SIZE%" == "" (
set JAVA_OPTS=%JAVA_OPTS% -XX:MaxDirectMemorySize=%ES_DIRECT_SIZE%
)

REM reduce the per-thread stack size
set JAVA_OPTS=%JAVA_OPTS% -Xss256k

REM set to headless, just in case
set JAVA_OPTS=%JAVA_OPTS% -Djava.awt.headless=true

REM Force the JVM to use IPv4 stack
if NOT "%ES_USE_IPV4%" == "" (
set JAVA_OPTS=%JAVA_OPTS% -Djava.net.preferIPv4Stack=true
)

set JAVA_OPTS=%JAVA_OPTS% -XX:+UseParNewGC
set JAVA_OPTS=%JAVA_OPTS% -XX:+UseConcMarkSweepGC

set JAVA_OPTS=%JAVA_OPTS% -XX:CMSInitiatingOccupancyFraction=75
set JAVA_OPTS=%JAVA_OPTS% -XX:+UseCMSInitiatingOccupancyOnly

REM When running under Java 7
REM JAVA_OPTS=%JAVA_OPTS% -XX:+UseCondCardMark

if NOT "%ES_USE_GC_LOGGING%" == "" set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCDetails
if NOT "%ES_USE_GC_LOGGING%" == "" set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCTimeStamps
if NOT "%ES_USE_GC_LOGGING%" == "" set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintClassHistogram
if NOT "%ES_USE_GC_LOGGING%" == "" set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintTenuringDistribution
if NOT "%ES_USE_GC_LOGGING%" == "" set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCApplicationStoppedTime
if NOT "%ES_USE_GC_LOGGING%" == "" set JAVA_OPTS=%JAVA_OPTS% -Xloggc:%ES_HOME%/logs/gc.log

REM Causes the JVM to dump its heap on OutOfMemory.
set JAVA_OPTS=%JAVA_OPTS% -XX:+HeapDumpOnOutOfMemoryError
REM The path to the heap dump location, note directory must exists and have enough
REM space for a full heap dump.
REM JAVA_OPTS=%JAVA_OPTS% -XX:HeapDumpPath=$ES_HOME/logs/heapdump.hprof

REM Disables explicit GC
set JAVA_OPTS=%JAVA_OPTS% -XX:+DisableExplicitGC

set ES_CLASSPATH=%ES_CLASSPATH%;%ES_HOME%/lib/${project.build.finalName}.jar;%ES_HOME%/lib/*;%ES_HOME%/lib/sigar/*
set ES_PARAMS=-Delasticsearch -Des-foreground=yes -Des.path.home="%ES_HOME%"