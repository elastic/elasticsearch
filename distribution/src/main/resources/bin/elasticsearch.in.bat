@echo off

if DEFINED JAVA_HOME goto cont

:err
ECHO JAVA_HOME environment variable must be set! 1>&2
EXIT /B 1

:cont
set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set ES_HOME=%%~dpfI


REM ***** JAVA options *****

if "%ES_MIN_MEM%" == "" (
set ES_MIN_MEM=${heap.min}
)

if "%ES_MAX_MEM%" == "" (
set ES_MAX_MEM=${heap.max}
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

REM set to headless, just in case
set JAVA_OPTS=%JAVA_OPTS% -Djava.awt.headless=true

REM Force the JVM to use IPv4 stack
if NOT "%ES_USE_IPV4%" == "" (
set JAVA_OPTS=%JAVA_OPTS% -Djava.net.preferIPv4Stack=true
)

REM Add gc options. ES_GC_OPTS is unsupported, for internal testing
if "%ES_GC_OPTS%" == "" (
set ES_GC_OPTS=%ES_GC_OPTS% -XX:+UseParNewGC
set ES_GC_OPTS=%ES_GC_OPTS% -XX:+UseConcMarkSweepGC
set ES_GC_OPTS=%ES_GC_OPTS% -XX:CMSInitiatingOccupancyFraction=75
set ES_GC_OPTS=%ES_GC_OPTS% -XX:+UseCMSInitiatingOccupancyOnly
REM When running under Java 7
REM JAVA_OPTS=%JAVA_OPTS% -XX:+UseCondCardMark
)
set JAVA_OPTS=%JAVA_OPTS%%ES_GC_OPTS%

if "%ES_GC_LOG_FILE%" == "" goto nogclog

:gclog
set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCDetails
set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCTimeStamps
set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCDateStamps
set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintClassHistogram
set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintTenuringDistribution
set JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCApplicationStoppedTime
set JAVA_OPTS=%JAVA_OPTS% -Xloggc:%ES_GC_LOG_FILE%
for %%F in ("%ES_GC_LOG_FILE%") do set ES_GC_LOG_FILE_DIRECTORY=%%~dpF
if NOT EXIST "%ES_GC_LOG_FILE_DIRECTORY%\." mkdir "%ES_GC_LOG_FILE_DIRECTORY%"

:nogclog

REM Causes the JVM to dump its heap on OutOfMemory.
set JAVA_OPTS=%JAVA_OPTS% -XX:+HeapDumpOnOutOfMemoryError
REM The path to the heap dump location, note directory must exists and have enough
REM space for a full heap dump.
REM JAVA_OPTS=%JAVA_OPTS% -XX:HeapDumpPath=$ES_HOME/logs/heapdump.hprof

REM Disables explicit GC
set JAVA_OPTS=%JAVA_OPTS% -XX:+DisableExplicitGC

REM Ensure UTF-8 encoding by default (e.g. filenames)
set JAVA_OPTS=%JAVA_OPTS% -Dfile.encoding=UTF-8

REM Use our provided JNA always versus the system one
set JAVA_OPTS=%JAVA_OPTS% -Djna.nosys=true

REM check in case a user was using this mechanism
if "%ES_CLASSPATH%" == "" (
set ES_CLASSPATH=%ES_HOME%/lib/elasticsearch-${project.version}.jar;%ES_HOME%/lib/*
) else (
ECHO Error: Don't modify the classpath with ES_CLASSPATH, Best is to add 1>&2
ECHO additional elements via the plugin mechanism, or if code must really be 1>&2
ECHO added to the main classpath, add jars to lib\, unsupported 1>&2
EXIT /B 1
)
set ES_PARAMS=-Delasticsearch -Des-foreground=yes -Des.path.home="%ES_HOME%"
