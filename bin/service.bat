@echo off
SETLOCAL

if NOT DEFINED JAVA_HOME goto err

set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set ES_HOME=%%~dpfI

rem Detect JVM version to figure out appropriate executable to use
if not exist "%JAVA_HOME%\bin\java.exe" (
echo JAVA_HOME points to an invalid Java installation (no java.exe found in "%JAVA_HOME%"^). Existing...
goto finally
)
"%JAVA_HOME%\bin\java" -version 2>&1 | find "64-Bit" >nul:

if errorlevel 1 (
    set EXECUTABLE=%ES_HOME%\bin\elasticsearch-service-x86.exe
    set SERVICE_ID=elasticsearch-service-x86
    set ARCH=32-bit
) else (
    set EXECUTABLE=%ES_HOME%\bin\elasticsearch-service-x64.exe
    set SERVICE_ID=elasticsearch-service-x64
    set ARCH=64-bit
)

if EXIST "%EXECUTABLE%" goto okExe
echo elasticsearch-service-(x86|x64).exe was not found...

:okExe
set ES_VERSION=${project.version}

if "%LOG_DIR%" == "" (
set LOG_DIR=%ES_HOME%\logs
)

if "x%1x" == "xx" goto displayUsage
set SERVICE_CMD=%1
shift
if "x%1x" == "xx" goto checkServiceCmd
set SERVICE_ID=%1

:checkServiceCmd

if "%LOG_OPTS%" == "" (
set LOG_OPTS=--LogPath "%LOG_DIR%" --LogPrefix "%SERVICE_ID%" --StdError auto --StdOutput auto
)

if /i %SERVICE_CMD% == install goto doInstall
if /i %SERVICE_CMD% == remove goto doRemove
if /i %SERVICE_CMD% == start goto doStart
if /i %SERVICE_CMD% == stop goto doStop
if /i %SERVICE_CMD% == manager goto doManagment
echo Unknown option "%SERVICE_CMD%"

:displayUsage
echo.
echo Usage: service.bat install^|remove^|start^|stop^|manager [SERVICE_ID]
goto finally

:doStart
"%EXECUTABLE%" //ES//%SERVICE_ID% %LOG_OPTS%
if not errorlevel 1 goto started
echo Failed starting '%SERVICE_ID%' service
goto finally
:started
echo The service '%SERVICE_ID%' has been started
goto finally

:doStop
"%EXECUTABLE%" //SS//%SERVICE_ID% %LOG_OPTS%
if not errorlevel 1 goto stopped
echo Failed stopping '%SERVICE_ID%' service
goto finally
:stopped
echo The service '%SERVICE_ID%' has been stopped
goto finally

:doManagment
set EXECUTABLE_MGR=%ES_HOME%\bin\elasticsearch-service-mgr.exe
"%EXECUTABLE_MGR%" //ES//%SERVICE_ID%
if not errorlevel 1 goto managed
echo Failed starting service manager for '%SERVICE_ID%'
goto finally
:managed
echo Succesfully started service manager for '%SERVICE_ID%'.
goto finally

:doRemove
rem Remove the service
"%EXECUTABLE%" //DS//%SERVICE_ID% %LOG_OPTS%
if not errorlevel 1 goto removed
echo Failed removing '%SERVICE_ID%' service
goto finally
:removed
echo The service '%SERVICE_ID%' has been removed
goto finally

:doInstall
echo Installing service      :  "%SERVICE_ID%"
echo Using JAVA_HOME (%ARCH%):  "%JAVA_HOME%"

rem Check JVM server dll first
set JVM_DLL=%JAVA_HOME%\jre\bin\server\jvm.dll

if exist %JVM_DLL% goto foundJVM

set JVM_DLL=%JAVA_HOME%\bin\client\jvm.dll

if exist %JVM_DLL% (
echo Warning: JAVA_HOME points to a JRE and not JDK installation; a client (not a server^) JVM will be used...
) else (
echo JAVA_HOME points to an invalid Java installation (no jvm.dll found in "%JAVA_HOME%"^). Existing...
goto finally
)

:foundJVM
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

if "%DATA_DIR%" == "" (
set DATA_DIR=%ES_HOME%\data
)

if "%WORK_DIR%" == "" (
set WORK_DIR=%ES_HOME%
)

if "%CONF_DIR%" == "" (
set CONF_DIR=%ES_HOME%\config
)

if "%CONF_FILE%" == "" (
set CONF_FILE=%CONF_DIR%\elasticsearch.yml
)

set ES_CLASSPATH=%ES_CLASSPATH%;%ES_HOME%/lib/elasticsearch-%ES_VERSION%.jar;%ES_HOME%/lib/*;%ES_HOME%/lib/sigar/*
set ES_PARAMS=-Delasticsearch;-Des.path.home="%ES_HOME%";-Des.default.config="%CONF_FILE%";-Des.default.path.home="%ES_HOME%";-Des.default.path.logs="%LOG_DIR%";-Des.default.path.data="%DATA_DIR%";-Des.default.path.work="%WORK_DIR%";-Des.default.path.conf="%CONF_DIR%"

set JAVA_OPTS=%JAVA_OPTS: =;%
set JVM_OPTS=%JAVA_OPTS%;%ES_PARAMS%

if not "%ES_JAVA_OPTS%" == "" (
set JVM_ES_JAVA_OPTS=%ES_JAVA_OPTS: =#%
set JVM_OPTS=%JVM_OPTS%;%JVM_ES_JAVA_OPTS%
)

"%EXECUTABLE%" //IS//%SERVICE_ID% --StartClass org.elasticsearch.bootstrap.ElasticSearch --StopClass org.elasticsearch.bootstrap.ElasticSearch --StartMethod main --StopMethod close --Classpath "%ES_CLASSPATH%" ++JvmOptions %JVM_OPTS% %LOG_OPTS% --PidFile "%SERVICE_ID%.pid" --DisplayName "Elasticsearch %ES_VERSION% (%SERVICE_ID%)" --Description "Elasticsearch %ES_VERSION% Windows Service - http://elasticsearch.org" --Jvm "%JVM_DLL%" --StartMode jvm --StopMode jvm --StartPath "%ES_HOME%"


if not errorlevel 1 goto installed
echo Failed installing '%SERVICE_ID%' service
goto finally

:installed
echo The service '%SERVICE_ID%' has been installed.
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause

:finally

ENDLOCAL