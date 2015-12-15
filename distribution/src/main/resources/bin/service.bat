@echo off
SETLOCAL

TITLE Elasticsearch Service ${project.version}

if NOT DEFINED JAVA_HOME goto err

if not "%CONF_FILE%" == "" goto conffileset

set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set ES_HOME=%%~dpfI

rem Detect JVM version to figure out appropriate executable to use
if not exist "%JAVA_HOME%\bin\java.exe" (
echo JAVA_HOME points to an invalid Java installation (no java.exe found in "%JAVA_HOME%"^). Exiting...
goto:eof
)

"%JAVA_HOME%\bin\java" -Xmx50M -version > nul 2>&1

if errorlevel 1 (
	echo Warning: Could not start JVM to detect version, defaulting to x86:
	goto x86
)

"%JAVA_HOME%\bin\java" -Xmx50M -version 2>&1 | "%windir%\System32\find" "64-Bit" >nul:

if errorlevel 1 goto x86
set EXECUTABLE=%ES_HOME%\bin\elasticsearch-service-x64.exe
set SERVICE_ID=elasticsearch-service-x64
set ARCH=64-bit
goto checkExe

:x86
set EXECUTABLE=%ES_HOME%\bin\elasticsearch-service-x86.exe
set SERVICE_ID=elasticsearch-service-x86
set ARCH=32-bit

:checkExe
if EXIST "%EXECUTABLE%" goto okExe
echo elasticsearch-service-(x86|x64).exe was not found...

:okExe
set ES_VERSION=${project.version}

if "%LOG_DIR%" == "" set LOG_DIR=%ES_HOME%\logs

if "x%1x" == "xx" goto displayUsage
set SERVICE_CMD=%1
shift
if "x%1x" == "xx" goto checkServiceCmd
set SERVICE_ID=%1

:checkServiceCmd

if "%LOG_OPTS%" == "" set LOG_OPTS=--LogPath "%LOG_DIR%" --LogPrefix "%SERVICE_ID%" --StdError auto --StdOutput auto

if /i %SERVICE_CMD% == install goto doInstall
if /i %SERVICE_CMD% == remove goto doRemove
if /i %SERVICE_CMD% == start goto doStart
if /i %SERVICE_CMD% == stop goto doStop
if /i %SERVICE_CMD% == manager goto doManagment
echo Unknown option "%SERVICE_CMD%"

:displayUsage
echo.
echo Usage: service.bat install^|remove^|start^|stop^|manager [SERVICE_ID]
goto:eof

:doStart
"%EXECUTABLE%" //ES//%SERVICE_ID% %LOG_OPTS%
if not errorlevel 1 goto started
echo Failed starting '%SERVICE_ID%' service
goto:eof
:started
echo The service '%SERVICE_ID%' has been started
goto:eof

:doStop
"%EXECUTABLE%" //SS//%SERVICE_ID% %LOG_OPTS%
if not errorlevel 1 goto stopped
echo Failed stopping '%SERVICE_ID%' service
goto:eof
:stopped
echo The service '%SERVICE_ID%' has been stopped
goto:eof

:doManagment
set EXECUTABLE_MGR=%ES_HOME%\bin\elasticsearch-service-mgr.exe
"%EXECUTABLE_MGR%" //ES//%SERVICE_ID%
if not errorlevel 1 goto managed
echo Failed starting service manager for '%SERVICE_ID%'
goto:eof
:managed
echo Successfully started service manager for '%SERVICE_ID%'.
goto:eof

:doRemove
rem Remove the service
"%EXECUTABLE%" //DS//%SERVICE_ID% %LOG_OPTS%
if not errorlevel 1 goto removed
echo Failed removing '%SERVICE_ID%' service
goto:eof
:removed
echo The service '%SERVICE_ID%' has been removed
goto:eof

:doInstall
echo Installing service      :  "%SERVICE_ID%"
echo Using JAVA_HOME (%ARCH%):  "%JAVA_HOME%"

rem Check JVM server dll first
set JVM_DLL=%JAVA_HOME%\jre\bin\server\jvm.dll
if exist "%JVM_DLL%" goto foundJVM

rem Check 'server' JRE (JRE installed on Windows Server)
set JVM_DLL=%JAVA_HOME%\bin\server\jvm.dll
if exist "%JVM_DLL%" goto foundJVM

rem Fallback to 'client' JRE
set JVM_DLL=%JAVA_HOME%\bin\client\jvm.dll

if exist "%JVM_DLL%" (
echo Warning: JAVA_HOME points to a JRE and not JDK installation; a client (not a server^) JVM will be used...
) else (
echo JAVA_HOME points to an invalid Java installation (no jvm.dll found in "%JAVA_HOME%"^). Existing...
goto:eof
)

:foundJVM
if "%ES_MIN_MEM%" == "" set ES_MIN_MEM=${heap.min}
if "%ES_MAX_MEM%" == "" set ES_MAX_MEM=${heap.max}

if NOT "%ES_HEAP_SIZE%" == "" set ES_MIN_MEM=%ES_HEAP_SIZE%
if NOT "%ES_HEAP_SIZE%" == "" set ES_MAX_MEM=%ES_HEAP_SIZE%

call:convertxm %ES_MIN_MEM% JVM_XMS
call:convertxm %ES_MAX_MEM% JVM_XMX

REM java_opts might be empty - init to avoid tripping commons daemon (if the command starts with ;)
if "%JAVA_OPTS%" == "" set JAVA_OPTS=-XX:+UseParNewGC

CALL "%ES_HOME%\bin\elasticsearch.in.bat"

rem thread stack size
set JVM_SS=256

if "%DATA_DIR%" == "" set DATA_DIR=%ES_HOME%\data

if "%CONF_DIR%" == "" set CONF_DIR=%ES_HOME%\config

set ES_PARAMS=-Delasticsearch;-Des.path.home="%ES_HOME%";-Des.default.path.home="%ES_HOME%";-Des.default.path.logs="%LOG_DIR%";-Des.default.path.data="%DATA_DIR%";-Des.default.path.conf="%CONF_DIR%"

set JVM_OPTS=%JAVA_OPTS: =;%

if not "%ES_JAVA_OPTS%" == "" set JVM_ES_JAVA_OPTS=%ES_JAVA_OPTS: =#%
if not "%ES_JAVA_OPTS%" == "" set JVM_OPTS=%JVM_OPTS%;%JVM_ES_JAVA_OPTS%

if "%ES_START_TYPE%" == "" set ES_START_TYPE=manual
if "%ES_STOP_TIMEOUT%" == "" set ES_STOP_TIMEOUT=0

"%EXECUTABLE%" //IS//%SERVICE_ID% --Startup %ES_START_TYPE% --StopTimeout %ES_STOP_TIMEOUT% --StartClass org.elasticsearch.bootstrap.Elasticsearch --StopClass org.elasticsearch.bootstrap.Elasticsearch --StartMethod main --StopMethod close --Classpath "%ES_CLASSPATH%" --JvmSs %JVM_SS% --JvmMs %JVM_XMS% --JvmMx %JVM_XMX% --JvmOptions %JVM_OPTS% ++JvmOptions %ES_PARAMS% %LOG_OPTS% --PidFile "%SERVICE_ID%.pid" --DisplayName "Elasticsearch %ES_VERSION% (%SERVICE_ID%)" --Description "Elasticsearch %ES_VERSION% Windows Service - http://elasticsearch.org" --Jvm "%JVM_DLL%" --StartMode jvm --StopMode jvm --StartPath "%ES_HOME%" ++StartParams start


if not errorlevel 1 goto installed
echo Failed installing '%SERVICE_ID%' service
goto:eof

:installed
echo The service '%SERVICE_ID%' has been installed.
goto:eof

:err
echo JAVA_HOME environment variable must be set!
pause
goto:eof

rem ---
rem Function for converting Xm[s|x] values into MB which Commons Daemon accepts
rem ---
:convertxm
set value=%~1
rem extract last char (unit)
set unit=%value:~-1%
rem assume the unit is specified
set conv=%value:~0,-1%

if "%unit%" == "k" goto kilo
if "%unit%" == "K" goto kilo
if "%unit%" == "m" goto mega
if "%unit%" == "M" goto mega
if "%unit%" == "g" goto giga
if "%unit%" == "G" goto giga

rem no unit found, must be bytes; consider the whole value
set conv=%value%
rem convert to KB
set /a conv=%conv% / 1024
:kilo
rem convert to MB
set /a conv=%conv% / 1024
goto mega
:giga
rem convert to MB
set /a conv=%conv% * 1024
:mega
set "%~2=%conv%"
goto:eof

:conffileset
echo CONF_FILE setting is no longer supported. elasticsearch.yml must be placed in the config directory and cannot be renamed.
goto:eof

ENDLOCAL
