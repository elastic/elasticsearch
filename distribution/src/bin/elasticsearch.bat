@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

call "%~dp0elasticsearch-env.bat" || exit /b 1

rem use a small heap size for the CLI tools, and thus the serial collector to
rem avoid stealing many CPU cycles; a user can override by setting CLI_JAVA_OPTS
set CLI_JAVA_OPTS=-Xms4m -Xmx64m -XX:+UseSerialGC %CLI_JAVA_OPTS%

set "NATIVE_LAUNCHER=%ES_HOME%\lib\tools\server-launcher\server-launcher.exe"
set LAUNCHER_LIBS=%ES_HOME%/lib/tools/server-launcher/*

rem Prefer the native binary if present; fall back to the JVM-based launcher.
if exist "%NATIVE_LAUNCHER%" (
  "%NATIVE_LAUNCHER%" %*
) else (
  %JAVA% ^
    %CLI_JAVA_OPTS% ^
    -cp "%LAUNCHER_LIBS%" ^
    org.elasticsearch.server.launcher.ServerLauncher ^
    %*
)

exit /b %ERRORLEVEL%

endlocal
endlocal
