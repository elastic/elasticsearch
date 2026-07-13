@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

call "%~dp0elasticsearch-env.bat" || exit /b 1

rem use a small heap size for the CLI tools, and thus the serial collector to
rem avoid stealing many CPU cycles; a user can override by setting CLI_JAVA_OPTS
set CLI_JAVA_OPTS=-Xms4m -Xmx64m -XX:+UseSerialGC %CLI_JAVA_OPTS%

%JAVA% ^
  %CLI_JAVA_OPTS% ^
  -cp "%ES_HOME%\lib\tools\server-launcher\*" ^
  org.elasticsearch.server.launcher.ServerLauncher ^
  %*

endlocal
endlocal
exit /b %ERRORLEVEL%
