@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set CLI_JAVA_OPTS=--add-opens java.base/sun.security.provider=ALL-UNNAMED %CLI_JAVA_OPTS%
set CLI_SCRIPT=%~0
set CLI_LIBS=lib/tools/plugin-cli
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || goto exit


endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
