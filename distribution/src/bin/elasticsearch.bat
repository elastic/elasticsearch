@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set CLI_NAME=server
set CLI_LIBS=lib/tools/server-cli
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || goto exit

:exit
exit /b %ERRORLEVEL%
