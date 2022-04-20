@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set LAUNCHER_TOOLNAME=server
set LAUNCHER_LIBS=lib/tools/server-cli
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || goto exit

:exit
exit /b %ERRORLEVEL%
