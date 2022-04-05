@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set LAUNCHER_TOOLNAME=keystore
set LAUNCHER_LIBS=lib/tools/keystore-cli
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || goto exit

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
