@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set SCRIPT_NAME=%0
set LAUNCHER_TOOLNAME=%SCRIPT_NAME:elasticsearch-=%
set LAUNCHER_LIBS=lib/tools/geoip-cli
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || goto exit

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
