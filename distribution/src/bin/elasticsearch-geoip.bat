@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set LAUNCHER_TOOLNAME=geoip
set LAUNCHER_LIBS=lib/tools/geoip-cli
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || goto exit

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
