@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set LAUNCHER_TOOLNAME=windows-service
set LAUNCHER_LIBS=lib/tools/windows-service-cli
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || goto exit

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
