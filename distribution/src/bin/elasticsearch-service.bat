@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set SCRIPT_NAME=%~n0
set LAUNCHER_TOOLNAME=%SCRIPT_NAME:elasticsearch-=%
set LAUNCHER_LIBS=lib/tools/windows-service-cli
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || goto exit

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
