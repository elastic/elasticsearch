@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set CLI_NAME=windows-service
set CLI_LIBS=lib/tools/windows-service-cli
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || goto exit

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
