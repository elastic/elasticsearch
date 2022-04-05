@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set SCRIPT_NAME=%0
set LAUNCHER_TOOLNAME=%SCRIPT_NAME:elasticsearch-=%
set LAUNCHER_LIBS=lib/tools/plugin-cli
set LAUNCHER_JAVA_OPTS=--add-opens java.base/sun.security.provider=ALL-UNNAMED %ES_JAVA_OPTS%
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || goto exit


endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
