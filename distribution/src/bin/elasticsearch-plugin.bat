@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set LAUNCHER_TOOLNAME=plugin
set LAUNCHER_LIBS=lib/tools/plugin-cli
set LAUNCHER_JAVA_OPTS=--add-opens java.base/sun.security.provider=ALL-UNNAMED %ES_JAVA_OPTS%
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || goto exit


endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
