@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set ES_MAIN_CLASS=org.elasticsearch.readiness.ReadinessCheckCli
set ES_ADDITIONAL_CLASSPATH_DIRECTORIES=lib/tools/readiness-check-cli
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || goto exit

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
