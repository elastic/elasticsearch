@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set ES_MAIN_CLASS=org.elasticsearch.cli.readiness.ReadinessProbeCli
set ES_ADDITIONAL_CLASSPATH_DIRECTORIES=lib/tools/readiness-probe-cli
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || goto exit

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
