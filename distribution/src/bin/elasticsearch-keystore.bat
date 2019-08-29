@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set ES_MAIN_CLASS=org.elasticsearch.common.settings.KeyStoreCli
set ES_ADDITIONAL_CLASSPATH_DIRECTORIES=lib/tools/keystore-cli
call "%~dp0elasticsearch-cli.bat" ^
  %%*

IF ERRORLEVEL 1 (
  EXIT /B 1
)

endlocal
endlocal
