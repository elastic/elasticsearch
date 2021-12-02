@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set ES_MAIN_CLASS=org.elasticsearch.cli.keystore.KeyStoreCli
set ES_ADDITIONAL_CLASSPATH_DIRECTORIES=lib/tools/keystore-cli
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || goto exit

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
