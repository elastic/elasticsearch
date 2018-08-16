@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set ES_MAIN_CLASS=org.elasticsearch.common.settings.KeyStoreCli
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || exit /b 1

endlocal
endlocal
