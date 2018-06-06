@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

call "%~dp0elasticsearch-cli.bat" ^
   org.elasticsearch.common.settings.KeyStoreCli ^
  %%* ^
  || exit /b 1

endlocal
endlocal
