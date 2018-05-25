@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

call "%~dp0elasticsearch-cli.bat" ^
  org.elasticsearch.index.translog.TranslogToolCli ^
  %* ^
  || exit /b 1

endlocal
endlocal
