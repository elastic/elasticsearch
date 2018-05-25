@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

call "%~dp0elasticsearch-cli.bat" ^
  org.elasticsearch.plugins.PluginCli ^
  %* ^
  || exit /b 1

endlocal
endlocal
