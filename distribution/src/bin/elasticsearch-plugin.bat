@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set ES_ADDITIONAL_CLASSPATH_DIRECTORIES=lib/tools/plugin-cli
call "%~dp0elasticsearch-cli.bat" ^
  org.elasticsearch.plugins.PluginCli ^
  %* ^
  || exit /b 1

endlocal
endlocal
