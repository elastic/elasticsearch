@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set ES_MAIN_CLASS=org.elasticsearch.plugins.PluginCli
set ES_ADDITIONAL_CLASSPATH_DIRECTORIES=lib/tools/plugin-cli
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || goto exit
  

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
