@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set ES_JAVA_OPTS=--add-opens java.base/sun.security.provider=ALL-UNNAMED %ES_JAVA_OPTS%
set ES_MAIN_CLASS=org.elasticsearch.plugins.PluginCli
set ES_ADDITIONAL_CLASSPATH_DIRECTORIES=lib/tools/plugin-cli
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || goto exit
  

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
