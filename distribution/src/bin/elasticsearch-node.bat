@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set ES_MAIN_CLASS=org.elasticsearch.cluster.coordination.NodeToolCli
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || exit /b 1

endlocal
endlocal
