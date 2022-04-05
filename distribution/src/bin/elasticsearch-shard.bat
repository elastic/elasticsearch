@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set LAUNCHER_TOOLNAME=shard
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || goto exit

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
