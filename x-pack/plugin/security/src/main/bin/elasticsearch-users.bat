@echo off

rem Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
rem or more contributor license agreements. Licensed under the Elastic License
rem 2.0; you may not use this file except in compliance with the Elastic License
rem 2.0.

setlocal enabledelayedexpansion
setlocal enableextensions

set ES_MAIN_CLASS=org.elasticsearch.xpack.security.authc.file.tool.UsersTool
set ES_ADDITIONAL_SOURCES=x-pack-env;x-pack-security-env
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || goto exit

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
