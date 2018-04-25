@echo off

rem
rem Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
rem or more contributor license agreements. Licensed under the Elastic License;
rem you may not use this file except in compliance with the Elastic License.
rem

setlocal enabledelayedexpansion
setlocal enableextensions

set SCRIPT=%0
for %%I in (%SCRIPT%) do set DEPRECATED_DIRECTORY=%%~dpI
set DEPRECATED_PATH=%DEPRECATED_DIRECTORY%certutil.bat
for %%I in ("%DEPRECATED_DIRECTORY%..") do set ELASTICSEARCH_PATH=%%~dpfI\elasticsearch-certutil.bat
echo %DEPRECATED_PATH% is deprecated, use %ELASTICSEARCH_PATH%

call "%ELASTICSEARCH_PATH%" || exit /b 1

endlocal
endlocal
