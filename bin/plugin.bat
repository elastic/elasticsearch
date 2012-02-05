@echo off

SETLOCAL

if NOT DEFINED JAVA_HOME goto err

set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set ES_HOME=%%~dpfI

set EXT_DIR=%ES_HOME%\lib;%JAVA_HOME%\lib;%JAVA_HOME%\lib\ext;%JAVA_HOME%\jre\lib\ext
"%JAVA_HOME%\bin\java" -Xmx64m -Xms16m -Des.path.home="%ES_HOME%" -Djava.ext.dirs="%EXT_DIR%" "org.elasticsearch.plugins.PluginManager" %*
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause


:finally

ENDLOCAL
