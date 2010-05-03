@echo off

SETLOCAL

if NOT DEFINED JAVA_HOME goto err

set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set ES_HOME=%%~dpfI


set ES_CLASSPATH=$CLASSPATH;"%ES_HOME%/lib/*"
set ES_PARAMS=-Delasticsearch -Des.path.home="%ES_HOME%"

"%JAVA_HOME%\bin\java" %JAVA_OPTS% %ES_JAVA_OPTS% %ES_PARAMS% -cp "%ES_CLASSPATH%" "org.elasticsearch.plugins.PluginManager" %*
goto finally


:err
echo JAVA_HOME environment variable must be set!
pause


:finally

ENDLOCAL