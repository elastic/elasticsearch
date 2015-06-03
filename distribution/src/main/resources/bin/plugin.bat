@echo off

SETLOCAL

if NOT DEFINED JAVA_HOME goto err

set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set ES_HOME=%%~dpfI

TITLE Elasticsearch Plugin Manager ${project.version}

SET HOSTNAME=%COMPUTERNAME%

"%JAVA_HOME%\bin\java" %JAVA_OPTS% %ES_JAVA_OPTS% -Xmx64m -Xms16m -Des.path.home="%ES_HOME%" -cp "%ES_HOME%/lib/*;" "org.elasticsearch.plugins.PluginManagerCliParser" %*
goto finally


:err
echo JAVA_HOME environment variable must be set!
pause


:finally

ENDLOCAL
