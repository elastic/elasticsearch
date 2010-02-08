@echo off

SETLOCAL

if NOT DEFINED JAVA_HOME goto err

set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set ES_HOME=%%~dpfI


REM ***** JAVA options *****

set JAVA_OPTS=^
 -Xms128m^
 -Xmx1G^
 -Djline.enabled=false^
 -Djava.net.preferIPv4Stack=true^
 -XX:+AggressiveOpts^
 -XX:+UseParNewGC^
 -XX:+UseConcMarkSweepGC^
 -XX:+CMSParallelRemarkEnabled^
 -XX:+HeapDumpOnOutOfMemoryError

set ES_CLASSPATH=$CLASSPATH;"%ELASTICSEARCH_HOME%/lib/*"
set ES_PARAMS=-Delasticsearch -Des-foreground=yes -Des.path.home="%ES_HOME%"

"%JAVA_HOME%\bin\java" %JAVA_OPTS% %ES_JAVA_OPTS% %ES_PARAMS% -cp "%ES_CLASSPATH%" "org.elasticsearch.bootstrap.Bootstrap"
goto finally


:err
echo JAVA_HOME environment variable must be set!
pause


:finally

ENDLOCAL