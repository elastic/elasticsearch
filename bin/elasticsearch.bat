@echo off

SETLOCAL

if NOT DEFINED JAVA_HOME goto err

set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set ES_HOME=%%~dpfI


REM ***** JAVA options *****

set JAVA_OPTS=^
 -Xms256m^
 -Xmx1G^
 -Djline.enabled=false^
 -XX:+AggressiveOpts^
 -XX:+UseParNewGC^
 -XX:+UseConcMarkSweepGC^
 -XX:+CMSParallelRemarkEnabled^
 -XX:+HeapDumpOnOutOfMemoryError

set ES_CLASSPATH=$CLASSPATH;"%ES_HOME%/lib/*";"%ES_HOME%/lib/sigar/*"
set ES_PARAMS=-Delasticsearch -Des-foreground=yes -Des.path.home="%ES_HOME%"

"%JAVA_HOME%\bin\java" %JAVA_OPTS% %ES_JAVA_OPTS% %ES_PARAMS% -cp "%ES_CLASSPATH%" "org.elasticsearch.bootstrap.Bootstrap"
goto finally


:err
echo JAVA_HOME environment variable must be set!
pause


:finally

ENDLOCAL