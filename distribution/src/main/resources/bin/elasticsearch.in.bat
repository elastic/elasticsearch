@echo off

IF DEFINED JAVA_HOME (
  set JAVA=%JAVA_HOME%\bin\java.exe
) ELSE (
  FOR %%I IN (java.exe) DO set JAVA=%%~$PATH:I
)
IF NOT EXIST "%JAVA%" (
  ECHO Could not find any executable java binary. Please install java in your PATH or set JAVA_HOME 1>&2
  EXIT /B 1
)

set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set ES_HOME=%%~dpfI

REM check in case a user was using this mechanism
if "%ES_CLASSPATH%" == "" (
set ES_CLASSPATH=!ES_HOME!/lib/elasticsearch-${project.version}.jar;!ES_HOME!/lib/*
) else (
ECHO Error: Don't modify the classpath with ES_CLASSPATH, Best is to add 1>&2
ECHO additional elements via the plugin mechanism, or if code must really be 1>&2
ECHO added to the main classpath, add jars to lib\, unsupported 1>&2
EXIT /B 1
)
set ES_PARAMS=-Delasticsearch -Des.path.home="%ES_HOME%"
