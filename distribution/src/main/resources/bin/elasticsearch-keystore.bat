@echo off

SETLOCAL enabledelayedexpansion

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

TITLE Elasticsearch Plugin Manager ${project.version}

SET path_props=-Des.path.home="%ES_HOME%"
IF DEFINED CONF_DIR (
  SET path_props=!path_props! -Des.path.conf="%CONF_DIR%"
)

SET args=%*
SET HOSTNAME=%COMPUTERNAME%

"%JAVA%" %ES_JAVA_OPTS% !path_props! -cp "%ES_HOME%/lib/*;" "org.elasticsearch.common.settings.KeyStoreCli" !args!

ENDLOCAL
