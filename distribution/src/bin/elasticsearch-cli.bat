@echo off

setlocal enabledelayedexpansion

call "%~dp0elasticsearch-env.bat" || exit /b 1

rem use a small heap size for the CLI tools, and thus the serial collector to
rem avoid stealing many CPU cycles; a user can override by setting CLI_JAVA_OPTS
set CLI_JAVA_OPTS=-Xms4m -Xmx64m -XX:+UseSerialGC %CLI_JAVA_OPTS%

set LAUNCHER_CLASSPATH=%ES_HOME%/lib/*;%ES_HOME%/lib/cli-launcher/*

%JAVA% ^
  %CLI_JAVA_OPTS% ^
  -Dcli.name="%CLI_NAME%" ^
  -Dcli.script="%CLI_SCRIPT%" ^
  -Dcli.libs="%CLI_LIBS%" ^
  -Des.path.home="%ES_HOME%" ^
  -Des.path.conf="%ES_PATH_CONF%" ^
  -Des.distribution.type="%ES_DISTRIBUTION_TYPE%" ^
  -Des.java.type="%JAVA_TYPE%" ^
  -cp "%LAUNCHER_CLASSPATH%" ^
  org.elasticsearch.launcher.CliToolLauncher ^
  %*

exit /b %ERRORLEVEL%

endlocal
