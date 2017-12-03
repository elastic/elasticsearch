@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

call "%~dp0elasticsearch-env.bat" || exit /b 1

%JAVA% ^
  %ES_JAVA_OPTS% ^
  -Des.path.home="%ES_HOME%" ^
  -Des.path.conf="%ES_PATH_CONF%" ^
  -cp "%ES_CLASSPATH%" ^
  org.elasticsearch.plugins.PluginCli ^
  %*

endlocal
endlocal
