@echo off

setlocal enabledelayedexpansion

call "%~dp0elasticsearch-env.bat"

%JAVA% ^
  %ES_JAVA_OPTS% ^
  -Des.path.home="%ES_HOME%" ^
  -Des.path.conf="%CONF_DIR%" ^
  -cp "%ES_CLASSPATH%" ^
  org.elasticsearch.plugins.PluginCli ^
  %*

endlocal
