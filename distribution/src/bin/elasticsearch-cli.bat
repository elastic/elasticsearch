call "%~dp0elasticsearch-env.bat" || exit /b 1

@source.path.env@

if not exist %ES_PATH_CONF% (
  echo "ES_PATH_CONF must be set to the configuration path"
  exit /b 1
)

set ES_DISTRIBUTION_FLAVOR=@es.distribution.flavor@
set ES_DISTRIBUTION_TYPE=@es.distribution.type@
set ES_BUNDLED_JDK=@es.bundled_jdk@
set LAUNCHER_CLASSPATH=%ES_HOME%/lib/*;%ES_HOME%/lib/launcher/*

%JAVA% ^
  %LAUNCHER_JAVA_OPTS% ^
  -Des.path.home="%ES_HOME%" ^
  -Des.path.conf="%ES_PATH_CONF%" ^
  -Des.distribution.flavor="%ES_DISTRIBUTION_FLAVOR%" ^
  -Des.distribution.type="%ES_DISTRIBUTION_TYPE%" ^
  -Des.bundled_jdk="%ES_BUNDLED_JDK%" ^
  -cp "%LAUNCHER_CLASSPATH%" ^
  "org.elasticsearch.launcher.Launcher" ^
  %*

exit /b %ERRORLEVEL%
