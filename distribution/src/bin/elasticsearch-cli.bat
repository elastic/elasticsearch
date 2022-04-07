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

rem use a small heap size for the CLI tools, and thus the serial collector to
rem avoid stealing many CPU cycles; a user can override by setting LAUNCHER_JAVA_OPTS
set LAUNCHER_JAVA_OPTS=-Xms4m -Xmx64m -XX:+UseSerialGC %LAUNCHER_JAVA_OPTS%

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
