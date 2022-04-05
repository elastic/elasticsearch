set SCRIPT=%0

rem determine Elasticsearch home; to do this, we strip from the path until we
rem find bin, and then strip bin (there is an assumption here that there is no
rem nested directory under bin also named bin)
for %%I in (%SCRIPT%) do set ES_HOME=%%~dpI

:es_home_loop
for %%I in ("%ES_HOME:~1,-1%") do set DIRNAME=%%~nxI
if not "%DIRNAME%" == "bin" (
  for %%I in ("%ES_HOME%..") do set ES_HOME=%%~dpfI
  goto es_home_loop
)
for %%I in ("%ES_HOME%..") do set ES_HOME=%%~dpfI

cd /d "%ES_HOME%"

@source.path.env@

if not exist %ES_PATH_CONF% (
  echo "ES_PATH_CONF must be set to the configuration path"
  exit /b 1
)

rem now set the path to java, pass "nojava" arg to skip setting ES_JAVA_HOME and JAVA
if "%1" == "nojava" (
   exit /b
)

rem comparing to empty string makes this equivalent to bash -v check on env var
rem and allows to effectively force use of the bundled jdk when launching ES
rem by setting ES_JAVA_HOME=
if defined ES_JAVA_HOME (
  set JAVA="%ES_JAVA_HOME%\bin\java.exe"
  set JAVA_TYPE=ES_JAVA_HOME
) else (
  rem use the bundled JDK (default)
  set JAVA="%ES_HOME%\jdk\bin\java.exe"
  set "ES_JAVA_HOME=%ES_HOME%\jdk"
  set JAVA_TYPE=bundled JDK
)

if not exist !JAVA! (
  echo "could not find java in !JAVA_TYPE! at !JAVA!" >&2
  exit /b 1
)

rem do not let JAVA_TOOL_OPTIONS slip in (as the JVM does by default)
if defined JAVA_TOOL_OPTIONS (
  echo warning: ignoring JAVA_TOOL_OPTIONS=%JAVA_TOOL_OPTIONS%
  set JAVA_TOOL_OPTIONS=
)

rem warn that we are not observing the value of $JAVA_HOME
if defined JAVA_HOME (
  echo warning: ignoring JAVA_HOME=%JAVA_HOME%; using %JAVA_TYPE% >&2
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
