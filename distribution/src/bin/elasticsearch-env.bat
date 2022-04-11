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

rem now set the classpath
set ES_CLASSPATH=!ES_HOME!\lib\*
set LAUNCHERS_CLASSPATH=!ES_CLASSPATH!;!ES_HOME!\lib\launchers\*

set HOSTNAME=%COMPUTERNAME%

if not defined ES_PATH_CONF (
  set ES_PATH_CONF=!ES_HOME!\config
)

rem now make ES_PATH_CONF absolute
for %%I in ("%ES_PATH_CONF%..") do set ES_PATH_CONF=%%~dpfI

set ES_DISTRIBUTION_FLAVOR=@es.distribution.flavor@
set ES_DISTRIBUTION_TYPE=@es.distribution.type@
set ES_BUNDLED_JDK=@es.bundled_jdk@

if "%ES_BUNDLED_JDK%" == "false" (
  echo "warning: no-jdk distributions that do not bundle a JDK are deprecated and will be removed in a future release" >&2
)

cd /d "%ES_HOME%"

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

rem JAVA_OPTS is not a built-in JVM mechanism but some people think it is so we
rem warn them that we are not observing the value of %JAVA_OPTS%
if defined JAVA_OPTS (
  (echo|set /p=warning: ignoring JAVA_OPTS=%JAVA_OPTS%; )
  echo pass JVM parameters via ES_JAVA_OPTS
)

rem check the Java version
%JAVA% -cp "%LAUNCHERS_CLASSPATH%" "org.elasticsearch.tools.java_version_checker.JavaVersionChecker" || exit /b 1


if "%ES_DISTRIBUTION_TYPE%" == "docker" (
  rem # Allow environment variables to be set by creating a file with the
  rem # contents, and setting an environment variable with the suffix _FILE to
  rem # point to it. This can be used to provide secrets to a container, without
  rem # the values being specified explicitly when running the container.
  rem source "$ES_HOME/bin/elasticsearch-env-from-file"

  rem # Parse Docker env vars to customize Elasticsearch
  rem #
  rem # e.g. Setting the env var cluster.name=testcluster
  rem #
  rem # will cause Elasticsearch to be invoked with -Ecluster.name=testcluster
  rem #
  rem # see https://www.elastic.co/guide/en/elasticsearch/reference/current/settings.html#_setting_default_settings

  rem A very important var is exposed to this script : newparams
  rem It contains all additional args passed to the CLI

  rem For an unknown reason we seems pass multiple times to this script
  rem we avoid duplicated parameters
  if "%_es_config_params%" == "" (
    rem # Elasticsearch settings need to have at least two dot separated lowercase
    rem # words, e.g. `cluster.name`
    rem copy all env vars xxx.xxxx as command line format (-Ecluster.name=xxxx) into es_arg_array
    rem Be careful !!! findstr doesn't implement regex char "+"
    for /f "delims== tokens=1,2" %%a in ('set ^| findstr /i /r "^[a-z0-9_]*\.[a-z0-9_]*"') do (
        SET _es_config_params=!_es_config_params! -E%%a=%%b
    )

    SET newparams=!newparams!!_es_config_params!
  )
)
