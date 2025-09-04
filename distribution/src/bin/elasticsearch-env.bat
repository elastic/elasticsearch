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
set ES_MODULEPATH=!ES_HOME!\lib
set LAUNCHERS_CLASSPATH=!ES_CLASSPATH!;!ES_HOME!\lib\launchers\*;!ES_HOME!\lib\java-version-checker\*
set SERVER_CLI_CLASSPATH=!ES_CLASSPATH!;!ES_HOME!\lib\tools\server-cli\*

set HOSTNAME=%COMPUTERNAME%

if not defined ES_PATH_CONF (
  set ES_PATH_CONF=!ES_HOME!\config
)

rem now make ES_PATH_CONF absolute
for %%I in ("%ES_PATH_CONF%..") do set ES_PATH_CONF=%%~dpfI

set ES_DISTRIBUTION_TYPE=@es.distribution.type@

cd /d "%ES_HOME%"

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

  if not exist !JAVA! (
    echo "could not find java in !JAVA_TYPE! at !JAVA!" >&2
    exit /b 1
  )

  rem check the user supplied jdk version
  !JAVA! -cp "%ES_HOME%\lib\java-version-checker\*" "org.elasticsearch.tools.java_version_checker.JavaVersionChecker" || exit /b 1
) else (
  rem use the bundled JDK (default)
  set JAVA="%ES_HOME%\jdk\bin\java.exe"
  set "ES_JAVA_HOME=%ES_HOME%\jdk"
  set JAVA_TYPE=bundled JDK
)

rem do not let JAVA_TOOL_OPTIONS or _JAVA_OPTIONS slip in (as the JVM does by default)
if defined JAVA_TOOL_OPTIONS (
  (echo|set /p=ignoring JAVA_TOOL_OPTIONS=%JAVA_TOOL_OPTIONS%; )
  echo pass JVM parameters via ES_JAVA_OPTS
  set JAVA_TOOL_OPTIONS=
)
if defined _JAVA_OPTIONS (
  (echo|set /p=ignoring _JAVA_OPTIONS=%_JAVA_OPTIONS%; )
  echo pass JVM parameters via ES_JAVA_OPTS
  set _JAVA_OPTIONS=
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

cd %ES_HOME%
