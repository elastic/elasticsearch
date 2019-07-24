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

set HOSTNAME=%COMPUTERNAME%

if not defined ES_PATH_CONF (
  set ES_PATH_CONF=!ES_HOME!\config
)

rem now make ES_PATH_CONF absolute
for %%I in ("%ES_PATH_CONF%..") do set ES_PATH_CONF=%%~dpfI

set ES_DISTRIBUTION_FLAVOR=${es.distribution.flavor}
set ES_DISTRIBUTION_TYPE=${es.distribution.type}
set ES_BUNDLED_JDK=${es.bundled_jdk}

cd /d "%ES_HOME%"

rem now set the path to java, pass "nojava" arg to skip setting JAVA_HOME and JAVA
if "%1" == "nojava" (
   exit /b
)

if defined JAVA_HOME (
  set JAVA="%JAVA_HOME%\bin\java.exe"
) else (
  set JAVA="%ES_HOME%\jdk\bin\java.exe"
  set JAVA_HOME="%ES_HOME%\jdk"
)

if not exist %JAVA% (
  echo "could not find java in JAVA_HOME or bundled at %ES_HOME%\jdk" >&2
  exit /b 1
)

rem do not let JAVA_TOOL_OPTIONS slip in (as the JVM does by default)
if defined JAVA_TOOL_OPTIONS (
  echo warning: ignoring JAVA_TOOL_OPTIONS=%JAVA_TOOL_OPTIONS%
  set JAVA_TOOL_OPTIONS=
)

rem JAVA_OPTS is not a built-in JVM mechanism but some people think it is so we
rem warn them that we are not observing the value of %JAVA_OPTS%
if defined JAVA_OPTS (
  (echo|set /p=warning: ignoring JAVA_OPTS=%JAVA_OPTS%; )
  echo pass JVM parameters via ES_JAVA_OPTS
)

rem check the Java version
%JAVA% -cp "%ES_CLASSPATH%" "org.elasticsearch.tools.java_version_checker.JavaVersionChecker" || exit /b 1

