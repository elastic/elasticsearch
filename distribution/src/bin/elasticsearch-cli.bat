call "%~dp0elasticsearch-env.bat" || exit /b 1

if defined ES_ADDITIONAL_SOURCES (
  for %%a in ("%ES_ADDITIONAL_SOURCES:;=","%") do (
    call "%~dp0%%a"
  )
)

if defined ES_ADDITIONAL_CLASSPATH_DIRECTORIES (
  for %%a in ("%ES_ADDITIONAL_CLASSPATH_DIRECTORIES:;=","%") do (
    set ES_CLASSPATH=!ES_CLASSPATH!;!ES_HOME!/%%a/*
  )
)

rem use a small heap size for the CLI tools, and thus the serial collector to
rem avoid stealing many CPU cycles; a user can override by setting ES_JAVA_OPTS
set ES_JAVA_OPTS=-Xms4m -Xmx64m -XX:+UseSerialGC %ES_JAVA_OPTS%

%JAVA% ^
  %ES_JAVA_OPTS% ^
  -Des.path.home="%ES_HOME%" ^
  -Des.path.conf="%ES_PATH_CONF%" ^
  -Des.distribution.flavor="%ES_DISTRIBUTION_FLAVOR%" ^
  -Des.distribution.type="%ES_DISTRIBUTION_TYPE%" ^
  -cp "%ES_CLASSPATH%" ^
  "%ES_MAIN_CLASS%" ^
  %*
  
exit /b %ERRORLEVEL%
