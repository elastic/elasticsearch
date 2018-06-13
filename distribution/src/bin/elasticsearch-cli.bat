call "%~dp0elasticsearch-env.bat" || exit /b 1

if defined ES_ADDITIONAL_SOURCES (
  for %%a in ("%ES_ADDITIONAL_SOURCES:;=","%") do (
    call %~dp0%%a
  )
)

if defined ES_ADDITIONAL_CLASSPATH_DIRECTORIES (
  for %%a in ("%ES_ADDITIONAL_CLASSPATH_DIRECTORIES:;=","%") do (
    set ES_CLASSPATH=!ES_CLASSPATH!;!ES_HOME!/%%a/*
  )
)

%JAVA% ^
  %ES_JAVA_OPTS% ^
  -Des.path.home="%ES_HOME%" ^
  -Des.path.conf="%ES_PATH_CONF%" ^
  -Des.distribution.flavor="%ES_DISTRIBUTION_FLAVOR%" ^
  -Des.distribution.type="%ES_DISTRIBUTION_TYPE%" ^
  -cp "%ES_CLASSPATH%" ^
  "%ES_MAIN_CLASS%" ^
  %*
