@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

SET params='%*'
SET checkpassword=Y
SET enrolltocluster=N
SET attemptautoconfig=Y

:loop
FOR /F "usebackq tokens=1* delims= " %%A IN (!params!) DO (
    SET previous=!current!
    SET current=%%A
    SET params='%%B'
	SET silent=N
	IF "!current!" == "-s" (
		SET silent=Y
	)
	IF "!current!" == "--silent" (
		SET silent=Y
	)

	IF "!current!" == "-h" (
		SET checkpassword=N
		SET attemptautoconfig=N
	)
	IF "!current!" == "--help" (
		SET checkpassword=N
		SET attemptautoconfig=N
	)

	IF "!current!" == "-V" (
		SET checkpassword=N
		SET attemptautoconfig=N
	)
	IF "!current!" == "--version" (
		SET checkpassword=N
		SET attemptautoconfig=N
	)

	IF "!current!" == "--enrollment-token" (
	    IF "!enrolltocluster!" == "Y" (
	        ECHO "Multiple --enrollment-token parameters are not allowed" 1>&2
	        goto exitwithone
	    )
		SET enrolltocluster=Y
		SET attemptautoconfig=N
	)

	IF "!previous!" == "--enrollment-token" (
		SET enrollmenttoken="!current!"
	)

	IF "!silent!" == "Y" (
		SET nopauseonerror=Y
	) ELSE (
	    SET SHOULD_SKIP=false
		IF "!previous!" == "--enrollment-token" SET SHOULD_SKIP=true
		IF "!current!" == "--enrollment-token" SET SHOULD_SKIP=true
		IF "!SHOULD_SKIP!" == "false" (
			IF "x!newparams!" NEQ "x" (
				SET newparams=!newparams! !current!
			) ELSE (
				SET newparams=!current!
			)
		)

	)

    IF "x!params!" NEQ "x" (
		GOTO loop
	)
)

CALL "%~dp0elasticsearch-env.bat" || exit /b 1
IF ERRORLEVEL 1 (
	IF NOT DEFINED nopauseonerror (
		PAUSE
	)
	EXIT /B %ERRORLEVEL%
)

SET KEYSTORE_PASSWORD=
IF "%checkpassword%"=="Y" (
  CALL "%~dp0elasticsearch-keystore.bat" has-passwd --silent
  IF !ERRORLEVEL! EQU 0 (
    SET /P KEYSTORE_PASSWORD=Elasticsearch keystore password:
    IF !ERRORLEVEL! NEQ 0 (
      ECHO Failed to read keystore password on standard input
      EXIT /B !ERRORLEVEL!
    )
  )
)

rem windows batch pipe will choke on special characters in strings
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^^=^^^^!
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^&=^^^&!
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^|=^^^|!
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^<=^^^<!
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^>=^^^>!
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^\=^^^\!

IF "%attemptautoconfig%"=="Y" (
    ECHO.!KEYSTORE_PASSWORD!| %JAVA% %ES_JAVA_OPTS% ^
      -Des.path.home="%ES_HOME%" ^
      -Des.path.conf="%ES_PATH_CONF%" ^
      -Des.distribution.flavor="%ES_DISTRIBUTION_FLAVOR%" ^
      -Des.distribution.type="%ES_DISTRIBUTION_TYPE%" ^
      -cp "!ES_CLASSPATH!;!ES_HOME!/lib/tools/security-cli/*;!ES_HOME!/modules/x-pack-core/*;!ES_HOME!/modules/x-pack-security/*" "org.elasticsearch.xpack.security.cli.AutoConfigureNode" !newparams!
    SET SHOULDEXIT=Y
    IF !ERRORLEVEL! EQU 0 SET SHOULDEXIT=N
    IF !ERRORLEVEL! EQU 73 SET SHOULDEXIT=N
    IF !ERRORLEVEL! EQU 78 SET SHOULDEXIT=N
    IF !ERRORLEVEL! EQU 80 SET SHOULDEXIT=N
    IF "!SHOULDEXIT!"=="Y" (
        exit /b !ERRORLEVEL!
    )
)

IF "!enrolltocluster!"=="Y" (
    ECHO.!KEYSTORE_PASSWORD!| %JAVA% %ES_JAVA_OPTS% ^
      -Des.path.home="%ES_HOME%" ^
      -Des.path.conf="%ES_PATH_CONF%" ^
      -Des.distribution.flavor="%ES_DISTRIBUTION_FLAVOR%" ^
      -Des.distribution.type="%ES_DISTRIBUTION_TYPE%" ^
      -cp "!ES_CLASSPATH!;!ES_HOME!/lib/tools/security-cli/*;!ES_HOME!/modules/x-pack-core/*;!ES_HOME!/modules/x-pack-security/*" "org.elasticsearch.xpack.security.cli.AutoConfigureNode" ^
      !newparams! --enrollment-token %enrollmenttoken%
	IF !ERRORLEVEL! NEQ 0 (
	    exit /b !ERRORLEVEL!
	)
)

if not defined ES_TMPDIR (
  for /f "tokens=* usebackq" %%a in (`CALL %JAVA% -cp "!LAUNCHERS_CLASSPATH!" "org.elasticsearch.tools.launchers.TempDirectory"`) do set  ES_TMPDIR=%%a
)

rem The JVM options parser produces the final JVM options to start
rem Elasticsearch. It does this by incorporating JVM options in the following
rem way:
rem   - first, system JVM options are applied (these are hardcoded options in
rem     the parser)
rem   - second, JVM options are read from jvm.options and
rem     jvm.options.d/*.options
rem   - third, JVM options from ES_JAVA_OPTS are applied
rem   - fourth, ergonomic JVM options are applied
@setlocal
for /F "usebackq delims=" %%a in (`CALL %JAVA% -cp "!LAUNCHERS_CLASSPATH!" "org.elasticsearch.tools.launchers.JvmOptionsParser" "!ES_PATH_CONF!" "!ES_HOME!"/plugins ^|^| echo jvm_options_parser_failed`) do set ES_JAVA_OPTS=%%a
@endlocal & set "MAYBE_JVM_OPTIONS_PARSER_FAILED=%ES_JAVA_OPTS%" & set ES_JAVA_OPTS=%ES_JAVA_OPTS%

if "%MAYBE_JVM_OPTIONS_PARSER_FAILED%" == "jvm_options_parser_failed" (
  exit /b 1
)

ECHO.!KEYSTORE_PASSWORD!| %JAVA% %ES_JAVA_OPTS% -Delasticsearch ^
  -Des.path.home="%ES_HOME%" -Des.path.conf="%ES_PATH_CONF%" ^
  -Des.distribution.flavor="%ES_DISTRIBUTION_FLAVOR%" ^
  -Des.distribution.type="%ES_DISTRIBUTION_TYPE%" ^
  -Des.bundled_jdk="%ES_BUNDLED_JDK%" ^
  -cp "%ES_CLASSPATH%" "org.elasticsearch.bootstrap.Elasticsearch" !newparams!

endlocal
endlocal
exit /b %ERRORLEVEL%

rem this hack is ugly but necessary because we can't exit with /b X from within the argument parsing loop.
rem exit 1 (without /b) would work for powershell but it will terminate the cmd process when run in cmd
:exitwithone
    exit /b 1
