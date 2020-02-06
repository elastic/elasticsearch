@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

SET params='%*'
SET checkpassword=Y

:loop
FOR /F "usebackq tokens=1* delims= " %%A IN (!params!) DO (
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
	)
	IF "!current!" == "--help" (
		SET checkpassword=N
	)

	IF "!current!" == "-V" (
		SET checkpassword=N
	)
	IF "!current!" == "--version" (
		SET checkpassword=N
	)

	IF "!silent!" == "Y" (
		SET nopauseonerror=Y
	) ELSE (
	    IF "x!newparams!" NEQ "x" (
	        SET newparams=!newparams! !current!
        ) ELSE (
            SET newparams=!current!
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

if not defined ES_TMPDIR (
  for /f "tokens=* usebackq" %%a in (`CALL %JAVA% -cp "!ES_CLASSPATH!" "org.elasticsearch.tools.launchers.TempDirectory"`) do set  ES_TMPDIR=%%a
)

set ES_JVM_OPTIONS=%ES_PATH_CONF%\jvm.options
@setlocal
for /F "usebackq delims=" %%a in (`CALL %JAVA% -cp "!ES_CLASSPATH!" "org.elasticsearch.tools.launchers.JvmOptionsParser" "!ES_JVM_OPTIONS!" ^|^| echo jvm_options_parser_failed`) do set ES_JAVA_OPTS=%%a
@endlocal & set "MAYBE_JVM_OPTIONS_PARSER_FAILED=%ES_JAVA_OPTS%" & set ES_JAVA_OPTS=%ES_JAVA_OPTS%

if "%MAYBE_JVM_OPTIONS_PARSER_FAILED%" == "jvm_options_parser_failed" (
  exit /b 1
)

rem windows batch pipe will choke on special characters in strings
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^^=^^^^!
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^&=^^^&!
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^|=^^^|!
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^<=^^^<!
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^>=^^^>!
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^\=^^^\!

ECHO.!KEYSTORE_PASSWORD!| %JAVA% %ES_JAVA_OPTS% -Delasticsearch ^
  -Des.path.home="%ES_HOME%" -Des.path.conf="%ES_PATH_CONF%" ^
  -Des.distribution.flavor="%ES_DISTRIBUTION_FLAVOR%" ^
  -Des.distribution.type="%ES_DISTRIBUTION_TYPE%" ^
  -Des.bundled_jdk="%ES_BUNDLED_JDK%" ^
  -cp "%ES_CLASSPATH%" "org.elasticsearch.bootstrap.Elasticsearch" !newparams!

endlocal
endlocal
exit /b %ERRORLEVEL%
