@echo off

SETLOCAL enabledelayedexpansion
TITLE Elasticsearch ${project.version}

SET params='%*'

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

SET HOSTNAME=%COMPUTERNAME%

if "%ES_JVM_OPTIONS%" == "" (
rem '0' is the batch file, '~dp' appends the drive and path
set ES_JVM_OPTIONS=%~dp0\..\config\jvm.options
)

@setlocal
rem extract the options from the JVM options file %ES_JVM_OPTIONS%
rem such options are the lines beginning with '-', thus "findstr /b"
for /F "usebackq delims=" %%a in (`findstr /b \- "%ES_JVM_OPTIONS%"`) do set JVM_OPTIONS=!JVM_OPTIONS! %%a
@endlocal & set ES_JAVA_OPTS=%JVM_OPTIONS% %ES_JAVA_OPTS%

CALL "%~dp0elasticsearch.in.bat"
IF ERRORLEVEL 1 (
	IF NOT DEFINED nopauseonerror (
		PAUSE
	)
	EXIT /B %ERRORLEVEL%
)

%JAVA% %ES_JAVA_OPTS% %ES_PARAMS% -cp "%ES_CLASSPATH%" "org.elasticsearch.bootstrap.Elasticsearch" !newparams!

ENDLOCAL
