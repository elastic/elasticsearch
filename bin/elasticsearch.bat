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

CALL "%~dp0elasticsearch.in.bat"
IF ERRORLEVEL 1 (
	IF NOT DEFINED nopauseonerror (
		PAUSE
	)
	EXIT /B %ERRORLEVEL%
)

"%JAVA_HOME%\bin\java" %JAVA_OPTS% %ES_JAVA_OPTS% %ES_PARAMS% !newparams! -cp "%ES_CLASSPATH%" "org.elasticsearch.bootstrap.Elasticsearch"

ENDLOCAL
