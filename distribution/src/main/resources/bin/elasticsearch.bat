@echo off

SETLOCAL enabledelayedexpansion
TITLE Elasticsearch ${project.version}

SET params='%*'

rem TODO: remove for Elasticsearch 6.x
set bad_env_var=0
if not "%ES_MIN_MEM%" == "" set bad_env_var=1
if not "%ES_MAX_MEM%" == "" set bad_env_var=1
if not "%ES_HEAP_SIZE%" == "" set bad_env_var=1
if not "%ES_HEAP_NEWSIZE%" == "" set bad_env_var=1
if not "%ES_DIRECT_SIZE%" == "" set bad_env_var=1
if not "%ES_USE_IPV4%" == "" set bad_env_var=1
if not "%ES_GC_OPTS%" == "" set bad_env_var=1
if not "%ES_GC_LOG_FILE%" == "" set bad_env_var=1
if %bad_env_var% == 1 (
    echo Error: encountered environment variables that are no longer supported
    echo Use jvm.options or ES_JAVA_OPTS to configure the JVM
    if not "%ES_MIN_MEM%" == "" echo ES_MIN_MEM=%ES_MIN_MEM%: set -Xms%ES_MIN_MEM% in jvm.options or add "-Xms%ES_MIN_MEM%" to ES_JAVA_OPTS
    if not "%ES_MAX_MEM%" == "" echo ES_MAX_MEM=%ES_MAX_MEM%: set -Xmx%ES_MAX_MEM% in jvm.options or add "-Xmx%ES_MAX_MEM%" to ES_JAVA_OPTS
    if not "%ES_HEAP_SIZE%" == "" echo ES_HEAP_SIZE=%ES_HEAP_SIZE%: set -Xms%ES_HEAP_SIZE% and -Xmx%ES_HEAP_SIZE% in jvm.options or add "-Xms%ES_HEAP_SIZE% -Xmx%ES_HEAP_SIZE%" to ES_JAVA_OPTS
    if not "%ES_HEAP_NEWSIZE%" == "" echo ES_HEAP_NEWSIZE=%ES_HEAP_NEWSIZE%: set -Xmn%ES_HEAP_NEWSIZE% in jvm.options or add "-Xmn%ES_HEAP_SIZE%" to ES_JAVA_OPTS
    if not "%ES_DIRECT_SIZE%" == "" echo ES_DIRECT_SIZE=%ES_DIRECT_SIZE%: set -XX:MaxDirectMemorySize=%ES_DIRECT_SIZE% in jvm.options or add "-XX:MaxDirectMemorySize=%ES_DIRECT_SIZE%" to ES_JAVA_OPTS
    if not "%ES_USE_IPV4%" == "" echo ES_USE_IPV4=%ES_USE_IPV4%: set -Djava.net.preferIPv4Stack=true in jvm.options or add "-Djava.net.preferIPv4Stack=true" to ES_JAVA_OPTS
    if not "%ES_GC_OPTS%" == "" echo ES_GC_OPTS=%ES_GC_OPTS%: set %ES_GC_OPTS: = and % in jvm.options or add "%ES_GC_OPTS%" to ES_JAVA_OPTS
    if not "%ES_GC_LOG_FILE%" == "" echo ES_GC_LOG_FILE=%ES_GC_LOG_FILE%: set -Xloggc:%ES_GC_LOG_FILE% in jvm.options or add "-Xloggc:%ES_GC_LOG_FILE%" to ES_JAVA_OPTS"
    exit /b 1
)
rem end TODO: remove for Elasticsearch 6.x

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
set ES_JVM_OPTIONS="%~dp0\..\config\jvm.options"
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

"%JAVA%" %ES_JAVA_OPTS% %ES_PARAMS% -cp "%ES_CLASSPATH%" "org.elasticsearch.bootstrap.Elasticsearch" !newparams!

ENDLOCAL
