@echo off

SETLOCAL enabledelayedexpansion

if NOT DEFINED JAVA_HOME IF EXIST %ProgramData%\Oracle\java\javapath\java.exe (
    for /f "tokens=2 delims=[]" %%a in ('dir %ProgramData%\Oracle\java\javapath\java.exe') do @set JAVA_EXE=%%a
)
if DEFINED JAVA_EXE set JAVA_HOME=%JAVA_EXE:\bin\java.exe=%
if DEFINED JAVA_EXE (
    ECHO Using JAVA_HOME=%JAVA_HOME% retrieved from %ProgramData%\Oracle\java\javapath\java.exe
)
set JAVA_EXE=
if NOT DEFINED JAVA_HOME goto err

set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set ES_HOME=%%~dpfI

TITLE Elasticsearch Plugin Manager ${project.version}

SET properties=
SET args=

:loop
SET "current=%~1"
SHIFT
IF "x!current!" == "x" GOTO breakloop

IF "!current:~0,2%!" == "-D" (
    ECHO "!current!" | FINDSTR /C:"=">nul && (
         :: current matches -D*=*
         IF "x!properties!" NEQ "x" (
             SET properties=!properties! "!current!"
         ) ELSE (
             SET properties="!current!"
         )
    ) || (
         :: current matches -D*
         IF "x!properties!" NEQ "x" (
            SET properties=!properties! "!current!=%~1"
         ) ELSE (
            SET properties="!current!=%~1"
         )
         SHIFT
    )
) ELSE (
    :: current matches *
    IF "x!args!" NEQ "x" (
        SET args=!args! "!current!"
    ) ELSE (
        SET args="!current!"
    )
)

GOTO loop
:breakloop

SET HOSTNAME=%COMPUTERNAME%

"%JAVA_HOME%\bin\java" %ES_JAVA_OPTS% -Des.path.home="%ES_HOME%" !properties! -cp "%ES_HOME%/lib/*;" "org.elasticsearch.plugins.PluginCli" !args!
goto finally


:err
echo JAVA_HOME environment variable must be set!
pause


:finally

ENDLOCAL
