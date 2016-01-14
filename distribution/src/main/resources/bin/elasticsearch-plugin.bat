@echo off

SETLOCAL enabledelayedexpansion

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

"%JAVA_HOME%\bin\java" -client -Des.path.home="%ES_HOME%" !properties! -cp "%ES_HOME%/lib/*;" "org.elasticsearch.plugins.PluginManagerCliParser" !args!
goto finally


:err
echo JAVA_HOME environment variable must be set!
pause


:finally

ENDLOCAL
