@echo off

SETLOCAL enabledelayedexpansion

IF DEFINED JAVA_HOME (
  set JAVA=%JAVA_HOME%\bin\java.exe
) ELSE (
  FOR %%I IN (java.exe) DO set JAVA=%%~$PATH:I
)
IF NOT EXIST "%JAVA%" (
  ECHO Could not find any executable java binary. Please install java in your PATH or set JAVA_HOME 1>&2
  EXIT /B 1
)

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

"%JAVA%" %ES_JAVA_OPTS% -Des.path.home="%ES_HOME%" !properties! -cp "%ES_HOME%/lib/*;" "org.elasticsearch.index.translog.TranslogToolCli" !args!

ENDLOCAL
