@if "%DEBUG%" == "" @echo off
@rem ##########################################################################
@rem                                                                         ##
@rem  Gradle startup script for Windows                                      ##
@rem                                                                         ##
@rem ##########################################################################

@rem
@rem $Revision: 10602 $ $Date: 2008-01-25 02:49:54 +0100 (ven., 25 janv. 2008) $
@rem

@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

@rem Uncomment those lines to set JVM options. GRADLE_OPTS and JAVA_OPTS can be used together.
set GRADLE_OPTS=%GRADLE_OPTS% -Xmx512m
@rem set JAVA_OPTS=%JAVA_OPTS% -Xmx512m

set DIRNAME=%~dp0
if "%DIRNAME%" == "" set DIRNAME=.\

@rem Determine the command interpreter to execute the "CD" later
set COMMAND_COM="cmd.exe"
if exist "%SystemRoot%\system32\cmd.exe" set COMMAND_COM="%SystemRoot%\system32\cmd.exe"
if exist "%SystemRoot%\command.com" set COMMAND_COM="%SystemRoot%\command.com"

@rem Use explicit find.exe to prevent cygwin and others find.exe from being used
set FIND_EXE="find.exe"
if exist "%SystemRoot%\system32\find.exe" set FIND_EXE="%SystemRoot%\system32\find.exe"
if exist "%SystemRoot%\command\find.exe" set FIND_EXE="%SystemRoot%\command\find.exe"

:check_JAVA_HOME
@rem Make sure we have a valid JAVA_HOME
if not "%JAVA_HOME%" == "" goto have_JAVA_HOME

echo.
echo ERROR: Environment variable JAVA_HOME has not been set.
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.
echo.
goto end

:have_JAVA_HOME
@rem Validate JAVA_HOME
%COMMAND_COM% /C DIR "%JAVA_HOME%" 2>&1 | %FIND_EXE% /I /C "%JAVA_HOME%" >nul
if not errorlevel 1 goto init

echo.
echo ERROR: JAVA_HOME might be set to an invalid directory: %JAVA_HOME%
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation if there are problems.
echo.

:init
@rem get name of script to launch with full path
@rem Get command-line arguments, handling Windowz variants
SET _marker=%JAVA_HOME: =%
@rem IF NOT "%_marker%" == "%JAVA_HOME%" ECHO JAVA_HOME "%JAVA_HOME%" contains spaces. Please change to a location without spaces if this causes problems.

if not "%OS%" == "Windows_NT" goto win9xME_args
if "%eval[2+2]" == "4" goto 4NT_args

IF "%_marker%" == "%JAVA_HOME%" goto :win9xME_args

set _FIXPATH=
call :fixpath "%JAVA_HOME%"
set JAVA_HOME=%_FIXPATH:~1%

goto win9xME_args

:fixpath
if not %1.==. (
for /f "tokens=1* delims=;" %%a in (%1) do (
call :shortfilename "%%a" & call :fixpath "%%b"
)
)
goto :EOF
:shortfilename
for %%i in (%1) do set _FIXPATH=%_FIXPATH%;%%~fsi
goto :EOF


:win9xME_args
@rem Slurp the command line arguments.
set CMD_LINE_ARGS=
set _SKIP=2

:win9xME_args_slurp
if "x%~1" == "x" goto execute

set CMD_LINE_ARGS=%*
goto execute

:4NT_args
@rem Get arguments from the 4NT Shell from JP Software
set CMD_LINE_ARGS=%$

:execute
@rem Setup the command line

set STARTER_MAIN_CLASS=org.gradle.wrapper.GradleWrapperMain
set CLASSPATH=%DIRNAME%\gradle\wrapper\gradle-wrapper.jar
set WRAPPER_PROPERTIES=%DIRNAME%\gradle\wrapper\gradle-wrapper.properties
set JAVA_EXE=%JAVA_HOME%\bin\java.exe

set GRADLE_OPTS=%JAVA_OPTS% %GRADLE_OPTS% -Dorg.gradle.wrapper.properties="%WRAPPER_PROPERTIES%"

"%JAVA_EXE%" %GRADLE_OPTS% -classpath "%CLASSPATH%" %STARTER_MAIN_CLASS% %CMD_LINE_ARGS%

:end
@rem End local scope for the variables with windows NT shell
if "%ERRORLEVEL%"=="0" goto mainEnd

if not "%OS%"=="Windows_NT" echo 1 > nul | choice /n /c:1

rem Set variable GRADLE_EXIT_CONSOLE if you need the _script_ return code instead of
rem the _cmd.exe /c_ return code!
if  not "" == "%GRADLE_EXIT_CONSOLE%" exit "%ERRORLEVEL%"
exit /b "%ERRORLEVEL%"

:mainEnd
if "%OS%"=="Windows_NT" endlocal

:omega