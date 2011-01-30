@if "%DEBUG%" == "" @echo off
@rem ##########################################################################
@rem                                                                         ##
@rem  Gradle startup script for Windows                                      ##
@rem                                                                         ##
@rem ##########################################################################

@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

@rem Uncomment those lines to set JVM options. GRADLE_OPTS and JAVA_OPTS can be used together.
set GRADLE_OPTS=%GRADLE_OPTS% -Xmx512m
@rem set JAVA_OPTS=%JAVA_OPTS% -Xmx512m

set DIRNAME=%~dp0
if "%DIRNAME%" == "" set DIRNAME=.\

@rem Find java.exe
set JAVA_EXE=java.exe
if not defined JAVA_HOME goto init

set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto init

echo.
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME%
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.
echo.
goto end

:init
@rem Get command-line arguments, handling Windowz variants

if not "%OS%" == "Windows_NT" goto win9xME_args
if "%eval[2+2]" == "4" goto 4NT_args

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

set GRADLE_OPTS=%JAVA_OPTS% %GRADLE_OPTS% -Dorg.gradle.wrapper.properties="%WRAPPER_PROPERTIES%"

@rem Execute Gradle
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