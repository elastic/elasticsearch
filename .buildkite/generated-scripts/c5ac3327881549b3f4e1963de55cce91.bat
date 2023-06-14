del /f /s /q %USERPROFILE%\.gradle\init.d\*.*
mkdir %USERPROFILE%\.gradle\init.d
copy .ci\init.gradle %USERPROFILE%\.gradle\init.d\
(
   echo powershell.exe .\.ci\scripts\packaging-test.ps1 -GradleTasks destructiveDistroTest.%PACKAGING_TASK%   ^|^| exit /b 1
) 
