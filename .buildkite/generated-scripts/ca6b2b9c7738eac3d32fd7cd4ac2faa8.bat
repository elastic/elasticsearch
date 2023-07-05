del /f /s /q %USERPROFILE%\.gradle\init.d\*.*
mkdir %USERPROFILE%\.gradle\init.d
copy .ci\init.gradle %USERPROFILE%\.gradle\init.d\
powershell.exe .\.ci\scripts\packaging-test.ps1 -GradleTasks help   || exit /b 1

