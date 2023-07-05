del /f /s /q %USERPROFILE%\.gradle\init.d\*.*
mkdir %USERPROFILE%\.gradle\init.d
copy .ci\init.gradle %USERPROFILE%\.gradle\init.d\
call %GRADLEW_BAT% --max-workers=4 -Dbwc.checkout.align=true help || exit /b 1

