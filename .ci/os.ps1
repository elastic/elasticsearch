If (-NOT ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator))
{
    # Relaunch as an elevated process:
    Start-Process powershell.exe "-File",('"{0}"' -f $MyInvocation.MyCommand.Path) -Verb RunAs
    exit
}

# CI configures these, uncoment if running manually 
#
# $env:ES_BUILD_JAVA="java12" 
#$env:ES_RUNTIME_JAVA="java11" 

$ErrorActionPreference="Stop"
$gradleInit = "C:\Users\$env:username\.gradle\init.d\"
Remove-Item -Recurse -Force $gradleInit -ErrorAction Ignore
New-Item -ItemType directory -Path $gradleInit

# Copy-Item .ci/init.gradle -Destination $gradleInit

[Environment]::SetEnvironmentVariable("JAVA_HOME", $null, "Machine")
$env:PATH="C:\Users\jenkins\.java\$env:ES_BUILD_JAVA\bin\;$env:PATH"
$env:JAVA_HOME=$null
$env:SYSTEM_JAVA_HOME="C:\Users\jenkins\.java\$env:ES_RUNTIME_JAVA"
Remove-Item -Recurse -Force \tmp -ErrorAction Ignore
New-Item -ItemType directory -Path \tmp

$ErrorActionPreference="Continue"
& .\gradlew.bat --parallel --scan --console=plain destructiveDistroTest

exit $?
