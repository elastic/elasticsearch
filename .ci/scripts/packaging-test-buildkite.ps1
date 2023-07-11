param($GradleTasks='destructiveDistroTest')

If (-NOT ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator))
{
    # Relaunch as an elevated process:
    Start-Process powershell.exe "-File",('"{0}"' -f $MyInvocation.MyCommand.Path) -Verb RunAs
    exit
}

$AppProps = ConvertFrom-StringData (Get-Content .ci/java-versions.properties -raw)
$env:ES_BUILD_JAVA=$AppProps.ES_BUILD_JAVA
$env:JAVA_TOOL_OPTIONS=''

$ErrorActionPreference="Stop"
$gradleInit = "$env:USERPROFILE\.gradle\init.d\"
echo "Remove $gradleInit"
Remove-Item -Recurse -Force $gradleInit -ErrorAction Ignore
New-Item -ItemType directory -Path $gradleInit
echo "Copy .ci/init.gradle to $gradleInit"
Copy-Item .ci/init.gradle -Destination $gradleInit

[Environment]::SetEnvironmentVariable("JAVA_HOME", $null, "Machine")
$env:PATH="$env:USERPROFILE\.java\$env:ES_BUILD_JAVA\bin\;$env:PATH"
$env:JAVA_HOME=$null
$env:SYSTEM_JAVA_HOME="$env:USERPROFILE\.java\$env:ES_BUILD_JAVA"
Remove-Item -Recurse -Force \tmp -ErrorAction Ignore
New-Item -ItemType directory -Path \tmp

$ErrorActionPreference="Continue"
bash -c "./gradlew -g '$env:USERPROFILE\.gradle' --parallel --no-daemon --scan --console=plain $GradleTasks"

exit $LastExitCode
