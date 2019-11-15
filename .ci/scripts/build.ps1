$ErrorActionPreference="Stop"

$gradleInit = "C:\Users\$env:username\.gradle\init.d\"
echo "Remove $gradleInit"
Remove-Item -Recurse -Force $gradleInit -ErrorAction Ignore
New-Item -ItemType directory -Path $gradleInit
echo "Copy .ci/init.gradle to $gradleInit"
Copy-Item .ci/init.gradle -Destination $gradleInit

$env:JAVA7_HOME='C:\Users\jenkins\.java\java7'
$env:JAVA8_HOME='C:\Users\jenkins\.java\java8'
$env:JAVA9_HOME='C:\Users\jenkins\.java\java9'
$env:JAVA10_HOME='C:\Users\jenkins\.java\java10'
$env:JAVA11_HOME='C:\Users\jenkins\.java\java11'
$env:JAVA12_HOME='C:\Users\jenkins\.java\java12'

if (-not (Test-Path env:JAVA_HOME)) { $env:JAVA_HOME = $env:JAVA12_HOME }
if (-not (Test-Path env:RUNTIME_JAVA_HOME)) { $env:RUNTIME_JAVA_HOME = $env:JAVA11_HOME }

$ErrorActionPreference="Continue"
& .\gradlew.bat `
  -g "C:\Users\$env:username\.gradle" `
  --parallel --max-workers=12 `
  -Dorg.elasticsearch.build.cache.url=https://gradle-enterprise.elastic.co/cache/ `
  --build-cache `
  --console=plain `
  $args
exit $LastExitCode
