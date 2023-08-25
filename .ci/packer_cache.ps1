$RUNBLD_DIR = 'C:\Program Files\runbld\src\runbld-7.0.3'
$RUNBLD = Join-Path $RUNBLD_DIR 'runbld'

# Check if 7.0.3 doesn't already exist
if (-not (Test-Path $RUNBLD)) {
  [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12;
  New-Item -Path $RUNBLD_DIR -ItemType Directory -Force
  Invoke-WebRequest -Uri 'https://packages.elasticsearch.org.s3.amazonaws.com/infra/runbld-7.0.3' -OutFile $RUNBLD

  $RUNBLD_HARDLINK_DIR = 'C:\Program Files\infra\bin'
  $RUNBLD_HARDLINK = Join-Path $RUNBLD_HARDLINK_DIR 'runbld-test'

  Remove-Item -Path $RUNBLD_HARDLINK -Force
  New-Item -Path $RUNBLD_HARDLINK_DIR -ItemType Directory -Force

  fsutil hardlink create $RUNBLD_HARDLINK $RUNBLD
}
