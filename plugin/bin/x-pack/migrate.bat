@echo off

rem Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
rem or more contributor license agreements. Licensed under the Elastic License;
rem you may not use this file except in compliance with the Elastic License.

setlocal enabledelayedexpansion

call "%~dp0..\elasticsearch-env.bat"

call "%~dp0x-pack-env.bat"

%JAVA% ^
  %ES_JAVA_OPTS% ^
  -Des.path.home="%ES_HOME%" ^
  -Des.path.conf="%CONF_DIR%" ^
  -cp "%ES_CLASSPATH%" ^
  org.elasticsearch.xpack.security.authc.esnative.ESNativeRealmMigrateTool ^
  %*

endlocal
