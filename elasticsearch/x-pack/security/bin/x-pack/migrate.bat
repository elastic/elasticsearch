@echo off

rem Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
rem or more contributor license agreements. Licensed under the Elastic License;
rem you may not use this file except in compliance with the Elastic License.

PUSHD "%~dp0"
CALL "%~dp0.in.bat" org.elasticsearch.xpack.security.authc.esnative.ESNativeRealmMigrateTool %*
POPD
