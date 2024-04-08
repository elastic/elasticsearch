#!/bin/bash

set -e -o pipefail

source "`dirname "$0"`"/elasticsearch-env

# use a small heap size for the CLI tools, and thus the serial collector to
# avoid stealing many CPU cycles; a user can override by setting CLI_JAVA_OPTS
CLI_JAVA_OPTS="-Xms4m -Xmx64m -XX:+UseSerialGC ${CLI_JAVA_OPTS}"

LAUNCHER_CLASSPATH=$ES_HOME/lib/*:$ES_HOME/lib/cli-launcher/*

exec \
  "$JAVA" \
  $CLI_JAVA_OPTS \
  -Dcli.name="$CLI_NAME" \
  -Dcli.script="$0" \
  -Dcli.libs="$CLI_LIBS" \
  -Des.path.home="$ES_HOME" \
  -Des.path.conf="$ES_PATH_CONF" \
  -Des.distribution.type="$ES_DISTRIBUTION_TYPE" \
  -cp "$LAUNCHER_CLASSPATH" \
  org.elasticsearch.launcher.CliToolLauncher \
  "$@"
