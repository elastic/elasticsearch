#!/bin/bash

# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License
# 2.0; you may not use this file except in compliance with the Elastic License
# 2.0.

CDPATH=""
SCRIPT="$0"

# SCRIPT may be an arbitrarily deep series of symlinks. Loop until we have the concrete path.
while [ -h "$SCRIPT" ] ; do
  ls=`ls -ld "$SCRIPT"`
  # Drop everything prior to ->
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    SCRIPT="$link"
  else
    SCRIPT=`dirname "$SCRIPT"`/"$link"
  fi
done

# determine license home
LICENSE_HOME=`dirname "$SCRIPT"`/..
# make LICENSE_HOME absolute
LICENSE_HOME=`cd "$LICENSE_HOME"; pwd`

# setup classpath
LICENSE_CLASSPATH=$LICENSE_CLASSPATH:$LICENSE_HOME/lib/*

if [ -x "$JAVA_HOME/bin/java" ]; then
    JAVA=$JAVA_HOME/bin/java
else
    JAVA=`which java`
fi

JAVA_OPTS="$JAVA_OPTS -Xmx64m -Xms16m"
CLI_NAME=$(basename $0)

exec \
  "$JAVA" \
  $JAVA_OPTS \
  -Dcli.name="$CLI_NAME" \
  -Des.path.home="`pwd`" \
  -cp "$LICENSE_CLASSPATH" \
  org.elasticsearch.launcher.CliToolLauncher \
  "$@"
