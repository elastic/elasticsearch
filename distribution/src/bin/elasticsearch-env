#!/bin/bash

set -e -o pipefail

CDPATH=""

SCRIPT="$0"

# SCRIPT might be an arbitrarily deep series of symbolic links; loop until we
# have the concrete path
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

# determine Elasticsearch home; to do this, we strip from the path until we find
# bin, and then strip bin (there is an assumption here that there is no nested
# directory under bin also named bin)
ES_HOME=`dirname "$SCRIPT"`

# now make ES_HOME absolute
ES_HOME=`cd "$ES_HOME"; pwd`

while [ "`basename "$ES_HOME"`" != "bin" ]; do
  ES_HOME=`dirname "$ES_HOME"`
done
ES_HOME=`dirname "$ES_HOME"`

# now set the path to java
if [ ! -z "$ES_JAVA_HOME" ]; then
  JAVA="$ES_JAVA_HOME/bin/java"
  JAVA_TYPE="ES_JAVA_HOME"

  if [ ! -x "$JAVA" ]; then
    echo "could not find java in $JAVA_TYPE at $JAVA" >&2
    exit 1
  fi

  # check the user supplied jdk version
  "$JAVA" -cp "$ES_HOME/lib/java-version-checker/*" org.elasticsearch.tools.java_version_checker.JavaVersionChecker
else
  # use the bundled JDK (default)
  if [ "$(uname -s)" = "Darwin" ]; then
    # macOS has a different structure
    JAVA="$ES_HOME/jdk.app/Contents/Home/bin/java"
  else
    JAVA="$ES_HOME/jdk/bin/java"
  fi
  JAVA_TYPE="bundled JDK"
fi

# do not let JAVA_TOOL_OPTIONS slip in (as the JVM does by default)
if [ ! -z "$JAVA_TOOL_OPTIONS" ]; then
  echo "warning: ignoring JAVA_TOOL_OPTIONS=$JAVA_TOOL_OPTIONS"
  unset JAVA_TOOL_OPTIONS
fi

# warn that we are not observing the value of JAVA_HOME
if [ ! -z "$JAVA_HOME" ]; then
  echo "warning: ignoring JAVA_HOME=$JAVA_HOME; using $JAVA_TYPE" >&2
fi

# JAVA_OPTS is not a built-in JVM mechanism but some people think it is so we
# warn them that we are not observing the value of $JAVA_OPTS
if [ ! -z "$JAVA_OPTS" ]; then
  echo -n "warning: ignoring JAVA_OPTS=$JAVA_OPTS; "
  echo "pass JVM parameters via ES_JAVA_OPTS"
fi

export HOSTNAME=$HOSTNAME

@source.path.env@

if [ -z "$ES_PATH_CONF" ]; then
  echo "ES_PATH_CONF must be set to the configuration path"
  exit 1
fi

# now make ES_PATH_CONF absolute
ES_PATH_CONF=`cd "$ES_PATH_CONF"; pwd`

ES_DISTRIBUTION_TYPE=@es.distribution.type@

cd "$ES_HOME"
