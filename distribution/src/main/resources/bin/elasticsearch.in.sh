#!/bin/sh

# check in case a user was using this mechanism
if [ "x$ES_CLASSPATH" != "x" ]; then
    cat >&2 << EOF
Error: Don't modify the classpath with ES_CLASSPATH. Best is to add
additional elements via the plugin mechanism, or if code must really be
added to the main classpath, add jars to lib/ (unsupported).
EOF
    exit 1
fi

ES_CLASSPATH="$ES_HOME/lib/elasticsearch-${project.version}.jar:$ES_HOME/lib/*"

if [ "x$ES_MIN_MEM" = "x" ]; then
    ES_MIN_MEM=${heap.min}
fi
if [ "x$ES_MAX_MEM" = "x" ]; then
    ES_MAX_MEM=${heap.max}
fi
if [ "x$ES_HEAP_SIZE" != "x" ]; then
    ES_MIN_MEM=$ES_HEAP_SIZE
    ES_MAX_MEM=$ES_HEAP_SIZE
fi

# min and max heap sizes should be set to the same value to avoid
# stop-the-world GC pauses during resize, and so that we can lock the
# heap in memory on startup to prevent any of it from being swapped
# out.
JAVA_OPTS="$JAVA_OPTS -Xms${ES_MIN_MEM}"
JAVA_OPTS="$JAVA_OPTS -Xmx${ES_MAX_MEM}"

# new generation
if [ "x$ES_HEAP_NEWSIZE" != "x" ]; then
    JAVA_OPTS="$JAVA_OPTS -Xmn${ES_HEAP_NEWSIZE}"
fi

# max direct memory
if [ "x$ES_DIRECT_SIZE" != "x" ]; then
    JAVA_OPTS="$JAVA_OPTS -XX:MaxDirectMemorySize=${ES_DIRECT_SIZE}"
fi

# set to headless, just in case
JAVA_OPTS="$JAVA_OPTS -Djava.awt.headless=true"

# Force the JVM to use IPv4 stack
if [ "x$ES_USE_IPV4" != "x" ]; then
  JAVA_OPTS="$JAVA_OPTS -Djava.net.preferIPv4Stack=true"
fi

# Add gc options. ES_GC_OPTS is unsupported, for internal testing
if [ "x$ES_GC_OPTS" = "x" ]; then
  ES_GC_OPTS="$ES_GC_OPTS -XX:+UseParNewGC"
  ES_GC_OPTS="$ES_GC_OPTS -XX:+UseConcMarkSweepGC"
  ES_GC_OPTS="$ES_GC_OPTS -XX:CMSInitiatingOccupancyFraction=75"
  ES_GC_OPTS="$ES_GC_OPTS -XX:+UseCMSInitiatingOccupancyOnly"
fi

JAVA_OPTS="$JAVA_OPTS $ES_GC_OPTS"

# GC logging options
if [ -n "$ES_GC_LOG_FILE" ]; then
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCDetails"
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCTimeStamps"
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCDateStamps"
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintClassHistogram"
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintTenuringDistribution"
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCApplicationStoppedTime"
  JAVA_OPTS="$JAVA_OPTS -Xloggc:$ES_GC_LOG_FILE"

  # Ensure that the directory for the log file exists: the JVM will not create it.
  mkdir -p "`dirname \"$ES_GC_LOG_FILE\"`"
fi

# Causes the JVM to dump its heap on OutOfMemory.
JAVA_OPTS="$JAVA_OPTS -XX:+HeapDumpOnOutOfMemoryError"
# The path to the heap dump location, note directory must exists and have enough
# space for a full heap dump.
#JAVA_OPTS="$JAVA_OPTS -XX:HeapDumpPath=$ES_HOME/logs/heapdump.hprof"

# Disables explicit GC
JAVA_OPTS="$JAVA_OPTS -XX:+DisableExplicitGC"

# Ensure UTF-8 encoding by default (e.g. filenames)
JAVA_OPTS="$JAVA_OPTS -Dfile.encoding=UTF-8"

# Use our provided JNA always versus the system one
JAVA_OPTS="$JAVA_OPTS -Djna.nosys=true"
