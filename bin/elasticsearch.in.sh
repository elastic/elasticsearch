ES_CLASSPATH=$ES_CLASSPATH:$ES_HOME/lib/*:$ES_HOME/lib/sigar/*

if [ "x$ES_MIN_MEM" = "x" ]; then
    ES_MIN_MEM=256m
fi
if [ "x$ES_MAX_MEM" = "x" ]; then
    ES_MAX_MEM=1g
fi

# min and max heap sizes should be set to the same value to avoid
# stop-the-world GC pauses during resize, and so that we can lock the
# heap in memory on startup to prevent any of it from being swapped
# out.
JAVA_OPTS="$JAVA_OPTS -Xms${ES_MIN_MEM}"
JAVA_OPTS="$JAVA_OPTS -Xmx${ES_MAX_MEM}"

# reduce the per-thread stack size
JAVA_OPTS="$JAVA_OPTS -Xss128k"

# Force the JVM to use IPv4 stack
# JAVA_OPTS="$JAVA_OPTS -Djava.net.preferIPv4Stack=true"

# Enable aggressive optimizations in the JVM
#    - Disabled by default as it might cause the JVM to crash
# JAVA_OPTS="$JAVA_OPTS -XX:+AggressiveOpts"

JAVA_OPTS="$JAVA_OPTS -XX:+UseParNewGC"
JAVA_OPTS="$JAVA_OPTS -XX:+UseConcMarkSweepGC"
JAVA_OPTS="$JAVA_OPTS -XX:+CMSParallelRemarkEnabled"
JAVA_OPTS="$JAVA_OPTS -XX:SurvivorRatio=8"
JAVA_OPTS="$JAVA_OPTS -XX:MaxTenuringThreshold=1"
JAVA_OPTS="$JAVA_OPTS -XX:CMSInitiatingOccupancyFraction=75"
JAVA_OPTS="$JAVA_OPTS -XX:+UseCMSInitiatingOccupancyOnly"

# GC logging options -- uncomment to enable
# JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCDetails"
# JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCTimeStamps"
# JAVA_OPTS="$JAVA_OPTS -XX:+PrintClassHistogram"
# JAVA_OPTS="$JAVA_OPTS -XX:+PrintTenuringDistribution"
# JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCApplicationStoppedTime"
# JAVA_OPTS="$JAVA_OPTS -Xloggc:/var/log/elasticsearch/gc.log"

# Causes the JVM to dump its heap on OutOfMemory.
JAVA_OPTS="$JAVA_OPTS -XX:+HeapDumpOnOutOfMemoryError"
# The path to the heap dump location, note directory must exists and have enough
# space for a full heap dump.
#JAVA_OPTS="$JAVA_OPTS -XX:HeapDumpPath=$ES_HOME/logs/heapdump.hprof"
