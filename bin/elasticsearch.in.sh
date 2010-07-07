CLASSPATH=$CLASSPATH:$ES_HOME/lib/elasticsearch-@ES_VERSION@.jar:$ES_HOME/lib/*:$ES_HOME/lib/sigar/*

if [ "x$ES_MIN_MEM" = "x" ]; then
    ES_MIN_MEM=256m
fi
if [ "x$ES_MAX_MEM" = "x" ]; then
    ES_MAX_MEM=1g
fi

# Arguments to pass to the JVM
JAVA_OPTS=" \
        -Xms${ES_MIN_MEM} \
        -Xmx${ES_MAX_MEM} \
        -Djline.enabled=true \
        -XX:+AggressiveOpts \
        -XX:+UseParNewGC \
        -XX:+UseConcMarkSweepGC \
        -XX:+CMSParallelRemarkEnabled \
        -XX:+HeapDumpOnOutOfMemoryError"
