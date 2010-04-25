CLASSPATH=$CLASSPATH:$ES_HOME/lib/*

# Arguments to pass to the JVM
JAVA_OPTS=" \
        -Xms128M \
        -Xmx1G \
        -Djline.enabled=true \
        -XX:+AggressiveOpts \
        -XX:+UseParNewGC \
        -XX:+UseConcMarkSweepGC \
        -XX:+CMSParallelRemarkEnabled \
        -XX:+HeapDumpOnOutOfMemoryError"
