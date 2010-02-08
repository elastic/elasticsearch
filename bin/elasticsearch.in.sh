CLASSPATH=$CLASSPATH:$ES_HOME/lib/*

# Arguments to pass to the JVM
# java.net.preferIPv4Stack=true: Better OOTB experience, especially with jgroups
JAVA_OPTS=" \
        -Xms128M \
        -Xmx1G \
        -Djline.enabled=true \
        -Djava.net.preferIPv4Stack=true \
        -XX:+AggressiveOpts \
        -XX:+UseParNewGC \
        -XX:+UseConcMarkSweepGC \
        -XX:+CMSParallelRemarkEnabled \
        -XX:+HeapDumpOnOutOfMemoryError"
