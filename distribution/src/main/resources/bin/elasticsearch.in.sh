#!/bin/bash

if [ `uname -o` == "FreeBSD" ]; then
	. /etc/rc.subr
	load_rc_config elasticsearch
	ES_MIN_MEM=${elasticsearch_min_mem}
	ES_MAX_MEM=${elasticsearch_max_mem}
	ES_HEAP_NEW_SIZE=${elasticsearch_heap_newsize}
	ES_DIRECT_SIZE=${elasticsearch_direct_size}
	ES_USE_IPV4=${elasticsearch_use_ipv4}
	ES_GC_OPTS=${elasticsearch_gc_opts}
	ES_GC_LOG_FILE=${elasticsearch_gc_logfile}
	JAVA_OPTS="$JAVA_OPTS -Des.path.conf=${elasticsearch_config:="%%PREFIX%%/etc/elasticsearch"}"
	JAVA_OPTS="$JAVA_OPTS -Des.path.scripts=${elasticsearch_scripts:="%%PREFIX%%/libexec/elasticsearch"}"
fi

# check in case a user was using this mechanism
if [ "x$ES_CLASSPATH" != "x" ]; then
    cat >&2 << EOF
Error: Don't modify the classpath with ES_CLASSPATH. Best is to add
additional elements via the plugin mechanism, or if code must really be
added to the main classpath, add jars to lib/ (unsupported).
EOF
    exit 1
fi

ES_CLASSPATH="$ES_HOME/lib/*"
