#! /bin/bash
### BEGIN INIT INFO
# Provides:          elasticsearch
# Required-Start:    $all
# Required-Stop:     $all
# Default-Start:
# Default-Stop:      0 1 6
# Short-Description: Starts elasticsearch
# chkconfig: - 80 15
# Description: Elasticsearch
### END INIT INFO

# Source function library.
. /etc/rc.d/init.d/functions

# Pull in sysconfig settings
[ -f /etc/sysconfig/elasticsearch ] && . /etc/sysconfig/elasticsearch

ES_HOME=/opt/elasticsearch
ES_USER=elasticsearch

DAEMON=${ES_HOME}/bin/elasticsearch
NAME=elasticsearch
PID_FILE=${PIDFILE:-/var/run/${NAME}/${NAME}.pid}
LOCK_FILE=${LOCKFILE:-/var/lock/subsys/${NAME}}
NFILES=${NFILES:-32768}

ES_PATH_LOG=${ES_PATH_LOG:-/var/log/${NAME}}
ES_PATH_DATA=${ES_PATH_DATA:-/var/lib/${NAME}}
ES_PATH_WORK=${ES_PATH_WORK:-/tmp/${NAME}}
ES_PATH_CONF=${ES_PATH_CONF:-/opt/${NAME}}/config
ES_PATH_PLUGINS=${ES_PATH_PLUGINS:-${ES_HOME}/plugins}
ES_CONFIG=${ES_CONFIG:-${ES_PATH_CONF}/elasticsearch.yml}

DAEMON_OPTS="-p ${PID_FILE} \
    -Des.config=${ES_CONFIG} \
    -Des.path.conf=${ES_PATH_CONF} \
    -Des.path.home=${ES_HOME} \
    -Des.path.logs=${ES_PATH_LOG} \
    -Des.path.data=${ES_PATH_DATA} \
    -Des.path.work=${ES_PATH_WORK} \
    -Des.path.plugins=${ES_PATH_PLUGINS}"

# These environment variables are passed over.
ES_MIN_MEM=${ES_MIN_MEM:-256m}
ES_MAX_MEM=${ES_MAX_MEM:-1g}
ES_INCLUDE=${ES_INCLUDE:-${ES_HOME}/bin/elasticsearch.in.sh}

start() {
    echo -n $"Starting ${NAME}: "
    mkdir -p $ES_PATH_WORK
    ulimit -n $NFILES
    daemon --pidfile=${PID_FILE} --user $ES_USER \
        ES_HOME=$ES_HOME \
        ES_INCLUDE=$ES_INCLUDE \
        ES_MIN_MEM=$ES_MIN_MEM \
        ES_MAX_MEM=$ES_MAX_MEM \
        $DAEMON $DAEMON_OPTS
    RETVAL=$?
    echo
    [ $RETVAL -eq 0 ] && touch $LOCK_FILE
    return $RETVAL
}

stop() {
    echo -n $"Stopping ${NAME}: "
    killproc -p ${PID_FILE} -d 10 $DAEMON
    RETVAL=$?
    echo
    [ $RETVAL = 0 ] && rm -f ${LOCK_FILE} ${PID_FILE}
    return $RETVAL
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    status)
        status -p ${PID_FILE} $DAEMON
        RETVAL=$?
        ;;
    restart|force-reload)
        stop
        start
        ;;
    *)
        N=/etc/init.d/${NAME}
        echo "Usage: $N {start|stop|restart|force-reload}" >&2
        RETVAL=2
        ;;
esac

exit $RETVAL
