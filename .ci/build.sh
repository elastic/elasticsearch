#!/bin/bash

JAVA_HOME=${JAVA_HOME:-$HOME/.java/openjdk13}
RUNTIME_JAVA_HOME=${RUNTIME_JAVA_HOME:-$HOME/.java/openjdk11}

JAVA7_HOME=$HOME/.java/java7
JAVA8_HOME=$HOME/.java/java8
JAVA9_HOME=$HOME/.java/java9
JAVA10_HOME=$HOME/.java/java10
JAVA11_HOME=$HOME/.java/java11
JAVA12_HOME=$HOME/.java/openjdk12
JAVA13_HOME=$HOME/.java/openjdk13

# drop page cache and kernel slab objects on linux
[[ -x /usr/local/sbin/drop-caches ]] && sudo /usr/local/sbin/drop-caches

rm -Rfv ~/.gradle/init.d/init.gradle
mkdir -p ~/.gradle/init.d && cp -v $WORKSPACE/.ci/init.gradle ~/.gradle/init.d

if [ -f /proc/cpuinfo ] ; then
   MAX_WORKERS=`grep '^cpu\scores' /proc/cpuinfo  | uniq | sed 's/\s\+//g' |  cut -d':' -f 2`
else
   if [[ "$OSTYPE" == "darwin"* ]]; then
      # Parallel is disabled at  this time (eventually set to 1) due to errors on the Mac workers
      # We'll have to do more testing to see if this can be re-enabled or what the proper value is.
      # MAX_WORKERS=`sysctl -n hw.physicalcpu | sed 's/\s\+//g'`
      MAX_WORKERS=2
   else
      echo "Unsupported OS Type: $OSTYPE"
      exit 1
   fi
fi

if pwd | grep -v -q ^/dev/shm ; then
   echo "Not running on a ramdisk, reducing number of workers"
   MAX_WORKERS=$(($MAX_WORKERS*2/3))
fi

export GRADLE_OPTS="-XX:+HeapDumpOnOutOfMemoryError -Xmx128m -Xms128m"
set -e
./gradlew --parallel --scan \
  -Dorg.elasticsearch.build.cache.url=https://gradle-enterprise.elastic.co/cache/ \
  --parallel --max-workers=$MAX_WORKERS \
   "$@"
