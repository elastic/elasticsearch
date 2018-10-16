#!/bin/bash

SCRIPT="$0"

# SCRIPT might be an arbitrarily deep series of symbolic links; loop until we
# have the concrete path
while [ -h "$SCRIPT" ] ; do
  ls=$(ls -ld "$SCRIPT")
  # Drop everything prior to ->
  link=$(expr "$ls" : '.*-> \(.*\)$')
  if expr "$link" : '/.*' > /dev/null; then
    SCRIPT="$link"
  else
    SCRIPT=$(dirname "$SCRIPT")/"$link"
  fi
done

cd $(dirname "${SCRIPT}")/..

resolveAllDependencies() {
    local REFRESH_FAILED=no
    local MAX_TRIES=3
    for i in $(seq 1 $MAX_TRIES) ; do 
        echo "Resolving dependencies try $i/$MAX_TRIES"
        JAVA_HOME="${HOME}"/.java/${ES_BUILD_JAVA} ./gradlew resolveAllDependencies --parallel && break
        REFRESH_FAILED=yes
    done
    if [ $REFRESH_FAILED != "no" ] ; then
        echo "Resolving dependencies failed after 3 retries ..."
        exit 1
    fi   
}

for branch in "master" "6.x" "5.6" ; do 
    echo "Resolving dependencies for $branch"
    if  ! git checkout $branch ; then 
        echo "Failed to switch branch to $branch"
        exit 2
    fi
    source .ci/java-versions.properties
    resolveAllDependencies
done

exit 0
