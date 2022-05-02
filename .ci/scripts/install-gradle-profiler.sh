#!/bin/bash

set -e
# profiler version we wanna install
PROFILER_VERSION="0.16.0"
wget https://repo.gradle.org/gradle/ext-releases-local/org/gradle/profiler/gradle-profiler/$PROFILER_VERSION/gradle-profiler-$PROFILER_VERSION.zip -O $WORKSPACE/gradle-profiler-$PROFILER_VERSION.zip
unzip $WORKSPACE/gradle-profiler-$PROFILER_VERSION.zip
mv $WORKSPACE/gradle-profiler-$PROFILER_VERSION $WORKSPACE/gradle-profiler
