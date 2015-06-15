#!/bin/bash

for i in * ; do
  if [ -d "$i" ]; then
    echo "**** $i"
	cd $i
	mvn clean install -DskipTests 1>> ../make.logs
	unzip -l target/releases/elasticsearch-$i-2.0.0-SNAPSHOT.zip
	cd ..
  fi
done

