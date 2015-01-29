#!/bin/bash

mvn clean package -DskipTests
cd target
cd releases
tar -xvf elasticsearch-2.0.0-SNAPSHOT.tar.gz
cd elasticsearch-2.0.0-SNAPSHOT
cd bin
./elasticsearch
