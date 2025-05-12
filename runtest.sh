#!/bin/bash

# Number of iterations
iterations=1000

# Gradle command template
base_command="./gradlew \":server:internalClusterTest\" --tests \"org.elasticsearch.search.aggregations.metrics.TopHitsIT.testTopHitsOnInnerHits\" -Dtests.locale=be-Cyrl-BY -Dtests.timezone=Asia/Muscat -Druntime.java=24 --info"

# Loop
for ((i=1; i<=iterations; i++)); do
  # Generate a random seed
  seed=$(openssl rand -hex 8 | tr 'a-f' 'A-F')
  
  # Run the command with the new seed
  echo "Running iteration $i with seed $seed..."
  eval "$base_command -Dtests.seed=$seed"
done
