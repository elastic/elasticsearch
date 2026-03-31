#!/bin/bash

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)

branches=($(cat "$ROOT_DIR/branches.json" | jq -r '.branches[].branch'))
for branch in "${branches[@]}"; do
  echo "Resolving dependencies for ${branch} branch"
  rm -rf "checkout/$branch"
  git clone /opt/git-mirrors/elastic-elasticsearch --branch "$branch" --single-branch "checkout/$branch"

  CHECKOUT_DIR=$(cd "./checkout/${branch}" && pwd)
  CI_DIR="$CHECKOUT_DIR/.ci"

  if [ "$(uname -m)" = "arm64" ] || [ "$(uname -m)" = "aarch64" ]; then
    ## On ARM we use a different properties file for setting java home
    ## Also, we don't bother attempting to resolve dependencies for the 6.8 branch
    source "$CI_DIR/java-versions-aarch64.properties"
    export JAVA16_HOME="$HOME/.java/jdk16"
  else
    source "$CI_DIR/java-versions.properties"
    ## We are caching BWC versions too, need these so we can build those
    export JAVA8_HOME="$HOME/.java/java8"
    export JAVA11_HOME="$HOME/.java/java11"
    export JAVA12_HOME="$HOME/.java/openjdk12"
    export JAVA13_HOME="$HOME/.java/openjdk13"
    export JAVA14_HOME="$HOME/.java/openjdk14"
    export JAVA15_HOME="$HOME/.java/openjdk15"
    export JAVA16_HOME="$HOME/.java/openjdk16"
  fi

  export JAVA_HOME="$HOME/.java/$ES_BUILD_JAVA"
  "checkout/${branch}/gradlew" --project-dir "$CHECKOUT_DIR" --parallel -s resolveAllDependencies -Dorg.gradle.warning.mode=none -DisCI --max-workers=4
  "checkout/${branch}/gradlew" --stop
  pkill -f '.*GradleDaemon.*'
  rm -rf "checkout/${branch}"
done
