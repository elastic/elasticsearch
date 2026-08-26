#!/usr/bin/env bash
#
# Prepare a loaddriver for esbench BEFORE running ~/scripts/benchmark.py.
#
# Esbench ansible always runs `esrally build` when installing from a remote fork.
# That Gradle build cannot download es-simdjson from Artifactory yet, so we:
#   1. Patch ansible to pass LOCAL_SIMDJSON_BINARY and use host Gradle (not Docker)
#   2. Stage libes_simdjson.so into the Rally checkout
#   3. Optionally pre-build the distribution tar (recommended — verifies the build)
#
# Usage (on the loaddriver, after esbench start, before benchmark.py):
#   ./prepare-esbench-loaddriver.sh              # ARM ES node (default)
#   ./prepare-esbench-loaddriver.sh x86_64       # x64 ES node
#
# Environment:
#   ES_SRC   Rally elasticsearch checkout (default: ~/.rally/benchmarks/src/elasticsearch)
#   ES_ZIP   Path to es-simdjson-*-local.zip (default: ./es-simdjson-0.1.0-local.zip)

set -euo pipefail

ARCH="${1:-aarch64}"
ES_SRC="${ES_SRC:-$HOME/.rally/benchmarks/src/elasticsearch}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ES_ZIP="${ES_ZIP:-$SCRIPT_DIR/es-simdjson-0.1.0-local.zip}"
COPY_ARTIFACT="$HOME/scripts/ansible/roles/copy_artifact/tasks/ubuntu.yml"

case "$ARCH" in
  aarch64)
    PLAT=linux-aarch64
    ZIP_MEMBER=linux-aarch64/libes_simdjson.so
    GRADLE_TAR=":distribution:archives:linux-aarch64-tar:assemble"
    DIST_GLOB="distribution/archives/linux-aarch64-tar/build/distributions/elasticsearch-*-linux-aarch64*.tar.gz"
    ;;
  x86_64)
    PLAT=linux-x64
    ZIP_MEMBER=linux-x64/libes_simdjson.so
    GRADLE_TAR=":distribution:archives:linux-tar:assemble"
    DIST_GLOB="distribution/archives/linux-tar/build/distributions/elasticsearch-*-linux-x86_64*.tar.gz"
    ;;
  *)
    echo "Unknown arch: $ARCH (expected aarch64 or x86_64)" >&2
    exit 1
    ;;
esac

if [ ! -f "$COPY_ARTIFACT" ]; then
  echo "Error: ansible copy_artifact task not found at $COPY_ARTIFACT" >&2
  echo "Run this script on the esbench loaddriver after esbench start." >&2
  exit 1
fi

patch_ansible() {
  if grep -q 'LOCAL_SIMDJSON_BINARY' "$COPY_ARTIFACT"; then
    echo "Ansible copy_artifact already patched."
    return
  fi
  echo "Patching $COPY_ARTIFACT (gradle build + LOCAL_SIMDJSON_BINARY) ..."
  sed -i \
    -e 's/--source-build-method=docker/--source-build-method=gradle/' \
    -e '/name: build elasticsearch on loaddriver/,/with_items:/{
      /PATH:.*local\.bin/a\        LOCAL_SIMDJSON_BINARY: "1"
    }' \
    "$COPY_ARTIFACT"
}

stage_native_lib() {
  local stage_dir="$ES_SRC/libs/native/libraries/build/platform/$PLAT"
  mkdir -p "$stage_dir"
  if [ -f "$stage_dir/libes_simdjson.so" ]; then
    echo "Native lib already staged at $stage_dir/libes_simdjson.so"
    return
  fi
  if [ -f "$ES_ZIP" ]; then
    echo "Staging from $ES_ZIP ($ZIP_MEMBER) ..."
    unzip -p "$ES_ZIP" "$ZIP_MEMBER" > "$stage_dir/libes_simdjson.so"
    return
  fi
  local built_so="$SCRIPT_DIR/build/libs/es_simdjson/shared/aarch64/libes_simdjson.so"
  if [ "$ARCH" = "x86_64" ]; then
    built_so="$SCRIPT_DIR/build/libs/es_simdjson/shared/amd64/libes_simdjson.so"
  fi
  if [ -f "$built_so" ]; then
    echo "Staging from $built_so ..."
    cp "$built_so" "$stage_dir/libes_simdjson.so"
    return
  fi
  echo "Error: libes_simdjson.so not found." >&2
  echo "Run ./publish_simdjson_binaries.sh --local in $SCRIPT_DIR, or:" >&2
  echo "  cd $SCRIPT_DIR && make install CLANG_CXX=clang++" >&2
  exit 1
}

prebuild_tarball() {
  if [ ! -d "$ES_SRC" ]; then
    echo "Note: $ES_SRC does not exist yet; Rally will clone on first build."
    echo "Skipping Gradle pre-build — benchmark.py will clone then build."
    return
  fi
  export LOCAL_SIMDJSON_BINARY=1
  echo "Pre-building distribution ($GRADLE_TAR) ..."
  (cd "$ES_SRC" && ./gradlew --no-daemon "$GRADLE_TAR")
  local tar
  tar=$(ls -t $ES_SRC/$DIST_GLOB 2>/dev/null | head -1)
  if [ -z "$tar" ]; then
    echo "Error: Gradle assemble succeeded but no tar found at $ES_SRC/$DIST_GLOB" >&2
    exit 1
  fi
  mkdir -p "$HOME/.rally/benchmarks/distributions"
  cp "$tar" "$HOME/.rally/benchmarks/distributions/"
  echo "Copied $(basename "$tar") to ~/.rally/benchmarks/distributions/"
  ls -la "$HOME/.rally/benchmarks/distributions/"*"$(basename "$tar")"
  echo "Verify native lib in tar:"
  tar -tzf "$tar" | grep libes_simdjson || {
    echo "Error: libes_simdjson.so missing from tarball" >&2
    exit 1
  }
}

patch_ansible
export LOCAL_SIMDJSON_BINARY=1

if [ -d "$ES_SRC" ]; then
  stage_native_lib
  prebuild_tarball
else
  echo "Checkout missing — will stage native lib after Rally clones (on first benchmark.py run)."
  echo "Ensure $ES_ZIP exists, then re-run this script before a second benchmark.py attempt."
fi

echo ""
echo "Ready. Run the benchmark:"
echo "  cd ~/scripts && ./benchmark.py"
