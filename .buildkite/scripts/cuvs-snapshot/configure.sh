#!/bin/bash

if [[ -f /etc/profile.d/elastic-nvidia.sh ]]; then
  export JAVA_HOME="$HOME/.java/openjdk24"
  export PATH="$JAVA_HOME/bin:$PATH"

  # Setup LD_LIBRARY_PATH, PATH

  export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"
  source /etc/profile.d/elastic-nvidia.sh

  # Not running this before the tests results in an error when running the tests
  # No idea why...
  if [[ "${BUILDKITE:-}" != "" && "${CI:-}" == "true" ]]; then
    nvidia-smi
  fi
fi

LIBCUVS_DIR="/opt/libcuvs"
sudo mkdir -p "$LIBCUVS_DIR"
sudo chmod 777 "$LIBCUVS_DIR"

CUVS_VERSION=$(grep 'cuvs_native' build-tools-internal/version.properties | awk '{print $3}')

LIBCUVS_VERSION_DIR="$LIBCUVS_DIR/$CUVS_VERSION"

if [[ ! -d "$LIBCUVS_VERSION_DIR" ]]; then
  mkdir -p "$LIBCUVS_VERSION_DIR"
  cd "$LIBCUVS_VERSION_DIR"
  CUVS_ARCHIVE="libcuvs-linux-x86_64-${CUVS_VERSION}_cuda12-archive.tar.xz"
  curl -fO "https://developer.download.nvidia.com/compute/cuvs/redist/libcuvs/linux-x86_64/$CUVS_ARCHIVE"
  tar -xJf "$CUVS_ARCHIVE"
  rm -f "$CUVS_ARCHIVE"
  cd -
fi

CUVS_ARCHIVE_DIR="libcuvs-linux-x86_64-${CUVS_VERSION}_cuda12-archive"
export LD_LIBRARY_PATH="$LIBCUVS_VERSION_DIR/$CUVS_ARCHIVE_DIR/lib:${LD_LIBRARY_PATH:-}"
