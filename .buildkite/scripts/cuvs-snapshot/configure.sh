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

LIBCUVS_GCS_BUCKET="elasticsearch-cuvs-snapshots"

LIBCUVS_DIR="/opt/libcuvs"
sudo mkdir -p "$LIBCUVS_DIR"
sudo chmod 777 "$LIBCUVS_DIR"

CUVS_VERSION=$(grep 'cuvs_java' build-tools-internal/version.properties | awk '{print $3}')

LIBCUVS_VERSION_DIR="$LIBCUVS_DIR/$CUVS_VERSION"

if [[ ! -d "$LIBCUVS_VERSION_DIR" ]]; then
  mkdir -p $LIBCUVS_VERSION_DIR
  cd "$LIBCUVS_VERSION_DIR"
  CUVS_ARCHIVE="libcuvs-$CUVS_VERSION.tar.gz"
  curl -fO "https://storage.googleapis.com/$LIBCUVS_GCS_BUCKET/libcuvs/$CUVS_ARCHIVE"
  tar -xzf "$CUVS_ARCHIVE"
  rm -f "$CUVS_ARCHIVE"
  if [[ -d "$CUVS_VERSION" ]]; then
    mv "$CUVS_VERSION/*" ./
  fi
  cd -
fi

export LD_LIBRARY_PATH="$LIBCUVS_VERSION_DIR:${LD_LIBRARY_PATH:-}"
