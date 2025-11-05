#!/bin/bash

if [[ -f /etc/profile.d/elastic-nvidia.sh ]]; then
  export JAVA_HOME="$HOME/.java/openjdk24"
  export PATH="$JAVA_HOME/bin:$PATH"

  # Setup LD_LIBRARY_PATH, PATH

  export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"
  source /etc/profile.d/elastic-nvidia.sh
fi

# Not running this before the tests results in an error when running the tests
# No idea why...
nvidia-smi

LIBCUVS_GCS_BUCKET="elasticsearch-cuvs-snapshots"

LIBCUVS_DIR="$HOME/libcuvs"
mkdir -p "$LIBCUVS_DIR"

CUVS_VERSION=$(grep 'cuvs_java' build-tools-internal/version.properties | awk '{print $3}')

LIBCUVS_VERSION_DIR="$LIBCUVS_DIR/$CUVS_VERSION"

if [[ ! -d "$LIBCUVS_VERSION_DIR" ]]; then
  cd "$LIBCUVS_DIR"
  CUVS_ARCHIVE="libcuvs-$CUVS_VERSION.tar.gz"
  curl -O "https://storage.googleapis.com/$LIBCUVS_GCS_BUCKET/$CUVS_ARCHIVE"
  tar -xzf "$CUVS_ARCHIVE"
  rm -f "$CUVS_ARCHIVE"

  cd -
fi

export LD_LIBRARY_PATH="$LIBCUVS_VERSION_DIR:$LD_LIBRARY_PATH"
