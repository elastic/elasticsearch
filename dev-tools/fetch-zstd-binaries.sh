#!/usr/bin/env bash
set -e

if [ "$#" -ne 1 ]; then
    printf 'Usage: %s <version>\n' "$(basename "$0")"
    exit 0;
fi

VERSION="$1"
TEMP=$(mktemp -d)

fetch_homebrew_artifact() {
  DIGEST=$(curl -sS --retry 3 -H "Accept: application/vnd.oci.image.index.v1+json" -H "Authorization: Bearer QQ==" \
      --location "https://ghcr.io/v2/homebrew/core/zstd/manifests/$VERSION" | jq -r \
      ".manifests[] | select(.platform.os == \"darwin\" and .platform.architecture == \"$1\" and .platform.\"os.version\" == \"macOS 13\") | .annotations.\"sh.brew.bottle.digest\"")

  OUTPUT_FILE="$TEMP/zstd-$VERSION-darwin-$1.tar.gz"
  curl -sS --retry 3 -H "Authorization: Bearer QQ==" --output "$OUTPUT_FILE" --location "https://ghcr.io/v2/homebrew/core/zstd/blobs/sha256:$DIGEST"
  echo $OUTPUT_FILE
}

echo 'Downloading MacOS zstd binaries...'
DARWIN_ARM_BREW=$(fetch_homebrew_artifact 'arm64')
DARWIN_X86_BREW=$(fetch_homebrew_artifact 'amd64')

build_darwin_jar() {
  ARTIFACT="$TEMP/zstd-$VERSION-darwin-$2.jar"
  TAR_DIR="$TEMP/darwin-$2"
  mkdir $TAR_DIR
  tar zxf $1 --strip-components=2 --include="*/LICENSE" --include="*/libzstd.$VERSION.dylib" -C $TAR_DIR
  mv $TAR_DIR/lib/libzstd.$VERSION.dylib $TAR_DIR/libzstd.dylib && rm -rf $TAR_DIR/lib
  FILE_COUNT=$(ls -1 $TAR_DIR | wc -l | xargs)
  if [ "$FILE_COUNT" -ne 2 ]; then
    >&2 echo "ERROR: Expected 2 files in $TAR_DIR but found $FILE_COUNT"
    exit 1
  fi
  (cd $TAR_DIR/../ && zip -rq - $(basename $TAR_DIR)) > $ARTIFACT && rm -rf $TAR_DIR
}

echo 'Building MacOS jars...'
build_darwin_jar $DARWIN_ARM_BREW "aarch64"
build_darwin_jar $DARWIN_X86_BREW "x86_64"

tree $TEMP
