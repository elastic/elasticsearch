#!/usr/bin/env bash
#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", or the "Server Side
# Public License, v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
source "$script_dir/zstd-inputs.sh"

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

sha256() {
  if command -v sha256sum >/dev/null; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

download() {
  local url="$1"
  local expected_sha256="$2"
  local destination="$3"

  curl --fail --location --retry 3 --silent --show-error --output "$destination" "$url"
  test "$(sha256 "$destination")" = "$expected_sha256"
}

download_bottle() {
  local url="$1"
  local expected_sha256="$2"
  local destination="$3"

  curl --fail --location --retry 3 --silent --show-error --header 'Authorization: Bearer QQ==' --output "$destination" "$url"
  test "$(sha256 "$destination")" = "$expected_sha256"
}

download_source() {
  local source_archive="$work_dir/zstd.tar.gz"
  download "$ZSTD_SOURCE_URL" "$ZSTD_SOURCE_SHA256" "$source_archive"
  tar -xzf "$source_archive" -C "$work_dir"
  source_dir="$work_dir/zstd-$ZSTD_VERSION"
  test -d "$source_dir"
}

copy_license() {
  local destination="$1"

  mkdir -p "$destination"
  cp "$source_dir/LICENSE" "$destination/LICENSE"
}

verify_linux_abi() {
  local library="$1"

  if readelf -V "$library" | grep -qE 'GLIBC_2\.(29|[3-9][0-9])'; then
    echo "$library requires glibc newer than 2.28" >&2
    exit 1
  fi
}

build_linux() {
  local platform="$1"
  local destination="$2"
  local compiler
  local cflags
  local ldflags
  local strip_command

  case "$platform" in
    linux-aarch64)
      compiler='clang-21 --target=aarch64-linux-gnu'
      ;;
    linux-x64)
      compiler='clang-21 --target=x86_64-linux-gnu'
      ;;
    *)
      echo "Unsupported Linux platform: $platform" >&2
      exit 1
      ;;
  esac

  cflags='-O3 -flto -fPIC -fvisibility=hidden'
  ldflags='-shared -flto -fuse-ld=lld'
  strip_command='llvm-strip-21'
  if [ "${ZSTD_BUILD_MODE:-docker}" = host ]; then
    compiler="${CC:-cc}"
    cflags="${CFLAGS:--O3} -fPIC -fvisibility=hidden"
    ldflags="-shared ${LDFLAGS:-}"
    strip_command="${STRIP:-strip}"
  fi

  make -C "$source_dir/lib" clean CC="$compiler"
  make -C "$source_dir/lib" lib-release-nomt CC="$compiler" CFLAGS="$cflags" LDFLAGS="$ldflags"

  mkdir -p "$destination"
  "$strip_command" --strip-unneeded "$source_dir/lib/libzstd.so.$ZSTD_VERSION"
  cp -f "$source_dir/lib/libzstd.so.$ZSTD_VERSION" "$destination/libzstd.so"
  if [ "${ZSTD_BUILD_MODE:-docker}" != host ]; then
    verify_linux_abi "$destination/libzstd.so"
  fi
  copy_license "$destination"
}

stage_darwin() {
  local platform="$1"
  local destination="$2"
  local archive="$work_dir/$platform.tar.gz"
  local extracted="$work_dir/$platform"
  local url
  local checksum

  case "$platform" in
    darwin-aarch64)
      url="$DARWIN_AARCH64_BOTTLE_URL"
      checksum="$DARWIN_AARCH64_BOTTLE_SHA256"
      ;;
    darwin-x64)
      url="$DARWIN_X64_BOTTLE_URL"
      checksum="$DARWIN_X64_BOTTLE_SHA256"
      ;;
    *)
      echo "Unsupported Darwin platform: $platform" >&2
      exit 1
      ;;
  esac

  download_bottle "$url" "$checksum" "$archive"
  mkdir -p "$extracted" "$destination"
  tar -xzf "$archive" -C "$extracted" "zstd/$ZSTD_VERSION/lib/libzstd.$ZSTD_VERSION.dylib"
  cp -f "$extracted/zstd/$ZSTD_VERSION/lib/libzstd.$ZSTD_VERSION.dylib" "$destination/libzstd.dylib"
  copy_license "$destination"
}

stage_windows() {
  local destination="$1"
  local archive="$work_dir/zstd-windows.zip"

  download "$WINDOWS_X64_ARCHIVE_URL" "$WINDOWS_X64_ARCHIVE_SHA256" "$archive"
  mkdir -p "$destination"
  rm -f "$destination/zstd.dll"
  unzip -p "$archive" "zstd-v$ZSTD_VERSION-win64/dll/libzstd.dll" > "$destination/zstd.dll"
  copy_license "$destination"
}

host_platform() {
  case "$(uname -s)-$(uname -m)" in
    Darwin-arm64|Darwin-aarch64)
      echo darwin-aarch64
      ;;
    Darwin-x86_64)
      echo darwin-x64
      ;;
    Linux-aarch64)
      echo linux-aarch64
      ;;
    Linux-x86_64)
      echo linux-x64
      ;;
    MINGW*-x86_64|MSYS*-x86_64|CYGWIN*-x86_64)
      echo windows-x64
      ;;
    *)
      echo "Unsupported host: $(uname -s)-$(uname -m)" >&2
      exit 1
      ;;
  esac
}

build_all() {
  local output_dir=build/zstd-staged

  download_source
  build_linux linux-aarch64 "$output_dir/linux-aarch64"
  build_linux linux-x64 "$output_dir/linux-x64"
  stage_darwin darwin-aarch64 "$output_dir/darwin-aarch64"
  stage_darwin darwin-x64 "$output_dir/darwin-x64"
  stage_windows "$output_dir/windows-x64"
}

build_host() {
  local output_dir="$1"
  local platform

  download_source
  platform="$(host_platform)"
  case "$platform" in
    linux-aarch64|linux-x64)
      ZSTD_BUILD_MODE=host build_linux "$platform" "$output_dir/$platform"
      ;;
    darwin-aarch64|darwin-x64)
      stage_darwin "$platform" "$output_dir/$platform"
      ;;
    windows-x64)
      stage_windows "$output_dir/$platform"
      ;;
  esac
}

case "${1:-}" in
  all)
    build_all
    ;;
  host)
    build_host "${2:?destination directory is required}"
    ;;
  *)
    echo "Usage: $0 all | host <destination>" >&2
    exit 1
    ;;
esac
