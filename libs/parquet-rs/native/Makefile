# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

# Builds libes_parquet_rs for all three targets:
#   build/darwin-aarch64/libes_parquet_rs.dylib  (rust-lld, -undefined dynamic_lookup)
#   build/linux-aarch64/libes_parquet_rs.so      (GCC cross-compiler)
#   build/linux-x64/libes_parquet_rs.so          (GCC cross-compiler)
#
# All targets can be cross-compiled from Linux inside the Docker toolchain
# container. The Darwin target uses rust-lld with -undefined dynamic_lookup
# so that framework symbols (CoreFoundation, etc.) are resolved by dyld at
# load time, avoiding the need for the macOS SDK.
#
# Usage:
#   make local       - build for the current platform
#   make install     - build and copy to the platform directory used by Gradle tests
#   make darwin      - cross-compile the Darwin binary (via rust-lld)
#   make linux       - cross-compile both Linux targets (via GCC)
#   make all         - build all three targets
#   make clean       - remove build artifacts
#
# See publish_pqrs_binaries.sh for the full build-and-publish workflow.

CARGO = cargo

# --- Platform detection for local builds ---

UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

ifeq ($(UNAME_S),Darwin)
  LIB_EXT = dylib
  ifeq ($(UNAME_M),arm64)
    PLATFORM_DIR = darwin-aarch64
  else
    PLATFORM_DIR = darwin-x64
  endif
else ifeq ($(UNAME_S),Linux)
  LIB_EXT = so
  ifeq ($(UNAME_M),aarch64)
    PLATFORM_DIR = linux-aarch64
  else
    PLATFORM_DIR = linux-x64
  endif
endif

LIB_NAME = libes_parquet_rs.$(LIB_EXT)
CARGO_OUTPUT = target/release/$(LIB_NAME)
INSTALL_DIR = ../../native/libraries/build/platform/$(PLATFORM_DIR)

# --- Cross-compilation targets ---

TARGET_DARWIN  = aarch64-apple-darwin
TARGET_LIN_ARM = aarch64-unknown-linux-gnu
TARGET_LIN_X64 = x86_64-unknown-linux-gnu

OUT_DARWIN  = build/darwin-aarch64/libes_parquet_rs.dylib
OUT_LIN_ARM = build/linux-aarch64/libes_parquet_rs.so
OUT_LIN_X64 = build/linux-x64/libes_parquet_rs.so

.PHONY: local install darwin linux all clean

# --- local: build for the current platform ---

local:
	$(CARGO) build --release

install: local
	mkdir -p $(INSTALL_DIR)
	cp $(CARGO_OUTPUT) $(INSTALL_DIR)/$(LIB_NAME)

# --- darwin: cross-compile via rust-lld ---
# Uses -undefined dynamic_lookup so framework symbols (CoreFoundation, etc.)
# are resolved by macOS dyld at load time rather than requiring the SDK at
# link time.
#
# The linker still needs to *find* the libraries that rustc passes (-lc, -lm,
# -lSystem, -framework CoreFoundation), even though all symbols are deferred.
# We generate minimal Apple TBD (text-based definition) stubs under
# build/.darwin-sysroot/ — empty YAML files that tell the linker "this library
# exists" without providing any code or symbol definitions.

DARWIN_SYSROOT = $(CURDIR)/build/.darwin-sysroot

darwin: $(OUT_DARWIN)

$(OUT_DARWIN): | $(DARWIN_SYSROOT)
	CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER="rust-lld" \
	CARGO_TARGET_AARCH64_APPLE_DARWIN_RUSTFLAGS="-C link-arg=-undefined -C link-arg=dynamic_lookup -C link-arg=-L$(DARWIN_SYSROOT)/lib -C link-arg=-F$(DARWIN_SYSROOT)/Frameworks" \
	  $(CARGO) build --release --target $(TARGET_DARWIN)
	mkdir -p $(dir $@)
	cp target/$(TARGET_DARWIN)/release/libes_parquet_rs.dylib $@

# Create minimal Apple TBD stubs. Each .tbd is a short YAML file in Apple's
# TAPI (text-based API) format that tells the linker "this library exists"
# without providing any symbols or code.
$(DARWIN_SYSROOT):
	@mkdir -p $@/lib $@/Frameworks/CoreFoundation.framework
	@for lib in libc libm libiconv libSystem; do \
	  printf '%s\n' \
	    '--- !tapi-tbd' \
	    'tbd-version: 4' \
	    'targets: [ arm64-macos ]' \
	    "install-name: /usr/lib/$${lib}.dylib" \
	    'exports: []' \
	    '...' > $@/lib/$${lib}.tbd; \
	done
	@printf '%s\n' \
	  '--- !tapi-tbd' \
	  'tbd-version: 4' \
	  'targets: [ arm64-macos ]' \
	  'install-name: /System/Library/Frameworks/CoreFoundation.framework/CoreFoundation' \
	  'exports: []' \
	  '...' > $@/Frameworks/CoreFoundation.framework/CoreFoundation.tbd

# --- linux: cross-compile both Linux targets (inside Docker) ---

linux: $(OUT_LIN_ARM) $(OUT_LIN_X64)

$(OUT_LIN_ARM):
	CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER="aarch64-linux-gnu-gcc" \
	  $(CARGO) build --release --target $(TARGET_LIN_ARM)
	mkdir -p $(dir $@)
	cp target/$(TARGET_LIN_ARM)/release/libes_parquet_rs.so $@
	aarch64-linux-gnu-strip --strip-unneeded $@

$(OUT_LIN_X64):
	CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="x86_64-linux-gnu-gcc" \
	  $(CARGO) build --release --target $(TARGET_LIN_X64)
	mkdir -p $(dir $@)
	cp target/$(TARGET_LIN_X64)/release/libes_parquet_rs.so $@
	x86_64-linux-gnu-strip --strip-unneeded $@

# --- all: build everything ---
# All three targets are cross-compiled inside the Docker toolchain container.
# See publish_pqrs_binaries.sh for the full workflow.

all: darwin linux

clean:
	$(CARGO) clean
	rm -rf build
