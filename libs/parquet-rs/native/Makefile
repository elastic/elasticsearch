# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

# Builds libesql_parquet_rs for all three targets:
#   build/darwin-aarch64/libesql_parquet_rs.dylib  (native macOS build)
#   build/linux-aarch64/libesql_parquet_rs.so      (GCC cross-compiler in Docker)
#   build/linux-x64/libesql_parquet_rs.so          (GCC cross-compiler in Docker)
#
# Usage:
#   make local       - build for the current platform
#   make install     - build and copy to the platform directory used by Gradle tests
#   make darwin      - build the Darwin binary (run on macOS)
#   make linux       - cross-compile both Linux targets (run inside Docker)
#   make all         - build all targets (darwin natively, then linux in Docker)
#   make clean       - remove build artifacts
#
# The Darwin binary must be built on macOS (cross-compilation from Linux
# requires the macOS SDK due to framework dependencies like CoreFoundation).
# The Linux binaries are cross-compiled inside a Docker container.
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

LIB_NAME = libesql_parquet_rs.$(LIB_EXT)
CARGO_OUTPUT = target/release/$(LIB_NAME)
INSTALL_DIR = ../../native/libraries/build/platform/$(PLATFORM_DIR)

# --- Cross-compilation targets ---

TARGET_LIN_ARM = aarch64-unknown-linux-gnu
TARGET_LIN_X64 = x86_64-unknown-linux-gnu

OUT_DARWIN  = build/darwin-aarch64/libesql_parquet_rs.dylib
OUT_LIN_ARM = build/linux-aarch64/libesql_parquet_rs.so
OUT_LIN_X64 = build/linux-x64/libesql_parquet_rs.so

.PHONY: local install darwin linux all clean

# --- local: build for the current platform ---

local:
	$(CARGO) build --release

install: local
	mkdir -p $(INSTALL_DIR)
	cp $(CARGO_OUTPUT) $(INSTALL_DIR)/$(LIB_NAME)

# --- darwin: build on macOS natively ---

darwin: $(OUT_DARWIN)

$(OUT_DARWIN):
	$(CARGO) build --release
	mkdir -p $(dir $@)
	cp target/release/libesql_parquet_rs.dylib $@
	strip -x $@

# --- linux: cross-compile both Linux targets (inside Docker) ---

linux: $(OUT_LIN_ARM) $(OUT_LIN_X64)

$(OUT_LIN_ARM):
	CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER="aarch64-linux-gnu-gcc" \
	  $(CARGO) build --release --target $(TARGET_LIN_ARM)
	mkdir -p $(dir $@)
	cp target/$(TARGET_LIN_ARM)/release/libesql_parquet_rs.so $@
	aarch64-linux-gnu-strip --strip-unneeded $@

$(OUT_LIN_X64):
	CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="x86_64-linux-gnu-gcc" \
	  $(CARGO) build --release --target $(TARGET_LIN_X64)
	mkdir -p $(dir $@)
	cp target/$(TARGET_LIN_X64)/release/libesql_parquet_rs.so $@
	x86_64-linux-gnu-strip --strip-unneeded $@

# --- all: build everything ---
# Darwin is built natively on the host (must be macOS).
# Linux targets are cross-compiled inside Docker.
# publish_pqrs_binaries.sh orchestrates both steps.

all: darwin linux

clean:
	$(CARGO) clean
	rm -rf build
