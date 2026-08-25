# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

# Builds libes_simdjson for all three targets:
#   build/libs/es_simdjson/shared/aarch64/libes_simdjson.dylib  (darwin-aarch64)
#   build/libs/es_simdjson/shared/aarch64/libes_simdjson.so     (linux-aarch64)
#   build/libs/es_simdjson/shared/amd64/libes_simdjson.so       (linux-x64)
#
# simdjson handles ISA selection internally (AVX-512 vs AVX2 vs NEON etc.),
# so we compile a single source file per platform — no tier-1/tier-2 split.
#
# Intended to be run inside the cross-compilation toolchain container.
# The Darwin target requires a macOS SDK, fetched on-demand via xmac from
# Apple's public CDN and cached in build/.macos-sdk/.

CXXFLAGS   = -std=c++17 -O3 -fpic -g -fstandalone-debug -DNDEBUG

# Compiler base — defaults to clang++-21 (the version in the Docker toolchain
# image). Override via env for local builds: CLANG_CXX=clang++ make local
CLANG_CXX ?= clang++-21

# Derive the LLVM tool version suffix from CLANG_CXX
LLVM_SUFFIX := $(patsubst clang++%,%,$(firstword $(CLANG_CXX)))

# --- macOS SDK (fetched on demand via xmac) ---
MACOS_SDK_DIR  = build/.macos-sdk
MACOS_SDK_OK   = $(MACOS_SDK_DIR)/.ok
MACOS_SDK      = $(MACOS_SDK_DIR)/SDKs/MacOSX.sdk
XMAC_ARCH      := $(if $(filter aarch64,$(shell uname -m)),arm64,x64)
XMAC_URL       = https://github.com/jarred-sumner/xmac/releases/latest/download/xmac-linux-$(XMAC_ARCH)

# Darwin cross-compilation using the macOS SDK sysroot
CLANG       = $(CLANG_CXX) --target=arm64-apple-macos14 -isysroot $(MACOS_SDK)
CLANG_LINK  = $(CLANG_CXX) --target=arm64-apple-macos14 -fuse-ld=lld \
                -isysroot $(MACOS_SDK) -nostdlib -Wl,-undefined,dynamic_lookup
CXX_ARM     = $(CLANG_CXX) --target=aarch64-linux-gnu
CXX_X64     = $(CLANG_CXX) --target=x86_64-linux-gnu

# Strip and debug-info commands per target platform
DARWIN_STRIP    = llvm-strip$(LLVM_SUFFIX) --strip-debug
LINUX_STRIP     = llvm-strip$(LLVM_SUFFIX) --strip-unneeded
LINUX_OBJCOPY   = llvm-objcopy$(LLVM_SUFFIX)
DARWIN_DSYMUTIL = $(shell llvm-config$(LLVM_SUFFIX) --bindir)/dsymutil

# Source files
SRC = src/es_simdjson.cpp src/simdjson.cpp

# Object directories
OBJ_DARWIN  = build/obj/darwin-aarch64
OBJ_LIN_ARM = build/obj/linux-aarch64
OBJ_LIN_X64 = build/obj/linux-x64

# Output files
OUT_DIR_AARCH64 = build/libs/es_simdjson/shared/aarch64
OUT_DIR_AMD64   = build/libs/es_simdjson/shared/amd64
DYLIB  = $(OUT_DIR_AARCH64)/libes_simdjson.dylib
SO_ARM = $(OUT_DIR_AARCH64)/libes_simdjson.so
SO_X64 = $(OUT_DIR_AMD64)/libes_simdjson.so

# --- macOS SDK fetch (cached in build volume) ---

.PHONY: all clean local install ensure-macos-sdk local-sdk-stub

ensure-macos-sdk: $(MACOS_SDK_OK)

$(MACOS_SDK_OK):
	@echo "Fetching macOS SDK via xmac..."
	@mkdir -p $(MACOS_SDK_DIR)
	@curl -fsSL $(XMAC_URL) -o $(MACOS_SDK_DIR)/xmac
	@chmod +x $(MACOS_SDK_DIR)/xmac
	$(MACOS_SDK_DIR)/xmac splat --accept-license --sdk 14 --sdk-only \
	    --output $(MACOS_SDK_DIR) --cache-dir build/.xmac-cache
	@touch $@

# --- local: build for the current platform using the native compiler ---

ifeq ($(shell uname -s),Darwin)
  LOCAL_PLATFORM = darwin-aarch64
  LOCAL_OUTPUT   = $(DYLIB)
  local: CLANG = clang++
  local: CLANG_LINK = clang++
  local: DARWIN_STRIP = strip -x
  local: DARWIN_DSYMUTIL = dsymutil
  local: local-sdk-stub $(DYLIB)
  local-sdk-stub:
	@mkdir -p $(MACOS_SDK_DIR) && touch $(MACOS_SDK_OK)
else ifeq ($(shell uname -m),aarch64)
  LOCAL_PLATFORM = linux-aarch64
  LOCAL_OUTPUT   = $(SO_ARM)
  local: CXX_ARM = $(CLANG_CXX)
  local: LINUX_STRIP = strip --strip-unneeded
  local: LINUX_OBJCOPY = objcopy
  local: $(SO_ARM)
else
  LOCAL_PLATFORM = linux-x64
  LOCAL_OUTPUT   = $(SO_X64)
  local: CXX_X64 = $(CLANG_CXX)
  local: LINUX_STRIP = strip --strip-unneeded
  local: LINUX_OBJCOPY = objcopy
  local: $(SO_X64)
endif

# Build and copy to the platform directory used by Gradle tests.
INSTALL_DIR = ../../native/libraries/build/platform/$(LOCAL_PLATFORM)
install: local
	mkdir -p $(INSTALL_DIR)
	cp $(LOCAL_OUTPUT) $(INSTALL_DIR)/$(notdir $(LOCAL_OUTPUT))

# --- all: cross-compile all three targets (inside Docker) ---

all: $(DYLIB) $(SO_ARM) $(SO_X64)

# --- darwin-aarch64 ---

$(OBJ_DARWIN)/es_simdjson.o: src/es_simdjson.cpp src/simdjson.h | $(OBJ_DARWIN) $(MACOS_SDK_OK)
	$(CLANG) $(CXXFLAGS) -c $< -o $@

$(OBJ_DARWIN)/simdjson.o: src/simdjson.cpp src/simdjson.h | $(OBJ_DARWIN) $(MACOS_SDK_OK)
	$(CLANG) $(CXXFLAGS) -c $< -o $@

$(DYLIB): $(OBJ_DARWIN)/es_simdjson.o $(OBJ_DARWIN)/simdjson.o | $(OUT_DIR_AARCH64)
	$(CLANG_LINK) -shared $^ -o $@
	$(DARWIN_DSYMUTIL) $@ -o $@.dSYM
	$(DARWIN_STRIP) $@

# --- linux-aarch64 ---

$(OBJ_LIN_ARM)/es_simdjson.o: src/es_simdjson.cpp src/simdjson.h | $(OBJ_LIN_ARM)
	$(CXX_ARM) $(CXXFLAGS) -c $< -o $@

$(OBJ_LIN_ARM)/simdjson.o: src/simdjson.cpp src/simdjson.h | $(OBJ_LIN_ARM)
	$(CXX_ARM) $(CXXFLAGS) -c $< -o $@

$(SO_ARM): $(OBJ_LIN_ARM)/es_simdjson.o $(OBJ_LIN_ARM)/simdjson.o | $(OUT_DIR_AARCH64)
	$(CXX_ARM) -fuse-ld=lld -shared $^ -o $@
	$(LINUX_OBJCOPY) --only-keep-debug $@ $@.debug
	$(LINUX_STRIP) $@
	$(LINUX_OBJCOPY) --add-gnu-debuglink=$@.debug $@

# --- linux-x64 ---

$(OBJ_LIN_X64)/es_simdjson.o: src/es_simdjson.cpp src/simdjson.h | $(OBJ_LIN_X64)
	$(CXX_X64) $(CXXFLAGS) -c $< -o $@

$(OBJ_LIN_X64)/simdjson.o: src/simdjson.cpp src/simdjson.h | $(OBJ_LIN_X64)
	$(CXX_X64) $(CXXFLAGS) -c $< -o $@

$(SO_X64): $(OBJ_LIN_X64)/es_simdjson.o $(OBJ_LIN_X64)/simdjson.o | $(OUT_DIR_AMD64)
	$(CXX_X64) -fuse-ld=lld -shared $^ -o $@
	$(LINUX_OBJCOPY) --only-keep-debug $@ $@.debug
	$(LINUX_STRIP) $@
	$(LINUX_OBJCOPY) --add-gnu-debuglink=$@.debug $@

# --- directories ---

$(OBJ_DARWIN) $(OBJ_LIN_ARM) $(OBJ_LIN_X64) $(OUT_DIR_AARCH64) $(OUT_DIR_AMD64):
	mkdir -p $@

clean:
	rm -rf build
