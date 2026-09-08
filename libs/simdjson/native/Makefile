# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

# Builds libsimdjson for all three targets:
#   build/libs/simdjson/shared/aarch64/libsimdjson.dylib  (darwin-aarch64)
#   build/libs/simdjson/shared/aarch64/libsimdjson.so     (linux-aarch64)
#   build/libs/simdjson/shared/amd64/libsimdjson.so       (linux-x64)
#
# simdjson handles ISA selection internally (AVX-512 vs AVX2 vs NEON etc.),
# so we compile a single source file per platform — no tier-1/tier-2 split.
#
# Intended to be run inside the shared cross-compilation toolchain container
# (es-native-cross-toolchain:6 — clang-21 plus GCC 8 libstdc++ cross libs and
# the open-source Darwin sysroot baked in). See publish_simdjson_binaries.sh.
#
# Linux cross builds pin libstdc++ to GCC 8 (GLIBCXX_3.4.25) while keeping
# clang-21 codegen. Clang supplies intrinsics/SIMD headers; only C++ stdlib
# headers and the link-time libstdc++.so ABI come from GCC 8.

CXXFLAGS   = -std=c++17 -O3 -fpic -g -fstandalone-debug -DNDEBUG

# GCC libstdc++ major version for Linux cross-compilation (RHEL 8 / glibc 2.28).
LINUX_STDCXX_MAJOR ?= 8

# Compiler base — defaults to clang++-21 (the version in the Docker toolchain
# image). Override via env for local builds: CLANG_CXX=clang++ make local
CLANG_CXX ?= clang++-21

# Derive the LLVM tool version suffix from CLANG_CXX
LLVM_SUFFIX := $(patsubst clang++%,%,$(firstword $(CLANG_CXX)))

# Darwin cross-compilation. The sysroot is assembled from Apple's open-source
# distributions plus upstream libc++ headers and baked into the toolchain image; see
# libs/simdvec/native/darwin-sysroot/assemble.sh. -nostdinc blocks the host (glibc)
# headers, then the three -isystem paths are added in this order deliberately:
# libc++ must come before the clang resource directory (otherwise libc++'s <cstddef>
# cannot find its own <stddef.h>), and the resource directory before the sysroot so
# clang supplies the target headers (arm_neon.h, arm_sve.h, stdarg.h).
MACOS_SYSROOT ?= /opt/darwin-sysroot
CLANG_RESOURCE = $(shell $(CLANG_CXX) -print-resource-dir)
CLANG       = $(CLANG_CXX) --target=arm64-apple-macos14 -nostdinc \
                -isystem $(MACOS_SYSROOT)/usr/include/c++/v1 \
                -isystem $(CLANG_RESOURCE)/include \
                -isystem $(MACOS_SYSROOT)/usr/include
CLANG_LINK  = $(CLANG_CXX) --target=arm64-apple-macos14 -fuse-ld=lld \
                -nostdlib -Wl,-undefined,dynamic_lookup
CLANG_RESOURCE_ARM = $(shell $(CLANG_CXX) --target=aarch64-linux-gnu -print-resource-dir)
CLANG_RESOURCE_X64 = $(shell $(CLANG_CXX) --target=x86_64-linux-gnu -print-resource-dir)

# -nostdinc++ keeps clang from picking up libstdc++-14; include paths below
# pair clang intrinsics with GCC 8 libstdc++ (see Dockerfile.cross-toolchain).
LINUX_CXXFLAGS_ARM = -nostdinc++ \
  -isystem $(CLANG_RESOURCE_ARM)/include \
  -isystem /usr/aarch64-linux-gnu/include \
  -isystem /usr/aarch64-linux-gnu/include/c++/$(LINUX_STDCXX_MAJOR) \
  -isystem /usr/aarch64-linux-gnu/include/c++/$(LINUX_STDCXX_MAJOR)/aarch64-linux-gnu
LINUX_CXXFLAGS_X64 = -nostdinc++ \
  -isystem $(CLANG_RESOURCE_X64)/include \
  -isystem /usr/x86_64-linux-gnu/include \
  -isystem /usr/x86_64-linux-gnu/include/c++/$(LINUX_STDCXX_MAJOR) \
  -isystem /usr/x86_64-linux-gnu/include/c++/$(LINUX_STDCXX_MAJOR)/x86_64-linux-gnu
LINUX_LDFLAGS_ARM = -L/usr/lib/gcc-cross/aarch64-linux-gnu/$(LINUX_STDCXX_MAJOR) \
  -L/usr/aarch64-linux-gnu/lib
LINUX_LDFLAGS_X64 = -L/usr/lib/gcc-cross/x86_64-linux-gnu/$(LINUX_STDCXX_MAJOR) \
  -L/usr/x86_64-linux-gnu/lib

CXX_ARM     = $(CLANG_CXX) --target=aarch64-linux-gnu $(LINUX_CXXFLAGS_ARM)
CXX_X64     = $(CLANG_CXX) --target=x86_64-linux-gnu $(LINUX_CXXFLAGS_X64)

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
OUT_DIR_AARCH64 = build/libs/simdjson/shared/aarch64
OUT_DIR_AMD64   = build/libs/simdjson/shared/amd64
DYLIB  = $(OUT_DIR_AARCH64)/libsimdjson.dylib
SO_ARM = $(OUT_DIR_AARCH64)/libsimdjson.so
SO_X64 = $(OUT_DIR_AMD64)/libsimdjson.so

.PHONY: all clean local local-install install verify-linux-abi

# --- local: build for the current platform using the native compiler ---

ifeq ($(shell uname -s),Darwin)
  LOCAL_PLATFORM = darwin-aarch64
  LOCAL_OUTPUT   = $(DYLIB)
  local: CLANG = clang++
  local: CLANG_LINK = clang++
  local: DARWIN_STRIP = strip -x
  local: DARWIN_DSYMUTIL = dsymutil
  local: $(DYLIB)
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

# Build and copy to a specified destination directory (used by the Gradle task).
local-install: local
ifndef DEST
	$(error DEST must be set, e.g. make local-install DEST=/path/to/output)
endif
	mkdir -p $(DEST)/$(LOCAL_PLATFORM)
	cp $(LOCAL_OUTPUT) $(DEST)/$(LOCAL_PLATFORM)/$(notdir $(LOCAL_OUTPUT))

# Convenience target for manual installs into the native-libraries extract tree.
INSTALL_DIR = ../../native/libraries/build/platform/$(LOCAL_PLATFORM)
install: local
	mkdir -p $(INSTALL_DIR)
	cp $(LOCAL_OUTPUT) $(INSTALL_DIR)/$(notdir $(LOCAL_OUTPUT))

# --- all: cross-compile all three targets (inside Docker) ---

all: $(DYLIB) $(SO_ARM) $(SO_X64)

# --- darwin-aarch64 ---

$(OBJ_DARWIN)/es_simdjson.o: src/es_simdjson.cpp src/simdjson.h | $(OBJ_DARWIN)
	$(CLANG) $(CXXFLAGS) -c $< -o $@

$(OBJ_DARWIN)/simdjson.o: src/simdjson.cpp src/simdjson.h | $(OBJ_DARWIN)
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
	$(CXX_ARM) -fuse-ld=lld -shared $(LINUX_LDFLAGS_ARM) $^ -lstdc++ -lm -lgcc_s -lc -o $@
	$(LINUX_OBJCOPY) --only-keep-debug $@ $@.debug
	$(LINUX_STRIP) $@
	$(LINUX_OBJCOPY) --add-gnu-debuglink=$@.debug $@

# --- linux-x64 ---

$(OBJ_LIN_X64)/es_simdjson.o: src/es_simdjson.cpp src/simdjson.h | $(OBJ_LIN_X64)
	$(CXX_X64) $(CXXFLAGS) -c $< -o $@

$(OBJ_LIN_X64)/simdjson.o: src/simdjson.cpp src/simdjson.h | $(OBJ_LIN_X64)
	$(CXX_X64) $(CXXFLAGS) -c $< -o $@

$(SO_X64): $(OBJ_LIN_X64)/es_simdjson.o $(OBJ_LIN_X64)/simdjson.o | $(OUT_DIR_AMD64)
	$(CXX_X64) -fuse-ld=lld -shared $(LINUX_LDFLAGS_X64) $^ -lstdc++ -lm -lgcc_s -lc -o $@
	$(LINUX_OBJCOPY) --only-keep-debug $@ $@.debug
	$(LINUX_STRIP) $@
	$(LINUX_OBJCOPY) --add-gnu-debuglink=$@.debug $@

# --- directories ---

$(OBJ_DARWIN) $(OBJ_LIN_ARM) $(OBJ_LIN_X64) $(OUT_DIR_AARCH64) $(OUT_DIR_AMD64):
	mkdir -p $@

# Fail the build if Linux .so files require GLIBCXX > 3.4.25 or GLIBC > 2.28
# (RHEL 8 baseline). Run after `make all` inside the simdjson toolchain image.
verify-linux-abi: $(SO_ARM) $(SO_X64)
	@for so in $(SO_ARM) $(SO_X64); do \
	  echo "Checking $$so ..."; \
	  if objdump -p "$$so" | grep -q 'GLIBCXX_3\.4\.\(2[6-9]\|3[0-9]\)'; then \
	    echo "Error: $$so requires GLIBCXX > 3.4.25 (not compatible with RHEL 8)"; exit 1; \
	  fi; \
	  if objdump -p "$$so" | grep -qE 'GLIBC_2\.(29|[3-9][0-9])'; then \
	    echo "Error: $$so requires GLIBC > 2.28 (not compatible with RHEL 8)"; exit 1; \
	  fi; \
	done
	@echo "Linux ABI check passed (GLIBCXX <= 3.4.25, GLIBC <= 2.28)."

clean:
	rm -rf build
