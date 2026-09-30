# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

# Builds libopenblas for all four targets:
#   build/libs/openblas/shared/aarch64/libopenblas.dylib     (darwin-aarch64)
#   build/libs/openblas/shared/aarch64/libopenblas.so        (linux-aarch64)
#   build/libs/openblas/shared/amd64/libopenblas.so          (linux-x64)
#   build/libs/openblas/shared/windows-x64/openblas.dll      (windows-x64)
#
# OpenBLAS is built with DYNAMIC_ARCH=1 so one binary handles all ISA variants
# at runtime. Threads are disabled (USE_THREAD=0 USE_LOCKING=1).
#
# Intended to be run inside the shared cross-compilation toolchain container
# (es-native-cross-toolchain:7 — clang-21, GCC 8 libstdc++ cross libs, and llvm-mingw;
# the open-source Darwin sysroot baked in). See publish_blas_binaries.sh.
#
# Sources are fetched from GitHub at build time and verified via SHA-256. They
# are not vendored because the OpenBLAS tree is large (tens of MB of kernels).
#
# Linux cross builds pin libstdc++ to GCC 8 (GLIBCXX_3.4.25) while keeping
# clang-21 codegen. Clang supplies intrinsics/SIMD headers; only C++ stdlib
# headers and the link-time libstdc++.so ABI come from GCC 8.

OPENBLAS_VERSION  ?= 0.3.34
OPENBLAS_SHA256   ?= cd7e129868320cc2d033afa920e31202dfe0b8066a5b66661900ccc0f197dfed

# Compiler base — defaults to clang++-21 (the version in the Docker toolchain
# image). Override via env for local builds: CLANG_CXX=clang++ make local
CLANG_CXX  ?= clang++-21
CLANG_CC   ?= clang-21

# GCC libstdc++ major version for Linux cross-compilation (RHEL 8 / glibc 2.28).
LINUX_STDCXX_MAJOR ?= 8

# Derive the LLVM tool version suffix from CLANG_CXX
LLVM_SUFFIX := $(patsubst clang++%,%,$(firstword $(CLANG_CXX)))

OPENBLAS_SRC_URL  = https://github.com/OpenMathLib/OpenBLAS/releases/download/v$(OPENBLAS_VERSION)/OpenBLAS-$(OPENBLAS_VERSION).tar.gz

SRC_DIR    = build/src/OpenBLAS-$(OPENBLAS_VERSION)
TARBALL    = build/src/openblas-$(OPENBLAS_VERSION).tar.gz

# OpenBLAS flags shared across all targets.
# DYNAMIC_ARCH=1: runtime CPU dispatch (AVX2, AVX-512 on x64; NEON variants on aarch64)
# USE_THREAD=0 USE_LOCKING=1: single-threaded, no pthreads
# NOFORTRAN=1 NO_LAPACK=1 NO_LAPACKE=1 ONLY_CBLAS=1: only the CBLAS C interface
# NO_STATIC=1: produce a shared library only
OPENBLAS_COMMON_FLAGS = \
    DYNAMIC_ARCH=1 \
    USE_THREAD=0 \
    USE_LOCKING=1 \
    NOFORTRAN=1 \
    NO_LAPACK=1 \
    NO_LAPACKE=1 \
    ONLY_CBLAS=1 \
    NO_STATIC=1 \
    NUM_THREADS=1

# Darwin cross-compilation settings (reuse the sysroot from simdvec/native).
MACOS_SYSROOT ?= /opt/darwin-sysroot
CLANG_RESOURCE = $(shell $(CLANG_CXX) -print-resource-dir)

# Linux ABI settings
CLANG_RESOURCE_ARM = $(shell $(CLANG_CXX) --target=aarch64-linux-gnu -print-resource-dir)
CLANG_RESOURCE_X64 = $(shell $(CLANG_CXX) --target=x86_64-linux-gnu -print-resource-dir)

LINUX_CFLAGS_ARM = -nostdinc++ \
    -isystem $(CLANG_RESOURCE_ARM)/include \
    -isystem /usr/aarch64-linux-gnu/include \
    -isystem /usr/aarch64-linux-gnu/include/c++/$(LINUX_STDCXX_MAJOR) \
    -isystem /usr/aarch64-linux-gnu/include/c++/$(LINUX_STDCXX_MAJOR)/aarch64-linux-gnu
LINUX_CFLAGS_X64 = -nostdinc++ \
    -isystem $(CLANG_RESOURCE_X64)/include \
    -isystem /usr/x86_64-linux-gnu/include \
    -isystem /usr/x86_64-linux-gnu/include/c++/$(LINUX_STDCXX_MAJOR) \
    -isystem /usr/x86_64-linux-gnu/include/c++/$(LINUX_STDCXX_MAJOR)/x86_64-linux-gnu
LINUX_LDFLAGS_ARM = -L/usr/lib/gcc-cross/aarch64-linux-gnu/$(LINUX_STDCXX_MAJOR) \
    -L/usr/aarch64-linux-gnu/lib
LINUX_LDFLAGS_X64 = -L/usr/lib/gcc-cross/x86_64-linux-gnu/$(LINUX_STDCXX_MAJOR) \
    -L/usr/x86_64-linux-gnu/lib

# Per-platform CC and LDFLAGS passed to OpenBLAS's sub-make.
# For host (local) builds the local target overrides these to plain cc/empty.
CC_ARM      = $(CLANG_CC) --target=aarch64-linux-gnu $(LINUX_CFLAGS_ARM)
CC_X64      = $(CLANG_CC) --target=x86_64-linux-gnu $(LINUX_CFLAGS_X64)
LDFLAGS_ARM = -fuse-ld=lld $(LINUX_LDFLAGS_ARM)
LDFLAGS_X64 = -fuse-ld=lld $(LINUX_LDFLAGS_X64)

# Output directories
OUT_DIR_AARCH64  = build/libs/openblas/shared/aarch64
OUT_DIR_AMD64    = build/libs/openblas/shared/amd64
OUT_DIR_WIN_X64  = build/libs/openblas/shared/windows-x64

# Build trees (one per platform — OpenBLAS embeds arch detection results)
BUILD_DARWIN     = build/openblas-darwin-aarch64
BUILD_LINUX_ARM  = build/openblas-linux-aarch64
BUILD_LINUX_X64  = build/openblas-linux-x64
BUILD_WIN_X64    = build/openblas-windows-x64

# Final outputs
DYLIB    = $(OUT_DIR_AARCH64)/libopenblas.dylib
SO_ARM   = $(OUT_DIR_AARCH64)/libopenblas.so
SO_X64   = $(OUT_DIR_AMD64)/libopenblas.so
DLL_WIN  = $(OUT_DIR_WIN_X64)/openblas.dll

# Strip tools
DARWIN_STRIP    = llvm-strip$(LLVM_SUFFIX) --strip-debug
LINUX_STRIP     = llvm-strip$(LLVM_SUFFIX) --strip-unneeded
LINUX_OBJCOPY   = llvm-objcopy$(LLVM_SUFFIX)
DARWIN_DSYMUTIL = $(shell llvm-config$(LLVM_SUFFIX) --bindir 2>/dev/null)/dsymutil

.PHONY: all fetch clean local local-install verify-linux-abi

# --- fetch: download and verify OpenBLAS sources ---

$(TARBALL):
	mkdir -p build/src
	curl -fL --retry 3 --retry-delay 2 -o $(TARBALL) $(OPENBLAS_SRC_URL)
	echo "$(OPENBLAS_SHA256)  $(TARBALL)" | sha256sum -c -

$(SRC_DIR): $(TARBALL)
	tar -C build/src -xzf $(TARBALL)
	touch $(SRC_DIR)

fetch: $(SRC_DIR)

# --- all: cross-compile all four targets (inside Docker) ---

all: $(DYLIB) $(SO_ARM) $(SO_X64) $(DLL_WIN)

# --- darwin-aarch64 ---
# The darwin cross build uses the same sysroot as simdvec.
# OpenBLAS's own Makefile calls the compiler via CC/FC; we set CC to our
# clang cross-compiler and disable Fortran entirely.

$(BUILD_DARWIN)/.done: $(SRC_DIR) | $(BUILD_DARWIN)
	cp -a $(SRC_DIR)/. $(BUILD_DARWIN)/
	touch $@

$(DYLIB): $(BUILD_DARWIN)/.done | $(OUT_DIR_AARCH64)
	$(MAKE) -C $(BUILD_DARWIN) \
	    CC="$(CLANG_CC) --target=arm64-apple-macos14 \
	        -nostdinc \
	        -isystem $(MACOS_SYSROOT)/usr/include/c++/v1 \
	        -isystem $(CLANG_RESOURCE)/include \
	        -isystem $(MACOS_SYSROOT)/usr/include" \
	    HOSTCC=cc \
	    TARGET=ARMV8 \
	    $(OPENBLAS_COMMON_FLAGS) \
	    LDFLAGS="-fuse-ld=lld --target=arm64-apple-macos14 \
	        -nostdlib -Wl,-undefined,dynamic_lookup" \
	    SONAME_PREFIX=""
	$(MAKE) -C $(BUILD_DARWIN) \
	    CC="$(CLANG_CC) --target=arm64-apple-macos14 \
	        -nostdinc \
	        -isystem $(MACOS_SYSROOT)/usr/include/c++/v1 \
	        -isystem $(CLANG_RESOURCE)/include \
	        -isystem $(MACOS_SYSROOT)/usr/include" \
	    HOSTCC=cc \
	    TARGET=ARMV8 \
	    $(OPENBLAS_COMMON_FLAGS) \
	    LDFLAGS="-fuse-ld=lld --target=arm64-apple-macos14 \
	        -nostdlib -Wl,-undefined,dynamic_lookup" \
	    SONAME_PREFIX="" \
	    PREFIX=$(abspath $(BUILD_DARWIN)/install) \
	    install
	cp $(BUILD_DARWIN)/install/lib/libopenblas.dylib $@
	-$(DARWIN_DSYMUTIL) $@ -o $@.dSYM 2>/dev/null || true
	$(DARWIN_STRIP) $@

# --- linux-aarch64 ---

$(BUILD_LINUX_ARM)/.done: $(SRC_DIR) | $(BUILD_LINUX_ARM)
	cp -a $(SRC_DIR)/. $(BUILD_LINUX_ARM)/
	touch $@

$(SO_ARM): $(BUILD_LINUX_ARM)/.done | $(OUT_DIR_AARCH64)
	$(MAKE) -C $(BUILD_LINUX_ARM) \
	    CC="$(CC_ARM)" \
	    HOSTCC=cc \
	    TARGET=ARMV8 \
	    $(OPENBLAS_COMMON_FLAGS) \
	    LDFLAGS="$(LDFLAGS_ARM)"
	$(MAKE) -C $(BUILD_LINUX_ARM) \
	    CC="$(CC_ARM)" \
	    HOSTCC=cc \
	    TARGET=ARMV8 \
	    $(OPENBLAS_COMMON_FLAGS) \
	    LDFLAGS="$(LDFLAGS_ARM)" \
	    PREFIX=$(abspath $(BUILD_LINUX_ARM)/install) \
	    install
	cp $(BUILD_LINUX_ARM)/install/lib/libopenblas.so $@
	$(LINUX_OBJCOPY) --only-keep-debug $@ $@.debug
	$(LINUX_STRIP) $@
	$(LINUX_OBJCOPY) --add-gnu-debuglink=$@.debug $@

# --- linux-x64 ---

$(BUILD_LINUX_X64)/.done: $(SRC_DIR) | $(BUILD_LINUX_X64)
	cp -a $(SRC_DIR)/. $(BUILD_LINUX_X64)/
	touch $@

$(SO_X64): $(BUILD_LINUX_X64)/.done | $(OUT_DIR_AMD64)
	$(MAKE) -C $(BUILD_LINUX_X64) \
	    CC="$(CC_X64)" \
	    HOSTCC=cc \
	    TARGET=HASWELL \
	    $(OPENBLAS_COMMON_FLAGS) \
	    LDFLAGS="$(LDFLAGS_X64)"
	$(MAKE) -C $(BUILD_LINUX_X64) \
	    CC="$(CC_X64)" \
	    HOSTCC=cc \
	    TARGET=HASWELL \
	    $(OPENBLAS_COMMON_FLAGS) \
	    LDFLAGS="$(LDFLAGS_X64)" \
	    PREFIX=$(abspath $(BUILD_LINUX_X64)/install) \
	    install
	cp $(BUILD_LINUX_X64)/install/lib/libopenblas.so $@
	$(LINUX_OBJCOPY) --only-keep-debug $@ $@.debug
	$(LINUX_STRIP) $@
	$(LINUX_OBJCOPY) --add-gnu-debuglink=$@.debug $@

# --- windows-x64 ---
# llvm-mingw provides x86_64-w64-mingw32-clang; OpenBLAS's Makefile understands CROSS=x86_64-w64-mingw32.

$(BUILD_WIN_X64)/.done: $(SRC_DIR) | $(BUILD_WIN_X64)
	cp -a $(SRC_DIR)/. $(BUILD_WIN_X64)/
	touch $@

$(DLL_WIN): $(BUILD_WIN_X64)/.done | $(OUT_DIR_WIN_X64)
	$(MAKE) -C $(BUILD_WIN_X64) \
	    CROSS=x86_64-w64-mingw32 \
	    HOSTCC=cc \
	    CC=x86_64-w64-mingw32-clang \
	    TARGET=HASWELL \
	    $(OPENBLAS_COMMON_FLAGS)
	$(MAKE) -C $(BUILD_WIN_X64) \
	    CROSS=x86_64-w64-mingw32 \
	    HOSTCC=cc \
	    CC=x86_64-w64-mingw32-clang \
	    TARGET=HASWELL \
	    $(OPENBLAS_COMMON_FLAGS) \
	    PREFIX=$(abspath $(BUILD_WIN_X64)/install) \
	    install
	cp $(BUILD_WIN_X64)/install/bin/libopenblas.dll $(DLL_WIN)

# --- local: build for the current platform using the native compiler ---

UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Darwin)
  LOCAL_PLATFORM = darwin-aarch64
  LOCAL_OUTPUT   = $(DYLIB)
  local: $(DYLIB)
else ifneq (,$(or $(findstring MINGW,$(UNAME_S)),$(findstring MSYS,$(UNAME_S)),$(findstring CYGWIN,$(UNAME_S))))
  LOCAL_PLATFORM = windows-x64
  LOCAL_OUTPUT   = $(DLL_WIN)
  local: $(DLL_WIN)
else ifeq ($(shell uname -m),aarch64)
  LOCAL_PLATFORM = linux-aarch64
  LOCAL_OUTPUT   = $(SO_ARM)
  # Host build: use the native compiler; drop cross-specific flags and LLVM strip/objcopy.
  local: CC_ARM      = cc
  local: LDFLAGS_ARM =
  local: LINUX_STRIP     = strip --strip-unneeded
  local: LINUX_OBJCOPY   = objcopy
  local: $(SO_ARM)
else
  LOCAL_PLATFORM = linux-x64
  LOCAL_OUTPUT   = $(SO_X64)
  # Host build: use the native compiler; drop cross-specific flags and LLVM strip/objcopy.
  local: CC_X64      = cc
  local: LDFLAGS_X64 =
  local: LINUX_STRIP     = strip --strip-unneeded
  local: LINUX_OBJCOPY   = objcopy
  local: $(SO_X64)
endif

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

# --- ABI check ---

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

# --- directories ---

$(BUILD_DARWIN) $(BUILD_LINUX_ARM) $(BUILD_LINUX_X64) $(BUILD_WIN_X64) \
$(OUT_DIR_AARCH64) $(OUT_DIR_AMD64) $(OUT_DIR_WIN_X64):
	mkdir -p $@

clean:
	rm -rf build
