# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

# Builds libvec for all three targets:
#   build/libs/vec/shared/aarch64/libvec.dylib  (darwin-aarch64)
#   build/libs/vec/shared/aarch64/libvec.so     (linux-aarch64)
#   build/libs/vec/shared/amd64/libvec.so       (linux-x64)
#
# *_2.cpp files are compiled with a higher -march that enables tier-2 ISA
# extensions (SVE for aarch64, AVX-512/icelake for amd64). All other files
# use the baseline -march for each architecture.
#
# Intended to be run inside the cross-compilation toolchain container.
# See publish_vec_binaries.sh.

HEADERS    = -I src/vec/headers/
CXXFLAGS   = -std=c++17 -O3 -fpic -DNDEBUG

# Per-architecture march flags
MARCH_AARCH64_TIER1 = armv8.2-a+dotprod
MARCH_AARCH64_TIER2 = armv8.2-a+sve
MARCH_AMD64_TIER1   = core-avx2
MARCH_AMD64_TIER2   = icelake-client

# Compilers
# Darwin cross-compilation: -nostdinc blocks host (glibc) headers, then we
# add our minimal C++ std headers (tinystd) and clang builtins.
CLANG_RESOURCE = $(shell clang++-18 -print-resource-dir)
CLANG       = clang++-18 --target=arm64-apple-macos14 -Winline -nostdinc \
                -isystem src/vec/headers/darwin/tinystd \
                -isystem $(CLANG_RESOURCE)/include
CLANG_LINK  = clang++-18 --target=arm64-apple-macos14 -fuse-ld=lld -nostdlib -Wl,-undefined,dynamic_lookup
CLANG_STRIP = llvm-strip-18 --strip-debug
GCC_ARM     = aarch64-linux-gnu-g++-14 -Winline
GCC_X64     = g++-14 -Winline

# Sources split by tier
SRC_AARCH64_TIER1 = $(filter-out %_2.cpp, $(wildcard src/vec/c/aarch64/*.cpp))
SRC_AARCH64_TIER2 = $(wildcard src/vec/c/aarch64/*_2.cpp)
SRC_AMD64_TIER1   = $(filter-out %_2.cpp, $(wildcard src/vec/c/amd64/*.cpp))
SRC_AMD64_TIER2   = $(wildcard src/vec/c/amd64/*_2.cpp)

# Object directories
OBJ_DARWIN  = build/obj/darwin-aarch64
OBJ_LIN_ARM = build/obj/linux-aarch64
OBJ_LIN_X64 = build/obj/linux-x64

# Object files
OBJ_DARWIN_TIER1  = $(SRC_AARCH64_TIER1:src/vec/c/aarch64/%.cpp=$(OBJ_DARWIN)/%.o)
OBJ_DARWIN_TIER2  = $(SRC_AARCH64_TIER2:src/vec/c/aarch64/%.cpp=$(OBJ_DARWIN)/%.o)
OBJ_LIN_ARM_TIER1 = $(SRC_AARCH64_TIER1:src/vec/c/aarch64/%.cpp=$(OBJ_LIN_ARM)/%.o)
OBJ_LIN_ARM_TIER2 = $(SRC_AARCH64_TIER2:src/vec/c/aarch64/%.cpp=$(OBJ_LIN_ARM)/%.o)
OBJ_LIN_X64_TIER1 = $(SRC_AMD64_TIER1:src/vec/c/amd64/%.cpp=$(OBJ_LIN_X64)/%.o)
OBJ_LIN_X64_TIER2 = $(SRC_AMD64_TIER2:src/vec/c/amd64/%.cpp=$(OBJ_LIN_X64)/%.o)

# Output files
OUT_DIR_AARCH64 = build/libs/vec/shared/aarch64
OUT_DIR_AMD64   = build/libs/vec/shared/amd64
DYLIB  = $(OUT_DIR_AARCH64)/libvec.dylib
SO_ARM = $(OUT_DIR_AARCH64)/libvec.so
SO_X64 = $(OUT_DIR_AMD64)/libvec.so

# --- local: build for the current platform using the native compiler ---
# On macOS, overrides CLANG/CLANG_LINK to use the system clang (SDK available natively).
# On Linux, the GCC variables already use the native compiler.

.PHONY: all clean local install

ifeq ($(shell uname -s),Darwin)
  LOCAL_PLATFORM = darwin-aarch64
  LOCAL_OUTPUT   = $(DYLIB)
  local: CLANG = clang++ -Winline
  local: CLANG_LINK = clang++
  local: CLANG_STRIP = strip -x
  local: $(DYLIB)
else ifeq ($(shell uname -m),aarch64)
  LOCAL_PLATFORM = linux-aarch64
  LOCAL_OUTPUT   = $(SO_ARM)
  local: $(SO_ARM)
else
  LOCAL_PLATFORM = linux-x64
  LOCAL_OUTPUT   = $(SO_X64)
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

$(OBJ_DARWIN_TIER1): $(OBJ_DARWIN)/%.o: src/vec/c/aarch64/%.cpp | $(OBJ_DARWIN)
	$(CLANG) $(CXXFLAGS) -march=$(MARCH_AARCH64_TIER1) $(HEADERS) -c $< -o $@

$(OBJ_DARWIN_TIER2): $(OBJ_DARWIN)/%.o: src/vec/c/aarch64/%.cpp | $(OBJ_DARWIN)
	$(CLANG) $(CXXFLAGS) -march=$(MARCH_AARCH64_TIER2) $(HEADERS) -c $< -o $@

$(DYLIB): $(OBJ_DARWIN_TIER1) $(OBJ_DARWIN_TIER2) | $(OUT_DIR_AARCH64)
	$(CLANG_LINK) -shared $^ -o $@
	$(CLANG_STRIP) $@

# --- linux-aarch64 ---

$(OBJ_LIN_ARM_TIER1): $(OBJ_LIN_ARM)/%.o: src/vec/c/aarch64/%.cpp | $(OBJ_LIN_ARM)
	$(GCC_ARM) $(CXXFLAGS) -march=$(MARCH_AARCH64_TIER1) $(HEADERS) -c $< -o $@

$(OBJ_LIN_ARM_TIER2): $(OBJ_LIN_ARM)/%.o: src/vec/c/aarch64/%.cpp | $(OBJ_LIN_ARM)
	$(GCC_ARM) $(CXXFLAGS) -march=$(MARCH_AARCH64_TIER2) $(HEADERS) -c $< -o $@

$(SO_ARM): $(OBJ_LIN_ARM_TIER1) $(OBJ_LIN_ARM_TIER2) | $(OUT_DIR_AARCH64)
	$(GCC_ARM) -shared $^ -o $@
	aarch64-linux-gnu-strip --strip-unneeded $@

# --- linux-x64 ---

$(OBJ_LIN_X64_TIER1): $(OBJ_LIN_X64)/%.o: src/vec/c/amd64/%.cpp | $(OBJ_LIN_X64)
	$(GCC_X64) $(CXXFLAGS) -march=$(MARCH_AMD64_TIER1) $(HEADERS) -c $< -o $@

$(OBJ_LIN_X64_TIER2): $(OBJ_LIN_X64)/%.o: src/vec/c/amd64/%.cpp | $(OBJ_LIN_X64)
	$(GCC_X64) $(CXXFLAGS) -march=$(MARCH_AMD64_TIER2) $(HEADERS) -c $< -o $@

$(SO_X64): $(OBJ_LIN_X64_TIER1) $(OBJ_LIN_X64_TIER2) | $(OUT_DIR_AMD64)
	$(GCC_X64) -shared $^ -o $@
	strip --strip-unneeded $@

# --- directories ---

$(OBJ_DARWIN) $(OBJ_LIN_ARM) $(OBJ_LIN_X64) $(OUT_DIR_AARCH64) $(OUT_DIR_AMD64):
	mkdir -p $@

clean:
	rm -rf build/obj build/libs/vec/shared
