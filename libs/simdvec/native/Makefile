# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

# Builds libvec for all four targets:
#   build/libs/vec/shared/aarch64/libvec.dylib     (darwin-aarch64)
#   build/libs/vec/shared/aarch64/libvec.so        (linux-aarch64)
#   build/libs/vec/shared/amd64/libvec.so          (linux-x64)
#   build/libs/vec/shared/windows-x64/vec.dll      (windows-x64)
#
# *_2.cpp files are compiled with a higher -march that enables tier-2 ISA
# extensions (SVE for aarch64, AVX-512/icelake for amd64). All other files
# use the baseline -march for each architecture.
#
# Intended to be run inside the cross-compilation toolchain container
# (es-native-cross-toolchain:7 — clang-21 plus GCC 8 libstdc++ cross libs and llvm-mingw;
# the open-source Darwin sysroot baked in). See publish_vec_binaries.sh.

HEADERS    = -I src/vec/headers/
CXXFLAGS   = -std=c++17 -O3 -fpic -g -fstandalone-debug -DNDEBUG

# Per-architecture march flags
MARCH_AARCH64_TIER1 = armv8.2-a+dotprod
MARCH_AARCH64_TIER2 = armv8.4-a+sve+bf16
MARCH_AMD64_TIER1   = core-avx2
MARCH_AMD64_TIER2   = icelake-client
MARCH_AMD64_TIER3   = cooperlake

# Compiler base — defaults to clang++-21 (the version in the Docker toolchain
# image, chosen for its codegen quality). Override via env for local builds
# with a different clang version, e.g. CLANG_CXX=clang++-14 make local
CLANG_CXX ?= clang++-21

# Derive the LLVM tool version suffix from CLANG_CXX (clang++-21 -> -21, clang++ -> empty)
LLVM_SUFFIX := $(patsubst clang++%,%,$(firstword $(CLANG_CXX)))

# Darwin cross-compilation. The sysroot is assembled from Apple's open-source
# distributions plus upstream libc++ headers and baked into the toolchain image; see
# darwin-sysroot/assemble.sh. -nostdinc blocks the host (glibc) headers, then the three
# -isystem paths are added in this order deliberately: libc++ must come before the clang
# resource directory (otherwise libc++'s <cstddef> cannot find its own <stddef.h>), and the
# resource directory before the sysroot so clang supplies the target headers (arm_neon.h,
# arm_sve.h, stdarg.h).
MACOS_SYSROOT ?= /opt/darwin-sysroot
CLANG_RESOURCE = $(shell $(CLANG_CXX) -print-resource-dir)
CLANG       = $(CLANG_CXX) --target=arm64-apple-macos14 -Winline -nostdinc \
                -isystem $(MACOS_SYSROOT)/usr/include/c++/v1 \
                -isystem $(CLANG_RESOURCE)/include \
                -isystem $(MACOS_SYSROOT)/usr/include
CLANG_LINK  = $(CLANG_CXX) --target=arm64-apple-macos14 -fuse-ld=lld -nostdlib -Wl,-undefined,dynamic_lookup
CXX_ARM     = $(CLANG_CXX) --target=aarch64-linux-gnu -Winline
CXX_X64     = $(CLANG_CXX) --target=x86_64-linux-gnu -Winline
CXX_WIN     = x86_64-w64-mingw32-clang++

# Strip and debug-info commands per target platform
DARWIN_STRIP    = llvm-strip$(LLVM_SUFFIX) --strip-debug
LINUX_STRIP     = llvm-strip$(LLVM_SUFFIX) --strip-unneeded
LINUX_OBJCOPY   = llvm-objcopy$(LLVM_SUFFIX)
DARWIN_DSYMUTIL = $(shell llvm-config$(LLVM_SUFFIX) --bindir)/dsymutil
LLVM_PDBUTIL    = llvm-pdbutil$(LLVM_SUFFIX)

# Sources split by tier
SRC_AARCH64_TIER1 = $(filter-out %_2.cpp, $(wildcard src/vec/c/aarch64/*.cpp))
SRC_AARCH64_TIER2 = $(wildcard src/vec/c/aarch64/*_2.cpp)
SRC_AMD64_TIER1   = $(filter-out %_2.cpp %_3.cpp, $(wildcard src/vec/c/amd64/*.cpp))
SRC_AMD64_TIER2   = $(wildcard src/vec/c/amd64/*_2.cpp)
SRC_AMD64_TIER3   = $(wildcard src/vec/c/amd64/*_3.cpp)

# Object directories
OBJ_DARWIN  = build/obj/darwin-aarch64
OBJ_LIN_ARM = build/obj/linux-aarch64
OBJ_LIN_X64 = build/obj/linux-x64
OBJ_WIN_X64 = build/obj/windows-x64

# Object files
OBJ_DARWIN_TIER1  = $(SRC_AARCH64_TIER1:src/vec/c/aarch64/%.cpp=$(OBJ_DARWIN)/%.o)
OBJ_DARWIN_TIER2  = $(SRC_AARCH64_TIER2:src/vec/c/aarch64/%.cpp=$(OBJ_DARWIN)/%.o)
OBJ_LIN_ARM_TIER1 = $(SRC_AARCH64_TIER1:src/vec/c/aarch64/%.cpp=$(OBJ_LIN_ARM)/%.o)
OBJ_LIN_ARM_TIER2 = $(SRC_AARCH64_TIER2:src/vec/c/aarch64/%.cpp=$(OBJ_LIN_ARM)/%.o)
OBJ_LIN_X64_TIER1 = $(SRC_AMD64_TIER1:src/vec/c/amd64/%.cpp=$(OBJ_LIN_X64)/%.o)
OBJ_LIN_X64_TIER2 = $(SRC_AMD64_TIER2:src/vec/c/amd64/%.cpp=$(OBJ_LIN_X64)/%.o)
OBJ_LIN_X64_TIER3 = $(SRC_AMD64_TIER3:src/vec/c/amd64/%.cpp=$(OBJ_LIN_X64)/%.o)
OBJ_WIN_X64_TIER1 = $(SRC_AMD64_TIER1:src/vec/c/amd64/%.cpp=$(OBJ_WIN_X64)/%.o)
OBJ_WIN_X64_TIER2 = $(SRC_AMD64_TIER2:src/vec/c/amd64/%.cpp=$(OBJ_WIN_X64)/%.o)
OBJ_WIN_X64_TIER3 = $(SRC_AMD64_TIER3:src/vec/c/amd64/%.cpp=$(OBJ_WIN_X64)/%.o)

# Output files
OUT_DIR_AARCH64 = build/libs/vec/shared/aarch64
OUT_DIR_AMD64   = build/libs/vec/shared/amd64
OUT_DIR_WIN_X64 = build/libs/vec/shared/windows-x64
DYLIB   = $(OUT_DIR_AARCH64)/libvec.dylib
SO_ARM  = $(OUT_DIR_AARCH64)/libvec.so
SO_X64  = $(OUT_DIR_AMD64)/libvec.so
DLL_WIN = $(OUT_DIR_WIN_X64)/vec.dll
PDB_WIN = $(OUT_DIR_WIN_X64)/vec.pdb

# --- local: build for the current platform using the native compiler ---
# On macOS, overrides CLANG/CLANG_LINK to use the system clang (SDK available natively).
# On Linux, overrides CXX_ARM/CXX_X64 to drop the cross-compilation target.
# On both, we use the system strip instead of llvm-strip.
# On Windows (MSYS/MinGW/Cygwin shell), reuses the same llvm-mingw driver as
# the docker path; requires x86_64-w64-mingw32-clang++ on PATH.

.PHONY: all clean local local-install

UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Darwin)
  LOCAL_PLATFORM = darwin-aarch64
  LOCAL_OUTPUT   = $(DYLIB)
  local: CLANG = clang++ -Winline
  local: CLANG_LINK = clang++
  local: DARWIN_STRIP = strip -x
  local: DARWIN_DSYMUTIL = dsymutil
  local: $(DYLIB)
else ifneq (,$(or $(findstring MINGW,$(UNAME_S)),$(findstring MSYS,$(UNAME_S)),$(findstring CYGWIN,$(UNAME_S))))
  LOCAL_PLATFORM = windows-x64
  LOCAL_OUTPUT   = $(DLL_WIN)
  local: $(DLL_WIN)
else ifeq ($(shell uname -m),aarch64)
  LOCAL_PLATFORM = linux-aarch64
  LOCAL_OUTPUT   = $(SO_ARM)
  local: CXX_ARM = $(CLANG_CXX) -Winline
  local: LINUX_STRIP = strip --strip-unneeded
  local: LINUX_OBJCOPY = objcopy
  local: $(SO_ARM)
else
  LOCAL_PLATFORM = linux-x64
  LOCAL_OUTPUT   = $(SO_X64)
  local: CXX_X64 = $(CLANG_CXX) -Winline
  local: LINUX_STRIP = strip --strip-unneeded
  local: LINUX_OBJCOPY = objcopy
  local: $(SO_X64)
endif

# Build and copy to a specified destination directory (used by the Gradle task).
# The Makefile remains the single source of truth for the platform→path mapping.
local-install: local
ifndef DEST
	$(error DEST must be set, e.g. make local-install DEST=/path/to/output)
endif
	mkdir -p $(DEST)/$(LOCAL_PLATFORM)
	cp $(LOCAL_OUTPUT) $(DEST)/$(LOCAL_PLATFORM)/$(notdir $(LOCAL_OUTPUT))

# --- all: cross-compile all four targets (inside Docker) ---

all: $(DYLIB) $(SO_ARM) $(SO_X64) $(DLL_WIN)

# --- darwin-aarch64 ---

$(OBJ_DARWIN_TIER1): $(OBJ_DARWIN)/%.o: src/vec/c/aarch64/%.cpp | $(OBJ_DARWIN)
	$(CLANG) $(CXXFLAGS) -march=$(MARCH_AARCH64_TIER1) $(HEADERS) -c $< -o $@

$(OBJ_DARWIN_TIER2): $(OBJ_DARWIN)/%.o: src/vec/c/aarch64/%.cpp | $(OBJ_DARWIN)
	$(CLANG) $(CXXFLAGS) -march=$(MARCH_AARCH64_TIER2) $(HEADERS) -c $< -o $@

$(DYLIB): $(OBJ_DARWIN_TIER1) $(OBJ_DARWIN_TIER2) | $(OUT_DIR_AARCH64)
	$(CLANG_LINK) -shared $^ -o $@
	$(DARWIN_DSYMUTIL) $@ -o $@.dSYM
	$(DARWIN_STRIP) $@

# --- linux-aarch64 ---

$(OBJ_LIN_ARM_TIER1): $(OBJ_LIN_ARM)/%.o: src/vec/c/aarch64/%.cpp | $(OBJ_LIN_ARM)
	$(CXX_ARM) $(CXXFLAGS) -march=$(MARCH_AARCH64_TIER1) $(HEADERS) -c $< -o $@

$(OBJ_LIN_ARM_TIER2): $(OBJ_LIN_ARM)/%.o: src/vec/c/aarch64/%.cpp | $(OBJ_LIN_ARM)
	$(CXX_ARM) $(CXXFLAGS) -march=$(MARCH_AARCH64_TIER2) $(HEADERS) -c $< -o $@

$(SO_ARM): $(OBJ_LIN_ARM_TIER1) $(OBJ_LIN_ARM_TIER2) | $(OUT_DIR_AARCH64)
	$(CXX_ARM) -fuse-ld=lld -shared $^ -o $@
	$(LINUX_OBJCOPY) --only-keep-debug $@ $@.debug
	$(LINUX_STRIP) $@
	$(LINUX_OBJCOPY) --add-gnu-debuglink=$@.debug $@

# --- linux-x64 ---

$(OBJ_LIN_X64_TIER1): $(OBJ_LIN_X64)/%.o: src/vec/c/amd64/%.cpp | $(OBJ_LIN_X64)
	$(CXX_X64) $(CXXFLAGS) -march=$(MARCH_AMD64_TIER1) $(HEADERS) -c $< -o $@

$(OBJ_LIN_X64_TIER2): $(OBJ_LIN_X64)/%.o: src/vec/c/amd64/%.cpp | $(OBJ_LIN_X64)
	$(CXX_X64) $(CXXFLAGS) -march=$(MARCH_AMD64_TIER2) $(HEADERS) -c $< -o $@

$(OBJ_LIN_X64_TIER3): $(OBJ_LIN_X64)/%.o: src/vec/c/amd64/%.cpp | $(OBJ_LIN_X64)
	$(CXX_X64) $(CXXFLAGS) -march=$(MARCH_AMD64_TIER3) $(HEADERS) -c $< -o $@

$(SO_X64): $(OBJ_LIN_X64_TIER1) $(OBJ_LIN_X64_TIER2) $(OBJ_LIN_X64_TIER3) | $(OUT_DIR_AMD64)
	$(CXX_X64) -fuse-ld=lld -shared $^ -o $@
	$(LINUX_OBJCOPY) --only-keep-debug $@ $@.debug
	$(LINUX_STRIP) $@
	$(LINUX_OBJCOPY) --add-gnu-debuglink=$@.debug $@

# --- windows-x64 ---
# -gcodeview emits CodeView debug info; -Wl,--pdb= writes the corresponding
# PDB next to the DLL; -Wl,-S strips the residual DWARF sections but keeps
# the COFF symbol table. See the "PDB support" section of mstorsjo/llvm-mingw's
# README for the officially documented recipe

$(OBJ_WIN_X64_TIER1): $(OBJ_WIN_X64)/%.o: src/vec/c/amd64/%.cpp | $(OBJ_WIN_X64)
	$(CXX_WIN) $(CXXFLAGS) -gcodeview -march=$(MARCH_AMD64_TIER1) $(HEADERS) -c $< -o $@

$(OBJ_WIN_X64_TIER2): $(OBJ_WIN_X64)/%.o: src/vec/c/amd64/%.cpp | $(OBJ_WIN_X64)
	$(CXX_WIN) $(CXXFLAGS) -gcodeview -march=$(MARCH_AMD64_TIER2) $(HEADERS) -c $< -o $@

$(OBJ_WIN_X64_TIER3): $(OBJ_WIN_X64)/%.o: src/vec/c/amd64/%.cpp | $(OBJ_WIN_X64)
	$(CXX_WIN) $(CXXFLAGS) -gcodeview -march=$(MARCH_AMD64_TIER3) $(HEADERS) -c $< -o $@

$(DLL_WIN): $(OBJ_WIN_X64_TIER1) $(OBJ_WIN_X64_TIER2) $(OBJ_WIN_X64_TIER3) | $(OUT_DIR_WIN_X64)
	$(CXX_WIN) -shared -static -Wl,--pdb=$(PDB_WIN) -Wl,-S $^ -o $@
	@if command -v $(LLVM_PDBUTIL) >/dev/null 2>&1; then \
	    $(LLVM_PDBUTIL) dump -summary $(PDB_WIN) > /dev/null; \
	else \
	    echo "note: $(LLVM_PDBUTIL) not on PATH; skipping PDB sanity check"; \
	fi

# --- directories ---

$(OBJ_DARWIN) $(OBJ_LIN_ARM) $(OBJ_LIN_X64) $(OBJ_WIN_X64) $(OUT_DIR_AARCH64) $(OUT_DIR_AMD64) $(OUT_DIR_WIN_X64):
	mkdir -p $@

clean:
	rm -rf build/obj build/libs/vec/shared
