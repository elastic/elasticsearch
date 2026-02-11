/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

#include <stddef.h>
#include <stdint.h>
#include <math.h>
#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"

#ifdef _MSC_VER
#include <intrin.h>
#elif __clang__
#include <x86intrin.h>
#elif __GNUC__
#include <x86intrin.h>
#endif

// Multi-platform CPUID "intrinsic"; it takes as input a "functionNumber" (or "leaf", the eax registry). "Subleaf"
// is always 0. Output is stored in the passed output parameter: output[0] = eax, output[1] = ebx, output[2] = ecx,
// output[3] = edx
static inline void cpuid(int output[4], int functionNumber) {
#if defined(__GNUC__) || defined(__clang__)
    // use inline assembly, Gnu/AT&T syntax
    int a, b, c, d;
    __asm("cpuid" : "=a"(a), "=b"(b), "=c"(c), "=d"(d) : "a"(functionNumber), "c"(0) : );
    output[0] = a;
    output[1] = b;
    output[2] = c;
    output[3] = d;

#elif defined (_MSC_VER)
    __cpuidex(output, functionNumber, 0);
#else
   #error Unsupported compiler
#endif
}

// Multi-platform XGETBV "intrinsic"
static inline int64_t xgetbv(int ctr) {
#if defined(__GNUC__) || defined(__clang__)
    // use inline assembly, Gnu/AT&T syntax
    uint32_t a, d;
    __asm("xgetbv" : "=a"(a),"=d"(d) : "c"(ctr) : );
    return a | (((uint64_t) d) << 32);

#elif (defined (_MSC_FULL_VER) && _MSC_FULL_VER >= 160040000) || (defined (__INTEL_COMPILER) && __INTEL_COMPILER >= 1200)
    // Microsoft or Intel compiler supporting _xgetbv intrinsic
    return _xgetbv(ctr);

#else
   #error Unsupported compiler
#endif
}

EXPORT int vec_caps() {
    int cpuInfo[4] = {-1};
    // Calling CPUID function 0x0 as the function_id argument
    // gets the number of the highest valid function ID.
    cpuid(cpuInfo, 0);
    int functionIds = cpuInfo[0];
    if (functionIds == 0) {
        // No CPUID functions
        return 0;
    }
    // call CPUID function 0x1 for feature flags
    cpuid(cpuInfo, 1);
    int hasOsXsave = (cpuInfo[2] & (1 << 27)) != 0;
    int avxEnabledInOS = hasOsXsave && ((xgetbv(0) & 6) == 6);
    if (functionIds >= 7) {
        // call CPUID function 0x7 for AVX2/512 flags
        cpuid(cpuInfo, 7);
        int ebx = cpuInfo[1];
        int ecx = cpuInfo[2];
        // AVX2 flag is the 5th bit
        // https://github.com/llvm/llvm-project/blob/50598f0ff44f3a4e75706f8c53f3380fe7faa896/clang/lib/Headers/cpuid.h#L148
        // We assume that all processors that have AVX2 also have FMA3
        int avx2 = (ebx & 0x00000020) != 0;

        // AVX512F
        // https://github.com/llvm/llvm-project/blob/50598f0ff44f3a4e75706f8c53f3380fe7faa896/clang/lib/Headers/cpuid.h#L155
        int avx512 = (ebx & 0x00010000) != 0;
        // AVX512VNNI (ECX register)
        int avx512_vnni = (ecx & 0x00000800) != 0;
        // AVX512VPOPCNTDQ (ECX register)
        int avx512_vpopcntdq = (ecx & 0x00004000) != 0;
        if (avx512 && avx512_vnni && avx512_vpopcntdq) {
            return avxEnabledInOS ? 2 : -2;
        }
        if (avx2) {
            return avxEnabledInOS ? 1 : -1;
        }
    }
    return 0;
}
