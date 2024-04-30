/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

#include <stddef.h>
#include <stdint.h>
#include "vec.h"

#include <emmintrin.h>
#include <immintrin.h>

#ifndef DOT8_STRIDE_BYTES_LEN
#define DOT8_STRIDE_BYTES_LEN 32
#endif

#ifndef SQR8S_STRIDE_BYTES_LEN
#define SQR8S_STRIDE_BYTES_LEN 16
#endif

#ifdef _MSC_VER
#include <intrin.h>
#elif __GNUC__
#include <x86intrin.h>
#elif __clang__
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

// Utility function to horizontally add 8 32-bit integers
static inline int hsum_i32_8(const __m256i a) {
    const __m128i sum128 = _mm_add_epi32(_mm256_castsi256_si128(a), _mm256_extractf128_si256(a, 1));
    const __m128i hi64 = _mm_unpackhi_epi64(sum128, sum128);
    const __m128i sum64 = _mm_add_epi32(hi64, sum128);
    const __m128i hi32  = _mm_shuffle_epi32(sum64, _MM_SHUFFLE(2, 3, 0, 1));
    return _mm_cvtsi128_si32(_mm_add_epi32(sum64, hi32));
}

EXPORT int vec_caps() {
    int cpuInfo[4] = {-1};
    // Calling __cpuid with 0x0 as the function_id argument
    // gets the number of the highest valid function ID.
    cpuid(cpuInfo, 0);
    int functionIds = cpuInfo[0];
    if (functionIds >= 7) {
        cpuid(cpuInfo, 7);
        int ebx = cpuInfo[1];
        // AVX2 flag is the 5th bit
        // We assume that all processors that have AVX2 also have FMA3
        return (ebx & (1 << 5)) != 0;
    }
    return 0;
}

EXPORT int dot8s_stride() {
    return DOT8_STRIDE_BYTES_LEN;
}

EXPORT int sqr8s_stride() {
    return SQR8S_STRIDE_BYTES_LEN;
}

EXPORT int32_t dot8s(int8_t* a, int8_t* b, size_t dims) {
    // Init accumulator(s) with 0
    __m256i acc1 = _mm256_setzero_si256();
    __m256i acc2 = _mm256_setzero_si256();

    for(int i = 0; i < dims; i += DOT8_STRIDE_BYTES_LEN) {
        // Load 32 packed 8-bit integers
        __m128i va1 = _mm_lddqu_si128(a + i);
        __m128i va2 = _mm_lddqu_si128(a + i + 16);
        __m128i vb1 = _mm_lddqu_si128(b + i);
        __m128i vb2 = _mm_lddqu_si128(b + i + 16);

        // Sign extend packed 8-bit integers into packed 16-bit integers
        const __m256i va1w = _mm256_cvtepi8_epi16(va1);
        const __m256i va2w = _mm256_cvtepi8_epi16(va2);
        const __m256i vb1w = _mm256_cvtepi8_epi16(vb1);
        const __m256i vb2w = _mm256_cvtepi8_epi16(vb2);

        // Vertically multiply each signed 16-bit integer from va* with the corresponding
        // signed 16-bit integer from vb*, producing intermediate signed 32-bit integers.
        // Horizontally add adjacent pairs of intermediate signed 32-bit integers, and pack the results.
        acc1 = _mm256_add_epi32(_mm256_madd_epi16(va1w, vb1w), acc1);
        acc2 = _mm256_add_epi32(_mm256_madd_epi16(va2w, vb2w), acc2);
    }

    // reduce (accumulate all)
    return hsum_i32_8(_mm256_add_epi32(acc1, acc2));
}

EXPORT int32_t sqr8s(int8_t *a, int8_t *b, size_t dims) {
    // Init accumulator(s) with 0
    __m256i acc1 = _mm256_setzero_si256();

    for(int i = 0; i < dims; i += SQR8S_STRIDE_BYTES_LEN) {
        // Load 16 packed 8-bit integers
        __m128i va = _mm_lddqu_si128(a + i);
        __m128i vb = _mm_lddqu_si128(b + i);

        const __m256i dist = _mm256_sub_epi16(_mm256_cvtepi8_epi16(va), _mm256_cvtepi8_epi16(vb));

        const __m256i sqr_add = _mm256_madd_epi16(dist, dist);
        acc1 = _mm256_add_epi32(sqr_add, acc1);
    }

    // reduce (accumulate all)
    return hsum_i32_8(acc1);
}

