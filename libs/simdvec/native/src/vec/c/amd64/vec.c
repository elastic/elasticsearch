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

#include <emmintrin.h>
#include <immintrin.h>

#ifndef STRIDE_BYTES_LEN
#define STRIDE_BYTES_LEN sizeof(__m256i) // Must be a power of 2
#endif

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
        // We assume that all processors that have AVX2 also have FMA3
        int avx2 = (ebx & 0x00000020) != 0;
        int avx512 = (ebx & 0x00010000) != 0;
        // int avx512_vnni = (ecx & 0x00000800) != 0;
        // if (avx512 && avx512_vnni) {
        if (avx512) {
            if (avxEnabledInOS) {
                return 2;
            } else {
                return -2;
            }
        }
        if (avx2) {
            if (avxEnabledInOS) {
                return 1;
            } else {
                return -1;
            }
        }
    }
    return 0;
}

static inline int32_t dot7u_inner(int8_t* a, int8_t* b, size_t dims) {
    const __m256i ones = _mm256_set1_epi16(1);

    // Init accumulator(s) with 0
    __m256i acc1 = _mm256_setzero_si256();

#pragma GCC unroll 4
    for(int i = 0; i < dims; i += STRIDE_BYTES_LEN) {
        // Load packed 8-bit integers
        __m256i va1 = _mm256_loadu_si256(a + i);
        __m256i vb1 = _mm256_loadu_si256(b + i);

        // Perform multiplication and create 16-bit values
        // Vertically multiply each unsigned 8-bit integer from va with the corresponding
        // 8-bit integer from vb, producing intermediate signed 16-bit integers.
        const __m256i vab = _mm256_maddubs_epi16(va1, vb1);
        // Horizontally add adjacent pairs of intermediate signed 16-bit integers, and pack the results.
        acc1 = _mm256_add_epi32(_mm256_madd_epi16(ones, vab), acc1);
    }

    // reduce (horizontally add all)
    return hsum_i32_8(acc1);
}

EXPORT int32_t dot7u(int8_t* a, int8_t* b, size_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims > STRIDE_BYTES_LEN) {
        i += dims & ~(STRIDE_BYTES_LEN - 1);
        res = dot7u_inner(a, b, i);
    }
    for (; i < dims; i++) {
        res += a[i] * b[i];
    }
    return res;
}

static inline int32_t sqr7u_inner(int8_t *a, int8_t *b, size_t dims) {
    // Init accumulator(s) with 0
    __m256i acc1 = _mm256_setzero_si256();

    const __m256i ones = _mm256_set1_epi16(1);

#pragma GCC unroll 4
    for(int i = 0; i < dims; i += STRIDE_BYTES_LEN) {
        // Load packed 8-bit integers
        __m256i va1 = _mm256_loadu_si256(a + i);
        __m256i vb1 = _mm256_loadu_si256(b + i);

        const __m256i dist1 = _mm256_sub_epi8(va1, vb1);
        const __m256i abs_dist1 = _mm256_sign_epi8(dist1, dist1);
        const __m256i sqr1 = _mm256_maddubs_epi16(abs_dist1, abs_dist1);
        acc1 = _mm256_add_epi32(_mm256_madd_epi16(ones, sqr1), acc1);
    }

    // reduce (accumulate all)
    return hsum_i32_8(acc1);
}

EXPORT int32_t sqr7u(int8_t* a, int8_t* b, size_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims > STRIDE_BYTES_LEN) {
        i += dims & ~(STRIDE_BYTES_LEN - 1);
        res = sqr7u_inner(a, b, i);
    }
    for (; i < dims; i++) {
        int32_t dist = a[i] - b[i];
        res += dist * dist;
    }
    return res;
}

// --- single precision floats

// Horizontal add of all 8 elements in a __m256 register
static inline float horizontal_sum_avx2(__m256 v) {
    // First, add the low and high 128-bit lanes
    __m128 low  = _mm256_castps256_ps128(v);      // lower 128 bits
    __m128 high = _mm256_extractf128_ps(v, 1);    // upper 128 bits
    __m128 sum128 = _mm_add_ps(low, high);        // sum 8 floats → 4 floats

    // Then do horizontal sum within 128-bit lane
    __m128 shuf = _mm_movehdup_ps(sum128);        // duplicate odd-index elements
    __m128 sums = _mm_add_ps(sum128, shuf);       // add pairs

    shuf = _mm_movehl_ps(shuf, sums);             // move high pair to low
    sums = _mm_add_ss(sums, shuf);                // add final two elements

    return _mm_cvtss_f32(sums);
}

// const float *a  pointer to the first float vector
// const float *b  pointer to the second float vector
// size_t elementCount  the number of floating point elements
EXPORT float cosf32(const float *a, const float *b, size_t elementCount) {
    __m256 dot0 = _mm256_setzero_ps();
    __m256 dot1 = _mm256_setzero_ps();
    __m256 dot2 = _mm256_setzero_ps();
    __m256 dot3 = _mm256_setzero_ps();

    __m256 norm_a0 = _mm256_setzero_ps();
    __m256 norm_a1 = _mm256_setzero_ps();
    __m256 norm_a2 = _mm256_setzero_ps();
    __m256 norm_a3 = _mm256_setzero_ps();

    __m256 norm_b0 = _mm256_setzero_ps();
    __m256 norm_b1 = _mm256_setzero_ps();
    __m256 norm_b2 = _mm256_setzero_ps();
    __m256 norm_b3 = _mm256_setzero_ps();

    size_t i = 0;
    // Each __m256 holds 8 floats, so unroll 4x = 32 floats per loop
    size_t unrolled_limit = elementCount & ~31UL;
    for (; i < unrolled_limit; i += 32) {
        __m256 a0 = _mm256_loadu_ps(a + i);
        __m256 b0 = _mm256_loadu_ps(b + i);
        __m256 a1 = _mm256_loadu_ps(a + i + 8);
        __m256 b1 = _mm256_loadu_ps(b + i + 8);
        __m256 a2 = _mm256_loadu_ps(a + i + 16);
        __m256 b2 = _mm256_loadu_ps(b + i + 16);
        __m256 a3 = _mm256_loadu_ps(a + i + 24);
        __m256 b3 = _mm256_loadu_ps(b + i + 24);

        dot0 = _mm256_fmadd_ps(a0, b0, dot0);
        dot1 = _mm256_fmadd_ps(a1, b1, dot1);
        dot2 = _mm256_fmadd_ps(a2, b2, dot2);
        dot3 = _mm256_fmadd_ps(a3, b3, dot3);

        norm_a0 = _mm256_fmadd_ps(a0, a0, norm_a0);
        norm_a1 = _mm256_fmadd_ps(a1, a1, norm_a1);
        norm_a2 = _mm256_fmadd_ps(a2, a2, norm_a2);
        norm_a3 = _mm256_fmadd_ps(a3, a3, norm_a3);

        norm_b0 = _mm256_fmadd_ps(b0, b0, norm_b0);
        norm_b1 = _mm256_fmadd_ps(b1, b1, norm_b1);
        norm_b2 = _mm256_fmadd_ps(b2, b2, norm_b2);
        norm_b3 = _mm256_fmadd_ps(b3, b3, norm_b3);
    }

    // combine and reduce vector accumulators
    __m256 dot_total = _mm256_add_ps(_mm256_add_ps(dot0, dot1), _mm256_add_ps(dot2, dot3));
    __m256 norm_a_total = _mm256_add_ps(_mm256_add_ps(norm_a0, norm_a1), _mm256_add_ps(norm_a2, norm_a3));
    __m256 norm_b_total = _mm256_add_ps(_mm256_add_ps(norm_b0, norm_b1), _mm256_add_ps(norm_b2, norm_b3));

    float dot_result = horizontal_sum_avx2(dot_total);
    float norm_a_result = horizontal_sum_avx2(norm_a_total);
    float norm_b_result = horizontal_sum_avx2(norm_b_total);

    // Handle remaining tail with scalar loop
    for (; i < elementCount; ++i) {
        float ai = a[i];
        float bi = b[i];
        dot_result += ai * bi;
        norm_a_result += ai * ai;
        norm_b_result += bi * bi;
    }

    float denom = sqrtf(norm_a_result) * sqrtf(norm_b_result);
    if (denom == 0.0f) {
        return 0.0f;
    }
    return dot_result / denom;
}

// const float *a  pointer to the first float vector
// const float *b  pointer to the second float vector
// size_t elementCount  the number of floating point elements
EXPORT float dotf32(const float *a, const float *b, size_t elementCount) {
    __m256 acc0 = _mm256_setzero_ps();
    __m256 acc1 = _mm256_setzero_ps();
    __m256 acc2 = _mm256_setzero_ps();
    __m256 acc3 = _mm256_setzero_ps();

    size_t i = 0;
    // Each __m256 holds 8 floats, so unroll 4x = 32 floats per loop
    size_t unrolled_limit = elementCount & ~31UL;
    for (; i < unrolled_limit; i += 32) {
        acc0 = _mm256_fmadd_ps(_mm256_loadu_ps(a + i),      _mm256_loadu_ps(b + i),      acc0);
        acc1 = _mm256_fmadd_ps(_mm256_loadu_ps(a + i + 8),  _mm256_loadu_ps(b + i + 8),  acc1);
        acc2 = _mm256_fmadd_ps(_mm256_loadu_ps(a + i + 16), _mm256_loadu_ps(b + i + 16), acc2);
        acc3 = _mm256_fmadd_ps(_mm256_loadu_ps(a + i + 24), _mm256_loadu_ps(b + i + 24), acc3);
    }

    // Combine all partial sums
    __m256 total_sum = _mm256_add_ps(_mm256_add_ps(acc0, acc1), _mm256_add_ps(acc2, acc3));
    float result = horizontal_sum_avx2(total_sum);

    for (; i < elementCount; ++i) {
        result += a[i] * b[i];
    }

    return result;
}

// const float *a  pointer to the first float vector
// const float *b  pointer to the second float vector
// size_t elementCount  the number of floating point elements
EXPORT float sqrf32(const float *a, const float *b, size_t elementCount) {
    __m256 sum0 = _mm256_setzero_ps();
    __m256 sum1 = _mm256_setzero_ps();
    __m256 sum2 = _mm256_setzero_ps();
    __m256 sum3 = _mm256_setzero_ps();

    size_t i = 0;
    size_t unrolled_limit = elementCount & ~31UL;
    // Each __m256 holds 8 floats, so unroll 4x = 32 floats per loop
    for (; i < unrolled_limit; i += 32) {
        __m256 d0 = _mm256_sub_ps(_mm256_loadu_ps(a + i),      _mm256_loadu_ps(b + i));
        __m256 d1 = _mm256_sub_ps(_mm256_loadu_ps(a + i + 8),  _mm256_loadu_ps(b + i + 8));
        __m256 d2 = _mm256_sub_ps(_mm256_loadu_ps(a + i + 16), _mm256_loadu_ps(b + i + 16));
        __m256 d3 = _mm256_sub_ps(_mm256_loadu_ps(a + i + 24), _mm256_loadu_ps(b + i + 24));

        sum0 = _mm256_fmadd_ps(d0, d0, sum0);
        sum1 = _mm256_fmadd_ps(d1, d1, sum1);
        sum2 = _mm256_fmadd_ps(d2, d2, sum2);
        sum3 = _mm256_fmadd_ps(d3, d3, sum3);
    }

    // reduce all partial sums
    __m256 total_sum = _mm256_add_ps(_mm256_add_ps(sum0, sum1), _mm256_add_ps(sum2, sum3));
    float result = horizontal_sum_avx2(total_sum);

    for (; i < elementCount; ++i) {
        float diff = a[i] - b[i];
        result += diff * diff;
    }

    return result;
}
