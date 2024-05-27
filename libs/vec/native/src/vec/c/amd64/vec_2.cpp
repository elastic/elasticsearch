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

#ifdef _MSC_VER
#include <intrin.h>
#elif __GNUC__
#pragma GCC push_options
#pragma GCC target ("arch=skylake-avx512")
#include <x86intrin.h>
#elif __clang__
#pragma clang attribute push (__attribute__((target("arch=skylake-avx512"))), apply_to=function)
#include <x86intrin.h>
#endif

#include <emmintrin.h>
#include <immintrin.h>

#ifndef DOT7U_STRIDE_BYTES_LEN
#define DOT7U_STRIDE_BYTES_LEN sizeof(__m512i) // Must be a power of 2
#endif

#ifndef SQR7U_STRIDE_BYTES_LEN
#define SQR7U_STRIDE_BYTES_LEN sizeof(__m512i) // Must be a power of 2
#endif

// Returns acc + ( p1 * p2 ), for 64-wide int lanes.
template<int offsetRegs>
inline __m512i fma8(__m512i acc, const int8_t* p1, const int8_t* p2) {
    constexpr int lanes = offsetRegs * sizeof(__m512i);
    const __m512i a = _mm512_loadu_si512((const __m512i*)(p1 + lanes));
    const __m512i b = _mm512_loadu_si512((const __m512i*)(p2 + lanes));
    // Perform multiplication and create 16-bit values
    // Vertically multiply each unsigned 8-bit integer from a with the corresponding
    // signed 8-bit integer from b, producing intermediate signed 16-bit integers.
    // These values will be at max 32385, at min −32640
    const __m512i dot = _mm512_maddubs_epi16(a, b);
    const __m512i ones = _mm512_set1_epi16(1);
    // Horizontally add adjacent pairs of intermediate signed 16-bit integers, and pack the results.
    return _mm512_add_epi32(_mm512_madd_epi16(ones, dot), acc);
    //const __m128i sum128 = _mm_add_epi32(_mm256_castsi256_si128(dot), _mm256_extractf128_si256(dot, 1));
    //return _mm256_add_epi32(acc, _mm256_cvtepi16_epi32(sum128));
}

static inline int32_t dot7u_inner_avx512(int8_t* a, int8_t* b, size_t dims) {
    constexpr int stride2 = 8 * DOT7U_STRIDE_BYTES_LEN;
    constexpr int stride = 4 * DOT7U_STRIDE_BYTES_LEN;
    const int8_t* p1 = a;
    const int8_t* p2 = b;

    const ptrdiff_t rem = (( dims - 1 ) % sizeof(__m512i)) + 1;
    const int8_t* const p1End = p1 + dims - rem;

    // Init accumulator(s) with 0
    __m512i acc0 = _mm512_setzero_si512();
    __m512i acc1 = _mm512_setzero_si512();
    __m512i acc2 = _mm512_setzero_si512();
    __m512i acc3 = _mm512_setzero_si512();
    __m512i acc4 = _mm512_setzero_si512();
    __m512i acc5 = _mm512_setzero_si512();
    __m512i acc6 = _mm512_setzero_si512();
    __m512i acc7 = _mm512_setzero_si512();

    while (p1 < p1End) {
        acc0 = fma8<0>(acc0, p1, p2);
        acc1 = fma8<1>(acc1, p1, p2);
        acc2 = fma8<2>(acc2, p1, p2);
        acc3 = fma8<3>(acc3, p1, p2);
        acc4 = fma8<4>(acc4, p1, p2);
        acc5 = fma8<5>(acc5, p1, p2);
        acc6 = fma8<6>(acc6, p1, p2);
        acc7 = fma8<7>(acc7, p1, p2);
        p1 += stride2;
        p2 += stride2;
    }

    while (p1 < p1End) {
        acc0 = fma8<0>(acc0, p1, p2);
        acc1 = fma8<1>(acc1, p1, p2);
        acc2 = fma8<2>(acc2, p1, p2);
        acc3 = fma8<3>(acc3, p1, p2);
        p1 += stride;
        p2 += stride;
    }

    while (p1 < p1End) {
        acc0 = fma8<0>(acc0, p1, p2);
        p1 += DOT7U_STRIDE_BYTES_LEN;
        p2 += DOT7U_STRIDE_BYTES_LEN;
    }

    // reduce (accumulate all)
    acc0 = _mm512_add_epi32(_mm512_add_epi32(acc0, acc1), _mm512_add_epi32(acc2, acc3));
    acc4 = _mm512_add_epi32(_mm512_add_epi32(acc4, acc5), _mm512_add_epi32(acc6, acc7));
    return _mm512_reduce_add_epi32(_mm512_add_epi32(acc0, acc4));
}

extern "C"
EXPORT int32_t dot7u_2(int8_t* a, int8_t* b, size_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims > DOT7U_STRIDE_BYTES_LEN) {
        i += dims & ~(DOT7U_STRIDE_BYTES_LEN - 1);
        res = dot7u_inner_avx512(a, b, i);
    }
    for (; i < dims; i++) {
        res += a[i] * b[i];
    }
    return res;
}

static inline int32_t sqr7u_inner_avx512(int8_t *a, int8_t *b, size_t dims) {
    const __m512i ones = _mm512_set1_epi16(1);

    // Init accumulator(s) with 0
    __m512i acc1 = _mm512_setzero_si512();

#pragma GCC unroll 8
    for(int i = 0; i < dims; i += SQR7U_STRIDE_BYTES_LEN) {
        // Load packed 8-bit integers
        __m512i va = _mm512_loadu_si512(a + i);
        __m512i vb = _mm512_loadu_si512(b + i);

        const __m512i dist = _mm512_sub_epi8(va, vb);
        const __m512i abs_dist = _mm512_abs_epi8(dist);

        // VNNI
        //acc1 = _mm512_dpbusd_epi32(acc1, abs_dist, abs_dist);
        const __m512i sqr = _mm512_maddubs_epi16(abs_dist, abs_dist);
        acc1 = _mm512_add_epi32(_mm512_madd_epi16(ones, sqr), acc1);
    }

    // reduce (accumulate all)
    return _mm512_reduce_add_epi32(acc1);
}

extern "C"
EXPORT int32_t sqr7u_2(int8_t* a, int8_t* b, size_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims > SQR7U_STRIDE_BYTES_LEN) {
        i += dims & ~(SQR7U_STRIDE_BYTES_LEN - 1);
        res = sqr7u_inner_avx512(a, b, i);
    }
    for (; i < dims; i++) {
        int32_t dist = a[i] - b[i];
        res += dist * dist;
    }
    return res;
}

#ifdef __GNUC__
#pragma GCC pop_options
#elif __clang__
#pragma clang attribute pop
#endif

