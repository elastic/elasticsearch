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

static inline int32_t dot7u_inner_avx512(int8_t* a, int8_t* b, size_t dims) {
    const __m512i ones = _mm512_set1_epi16(1);

    // Init accumulator(s) with 0
    __m512i acc1 = _mm512_setzero_si512();

#pragma GCC unroll 4
    for(int i = 0; i < dims; i += DOT7U_STRIDE_BYTES_LEN) {
        // Load 32 packed 8-bit integers
        __m512i va = _mm512_loadu_si512(a + i);
        __m512i vb = _mm512_loadu_si512(b + i);

        // Perform multiplication and create 16-bit values
        // Vertically multiply each unsigned 8-bit integer from va with the corresponding
        // signed 8-bit integer from vb, producing intermediate signed 16-bit integers.
        // These values will be at max 32385, at min −32640,
        // Horizontally add adjacent pairs of intermediate signed 16-bit integers, and pack the results.

        // VNNI
        //acc1 = _mm512_dpbusd_epi32(acc1, va1, vb1);
        const __m512i vab = _mm512_maddubs_epi16(va, vb);
        acc1 = _mm512_add_epi32(_mm512_madd_epi16(ones, vab), acc1);
    }

    // reduce (accumulate all)
    return _mm512_reduce_add_epi32(acc1);
}

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

#pragma GCC unroll 4
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

