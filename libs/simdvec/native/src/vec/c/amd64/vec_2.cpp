/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

 // This file contains implementations for processors supporting "2nd level" vector
 // capabilities; in the case of x64, this second level is support for AVX-512
 // instructions.

#include <stddef.h>
#include <stdint.h>

// Force the preprocessor to pick up AVX-512 intrinsics, and the compiler to emit AVX-512 code
#ifdef __clang__
#pragma clang attribute push(__attribute__((target("arch=icelake-client"))), apply_to=function)
#elif __GNUC__
#pragma GCC push_options
#pragma GCC target ("arch=icelake-client")
#endif

#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"

// Includes for intrinsics
#ifdef _MSC_VER
#include <intrin.h>
#elif __clang__
#include <x86intrin.h>
#elif __GNUC__
#include <x86intrin.h>
#endif

#include <emmintrin.h>
#include <immintrin.h>

#ifndef STRIDE_BYTES_LEN
#define STRIDE_BYTES_LEN sizeof(__m512i) // Must be a power of 2
#endif

#ifndef STRIDE
#define STRIDE(size, num) STRIDE_BYTES_LEN / size * num
#endif

// Returns acc + ( p1 * p2 ), for 64-wide int lanes.
template<int offsetRegs>
inline __m512i fma8(__m512i acc, const int8_t* p1, const int8_t* p2) {
    constexpr int lanes = offsetRegs * STRIDE_BYTES_LEN;
    const __m512i a = _mm512_loadu_si512((const __m512i*)(p1 + lanes));
    const __m512i b = _mm512_loadu_si512((const __m512i*)(p2 + lanes));
    // Perform multiplication and create 16-bit values
    // Vertically multiply each unsigned 8-bit integer from a with the corresponding
    // signed 8-bit integer from b, producing intermediate signed 16-bit integers.
    // These values will be at max 32385, at min −32640
    const __m512i dot = _mm512_maddubs_epi16(a, b);
    const __m512i ones = _mm512_set1_epi16(1);
    // Horizontally add adjacent pairs of intermediate signed 16-bit ints, and pack the results in 32-bit ints.
    // Using madd with 1, as this is faster than extract 2 halves, add 16-bit ints, and convert to 32-bit ints.
    return _mm512_add_epi32(_mm512_madd_epi16(ones, dot), acc);
}

static inline int32_t dot7u_inner_avx512(const int8_t* a, const int8_t* b, const int32_t dims) {
    constexpr int batches = 8;
    constexpr int half_batches = 4;
    constexpr int stride8 = batches * STRIDE_BYTES_LEN;
    constexpr int stride4 = half_batches * STRIDE_BYTES_LEN;
    const int8_t* p1 = a;
    const int8_t* p2 = b;



    __m512i acc[batches];
    // Init accumulator(s) with 0
    apply_indexed<batches>([&](auto I) {
        acc[I] = _mm512_setzero_si512();
    });

    const int8_t* p1End = a + (dims & ~(stride8 - 1));
    while (p1 < p1End) {
        apply_indexed<batches>([&](auto I) {
            acc[I] = fma8<I>(acc[I], p1, p2);
        });
        p1 += stride8;
        p2 += stride8;
    }

    p1End = a + (dims & ~(stride4 - 1));
    while (p1 < p1End) {
        apply_indexed<half_batches>([&](auto I) {
            acc[I] = fma8<I>(acc[I], p1, p2);
        });
        p1 += stride4;
        p2 += stride4;
    }

    p1End = a + (dims & ~(STRIDE_BYTES_LEN - 1));
    while (p1 < p1End) {
        acc[0] = fma8<0>(acc[0], p1, p2);
        p1 += STRIDE_BYTES_LEN;
        p2 += STRIDE_BYTES_LEN;
    }

    // reduce (accumulate all)
    __m512i total_sum = tree_reduce<batches, __m512i, _mm512_add_epi32>(acc);
    return _mm512_reduce_add_epi32(total_sum);
}

EXPORT int32_t vec_dot7u_2(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims > STRIDE_BYTES_LEN) {
        i += dims & ~(STRIDE_BYTES_LEN - 1);
        res = dot7u_inner_avx512(a, b, i);
    }
    for (; i < dims; i++) {
        res += a[i] * b[i];
    }
    return res;
}

EXPORT void vec_dot7u_bulk_2(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<int8_t, sequential_mapper, dot7u_inner_avx512, dot_scalar<int8_t>, vec_dot7u_2, STRIDE_BYTES_LEN, 4>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_dot7u_bulk_offsets_2(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<int8_t, offsets_mapper, dot7u_inner_avx512, dot_scalar<int8_t>, vec_dot7u_2, STRIDE_BYTES_LEN, 4>(a, b, dims, pitch, offsets, count, results);
}

template<int offsetRegs>
inline __m512i sqr8(__m512i acc, const int8_t* p1, const int8_t* p2) {
    constexpr int lanes = offsetRegs * STRIDE_BYTES_LEN;
    const __m512i a = _mm512_loadu_si512((const __m512i*)(p1 + lanes));
    const __m512i b = _mm512_loadu_si512((const __m512i*)(p2 + lanes));

    const __m512i dist = _mm512_sub_epi8(a, b);
    const __m512i abs_dist = _mm512_abs_epi8(dist);
    const __m512i sqr_add = _mm512_maddubs_epi16(abs_dist, abs_dist);
    const __m512i ones = _mm512_set1_epi16(1);
    // Horizontally add adjacent pairs of intermediate signed 16-bit integers, and pack the results.
    return _mm512_add_epi32(_mm512_madd_epi16(ones, sqr_add), acc);
}

static inline int32_t sqr7u_inner_avx512(const int8_t* a, const int8_t* b, const int32_t dims) {
    constexpr int stride8 = 8 * STRIDE_BYTES_LEN;
    constexpr int stride4 = 4 * STRIDE_BYTES_LEN;
    const int8_t* p1 = a;
    const int8_t* p2 = b;

    // Init accumulator(s) with 0
    __m512i acc0 = _mm512_setzero_si512();
    __m512i acc1 = _mm512_setzero_si512();
    __m512i acc2 = _mm512_setzero_si512();
    __m512i acc3 = _mm512_setzero_si512();
    __m512i acc4 = _mm512_setzero_si512();
    __m512i acc5 = _mm512_setzero_si512();
    __m512i acc6 = _mm512_setzero_si512();
    __m512i acc7 = _mm512_setzero_si512();

    const int8_t* p1End = a + (dims & ~(stride8 - 1));
    while (p1 < p1End) {
        acc0 = sqr8<0>(acc0, p1, p2);
        acc1 = sqr8<1>(acc1, p1, p2);
        acc2 = sqr8<2>(acc2, p1, p2);
        acc3 = sqr8<3>(acc3, p1, p2);
        acc4 = sqr8<4>(acc4, p1, p2);
        acc5 = sqr8<5>(acc5, p1, p2);
        acc6 = sqr8<6>(acc6, p1, p2);
        acc7 = sqr8<7>(acc7, p1, p2);
        p1 += stride8;
        p2 += stride8;
    }

    p1End = a + (dims & ~(stride4 - 1));
    while (p1 < p1End) {
        acc0 = sqr8<0>(acc0, p1, p2);
        acc1 = sqr8<1>(acc1, p1, p2);
        acc2 = sqr8<2>(acc2, p1, p2);
        acc3 = sqr8<3>(acc3, p1, p2);
        p1 += stride4;
        p2 += stride4;
    }

    p1End = a + (dims & ~(STRIDE_BYTES_LEN - 1));
    while (p1 < p1End) {
        acc0 = sqr8<0>(acc0, p1, p2);
        p1 += STRIDE_BYTES_LEN;
        p2 += STRIDE_BYTES_LEN;
    }

    // reduce (accumulate all)
    acc0 = _mm512_add_epi32(_mm512_add_epi32(acc0, acc1), _mm512_add_epi32(acc2, acc3));
    acc4 = _mm512_add_epi32(_mm512_add_epi32(acc4, acc5), _mm512_add_epi32(acc6, acc7));
    return _mm512_reduce_add_epi32(_mm512_add_epi32(acc0, acc4));
}

EXPORT int32_t vec_sqr7u_2(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims > STRIDE_BYTES_LEN) {
        i += dims & ~(STRIDE_BYTES_LEN - 1);
        res = sqr7u_inner_avx512(a, b, i);
    }
    for (; i < dims; i++) {
        int32_t dist = a[i] - b[i];
        res += dist * dist;
    }
    return res;
}

EXPORT void vec_sqr7u_bulk_2(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<int8_t, sequential_mapper, sqr7u_inner_avx512, sqr_scalar<int8_t>, vec_sqr7u_2, STRIDE_BYTES_LEN, 4>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqr7u_bulk_offsets_2(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<int8_t, offsets_mapper, sqr7u_inner_avx512, sqr_scalar<int8_t>, vec_sqr7u_2, STRIDE_BYTES_LEN, 4>(a, b, dims, pitch, offsets, count, results);
}

#ifdef __clang__
#pragma clang attribute pop
#elif __GNUC__
#pragma GCC pop_options
#endif
