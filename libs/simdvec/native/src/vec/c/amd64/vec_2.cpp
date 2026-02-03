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
#include <math.h>

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
        acc0 = fma8<0>(acc0, p1, p2);
        acc1 = fma8<1>(acc1, p1, p2);
        acc2 = fma8<2>(acc2, p1, p2);
        acc3 = fma8<3>(acc3, p1, p2);
        acc4 = fma8<4>(acc4, p1, p2);
        acc5 = fma8<5>(acc5, p1, p2);
        acc6 = fma8<6>(acc6, p1, p2);
        acc7 = fma8<7>(acc7, p1, p2);
        p1 += stride8;
        p2 += stride8;
    }

    p1End = a + (dims & ~(stride4 - 1));
    while (p1 < p1End) {
        acc0 = fma8<0>(acc0, p1, p2);
        acc1 = fma8<1>(acc1, p1, p2);
        acc2 = fma8<2>(acc2, p1, p2);
        acc3 = fma8<3>(acc3, p1, p2);
        p1 += stride4;
        p2 += stride4;
    }

    p1End = a + (dims & ~(STRIDE_BYTES_LEN - 1));
    while (p1 < p1End) {
        acc0 = fma8<0>(acc0, p1, p2);
        p1 += STRIDE_BYTES_LEN;
        p2 += STRIDE_BYTES_LEN;
    }

    // reduce (accumulate all)
    acc0 = _mm512_add_epi32(_mm512_add_epi32(acc0, acc1), _mm512_add_epi32(acc2, acc3));
    acc4 = _mm512_add_epi32(_mm512_add_epi32(acc4, acc5), _mm512_add_epi32(acc6, acc7));
    return _mm512_reduce_add_epi32(_mm512_add_epi32(acc0, acc4));
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

template <int64_t(*mapper)(int32_t, const int32_t*)>
static inline void dot7u_inner_bulk(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    const int blk = dims & ~(STRIDE_BYTES_LEN - 1);
    const int lines_to_fetch = dims / CACHE_LINE_SIZE + 1;
    int c = 0;

    const int8_t* a0 = safe_mapper_offset<int8_t, 0, mapper>(a, pitch, offsets, count);
    const int8_t* a1 = safe_mapper_offset<int8_t, 1, mapper>(a, pitch, offsets, count);
    const int8_t* a2 = safe_mapper_offset<int8_t, 2, mapper>(a, pitch, offsets, count);
    const int8_t* a3 = safe_mapper_offset<int8_t, 3, mapper>(a, pitch, offsets, count);

    // Process a batch of 4 vectors at a time, after instructing the CPU to
    // prefetch the next batch.
    // Prefetching multiple memory locations while computing keeps the CPU
    // execution units busy.
    for (; c + 7 < count; c += 4) {
        const int8_t* next_a0 = a + mapper(c + 4, offsets) * pitch;
        const int8_t* next_a1 = a + mapper(c + 5, offsets) * pitch;
        const int8_t* next_a2 = a + mapper(c + 6, offsets) * pitch;
        const int8_t* next_a3 = a + mapper(c + 7, offsets) * pitch;

        prefetch(next_a0, lines_to_fetch);
        prefetch(next_a1, lines_to_fetch);
        prefetch(next_a2, lines_to_fetch);
        prefetch(next_a3, lines_to_fetch);

        int32_t res0 = 0;
        int32_t res1 = 0;
        int32_t res2 = 0;
        int32_t res3 = 0;
        int i = 0;
        if (dims > STRIDE_BYTES_LEN) {
            i = blk;
            res0 = dot7u_inner_avx512(a0, b, i);
            res1 = dot7u_inner_avx512(a1, b, i);
            res2 = dot7u_inner_avx512(a2, b, i);
            res3 = dot7u_inner_avx512(a3, b, i);
        }
        for (; i < dims; i++) {
            const int8_t bb = b[i];
            res0 += a0[i] * bb;
            res1 += a1[i] * bb;
            res2 += a2[i] * bb;
            res3 += a3[i] * bb;
        }
        results[c + 0] = (f32_t)res0;
        results[c + 1] = (f32_t)res1;
        results[c + 2] = (f32_t)res2;
        results[c + 3] = (f32_t)res3;
        a0 = next_a0;
        a1 = next_a1;
        a2 = next_a2;
        a3 = next_a3;
    }

    // Tail-handling: remaining vectors
    for (; c < count; c++) {
        const int8_t* a0 = a + mapper(c, offsets) * pitch;
        results[c] = (f32_t)vec_dot7u_2(a0, b, dims);
    }
}

EXPORT void vec_dot7u_bulk_2(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    dot7u_inner_bulk<identity_mapper>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_dot7u_bulk_offsets_2(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dot7u_inner_bulk<array_mapper>(a, b, dims, pitch, offsets, count, results);
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

template <int64_t(*mapper)(int32_t, const int32_t*)>
static inline void sqr7u_inner_bulk(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    for (int c = 0; c < count; c++) {
        const int8_t* a0 = a + mapper(c, offsets) * pitch;
        results[c] = (f32_t)vec_sqr7u_2(a0, b, dims);
    }
}

EXPORT void vec_sqr7u_bulk_2(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    sqr7u_inner_bulk<identity_mapper>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqr7u_bulk_offsets_2(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    sqr7u_inner_bulk<array_mapper>(a, b, dims, pitch, offsets, count, results);
}

static inline __m512i dot_bit_512(const __m512i a, const int8_t* b) {
    const __m512i q0 = _mm512_loadu_si512((const __m512i *)b);
    return _mm512_popcnt_epi64(_mm512_and_si512(q0, a));
}

static inline int64_t dot_int1_int4_inner(const int8_t* a, const int8_t* query, const int32_t length) {
    int r = 0;

    // Init accumulator(s) with 0
    __m512i acc0 = _mm512_setzero_si512();
    __m512i acc1 = _mm512_setzero_si512();
    __m512i acc2 = _mm512_setzero_si512();
    __m512i acc3 = _mm512_setzero_si512();

    int upperBound = length & ~(STRIDE_BYTES_LEN - 1);
    for (; r < upperBound; r += STRIDE_BYTES_LEN) {
        const __m512i value = _mm512_loadu_si512((const __m512i*)(a + r));

        acc0 = _mm512_add_epi64(acc0, dot_bit_512(value, query + r));
        acc1 = _mm512_add_epi64(acc1, dot_bit_512(value, query + r + length));
        acc2 = _mm512_add_epi64(acc2, dot_bit_512(value, query + r + 2 * length));
        acc3 = _mm512_add_epi64(acc3, dot_bit_512(value, query + r + 3 * length));
    }

    int64_t subRet0 = _mm512_reduce_add_epi64(acc0);
    int64_t subRet1 = _mm512_reduce_add_epi64(acc1);
    int64_t subRet2 = _mm512_reduce_add_epi64(acc2);
    int64_t subRet3 = _mm512_reduce_add_epi64(acc3);

    for (; r < length; r++) {
        int8_t value = *(a + r);
        int8_t q0 = *(query + r);
        subRet0 += __builtin_popcount(q0 & value & 0xFF);
        int8_t q1 = *(query + r + length);
        subRet1 += __builtin_popcount(q1 & value & 0xFF);
        int8_t q2 = *(query + r + 2 * length);
        subRet2 += __builtin_popcount(q2 & value & 0xFF);
        int8_t q3 = *(query + r + 3 * length);
        subRet3 += __builtin_popcount(q3 & value & 0xFF);
    }

    return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
}

EXPORT int64_t vec_dot_int1_int4_2(const int8_t* a, const int8_t* query, const int32_t length) {
    return dot_int1_int4_inner(a, query, length);
}

template <int64_t(*mapper)(const int32_t, const int32_t*)>
static inline void dot_int1_int4_inner_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    const int lines_to_fetch = length / CACHE_LINE_SIZE + 1;
    int c = 0;

    const int8_t* a0 = safe_mapper_offset<int8_t, 0, mapper>(a, pitch, offsets, count);
    const int8_t* a1 = safe_mapper_offset<int8_t, 1, mapper>(a, pitch, offsets, count);
    const int8_t* a2 = safe_mapper_offset<int8_t, 2, mapper>(a, pitch, offsets, count);
    const int8_t* a3 = safe_mapper_offset<int8_t, 3, mapper>(a, pitch, offsets, count);

    // Process a batch of 4 vectors at a time, after instructing the CPU to
    // prefetch the next batch.
    // Prefetching multiple memory locations while computing keeps the CPU
    // execution units busy.
    for (; c + 7 < count; c += 4) {
        const int8_t* next_a0 = a + mapper(c + 4, offsets) * pitch;
        const int8_t* next_a1 = a + mapper(c + 5, offsets) * pitch;
        const int8_t* next_a2 = a + mapper(c + 6, offsets) * pitch;
        const int8_t* next_a3 = a + mapper(c + 7, offsets) * pitch;

        prefetch(next_a0, lines_to_fetch);
        prefetch(next_a1, lines_to_fetch);
        prefetch(next_a2, lines_to_fetch);
        prefetch(next_a3, lines_to_fetch);

        results[c + 0] = (f32_t)dot_int1_int4_inner(a0, query, length);
        results[c + 1] = (f32_t)dot_int1_int4_inner(a1, query, length);
        results[c + 2] = (f32_t)dot_int1_int4_inner(a2, query, length);
        results[c + 3] = (f32_t)dot_int1_int4_inner(a3, query, length);

        a0 = next_a0;
        a1 = next_a1;
        a2 = next_a2;
        a3 = next_a3;
    }

    // Tail-handling: remaining vectors
    for (; c < count; c++) {
        const int8_t* a0 = a + mapper(c, offsets) * pitch;
        results[c] = (f32_t)dot_int1_int4_inner(a0, query, length);
    }
}

EXPORT void vec_dot_int1_int4_bulk_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dot_int1_int4_inner_bulk<identity_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dot_int1_int4_bulk_offsets_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dot_int1_int4_inner_bulk<array_mapper>(a, query, length, pitch, offsets, count, results);
}

#ifdef __clang__
#pragma clang attribute pop
#elif __GNUC__
#pragma GCC pop_options
#endif
