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
#pragma clang attribute push(__attribute__((target("arch=skylake-avx512"))), apply_to=function)
#elif __GNUC__
#pragma GCC push_options
#pragma GCC target ("arch=skylake-avx512")
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

    const int8_t* a0 = safe_mapper_offset<0, mapper>(a, pitch, offsets, count);
    const int8_t* a1 = safe_mapper_offset<1, mapper>(a, pitch, offsets, count);
    const int8_t* a2 = safe_mapper_offset<2, mapper>(a, pitch, offsets, count);
    const int8_t* a3 = safe_mapper_offset<3, mapper>(a, pitch, offsets, count);

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

static inline int32_t sqr7u_inner_avx512(int8_t *a, int8_t *b, const int32_t dims) {
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

EXPORT int32_t vec_sqr7u_2(int8_t* a, int8_t* b, const int32_t dims) {
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
    if (dims > STRIDE_BYTES_LEN) {
        const int limit = dims & ~(STRIDE_BYTES_LEN - 1);
        for (int32_t c = 0; c < count; c++) {
            const int8_t* a0 = a + (mapper(c, offsets) * pitch);
            int i = limit;
            int32_t res = sqr7u_inner_avx512(a0, b, i);
            for (; i < dims; i++) {
                int32_t dist = a0[i] - b[i];
                res += dist * dist;
            }
            results[c] = (f32_t)res;
        }
    } else {
        for (int32_t c = 0; c < count; c++) {
            const int8_t* a0 = a + (mapper(c, offsets) * pitch);
            int32_t res = 0;
            for (int32_t i = 0; i < dims; i++) {
                int32_t dist = a0[i] - b[i];
                res += dist * dist;
            }
            results[c] = (f32_t)res;
        }
    }
}

EXPORT void vec_sqr7u_bulk_2(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    sqr7u_inner_bulk<identity>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqrt7u_bulk_offsets_2(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    sqr7u_inner_bulk<index>(a, b, dims, pitch, offsets, count, results);
}

// --- single precision floats

// const f32_t *a  pointer to the first float vector
// const f32_t *b  pointer to the second float vector
// const int32_t elementCount  the number of floating point elements
EXPORT f32_t vec_cosf32_2(const f32_t *a, const f32_t *b, const int32_t elementCount) {
    __m512 dot0 = _mm512_setzero_ps();
    __m512 dot1 = _mm512_setzero_ps();
    __m512 dot2 = _mm512_setzero_ps();
    __m512 dot3 = _mm512_setzero_ps();

    __m512 norm_a0 = _mm512_setzero_ps();
    __m512 norm_a1 = _mm512_setzero_ps();
    __m512 norm_a2 = _mm512_setzero_ps();
    __m512 norm_a3 = _mm512_setzero_ps();

    __m512 norm_b0 = _mm512_setzero_ps();
    __m512 norm_b1 = _mm512_setzero_ps();
    __m512 norm_b2 = _mm512_setzero_ps();
    __m512 norm_b3 = _mm512_setzero_ps();

    int32_t i = 0;
    // Each __m512 holds 16 floats, so unroll 4x = 64 floats per loop
    int32_t unrolled_limit = elementCount & ~63UL;
    for (; i < unrolled_limit; i += 64) {
        // Load and compute 4 blocks of 16 elements
        __m512 a0 = _mm512_loadu_ps(a + i);
        __m512 b0 = _mm512_loadu_ps(b + i);
        __m512 a1 = _mm512_loadu_ps(a + i + 16);
        __m512 b1 = _mm512_loadu_ps(b + i + 16);
        __m512 a2 = _mm512_loadu_ps(a + i + 32);
        __m512 b2 = _mm512_loadu_ps(b + i + 32);
        __m512 a3 = _mm512_loadu_ps(a + i + 48);
        __m512 b3 = _mm512_loadu_ps(b + i + 48);

        dot0 = _mm512_fmadd_ps(a0, b0, dot0);
        dot1 = _mm512_fmadd_ps(a1, b1, dot1);
        dot2 = _mm512_fmadd_ps(a2, b2, dot2);
        dot3 = _mm512_fmadd_ps(a3, b3, dot3);

        norm_a0 = _mm512_fmadd_ps(a0, a0, norm_a0);
        norm_a1 = _mm512_fmadd_ps(a1, a1, norm_a1);
        norm_a2 = _mm512_fmadd_ps(a2, a2, norm_a2);
        norm_a3 = _mm512_fmadd_ps(a3, a3, norm_a3);

        norm_b0 = _mm512_fmadd_ps(b0, b0, norm_b0);
        norm_b1 = _mm512_fmadd_ps(b1, b1, norm_b1);
        norm_b2 = _mm512_fmadd_ps(b2, b2, norm_b2);
        norm_b3 = _mm512_fmadd_ps(b3, b3, norm_b3);
    }

    // combine and reduce vector accumulators
    __m512 dot_total = _mm512_add_ps(_mm512_add_ps(dot0, dot1), _mm512_add_ps(dot2, dot3));
    __m512 norm_a_total = _mm512_add_ps(_mm512_add_ps(norm_a0, norm_a1), _mm512_add_ps(norm_a2, norm_a3));
    __m512 norm_b_total = _mm512_add_ps(_mm512_add_ps(norm_b0, norm_b1), _mm512_add_ps(norm_b2, norm_b3));

    f32_t dot_result = _mm512_reduce_add_ps(dot_total);
    f32_t norm_a_result = _mm512_reduce_add_ps(norm_a_total);
    f32_t norm_b_result = _mm512_reduce_add_ps(norm_b_total);

    // Handle remaining tail with scalar loop
    for (; i < elementCount; ++i) {
        f32_t ai = a[i];
        f32_t bi = b[i];
        dot_result += ai * bi;
        norm_a_result += ai * ai;
        norm_b_result += bi * bi;
    }

    f32_t denom = sqrtf(norm_a_result) * sqrtf(norm_b_result);
    if (denom == 0.0f) {
        return 0.0f;
    }
    return dot_result / denom;
}

// const f32_t *a  pointer to the first float vector
// const f32_t *b  pointer to the second float vector
// const int32_t elementCount  the number of floating point elements
EXPORT f32_t vec_dotf32_2(const f32_t *a, const f32_t *b, const int32_t elementCount) {
    __m512 sum0 = _mm512_setzero_ps();
    __m512 sum1 = _mm512_setzero_ps();
    __m512 sum2 = _mm512_setzero_ps();
    __m512 sum3 = _mm512_setzero_ps();

    int32_t i = 0;
    int32_t unrolled_limit = elementCount & ~63UL;
    // Each __m512 holds 16 floats, so unroll 4x = 64 floats per loop
    for (; i < unrolled_limit; i += 64) {
        sum0 = _mm512_fmadd_ps(_mm512_loadu_ps(a + i),      _mm512_loadu_ps(b + i),      sum0);
        sum1 = _mm512_fmadd_ps(_mm512_loadu_ps(a + i + 16), _mm512_loadu_ps(b + i + 16), sum1);
        sum2 = _mm512_fmadd_ps(_mm512_loadu_ps(a + i + 32), _mm512_loadu_ps(b + i + 32), sum2);
        sum3 = _mm512_fmadd_ps(_mm512_loadu_ps(a + i + 48), _mm512_loadu_ps(b + i + 48), sum3);
    }

    // reduce all partial sums
    __m512 total_sum = _mm512_add_ps(_mm512_add_ps(sum0, sum1), _mm512_add_ps(sum2, sum3));
    f32_t result = _mm512_reduce_add_ps(total_sum);

    for (; i < elementCount; ++i) {
        result += a[i] * b[i];
    }

    return result;
}

// const f32_t *a  pointer to the first float vector
// const f32_t *b  pointer to the second float vector
// const int32_t elementCount  the number of floating point elements
EXPORT f32_t vec_sqrf32_2(const f32_t *a, const f32_t *b, const int32_t elementCount) {
    __m512 sum0 = _mm512_setzero_ps();
    __m512 sum1 = _mm512_setzero_ps();
    __m512 sum2 = _mm512_setzero_ps();
    __m512 sum3 = _mm512_setzero_ps();

    int32_t i = 0;
    int32_t unrolled_limit = elementCount & ~63UL;
    // Each __m512 holds 16 floats, so unroll 4x = 64 floats per loop
    for (; i < unrolled_limit; i += 64) {
        __m512 d0 = _mm512_sub_ps(_mm512_loadu_ps(a + i),      _mm512_loadu_ps(b + i));
        __m512 d1 = _mm512_sub_ps(_mm512_loadu_ps(a + i + 16), _mm512_loadu_ps(b + i + 16));
        __m512 d2 = _mm512_sub_ps(_mm512_loadu_ps(a + i + 32), _mm512_loadu_ps(b + i + 32));
        __m512 d3 = _mm512_sub_ps(_mm512_loadu_ps(a + i + 48), _mm512_loadu_ps(b + i + 48));

        sum0 = _mm512_fmadd_ps(d0, d0, sum0);
        sum1 = _mm512_fmadd_ps(d1, d1, sum1);
        sum2 = _mm512_fmadd_ps(d2, d2, sum2);
        sum3 = _mm512_fmadd_ps(d3, d3, sum3);
    }

    // reduce all partial sums
    __m512 total_sum = _mm512_add_ps(_mm512_add_ps(sum0, sum1), _mm512_add_ps(sum2, sum3));
    f32_t result = _mm512_reduce_add_ps(total_sum);

    for (; i < elementCount; ++i) {
        f32_t diff = a[i] - b[i];
        result += diff * diff;
    }

    return result;
}

#ifdef __clang__
#pragma clang attribute pop
#elif __GNUC__
#pragma GCC pop_options
#endif
