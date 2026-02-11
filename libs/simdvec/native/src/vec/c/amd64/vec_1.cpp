/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

  // This file contains implementations for basic vector processing functionalities,
  // including support for "1st tier" vector capabilities; in the case of x64,
  // this first tier include functions for processors supporting at least AVX2.

#include <stddef.h>
#include <stdint.h>
#include <math.h>
#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"

#ifndef STRIDE_BYTES_LEN
#define STRIDE_BYTES_LEN sizeof(__m256i) // Must be a power of 2
#endif

#ifndef STRIDE
#define STRIDE(size, num) STRIDE_BYTES_LEN / size * num
#endif

static inline int32_t doti7u_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    const __m256i ones = _mm256_set1_epi16(1);

    // Init accumulator(s) with 0
    __m256i acc1 = _mm256_setzero_si256();

#pragma GCC unroll 4
    for(int i = 0; i < dims; i += STRIDE_BYTES_LEN) {
        // Load packed 8-bit integers
        __m256i va1 = _mm256_loadu_si256((const __m256i_u*)(a + i));
        __m256i vb1 = _mm256_loadu_si256((const __m256i_u*)(b + i));

        // Perform multiplication and create 16-bit values
        // Vertically multiply each unsigned 8-bit integer from va with the corresponding
        // 8-bit integer from vb, producing intermediate signed 16-bit integers.
        const __m256i vab = _mm256_maddubs_epi16(va1, vb1);
        // Horizontally add adjacent pairs of intermediate signed 16-bit integers, and pack the results.
        acc1 = _mm256_add_epi32(_mm256_madd_epi16(ones, vab), acc1);
    }

    // reduce (horizontally add all)
    return mm256_reduce_epi32<_mm_add_epi32>(acc1);
}

EXPORT int32_t vec_doti7u(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims > STRIDE_BYTES_LEN) {
        i += dims & ~(STRIDE_BYTES_LEN - 1);
        res = doti7u_inner(a, b, i);
    }
    for (; i < dims; i++) {
        res += a[i] * b[i];
    }
    return res;
}

template <int64_t(*mapper)(int32_t, const int32_t*)>
static inline void doti7u_inner_bulk(
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

    // Process a batch of 2 vectors at a time, after instructing the CPU to
    // prefetch the next batch.
    // Prefetching multiple memory locations while computing keeps the CPU
    // execution units busy. For this "older" generation of x64 processors
    // (supporting AVX2, but not AVX-512), benchmarks show that a batch of 2
    // is ideal -- more, and it starts to hurt performances due to bandwidth
    for (; c + 3 < count; c += 2) {
        const int8_t* next_a0 = a + mapper(c + 2, offsets) * pitch;
        const int8_t* next_a1 = a + mapper(c + 3, offsets) * pitch;

        prefetch(next_a0, lines_to_fetch);
        prefetch(next_a1, lines_to_fetch);

        int32_t res0 = 0;
        int32_t res1 = 0;
        int i = 0;
        if (dims > STRIDE_BYTES_LEN) {
            i = blk;
            res0 = doti7u_inner(a0, b, i);
            res1 = doti7u_inner(a1, b, i);
        }
        for (; i < dims; i++) {
            const int8_t bb = b[i];
            res0 += a0[i] * bb;
            res1 += a1[i] * bb;
        }
        results[c + 0] = (f32_t)res0;
        results[c + 1] = (f32_t)res1;
        a0 = next_a0;
        a1 = next_a1;
    }

    // Tail-handling: remaining vectors
    for (; c < count; c++) {
        const int8_t* a0 = a + mapper(c, offsets) * pitch;
        results[c] = (f32_t)vec_doti7u(a0, b, dims);
    }
}

EXPORT void vec_doti7u_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    doti7u_inner_bulk<identity_mapper>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_doti7u_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    doti7u_inner_bulk<array_mapper>(a, b, dims, pitch, offsets, count, results);
}

static inline int32_t sqri7u_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    // Init accumulator(s) with 0
    __m256i acc1 = _mm256_setzero_si256();

    const __m256i ones = _mm256_set1_epi16(1);

#pragma GCC unroll 4
    for(int i = 0; i < dims; i += STRIDE_BYTES_LEN) {
        // Load packed 8-bit integers
        __m256i va1 = _mm256_loadu_si256((const __m256i_u*)(a + i));
        __m256i vb1 = _mm256_loadu_si256((const __m256i_u*)(b + i));

        const __m256i dist1 = _mm256_sub_epi8(va1, vb1);
        const __m256i abs_dist1 = _mm256_sign_epi8(dist1, dist1);
        const __m256i sqr1 = _mm256_maddubs_epi16(abs_dist1, abs_dist1);
        acc1 = _mm256_add_epi32(_mm256_madd_epi16(ones, sqr1), acc1);
    }

    // reduce (accumulate all)
    return mm256_reduce_epi32<_mm_add_epi32>(acc1);
}

EXPORT int32_t vec_sqri7u(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims > STRIDE_BYTES_LEN) {
        i += dims & ~(STRIDE_BYTES_LEN - 1);
        res = sqri7u_inner(a, b, i);
    }
    for (; i < dims; i++) {
        int32_t dist = a[i] - b[i];
        res += dist * dist;
    }
    return res;
}

template <int64_t(*mapper)(int32_t, const int32_t*)>
static inline void sqri7u_inner_bulk(
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
        results[c] = (f32_t)vec_sqri7u(a0, b, dims);
    }
}

EXPORT void vec_sqri7u_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    sqri7u_inner_bulk<identity_mapper>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqri7u_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    sqri7u_inner_bulk<array_mapper>(a, b, dims, pitch, offsets, count, results);
}

// --- byte vectors

/*
 * AVX2 doesn't have sign-symmetric 8-bit operations,
 * so we have to sign-extend to 16-bits and operate on those values
 * instead, at the cost of doing double the loops
 */

struct cosine_results_t {
    int32_t sum;
    int32_t norm1;
    int32_t norm2;
};

static inline cosine_results_t cosi8_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    // Init accumulator(s) with 0
    __m256i sum = _mm256_setzero_si256();
    __m256i norm1 = _mm256_setzero_si256();
    __m256i norm2 = _mm256_setzero_si256();

    for(int i = 0; i < dims; i += sizeof(__m128i)) {
        // Load packed 8-bit integers
        __m128i va8 = _mm_loadu_si128((const __m128i*)(a + i));
        __m128i vb8 = _mm_loadu_si128((const __m128i*)(b + i));

        // sign-extend to 16-bits
        __m256i va16 = _mm256_cvtepi8_epi16(va8);
        __m256i vb16 = _mm256_cvtepi8_epi16(vb8);

        // vertically multiply and add a little bit to 32-bit values
        __m256i sums = _mm256_madd_epi16(va16, vb16);
        __m256i norm1s = _mm256_madd_epi16(va16, va16);
        __m256i norm2s = _mm256_madd_epi16(vb16, vb16);

        // accumulate
        sum = _mm256_add_epi32(sums, sum);
        norm1 = _mm256_add_epi32(norm1s, norm1);
        norm2 = _mm256_add_epi32(norm2s, norm2);
    }

    // reduce (horizontally add all)
    return cosine_results_t {
        mm256_reduce_epi32<_mm_add_epi32>(sum),
        mm256_reduce_epi32<_mm_add_epi32>(norm1),
        mm256_reduce_epi32<_mm_add_epi32>(norm2)
    };
}

EXPORT f32_t vec_cosi8(const int8_t* a, const int8_t* b, const int32_t dims) {
    cosine_results_t res = cosine_results_t { 0, 0, 0 };
    int i = 0;
    if (dims > sizeof(__m128i)) {
        i += dims & ~(sizeof(__m128i) - 1);
        res = cosi8_inner(a, b, i);
    }
    for (; i < dims; i++) {
        int32_t ai = (int32_t) a[i];
        int32_t bi = (int32_t) b[i];
        res.sum += ai * bi;
        res.norm1 += ai * ai;
        res.norm2 += bi * bi;
    }

    return (f32_t) ((double) res.sum / sqrt((double) res.norm1 * res.norm2));
}

template <int64_t(*mapper)(int32_t, const int32_t*)>
static inline void cosi8_inner_bulk(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    for (int c=0; c<count; c++) {
        const int8_t* a0 = a + mapper(c, offsets) * pitch;
        results[c] = vec_cosi8(a0, b, dims);
    }
}

EXPORT void vec_cosi8_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    cosi8_inner_bulk<identity_mapper>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_cosi8_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    cosi8_inner_bulk<array_mapper>(a, b, dims, pitch, offsets, count, results);
}

static inline int32_t doti8_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    // Init accumulator(s) with 0
    __m256i acc1 = _mm256_setzero_si256();

#pragma GCC unroll 4
    for(int i = 0; i < dims; i += sizeof(__m128i)) {
        // Load packed 8-bit integers
        __m128i va8 = _mm_loadu_si128((const __m128i*)(a + i));
        __m128i vb8 = _mm_loadu_si128((const __m128i*)(b + i));

        // sign-extend to 16-bits
        __m256i va16 = _mm256_cvtepi8_epi16(va8);
        __m256i vb16 = _mm256_cvtepi8_epi16(vb8);

        // vertically multiply and add a little bit to 32-bit values
        __m256i vab = _mm256_madd_epi16(va16, vb16);

        // accumulate
        acc1 = _mm256_add_epi32(vab, acc1);
    }

    // reduce (horizontally add all)
    return mm256_reduce_epi32<_mm_add_epi32>(acc1);
}

EXPORT f32_t vec_doti8(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims > sizeof(__m128i)) {
        i += dims & ~(sizeof(__m128i) - 1);
        res = doti8_inner(a, b, i);
    }
    for (; i < dims; i++) {
        res += a[i] * b[i];
    }
    return (f32_t)res;
}

template <int64_t(*mapper)(int32_t, const int32_t*)>
static inline void doti8_inner_bulk(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    for (int c=0; c<count; c++) {
        const int8_t* a0 = a + mapper(c, offsets) * pitch;
        results[c] = vec_doti8(a0, b, dims);
    }
}

EXPORT void vec_doti8_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    doti8_inner_bulk<identity_mapper>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_doti8_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    doti8_inner_bulk<array_mapper>(a, b, dims, pitch, offsets, count, results);
}

static inline int32_t sqri8_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    // Init accumulator(s) with 0
    __m256i acc1 = _mm256_setzero_si256();

#pragma GCC unroll 4
    for(int i = 0; i < dims; i += sizeof(__m128i)) {
        // Load packed 8-bit integers
        __m128i va8 = _mm_loadu_si128((const __m128i*)(a + i));
        __m128i vb8 = _mm_loadu_si128((const __m128i*)(b + i));

        // sign-extend to 16-bits
        __m256i va16 = _mm256_cvtepi8_epi16(va8);
        __m256i vb16 = _mm256_cvtepi8_epi16(vb8);

        // do the sqr distance and accumulate to 32-bit ints
        __m256i dist = _mm256_sub_epi16(va16, vb16);
        __m256i sqr = _mm256_madd_epi16(dist, dist);
        acc1 = _mm256_add_epi32(sqr, acc1);
    }

    // reduce (accumulate all)
    return mm256_reduce_epi32<_mm_add_epi32>(acc1);
}

EXPORT f32_t vec_sqri8(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims > sizeof(__m128i)) {
        i += dims & ~(sizeof(__m128i) - 1);
        res = sqri8_inner(a, b, i);
    }
    for (; i < dims; i++) {
        int32_t dist = a[i] - b[i];
        res += dist * dist;
    }
    return (f32_t)res;
}

template <int64_t(*mapper)(int32_t, const int32_t*)>
static inline void sqri8_inner_bulk(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    for (int c=0; c<count; c++) {
        const int8_t* a0 = a + mapper(c, offsets) * pitch;
        results[c] = vec_sqri8(a0, b, dims);
    }
}

EXPORT void vec_sqri8_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    sqri8_inner_bulk<identity_mapper>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqri8_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    sqri8_inner_bulk<array_mapper>(a, b, dims, pitch, offsets, count, results);
}

// --- single precision floats

// const f32_t* a  pointer to the first float vector
// const f32_t* b  pointer to the second float vector
// const int32_t elementCount  the number of floating point elements
EXPORT f32_t vec_dotf32(const f32_t* a, const f32_t* b, const int32_t elementCount) {
    __m256 acc0 = _mm256_setzero_ps();
    __m256 acc1 = _mm256_setzero_ps();
    __m256 acc2 = _mm256_setzero_ps();
    __m256 acc3 = _mm256_setzero_ps();

    int i = 0;
    int unrolled_limit = elementCount & ~(STRIDE(sizeof(f32_t), 4) - 1);
    for (; i < unrolled_limit; i += STRIDE(sizeof(f32_t), 4)) {
        acc0 = _mm256_fmadd_ps(_mm256_loadu_ps(a + i),      _mm256_loadu_ps(b + i),      acc0);
        acc1 = _mm256_fmadd_ps(_mm256_loadu_ps(a + i + 8),  _mm256_loadu_ps(b + i + 8),  acc1);
        acc2 = _mm256_fmadd_ps(_mm256_loadu_ps(a + i + 16), _mm256_loadu_ps(b + i + 16), acc2);
        acc3 = _mm256_fmadd_ps(_mm256_loadu_ps(a + i + 24), _mm256_loadu_ps(b + i + 24), acc3);
    }

    // Combine all partial sums
    __m256 total_sum = _mm256_add_ps(_mm256_add_ps(acc0, acc1), _mm256_add_ps(acc2, acc3));
    f32_t result = mm256_reduce_ps<_mm_add_ps>(total_sum);

    for (; i < elementCount; ++i) {
        result += a[i] * b[i];
    }

    return result;
}

template <int64_t(*mapper)(int32_t, const int32_t*)>
static inline void dotf32_inner_bulk(
    const f32_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    const int vec_size = pitch / sizeof(f32_t);
    int c = 0;
    for (; c + 3 < count; c += 4) {
        const f32_t* a0 = a + mapper(c + 0, offsets) * vec_size;
        const f32_t* a1 = a + mapper(c + 1, offsets) * vec_size;
        const f32_t* a2 = a + mapper(c + 2, offsets) * vec_size;
        const f32_t* a3 = a + mapper(c + 3, offsets) * vec_size;

        __m256 sum0 = _mm256_setzero_ps();
        __m256 sum1 = _mm256_setzero_ps();
        __m256 sum2 = _mm256_setzero_ps();
        __m256 sum3 = _mm256_setzero_ps();

        int32_t i = 0;
        int32_t unrolled_limit = dims & ~7UL;
        // do 4 vectors at a time, iterating through the dimensions in parallel
        // Each __m256 holds 8 floats
        for (; i < unrolled_limit; i += 8) {
            __m256 bi = _mm256_loadu_ps(b + i);
            sum0 = _mm256_fmadd_ps(_mm256_loadu_ps(a0 + i), bi, sum0);
            sum1 = _mm256_fmadd_ps(_mm256_loadu_ps(a1 + i), bi, sum1);
            sum2 = _mm256_fmadd_ps(_mm256_loadu_ps(a2 + i), bi, sum2);
            sum3 = _mm256_fmadd_ps(_mm256_loadu_ps(a3 + i), bi, sum3);
        }

        f32_t result0 = mm256_reduce_ps<_mm_add_ps>(sum0);
        f32_t result1 = mm256_reduce_ps<_mm_add_ps>(sum1);
        f32_t result2 = mm256_reduce_ps<_mm_add_ps>(sum2);
        f32_t result3 = mm256_reduce_ps<_mm_add_ps>(sum3);

        // dimensions tail
        for (; i < dims; i++) {
            result0 += a0[i] * b[i];
            result1 += a1[i] * b[i];
            result2 += a2[i] * b[i];
            result3 += a3[i] * b[i];
        }

        results[c + 0] = result0;
        results[c + 1] = result1;
        results[c + 2] = result2;
        results[c + 3] = result3;
    }

    // vectors tail
    for (; c < count; c++) {
        const f32_t* a0 = a + mapper(c, offsets) * vec_size;
        results[c] = vec_dotf32(a0, b, dims);
    }
}

EXPORT void vec_dotf32_bulk(const f32_t* a, const f32_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    dotf32_inner_bulk<identity_mapper>(a, b, dims, dims * sizeof(f32_t), NULL, count, results);
}

EXPORT void vec_dotf32_bulk_offsets(
    const f32_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dotf32_inner_bulk<array_mapper>(a, b, dims, pitch, offsets, count, results);
}

// const f32_t* a  pointer to the first float vector
// const f32_t* b  pointer to the second float vector
// const int32_t elementCount  the number of floating point elements
EXPORT f32_t vec_sqrf32(const f32_t* a, const f32_t* b, const int32_t elementCount) {
    __m256 sum0 = _mm256_setzero_ps();
    __m256 sum1 = _mm256_setzero_ps();
    __m256 sum2 = _mm256_setzero_ps();
    __m256 sum3 = _mm256_setzero_ps();

    int i = 0;
    int unrolled_limit = elementCount & ~(STRIDE(sizeof(f32_t), 4) - 1);
    for (; i < unrolled_limit; i += STRIDE(sizeof(f32_t), 4)) {
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
    f32_t result = mm256_reduce_ps<_mm_add_ps>(total_sum);

    for (; i < elementCount; ++i) {
        f32_t diff = a[i] - b[i];
        result += diff * diff;
    }

    return result;
}

template <int64_t(*mapper)(int32_t, const int32_t*)>
static inline void sqrf32_inner_bulk(
    const f32_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    const int vec_size = pitch / sizeof(f32_t);
    int c = 0;
    for (; c + 3 < count; c += 4) {
        const f32_t* a0 = a + mapper(c + 0, offsets) * vec_size;
        const f32_t* a1 = a + mapper(c + 1, offsets) * vec_size;
        const f32_t* a2 = a + mapper(c + 2, offsets) * vec_size;
        const f32_t* a3 = a + mapper(c + 3, offsets) * vec_size;

        __m256 sum0 = _mm256_setzero_ps();
        __m256 sum1 = _mm256_setzero_ps();
        __m256 sum2 = _mm256_setzero_ps();
        __m256 sum3 = _mm256_setzero_ps();

        int32_t i = 0;
        int32_t unrolled_limit = dims & ~7UL;
        // do 4 vectors at a time, iterating through the dimensions in parallel
        // Each __m256 holds 8 floats
        for (; i < unrolled_limit; i += 8) {
            __m256 bi = _mm256_loadu_ps(b + i);
            __m256 d0 = _mm256_sub_ps(_mm256_loadu_ps(a0 + i), bi);
            __m256 d1 = _mm256_sub_ps(_mm256_loadu_ps(a1 + i), bi);
            __m256 d2 = _mm256_sub_ps(_mm256_loadu_ps(a2 + i), bi);
            __m256 d3 = _mm256_sub_ps(_mm256_loadu_ps(a3 + i), bi);

            sum0 = _mm256_fmadd_ps(d0, d0, sum0);
            sum1 = _mm256_fmadd_ps(d1, d1, sum1);
            sum2 = _mm256_fmadd_ps(d2, d2, sum2);
            sum3 = _mm256_fmadd_ps(d3, d3, sum3);
        }

        f32_t result0 = mm256_reduce_ps<_mm_add_ps>(sum0);
        f32_t result1 = mm256_reduce_ps<_mm_add_ps>(sum1);
        f32_t result2 = mm256_reduce_ps<_mm_add_ps>(sum2);
        f32_t result3 = mm256_reduce_ps<_mm_add_ps>(sum3);

        // dimensions tail
        for (; i < dims; i++) {
            f32_t diff0 = a0[i] - b[i];
            f32_t diff1 = a1[i] - b[i];
            f32_t diff2 = a2[i] - b[i];
            f32_t diff3 = a3[i] - b[i];

            result0 += diff0 * diff0;
            result1 += diff1 * diff1;
            result2 += diff2 * diff2;
            result3 += diff3 * diff3;
        }

        results[c + 0] = result0;
        results[c + 1] = result1;
        results[c + 2] = result2;
        results[c + 3] = result3;
    }

    // vectors tail
    for (; c < count; c++) {
        const f32_t* a0 = a + mapper(c, offsets) * vec_size;
        results[c] = vec_sqrf32(a0, b, dims);
    }
}

EXPORT void vec_sqrf32_bulk(const f32_t* a, const f32_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    sqrf32_inner_bulk<identity_mapper>(a, b, dims, dims * sizeof(f32_t), NULL, count, results);
}

EXPORT void vec_sqrf32_bulk_offsets(
    const f32_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    sqrf32_inner_bulk<array_mapper>(a, b, dims, pitch, offsets, count, results);
}

// Fast AVX2 popcount, based on "Faster Population Counts Using AVX2 Instructions"
// See https://arxiv.org/abs/1611.07612 and https://github.com/WojciechMula/sse-popcount
static inline __m256i dot_bit_256(const __m256i a, const int8_t* b) {
    const __m256i lookup = _mm256_setr_epi8(
        /* 0 */ 0, /* 1 */ 1, /* 2 */ 1, /* 3 */ 2,
        /* 4 */ 1, /* 5 */ 2, /* 6 */ 2, /* 7 */ 3,
        /* 8 */ 1, /* 9 */ 2, /* a */ 2, /* b */ 3,
        /* c */ 2, /* d */ 3, /* e */ 3, /* f */ 4,

        /* 0 */ 0, /* 1 */ 1, /* 2 */ 1, /* 3 */ 2,
        /* 4 */ 1, /* 5 */ 2, /* 6 */ 2, /* 7 */ 3,
        /* 8 */ 1, /* 9 */ 2, /* a */ 2, /* b */ 3,
        /* c */ 2, /* d */ 3, /* e */ 3, /* f */ 4
    );

    const __m256i low_mask = _mm256_set1_epi8(0x0f);

    __m256i local = _mm256_setzero_si256();
    __m256i q0 = _mm256_loadu_si256((const __m256i_u*)b);
    __m256i vec = _mm256_and_si256(q0, a);

   const __m256i lo  = _mm256_and_si256(vec, low_mask);
   const __m256i hi  = _mm256_and_si256(_mm256_srli_epi16(vec, 4), low_mask);
   const __m256i popcnt1 = _mm256_shuffle_epi8(lookup, lo);
   const __m256i popcnt2 = _mm256_shuffle_epi8(lookup, hi);
   local = _mm256_add_epi8(local, popcnt1);
   local = _mm256_add_epi8(local, popcnt2);
   return local;
}

static inline int64_t dotd1q4_inner(const int8_t* a, const int8_t* query, const int32_t length) {
    int r = 0;

    __m256i acc0 = _mm256_setzero_si256();
    __m256i acc1 = _mm256_setzero_si256();
    __m256i acc2 = _mm256_setzero_si256();
    __m256i acc3 = _mm256_setzero_si256();

    int upperBound = length & ~(STRIDE_BYTES_LEN - 1);
    for (; r < upperBound; r += STRIDE_BYTES_LEN) {
        __m256i value = _mm256_loadu_si256((const __m256i_u*)(a + r));

        __m256i local = dot_bit_256(value, query + r);
        acc0 = _mm256_add_epi64(acc0, _mm256_sad_epu8(local, _mm256_setzero_si256()));

        local = dot_bit_256(value, query + r + length);
        acc1 = _mm256_add_epi64(acc1, _mm256_sad_epu8(local, _mm256_setzero_si256()));

        local = dot_bit_256(value, query + r + 2 * length);
        acc2 = _mm256_add_epi64(acc2, _mm256_sad_epu8(local, _mm256_setzero_si256()));

        local = dot_bit_256(value, query + r + 3 * length);
        acc3 = _mm256_add_epi64(acc3, _mm256_sad_epu8(local, _mm256_setzero_si256()));
    }

    int64_t subRet0 = mm256_reduce_epi64<_mm_add_epi64>(acc0);
    int64_t subRet1 = mm256_reduce_epi64<_mm_add_epi64>(acc1);
    int64_t subRet2 = mm256_reduce_epi64<_mm_add_epi64>(acc2);
    int64_t subRet3 = mm256_reduce_epi64<_mm_add_epi64>(acc3);

    upperBound = length & ~(sizeof(int32_t) - 1);
    for (; r < upperBound; r += sizeof(int32_t)) {
        int32_t value = *((int32_t*)(a + r));
        int32_t q0 = *((int32_t*)(query + r));
        subRet0 += __builtin_popcount(q0 & value);
        int32_t q1 = *((int32_t*)(query + r + length));
        subRet1 += __builtin_popcount(q1 & value);
        int32_t q2 = *((int32_t*)(query + r + 2 * length));
        subRet2 += __builtin_popcount(q2 & value);
        int32_t q3 = *((int32_t*)(query + r + 3 * length));
        subRet3 += __builtin_popcount(q3 & value);
    }
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

EXPORT int64_t vec_dotd1q4(
    const int8_t* a_ptr,
    const int8_t* query_ptr,
    const int32_t length
) {
    return dotd1q4_inner(a_ptr, query_ptr, length);
}

EXPORT int64_t vec_dotd2q4(
    const int8_t* a_ptr,
    const int8_t* query_ptr,
    const int32_t length
) {
    int64_t lower = dotd1q4_inner(a_ptr, query_ptr, length/2);
    int64_t upper = dotd1q4_inner(a_ptr + length/2, query_ptr, length/2);
    return lower + (upper << 1);
}

template <int64_t(*mapper)(const int32_t, const int32_t*)>
static inline void dotd1q4_inner_bulk(
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

        results[c + 0] = (f32_t)dotd1q4_inner(a0, query, length);
        results[c + 1] = (f32_t)dotd1q4_inner(a1, query, length);
        results[c + 2] = (f32_t)dotd1q4_inner(a2, query, length);
        results[c + 3] = (f32_t)dotd1q4_inner(a3, query, length);

        a0 = next_a0;
        a1 = next_a1;
        a2 = next_a2;
        a3 = next_a3;
    }

    // Tail-handling: remaining vectors
    for (; c < count; c++) {
        const int8_t* a0 = a + mapper(c, offsets) * pitch;
        results[c] = (f32_t)dotd1q4_inner(a0, query, length);
    }
}

EXPORT void vec_dotd1q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1q4_inner_bulk<identity_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd1q4_bulk_offsets(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dotd1q4_inner_bulk<array_mapper>(a, query, length, pitch, offsets, count, results);
}

template <int64_t(*mapper)(const int32_t, const int32_t*)>
static inline void dotd2q4_inner_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    const int lines_to_fetch = length / CACHE_LINE_SIZE + 1;
    const int bit_length = length/2;
    int c = 0;

    const int8_t* a0 = safe_mapper_offset<int8_t, 0, mapper>(a, pitch, offsets, count);
    const int8_t* a1 = safe_mapper_offset<int8_t, 1, mapper>(a, pitch, offsets, count);

    // Process a batch of 2 vectors at a time, after instructing the CPU to
    // prefetch the next batch.
    // Prefetching multiple memory locations while computing keeps the CPU
    // execution units busy.
    for (; c + 3 < count; c += 2) {
        const int8_t* next_a0 = a + mapper(c + 2, offsets) * pitch;
        const int8_t* next_a1 = a + mapper(c + 3, offsets) * pitch;

        prefetch(next_a0, lines_to_fetch);
        prefetch(next_a1, lines_to_fetch);

        int64_t lower0 = dotd1q4_inner(a0, query, bit_length);
        int64_t lower1 = dotd1q4_inner(a1, query, bit_length);
        int64_t upper0 = dotd1q4_inner(a0 + bit_length, query, bit_length);
        int64_t upper1 = dotd1q4_inner(a1 + bit_length, query, bit_length);

        results[c + 0] = (f32_t)(lower0 + (upper0 << 1));
        results[c + 1] = (f32_t)(lower1 + (upper1 << 1));

        a0 = next_a0;
        a1 = next_a1;
    }

    // Tail-handling: remaining vectors
    for (; c < count; c++) {
        const int8_t* a0 = a + mapper(c, offsets) * pitch;
        int64_t lower = dotd1q4_inner(a0, query, length/2);
        int64_t upper = dotd1q4_inner(a0 + length/2, query, length/2);
        results[c] = (f32_t)(lower + (upper << 1));
    }
}

EXPORT void vec_dotd2q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<identity_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd2q4_bulk_offsets(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<array_mapper>(a, query, length, pitch, offsets, count, results);
}

EXPORT int64_t vec_dotd4q4(const int8_t* a, const int8_t* query, const int32_t length) {
    const int32_t bit_length = length / 4;
    int64_t p0 = dotd1q4_inner(a + 0 * bit_length, query, bit_length);
    int64_t p1 = dotd1q4_inner(a + 1 * bit_length, query, bit_length);
    int64_t p2 = dotd1q4_inner(a + 2 * bit_length, query, bit_length);
    int64_t p3 = dotd1q4_inner(a + 3 * bit_length, query, bit_length);
    return p0 + (p1 << 1) + (p2 << 2) + (p3 << 3);
}

template <int64_t(*mapper)(const int32_t, const int32_t*)>
static inline void dotd4q4_inner_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    const int32_t bit_length = length / 4;
    for (int c = 0; c < count; c++) {
        const int8_t* a0 = a + mapper(c, offsets) * pitch;

        int64_t p0 = dotd1q4_inner(a0 + 0 * bit_length, query, bit_length);
        int64_t p1 = dotd1q4_inner(a0 + 1 * bit_length, query, bit_length);
        int64_t p2 = dotd1q4_inner(a0 + 2 * bit_length, query, bit_length);
        int64_t p3 = dotd1q4_inner(a0 + 3 * bit_length, query, bit_length);

        results[c] = (f32_t)(p0 + (p1 << 1) + (p2 << 2) + (p3 << 3));
    }
}

EXPORT void vec_dotd4q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<identity_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd4q4_bulk_offsets(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<array_mapper>(a, query, length, pitch, offsets, count, results);
}
