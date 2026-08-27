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
#include <algorithm>
#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"

#ifndef STRIDE_BYTES_LEN
#define STRIDE_BYTES_LEN sizeof(__m256i) // Must be a power of 2
#endif

// Accumulates acc += dot(pa, pb) for unsigned 7-bit int lanes (32 bytes per step).
template<int offsetRegs>
inline void fmai7u(__m256i& acc, const int8_t* pa, const int8_t* pb) {
    constexpr int lanes = offsetRegs * STRIDE_BYTES_LEN;
    const __m256i a = _mm256_loadu_si256((const __m256i_u*)(pa + lanes));
    const __m256i b = _mm256_loadu_si256((const __m256i_u*)(pb + lanes));
    const __m256i vab = _mm256_maddubs_epi16(a, b);
    const __m256i ones = _mm256_set1_epi16(1);
    acc = _mm256_add_epi32(_mm256_madd_epi16(ones, vab), acc);
}

static inline int32_t doti7u_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims >= STRIDE_BYTES_LEN) {
        i = dims & ~(STRIDE_BYTES_LEN - 1);

        constexpr int batches = 4;
        static_assert(batches % 2 == 0, "batches must be even");
        constexpr int half_batches = batches / 2;
        constexpr int batch_stride = batches * STRIDE_BYTES_LEN;
        constexpr int half_batch_stride = half_batches * STRIDE_BYTES_LEN;
        const int8_t* pa = a;
        const int8_t* pb = b;

        __m256i acc[batches];
        apply_indexed<batches>([&](auto I) {
            acc[I] = _mm256_setzero_si256();
        });

        const int8_t* pa_end = a + (i & ~(batch_stride - 1));
        while (pa < pa_end) {
            apply_indexed<batches>([&](auto I) {
                fmai7u<I>(acc[I], pa, pb);
            });
            pa += batch_stride;
            pb += batch_stride;
        }

        pa_end = a + (i & ~(half_batch_stride - 1));
        while (pa < pa_end) {
            apply_indexed<half_batches>([&](auto I) {
                fmai7u<I>(acc[I], pa, pb);
            });
            pa += half_batch_stride;
            pb += half_batch_stride;
        }

        pa_end = a + i;
        while (pa < pa_end) {
            fmai7u<0>(acc[0], pa, pb);
            pa += STRIDE_BYTES_LEN;
            pb += STRIDE_BYTES_LEN;
        }

        __m256i total_sum = tree_reduce<batches, __m256i, _mm256_add_epi32>(acc);
        res = mm256_reduce_epi32<_mm_add_epi32>(total_sum);
    }
    // scalar tail
    for (; i < dims; i++) {
        res += dot_scalar(a[i], b[i]);
    }
    return res;
}

EXPORT int32_t vec_doti7u(const int8_t* a, const int8_t* b, const int32_t dims) {
    return doti7u_inner(a, b, dims);
}

EXPORT void vec_doti7u_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<int8_t, sequential_mapper, doti7u_inner>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_doti7u_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<int8_t, offsets_mapper, doti7u_inner>(a, b, dims, pitch, offsets, count, results);
}

EXPORT void vec_doti7u_bulk_sparse(
    const void* const* addresses,
    const int8_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<const int8_t*, sparse_mapper, doti7u_inner>((const int8_t* const*)addresses, b, dims, 0, NULL, count, results);
}

// Accumulates acc += sqr_distance(pa, pb) for unsigned 7-bit int lanes (32 bytes per step).
template<int offsetRegs>
inline void sqri7u(__m256i& acc, const int8_t* pa, const int8_t* pb) {
    constexpr int lanes = offsetRegs * STRIDE_BYTES_LEN;
    const __m256i a = _mm256_loadu_si256((const __m256i_u*)(pa + lanes));
    const __m256i b = _mm256_loadu_si256((const __m256i_u*)(pb + lanes));
    const __m256i dist = _mm256_sub_epi8(a, b);
    const __m256i abs_dist = _mm256_sign_epi8(dist, dist);
    const __m256i sqr = _mm256_maddubs_epi16(abs_dist, abs_dist);
    const __m256i ones = _mm256_set1_epi16(1);
    acc = _mm256_add_epi32(_mm256_madd_epi16(ones, sqr), acc);
}

static inline int32_t sqri7u_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims >= STRIDE_BYTES_LEN) {
        i = dims & ~(STRIDE_BYTES_LEN - 1);

        constexpr int batches = 4;
        static_assert(batches % 2 == 0, "batches must be even");
        constexpr int half_batches = batches / 2;
        constexpr int batch_stride = batches * STRIDE_BYTES_LEN;
        constexpr int half_batch_stride = half_batches * STRIDE_BYTES_LEN;
        const int8_t* pa = a;
        const int8_t* pb = b;

        __m256i acc[batches];
        apply_indexed<batches>([&](auto I) {
            acc[I] = _mm256_setzero_si256();
        });

        const int8_t* pa_end = a + (i & ~(batch_stride - 1));
        while (pa < pa_end) {
            apply_indexed<batches>([&](auto I) {
                sqri7u<I>(acc[I], pa, pb);
            });
            pa += batch_stride;
            pb += batch_stride;
        }

        pa_end = a + (i & ~(half_batch_stride - 1));
        while (pa < pa_end) {
            apply_indexed<half_batches>([&](auto I) {
                sqri7u<I>(acc[I], pa, pb);
            });
            pa += half_batch_stride;
            pb += half_batch_stride;
        }

        pa_end = a + i;
        while (pa < pa_end) {
            sqri7u<0>(acc[0], pa, pb);
            pa += STRIDE_BYTES_LEN;
            pb += STRIDE_BYTES_LEN;
        }

        __m256i total_sum = tree_reduce<batches, __m256i, _mm256_add_epi32>(acc);
        res = mm256_reduce_epi32<_mm_add_epi32>(total_sum);
    }
    // scalar tail
    for (; i < dims; i++) {
        res += sqr_scalar(a[i], b[i]);
    }
    return res;
}

EXPORT int32_t vec_sqri7u(const int8_t* a, const int8_t* b, const int32_t dims) {
    return sqri7u_inner(a, b, dims);
}

EXPORT void vec_sqri7u_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<int8_t, sequential_mapper, sqri7u_inner>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqri7u_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<int8_t, offsets_mapper, sqri7u_inner>(a, b, dims, pitch, offsets, count, results);
}

EXPORT void vec_sqri7u_bulk_sparse(
    const void* const* addresses,
    const int8_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<const int8_t*, sparse_mapper, sqri7u_inner>((const int8_t* const*)addresses, b, dims, 0, NULL, count, results);
}
