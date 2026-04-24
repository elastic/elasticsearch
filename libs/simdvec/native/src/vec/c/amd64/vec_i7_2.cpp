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

#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"

// Accumulates acc += dot(pa, pb) for unsigned 7-bit int lanes (64 bytes per step).
template<int offsetRegs>
inline void fmai7u(__m512i& acc, const int8_t* pa, const int8_t* pb) {
    constexpr int lanes = offsetRegs * sizeof(__m512i);
    const __m512i a = _mm512_loadu_si512((const __m512i*)(pa + lanes));
    const __m512i b = _mm512_loadu_si512((const __m512i*)(pb + lanes));
    // Vertically multiply each unsigned 8-bit integer from a with the corresponding
    // signed 8-bit integer from b, producing intermediate signed 16-bit integers.
    const __m512i dot = _mm512_maddubs_epi16(a, b);
    const __m512i ones = _mm512_set1_epi16(1);
    // Horizontally add adjacent pairs of intermediate signed 16-bit ints, and pack the results in 32-bit ints.
    acc = _mm512_add_epi32(_mm512_madd_epi16(ones, dot), acc);
}

static inline int32_t dot7u_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    int i = 0;
    __m512i total_sum = _mm512_setzero_si512();
    if (dims >= sizeof(__m512i)) {
        i = dims & ~(sizeof(__m512i) - 1);

        constexpr int batches = 4;
        static_assert(batches % 2 == 0, "batches must be even for vectorized AVX-512 operations");
        constexpr int half_batches = batches / 2;
        constexpr int batch_stride = batches * sizeof(__m512i);
        constexpr int half_batch_stride = half_batches * sizeof(__m512i);
        const int8_t* pa = a;
        const int8_t* pb = b;

        __m512i acc[batches];
        // Init accumulator(s) with 0
        apply_indexed<batches>([&](auto I) {
            acc[I] = _mm512_setzero_si512();
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
            pa += sizeof(__m512i);
            pb += sizeof(__m512i);
        }

        total_sum = tree_reduce<batches, __m512i, _mm512_add_epi32>(acc);
    }
    // Masked tail: handle remaining elements (< 64) with a single masked SIMD iteration.
    // Zeroed lanes from maskz_loadu produce zeros in maddubs, contributing nothing to the sum.
    const int remaining = dims - i;
    if (remaining > 0) {
        const __mmask64 mask = (__mmask64)((1ULL << remaining) - 1);
        const __m512i va = _mm512_maskz_loadu_epi8(mask, a + i);
        const __m512i vb = _mm512_maskz_loadu_epi8(mask, b + i);
        const __m512i vab = _mm512_maddubs_epi16(va, vb);
        const __m512i ones = _mm512_set1_epi16(1);
        total_sum = _mm512_add_epi32(total_sum, _mm512_madd_epi16(ones, vab));
    }
    return _mm512_reduce_add_epi32(total_sum);
}

EXPORT int32_t vec_dot7u_2(const int8_t* a, const int8_t* b, const int32_t dims) {
    return dot7u_inner(a, b, dims);
}

EXPORT void vec_dot7u_bulk_2(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<int8_t, sequential_mapper, dot7u_inner, 4>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_dot7u_bulk_offsets_2(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<int8_t, offsets_mapper, dot7u_inner, 4>(a, b, dims, pitch, offsets, count, results);
}

// Accumulates acc += sqr_distance(pa, pb) for unsigned 7-bit int lanes (64 bytes per step).
template<int offsetRegs>
inline void sqri7u(__m512i& acc, const int8_t* pa, const int8_t* pb) {
    constexpr int lanes = offsetRegs * sizeof(__m512i);
    const __m512i a = _mm512_loadu_si512((const __m512i*)(pa + lanes));
    const __m512i b = _mm512_loadu_si512((const __m512i*)(pb + lanes));

    const __m512i dist = _mm512_sub_epi8(a, b);
    const __m512i abs_dist = _mm512_abs_epi8(dist);
    const __m512i sqr_add = _mm512_maddubs_epi16(abs_dist, abs_dist);
    const __m512i ones = _mm512_set1_epi16(1);
    acc = _mm512_add_epi32(_mm512_madd_epi16(ones, sqr_add), acc);
}

static inline int32_t sqr7u_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    int i = 0;
    __m512i total_sum = _mm512_setzero_si512();
    if (dims >= sizeof(__m512i)) {
        i = dims & ~(sizeof(__m512i) - 1);

        constexpr int batches = 4;
        static_assert(batches % 2 == 0, "batches must be even for vectorized AVX-512 operations");
        constexpr int half_batches = batches / 2;
        constexpr int batch_stride = batches * sizeof(__m512i);
        constexpr int half_batch_stride = half_batches * sizeof(__m512i);
        const int8_t* pa = a;
        const int8_t* pb = b;

        __m512i acc[batches];
        // Init accumulator(s) with 0
        apply_indexed<batches>([&](auto I) {
            acc[I] = _mm512_setzero_si512();
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
            pa += sizeof(__m512i);
            pb += sizeof(__m512i);
        }

        total_sum = tree_reduce<batches, __m512i, _mm512_add_epi32>(acc);
    }
    // Masked tail: handle remaining elements (< 64) with a single masked SIMD iteration.
    // Zeroed lanes from maskz_loadu produce zeros in sub/abs/maddubs, contributing nothing to the sum.
    const int remaining = dims - i;
    if (remaining > 0) {
        const __mmask64 mask = (__mmask64)((1ULL << remaining) - 1);
        const __m512i va = _mm512_maskz_loadu_epi8(mask, a + i);
        const __m512i vb = _mm512_maskz_loadu_epi8(mask, b + i);
        const __m512i dist = _mm512_sub_epi8(va, vb);
        const __m512i abs_dist = _mm512_abs_epi8(dist);
        const __m512i sqr_add = _mm512_maddubs_epi16(abs_dist, abs_dist);
        const __m512i ones = _mm512_set1_epi16(1);
        total_sum = _mm512_add_epi32(total_sum, _mm512_madd_epi16(ones, sqr_add));
    }
    return _mm512_reduce_add_epi32(total_sum);
}

EXPORT int32_t vec_sqr7u_2(const int8_t* a, const int8_t* b, const int32_t dims) {
    return sqr7u_inner(a, b, dims);
}

EXPORT void vec_sqr7u_bulk_2(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<int8_t, sequential_mapper, sqr7u_inner, 4>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqr7u_bulk_offsets_2(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<int8_t, offsets_mapper, sqr7u_inner, 4>(a, b, dims, pitch, offsets, count, results);
}
