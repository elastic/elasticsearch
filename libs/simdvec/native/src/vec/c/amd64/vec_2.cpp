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
    int32_t res = 0;
    int i = 0;
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

        // reduce (accumulate all)
        __m512i total_sum = tree_reduce<batches, __m512i, _mm512_add_epi32>(acc);
        res = _mm512_reduce_add_epi32(total_sum);
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
        res += _mm512_reduce_add_epi32(_mm512_madd_epi16(ones, vab));
    }
    return res;
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
    int32_t res = 0;
    int i = 0;
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

        // reduce (accumulate all)
        __m512i total_sum = tree_reduce<batches, __m512i, _mm512_add_epi32>(acc);
        res = _mm512_reduce_add_epi32(total_sum);
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
        res += _mm512_reduce_add_epi32(_mm512_madd_epi16(ones, sqr_add));
    }
    return res;
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

// --- int8 (signed, full range -128..127) AVX-512 implementations ---
// Unlike i7u which uses maddubs (unsigned x signed), i8 needs sign-extension to 16-bit
// before multiply, because both operands can be negative. Each iteration loads 32 bytes
// (into __m256i), sign-extends to __m512i, then uses signed madd_epi16.

// Accumulates acc += dot(pa, pb) for signed int8. Loads 32 bytes at compile-time
// offset, sign-extends to 16-bit, then signed multiply-accumulate.
template<int offsetRegs>
inline void fmai8(__m512i& acc, const int8_t* pa, const int8_t* pb) {
    constexpr int lanes = offsetRegs * sizeof(__m256i);  // 32 bytes per step
    const __m512i a16 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(pa + lanes)));
    const __m512i b16 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(pb + lanes)));
    acc = _mm512_add_epi32(_mm512_madd_epi16(a16, b16), acc);
}

static inline int32_t doti8_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims >= sizeof(__m256i)) {
        i = dims & ~(sizeof(__m256i) - 1);

        constexpr int batches = 4;
        static_assert(batches % 2 == 0, "batches must be even for vectorized AVX-512 operations");
        constexpr int half_batches = batches / 2;
        constexpr int batch_stride = batches * sizeof(__m256i);
        constexpr int half_batch_stride = half_batches * sizeof(__m256i);
        const int8_t* pa = a;
        const int8_t* pb = b;

        __m512i acc[batches];
        apply_indexed<batches>([&](auto I) {
            acc[I] = _mm512_setzero_si512();
        });

        const int8_t* pa_end = a + (i & ~(batch_stride - 1));
        while (pa < pa_end) {
            apply_indexed<batches>([&](auto I) {
                fmai8<I>(acc[I], pa, pb);
            });
            pa += batch_stride;
            pb += batch_stride;
        }

        pa_end = a + (i & ~(half_batch_stride - 1));
        while (pa < pa_end) {
            apply_indexed<half_batches>([&](auto I) {
                fmai8<I>(acc[I], pa, pb);
            });
            pa += half_batch_stride;
            pb += half_batch_stride;
        }

        pa_end = a + i;
        while (pa < pa_end) {
            fmai8<0>(acc[0], pa, pb);
            pa += sizeof(__m256i);
            pb += sizeof(__m256i);
        }

        __m512i total_sum = tree_reduce<batches, __m512i, _mm512_add_epi32>(acc);
        res = _mm512_reduce_add_epi32(total_sum);
    }
    // Masked tail: sign-extend remaining elements (< 32) with a single masked SIMD iteration.
    // Zeroed lanes from maskz_loadu produce zeros in madd_epi16, contributing nothing to the sum.
    const int remaining = dims - i;
    if (remaining > 0) {
        const __mmask32 mask = (__mmask32)((1ULL << remaining) - 1);
        const __m512i a16 = _mm512_cvtepi8_epi16(_mm256_maskz_loadu_epi8(mask, a + i));
        const __m512i b16 = _mm512_cvtepi8_epi16(_mm256_maskz_loadu_epi8(mask, b + i));
        res += _mm512_reduce_add_epi32(_mm512_madd_epi16(a16, b16));
    }
    return res;
}

EXPORT f32_t vec_doti8_2(const int8_t* a, const int8_t* b, const int32_t dims) {
    return (f32_t)doti8_inner(a, b, dims);
}

EXPORT void vec_doti8_bulk_2(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<int8_t, sequential_mapper, doti8_inner, 4>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_doti8_bulk_offsets_2(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<int8_t, offsets_mapper, doti8_inner, 4>(a, b, dims, pitch, offsets, count, results);
}

EXPORT void vec_doti8_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<const int8_t*, sparse_mapper, doti8_inner, 4>((const int8_t* const*)addresses, b, dims, 0, NULL, count, results);
}

// Accumulates acc += sqr_distance(pa, pb) for signed int8. Sign-extends to 16-bit,
// subtracts, then madd for squared accumulation.
template<int offsetRegs>
inline void sqri8(__m512i& acc, const int8_t* pa, const int8_t* pb) {
    constexpr int lanes = offsetRegs * sizeof(__m256i);
    const __m512i a16 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(pa + lanes)));
    const __m512i b16 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(pb + lanes)));
    const __m512i dist = _mm512_sub_epi16(a16, b16);
    acc = _mm512_add_epi32(_mm512_madd_epi16(dist, dist), acc);
}

static inline int32_t sqri8_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims >= sizeof(__m256i)) {
        i = dims & ~(sizeof(__m256i) - 1);

        constexpr int batches = 4;
        static_assert(batches % 2 == 0, "batches must be even for vectorized AVX-512 operations");
        constexpr int half_batches = batches / 2;
        constexpr int batch_stride = batches * sizeof(__m256i);
        constexpr int half_batch_stride = half_batches * sizeof(__m256i);
        const int8_t* pa = a;
        const int8_t* pb = b;

        __m512i acc[batches];
        apply_indexed<batches>([&](auto I) {
            acc[I] = _mm512_setzero_si512();
        });

        const int8_t* pa_end = a + (i & ~(batch_stride - 1));
        while (pa < pa_end) {
            apply_indexed<batches>([&](auto I) {
                sqri8<I>(acc[I], pa, pb);
            });
            pa += batch_stride;
            pb += batch_stride;
        }

        pa_end = a + (i & ~(half_batch_stride - 1));
        while (pa < pa_end) {
            apply_indexed<half_batches>([&](auto I) {
                sqri8<I>(acc[I], pa, pb);
            });
            pa += half_batch_stride;
            pb += half_batch_stride;
        }

        pa_end = a + i;
        while (pa < pa_end) {
            sqri8<0>(acc[0], pa, pb);
            pa += sizeof(__m256i);
            pb += sizeof(__m256i);
        }

        __m512i total_sum = tree_reduce<batches, __m512i, _mm512_add_epi32>(acc);
        res = _mm512_reduce_add_epi32(total_sum);
    }
    // Masked tail: zeroed lanes from maskz_loadu produce zeros in sub/madd, contributing nothing.
    const int remaining = dims - i;
    if (remaining > 0) {
        const __mmask32 mask = (__mmask32)((1ULL << remaining) - 1);
        const __m512i a16 = _mm512_cvtepi8_epi16(_mm256_maskz_loadu_epi8(mask, a + i));
        const __m512i b16 = _mm512_cvtepi8_epi16(_mm256_maskz_loadu_epi8(mask, b + i));
        const __m512i dist = _mm512_sub_epi16(a16, b16);
        res += _mm512_reduce_add_epi32(_mm512_madd_epi16(dist, dist));
    }
    return res;
}

EXPORT f32_t vec_sqri8_2(const int8_t* a, const int8_t* b, const int32_t dims) {
    return (f32_t)sqri8_inner(a, b, dims);
}

EXPORT void vec_sqri8_bulk_2(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<int8_t, sequential_mapper, sqri8_inner, 4>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqri8_bulk_offsets_2(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<int8_t, offsets_mapper, sqri8_inner, 4>(a, b, dims, pitch, offsets, count, results);
}

EXPORT void vec_sqri8_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<const int8_t*, sparse_mapper, sqri8_inner, 4>((const int8_t* const*)addresses, b, dims, 0, NULL, count, results);
}

// --- Cosine i8 (signed) AVX-512 ---
// Computes cosine similarity: sum(a*b) / sqrt(sum(a*a) * sum(b*b))
// Uses sign-extension like dot/sqr i8, with 3 accumulators (sum, a_norm, b_norm).
static inline f32_t cosi8_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    __m512i sum = _mm512_setzero_si512();
    __m512i a_norm = _mm512_setzero_si512();
    __m512i b_norm = _mm512_setzero_si512();

    int i = 0;
    const int blk = dims & ~(sizeof(__m256i) - 1);
    for (; i < blk; i += sizeof(__m256i)) {
        const __m512i a16 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(a + i)));
        const __m512i b16 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(b + i)));
        sum = _mm512_add_epi32(_mm512_madd_epi16(a16, b16), sum);
        a_norm = _mm512_add_epi32(_mm512_madd_epi16(a16, a16), a_norm);
        b_norm = _mm512_add_epi32(_mm512_madd_epi16(b16, b16), b_norm);
    }

    int32_t sum_i32 = _mm512_reduce_add_epi32(sum);
    int32_t a_norm_i32 = _mm512_reduce_add_epi32(a_norm);
    int32_t b_norm_i32 = _mm512_reduce_add_epi32(b_norm);

    // Masked tail
    const int remaining = dims - i;
    if (remaining > 0) {
        const __mmask32 mask = (__mmask32)((1ULL << remaining) - 1);
        const __m512i a16 = _mm512_cvtepi8_epi16(_mm256_maskz_loadu_epi8(mask, a + i));
        const __m512i b16 = _mm512_cvtepi8_epi16(_mm256_maskz_loadu_epi8(mask, b + i));
        sum_i32 += _mm512_reduce_add_epi32(_mm512_madd_epi16(a16, b16));
        a_norm_i32 += _mm512_reduce_add_epi32(_mm512_madd_epi16(a16, a16));
        b_norm_i32 += _mm512_reduce_add_epi32(_mm512_madd_epi16(b16, b16));
    }

    return (f32_t) ((double) sum_i32 / __builtin_sqrt((double) a_norm_i32 * b_norm_i32));
}

EXPORT f32_t vec_cosi8_2(const int8_t* a, const int8_t* b, const int32_t dims) {
    return cosi8_inner(a, b, dims);
}

template <typename TData, const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t), int batches = 4>
static inline void cosi8_inner_bulk(
    const TData* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    const int blk = dims & ~(sizeof(__m256i) - 1);
    const int lines_to_fetch = dims / CACHE_LINE_SIZE + 1;
    int c = 0;

    // Precompute b norm
    __m512i b_norms = _mm512_setzero_si512();
    int bi = 0;
    for (; bi < blk; bi += sizeof(__m256i)) {
        const __m512i vb16 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(b + bi)));
        b_norms = _mm512_add_epi32(_mm512_madd_epi16(vb16, vb16), b_norms);
    }
    int32_t b_norm = _mm512_reduce_add_epi32(b_norms);
    // Masked tail for b norm
    const int b_remaining = dims - bi;
    if (b_remaining > 0) {
        const __mmask32 mask = (__mmask32)((1ULL << b_remaining) - 1);
        const __m512i vb16 = _mm512_cvtepi8_epi16(_mm256_maskz_loadu_epi8(mask, b + bi));
        b_norm += _mm512_reduce_add_epi32(_mm512_madd_epi16(vb16, vb16));
    }

    const int8_t* current_vecs[batches];
    init_pointers<batches, TData, int8_t, mapper>(current_vecs, a, pitch, offsets, 0, count);

    static_assert(batches % 2 == 0, "batches must be even for vectorized cosine");
    for (; c + batches - 1 < count; c += batches) {
        const int8_t* next_vecs[batches];
        const bool has_next = c + 2 * batches - 1 < count;
        if (has_next) {
            apply_indexed<batches>([&](auto I) {
                next_vecs[I] = mapper(a, c + batches + I, offsets, pitch);
                prefetch(next_vecs[I], lines_to_fetch);
            });
        }

        __m512i sums[batches];
        __m512i a_norms[batches];
        apply_indexed<batches>([&](auto I) {
            sums[I] = _mm512_setzero_si512();
            a_norms[I] = _mm512_setzero_si512();
        });

        int i = 0;
        for (; i < blk; i += sizeof(__m256i)) {
            const __m512i vb16 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(b + i)));

            apply_indexed<batches>([&](auto I) {
                const __m512i va16 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(current_vecs[I] + i)));
                sums[I] = _mm512_add_epi32(_mm512_madd_epi16(va16, vb16), sums[I]);
                a_norms[I] = _mm512_add_epi32(_mm512_madd_epi16(va16, va16), a_norms[I]);
            });
        }

        int32_t sum[batches];
        int32_t a_norm[batches];
        apply_indexed<batches>([&](auto I) {
            sum[I] = _mm512_reduce_add_epi32(sums[I]);
            a_norm[I] = _mm512_reduce_add_epi32(a_norms[I]);
        });

        // Masked tail
        const int remaining = dims - i;
        if (remaining > 0) {
            const __mmask32 mask = (__mmask32)((1ULL << remaining) - 1);
            const __m512i vb16 = _mm512_cvtepi8_epi16(_mm256_maskz_loadu_epi8(mask, b + i));
            apply_indexed<batches>([&](auto I) {
                const __m512i va16 = _mm512_cvtepi8_epi16(_mm256_maskz_loadu_epi8(mask, current_vecs[I] + i));
                sum[I] += _mm512_reduce_add_epi32(_mm512_madd_epi16(va16, vb16));
                a_norm[I] += _mm512_reduce_add_epi32(_mm512_madd_epi16(va16, va16));
            });
        }

        // Vectorized cosine finalization: results[i] = sum[i] / sqrt(a_norm[i] * b_norm)
        // batches=4 fits exactly in one __m128
        __m128 sum_ps = _mm_setr_ps(sum[0], sum[1], sum[2], sum[3]);
        __m128 a_norm_ps = _mm_setr_ps(a_norm[0], a_norm[1], a_norm[2], a_norm[3]);
        __m128 b_norm_ps = _mm_set1_ps(b_norm);
        __m128 res = _mm_div_ps(sum_ps, _mm_sqrt_ps(_mm_mul_ps(a_norm_ps, b_norm_ps)));
        _mm_storeu_ps(results + c, res);

        if (has_next) {
            std::copy_n(next_vecs, batches, current_vecs);
        }
    }

    // Tail-handling: remaining vectors
    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = cosi8_inner(a0, b, dims);
    }
}

EXPORT void vec_cosi8_bulk_2(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    cosi8_inner_bulk<int8_t, sequential_mapper>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_cosi8_bulk_offsets_2(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    cosi8_inner_bulk<int8_t, offsets_mapper>(a, b, dims, pitch, offsets, count, results);
}

EXPORT void vec_cosi8_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results) {
    cosi8_inner_bulk<const int8_t*, sparse_mapper>((const int8_t* const*)addresses, b, dims, 0, NULL, count, results);
}
