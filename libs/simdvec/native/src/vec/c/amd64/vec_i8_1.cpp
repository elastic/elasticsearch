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

/*
 * AVX2 doesn't have sign-symmetric 8-bit operations,
 * so we have to sign-extend to 16-bits and operate on those values
 * instead, at the cost of doing double the loops
 */

static inline f32_t cosi8_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    __m256i sum = _mm256_setzero_si256();
    __m256i norm1 = _mm256_setzero_si256();
    __m256i norm2 = _mm256_setzero_si256();

    int i = 0;
    const int blk = dims & ~(sizeof(__m128i) - 1);
    for (; i < blk; i += sizeof(__m128i)) {
        __m128i va8 = _mm_loadu_si128((const __m128i*)(a + i));
        __m128i vb8 = _mm_loadu_si128((const __m128i*)(b + i));

        // sign-extend to 16-bits
        __m256i va16 = _mm256_cvtepi8_epi16(va8);
        __m256i vb16 = _mm256_cvtepi8_epi16(vb8);

        sum = _mm256_add_epi32(_mm256_madd_epi16(va16, vb16), sum);
        norm1 = _mm256_add_epi32(_mm256_madd_epi16(va16, va16), norm1);
        norm2 = _mm256_add_epi32(_mm256_madd_epi16(vb16, vb16), norm2);
    }

    int32_t sum_i32 = mm256_reduce_epi32<_mm_add_epi32>(sum);
    int32_t norm1_i32 = mm256_reduce_epi32<_mm_add_epi32>(norm1);
    int32_t norm2_i32 = mm256_reduce_epi32<_mm_add_epi32>(norm2);
    // scalar tail
    for (; i < dims; i++) {
        int32_t ai = (int32_t) a[i];
        int32_t bi = (int32_t) b[i];
        sum_i32 += ai * bi;
        norm1_i32 += ai * ai;
        norm2_i32 += bi * bi;
    }

    return (f32_t) ((double) sum_i32 / __builtin_sqrt((double) norm1_i32 * norm2_i32));
}

EXPORT f32_t vec_cosi8(const int8_t* a, const int8_t* b, const int32_t dims) {
    return cosi8_inner(a, b, dims);
}

template <typename TData, const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t), int batches = 2>
static inline void cosi8_inner_bulk(
    const TData* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    const int blk = dims & ~(sizeof(__m128i) - 1);
    const int lines_to_fetch = dims / CACHE_LINE_SIZE + 1;
    int c = 0;

    // First of all, calculate the b norm
    __m256i b_norms = _mm256_setzero_si256();

    int bi = 0;
    for(; bi < blk; bi += sizeof(__m128i)) {
        __m128i vb8 = _mm_loadu_si128((const __m128i*)(b + bi));
        __m256i vb16 = _mm256_cvtepi8_epi16(vb8);
        b_norms = _mm256_add_epi32(b_norms, _mm256_madd_epi16(vb16, vb16));
    }
    int32_t b_norm = mm256_reduce_epi32<_mm_add_epi32>(b_norms);
    for (; bi < dims; bi++) {
        b_norm += b[bi] * b[bi];
    }

    const int8_t* current_vecs[batches];
    init_pointers<batches, TData, int8_t, mapper>(current_vecs, a, pitch, offsets, 0, count);

    // Process a batch of `batches` vectors at a time, after instructing the
    // CPU to prefetch the next batch.
    // Prefetching multiple memory locations while computing keeps the CPU
    // execution units busy. For this "older" generation of x64 processors
    // (supporting AVX2, but not AVX-512), benchmarks show that a batch of 2
    // is ideal -- more, and it starts to hurt performances due to bandwidth
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

        __m256i sums[batches];
        __m256i a_norms[batches];
        apply_indexed<batches>([&](auto I) {
            sums[I] = _mm256_setzero_si256();
            a_norms[I] = _mm256_setzero_si256();
        });

        int i = 0;
        for (; i < blk; i += sizeof(__m128i)) {
            __m128i vb8 = _mm_loadu_si128((const __m128i*)(b + i));
            __m256i vb16 = _mm256_cvtepi8_epi16(vb8);

            apply_indexed<batches>([&](auto I) {
                __m128i va8 = _mm_loadu_si128((const __m128i*)(current_vecs[I] + i));
                __m256i va16 = _mm256_cvtepi8_epi16(va8);
                sums[I] = _mm256_add_epi32(_mm256_madd_epi16(va16, vb16), sums[I]);
                a_norms[I] = _mm256_add_epi32(_mm256_madd_epi16(va16, va16), a_norms[I]);
            });
        }

        int32_t sum[batches];
        int32_t a_norm[batches];
        apply_indexed<batches>([&](auto I) {
            sum[I] = mm256_reduce_epi32<_mm_add_epi32>(sums[I]);
            a_norm[I] = mm256_reduce_epi32<_mm_add_epi32>(a_norms[I]);
        });

        for (; i < dims; i++) {
            int32_t bv = (int32_t) b[i];
            apply_indexed<batches>([&](auto I) {
                int32_t av = (int32_t) current_vecs[I][i];
                sum[I] += av * bv;
                a_norm[I] += av * av;
            });
        }

        // Vectorized cosine finalization: results[i] = sum[i] / sqrt(a_norm[i] * b_norm)
        // __m128 holds 4 floats; process full groups of 4, then a remaining
        // pair (if batches % 4 != 0) with masked store.
        constexpr int full_quads = batches / 4;
        constexpr int has_remainder = (batches % 4) != 0;

        apply_indexed<full_quads>([&](auto J) {
            constexpr int j = J * 4;
            __m128 sum_ps = _mm_setr_ps(sum[j], sum[j + 1], sum[j + 2], sum[j + 3]);
            __m128 a_norm_ps = _mm_setr_ps(a_norm[j], a_norm[j + 1], a_norm[j + 2], a_norm[j + 3]);
            __m128 b_norm_ps = _mm_set1_ps(b_norm);
            __m128 res = _mm_div_ps(sum_ps, _mm_sqrt_ps(_mm_mul_ps(a_norm_ps, b_norm_ps)));
            _mm_storeu_ps(results + c + j, res);
        });

        if constexpr (has_remainder) {
            constexpr int j = full_quads * 4;
            __m128 sum_ps = _mm_setr_ps(sum[j], sum[j + 1], 0.0f, 0.0f);
            __m128 a_norm_ps = _mm_setr_ps(a_norm[j], a_norm[j + 1], 0.0f, 0.0f);
            __m128 b_norm_ps = _mm_setr_ps(b_norm, b_norm, 0.0f, 0.0f);
            __m128 res = _mm_div_ps(sum_ps, _mm_sqrt_ps(_mm_mul_ps(a_norm_ps, b_norm_ps)));
            _mm_maskstore_ps(results + c + j, _mm_setr_epi32(-1, -1, 0, 0), res);
        }

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

EXPORT void vec_cosi8_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    cosi8_inner_bulk<int8_t, sequential_mapper>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_cosi8_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    cosi8_inner_bulk<int8_t, offsets_mapper>(a, b, dims, pitch, offsets, count, results);
}

EXPORT void vec_cosi8_bulk_sparse(
    const void* const* addresses,
    const int8_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results) {
    cosi8_inner_bulk<const int8_t*, sparse_mapper>((const int8_t* const*)addresses, b, dims, 0, NULL, count, results);
}

// Accumulates acc += dot(pa, pb) for signed int8. Loads 16 bytes at compile-time offset,
// sign-extends to 16-bit, then signed multiply-accumulate.
template<int offsetRegs>
inline void fmai8(__m256i& acc, const int8_t* pa, const int8_t* pb) {
    constexpr int lanes = offsetRegs * sizeof(__m128i);
    const __m256i a16 = _mm256_cvtepi8_epi16(_mm_loadu_si128((const __m128i*)(pa + lanes)));
    const __m256i b16 = _mm256_cvtepi8_epi16(_mm_loadu_si128((const __m128i*)(pb + lanes)));
    acc = _mm256_add_epi32(_mm256_madd_epi16(a16, b16), acc);
}

static inline int32_t doti8_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims >= sizeof(__m128i)) {
        i = dims & ~(sizeof(__m128i) - 1);

        constexpr int batches = 4;
        static_assert(batches % 2 == 0, "batches must be even");
        constexpr int half_batches = batches / 2;
        constexpr int batch_stride = batches * sizeof(__m128i);
        constexpr int half_batch_stride = half_batches * sizeof(__m128i);
        const int8_t* pa = a;
        const int8_t* pb = b;

        __m256i acc[batches];
        apply_indexed<batches>([&](auto I) {
            acc[I] = _mm256_setzero_si256();
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
            pa += sizeof(__m128i);
            pb += sizeof(__m128i);
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

EXPORT f32_t vec_doti8(const int8_t* a, const int8_t* b, const int32_t dims) {
    return (f32_t)doti8_inner(a, b, dims);
}

EXPORT void vec_doti8_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<int8_t, sequential_mapper, doti8_inner>(
        a,
        b,
        dims,
        dims,
        NULL,
        count,
        results
    );
}

EXPORT void vec_doti8_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<int8_t, offsets_mapper, doti8_inner>(
        a,
        b,
        dims,
        pitch,
        offsets,
        count,
        results
    );
}

EXPORT void vec_doti8_bulk_sparse(
    const void* const* addresses,
    const int8_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<const int8_t*, sparse_mapper, doti8_inner>(
        (const int8_t* const*)addresses,
        b,
        dims,
        0,
        NULL,
        count,
        results
    );
}

// Accumulates acc += sqr_distance(pa, pb) for signed int8. Sign-extends to 16-bit,
// subtracts, then madd for squared accumulation.
template<int offsetRegs>
inline void sqri8(__m256i& acc, const int8_t* pa, const int8_t* pb) {
    constexpr int lanes = offsetRegs * sizeof(__m128i);
    const __m256i a16 = _mm256_cvtepi8_epi16(_mm_loadu_si128((const __m128i*)(pa + lanes)));
    const __m256i b16 = _mm256_cvtepi8_epi16(_mm_loadu_si128((const __m128i*)(pb + lanes)));
    const __m256i dist = _mm256_sub_epi16(a16, b16);
    acc = _mm256_add_epi32(_mm256_madd_epi16(dist, dist), acc);
}

static inline int32_t sqri8_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims >= sizeof(__m128i)) {
        i = dims & ~(sizeof(__m128i) - 1);

        constexpr int batches = 4;
        static_assert(batches % 2 == 0, "batches must be even");
        constexpr int half_batches = batches / 2;
        constexpr int batch_stride = batches * sizeof(__m128i);
        constexpr int half_batch_stride = half_batches * sizeof(__m128i);
        const int8_t* pa = a;
        const int8_t* pb = b;

        __m256i acc[batches];
        apply_indexed<batches>([&](auto I) {
            acc[I] = _mm256_setzero_si256();
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
            pa += sizeof(__m128i);
            pb += sizeof(__m128i);
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

EXPORT f32_t vec_sqri8(const int8_t* a, const int8_t* b, const int32_t dims) {
    return (f32_t)sqri8_inner(a, b, dims);
}

EXPORT void vec_sqri8_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<int8_t, sequential_mapper, sqri8_inner>(
        a,
        b,
        dims,
        dims,
        NULL,
        count,
        results
    );
}

EXPORT void vec_sqri8_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<int8_t, offsets_mapper, sqri8_inner>(
        a,
        b,
        dims,
        pitch,
        offsets,
        count,
        results
    );
}

EXPORT void vec_sqri8_bulk_sparse(
    const void* const* addresses,
    const int8_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<const int8_t*, sparse_mapper, sqri8_inner>(
        (const int8_t* const*)addresses,
        b,
        dims,
        0,
        NULL,
        count,
        results
    );
}
