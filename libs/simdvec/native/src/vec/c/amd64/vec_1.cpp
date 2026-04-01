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
    if (dims >= STRIDE_BYTES_LEN) {
        i += dims & ~(STRIDE_BYTES_LEN - 1);
        res = doti7u_inner(a, b, i);
    }
    for (; i < dims; i++) {
        res += a[i] * b[i];
    }
    return res;
}

template <
    typename TData,
    const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int32_t(*inner_op)(const int8_t*, const int8_t*, const int32_t),
    int32_t(*scalar_op)(const int8_t, const int8_t),
    auto(*bulk_tail)(const int8_t*, const int8_t*, const int32_t),
    int batches = 2,
    int stride = STRIDE_BYTES_LEN
>
static inline void call_i8_bulk(
    const TData* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    const int blk = dims & ~(stride - 1);
    const int lines_to_fetch = dims / CACHE_LINE_SIZE + 1;
    int c = 0;

    // Pointers to the current batch of input vectors, resolved via mapper.
    // current_vecs[0] points to the vector for index 0, [1] for index 1, etc.
    const int8_t* current_vecs[batches];
    init_pointers<batches, TData, int8_t, mapper>(current_vecs, a, pitch, offsets, 0, count);

    // Process a batch of `batches` vectors at a time, after instructing the
    // CPU to prefetch the next batch.
    // Prefetching multiple memory locations while computing keeps the CPU
    // execution units busy. For this "older" generation of x64 processors
    // (supporting AVX2, but not AVX-512), benchmarks show that a batch of 2
    // is ideal -- more, and it starts to hurt performances due to bandwidth
    for (; c + batches - 1 < count; c += batches) {
        // apply_indexed is a compile-time loop: the compiler unrolls it into
        // `batches` copies of the lambda body, each with I as a compile-time
        // constant (0, 1, ..., batches-1). The [&] capture has no runtime
        // cost -- it just lets the lambda reference local variables inline.
        const int8_t* next_vecs[batches];
        const bool has_next = c + 2 * batches - 1 < count;
        if (has_next) {
            apply_indexed<batches>([&](auto I) {
                next_vecs[I] = mapper(a, c + batches + I, offsets, pitch);
                prefetch(next_vecs[I], lines_to_fetch);
            });
        }

        int32_t res[batches] = {};  // needs to be initialized to 0
        int i = 0;
        if (dims > stride) {
            i = blk;
            apply_indexed<batches>([&](auto I) {
                res[I] = inner_op(current_vecs[I], b, i);
            });
        }
        for (; i < dims; i++) {
            const int8_t bb = b[i];
            apply_indexed<batches>([&](auto I) {
                res[I] += scalar_op(current_vecs[I][i], bb);
            });
        }

        apply_indexed<batches>([&](auto I) {
            results[c + I] = (f32_t)res[I];
        });
        if (has_next) {
            std::copy_n(next_vecs, batches, current_vecs);
        }
    }

    // Tail-handling: remaining vectors
    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = (f32_t)bulk_tail(a0, b, dims);
    }
}

EXPORT void vec_doti7u_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<int8_t, sequential_mapper, doti7u_inner, dot_scalar<int8_t>, vec_doti7u>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_doti7u_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<int8_t, offsets_mapper, doti7u_inner, dot_scalar<int8_t>, vec_doti7u>(a, b, dims, pitch, offsets, count, results);
}

EXPORT void vec_doti7u_bulk_sparse(
    const void* const* addresses,
    const int8_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<const int8_t*, sparse_mapper, doti7u_inner, dot_scalar<int8_t>, vec_doti7u>((const int8_t* const*)addresses, b, dims, 0, NULL, count, results);
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
    if (dims >= STRIDE_BYTES_LEN) {
        i += dims & ~(STRIDE_BYTES_LEN - 1);
        res = sqri7u_inner(a, b, i);
    }
    for (; i < dims; i++) {
        int32_t dist = a[i] - b[i];
        res += dist * dist;
    }
    return res;
}

EXPORT void vec_sqri7u_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<int8_t, sequential_mapper, sqri7u_inner, sqr_scalar<int8_t>, vec_sqri7u>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqri7u_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<int8_t, offsets_mapper, sqri7u_inner, sqr_scalar<int8_t>, vec_sqri7u>(a, b, dims, pitch, offsets, count, results);
}

EXPORT void vec_sqri7u_bulk_sparse(
    const void* const* addresses,
    const int8_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<const int8_t*, sparse_mapper, sqri7u_inner, sqr_scalar<int8_t>, vec_sqri7u>((const int8_t* const*)addresses, b, dims, 0, NULL, count, results);
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
    if (dims >= sizeof(__m128i)) {
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

    return (f32_t) ((double) res.sum / __builtin_sqrt((double) res.norm1 * res.norm2));
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
        results[c] = vec_cosi8(a0, b, dims);
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
    if (dims >= sizeof(__m128i)) {
        i += dims & ~(sizeof(__m128i) - 1);
        res = doti8_inner(a, b, i);
    }
    for (; i < dims; i++) {
        res += a[i] * b[i];
    }
    return (f32_t)res;
}

EXPORT void vec_doti8_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<int8_t, sequential_mapper, doti8_inner, dot_scalar<int8_t>, vec_doti8, 2, sizeof(__m128i)>(
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
    call_i8_bulk<int8_t, offsets_mapper, doti8_inner, dot_scalar<int8_t>, vec_doti8, 2, sizeof(__m128i)>(
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
    call_i8_bulk<const int8_t*, sparse_mapper, doti8_inner, dot_scalar<int8_t>, vec_doti8, 2, sizeof(__m128i)>(
        (const int8_t* const*)addresses,
        b,
        dims,
        0,
        NULL,
        count,
        results
    );
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
    if (dims >= sizeof(__m128i)) {
        i += dims & ~(sizeof(__m128i) - 1);
        res = sqri8_inner(a, b, i);
    }
    for (; i < dims; i++) {
        int32_t dist = a[i] - b[i];
        res += dist * dist;
    }
    return (f32_t)res;
}

EXPORT void vec_sqri8_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<int8_t, sequential_mapper, sqri8_inner, sqr_scalar<int8_t>, vec_sqri8, 2, sizeof(__m128i)>(
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
    call_i8_bulk<int8_t, offsets_mapper, sqri8_inner, sqr_scalar<int8_t>, vec_sqri8, 2, sizeof(__m128i)>(
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
    call_i8_bulk<const int8_t*, sparse_mapper, sqri8_inner, sqr_scalar<int8_t>, vec_sqri8, 2, sizeof(__m128i)>(
        (const int8_t* const*)addresses,
        b,
        dims,
        0,
        NULL,
        count,
        results
    );
}

// --- single precision floats

/*
 * Float bulk operation. Iterates over 4 sequential vectors at a time.
 *
 * Template parameters:
 * mapper: gets the nth vector from the input array.
 * inner_op: SIMD per-dimension vector operation, takes a, b, sum, returns new sum
 * scalar_op: scalar per-dimension vector operation, takes a, b, returns sum
 * bulk_tail: complete vector comparison on a single vector
 *
 * This should compile to a single inline method, with no function callouts.
 */
template <
    typename TData,
    const f32_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    __m256(*inner_op)(const __m256, const __m256, const __m256),
    f32_t(*scalar_op)(const f32_t, const f32_t),
    f32_t(*bulk_tail)(const f32_t*, const f32_t*, const int32_t),
    int batches = 4
>
static inline void call_f32_bulk(
    const TData* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    int c = 0;

    for (; c + batches - 1 < count; c += batches) {
        // Pointers to the current batch of input vectors, resolved via mapper.
        // as[0] points to the vector for index 0, [1] for index 1, etc
        const f32_t* as[batches];
        __m256 sums[batches] = {};
        apply_indexed<batches>([&](auto I) {
            as[I] = mapper(a, c + I, offsets, pitch);
            sums[I] = _mm256_setzero_ps();
        });

        int32_t i = 0;
        // do <batches> vectors at a time, iterating through the dimensions in parallel
        // Each __m256 holds 8 floats
        constexpr int stride = sizeof(__m256) / sizeof(f32_t);
        for (; i < (dims & ~(stride - 1)); i += stride) {
            __m256 bi = _mm256_loadu_ps(b + i);
            apply_indexed<batches>([&](auto I) {
                sums[I] = inner_op(_mm256_loadu_ps(as[I] + i), bi, sums[I]);
            });
        }

        f32_t res[batches];
        apply_indexed<batches>([&](auto I) {
            res[I] = mm256_reduce_ps<_mm_add_ps>(sums[I]);
        });

        // dimensions tail
        for (; i < dims; i++) {
            apply_indexed<batches>([&](auto I) {
                res[I] += scalar_op(as[I][i], b[i]);
            });
        }

        // this should be turned into direct value copies by the compiler
        std::copy_n(res, batches, results + c);
    }

    // Tail-handling: remaining vectors
    for (; c < count; c++) {
        const f32_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = bulk_tail(a0, b, dims);
    }
}

// const f32_t* a  pointer to the first float vector
// const f32_t* b  pointer to the second float vector
// const int32_t elementCount  the number of floating point elements
EXPORT f32_t vec_dotf32(const f32_t* a, const f32_t* b, const int32_t elementCount) {
    constexpr int batches = 4;

    __m256 sums[batches];
    apply_indexed<batches>([&](auto I) {
        sums[I] = _mm256_setzero_ps();
    });

    int i = 0;
    // each value has <elements> floats, and we iterate over <stride> floats at a time
    constexpr int elements = sizeof(__m256) / sizeof(f32_t);
    constexpr int stride = sizeof(__m256) / sizeof(f32_t) * batches;
    for (; i < (elementCount & ~(stride - 1)); i += stride) {
        apply_indexed<batches>([&](auto I) {
            sums[I] = _mm256_fmadd_ps(_mm256_loadu_ps(a + i + I * elements), _mm256_loadu_ps(b + i + I * elements), sums[I]);
        });
    }

    // Combine all partial sums
    __m256 total_sum = tree_reduce<batches, __m256, _mm256_add_ps>(sums);
    f32_t result = mm256_reduce_ps<_mm_add_ps>(total_sum);

    for (; i < elementCount; ++i) {
        result += dot_scalar(a[i], b[i]);
    }

    return result;
}

EXPORT void vec_dotf32_bulk(const f32_t* a, const f32_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_f32_bulk<f32_t, sequential_mapper, _mm256_fmadd_ps, dot_scalar<f32_t>, vec_dotf32>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_dotf32_bulk_offsets(
    const f32_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_f32_bulk<f32_t, offsets_mapper, _mm256_fmadd_ps, dot_scalar<f32_t>, vec_dotf32>(a, b, dims, pitch / sizeof(f32_t), offsets, count, results);
}

static inline __m256 sqrf32_vector(__m256 a, __m256 b, __m256 sum) {
    __m256 diff = _mm256_sub_ps(a, b);
    return _mm256_fmadd_ps(diff, diff, sum);
}

// const f32_t* a  pointer to the first float vector
// const f32_t* b  pointer to the second float vector
// const int32_t elementCount  the number of floating point elements
EXPORT f32_t vec_sqrf32(const f32_t* a, const f32_t* b, const int32_t elementCount) {
    constexpr int batches = 4;

    __m256 sums[batches];
    apply_indexed<batches>([&](auto I) {
        sums[I] = _mm256_setzero_ps();
    });

    int i = 0;
    // each value has <elements> floats, and we iterate over <stride> floats at a time
    constexpr int elements = sizeof(__m256) / sizeof(f32_t);
    constexpr int stride = sizeof(__m256) / sizeof(f32_t) * batches;
    for (; i < (elementCount & ~(stride - 1)); i += stride) {
        apply_indexed<batches>([&](auto I) {
            sums[I] = sqrf32_vector(_mm256_loadu_ps(a + i + I * elements), _mm256_loadu_ps(b + i + I * elements), sums[I]);
        });
    }

    // reduce all partial sums
    __m256 total_sum = tree_reduce<batches, __m256, _mm256_add_ps>(sums);
    f32_t result = mm256_reduce_ps<_mm_add_ps>(total_sum);

    for (; i < elementCount; ++i) {
        result += sqr_scalar(a[i], b[i]);
    }

    return result;
}

EXPORT void vec_sqrf32_bulk(const f32_t* a, const f32_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_f32_bulk<f32_t, sequential_mapper, sqrf32_vector, sqr_scalar<f32_t>, vec_sqrf32>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqrf32_bulk_offsets(
    const f32_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_f32_bulk<f32_t, offsets_mapper, sqrf32_vector, sqr_scalar<f32_t>, vec_sqrf32>(a, b, dims, pitch / sizeof(f32_t), offsets, count, results);
}
