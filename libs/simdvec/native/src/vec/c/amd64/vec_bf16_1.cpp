/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// BF16 vector implementations for AVX2 processors

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"

static inline __m256 bf16_to_f32(__m128i bf16) {
    __m256i widened = _mm256_cvtepu16_epi32(bf16);
    __m256i shifted = _mm256_slli_epi32(widened, 16);
    return _mm256_castsi256_ps(shifted);
}

static inline __m256 load_bf16(const bf16_t* ptr, const int elements) {
    return bf16_to_f32(_mm_lddqu_si128((const __m128i*)(ptr + elements)));
}

static inline __m256 load_f32(const f32_t* ptr, const int elements) {
    return _mm256_loadu_ps(ptr + elements);
}

/*
 * BFloat16 single operation. Processes 4 dimensions at a time.
 *
 * Template parameters:
 * Q: the type of query vector
 * load_q: loads the query vector as a __m256 of 32-bit floats
 * inner_op: SIMD per-dimension vector operation, takes a, b, sum, returns new sum
 * scalar_op: scalar per-dimension vector operation, takes a, b, returns sum
 *
 * This should compile to a single inline method, with no function callouts.
 */
template<
    typename TQuery,
    __m256(*load_q)(const TQuery*, const int),
    __m256(*vector_op)(const __m256, const __m256, const __m256),
    f32_t(*scalar_op)(const bf16_t, const TQuery)
>
static inline f32_t bf16_inner(const bf16_t* d, const TQuery* q, const int32_t elementCount) {
    constexpr int batches = 4;

    __m256 sums[batches];
    apply_indexed<batches>([&](auto I) {
        sums[I] = _mm256_setzero_ps();
    });

    int i = 0;
    // each value has <elements> floats, and we iterate over <stride> floats at a time
    constexpr int elements = sizeof(__m128) / sizeof(bf16_t);
    constexpr int stride = sizeof(__m128) / sizeof(bf16_t) * batches;
    for (; i < (elementCount & ~(stride - 1)); i += stride) {
        apply_indexed<batches>([&](auto I) {
            sums[I] = vector_op(load_bf16(d, i + I * elements), load_q(q, i + I * elements), sums[I]);
        });
    }

    // Combine all partial sums
    __m256 total_sum = tree_reduce<batches, __m256, _mm256_add_ps>(sums);
    f32_t result = mm256_reduce_ps<_mm_add_ps>(total_sum);

    for (; i < elementCount; ++i) {
        result += scalar_op(d[i], q[i]);
    }

    return result;
}

/*
 * BFloat16 bulk operation. Iterates over 4 sequential vectors at a time.
 *
 * Template parameters:
 * TQuery: the type of query vector
 * mapper: gets the nth vector from the input array.
 * load_q: loads the query vector as a __m256 of 32-bit floats
 * vector_op: SIMD per-dimension vector operation, takes a, b, sum, returns new sum
 * scalar_op: scalar per-dimension vector operation, takes a, b, returns sum
 * bulk_tail: complete vector comparison on a single vector
 *
 * This should compile to a single inline method, with no function callouts.
 */
template <
    typename TQuery,
    const bf16_t*(*mapper)(const bf16_t*, const int32_t, const int32_t*, const int32_t),
    __m256(*load_q)(const TQuery*, const int),
    __m256(*vector_op)(const __m256, const __m256, const __m256),
    f32_t(*scalar_op)(const bf16_t, const TQuery),
    f32_t(*bulk_tail)(const bf16_t*, const TQuery*, const int32_t),
    int batches = 4
>
static inline void bf16_bulk_inner(
    const bf16_t* a,
    const TQuery* b,
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
        const bf16_t* as[batches];
        __m256 sums[batches] = {};
        apply_indexed<batches>([&](auto I) {
            as[I] = mapper(a, c + I, offsets, pitch);
            sums[I] = _mm256_setzero_ps();
        });

        int i = 0;
        // do <batches> vectors at a time, iterating through the dimensions in parallel
        // Each __m128 holds 8 bfloats
        constexpr int stride = sizeof(__m128) / sizeof(bf16_t);
        for (; i < (dims & ~(stride - 1)); i += stride) {
            __m256 bi = load_q(b, i);
            apply_indexed<batches>([&](auto I) {
                sums[I] = vector_op(load_bf16(as[I], i), bi, sums[I]);
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
        const bf16_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = bulk_tail(a0, b, dims);
    }
}

EXPORT f32_t vec_dotDbf16Qf32(const bf16_t* a, const f32_t* b, const int32_t elementCount) {
    return bf16_inner<f32_t, load_f32, _mm256_fmadd_ps, dot_scalar>(a, b, elementCount);
}

EXPORT f32_t vec_dotDbf16Qbf16(const bf16_t* a, const bf16_t* b, const int32_t elementCount) {
    return bf16_inner<bf16_t, load_bf16, _mm256_fmadd_ps, dot_scalar>(a, b, elementCount);
}

EXPORT void vec_dotDbf16Qf32_bulk(const bf16_t* a, const f32_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    bf16_bulk_inner<f32_t, sequential_mapper, load_f32, _mm256_fmadd_ps, dot_scalar, vec_dotDbf16Qf32>(
        a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_dotDbf16Qbf16_bulk(const bf16_t* a, const bf16_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    bf16_bulk_inner<bf16_t, sequential_mapper, load_bf16, _mm256_fmadd_ps, dot_scalar, vec_dotDbf16Qbf16>(
        a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_dotDbf16Qf32_bulk_offsets(
    const bf16_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    bf16_bulk_inner<f32_t, offsets_mapper, load_f32, _mm256_fmadd_ps, dot_scalar, vec_dotDbf16Qf32>(
        a, b, dims, pitch / sizeof(bf16_t), offsets, count, results);
}

EXPORT void vec_dotDbf16Qbf16_bulk_offsets(
    const bf16_t* a,
    const bf16_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    bf16_bulk_inner<bf16_t, offsets_mapper, load_bf16, _mm256_fmadd_ps, dot_scalar, vec_dotDbf16Qbf16>(
        a, b, dims, pitch / sizeof(bf16_t), offsets, count, results);
}

static inline __m256 sqrf32_vector(const __m256 a, const __m256 b, const __m256 sum) {
    __m256 diff = _mm256_sub_ps(a, b);
    return _mm256_fmadd_ps(diff, diff, sum);
}

EXPORT f32_t vec_sqrDbf16Qf32(const bf16_t* a, const f32_t* b, const int32_t elementCount) {
    return bf16_inner<f32_t, load_f32, sqrf32_vector, sqr_scalar>(a, b, elementCount);
}

EXPORT f32_t vec_sqrDbf16Qbf16(const bf16_t* a, const bf16_t* b, const int32_t elementCount) {
    return bf16_inner<bf16_t, load_bf16, sqrf32_vector, sqr_scalar>(a, b, elementCount);
}

EXPORT void vec_sqrDbf16Qf32_bulk(const bf16_t* a, const f32_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    bf16_bulk_inner<f32_t, sequential_mapper, load_f32, sqrf32_vector, sqr_scalar, vec_sqrDbf16Qf32>(
        a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqrDbf16Qbf16_bulk(const bf16_t* a, const bf16_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    bf16_bulk_inner<bf16_t, sequential_mapper, load_bf16, sqrf32_vector, sqr_scalar, vec_sqrDbf16Qbf16>(
        a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqrDbf16Qf32_bulk_offsets(
    const bf16_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    bf16_bulk_inner<f32_t, offsets_mapper, load_f32, sqrf32_vector, sqr_scalar, vec_sqrDbf16Qf32>(
        a, b, dims, pitch / sizeof(bf16_t), offsets, count, results);
}

EXPORT void vec_sqrDbf16Qbf16_bulk_offsets(
    const bf16_t* a,
    const bf16_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    bf16_bulk_inner<bf16_t, offsets_mapper, load_bf16, sqrf32_vector, sqr_scalar, vec_sqrDbf16Qbf16>(
        a, b, dims, pitch / sizeof(bf16_t), offsets, count, results);
}

