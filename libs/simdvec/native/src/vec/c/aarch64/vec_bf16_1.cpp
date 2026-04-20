/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// BF16 vector implementations for basic ARM NEON processors

#include <stddef.h>
#include <arm_neon.h>
#include <algorithm>
#include "vec.h"
#include "vec_common.h"
#include "aarch64/aarch64_vec_common.h"

static inline float32x4_t bf16_to_f32(uint16x4_t bf16) {
    return vreinterpretq_f32_u32(vshll_n_u16(bf16, 16));
}

static inline float32x4_t load_bf16(const bf16_t* ptr, int elements) {
    return bf16_to_f32(vld1_u16((const uint16_t*)(ptr + elements)));
}

static inline float32x4_t load_f32(const f32_t* ptr, int elements) {
    return vld1q_f32(ptr + elements);
}

template<
    typename TQuery,
    float32x4_t(*load_q)(const TQuery*, int element),
    float32x4_t(*vector_op)(float32x4_t, float32x4_t, float32x4_t),
    f32_t(*scalar_op)(bf16_t, TQuery)
>
static inline f32_t bf16_inner(const bf16_t* d, const TQuery* q, const int32_t elementCount) {
    constexpr int batches = 8;

    float32x4_t sums[batches];
    apply_indexed<batches>([&](auto I) {
        sums[I] = vdupq_n_f32(0.0f);
    });

    int i = 0;
    // each value has <elements> floats, and we iterate over <stride> floats at a time
    constexpr int elements = sizeof(uint16x4_t) / sizeof(bf16_t);
    constexpr int stride = sizeof(uint16x4_t) / sizeof(bf16_t) * batches;
    for (; i < (elementCount & ~(stride - 1)); i += stride) {
        apply_indexed<batches>([&](auto I) {
            sums[I] = vector_op(sums[I], load_bf16(d, i + I * elements), load_q(q, i + I * elements));
        });
    }

    float32x4_t total = tree_reduce<batches, float32x4_t, vaddq_f32>(sums);
    f32_t result = vaddvq_f32(total);

    // Handle remaining elements
    for (; i < elementCount; ++i) {
        result += scalar_op(d[i], q[i]);
    }

    return result;
}

/*
 * BFloat16 bulk operation. Iterates over 8 sequential vectors at a time.
 *
 * Template parameters:
 * TData:    type of the input data pointer (e.g. bf16_t or const bf16_t*)
 * TQuery: the type of query vector
 * mapper: gets the nth vector from the input array.
 * load_q: loads the query vector as a float32x4_t
 * inner_op: SIMD per-dimension vector operation, takes sum, a, b, returns new sum
 * scalar_op: scalar per-dimension vector operation, takes a, b, returns sum
 * bulk_tail: complete vector comparison on a single vector
 *
 * This should compile to a single inline method, with no function callouts.
 */
template <
    typename TData,
    typename TQuery,
    const bf16_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    float32x4_t(*load_q)(const TQuery*, const int),
    float32x4_t(*inner_op)(const float32x4_t, const float32x4_t, const float32x4_t),
    f32_t(*scalar_op)(const bf16_t, const TQuery),
    f32_t(*bulk_tail)(const bf16_t*, const TQuery*, const int32_t),
    int batches = 8
>
static inline void bf16_bulk_inner(
    const TData* a,
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
        float32x4_t sums[batches];
        apply_indexed<batches>([&](auto I) {
            as[I] = mapper(a, c + I, offsets, pitch);
            sums[I] = vdupq_n_f32(0.0f);
        });

        int32_t i = 0;
        // do <batches> vectors at a time, iterating through the dimensions in parallel
        constexpr int stride = sizeof(uint16x4_t) / sizeof(bf16_t);
        for (; i < (dims & ~(stride - 1)); i += stride) {
            float32x4_t bi = load_q(b, i);
            apply_indexed<batches>([&](auto I) {
                sums[I] = inner_op(sums[I], load_bf16(as[I], i), bi);
            });
        }

        f32_t res[batches];
        apply_indexed<batches>([&](auto I) {
            res[I] = vaddvq_f32(sums[I]);
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

// const bf16_t* a  pointer to the first float vector
// const f32_t* b  pointer to the second float vector
// const int32_t elementCount  the number of floating point elements
EXPORT f32_t vec_dotDbf16Qf32(const bf16_t* a, const f32_t* b, const int32_t elementCount) {
    return bf16_inner<f32_t, load_f32, vfmaq_f32, dot_scalar>(a, b, elementCount);
}

// const bf16_t* a  pointer to the first float vector
// const bf16_t* b  pointer to the second float vector
// const int32_t elementCount  the number of floating point elements
EXPORT f32_t vec_dotDbf16Qbf16(const bf16_t* a, const bf16_t* b, const int32_t elementCount) {
    return bf16_inner<bf16_t, load_bf16, vfmaq_f32, dot_scalar>(a, b, elementCount);
}

EXPORT void vec_dotDbf16Qf32_bulk(
    const bf16_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results
) {
    bf16_bulk_inner<bf16_t, f32_t, sequential_mapper, load_f32, vfmaq_f32, dot_scalar, vec_dotDbf16Qf32>(
        a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_dotDbf16Qbf16_bulk(
    const bf16_t* a,
    const bf16_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results
) {
    bf16_bulk_inner<bf16_t, bf16_t, sequential_mapper, load_bf16, vfmaq_f32, dot_scalar, vec_dotDbf16Qbf16>(
        a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_dotDbf16Qf32_bulk_sparse(
    const void* const* addresses,
    const f32_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    bf16_bulk_inner<const bf16_t*, f32_t, sparse_mapper, load_f32, vfmaq_f32, dot_scalar, vec_dotDbf16Qf32>(
        (const bf16_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT void vec_dotDbf16Qbf16_bulk_sparse(
    const void* const* addresses,
    const bf16_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    bf16_bulk_inner<const bf16_t*, bf16_t, sparse_mapper, load_bf16, vfmaq_f32, dot_scalar, vec_dotDbf16Qbf16>(
        (const bf16_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT void vec_dotDbf16Qf32_bulk_offsets(
    const bf16_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    bf16_bulk_inner<bf16_t, f32_t, offsets_mapper, load_f32, vfmaq_f32, dot_scalar, vec_dotDbf16Qf32>(
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
    bf16_bulk_inner<bf16_t, bf16_t, offsets_mapper, load_bf16, vfmaq_f32, dot_scalar, vec_dotDbf16Qbf16>(
        a, b, dims, pitch / sizeof(bf16_t), offsets, count, results);
}

static inline float32x4_t sqrf32_vector(float32x4_t sum, float32x4_t a, float32x4_t b) {
    float32x4_t diff = vsubq_f32(a, b);
    return vmlaq_f32(sum, diff, diff);
}

// const bf16_t* a  pointer to the first float vector
// const f32_t* b  pointer to the second float vector
// const int32_t elementCount  the number of floating point elements
EXPORT f32_t vec_sqrDbf16Qf32(const bf16_t* a, const f32_t* b, const int32_t elementCount) {
    return bf16_inner<f32_t, load_f32, sqrf32_vector, sqr_scalar>(a, b, elementCount);
}

// const bf16_t* a  pointer to the first float vector
// const bf16_t* b  pointer to the second float vector
// const int32_t elementCount  the number of floating point elements
EXPORT f32_t vec_sqrDbf16Qbf16(const bf16_t* a, const bf16_t* b, const int32_t elementCount) {
    return bf16_inner<bf16_t, load_bf16, sqrf32_vector, sqr_scalar>(a, b, elementCount);
}

EXPORT void vec_sqrDbf16Qf32_bulk(
    const bf16_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results
) {
    bf16_bulk_inner<bf16_t, f32_t, sequential_mapper, load_f32, sqrf32_vector, sqr_scalar, vec_sqrDbf16Qf32>(
        a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqrDbf16Qbf16_bulk(
    const bf16_t* a,
    const bf16_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results
) {
    bf16_bulk_inner<bf16_t, bf16_t, sequential_mapper, load_bf16, sqrf32_vector, sqr_scalar, vec_sqrDbf16Qbf16>(
        a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqrDbf16Qf32_bulk_sparse(
    const void* const* addresses,
    const f32_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    bf16_bulk_inner<const bf16_t*, f32_t, sparse_mapper, load_f32, sqrf32_vector, sqr_scalar, vec_sqrDbf16Qf32>(
        (const bf16_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT void vec_sqrDbf16Qbf16_bulk_sparse(
    const void* const* addresses,
    const bf16_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    bf16_bulk_inner<const bf16_t*, bf16_t, sparse_mapper, load_bf16, sqrf32_vector, sqr_scalar, vec_sqrDbf16Qbf16>(
        (const bf16_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT void vec_sqrDbf16Qf32_bulk_offsets(
    const bf16_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    bf16_bulk_inner<bf16_t, f32_t, offsets_mapper, load_f32, sqrf32_vector, sqr_scalar, vec_sqrDbf16Qf32>(
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
    bf16_bulk_inner<bf16_t, bf16_t, offsets_mapper, load_bf16, sqrf32_vector, sqr_scalar, vec_sqrDbf16Qbf16>(
        a, b, dims, pitch / sizeof(bf16_t), offsets, count, results);
}
