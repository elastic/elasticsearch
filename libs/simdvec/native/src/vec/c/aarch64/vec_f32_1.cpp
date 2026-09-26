/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

 // This file contains implementations for basic vector processing functionalities,
 // including support for "1st tier" vector capabilities; in the case of ARM,
 // this first tier include functions for processors supporting at least the NEON
 // instruction set.

#include <stddef.h>
#include <arm_neon.h>
#include <algorithm>
#include "vec.h"
#include "vec_common.h"
#include "aarch64/aarch64_vec_common.h"

/*
 * Float single operation. Iterates over 8 sets of dimensions at a time
 *
 * Template parameters:
 * inner_op: SIMD per-dimension vector operation, takes sum, a, b, returns new sum
 * scalar_op: scalar per-dimension vector operation, takes a, b, returns sum
 *
 * This should compile to a single inline method, with no function callouts.
 */
template <
    float32x4_t(*inner_op)(const float32x4_t, const float32x4_t, const float32x4_t),
    f32_t(*scalar_op)(const f32_t, const f32_t),
    int batches = 8
>
static inline f32_t call_f32_inner(const f32_t* a, const f32_t* b, const int32_t elementCount) {
    float32x4_t sums[batches];
    apply_indexed<batches>([&](auto I) {
        sums[I] = vdupq_n_f32(0.0f);
    });

    int i = 0;
    // each value has <elements> floats, and we iterate over <stride> floats at a time
    constexpr int elements = sizeof(float32x4_t) / sizeof(f32_t);
    constexpr int stride = sizeof(float32x4_t) / sizeof(f32_t) * batches;
    for (; i < (elementCount & ~(stride - 1)); i += stride) {
        apply_indexed<batches>([&](auto I) {
            sums[I] = inner_op(sums[I], vld1q_f32(a + i + I * elements), vld1q_f32(b + i + I * elements));
        });
    }

    float32x4_t total = tree_reduce<batches, float32x4_t, vaddq_f32>(sums);
    f32_t result = vaddvq_f32(total);

    // Handle remaining elements
    for (; i < elementCount; ++i) {
        result += scalar_op(a[i], b[i]);
    }

    return result;
}
/*
 * Float bulk operation. Iterates over 4 sequential vectors at a time.
 *
 * Template parameters:
 * TData: type of the input data pointer (e.g. f32_t or const f32_t*)
 * mapper: gets the nth vector from the input array.
 * inner_op: SIMD per-dimension vector operation, takes sum, a, b, returns new sum
 * scalar_op: scalar per-dimension vector operation, takes a, b, returns sum
 * bulk_tail: complete vector comparison on a single vector
 *
 * This should compile to a single inline method, with no function callouts.
 */
template <
    typename TData,
    const f32_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    float32x4_t(*inner_op)(const float32x4_t, const float32x4_t, const float32x4_t),
    f32_t(*scalar_op)(const f32_t, const f32_t),
    f32_t(*bulk_tail)(const f32_t*, const f32_t*, const int32_t),
    int batches = 8
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
        float32x4_t sums[batches];
        apply_indexed<batches>([&](auto I) {
            as[I] = mapper(a, c + I, offsets, pitch);
            sums[I] = vdupq_n_f32(0.0f);
        });

        int32_t i = 0;
        // do <batches> vectors at a time, iterating through the dimensions in parallel
        constexpr int stride = sizeof(float32x4_t) / sizeof(f32_t);
        for (; i < (dims & ~(stride - 1)); i += stride) {
            float32x4_t bi = vld1q_f32(b + i);
            apply_indexed<batches>([&](auto I) {
                sums[I] = inner_op(sums[I], vld1q_f32(as[I] + i), bi);
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
        const f32_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = bulk_tail(a0, b, dims);
    }
}

// const f32_t* a  pointer to the first float vector
// const f32_t* b  pointer to the second float vector
// const int32_t elementCount  the number of floating point elements
EXPORT f32_t vec_dotf32(const f32_t* a, const f32_t* b, const int32_t elementCount) {
    return call_f32_inner<vfmaq_f32, dot_scalar>(a, b, elementCount);
}

EXPORT void vec_dotf32_bulk(const f32_t* a, const f32_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_f32_bulk<f32_t, sequential_mapper, vfmaq_f32, dot_scalar<f32_t>, vec_dotf32>(
        a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_dotf32_bulk_sparse(
    const void* const* addresses,
    const f32_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    call_f32_bulk<const f32_t*, sparse_mapper, vfmaq_f32, dot_scalar<f32_t>, vec_dotf32>(
        (const f32_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT void vec_dotf32_bulk_offsets(
    const f32_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_f32_bulk<f32_t, offsets_mapper, vfmaq_f32, dot_scalar<f32_t>, vec_dotf32>(
        a, b, dims, pitch / sizeof(f32_t), offsets, count, results);
}

static inline float32x4_t sqrf32_vector(float32x4_t sum, float32x4_t a, float32x4_t b) {
    float32x4_t diff = vsubq_f32(a, b);
    return vmlaq_f32(sum, diff, diff);
}

EXPORT f32_t vec_sqrf32(const f32_t* a, const f32_t* b, const int32_t elementCount) {
    return call_f32_inner<sqrf32_vector, sqr_scalar>(a, b, elementCount);
}

EXPORT void vec_sqrf32_bulk(const f32_t* a, const f32_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_f32_bulk<f32_t, sequential_mapper, sqrf32_vector, sqr_scalar, vec_sqrf32>(
        a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqrf32_bulk_sparse(
    const void* const* addresses,
    const f32_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    call_f32_bulk<const f32_t*, sparse_mapper, sqrf32_vector, sqr_scalar, vec_sqrf32>(
        (const f32_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT void vec_sqrf32_bulk_offsets(
    const f32_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    call_f32_bulk<f32_t, offsets_mapper, sqrf32_vector, sqr_scalar, vec_sqrf32>(
        a, b, dims, pitch / sizeof(f32_t), offsets, count, results);
}
