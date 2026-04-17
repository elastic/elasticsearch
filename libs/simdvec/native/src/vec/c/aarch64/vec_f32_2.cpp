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
#include <arm_sve.h>
#include "vec.h"
#include "vec_common.h"
#include "aarch64/aarch64_vec_common.h"

/*
 * Float bulk operation. Iterates over 4 sequential vectors at a time.
 *
 * Template parameters:
 * mapper: gets the nth vector from the input array.
 * inner_op: SIMD per-dimension vector operation, takes pred, sum, a, b, returns new sum
 * bulk_tail: complete vector comparison on a single vector
 *
 * This should compile to a single inline method, with no function callouts.
 */
template <
    typename TData,
    const f32_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    svfloat32_t(*inner_op)(const svbool_t, const svfloat32_t, const svfloat32_t, const svfloat32_t),
    f32_t(*bulk_tail)(const f32_t*, const f32_t*, const int32_t)
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

    for (; c + 4 <= count; c += 4) {
        const f32_t* a0 = mapper(a, c + 0, offsets, pitch);
        const f32_t* a1 = mapper(a, c + 1, offsets, pitch);
        const f32_t* a2 = mapper(a, c + 2, offsets, pitch);
        const f32_t* a3 = mapper(a, c + 3, offsets, pitch);

        svfloat32_t sum0 = svdup_f32(0.0f);
        svfloat32_t sum1 = svdup_f32(0.0f);
        svfloat32_t sum2 = svdup_f32(0.0f);
        svfloat32_t sum3 = svdup_f32(0.0f);

        int i = 0;
        for (; i < dims; i += svcntw()) {
            svbool_t pg = svwhilelt_b32(i, dims);

            svfloat32_t bv = svld1(pg, b + i);

            sum0 = inner_op(pg, sum0, svld1(pg, a0 + i), bv);
            sum1 = inner_op(pg, sum1, svld1(pg, a1 + i), bv);
            sum2 = inner_op(pg, sum2, svld1(pg, a2 + i), bv);
            sum3 = inner_op(pg, sum3, svld1(pg, a3 + i), bv);
        }

        // Horizontal reduction of the vector accumulators
        results[c + 0] = svaddv(svptrue_b32(), sum0);
        results[c + 1] = svaddv(svptrue_b32(), sum1);
        results[c + 2] = svaddv(svptrue_b32(), sum2);
        results[c + 3] = svaddv(svptrue_b32(), sum3);
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
EXPORT f32_t vec_dotf32_2(const f32_t* a, const f32_t* b, const int32_t elementCount) {
    int i = 0;

    svfloat32_t sum0 = svdup_f32(0.0f);
    svfloat32_t sum1 = svdup_f32(0.0f);
    svfloat32_t sum2 = svdup_f32(0.0f);
    svfloat32_t sum3 = svdup_f32(0.0f);

    const int elements = svcntw();
    const int stride = elements * 4;
    const svbool_t all = svptrue_b32();
    for (; i + stride <= elementCount; i += stride) {
        sum0 = svmla_f32_x(all, sum0, svld1_vnum(all, a + i, 0), svld1_vnum(all, b + i, 0));
        sum1 = svmla_f32_x(all, sum1, svld1_vnum(all, a + i, 1), svld1_vnum(all, b + i, 1));
        sum2 = svmla_f32_x(all, sum2, svld1_vnum(all, a + i, 2), svld1_vnum(all, b + i, 2));
        sum3 = svmla_f32_x(all, sum3, svld1_vnum(all, a + i, 3), svld1_vnum(all, b + i, 3));
    }

    svfloat32_t sum = svadd_f32_x(all,
        svadd_f32_x(all, sum0, sum1),
        svadd_f32_x(all, sum2, sum3));

    // unstrided tail
    for (; i < elementCount; i += elements) {
        svbool_t pg = svwhilelt_b32(i, elementCount);
        sum = svmla_f32_m(pg, sum, svld1(pg, a + i), svld1(pg, b + i));
    }

    return svaddv_f32(svptrue_b32(), sum);
}

static inline svfloat32_t dotf32_vector(const svbool_t pg, const svfloat32_t acc, const svfloat32_t a, const svfloat32_t b) {
    return svmla_f32_m(pg, acc, a, b);
}

EXPORT void vec_dotf32_bulk_2(const f32_t* a, const f32_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_f32_bulk<f32_t, sequential_mapper, dotf32_vector, vec_dotf32_2>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_dotf32_bulk_offsets_2(
    const f32_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_f32_bulk<f32_t, offsets_mapper, dotf32_vector, vec_dotf32_2>(a, b, dims, pitch / sizeof(f32_t), offsets, count, results);
}
/*
static inline float32x4_t sqrf32_vector(float32x4_t sum, float32x4_t a, float32x4_t b) {
    float32x4_t diff = vsubq_f32(a, b);
    return vmlaq_f32(sum, diff, diff);
}

EXPORT f32_t vec_sqrf32(const f32_t* a, const f32_t* b, const int32_t elementCount) {
    constexpr int batches = 8;

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
            sums[I] = sqrf32_vector(sums[I], vld1q_f32(a + i + I * elements), vld1q_f32(b + i + I * elements));
        });
    }

    float32x4_t total = tree_reduce<batches, float32x4_t, vaddq_f32>(sums);
    f32_t result = vaddvq_f32(total);

    // Handle remaining elements
    for (; i < elementCount; ++i) {
        result += sqr_scalar(a[i], b[i]);
    }

    return result;
}

EXPORT void vec_sqrf32_bulk(const f32_t* a, const f32_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_f32_bulk<f32_t, sequential_mapper, sqrf32_vector, sqr_scalar, vec_sqrf32>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqrf32_bulk_offsets(
    const f32_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_f32_bulk<f32_t, offsets_mapper, sqrf32_vector, sqr_scalar, vec_sqrf32>(a, b, dims, pitch / sizeof(f32_t), offsets, count, results);
}
*/
