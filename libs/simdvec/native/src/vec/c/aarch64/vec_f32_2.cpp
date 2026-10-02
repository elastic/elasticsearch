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
 * Float single operation. Iterates over 8 sets of dimensions at a time
 *
 * Template parameters:
 * inner_op: SIMD per-dimension vector operation, takes pred, sum, a, b, returns new sum
 *
 * This should compile to a single inline method, with no function callouts.
 */
template <
    svfloat32_t(*inner_op)(const svbool_t, const svfloat32_t, const svfloat32_t, const svfloat32_t)
>
static inline f32_t call_f32_inner(const f32_t* a, const f32_t* b, const int32_t elementCount) {
    constexpr int batches = 8;
    int i = 0;

    svfloat32_t sum0 = svdup_f32(0.0f);
    svfloat32_t sum1 = svdup_f32(0.0f);
    svfloat32_t sum2 = svdup_f32(0.0f);
    svfloat32_t sum3 = svdup_f32(0.0f);
    svfloat32_t sum4 = svdup_f32(0.0f);
    svfloat32_t sum5 = svdup_f32(0.0f);
    svfloat32_t sum6 = svdup_f32(0.0f);
    svfloat32_t sum7 = svdup_f32(0.0f);

    const int elements = svcntw();
    const int stride = elements * batches;
    const svbool_t all = svptrue_b32();
    for (; i + stride <= elementCount; i += stride) {
        sum0 = inner_op(all, sum0, svld1_vnum(all, a + i, 0), svld1_vnum(all, b + i, 0));
        sum1 = inner_op(all, sum1, svld1_vnum(all, a + i, 1), svld1_vnum(all, b + i, 1));
        sum2 = inner_op(all, sum2, svld1_vnum(all, a + i, 2), svld1_vnum(all, b + i, 2));
        sum3 = inner_op(all, sum3, svld1_vnum(all, a + i, 3), svld1_vnum(all, b + i, 3));
        sum4 = inner_op(all, sum4, svld1_vnum(all, a + i, 4), svld1_vnum(all, b + i, 4));
        sum5 = inner_op(all, sum5, svld1_vnum(all, a + i, 5), svld1_vnum(all, b + i, 5));
        sum6 = inner_op(all, sum6, svld1_vnum(all, a + i, 6), svld1_vnum(all, b + i, 6));
        sum7 = inner_op(all, sum7, svld1_vnum(all, a + i, 7), svld1_vnum(all, b + i, 7));
    }

    svfloat32_t sum = svadd_f32_x(all,
        svadd_f32_x(all,
            svadd_f32_x(all, sum0, sum1),
            svadd_f32_x(all, sum2, sum3)),
        svadd_f32_x(all,
             svadd_f32_x(all, sum4, sum5),
             svadd_f32_x(all, sum6, sum7)));

    // unstrided tail
    for (; i < elementCount; i += elements) {
        svbool_t pg = svwhilelt_b32(i, elementCount);
        sum = inner_op(pg, sum, svld1(pg, a + i), svld1(pg, b + i));
    }

    return svaddv_f32(all, sum);
}

/*
 * Float bulk operation. Iterates over 8 sequential vectors at a time.
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
    constexpr int batches = 8;
    int c = 0;

    for (; c + batches <= count; c += batches) {
        const f32_t* a0 = mapper(a, c + 0, offsets, pitch);
        const f32_t* a1 = mapper(a, c + 1, offsets, pitch);
        const f32_t* a2 = mapper(a, c + 2, offsets, pitch);
        const f32_t* a3 = mapper(a, c + 3, offsets, pitch);
        const f32_t* a4 = mapper(a, c + 4, offsets, pitch);
        const f32_t* a5 = mapper(a, c + 5, offsets, pitch);
        const f32_t* a6 = mapper(a, c + 6, offsets, pitch);
        const f32_t* a7 = mapper(a, c + 7, offsets, pitch);

        svfloat32_t sum0 = svdup_f32(0.0f);
        svfloat32_t sum1 = svdup_f32(0.0f);
        svfloat32_t sum2 = svdup_f32(0.0f);
        svfloat32_t sum3 = svdup_f32(0.0f);
        svfloat32_t sum4 = svdup_f32(0.0f);
        svfloat32_t sum5 = svdup_f32(0.0f);
        svfloat32_t sum6 = svdup_f32(0.0f);
        svfloat32_t sum7 = svdup_f32(0.0f);

        int i = 0;
        for (; i < dims; i += svcntw()) {
            svbool_t pg = svwhilelt_b32(i, dims);

            svfloat32_t bv = svld1(pg, b + i);

            sum0 = inner_op(pg, sum0, svld1(pg, a0 + i), bv);
            sum1 = inner_op(pg, sum1, svld1(pg, a1 + i), bv);
            sum2 = inner_op(pg, sum2, svld1(pg, a2 + i), bv);
            sum3 = inner_op(pg, sum3, svld1(pg, a3 + i), bv);
            sum4 = inner_op(pg, sum4, svld1(pg, a4 + i), bv);
            sum5 = inner_op(pg, sum5, svld1(pg, a5 + i), bv);
            sum6 = inner_op(pg, sum6, svld1(pg, a6 + i), bv);
            sum7 = inner_op(pg, sum7, svld1(pg, a7 + i), bv);
        }

        // Horizontal reduction of the vector accumulators
        results[c + 0] = svaddv(svptrue_b32(), sum0);
        results[c + 1] = svaddv(svptrue_b32(), sum1);
        results[c + 2] = svaddv(svptrue_b32(), sum2);
        results[c + 3] = svaddv(svptrue_b32(), sum3);
        results[c + 4] = svaddv(svptrue_b32(), sum4);
        results[c + 5] = svaddv(svptrue_b32(), sum5);
        results[c + 6] = svaddv(svptrue_b32(), sum6);
        results[c + 7] = svaddv(svptrue_b32(), sum7);
    }

    // Tail-handling: remaining vectors
    for (; c < count; c++) {
        const f32_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = bulk_tail(a0, b, dims);
    }
}

static inline svfloat32_t dotf32_vector(const svbool_t pg, const svfloat32_t sum, const svfloat32_t a, const svfloat32_t b) {
    return svmla_f32_m(pg, sum, a, b);
}

EXPORT f32_t vec_dotf32_2(const f32_t* a, const f32_t* b, const int32_t elementCount) {
    return call_f32_inner<dotf32_vector>(a, b, elementCount);
}

EXPORT void vec_dotf32_bulk_2(const f32_t* a, const f32_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_f32_bulk<f32_t, sequential_mapper, dotf32_vector, vec_dotf32_2>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_dotf32_bulk_sparse_2(
    const void* const* addresses,
    const f32_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    call_f32_bulk<const f32_t*, sparse_mapper, dotf32_vector, vec_dotf32_2>(
        (const f32_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT void vec_dotf32_bulk_offsets_2(
    const f32_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_f32_bulk<f32_t, offsets_mapper, dotf32_vector, vec_dotf32_2>(
        a, b, dims, pitch / sizeof(f32_t), offsets, count, results);
}

static inline svfloat32_t sqrf32_vector(const svbool_t pg, const svfloat32_t sum, const svfloat32_t a, const svfloat32_t b) {
    svfloat32_t diff = svsub_f32_x(pg, a, b);
    return svmla_f32_m(pg, sum, diff, diff);
}

EXPORT f32_t vec_sqrf32_2(const f32_t* a, const f32_t* b, const int32_t elementCount) {
    return call_f32_inner<sqrf32_vector>(a, b, elementCount);
}

EXPORT void vec_sqrf32_bulk_2(const f32_t* a, const f32_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_f32_bulk<f32_t, sequential_mapper, sqrf32_vector, vec_sqrf32_2>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqrf32_bulk_sparse_2(
    const void* const* addresses,
    const f32_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    call_f32_bulk<const f32_t*, sparse_mapper, sqrf32_vector, vec_sqrf32_2>(
        (const f32_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT void vec_sqrf32_bulk_offsets_2(
    const f32_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_f32_bulk<f32_t, offsets_mapper, sqrf32_vector, vec_sqrf32_2>(
        a, b, dims, pitch / sizeof(f32_t), offsets, count, results);
}
