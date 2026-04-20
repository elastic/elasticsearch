/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// BF16 vector implementations for ARM SVE processors

#include <stddef.h>
#include <arm_sve.h>
#include "vec.h"
#include "vec_common.h"
#include "aarch64/aarch64_vec_common.h"
/*
static inline float32x4_t bf16_to_f32(uint16x4_t bf16) {
    return vreinterpretq_f32_u32(vshll_n_u16(bf16, 16));
}

static inline float32x4_t load_bf16(const bf16_t* ptr, int elements) {
    return bf16_to_f32(vld1_u16((const uint16_t*)(ptr + elements)));
}

static inline float32x4_t load_f32(const f32_t* ptr, int elements) {
    return vld1q_f32(ptr + elements);
}
*/
template<
    typename TQuery,
    typename TOp,
    TOp(*load_d)(const bf16_t*, int element),
    TOp(*load_q)(const TQuery*, int element),
    svfloat32_t(*vector_op)(svfloat32_t, TOp, TOp),
    f32_t(*scalar_op)(bf16_t, TQuery)
>
static inline f32_t bf16_inner(const bf16_t* d, const TQuery* q, const int32_t elementCount) {
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
        sum0 = vector_op(all, sum0, svld1_vnum(all, d + i, 0), svld1_vnum(all, q + i, 0));
        sum1 = vector_op(all, sum1, svld1_vnum(all, d + i, 1), svld1_vnum(all, q + i, 1));
        sum2 = vector_op(all, sum2, svld1_vnum(all, d + i, 2), svld1_vnum(all, q + i, 2));
        sum3 = vector_op(all, sum3, svld1_vnum(all, d + i, 3), svld1_vnum(all, q + i, 3));
        sum4 = vector_op(all, sum4, svld1_vnum(all, d + i, 4), svld1_vnum(all, q + i, 4));
        sum5 = vector_op(all, sum5, svld1_vnum(all, d + i, 5), svld1_vnum(all, q + i, 5));
        sum6 = vector_op(all, sum6, svld1_vnum(all, d + i, 6), svld1_vnum(all, q + i, 6));
        sum7 = vector_op(all, sum7, svld1_vnum(all, d + i, 7), svld1_vnum(all, q + i, 7));
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
        sum = inner_op(pg, sum, svld1(pg, d + i), svld1(pg, q + i));
    }

    return svaddv_f32(all, sum);
}

template<
    svfloat32_t(*vector_op)(svfloat32_t, svbfloat16_t, svbfloat16_t)
>
static inline f32_t Dbf16Qbf16_inner(const bfloat16_t* d, const bfloat16_t* q, const int32_t elementCount) {
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

    const int elements = svcnth();
    const int stride = elements * batches;
    const svbool_t all16 = svptrue_b16();
    const svbool_t all32 = svptrue_b32();
    for (; i + stride <= elementCount; i += stride) {
        sum0 = vector_op(sum0, svld1_vnum(all16, d + i, 0), svld1_vnum(all16, q + i, 0));
        sum1 = vector_op(sum1, svld1_vnum(all16, d + i, 1), svld1_vnum(all16, q + i, 1));
        sum2 = vector_op(sum2, svld1_vnum(all16, d + i, 2), svld1_vnum(all16, q + i, 2));
        sum3 = vector_op(sum3, svld1_vnum(all16, d + i, 3), svld1_vnum(all16, q + i, 3));
        sum4 = vector_op(sum4, svld1_vnum(all16, d + i, 4), svld1_vnum(all16, q + i, 4));
        sum5 = vector_op(sum5, svld1_vnum(all16, d + i, 5), svld1_vnum(all16, q + i, 5));
        sum6 = vector_op(sum6, svld1_vnum(all16, d + i, 6), svld1_vnum(all16, q + i, 6));
        sum7 = vector_op(sum7, svld1_vnum(all16, d + i, 7), svld1_vnum(all16, q + i, 7));
    }

    svfloat32_t sum = svadd_f32_x(all32,
        svadd_f32_x(all32,
            svadd_f32_x(all32, sum0, sum1),
            svadd_f32_x(all32, sum2, sum3)),
        svadd_f32_x(all32,
             svadd_f32_x(all32, sum4, sum5),
             svadd_f32_x(all32, sum6, sum7)));

    // unstrided tail
    for (; i < elementCount; i += elements) {
        svbool_t pg = svwhilelt_b16(i, elementCount);
        // don't need predicated vector_op (even for odd dimensions), inactive lanes are all zero anyway
        sum = svbfdot_f32(sum, svld1(pg, d + i), svld1(pg, q + i));
    }

    return svaddv_f32(all32, sum);
}

static inline svfloat32_t dotbf16_vector(const svfloat32_t sum, const svbfloat16_t a, const svbfloat16_t b) {
    return svbfdot_f32(sum, a, b);
}

EXPORT f32_t vec_dotDbf16Qbf16_2(const bf16_t* a, const bf16_t* b, const int32_t elementCount) {
    return Dbf16Qbf16_inner<dotbf16_vector>((const bfloat16_t*)a, (const bfloat16_t*)b, elementCount);
}

/*
static inline float32x4_t sqrf32_vector(float32x4_t sum, float32x4_t a, float32x4_t b) {
    float32x4_t diff = vsubq_f32(a, b);
    return vmlaq_f32(sum, diff, diff);
}
EXPORT f32_t vec_sqrDbf16Qbf16(const bf16_t* a, const bf16_t* b, const int32_t elementCount) {
    return bf16_inner<bf16_t, load_bf16, sqrf32_vector, sqr_scalar>(a, b, elementCount);
}
*/
