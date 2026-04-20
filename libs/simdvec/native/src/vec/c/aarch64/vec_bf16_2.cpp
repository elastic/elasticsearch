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

static inline svfloat32x2_t bf16_to_f32(svbfloat16_t bf16) {
    const svuint16_t u16 = svreinterpret_u16_bf16(bf16);

    svuint32_t low = svlsl_n_u32_x(svptrue_b32(), svunpklo_u32(u16), 16);
    svuint32_t high = svlsl_n_u32_x(svptrue_b32(), svunpkhi_u32(u16), 16);

    return svcreate2(svreinterpret_f32_u32(low), svreinterpret_f32_u32(high));
}

static inline svfloat32_t low_bf16_to_f32(svbfloat16_t bf16) {
    const svuint16_t u16 = svreinterpret_u16_bf16(bf16);

    svuint32_t low = svlsl_n_u32_x(svptrue_b32(), svunpklo_u32(u16), 16);

    return svreinterpret_f32_u32(low);
}

/*
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

static inline f32_t dotDbf16Qbf16_inner_sve(const bfloat16_t* d, const bfloat16_t* q, const int32_t elementCount) {
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
    for (; i + stride <= elementCount; i += stride) {
        sum0 = svbfdot_f32(sum0, svld1_vnum(all16, d + i, 0), svld1_vnum(all16, q + i, 0));
        sum1 = svbfdot_f32(sum1, svld1_vnum(all16, d + i, 1), svld1_vnum(all16, q + i, 1));
        sum2 = svbfdot_f32(sum2, svld1_vnum(all16, d + i, 2), svld1_vnum(all16, q + i, 2));
        sum3 = svbfdot_f32(sum3, svld1_vnum(all16, d + i, 3), svld1_vnum(all16, q + i, 3));
        sum4 = svbfdot_f32(sum4, svld1_vnum(all16, d + i, 4), svld1_vnum(all16, q + i, 4));
        sum5 = svbfdot_f32(sum5, svld1_vnum(all16, d + i, 5), svld1_vnum(all16, q + i, 5));
        sum6 = svbfdot_f32(sum6, svld1_vnum(all16, d + i, 6), svld1_vnum(all16, q + i, 6));
        sum7 = svbfdot_f32(sum7, svld1_vnum(all16, d + i, 7), svld1_vnum(all16, q + i, 7));
    }

    const svbool_t all32 = svptrue_b32();
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

EXPORT f32_t vec_dotDbf16Qbf16_2(const bf16_t* a, const bf16_t* b, const int32_t elementCount) {
    return dotDbf16Qbf16_inner_sve((const bfloat16_t*)a, (const bfloat16_t*)b, elementCount);
}

static inline f32_t dotDbf16Qf32_inner_sve(const bfloat16_t* d, const f32_t* q, const int32_t elementCount) {
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
    const svbool_t all16 = svptrue_b16();
    const svbool_t all32 = svptrue_b32();
    for (; i + stride <= elementCount; i += stride) {
        // load 2 sets of bf16 vectors & extend
        svfloat32x2_t bf0 = bf16_to_f32(svld1_vnum_bf16(all16, d + i, 0));
        sum0 = svmla_f32_m(all32, sum0, svget2(bf0, 0), svld1_vnum(all32, q + i, 0));
        sum1 = svmla_f32_m(all32, sum1, svget2(bf0, 1), svld1_vnum(all32, q + i, 1));

        svfloat32x2_t bf1 = bf16_to_f32(svld1_vnum_bf16(all16, d + i, 1));
        sum2 = svmla_f32_m(all32, sum2, svget2(bf1, 0), svld1_vnum(all32, q + i, 2));
        sum3 = svmla_f32_m(all32, sum3, svget2(bf1, 1), svld1_vnum(all32, q + i, 3));

        svfloat32x2_t bf2 = bf16_to_f32(svld1_vnum_bf16(all16, d + i, 2));
        sum4 = svmla_f32_m(all32, sum4, svget2(bf2, 0), svld1_vnum(all32, q + i, 4));
        sum5 = svmla_f32_m(all32, sum5, svget2(bf2, 1), svld1_vnum(all32, q + i, 5));

        svfloat32x2_t bf3 = bf16_to_f32(svld1_vnum_bf16(all16, d + i, 3));
        sum6 = svmla_f32_m(all32, sum6, svget2(bf3, 0), svld1_vnum(all32, q + i, 6));
        sum7 = svmla_f32_m(all32, sum7, svget2(bf3, 1), svld1_vnum(all32, q + i, 7));
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
        // this might end up loading more than we need
        // TODO: is there a way to limit the load to the f32 count?
        svfloat32_t bf = low_bf16_to_f32(svld1(svwhilelt_b16(i, elementCount), d + i));

        svbool_t pg_f32 = svwhilelt_b32(i, elementCount);
        sum = svmla_f32_m(pg_f32, sum, bf, svld1(pg_f32, q + i));
    }

    return svaddv_f32(all32, sum);
}

EXPORT f32_t vec_dotDbf16Qf32_2(const bf16_t* a, const f32_t* b, const int32_t elementCount) {
    return dotDbf16Qf32_inner_sve((const bfloat16_t*)a, b, elementCount);
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
