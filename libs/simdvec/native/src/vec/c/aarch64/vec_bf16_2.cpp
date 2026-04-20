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

// take the low half only - ignore the high half
static inline svfloat32_t low_bf16_to_f32(svbfloat16_t bf16) {
    const svuint16_t u16 = svreinterpret_u16_bf16(bf16);

    svuint32_t low = svlsl_n_u32_x(svptrue_b32(), svunpklo_u32(u16), 16);

    return svreinterpret_f32_u32(low);
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

template<svfloat32_t(*vector_op)(const svbool_t, const svfloat32_t, const svfloat32_t, const svfloat32_t)>
static inline f32_t bf16Qf32_inner_sve(const bfloat16_t* d, const f32_t* q, const int32_t elementCount) {
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
        sum0 = vector_op(all32, sum0, svget2(bf0, 0), svld1_vnum(all32, q + i, 0));
        sum1 = vector_op(all32, sum1, svget2(bf0, 1), svld1_vnum(all32, q + i, 1));

        svfloat32x2_t bf1 = bf16_to_f32(svld1_vnum_bf16(all16, d + i, 1));
        sum2 = vector_op(all32, sum2, svget2(bf1, 0), svld1_vnum(all32, q + i, 2));
        sum3 = vector_op(all32, sum3, svget2(bf1, 1), svld1_vnum(all32, q + i, 3));

        svfloat32x2_t bf2 = bf16_to_f32(svld1_vnum_bf16(all16, d + i, 2));
        sum4 = vector_op(all32, sum4, svget2(bf2, 0), svld1_vnum(all32, q + i, 4));
        sum5 = vector_op(all32, sum5, svget2(bf2, 1), svld1_vnum(all32, q + i, 5));

        svfloat32x2_t bf3 = bf16_to_f32(svld1_vnum_bf16(all16, d + i, 3));
        sum6 = vector_op(all32, sum6, svget2(bf3, 0), svld1_vnum(all32, q + i, 6));
        sum7 = vector_op(all32, sum7, svget2(bf3, 1), svld1_vnum(all32, q + i, 7));
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
        // this might end up loading more than we need, but anything extra
        // will just be re-loaded next iteration
        // TODO: is that a problem? is there a way to limit the load to the f32 count?
        svfloat32_t bf = low_bf16_to_f32(svld1(svwhilelt_b16(i, elementCount), d + i));

        svbool_t pg_f32 = svwhilelt_b32(i, elementCount);
        sum = vector_op(pg_f32, sum, bf, svld1(pg_f32, q + i));
    }

    return svaddv_f32(all32, sum);
}

static inline svfloat32_t dotf32_vector(svbool_t pg, svfloat32_t sum, svfloat32_t a, svfloat32_t b) {
    return svmla_m(pg, sum, a, b);
}

EXPORT f32_t vec_dotDbf16Qf32_2(const bf16_t* a, const f32_t* b, const int32_t elementCount) {
    return bf16Qf32_inner_sve<dotf32_vector>((const bfloat16_t*)a, b, elementCount);
}

/*
 * Squared distance of 2 bf16 vectors using bfdot, via the identity:
 * |a - b|^2 = a*a - 2*a*b + b*b
 * Each term is a dot product computed natively with bfdot, avoiding
 * the costly bf16 -> f32 conversion
 */
static inline f32_t sqrDbf16Qbf16_inner_sve(const bfloat16_t* d, const bfloat16_t* q, const int32_t elementCount) {
    constexpr int batches = 8;
    int i = 0;

    svfloat32_t sum_self0 = svdup_f32(0.0f);
    svfloat32_t sum_self1 = svdup_f32(0.0f);
    svfloat32_t sum_self2 = svdup_f32(0.0f);
    svfloat32_t sum_self3 = svdup_f32(0.0f);
    svfloat32_t sum_self4 = svdup_f32(0.0f);
    svfloat32_t sum_self5 = svdup_f32(0.0f);
    svfloat32_t sum_self6 = svdup_f32(0.0f);
    svfloat32_t sum_self7 = svdup_f32(0.0f);
    svfloat32_t sum_cross0 = svdup_f32(0.0f);
    svfloat32_t sum_cross1 = svdup_f32(0.0f);
    svfloat32_t sum_cross2 = svdup_f32(0.0f);
    svfloat32_t sum_cross3 = svdup_f32(0.0f);
    svfloat32_t sum_cross4 = svdup_f32(0.0f);
    svfloat32_t sum_cross5 = svdup_f32(0.0f);
    svfloat32_t sum_cross6 = svdup_f32(0.0f);
    svfloat32_t sum_cross7 = svdup_f32(0.0f);

    const int elements = svcnth();
    const int stride = elements * batches;
    const svbool_t all16 = svptrue_b16();
    for (; i + stride <= elementCount; i += stride) {
        svbfloat16_t av0 = svld1_vnum(all16, d + i, 0);
        svbfloat16_t bv0 = svld1_vnum(all16, q + i, 0);
        sum_self0 = svbfdot_f32(sum_self0, av0, av0);
        sum_cross0 = svbfdot_f32(sum_cross0, av0, bv0);
        sum_self0 = svbfdot_f32(sum_self0, bv0, bv0);

        svbfloat16_t av1 = svld1_vnum(all16, d + i, 1);
        svbfloat16_t bv1 = svld1_vnum(all16, q + i, 1);
        sum_self1 = svbfdot_f32(sum_self1, av1, av1);
        sum_cross1 = svbfdot_f32(sum_cross1, av1, bv1);
        sum_self1 = svbfdot_f32(sum_self1, bv1, bv1);

        svbfloat16_t av2 = svld1_vnum(all16, d + i, 2);
        svbfloat16_t bv2 = svld1_vnum(all16, q + i, 2);
        sum_self2 = svbfdot_f32(sum_self2, av2, av2);
        sum_cross2 = svbfdot_f32(sum_cross2, av2, bv2);
        sum_self2 = svbfdot_f32(sum_self2, bv2, bv2);

        svbfloat16_t av3 = svld1_vnum(all16, d + i, 3);
        svbfloat16_t bv3 = svld1_vnum(all16, q + i, 3);
        sum_self3 = svbfdot_f32(sum_self3, av3, av3);
        sum_cross3 = svbfdot_f32(sum_cross3, av3, bv3);
        sum_self3 = svbfdot_f32(sum_self3, bv3, bv3);

        svbfloat16_t av4 = svld1_vnum(all16, d + i, 4);
        svbfloat16_t bv4 = svld1_vnum(all16, q + i, 4);
        sum_self4 = svbfdot_f32(sum_self4, av4, av4);
        sum_cross4 = svbfdot_f32(sum_cross4, av4, bv4);
        sum_self4 = svbfdot_f32(sum_self4, bv4, bv4);

        svbfloat16_t av5 = svld1_vnum(all16, d + i, 5);
        svbfloat16_t bv5 = svld1_vnum(all16, q + i, 5);
        sum_self5 = svbfdot_f32(sum_self5, av5, av5);
        sum_cross5 = svbfdot_f32(sum_cross5, av5, bv5);
        sum_self5 = svbfdot_f32(sum_self5, bv5, bv5);

        svbfloat16_t av6 = svld1_vnum(all16, d + i, 6);
        svbfloat16_t bv6 = svld1_vnum(all16, q + i, 6);
        sum_self6 = svbfdot_f32(sum_self6, av6, av6);
        sum_cross6 = svbfdot_f32(sum_cross6, av6, bv6);
        sum_self6 = svbfdot_f32(sum_self6, bv6, bv6);

        svbfloat16_t av7 = svld1_vnum(all16, d + i, 7);
        svbfloat16_t bv7 = svld1_vnum(all16, q + i, 7);
        sum_self7 = svbfdot_f32(sum_self7, av7, av7);
        sum_cross7 = svbfdot_f32(sum_cross7, av7, bv7);
        sum_self7 = svbfdot_f32(sum_self7, bv7, bv7);
    }

    const svbool_t all32 = svptrue_b32();
    svfloat32_t sum_self = svadd_f32_x(all32,
        svadd_f32_x(all32,
            svadd_f32_x(all32, sum_self0, sum_self1),
            svadd_f32_x(all32, sum_self2, sum_self3)),
        svadd_f32_x(all32,
             svadd_f32_x(all32, sum_self4, sum_self5),
             svadd_f32_x(all32, sum_self6, sum_self7)));
    svfloat32_t sum_cross = svadd_f32_x(all32,
        svadd_f32_x(all32,
            svadd_f32_x(all32, sum_cross0, sum_cross1),
            svadd_f32_x(all32, sum_cross2, sum_cross3)),
        svadd_f32_x(all32,
             svadd_f32_x(all32, sum_cross4, sum_cross5),
             svadd_f32_x(all32, sum_cross6, sum_cross7)));

    // unstrided tail
    for (; i < elementCount; i += elements) {
        svbool_t pg = svwhilelt_b16(i, elementCount);
        svbfloat16_t av = svld1(pg, d + i);
        svbfloat16_t bv = svld1(pg, q + i);
        // don't need predicated vector_op (even for odd dimensions), inactive lanes are all zero anyway
        sum_self = svbfdot_f32(sum_self, av, av);
        sum_cross = svbfdot_f32(sum_cross, av, bv);
        sum_self = svbfdot_f32(sum_self, bv, bv);
    }

    // |a - b|^2 = a*a - 2*a*b + b*b
    return svaddv_f32(all32, sum_self) - 2.0f * svaddv_f32(all32, sum_cross);
}

EXPORT f32_t vec_sqrDbf16Qbf16_2(const bf16_t* a, const bf16_t* b, const int32_t elementCount) {
    return sqrDbf16Qbf16_inner_sve((const bfloat16_t*)a, (const bfloat16_t*)b, elementCount);
}

static inline svfloat32_t sqrf32_vector(svbool_t pg, svfloat32_t sum, svfloat32_t a, svfloat32_t b) {
    svfloat32_t diff = svsub_x(pg, a, b);
    return svmla_m(pg, sum, diff, diff);
}

EXPORT f32_t vec_sqrDbf16Qf32_2(const bf16_t* a, const f32_t* b, const int32_t elementCount) {
    return bf16Qf32_inner_sve<sqrf32_vector>((const bfloat16_t*)a, b, elementCount);
}
