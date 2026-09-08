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
 * We only have bf16-bf16 implementations in SVE, not f32-bf16.
 * In SVE we can use BFDOT to calculate the dot product of two bf16-bf16 vectors,
 * but for f32-bf16 we need to convert to f32 first.
 * NEON has the fast USHLL instruction, but because SVE doesn't specify vector widths explicitly,
 * we can't easily process a 'half-width' bf16 vector.
 * Instead, we would need to use the more expensive UUNPK + LSL instructions to convert bf16 to f32.
 *
 * This means that an SVE implementation is the same speed as
 * or slower than NEON on all relevant processors (as of 2026),
 * so just use the NEON implementation for f32-bf16.
 */

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

/*
 * bf16 is the upper half of the f32 bit pattern, so a vector of bf16 values viewed as 32-bit
 * lanes widens without unpacking: even elements (low half of each lane) by a shift, odd
 * elements (already in the high half) by masking the low half. Element order differs from
 * memory, which is irrelevant for reductions.
 */
static inline svfloat32_t bf16_even_to_f32(svbool_t pg, svint32_t v) {
    return svreinterpret_f32_s32(svlsl_n_s32_x(pg, v, 16));
}

static inline svfloat32_t bf16_odd_to_f32(svbool_t pg, svint32_t v) {
    return svreinterpret_f32_s32(svand_n_s32_x(pg, v, (int32_t)0xFFFF0000));
}

/*
 * Squared distance of 2 bf16 vectors, computed as sum((a - b)^2) on f32-widened lanes.
 * Differencing before squaring keeps the result exact to f32 rounding of the distance itself, so
 * duplicates score exactly 0 and near-duplicates keep their precision even when the norms are
 * large. Four blocks per pass with two accumulators each (even and odd lanes).
 */
static inline f32_t sqrDbf16Qbf16_inner_sve(const bfloat16_t* d, const bfloat16_t* q, const int32_t elementCount) {
    constexpr int batches = 4;
    int i = 0;

    const svbool_t all16 = svptrue_b16();
    const svbool_t all32 = svptrue_b32();
    svfloat32_t se0 = svdup_f32(0.0f), so0 = svdup_f32(0.0f);
    svfloat32_t se1 = svdup_f32(0.0f), so1 = svdup_f32(0.0f);
    svfloat32_t se2 = svdup_f32(0.0f), so2 = svdup_f32(0.0f);
    svfloat32_t se3 = svdup_f32(0.0f), so3 = svdup_f32(0.0f);

    const uint16_t* du = (const uint16_t*)d;
    const uint16_t* qu = (const uint16_t*)q;
    const int elements = svcnth();
    const int stride = elements * batches;
    for (; i + stride <= elementCount; i += stride) {
        svint32_t av0 = svreinterpret_s32_u16(svld1_vnum_u16(all16, du + i, 0));
        svint32_t bv0 = svreinterpret_s32_u16(svld1_vnum_u16(all16, qu + i, 0));
        svfloat32_t de0 = svsub_f32_x(all32, bf16_even_to_f32(all32, av0), bf16_even_to_f32(all32, bv0));
        svfloat32_t do0 = svsub_f32_x(all32, bf16_odd_to_f32(all32, av0), bf16_odd_to_f32(all32, bv0));
        se0 = svmla_f32_x(all32, se0, de0, de0);
        so0 = svmla_f32_x(all32, so0, do0, do0);

        svint32_t av1 = svreinterpret_s32_u16(svld1_vnum_u16(all16, du + i, 1));
        svint32_t bv1 = svreinterpret_s32_u16(svld1_vnum_u16(all16, qu + i, 1));
        svfloat32_t de1 = svsub_f32_x(all32, bf16_even_to_f32(all32, av1), bf16_even_to_f32(all32, bv1));
        svfloat32_t do1 = svsub_f32_x(all32, bf16_odd_to_f32(all32, av1), bf16_odd_to_f32(all32, bv1));
        se1 = svmla_f32_x(all32, se1, de1, de1);
        so1 = svmla_f32_x(all32, so1, do1, do1);

        svint32_t av2 = svreinterpret_s32_u16(svld1_vnum_u16(all16, du + i, 2));
        svint32_t bv2 = svreinterpret_s32_u16(svld1_vnum_u16(all16, qu + i, 2));
        svfloat32_t de2 = svsub_f32_x(all32, bf16_even_to_f32(all32, av2), bf16_even_to_f32(all32, bv2));
        svfloat32_t do2 = svsub_f32_x(all32, bf16_odd_to_f32(all32, av2), bf16_odd_to_f32(all32, bv2));
        se2 = svmla_f32_x(all32, se2, de2, de2);
        so2 = svmla_f32_x(all32, so2, do2, do2);

        svint32_t av3 = svreinterpret_s32_u16(svld1_vnum_u16(all16, du + i, 3));
        svint32_t bv3 = svreinterpret_s32_u16(svld1_vnum_u16(all16, qu + i, 3));
        svfloat32_t de3 = svsub_f32_x(all32, bf16_even_to_f32(all32, av3), bf16_even_to_f32(all32, bv3));
        svfloat32_t do3 = svsub_f32_x(all32, bf16_odd_to_f32(all32, av3), bf16_odd_to_f32(all32, bv3));
        se3 = svmla_f32_x(all32, se3, de3, de3);
        so3 = svmla_f32_x(all32, so3, do3, do3);
    }

    // Predicated tail. Inactive 16-bit lanes load as zero, so a half-covered 32-bit lane (odd
    // element count) yields an exact zero difference and the tail also covers odd counts.
    for (; i < elementCount; i += elements) {
        svbool_t pg = svwhilelt_b16(i, elementCount);
        svint32_t av = svreinterpret_s32_u16(svld1_u16(pg, du + i));
        svint32_t bv = svreinterpret_s32_u16(svld1_u16(pg, qu + i));
        svfloat32_t de = svsub_f32_x(all32, bf16_even_to_f32(all32, av), bf16_even_to_f32(all32, bv));
        svfloat32_t dodd = svsub_f32_x(all32, bf16_odd_to_f32(all32, av), bf16_odd_to_f32(all32, bv));
        se0 = svmla_f32_x(all32, se0, de, de);
        so0 = svmla_f32_x(all32, so0, dodd, dodd);
    }

    svfloat32_t total = svadd_f32_x(all32,
        svadd_f32_x(all32, svadd_f32_x(all32, se0, se1), svadd_f32_x(all32, se2, se3)),
        svadd_f32_x(all32, svadd_f32_x(all32, so0, so1), svadd_f32_x(all32, so2, so3)));
    return svaddv_f32(all32, total);
}

EXPORT f32_t vec_sqrDbf16Qbf16_2(const bf16_t* a, const bf16_t* b, const int32_t elementCount) {
    return sqrDbf16Qbf16_inner_sve((const bfloat16_t*)a, (const bfloat16_t*)b, elementCount);
}

// --- Bulk operations ---

/*
 * Bulk dot product for bf16×bf16 using bfdot.
 * Loads query once per dimension step, applies to all vectors in the batch.
 */
template <
    typename TData,
    const bf16_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t)
>
static inline void dotDbf16Qbf16_bulk_sve(
    const TData* a,
    const bf16_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    constexpr int batches = 8;

    int c = 0;
    const int elements = svcnth();

    for (; c + batches <= count; c += batches) {
        svfloat32_t sums0 = svdup_f32(0.0f);
        svfloat32_t sums1 = svdup_f32(0.0f);
        svfloat32_t sums2 = svdup_f32(0.0f);
        svfloat32_t sums3 = svdup_f32(0.0f);
        svfloat32_t sums4 = svdup_f32(0.0f);
        svfloat32_t sums5 = svdup_f32(0.0f);
        svfloat32_t sums6 = svdup_f32(0.0f);
        svfloat32_t sums7 = svdup_f32(0.0f);

        const bfloat16_t* a0 = (const bfloat16_t*)mapper(a, c + 0, offsets, pitch);
        const bfloat16_t* a1 = (const bfloat16_t*)mapper(a, c + 1, offsets, pitch);
        const bfloat16_t* a2 = (const bfloat16_t*)mapper(a, c + 2, offsets, pitch);
        const bfloat16_t* a3 = (const bfloat16_t*)mapper(a, c + 3, offsets, pitch);
        const bfloat16_t* a4 = (const bfloat16_t*)mapper(a, c + 4, offsets, pitch);
        const bfloat16_t* a5 = (const bfloat16_t*)mapper(a, c + 5, offsets, pitch);
        const bfloat16_t* a6 = (const bfloat16_t*)mapper(a, c + 6, offsets, pitch);
        const bfloat16_t* a7 = (const bfloat16_t*)mapper(a, c + 7, offsets, pitch);

        int i = 0;
        for (; i < dims; i += elements) {
            svbool_t pg = svwhilelt_b16(i, dims);
            svbfloat16_t qi = svld1_bf16(pg, (const bfloat16_t*)(b + i));
            sums0 = svbfdot_f32(sums0, svld1_bf16(pg, (const bfloat16_t*)(a0 + i)), qi);
            sums1 = svbfdot_f32(sums1, svld1_bf16(pg, (const bfloat16_t*)(a1 + i)), qi);
            sums2 = svbfdot_f32(sums2, svld1_bf16(pg, (const bfloat16_t*)(a2 + i)), qi);
            sums3 = svbfdot_f32(sums3, svld1_bf16(pg, (const bfloat16_t*)(a3 + i)), qi);
            sums4 = svbfdot_f32(sums4, svld1_bf16(pg, (const bfloat16_t*)(a4 + i)), qi);
            sums5 = svbfdot_f32(sums5, svld1_bf16(pg, (const bfloat16_t*)(a5 + i)), qi);
            sums6 = svbfdot_f32(sums6, svld1_bf16(pg, (const bfloat16_t*)(a6 + i)), qi);
            sums7 = svbfdot_f32(sums7, svld1_bf16(pg, (const bfloat16_t*)(a7 + i)), qi);
        }

        const svbool_t all32 = svptrue_b32();
        results[c + 0] = svaddv_f32(all32, sums0);
        results[c + 1] = svaddv_f32(all32, sums1);
        results[c + 2] = svaddv_f32(all32, sums2);
        results[c + 3] = svaddv_f32(all32, sums3);
        results[c + 4] = svaddv_f32(all32, sums4);
        results[c + 5] = svaddv_f32(all32, sums5);
        results[c + 6] = svaddv_f32(all32, sums6);
        results[c + 7] = svaddv_f32(all32, sums7);
    }

    // vectors tail
    for (; c < count; c++) {
        const bf16_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = vec_dotDbf16Qbf16_2(a0, b, dims);
    }
}

EXPORT void vec_dotDbf16Qbf16_bulk_2(
    const bf16_t* a,
    const bf16_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results
) {
    dotDbf16Qbf16_bulk_sve<bf16_t, sequential_mapper>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_dotDbf16Qbf16_bulk_sparse_2(
    const void* const* addresses,
    const bf16_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    dotDbf16Qbf16_bulk_sve<const bf16_t*, sparse_mapper>(
        (const bf16_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT void vec_dotDbf16Qbf16_bulk_offsets_2(
    const bf16_t* a,
    const bf16_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    dotDbf16Qbf16_bulk_sve<bf16_t, offsets_mapper>(a, b, dims, pitch / sizeof(bf16_t), offsets, count, results);
}

/*
 * Bulk squared distance for bf16 x bf16, computed as sum((a - b)^2) on f32-widened lanes
 * (see sqrDbf16Qbf16_inner_sve for the numerical rationale). The widened query halves are shared by all vectors in
 * the batch and computed once per dimension step. Eight vectors per batch with two
 * accumulators each fit in the 32 SVE registers without spilling.
 */
template <
    typename TData,
    const bf16_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t)
>
static inline void sqrDbf16Qbf16_bulk_sve(
    const TData* a,
    const bf16_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    constexpr int batches = 8;

    int c = 0;
    const int elements = svcnth();
    const svbool_t all32 = svptrue_b32();
    const uint16_t* bu = (const uint16_t*)b;

    for (; c + batches <= count; c += batches) {
        svfloat32_t se0 = svdup_f32(0.0f), so0 = svdup_f32(0.0f);
        svfloat32_t se1 = svdup_f32(0.0f), so1 = svdup_f32(0.0f);
        svfloat32_t se2 = svdup_f32(0.0f), so2 = svdup_f32(0.0f);
        svfloat32_t se3 = svdup_f32(0.0f), so3 = svdup_f32(0.0f);
        svfloat32_t se4 = svdup_f32(0.0f), so4 = svdup_f32(0.0f);
        svfloat32_t se5 = svdup_f32(0.0f), so5 = svdup_f32(0.0f);
        svfloat32_t se6 = svdup_f32(0.0f), so6 = svdup_f32(0.0f);
        svfloat32_t se7 = svdup_f32(0.0f), so7 = svdup_f32(0.0f);

        const uint16_t* a0 = (const uint16_t*)mapper(a, c + 0, offsets, pitch);
        const uint16_t* a1 = (const uint16_t*)mapper(a, c + 1, offsets, pitch);
        const uint16_t* a2 = (const uint16_t*)mapper(a, c + 2, offsets, pitch);
        const uint16_t* a3 = (const uint16_t*)mapper(a, c + 3, offsets, pitch);
        const uint16_t* a4 = (const uint16_t*)mapper(a, c + 4, offsets, pitch);
        const uint16_t* a5 = (const uint16_t*)mapper(a, c + 5, offsets, pitch);
        const uint16_t* a6 = (const uint16_t*)mapper(a, c + 6, offsets, pitch);
        const uint16_t* a7 = (const uint16_t*)mapper(a, c + 7, offsets, pitch);

        int i = 0;
        for (; i < dims; i += elements) {
            svbool_t pg = svwhilelt_b16(i, dims);

            svint32_t qv = svreinterpret_s32_u16(svld1_u16(pg, bu + i));
            svfloat32_t q_even = bf16_even_to_f32(all32, qv);
            svfloat32_t q_odd = bf16_odd_to_f32(all32, qv);

            svint32_t av0 = svreinterpret_s32_u16(svld1_u16(pg, a0 + i));
            svfloat32_t de0 = svsub_f32_x(all32, bf16_even_to_f32(all32, av0), q_even);
            svfloat32_t do0 = svsub_f32_x(all32, bf16_odd_to_f32(all32, av0), q_odd);
            se0 = svmla_f32_x(all32, se0, de0, de0);
            so0 = svmla_f32_x(all32, so0, do0, do0);

            svint32_t av1 = svreinterpret_s32_u16(svld1_u16(pg, a1 + i));
            svfloat32_t de1 = svsub_f32_x(all32, bf16_even_to_f32(all32, av1), q_even);
            svfloat32_t do1 = svsub_f32_x(all32, bf16_odd_to_f32(all32, av1), q_odd);
            se1 = svmla_f32_x(all32, se1, de1, de1);
            so1 = svmla_f32_x(all32, so1, do1, do1);

            svint32_t av2 = svreinterpret_s32_u16(svld1_u16(pg, a2 + i));
            svfloat32_t de2 = svsub_f32_x(all32, bf16_even_to_f32(all32, av2), q_even);
            svfloat32_t do2 = svsub_f32_x(all32, bf16_odd_to_f32(all32, av2), q_odd);
            se2 = svmla_f32_x(all32, se2, de2, de2);
            so2 = svmla_f32_x(all32, so2, do2, do2);

            svint32_t av3 = svreinterpret_s32_u16(svld1_u16(pg, a3 + i));
            svfloat32_t de3 = svsub_f32_x(all32, bf16_even_to_f32(all32, av3), q_even);
            svfloat32_t do3 = svsub_f32_x(all32, bf16_odd_to_f32(all32, av3), q_odd);
            se3 = svmla_f32_x(all32, se3, de3, de3);
            so3 = svmla_f32_x(all32, so3, do3, do3);

            svint32_t av4 = svreinterpret_s32_u16(svld1_u16(pg, a4 + i));
            svfloat32_t de4 = svsub_f32_x(all32, bf16_even_to_f32(all32, av4), q_even);
            svfloat32_t do4 = svsub_f32_x(all32, bf16_odd_to_f32(all32, av4), q_odd);
            se4 = svmla_f32_x(all32, se4, de4, de4);
            so4 = svmla_f32_x(all32, so4, do4, do4);

            svint32_t av5 = svreinterpret_s32_u16(svld1_u16(pg, a5 + i));
            svfloat32_t de5 = svsub_f32_x(all32, bf16_even_to_f32(all32, av5), q_even);
            svfloat32_t do5 = svsub_f32_x(all32, bf16_odd_to_f32(all32, av5), q_odd);
            se5 = svmla_f32_x(all32, se5, de5, de5);
            so5 = svmla_f32_x(all32, so5, do5, do5);

            svint32_t av6 = svreinterpret_s32_u16(svld1_u16(pg, a6 + i));
            svfloat32_t de6 = svsub_f32_x(all32, bf16_even_to_f32(all32, av6), q_even);
            svfloat32_t do6 = svsub_f32_x(all32, bf16_odd_to_f32(all32, av6), q_odd);
            se6 = svmla_f32_x(all32, se6, de6, de6);
            so6 = svmla_f32_x(all32, so6, do6, do6);

            svint32_t av7 = svreinterpret_s32_u16(svld1_u16(pg, a7 + i));
            svfloat32_t de7 = svsub_f32_x(all32, bf16_even_to_f32(all32, av7), q_even);
            svfloat32_t do7 = svsub_f32_x(all32, bf16_odd_to_f32(all32, av7), q_odd);
            se7 = svmla_f32_x(all32, se7, de7, de7);
            so7 = svmla_f32_x(all32, so7, do7, do7);
        }

        results[c + 0] = svaddv_f32(all32, svadd_f32_x(all32, se0, so0));
        results[c + 1] = svaddv_f32(all32, svadd_f32_x(all32, se1, so1));
        results[c + 2] = svaddv_f32(all32, svadd_f32_x(all32, se2, so2));
        results[c + 3] = svaddv_f32(all32, svadd_f32_x(all32, se3, so3));
        results[c + 4] = svaddv_f32(all32, svadd_f32_x(all32, se4, so4));
        results[c + 5] = svaddv_f32(all32, svadd_f32_x(all32, se5, so5));
        results[c + 6] = svaddv_f32(all32, svadd_f32_x(all32, se6, so6));
        results[c + 7] = svaddv_f32(all32, svadd_f32_x(all32, se7, so7));
    }

    // vectors tail
    for (; c < count; c++) {
        const bf16_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = vec_sqrDbf16Qbf16_2(a0, b, dims);
    }
}

EXPORT void vec_sqrDbf16Qbf16_bulk_2(
    const bf16_t* a,
    const bf16_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results
) {
    sqrDbf16Qbf16_bulk_sve<bf16_t, sequential_mapper>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqrDbf16Qbf16_bulk_sparse_2(
    const void* const* addresses,
    const bf16_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    sqrDbf16Qbf16_bulk_sve<const bf16_t*, sparse_mapper>(
        (const bf16_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT void vec_sqrDbf16Qbf16_bulk_offsets_2(
    const bf16_t* a,
    const bf16_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    sqrDbf16Qbf16_bulk_sve<bf16_t, offsets_mapper>(a, b, dims, pitch / sizeof(bf16_t), offsets, count, results);
}

