/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// This file contains implementations for vector processing functionalities,
// for the "2nd tier" vector capabilities; in the case of ARM, this second tier
// consist of functions for processors supporting the SVE/SVE2
// instruction set.

#include <stddef.h>
#include <arm_sve.h>
#include "vec.h"
#include "vec_common.h"
#include "aarch64/aarch64_vec_common.h"

static inline svuint64_t dot_bit_sv(const svbool_t pg, const svuint8_t a, const int8_t* b) {
    const svuint8_t q0 = svld1_u8(pg, (const uint8_t*)b);
    // reinterpret the u8 result as u64 so the count doesn't overflow
    return svcnt_u64_x(svptrue_b64(), svreinterpret_u64(svand_u8_x(svptrue_b8(), q0, a)));
}

// Counts the bits of a & q per byte and folds the counts into acc with UDOT against a constant weight
// vector: one instruction applies the query plane's bit value and sums four bytes into each 32-bit lane.
// Per lane a step adds at most 4 bytes * 8 bits * weight, far from a u32 overflow.
static inline svuint32_t dot_bit_weighted_sv(const svuint32_t acc, const svuint8_t a, const svuint8_t q, const svuint8_t weight) {
    return svdot_u32(acc, svcnt_u8_x(svptrue_b8(), svand_u8_x(svptrue_b8(), a, q)), weight);
}

static inline int64_t dotd1q4_inner(const int8_t* a, const int8_t* query, const int32_t length) {
    int r = 0;

    // Init accumulator(s) with 0
    svuint64_t acc0 = svdup_n_u64(0);
    svuint64_t acc1 = svdup_n_u64(0);
    svuint64_t acc2 = svdup_n_u64(0);
    svuint64_t acc3 = svdup_n_u64(0);

    for (svbool_t pg = svwhilelt_b8(r, length); svptest_any(svptrue_b8(), pg); pg = svwhilelt_b8(r, length)) {
        const svuint8_t value = svld1_u8(pg, (const uint8_t*)(a + r));

        acc0 = svadd_u64_x(svptrue_b64(), acc0, dot_bit_sv(pg, value, query + r));
        acc1 = svadd_u64_x(svptrue_b64(), acc1, dot_bit_sv(pg, value, query + r + length));
        acc2 = svadd_u64_x(svptrue_b64(), acc2, dot_bit_sv(pg, value, query + r + 2 * length));
        acc3 = svadd_u64_x(svptrue_b64(), acc3, dot_bit_sv(pg, value, query + r + 3 * length));

        r += svcntb();
    }

    int64_t subRet0 = svaddv_u64(svptrue_b64(), acc0);
    int64_t subRet1 = svaddv_u64(svptrue_b64(), acc1);
    int64_t subRet2 = svaddv_u64(svptrue_b64(), acc2);
    int64_t subRet3 = svaddv_u64(svptrue_b64(), acc3);

    return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
}

EXPORT int64_t vec_dotd1q4_2(const int8_t* a, const int8_t* query, const int32_t length) {
    return dotd1q4_inner(a, query, length);
}

static inline int64_t dotd1q1_inner(const int8_t* a, const int8_t* query, const int32_t length) {
    int r = 0;
    svuint64_t acc = svdup_n_u64(0);

    for (svbool_t pg = svwhilelt_b8(r, length); svptest_any(svptrue_b8(), pg); pg = svwhilelt_b8(r, length)) {
        const svuint8_t value = svld1_u8(pg, (const uint8_t*)(a + r));
        acc = svadd_u64_x(svptrue_b64(), acc, dot_bit_sv(pg, value, query + r));
        r += svcntb();
    }

    return svaddv_u64(svptrue_b64(), acc);
}

EXPORT int64_t vec_dotd1q1_2(const int8_t* a, const int8_t* query, const int32_t length) {
    return dotd1q1_inner(a, query, length);
}

template <
    typename TData,
    const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int32_t(*out_mapper)(const int32_t*, const int32_t) = compact_output>
static inline void dotd1q1_inner_bulk(
    const TData* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    int c = 0;

    for (; c + 3 < count; c += 4) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        const int8_t* a1 = mapper(a, c + 1, offsets, pitch);
        const int8_t* a2 = mapper(a, c + 2, offsets, pitch);
        const int8_t* a3 = mapper(a, c + 3, offsets, pitch);

        int r = 0;
        svuint64_t acc0 = svdup_n_u64(0);
        svuint64_t acc1 = svdup_n_u64(0);
        svuint64_t acc2 = svdup_n_u64(0);
        svuint64_t acc3 = svdup_n_u64(0);

        for (svbool_t pg = svwhilelt_b8(r, length); svptest_any(svptrue_b8(), pg); pg = svwhilelt_b8(r, length)) {
            const svuint8_t v0 = svld1_u8(pg, (const uint8_t*)(a0 + r));
            const svuint8_t v1 = svld1_u8(pg, (const uint8_t*)(a1 + r));
            const svuint8_t v2 = svld1_u8(pg, (const uint8_t*)(a2 + r));
            const svuint8_t v3 = svld1_u8(pg, (const uint8_t*)(a3 + r));

            acc0 = svadd_u64_x(svptrue_b64(), acc0, dot_bit_sv(pg, v0, query + r));
            acc1 = svadd_u64_x(svptrue_b64(), acc1, dot_bit_sv(pg, v1, query + r));
            acc2 = svadd_u64_x(svptrue_b64(), acc2, dot_bit_sv(pg, v2, query + r));
            acc3 = svadd_u64_x(svptrue_b64(), acc3, dot_bit_sv(pg, v3, query + r));

            r += svcntb();
        }

        results[out_mapper(offsets, c)] = (f32_t)svaddv_u64(svptrue_b64(), acc0);
        results[out_mapper(offsets, c + 1)] = (f32_t)svaddv_u64(svptrue_b64(), acc1);
        results[out_mapper(offsets, c + 2)] = (f32_t)svaddv_u64(svptrue_b64(), acc2);
        results[out_mapper(offsets, c + 3)] = (f32_t)svaddv_u64(svptrue_b64(), acc3);
    }

    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        results[out_mapper(offsets, c)] = (f32_t)dotd1q1_inner(a0, query, length);
    }
}

EXPORT void vec_dotd1q1_bulk_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1q1_inner_bulk<int8_t, sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd1q1_bulk_masked_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t mask,
    const int32_t count,
    f32_t* results) {
    dotd1q1_inner_bulk<int8_t, masked_mapper, masked_output>(
        a, query, length, pitch, &mask, __builtin_popcount((uint32_t)mask), results);
}

EXPORT void vec_dotd1q1_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1q1_inner_bulk<const int8_t*, sparse_mapper>((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

template <
    typename TData,
    const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int32_t(*out_mapper)(const int32_t*, const int32_t) = compact_output>
static inline void dotd1q4_inner_bulk(
    const TData* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    int c = 0;

    // The bit value of each query plane is applied by UDOT against these weights, so one accumulator per
    // vector collects all four planes and eight vectors fit in the 32 z registers together with the query
    // planes, the weights and the temporaries. Eight vectors per batch keep twice as many cache misses in
    // flight as four, which is what bounds this kernel once the vectors come from beyond L2.
    const svuint8_t w0 = svdup_n_u8(1);
    const svuint8_t w1 = svdup_n_u8(2);
    const svuint8_t w2 = svdup_n_u8(4);
    const svuint8_t w3 = svdup_n_u8(8);

    for (; c + 7 < count; c += 8) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        const int8_t* a1 = mapper(a, c + 1, offsets, pitch);
        const int8_t* a2 = mapper(a, c + 2, offsets, pitch);
        const int8_t* a3 = mapper(a, c + 3, offsets, pitch);
        const int8_t* a4 = mapper(a, c + 4, offsets, pitch);
        const int8_t* a5 = mapper(a, c + 5, offsets, pitch);
        const int8_t* a6 = mapper(a, c + 6, offsets, pitch);
        const int8_t* a7 = mapper(a, c + 7, offsets, pitch);

        int r = 0;
        svuint32_t acc0 = svdup_n_u32(0);
        svuint32_t acc1 = svdup_n_u32(0);
        svuint32_t acc2 = svdup_n_u32(0);
        svuint32_t acc3 = svdup_n_u32(0);
        svuint32_t acc4 = svdup_n_u32(0);
        svuint32_t acc5 = svdup_n_u32(0);
        svuint32_t acc6 = svdup_n_u32(0);
        svuint32_t acc7 = svdup_n_u32(0);

        for (svbool_t pg = svwhilelt_b8(r, length); svptest_any(svptrue_b8(), pg); pg = svwhilelt_b8(r, length)) {
            const svuint8_t q0 = svld1_u8(pg, (const uint8_t*)(query + r));
            const svuint8_t q1 = svld1_u8(pg, (const uint8_t*)(query + r + length));
            const svuint8_t q2 = svld1_u8(pg, (const uint8_t*)(query + r + 2 * length));
            const svuint8_t q3 = svld1_u8(pg, (const uint8_t*)(query + r + 3 * length));

            const svuint8_t v0 = svld1_u8(pg, (const uint8_t*)(a0 + r));
            const svuint8_t v1 = svld1_u8(pg, (const uint8_t*)(a1 + r));
            const svuint8_t v2 = svld1_u8(pg, (const uint8_t*)(a2 + r));
            const svuint8_t v3 = svld1_u8(pg, (const uint8_t*)(a3 + r));
            const svuint8_t v4 = svld1_u8(pg, (const uint8_t*)(a4 + r));
            const svuint8_t v5 = svld1_u8(pg, (const uint8_t*)(a5 + r));
            const svuint8_t v6 = svld1_u8(pg, (const uint8_t*)(a6 + r));
            const svuint8_t v7 = svld1_u8(pg, (const uint8_t*)(a7 + r));

            acc0 = dot_bit_weighted_sv(acc0, v0, q0, w0);
            acc0 = dot_bit_weighted_sv(acc0, v0, q1, w1);
            acc0 = dot_bit_weighted_sv(acc0, v0, q2, w2);
            acc0 = dot_bit_weighted_sv(acc0, v0, q3, w3);

            acc1 = dot_bit_weighted_sv(acc1, v1, q0, w0);
            acc1 = dot_bit_weighted_sv(acc1, v1, q1, w1);
            acc1 = dot_bit_weighted_sv(acc1, v1, q2, w2);
            acc1 = dot_bit_weighted_sv(acc1, v1, q3, w3);

            acc2 = dot_bit_weighted_sv(acc2, v2, q0, w0);
            acc2 = dot_bit_weighted_sv(acc2, v2, q1, w1);
            acc2 = dot_bit_weighted_sv(acc2, v2, q2, w2);
            acc2 = dot_bit_weighted_sv(acc2, v2, q3, w3);

            acc3 = dot_bit_weighted_sv(acc3, v3, q0, w0);
            acc3 = dot_bit_weighted_sv(acc3, v3, q1, w1);
            acc3 = dot_bit_weighted_sv(acc3, v3, q2, w2);
            acc3 = dot_bit_weighted_sv(acc3, v3, q3, w3);

            acc4 = dot_bit_weighted_sv(acc4, v4, q0, w0);
            acc4 = dot_bit_weighted_sv(acc4, v4, q1, w1);
            acc4 = dot_bit_weighted_sv(acc4, v4, q2, w2);
            acc4 = dot_bit_weighted_sv(acc4, v4, q3, w3);

            acc5 = dot_bit_weighted_sv(acc5, v5, q0, w0);
            acc5 = dot_bit_weighted_sv(acc5, v5, q1, w1);
            acc5 = dot_bit_weighted_sv(acc5, v5, q2, w2);
            acc5 = dot_bit_weighted_sv(acc5, v5, q3, w3);

            acc6 = dot_bit_weighted_sv(acc6, v6, q0, w0);
            acc6 = dot_bit_weighted_sv(acc6, v6, q1, w1);
            acc6 = dot_bit_weighted_sv(acc6, v6, q2, w2);
            acc6 = dot_bit_weighted_sv(acc6, v6, q3, w3);

            acc7 = dot_bit_weighted_sv(acc7, v7, q0, w0);
            acc7 = dot_bit_weighted_sv(acc7, v7, q1, w1);
            acc7 = dot_bit_weighted_sv(acc7, v7, q2, w2);
            acc7 = dot_bit_weighted_sv(acc7, v7, q3, w3);

            r += svcntb();
        }

        results[out_mapper(offsets, c)] = (f32_t)svaddv_u32(svptrue_b32(), acc0);
        results[out_mapper(offsets, c + 1)] = (f32_t)svaddv_u32(svptrue_b32(), acc1);
        results[out_mapper(offsets, c + 2)] = (f32_t)svaddv_u32(svptrue_b32(), acc2);
        results[out_mapper(offsets, c + 3)] = (f32_t)svaddv_u32(svptrue_b32(), acc3);
        results[out_mapper(offsets, c + 4)] = (f32_t)svaddv_u32(svptrue_b32(), acc4);
        results[out_mapper(offsets, c + 5)] = (f32_t)svaddv_u32(svptrue_b32(), acc5);
        results[out_mapper(offsets, c + 6)] = (f32_t)svaddv_u32(svptrue_b32(), acc6);
        results[out_mapper(offsets, c + 7)] = (f32_t)svaddv_u32(svptrue_b32(), acc7);
    }

    // handle the vectors tail
    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        results[out_mapper(offsets, c)] = (f32_t)dotd1q4_inner(a0, query, length);
    }
}

EXPORT void vec_dotd1q4_bulk_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1q4_inner_bulk<int8_t, sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd1q4_bulk_masked_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t mask,
    const int32_t count,
    f32_t* results) {
    dotd1q4_inner_bulk<int8_t, masked_mapper, masked_output>(
        a, query, length, pitch, &mask, __builtin_popcount((uint32_t)mask), results);
}

EXPORT void vec_dotd1q4_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1q4_inner_bulk<const int8_t*, sparse_mapper>((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

static inline int64_t dotd1q2_inner(const int8_t* a, const int8_t* query, const int32_t length) {
    int r = 0;

    svuint64_t acc0 = svdup_n_u64(0);
    svuint64_t acc1 = svdup_n_u64(0);

    for (svbool_t pg = svwhilelt_b8(r, length); svptest_any(svptrue_b8(), pg); pg = svwhilelt_b8(r, length)) {
        const svuint8_t value = svld1_u8(pg, (const uint8_t*)(a + r));

        acc0 = svadd_u64_x(svptrue_b64(), acc0, dot_bit_sv(pg, value, query + r));
        acc1 = svadd_u64_x(svptrue_b64(), acc1, dot_bit_sv(pg, value, query + r + length));

        r += svcntb();
    }

    int64_t subRet0 = svaddv_u64(svptrue_b64(), acc0);
    int64_t subRet1 = svaddv_u64(svptrue_b64(), acc1);

    return subRet0 + (subRet1 << 1);
}

EXPORT int64_t vec_dotd2q2_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length
) {
    int64_t lower = dotd1q2_inner(a, query, length/2);
    int64_t upper = dotd1q2_inner(a + length/2, query, length/2);
    return lower + (upper << 1);
}

template <
    typename TData,
    const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int32_t(*out_mapper)(const int32_t*, const int32_t) = compact_output>
static inline void dotd2q2_inner_bulk(
    const TData* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    int c = 0;
    const int bit_length = length/2;
    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        int64_t lower = dotd1q2_inner(a0, query, bit_length);
        int64_t upper = dotd1q2_inner(a0 + bit_length, query, bit_length);
        results[out_mapper(offsets, c)] = (f32_t)(lower + (upper << 1));
    }
}

EXPORT void vec_dotd2q2_bulk_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q2_inner_bulk<int8_t, sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd2q2_bulk_masked_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t mask,
    const int32_t count,
    f32_t* results) {
    dotd2q2_inner_bulk<int8_t, masked_mapper, masked_output>(
        a, query, length, pitch, &mask, __builtin_popcount((uint32_t)mask), results);
}

EXPORT void vec_dotd2q2_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q2_inner_bulk<const int8_t*, sparse_mapper>((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

// 2-bit doc x 4-bit query, both doc bit planes in one pass. The doc is two contiguous planes of
// length/2 bytes (low bit, then high bit), the query four planes of length/2 bytes. UDOT applies
// 2^(query bit + doc bit) as a weight; the eight plane pairs are spread over four accumulators to keep
// the UDOT chains short, and summed before a single reduction. The accumulators here and in the pair and
// batch kernels below are spelled out rather than kept in arrays driven by apply_indexed, because SVE
// types are sizeless and cannot be array elements.
static inline int64_t dotd2q4_inner(const int8_t* a, const int8_t* query, const int32_t length) {
    const int32_t bit_length = length / 2;
    const int8_t* a_hi = a + bit_length;
    const svuint8_t w1 = svdup_n_u8(1);
    const svuint8_t w2 = svdup_n_u8(2);
    const svuint8_t w4 = svdup_n_u8(4);
    const svuint8_t w8 = svdup_n_u8(8);
    const svuint8_t w16 = svdup_n_u8(16);

    int r = 0;
    svuint32_t acc0 = svdup_n_u32(0);
    svuint32_t acc1 = svdup_n_u32(0);
    svuint32_t acc2 = svdup_n_u32(0);
    svuint32_t acc3 = svdup_n_u32(0);
    // full-width steps without a predicate, then at most one predicated tail step
    const auto step = [&](const svbool_t pg) {
        const svuint8_t q0 = svld1_u8(pg, (const uint8_t*)(query + r));
        const svuint8_t q1 = svld1_u8(pg, (const uint8_t*)(query + r + bit_length));
        const svuint8_t q2 = svld1_u8(pg, (const uint8_t*)(query + r + 2 * bit_length));
        const svuint8_t q3 = svld1_u8(pg, (const uint8_t*)(query + r + 3 * bit_length));
        const svuint8_t lo = svld1_u8(pg, (const uint8_t*)(a + r));
        const svuint8_t hi = svld1_u8(pg, (const uint8_t*)(a_hi + r));

        acc0 = dot_bit_weighted_sv(acc0, lo, q0, w1);
        acc1 = dot_bit_weighted_sv(acc1, lo, q2, w4);
        acc2 = dot_bit_weighted_sv(acc2, hi, q0, w2);
        acc3 = dot_bit_weighted_sv(acc3, hi, q2, w8);
        acc0 = dot_bit_weighted_sv(acc0, lo, q1, w2);
        acc1 = dot_bit_weighted_sv(acc1, lo, q3, w8);
        acc2 = dot_bit_weighted_sv(acc2, hi, q1, w4);
        acc3 = dot_bit_weighted_sv(acc3, hi, q3, w16);
    };
    const int32_t full = bit_length - bit_length % (int32_t)svcntb();
    for (; r < full; r += svcntb()) {
        step(svptrue_b8());
    }
    if (r < bit_length) {
        step(svwhilelt_b8(r, bit_length));
    }
    const svuint32_t acc = svadd_u32_x(svptrue_b32(), svadd_u32_x(svptrue_b32(), acc0, acc1), svadd_u32_x(svptrue_b32(), acc2, acc3));
    return svaddv_u32(svptrue_b32(), acc);
}

// Two vectors in lockstep, for the remainder of a bulk call: few enough registers to avoid saving the
// callee-saved ones, while the two vectors' UDOT chains run side by side.
static inline void dotd2q4_inner_pair(
    const int8_t* a0,
    const int8_t* a1,
    const int8_t* query,
    const int32_t length,
    int64_t* r0,
    int64_t* r1
) {
    const int32_t bit_length = length / 2;
    const svuint8_t w1 = svdup_n_u8(1);
    const svuint8_t w2 = svdup_n_u8(2);
    const svuint8_t w4 = svdup_n_u8(4);
    const svuint8_t w8 = svdup_n_u8(8);
    const svuint8_t w16 = svdup_n_u8(16);

    int r = 0;
    svuint32_t lo_acc0 = svdup_n_u32(0);
    svuint32_t hi_acc0 = svdup_n_u32(0);
    svuint32_t lo_acc1 = svdup_n_u32(0);
    svuint32_t hi_acc1 = svdup_n_u32(0);
    // full-width steps without a predicate, then at most one predicated tail step
    const auto step = [&](const svbool_t pg) {
        const svuint8_t q0 = svld1_u8(pg, (const uint8_t*)(query + r));
        const svuint8_t q1 = svld1_u8(pg, (const uint8_t*)(query + r + bit_length));
        const svuint8_t q2 = svld1_u8(pg, (const uint8_t*)(query + r + 2 * bit_length));
        const svuint8_t q3 = svld1_u8(pg, (const uint8_t*)(query + r + 3 * bit_length));
        const svuint8_t lo0 = svld1_u8(pg, (const uint8_t*)(a0 + r));
        const svuint8_t hi0 = svld1_u8(pg, (const uint8_t*)(a0 + bit_length + r));
        const svuint8_t lo1 = svld1_u8(pg, (const uint8_t*)(a1 + r));
        const svuint8_t hi1 = svld1_u8(pg, (const uint8_t*)(a1 + bit_length + r));

        lo_acc0 = dot_bit_weighted_sv(lo_acc0, lo0, q0, w1);
        hi_acc0 = dot_bit_weighted_sv(hi_acc0, hi0, q0, w2);
        lo_acc1 = dot_bit_weighted_sv(lo_acc1, lo1, q0, w1);
        hi_acc1 = dot_bit_weighted_sv(hi_acc1, hi1, q0, w2);
        lo_acc0 = dot_bit_weighted_sv(lo_acc0, lo0, q1, w2);
        hi_acc0 = dot_bit_weighted_sv(hi_acc0, hi0, q1, w4);
        lo_acc1 = dot_bit_weighted_sv(lo_acc1, lo1, q1, w2);
        hi_acc1 = dot_bit_weighted_sv(hi_acc1, hi1, q1, w4);
        lo_acc0 = dot_bit_weighted_sv(lo_acc0, lo0, q2, w4);
        hi_acc0 = dot_bit_weighted_sv(hi_acc0, hi0, q2, w8);
        lo_acc1 = dot_bit_weighted_sv(lo_acc1, lo1, q2, w4);
        hi_acc1 = dot_bit_weighted_sv(hi_acc1, hi1, q2, w8);
        lo_acc0 = dot_bit_weighted_sv(lo_acc0, lo0, q3, w8);
        hi_acc0 = dot_bit_weighted_sv(hi_acc0, hi0, q3, w16);
        lo_acc1 = dot_bit_weighted_sv(lo_acc1, lo1, q3, w8);
        hi_acc1 = dot_bit_weighted_sv(hi_acc1, hi1, q3, w16);
    };
    const int32_t full = bit_length - bit_length % (int32_t)svcntb();
    for (; r < full; r += svcntb()) {
        step(svptrue_b8());
    }
    if (r < bit_length) {
        step(svwhilelt_b8(r, bit_length));
    }
    *r0 = svaddv_u32(svptrue_b32(), svadd_u32_x(svptrue_b32(), lo_acc0, hi_acc0));
    *r1 = svaddv_u32(svptrue_b32(), svadd_u32_x(svptrue_b32(), lo_acc1, hi_acc1));
}

EXPORT int64_t vec_dotd2q4_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length
) {
    return dotd2q4_inner(a, query, length);
}

template <
    typename TData,
    const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int32_t(*out_mapper)(const int32_t*, const int32_t) = compact_output>
__attribute__((noinline)) static void dotd2q4_bulk4(
    const TData* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count4,
    f32_t* results
) {
    int c = 0;
    const int32_t bit_length = length / 2;

    // Four vectors per batch, each with one accumulator per doc bit plane so the UDOT chains stay short:
    // the eight bit planes, the four query planes, the five weights and the eight accumulators fit in the
    // 32 z registers. Using that many registers makes the function save the callee-saved ones, so it is
    // kept out of line and only called with at least one full batch.
    const svuint8_t w1 = svdup_n_u8(1);
    const svuint8_t w2 = svdup_n_u8(2);
    const svuint8_t w4 = svdup_n_u8(4);
    const svuint8_t w8 = svdup_n_u8(8);
    const svuint8_t w16 = svdup_n_u8(16);

    for (; c < count4; c += 4) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        const int8_t* a1 = mapper(a, c + 1, offsets, pitch);
        const int8_t* a2 = mapper(a, c + 2, offsets, pitch);
        const int8_t* a3 = mapper(a, c + 3, offsets, pitch);

        int r = 0;
        svuint32_t lo_acc0 = svdup_n_u32(0);
        svuint32_t hi_acc0 = svdup_n_u32(0);
        svuint32_t lo_acc1 = svdup_n_u32(0);
        svuint32_t hi_acc1 = svdup_n_u32(0);
        svuint32_t lo_acc2 = svdup_n_u32(0);
        svuint32_t hi_acc2 = svdup_n_u32(0);
        svuint32_t lo_acc3 = svdup_n_u32(0);
        svuint32_t hi_acc3 = svdup_n_u32(0);

        // full-width steps without a predicate, then at most one predicated tail step
        const auto step = [&](const svbool_t pg) {
            const svuint8_t q0 = svld1_u8(pg, (const uint8_t*)(query + r));
            const svuint8_t q1 = svld1_u8(pg, (const uint8_t*)(query + r + bit_length));
            const svuint8_t q2 = svld1_u8(pg, (const uint8_t*)(query + r + 2 * bit_length));
            const svuint8_t q3 = svld1_u8(pg, (const uint8_t*)(query + r + 3 * bit_length));

            const svuint8_t lo0 = svld1_u8(pg, (const uint8_t*)(a0 + r));
            const svuint8_t hi0 = svld1_u8(pg, (const uint8_t*)(a0 + bit_length + r));
            const svuint8_t lo1 = svld1_u8(pg, (const uint8_t*)(a1 + r));
            const svuint8_t hi1 = svld1_u8(pg, (const uint8_t*)(a1 + bit_length + r));
            const svuint8_t lo2 = svld1_u8(pg, (const uint8_t*)(a2 + r));
            const svuint8_t hi2 = svld1_u8(pg, (const uint8_t*)(a2 + bit_length + r));
            const svuint8_t lo3 = svld1_u8(pg, (const uint8_t*)(a3 + r));
            const svuint8_t hi3 = svld1_u8(pg, (const uint8_t*)(a3 + bit_length + r));

            lo_acc0 = dot_bit_weighted_sv(lo_acc0, lo0, q0, w1);
            lo_acc0 = dot_bit_weighted_sv(lo_acc0, lo0, q1, w2);
            lo_acc0 = dot_bit_weighted_sv(lo_acc0, lo0, q2, w4);
            lo_acc0 = dot_bit_weighted_sv(lo_acc0, lo0, q3, w8);
            hi_acc0 = dot_bit_weighted_sv(hi_acc0, hi0, q0, w2);
            hi_acc0 = dot_bit_weighted_sv(hi_acc0, hi0, q1, w4);
            hi_acc0 = dot_bit_weighted_sv(hi_acc0, hi0, q2, w8);
            hi_acc0 = dot_bit_weighted_sv(hi_acc0, hi0, q3, w16);

            lo_acc1 = dot_bit_weighted_sv(lo_acc1, lo1, q0, w1);
            lo_acc1 = dot_bit_weighted_sv(lo_acc1, lo1, q1, w2);
            lo_acc1 = dot_bit_weighted_sv(lo_acc1, lo1, q2, w4);
            lo_acc1 = dot_bit_weighted_sv(lo_acc1, lo1, q3, w8);
            hi_acc1 = dot_bit_weighted_sv(hi_acc1, hi1, q0, w2);
            hi_acc1 = dot_bit_weighted_sv(hi_acc1, hi1, q1, w4);
            hi_acc1 = dot_bit_weighted_sv(hi_acc1, hi1, q2, w8);
            hi_acc1 = dot_bit_weighted_sv(hi_acc1, hi1, q3, w16);

            lo_acc2 = dot_bit_weighted_sv(lo_acc2, lo2, q0, w1);
            lo_acc2 = dot_bit_weighted_sv(lo_acc2, lo2, q1, w2);
            lo_acc2 = dot_bit_weighted_sv(lo_acc2, lo2, q2, w4);
            lo_acc2 = dot_bit_weighted_sv(lo_acc2, lo2, q3, w8);
            hi_acc2 = dot_bit_weighted_sv(hi_acc2, hi2, q0, w2);
            hi_acc2 = dot_bit_weighted_sv(hi_acc2, hi2, q1, w4);
            hi_acc2 = dot_bit_weighted_sv(hi_acc2, hi2, q2, w8);
            hi_acc2 = dot_bit_weighted_sv(hi_acc2, hi2, q3, w16);

            lo_acc3 = dot_bit_weighted_sv(lo_acc3, lo3, q0, w1);
            lo_acc3 = dot_bit_weighted_sv(lo_acc3, lo3, q1, w2);
            lo_acc3 = dot_bit_weighted_sv(lo_acc3, lo3, q2, w4);
            lo_acc3 = dot_bit_weighted_sv(lo_acc3, lo3, q3, w8);
            hi_acc3 = dot_bit_weighted_sv(hi_acc3, hi3, q0, w2);
            hi_acc3 = dot_bit_weighted_sv(hi_acc3, hi3, q1, w4);
            hi_acc3 = dot_bit_weighted_sv(hi_acc3, hi3, q2, w8);
            hi_acc3 = dot_bit_weighted_sv(hi_acc3, hi3, q3, w16);
        };
        const int32_t full = bit_length - bit_length % (int32_t)svcntb();
        for (; r < full; r += svcntb()) {
            step(svptrue_b8());
        }
        if (r < bit_length) {
            step(svwhilelt_b8(r, bit_length));
        }

        results[out_mapper(offsets, c)] = (f32_t)svaddv_u32(svptrue_b32(), svadd_u32_x(svptrue_b32(), lo_acc0, hi_acc0));
        results[out_mapper(offsets, c + 1)] = (f32_t)svaddv_u32(svptrue_b32(), svadd_u32_x(svptrue_b32(), lo_acc1, hi_acc1));
        results[out_mapper(offsets, c + 2)] = (f32_t)svaddv_u32(svptrue_b32(), svadd_u32_x(svptrue_b32(), lo_acc2, hi_acc2));
        results[out_mapper(offsets, c + 3)] = (f32_t)svaddv_u32(svptrue_b32(), svadd_u32_x(svptrue_b32(), lo_acc3, hi_acc3));
    }
}

template <
    typename TData,
    const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int32_t(*out_mapper)(const int32_t*, const int32_t) = compact_output>
static inline void dotd2q4_inner_bulk(
    const TData* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    const int32_t count4 = count & ~3;
    if (count4 > 0) {
        dotd2q4_bulk4<TData, mapper, out_mapper>(a, query, length, pitch, offsets, count4, results);
    }
    int c = count4;
    if (c + 1 < count) {
        int64_t r0, r1;
        dotd2q4_inner_pair(mapper(a, c, offsets, pitch), mapper(a, c + 1, offsets, pitch), query, length, &r0, &r1);
        results[out_mapper(offsets, c)] = (f32_t)r0;
        results[out_mapper(offsets, c + 1)] = (f32_t)r1;
        c += 2;
    }
    if (c < count) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        results[out_mapper(offsets, c)] = (f32_t)dotd2q4_inner(a0, query, length);
    }
}

EXPORT void vec_dotd2q4_bulk_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<int8_t, sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd2q4_bulk_masked_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t mask,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<int8_t, masked_mapper, masked_output>(
        a, query, length, pitch, &mask, __builtin_popcount((uint32_t)mask), results);
}

EXPORT void vec_dotd2q4_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<const int8_t*, sparse_mapper>((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT int64_t vec_dotd4q4_2(const int8_t* a, const int8_t* query, const int32_t length) {
    const int32_t bit_length = length / 4;
    int64_t p0 = dotd1q4_inner(a + 0 * bit_length, query, bit_length);
    int64_t p1 = dotd1q4_inner(a + 1 * bit_length, query, bit_length);
    int64_t p2 = dotd1q4_inner(a + 2 * bit_length, query, bit_length);
    int64_t p3 = dotd1q4_inner(a + 3 * bit_length, query, bit_length);
    return p0 + (p1 << 1) + (p2 << 2) + (p3 << 3);
}

template <
    typename TData,
    const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int32_t(*out_mapper)(const int32_t*, const int32_t) = compact_output>
static inline void dotd4q4_inner_bulk(
    const TData* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    const int32_t bit_length = length / 4;

    for (int c = 0; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);

        int64_t p0 = dotd1q4_inner(a0 + 0 * bit_length, query, bit_length);
        int64_t p1 = dotd1q4_inner(a0 + 1 * bit_length, query, bit_length);
        int64_t p2 = dotd1q4_inner(a0 + 2 * bit_length, query, bit_length);
        int64_t p3 = dotd1q4_inner(a0 + 3 * bit_length, query, bit_length);

        results[out_mapper(offsets, c)] = (f32_t)(p0 + (p1 << 1) + (p2 << 2) + (p3 << 3));
    }
}

EXPORT void vec_dotd4q4_bulk_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<int8_t, sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd4q4_bulk_masked_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t mask,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<int8_t, masked_mapper, masked_output>(
        a, query, length, pitch, &mask, __builtin_popcount((uint32_t)mask), results);
}

EXPORT void vec_dotd4q4_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<const int8_t*, sparse_mapper>((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}
