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
    const int32_t block_mask = clamp_mask(mask, count);
    dotd1q1_inner_bulk<int8_t, masked_mapper, masked_output>(
        a, query, length, pitch, &block_mask, __builtin_popcount((uint32_t)block_mask), results);
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
    const int32_t block_mask = clamp_mask(mask, count);
    dotd1q4_inner_bulk<int8_t, masked_mapper, masked_output>(
        a, query, length, pitch, &block_mask, __builtin_popcount((uint32_t)block_mask), results);
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
    const int32_t block_mask = clamp_mask(mask, count);
    dotd2q2_inner_bulk<int8_t, masked_mapper, masked_output>(
        a, query, length, pitch, &block_mask, __builtin_popcount((uint32_t)block_mask), results);
}

EXPORT void vec_dotd2q2_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q2_inner_bulk<const int8_t*, sparse_mapper>((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

// One step of one vector of the d2q4 batch kernel: the low doc plane against the four query planes into
// lo, the high plane into hi. The pair kernel interleaves its two vectors' dot products instead.
static inline void dot_d2q4_step_sv(
    svuint32_t& lo,
    svuint32_t& hi,
    const svuint8_t lo_plane,
    const svuint8_t hi_plane,
    const svuint8_t q0,
    const svuint8_t q1,
    const svuint8_t q2,
    const svuint8_t q3,
    const svuint8_t w0,
    const svuint8_t w1,
    const svuint8_t w2,
    const svuint8_t w3,
    const svuint8_t w4
) {
    lo = dot_bit_weighted_sv(lo, lo_plane, q0, w0);
    lo = dot_bit_weighted_sv(lo, lo_plane, q1, w1);
    lo = dot_bit_weighted_sv(lo, lo_plane, q2, w2);
    lo = dot_bit_weighted_sv(lo, lo_plane, q3, w3);
    hi = dot_bit_weighted_sv(hi, hi_plane, q0, w1);
    hi = dot_bit_weighted_sv(hi, hi_plane, q1, w2);
    hi = dot_bit_weighted_sv(hi, hi_plane, q2, w3);
    hi = dot_bit_weighted_sv(hi, hi_plane, q3, w4);
}

// 2-bit doc x 4-bit query, both doc bit planes in one pass. The doc is two contiguous planes of
// length/2 bytes (low bit, then high bit), the query four planes of length/2 bytes. UDOT applies
// 2^(query bit + doc bit) as a weight; the eight plane pairs are spread over four accumulators to keep
// the UDOT chains short, and summed before a single reduction. Accumulators are separate variables, as
// SVE types cannot be array elements.
// A longer SVE vector length does not mean more throughput: Neoverse V1 runs its 256-bit SVE vectors on
// the same 128-bit pipes as NEON. It halves the number of steps per vector, though, which shortens the
// latency-bound UDOT chains of the single and pair kernels; the batch kernel is bound by throughput.
static inline int64_t dotd2q4_inner(const int8_t* a, const int8_t* query, const int32_t length) {
    const int32_t bit_length = length / 2;
    const int8_t* a_hi = a + bit_length;
    const svuint8_t w0 = svdup_n_u8(1);
    const svuint8_t w1 = svdup_n_u8(2);
    const svuint8_t w2 = svdup_n_u8(4);
    const svuint8_t w3 = svdup_n_u8(8);
    const svuint8_t w4 = svdup_n_u8(16);

    int r = 0;
    svuint32_t acc0 = svdup_n_u32(0);
    svuint32_t acc1 = svdup_n_u32(0);
    svuint32_t acc2 = svdup_n_u32(0);
    svuint32_t acc3 = svdup_n_u32(0);
    for (svbool_t pg = svwhilelt_b8(r, bit_length); svptest_any(svptrue_b8(), pg); pg = svwhilelt_b8(r, bit_length)) {
        const svuint8_t q0 = svld1_u8(pg, (const uint8_t*)(query + r));
        const svuint8_t q1 = svld1_u8(pg, (const uint8_t*)(query + r + bit_length));
        const svuint8_t q2 = svld1_u8(pg, (const uint8_t*)(query + r + 2 * bit_length));
        const svuint8_t q3 = svld1_u8(pg, (const uint8_t*)(query + r + 3 * bit_length));
        const svuint8_t lo = svld1_u8(pg, (const uint8_t*)(a + r));
        const svuint8_t hi = svld1_u8(pg, (const uint8_t*)(a_hi + r));

        acc0 = dot_bit_weighted_sv(acc0, lo, q0, w0);
        acc1 = dot_bit_weighted_sv(acc1, lo, q2, w2);
        acc2 = dot_bit_weighted_sv(acc2, hi, q0, w1);
        acc3 = dot_bit_weighted_sv(acc3, hi, q2, w3);
        acc0 = dot_bit_weighted_sv(acc0, lo, q1, w1);
        acc1 = dot_bit_weighted_sv(acc1, lo, q3, w3);
        acc2 = dot_bit_weighted_sv(acc2, hi, q1, w2);
        acc3 = dot_bit_weighted_sv(acc3, hi, q3, w4);
        r += svcntb();
    }
    const svuint32_t acc = svadd_u32_x(svptrue_b32(), svadd_u32_x(svptrue_b32(), acc0, acc1), svadd_u32_x(svptrue_b32(), acc2, acc3));
    return svaddv_u32(svptrue_b32(), acc);
}

// Two vectors in lockstep, for the remainder of a bulk call: few enough registers to avoid saving the
// callee-saved ones, with one accumulator per doc bit plane per vector, so the two vectors' UDOT chains
// run side by side.
static inline void dotd2q4_inner_pair(
    const int8_t* a0,
    const int8_t* a1,
    const int8_t* query,
    const int32_t length,
    int64_t* r0,
    int64_t* r1
) {
    const int32_t bit_length = length / 2;
    const svuint8_t w0 = svdup_n_u8(1);
    const svuint8_t w1 = svdup_n_u8(2);
    const svuint8_t w2 = svdup_n_u8(4);
    const svuint8_t w3 = svdup_n_u8(8);
    const svuint8_t w4 = svdup_n_u8(16);

    int r = 0;
    svuint32_t lo_acc0 = svdup_n_u32(0);
    svuint32_t hi_acc0 = svdup_n_u32(0);
    svuint32_t lo_acc1 = svdup_n_u32(0);
    svuint32_t hi_acc1 = svdup_n_u32(0);
    for (svbool_t pg = svwhilelt_b8(r, bit_length); svptest_any(svptrue_b8(), pg); pg = svwhilelt_b8(r, bit_length)) {
        const svuint8_t q0 = svld1_u8(pg, (const uint8_t*)(query + r));
        const svuint8_t q1 = svld1_u8(pg, (const uint8_t*)(query + r + bit_length));
        const svuint8_t q2 = svld1_u8(pg, (const uint8_t*)(query + r + 2 * bit_length));
        const svuint8_t q3 = svld1_u8(pg, (const uint8_t*)(query + r + 3 * bit_length));
        const svuint8_t lo0 = svld1_u8(pg, (const uint8_t*)(a0 + r));
        const svuint8_t hi0 = svld1_u8(pg, (const uint8_t*)(a0 + bit_length + r));
        const svuint8_t lo1 = svld1_u8(pg, (const uint8_t*)(a1 + r));
        const svuint8_t hi1 = svld1_u8(pg, (const uint8_t*)(a1 + bit_length + r));

        lo_acc0 = dot_bit_weighted_sv(lo_acc0, lo0, q0, w0);
        hi_acc0 = dot_bit_weighted_sv(hi_acc0, hi0, q0, w1);
        lo_acc1 = dot_bit_weighted_sv(lo_acc1, lo1, q0, w0);
        hi_acc1 = dot_bit_weighted_sv(hi_acc1, hi1, q0, w1);
        lo_acc0 = dot_bit_weighted_sv(lo_acc0, lo0, q1, w1);
        hi_acc0 = dot_bit_weighted_sv(hi_acc0, hi0, q1, w2);
        lo_acc1 = dot_bit_weighted_sv(lo_acc1, lo1, q1, w1);
        hi_acc1 = dot_bit_weighted_sv(hi_acc1, hi1, q1, w2);
        lo_acc0 = dot_bit_weighted_sv(lo_acc0, lo0, q2, w2);
        hi_acc0 = dot_bit_weighted_sv(hi_acc0, hi0, q2, w3);
        lo_acc1 = dot_bit_weighted_sv(lo_acc1, lo1, q2, w2);
        hi_acc1 = dot_bit_weighted_sv(hi_acc1, hi1, q2, w3);
        lo_acc0 = dot_bit_weighted_sv(lo_acc0, lo0, q3, w3);
        hi_acc0 = dot_bit_weighted_sv(hi_acc0, hi0, q3, w4);
        lo_acc1 = dot_bit_weighted_sv(lo_acc1, lo1, q3, w3);
        hi_acc1 = dot_bit_weighted_sv(hi_acc1, hi1, q3, w4);
        r += svcntb();
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
static void dotd2q4_inner_bulk4(
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
    // 32 z registers. The ARM calling convention requires d8-d15 to be saved, which overlaps with SIMD
    // registers z8-z15. So, because this function uses those, it needs to save the existing contents first.
    // If this function was inlined, the top-level function would need to do this all the time. By not
    // forcing inlining, this function needs to do it, but it's only called some of the time; whether
    // to actually inline it is left to the compiler. Each step's dot products are grouped
    // per accumulator in the source; the compiler interleaves the eight independent chains itself. Four
    // vectors of two bit planes keep as many streams in flight as the eight single-plane vectors of the
    // d1q4 batch; eight d2q4 vectors would need more than 32 registers.
    const svuint8_t w0 = svdup_n_u8(1);
    const svuint8_t w1 = svdup_n_u8(2);
    const svuint8_t w2 = svdup_n_u8(4);
    const svuint8_t w3 = svdup_n_u8(8);
    const svuint8_t w4 = svdup_n_u8(16);

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

        for (svbool_t pg = svwhilelt_b8(r, bit_length); svptest_any(svptrue_b8(), pg); pg = svwhilelt_b8(r, bit_length)) {
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

            dot_d2q4_step_sv(lo_acc0, hi_acc0, lo0, hi0, q0, q1, q2, q3, w0, w1, w2, w3, w4);
            dot_d2q4_step_sv(lo_acc1, hi_acc1, lo1, hi1, q0, q1, q2, q3, w0, w1, w2, w3, w4);
            dot_d2q4_step_sv(lo_acc2, hi_acc2, lo2, hi2, q0, q1, q2, q3, w0, w1, w2, w3, w4);
            dot_d2q4_step_sv(lo_acc3, hi_acc3, lo3, hi3, q0, q1, q2, q3, w0, w1, w2, w3, w4);
            r += svcntb();
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
        dotd2q4_inner_bulk4<TData, mapper, out_mapper>(a, query, length, pitch, offsets, count4, results);
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
    const int32_t block_mask = clamp_mask(mask, count);
    dotd2q4_inner_bulk<int8_t, masked_mapper, masked_output>(
        a, query, length, pitch, &block_mask, __builtin_popcount((uint32_t)block_mask), results);
}

EXPORT void vec_dotd2q4_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<const int8_t*, sparse_mapper>((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

// One doc plane of a d4q4 step: the plane against the four query planes, each weighted by its bit value,
// into the plane's accumulator.
static inline svuint32_t dot_d4q4_plane_sv(
    const svuint32_t acc,
    const svuint8_t doc_plane,
    const svuint8_t q0,
    const svuint8_t q1,
    const svuint8_t q2,
    const svuint8_t q3,
    const svuint8_t w0,
    const svuint8_t w1,
    const svuint8_t w2,
    const svuint8_t w3
) {
    svuint32_t r = dot_bit_weighted_sv(acc, doc_plane, q0, w0);
    r = dot_bit_weighted_sv(r, doc_plane, q1, w1);
    r = dot_bit_weighted_sv(r, doc_plane, q2, w2);
    return dot_bit_weighted_sv(r, doc_plane, q3, w3);
}

// One step of one vector of the d4q4 kernels: each of the four doc planes against the four query planes,
// into the doc plane's accumulator.
static inline void dot_d4q4_step_sv(
    svuint32_t& acc0,
    svuint32_t& acc1,
    svuint32_t& acc2,
    svuint32_t& acc3,
    const svbool_t pg,
    const int8_t* a,
    const int32_t bit_length,
    const svuint8_t q0,
    const svuint8_t q1,
    const svuint8_t q2,
    const svuint8_t q3,
    const svuint8_t w0,
    const svuint8_t w1,
    const svuint8_t w2,
    const svuint8_t w3
) {
    acc0 = dot_d4q4_plane_sv(acc0, svld1_u8(pg, (const uint8_t*)a), q0, q1, q2, q3, w0, w1, w2, w3);
    acc1 = dot_d4q4_plane_sv(acc1, svld1_u8(pg, (const uint8_t*)(a + bit_length)), q0, q1, q2, q3, w0, w1, w2, w3);
    acc2 = dot_d4q4_plane_sv(acc2, svld1_u8(pg, (const uint8_t*)(a + 2 * bit_length)), q0, q1, q2, q3, w0, w1, w2, w3);
    acc3 = dot_d4q4_plane_sv(acc3, svld1_u8(pg, (const uint8_t*)(a + 3 * bit_length)), q0, q1, q2, q3, w0, w1, w2, w3);
}

// Sum of a vector's four doc plane accumulators, each weighted by its doc bit value.
static inline int64_t sum_d4q4_sv(const svuint32_t acc0, const svuint32_t acc1, const svuint32_t acc2, const svuint32_t acc3) {
    const svbool_t all = svptrue_b32();
    return (int64_t)svaddv_u32(all, acc0) + ((int64_t)svaddv_u32(all, acc1) << 1) + ((int64_t)svaddv_u32(all, acc2) << 2)
        + ((int64_t)svaddv_u32(all, acc3) << 3);
}

// 4-bit doc x 4-bit query in one pass. The doc is four contiguous bit planes of length/4 bytes, the
// query four planes of length/4 bytes. Each doc plane has its own accumulator, into which UDOT applies the
// query plane's bit value as a weight; the doc plane's bit value is applied after the reduction.
// Accumulators are separate variables, as SVE types cannot be array elements.
static inline int64_t dotd4q4_inner(const int8_t* a, const int8_t* query, const int32_t length) {
    const int32_t bit_length = length / 4;
    const svuint8_t w0 = svdup_n_u8(1);
    const svuint8_t w1 = svdup_n_u8(2);
    const svuint8_t w2 = svdup_n_u8(4);
    const svuint8_t w3 = svdup_n_u8(8);

    int r = 0;
    svuint32_t acc0 = svdup_n_u32(0);
    svuint32_t acc1 = svdup_n_u32(0);
    svuint32_t acc2 = svdup_n_u32(0);
    svuint32_t acc3 = svdup_n_u32(0);
    for (svbool_t pg = svwhilelt_b8(r, bit_length); svptest_any(svptrue_b8(), pg); pg = svwhilelt_b8(r, bit_length)) {
        const svuint8_t q0 = svld1_u8(pg, (const uint8_t*)(query + r));
        const svuint8_t q1 = svld1_u8(pg, (const uint8_t*)(query + r + bit_length));
        const svuint8_t q2 = svld1_u8(pg, (const uint8_t*)(query + r + 2 * bit_length));
        const svuint8_t q3 = svld1_u8(pg, (const uint8_t*)(query + r + 3 * bit_length));
        dot_d4q4_step_sv(acc0, acc1, acc2, acc3, pg, a + r, bit_length, q0, q1, q2, q3, w0, w1, w2, w3);
        r += svcntb();
    }
    return sum_d4q4_sv(acc0, acc1, acc2, acc3);
}

EXPORT int64_t vec_dotd4q4_2(const int8_t* a, const int8_t* query, const int32_t length) {
    return dotd4q4_inner(a, query, length);
}

// Bulk scoring two vectors at a time in lockstep, one accumulator per doc bit plane per vector, so the two
// vectors' UDOT chains run side by side; an odd last vector uses the single kernel.
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
    const svuint8_t w0 = svdup_n_u8(1);
    const svuint8_t w1 = svdup_n_u8(2);
    const svuint8_t w2 = svdup_n_u8(4);
    const svuint8_t w3 = svdup_n_u8(8);

    int c = 0;
    for (; c + 1 < count; c += 2) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        const int8_t* a1 = mapper(a, c + 1, offsets, pitch);
        int r = 0;
        svuint32_t acc00 = svdup_n_u32(0);
        svuint32_t acc01 = svdup_n_u32(0);
        svuint32_t acc02 = svdup_n_u32(0);
        svuint32_t acc03 = svdup_n_u32(0);
        svuint32_t acc10 = svdup_n_u32(0);
        svuint32_t acc11 = svdup_n_u32(0);
        svuint32_t acc12 = svdup_n_u32(0);
        svuint32_t acc13 = svdup_n_u32(0);
        for (svbool_t pg = svwhilelt_b8(r, bit_length); svptest_any(svptrue_b8(), pg); pg = svwhilelt_b8(r, bit_length)) {
            const svuint8_t q0 = svld1_u8(pg, (const uint8_t*)(query + r));
            const svuint8_t q1 = svld1_u8(pg, (const uint8_t*)(query + r + bit_length));
            const svuint8_t q2 = svld1_u8(pg, (const uint8_t*)(query + r + 2 * bit_length));
            const svuint8_t q3 = svld1_u8(pg, (const uint8_t*)(query + r + 3 * bit_length));
            dot_d4q4_step_sv(acc00, acc01, acc02, acc03, pg, a0 + r, bit_length, q0, q1, q2, q3, w0, w1, w2, w3);
            dot_d4q4_step_sv(acc10, acc11, acc12, acc13, pg, a1 + r, bit_length, q0, q1, q2, q3, w0, w1, w2, w3);
            r += svcntb();
        }
        results[out_mapper(offsets, c)] = (f32_t)sum_d4q4_sv(acc00, acc01, acc02, acc03);
        results[out_mapper(offsets, c + 1)] = (f32_t)sum_d4q4_sv(acc10, acc11, acc12, acc13);
    }
    if (c < count) {
        results[out_mapper(offsets, c)] = (f32_t)dotd4q4_inner(mapper(a, c, offsets, pitch), query, length);
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
    const int32_t block_mask = clamp_mask(mask, count);
    dotd4q4_inner_bulk<int8_t, masked_mapper, masked_output>(
        a, query, length, pitch, &block_mask, __builtin_popcount((uint32_t)block_mask), results);
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
