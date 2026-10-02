/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

 // This file contains implementations for BBQ vector operations,
 // including support for "1st tier" vector capabilities; in the case of ARM,
 // this first tier include functions for processors supporting at least the NEON
 // instruction set.

#include <stddef.h>
#include <arm_neon.h>
#include "vec.h"
#include "vec_common.h"
#include "aarch64/aarch64_vec_common.h"

// Counts the bits of a & q per byte and folds the counts into acc with UDOT against a constant weight
// vector: one instruction applies the query plane's bit value and sums four bytes into each 32-bit lane, so
// no 8-bit partial sums and no periodic reduction are needed. Per lane a step adds at most
// 4 bytes * 8 bits * weight, far from a u32 overflow.
static inline uint32x4_t dot_bit_weighted_neon(const uint32x4_t acc, const uint8x16_t a, const uint8x16_t q, const uint8x16_t weight) {
    return vdotq_u32(acc, vcntq_u8(vandq_u8(a, q)), weight);
}

template<int query_bits>
static inline int64_t dotd1qN_inner(const int8_t* a, const int8_t* q, const int32_t length) {
    int64_t bit_result[query_bits] = {};
    int64_t sum = 0;

    const uint8_t* query[query_bits];
    apply_indexed<query_bits>([&](auto I) {
        query[I] = (const uint8_t*)q + I * length;
    });

    int r = 0;
    constexpr int chunk_size = sizeof(uint8x16_t);
    if (length >= chunk_size) {
        // one accumulator per query plane keeps the UDOT chains independent
        uint32x4_t acc[query_bits];
        uint8x16_t weight[query_bits];
        apply_indexed<query_bits>([&](auto I) {
            acc[I] = vdupq_n_u32(0);
            weight[I] = vdupq_n_u8(1 << I);
        });

        for (; r + chunk_size <= length; r += chunk_size) {
            const uint8x16_t yv = vld1q_u8((const uint8_t*)a + r);
            apply_indexed<query_bits>([&](auto I) {
                acc[I] = dot_bit_weighted_neon(acc[I], yv, vld1q_u8(query[I] + r), weight[I]);
            });
        }

        apply_indexed<query_bits>([&](auto I) {
            sum += vaddvq_u32(acc[I]);
        });
    }

    // switch to single 64-bit ops
    int upperBound = length & ~(sizeof(int64_t) - 1);
    for (; r < upperBound; r += sizeof(int64_t)) {
        int64_t value = *((int64_t*)(a + r));
        apply_indexed<query_bits>([&](auto I) {
            int64_t bits = *((int64_t*)(q + r + I * length));
            bit_result[I] += __builtin_popcountll(bits & value);
        });
    }

    // then 32-bit ops
    upperBound = length & ~(sizeof(int32_t) - 1);
    for (; r < upperBound; r += sizeof(int32_t)) {
        int32_t value = *((int32_t*)(a + r));
        apply_indexed<query_bits>([&](auto I) {
            int32_t bits = *((int32_t*)(q + r + I * length));
            bit_result[I] += __builtin_popcount(bits & value);
        });
    }

    // then single bytes
    for (; r < length; r++) {
        int8_t value = *(a + r);
        apply_indexed<query_bits>([&](auto I) {
            int32_t bits = *(q + r + I * length);
            bit_result[I] += __builtin_popcount(bits & value & 0xFF);
        });
    }
    apply_indexed<query_bits>([&](auto I) {
        sum += (bit_result[I] << I);
    });
    return sum;
}

EXPORT int64_t vec_dotd1q4(const int8_t* a, const int8_t* query, const int32_t length) {
    return dotd1qN_inner<4>(a, query, length);
}

EXPORT int64_t vec_dotd1q1(const int8_t* a, const int8_t* query, const int32_t length) {
    return dotd1qN_inner<1>(a, query, length);
}

template <typename TData, int query_bits, const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t)>
static inline void dotd1qN_inner_bulk(
    const TData* a,
    const int8_t* q,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    // four vectors per batch keep twice as many cache misses in flight as two; two query planes share one
    // accumulator, UDOT folds the plane weight in, so the accumulators, the query planes, the weights and the
    // loaded vectors of a batch fit in the 32 NEON registers
    constexpr int batches = 4;
    constexpr int chunk_size = sizeof(uint8x16_t);
    constexpr int accs = (query_bits + 1) / 2;

    const uint8_t* query[query_bits];
    uint8x16_t weight[query_bits];
    apply_indexed<query_bits>([&](auto I) {
        query[I] = (const uint8_t*)q + I * length;
        weight[I] = vdupq_n_u8(1 << I);
    });

    const int blk = length & ~(chunk_size - 1);

    int c = 0;

    for (; c + batches - 1 < count; c += batches) {
        const uint8_t* as[batches];
        apply_indexed<batches>([&](auto I) {
            as[I] = (const uint8_t*)mapper(a, c + I, offsets, pitch);
        });

        uint32x4_t acc[batches * accs];
        apply_indexed<batches * accs>([&](auto I) {
            acc[I] = vdupq_n_u32(0);
        });

        for (int r = 0; r < blk; r += chunk_size) {
            uint8x16_t qv[query_bits];
            apply_indexed<query_bits>([&](auto I) {
                qv[I] = vld1q_u8(query[I] + r);
            });

            apply_indexed<batches>([&](auto B) {
                const uint8x16_t yv = vld1q_u8(as[B] + r);
                apply_indexed<query_bits>([&](auto Q) {
                    constexpr int idx = B * accs + Q / 2;
                    acc[idx] = dot_bit_weighted_neon(acc[idx], yv, qv[Q], weight[Q]);
                });
            });
        }

        int64_t res[batches];
        apply_indexed<batches>([&](auto B) {
            res[B] = 0;
            apply_indexed<accs>([&](auto K) {
                res[B] += vaddvq_u32(acc[B * accs + K]);
            });
        });

        // Byte tail. Single-byte loads only: with sparse addresses a vector can
        // end right before an unmapped page, so wider loads could fault.
        for (int r = blk; r < length; r++) {
            uint8_t vs[batches];
            apply_indexed<batches>([&](auto I) {
                vs[I] = as[I][r];
            });

            uint8_t qs[query_bits];
            apply_indexed<query_bits>([&](auto I) {
                qs[I] = query[I][r];
            });

            apply_indexed<batches>([&](auto B) {
                apply_indexed<query_bits>([&](auto Q) {
                    res[B] += __builtin_popcount(qs[Q] & vs[B]) << Q;
                });
            });
        }
        apply_indexed<batches>([&](auto B) {
            results[c + B] = (f32_t)res[B];
        });
    }

    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = (f32_t)dotd1qN_inner<query_bits>(a0, q, length);
    }
}

EXPORT void vec_dotd1q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1qN_inner_bulk<int8_t, 4, sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd1q4_bulk_offsets(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dotd1qN_inner_bulk<int8_t, 4, offsets_mapper>(a, query, length, pitch, offsets, count, results);
}

EXPORT void vec_dotd1q4_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1qN_inner_bulk<const int8_t*, 4, sparse_mapper>((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT void vec_dotd1q1_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1qN_inner_bulk<int8_t, 1, sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd1q1_bulk_offsets(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dotd1qN_inner_bulk<int8_t, 1, offsets_mapper>(a, query, length, pitch, offsets, count, results);
}

EXPORT void vec_dotd1q1_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1qN_inner_bulk<const int8_t*, 1, sparse_mapper>((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT int64_t vec_dotd2q2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length
) {
    int64_t lower = dotd1qN_inner<2>(a, query, length/2);
    int64_t upper = dotd1qN_inner<2>(a + length/2, query, length/2);
    return lower + (upper << 1);
}

template <typename TData, const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t)>
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
        int64_t lower = dotd1qN_inner<2>(a0, query, bit_length);
        int64_t upper = dotd1qN_inner<2>(a0 + bit_length, query, bit_length);
        results[c] = (f32_t)(lower + (upper << 1));
    }
}

EXPORT void vec_dotd2q2_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q2_inner_bulk<int8_t, sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd2q2_bulk_offsets(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dotd2q2_inner_bulk<int8_t, offsets_mapper>(a, query, length, pitch, offsets, count, results);
}

EXPORT void vec_dotd2q2_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q2_inner_bulk<const int8_t*, sparse_mapper>((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT int64_t vec_dotd2q4(
    const int8_t* a,
    const int8_t* query,
    const int32_t length
) {
    int64_t lower = dotd1qN_inner<4>(a, query, length/2);
    int64_t upper = dotd1qN_inner<4>(a + length/2, query, length/2);
    return lower + (upper << 1);
}

template <typename TData, const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t)>
static inline void dotd2q4_inner_bulk(
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
        int64_t lower = dotd1qN_inner<4>(a0, query, bit_length);
        int64_t upper = dotd1qN_inner<4>(a0 + bit_length, query, bit_length);
        results[c] = (f32_t)(lower + (upper << 1));
    }
}

EXPORT void vec_dotd2q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<int8_t, sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd2q4_bulk_offsets(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<int8_t, offsets_mapper>(a, query, length, pitch, offsets, count, results);
}

EXPORT void vec_dotd2q4_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<const int8_t*, sparse_mapper>((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT int64_t vec_dotd4q4(const int8_t* a, const int8_t* query, const int32_t length) {
    const int32_t bit_length = length / 4;
    int64_t p0 = dotd1qN_inner<4>(a + 0 * bit_length, query, bit_length);
    int64_t p1 = dotd1qN_inner<4>(a + 1 * bit_length, query, bit_length);
    int64_t p2 = dotd1qN_inner<4>(a + 2 * bit_length, query, bit_length);
    int64_t p3 = dotd1qN_inner<4>(a + 3 * bit_length, query, bit_length);
    return p0 + (p1 << 1) + (p2 << 2) + (p3 << 3);
}

template <typename TData, const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t)>
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

        int64_t p0 = dotd1qN_inner<4>(a0 + 0 * bit_length, query, bit_length);
        int64_t p1 = dotd1qN_inner<4>(a0 + 1 * bit_length, query, bit_length);
        int64_t p2 = dotd1qN_inner<4>(a0 + 2 * bit_length, query, bit_length);
        int64_t p3 = dotd1qN_inner<4>(a0 + 3 * bit_length, query, bit_length);

        results[c] = (f32_t)(p0 + (p1 << 1) + (p2 << 2) + (p3 << 3));
    }
}

EXPORT void vec_dotd4q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<int8_t, sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd4q4_bulk_offsets(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<int8_t, offsets_mapper>(a, query, length, pitch, offsets, count, results);
}

EXPORT void vec_dotd4q4_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<const int8_t*, sparse_mapper>((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}
