/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// NEON-vectorized N-bit packed doc BBQ kernels.

#include <stddef.h>
#include <arm_neon.h>
#include <algorithm>
#include "vec.h"
#include "vec_common.h"
#include "aarch64/aarch64_vec_common.h"

// --- D2Q4 ---

// Doc layout: packed_len bytes, four 2-bit values per byte:
//   [v0:7..6 | v1:5..4 | v2:3..2 | v3:1..0]
// Query layout: 4*packed_len bytes, one 4-bit value per byte (range 0..15), partitioned
// into four contiguous stripes that align with the doc's 2-bit fields:
//   [0..packed_len)            -> stripe 0 (pairs with bits 7:6 of packed)
//   [packed_len..2*packed_len) -> stripe 1 (bits 5:4)
//   [2*packed_len..3*packed_len) -> stripe 2 (bits 3:2)
//   [3*packed_len..4*packed_len) -> stripe 3 (bits 1:0)
static inline int32_t dotd2q4_packed_inner(const int8_t* a, const int8_t* query, int32_t packed_len) {
    const uint8x16_t mask_two_bits = vdupq_n_u8(0x03);
    uint32x4_t acc_s0 = vdupq_n_u32(0);
    uint32x4_t acc_s1 = vdupq_n_u32(0);
    uint32x4_t acc_s2 = vdupq_n_u32(0);
    uint32x4_t acc_s3 = vdupq_n_u32(0);

    constexpr int stride = sizeof(uint8x16_t);
    const int blk = packed_len & ~(stride - 1);

    // vdotq_u32 accumulates 4-byte products directly into 32-bit lanes; max product per
    // group is 4 * 3 * 15 = 180 (2-bit doc * 4-bit query). 2^32/180 ≈ 23M iterations of
    // headroom — effectively unbounded for any realistic packed_len.
    for (int i = 0; i < blk; i += stride) {
        uint8x16_t doc_bytes = vld1q_u8((const uint8_t*)(a + i));
        // Extract the four 2-bit planes via per-byte right shifts (NEON has u8 shift, no cross-byte leakage).
        uint8x16_t doc_s0 = vshrq_n_u8(doc_bytes, 6);
        uint8x16_t doc_s1 = vandq_u8(vshrq_n_u8(doc_bytes, 4), mask_two_bits);
        uint8x16_t doc_s2 = vandq_u8(vshrq_n_u8(doc_bytes, 2), mask_two_bits);
        uint8x16_t doc_s3 = vandq_u8(doc_bytes, mask_two_bits);

        uint8x16_t query_s0 = vld1q_u8((const uint8_t*)(query + i));
        uint8x16_t query_s1 = vld1q_u8((const uint8_t*)(query + i + packed_len));
        uint8x16_t query_s2 = vld1q_u8((const uint8_t*)(query + i + 2 * packed_len));
        uint8x16_t query_s3 = vld1q_u8((const uint8_t*)(query + i + 3 * packed_len));

        acc_s0 = vdotq_u32(acc_s0, doc_s0, query_s0);
        acc_s1 = vdotq_u32(acc_s1, doc_s1, query_s1);
        acc_s2 = vdotq_u32(acc_s2, doc_s2, query_s2);
        acc_s3 = vdotq_u32(acc_s3, doc_s3, query_s3);
    }

    int32_t total = (int32_t)vaddvq_u32(vaddq_u32(vaddq_u32(acc_s0, acc_s1), vaddq_u32(acc_s2, acc_s3)));

    for (int i = blk; i < packed_len; i++) {
        uint8_t doc_byte = (uint8_t)a[i];
        total += ((doc_byte >> 6) & 0x03) * query[i];
        total += ((doc_byte >> 4) & 0x03) * query[i + packed_len];
        total += ((doc_byte >> 2) & 0x03) * query[i + 2 * packed_len];
        total += (doc_byte & 0x03) * query[i + 3 * packed_len];
    }
    return total;
}

EXPORT int64_t vec_dotd2q4_packed(const int8_t* a, const int8_t* query, int32_t packed_len) {
    return (int64_t)dotd2q4_packed_inner(a, query, packed_len);
}

template <typename TData, const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t), int batches = 4>
static inline void dotd2q4_packed_bulk_impl(
    const TData* docs,
    const int8_t* query,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    const uint8x16_t mask_two_bits = vdupq_n_u8(0x03);
    constexpr int stride = sizeof(uint8x16_t);
    const int blk = packed_len & ~(stride - 1);

    int c = 0;

    for (; c + batches - 1 < count; c += batches) {
        const int8_t* doc_ptrs[batches];
        uint32x4_t acc_s0[batches];
        uint32x4_t acc_s1[batches];
        uint32x4_t acc_s2[batches];
        uint32x4_t acc_s3[batches];

        apply_indexed<batches>([&](auto I) {
            doc_ptrs[I] = mapper(docs, c + I, offsets, pitch);
            acc_s0[I] = vdupq_n_u32(0);
            acc_s1[I] = vdupq_n_u32(0);
            acc_s2[I] = vdupq_n_u32(0);
            acc_s3[I] = vdupq_n_u32(0);
        });

        int i = 0;
        for (; i < blk; i += stride) {
            uint8x16_t query_s0 = vld1q_u8((const uint8_t*)(query + i));
            uint8x16_t query_s1 = vld1q_u8((const uint8_t*)(query + i + packed_len));
            uint8x16_t query_s2 = vld1q_u8((const uint8_t*)(query + i + 2 * packed_len));
            uint8x16_t query_s3 = vld1q_u8((const uint8_t*)(query + i + 3 * packed_len));

            apply_indexed<batches>([&](auto I) {
                uint8x16_t doc_bytes = vld1q_u8((const uint8_t*)(doc_ptrs[I] + i));
                uint8x16_t doc_s0 = vshrq_n_u8(doc_bytes, 6);
                uint8x16_t doc_s1 = vandq_u8(vshrq_n_u8(doc_bytes, 4), mask_two_bits);
                uint8x16_t doc_s2 = vandq_u8(vshrq_n_u8(doc_bytes, 2), mask_two_bits);
                uint8x16_t doc_s3 = vandq_u8(doc_bytes, mask_two_bits);

                acc_s0[I] = vdotq_u32(acc_s0[I], doc_s0, query_s0);
                acc_s1[I] = vdotq_u32(acc_s1[I], doc_s1, query_s1);
                acc_s2[I] = vdotq_u32(acc_s2[I], doc_s2, query_s2);
                acc_s3[I] = vdotq_u32(acc_s3[I], doc_s3, query_s3);
            });
        }

        int32_t res[batches];
        apply_indexed<batches>([&](auto I) {
            res[I] = (int32_t)vaddvq_u32(vaddq_u32(vaddq_u32(acc_s0[I], acc_s1[I]), vaddq_u32(acc_s2[I], acc_s3[I])));
        });

        for (; i < packed_len; i++) {
            uint8_t q0 = (uint8_t)query[i];
            uint8_t q1 = (uint8_t)query[i + packed_len];
            uint8_t q2 = (uint8_t)query[i + 2 * packed_len];
            uint8_t q3 = (uint8_t)query[i + 3 * packed_len];
            apply_indexed<batches>([&](auto I) {
                uint8_t doc_byte = (uint8_t)doc_ptrs[I][i];
                res[I] += ((doc_byte >> 6) & 0x03) * q0;
                res[I] += ((doc_byte >> 4) & 0x03) * q1;
                res[I] += ((doc_byte >> 2) & 0x03) * q2;
                res[I] += (doc_byte & 0x03) * q3;
            });
        }

        apply_indexed<batches>([&](auto I) {
            results[c + I] = (f32_t)res[I];
        });
    }

    for (; c < count; c++) {
        const int8_t* doc = mapper(docs, c, offsets, pitch);
        results[c] = (f32_t)dotd2q4_packed_inner(doc, query, packed_len);
    }
}

EXPORT void vec_dotd2q4_packed_bulk(
    const int8_t* docs,
    const int8_t* query,
    int32_t packed_len,
    int32_t count,
    f32_t* results
) {
    dotd2q4_packed_bulk_impl<int8_t, sequential_mapper>(docs, query, packed_len, packed_len, NULL, count, results);
}

EXPORT void vec_dotd2q4_packed_bulk_offsets(
    const int8_t* docs,
    const int8_t* query,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    dotd2q4_packed_bulk_impl<int8_t, offsets_mapper>(docs, query, packed_len, pitch, offsets, count, results);
}

EXPORT void vec_dotd2q4_packed_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    int32_t packed_len,
    int32_t count,
    f32_t* results
) {
    dotd2q4_packed_bulk_impl<const int8_t*, sparse_mapper>(
        (const int8_t* const*)addresses, query, packed_len, 0, NULL, count, results
    );
}
