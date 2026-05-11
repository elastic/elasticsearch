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
// Four 2-bit fields per doc byte: element E occupies bits [7-2E : 6-2E], i.e. shifts 6/4/2/0.
constexpr int elements_per_byte = 4;

static inline int32_t dotd2q4_packed_inner(const int8_t* a, const int8_t* query, int32_t packed_len) {
    const uint8x16_t mask_two_bits = vdupq_n_u8(0x03);
    uint32x4_t acc[elements_per_byte];
    apply_indexed<elements_per_byte>([&](auto E) { acc[E] = vdupq_n_u32(0); });

    constexpr int stride = sizeof(uint8x16_t);
    const int blk = packed_len & ~(stride - 1);

    // vdotq_u32 accumulates 4-byte products directly into 32-bit lanes; max product per
    // group is 4 * 3 * 15 = 180 (2-bit doc * 4-bit query). 2^32/180 ≈ 23M iterations of
    // headroom — effectively unbounded for any realistic packed_len.
    for (int i = 0; i < blk; i += stride) {
        uint8x16_t doc_bytes = vld1q_u8((const uint8_t*)(a + i));

        // Extract the four 2-bit planes via per-byte right shifts (NEON's u8 shift has no
        // cross-byte leakage, so the highest plane needs no mask).
        uint8x16_t doc_e[elements_per_byte];
        doc_e[0] = vshrq_n_u8(doc_bytes, 6);
        doc_e[1] = vandq_u8(vshrq_n_u8(doc_bytes, 4), mask_two_bits);
        doc_e[2] = vandq_u8(vshrq_n_u8(doc_bytes, 2), mask_two_bits);
        doc_e[3] = vandq_u8(doc_bytes, mask_two_bits);

        apply_indexed<elements_per_byte>([&](auto E) {
            uint8x16_t query_e = vld1q_u8((const uint8_t*)(query + i + E * packed_len));
            acc[E] = vdotq_u32(acc[E], doc_e[E], query_e);
        });
    }

    int32_t total = (int32_t)vaddvq_u32(tree_reduce<elements_per_byte, uint32x4_t, vaddq_u32>(acc));

    for (int i = blk; i < packed_len; i++) {
        uint8_t doc_byte = (uint8_t)a[i];
        apply_indexed<elements_per_byte>([&](auto E) {
            constexpr int shift = 6 - 2 * E;
            total += ((doc_byte >> shift) & 0x03) * query[i + E * packed_len];
        });
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
        uint32x4_t acc[elements_per_byte][batches];

        apply_indexed<batches>([&](auto I) {
            doc_ptrs[I] = mapper(docs, c + I, offsets, pitch);
            apply_indexed<elements_per_byte>([&](auto E) {
                acc[E][I] = vdupq_n_u32(0);
            });
        });

        int i = 0;
        for (; i < blk; i += stride) {
            uint8x16_t query_e[elements_per_byte];
            apply_indexed<elements_per_byte>([&](auto E) {
                query_e[E] = vld1q_u8((const uint8_t*)(query + i + E * packed_len));
            });

            apply_indexed<batches>([&](auto I) {
                uint8x16_t doc_bytes = vld1q_u8((const uint8_t*)(doc_ptrs[I] + i));
                // Extract the four 2-bit planes via per-byte right shifts (NEON's u8 shift has no
                // cross-byte leakage, so the highest plane needs no mask).
                uint8x16_t doc_e[elements_per_byte];
                doc_e[0] = vshrq_n_u8(doc_bytes, 6);
                doc_e[1] = vandq_u8(vshrq_n_u8(doc_bytes, 4), mask_two_bits);
                doc_e[2] = vandq_u8(vshrq_n_u8(doc_bytes, 2), mask_two_bits);
                doc_e[3] = vandq_u8(doc_bytes, mask_two_bits);

                apply_indexed<elements_per_byte>([&](auto E) {
                    acc[E][I] = vdotq_u32(acc[E][I], doc_e[E], query_e[E]);
                });
            });
        }

        int32_t res[batches];
        apply_indexed<batches>([&](auto I) {
            uint32x4_t per_batch[elements_per_byte];
            apply_indexed<elements_per_byte>([&](auto E) { per_batch[E] = acc[E][I]; });
            res[I] = (int32_t)vaddvq_u32(tree_reduce<elements_per_byte, uint32x4_t, vaddq_u32>(per_batch));
        });

        for (; i < packed_len; i++) {
            uint8_t q[elements_per_byte];
            apply_indexed<elements_per_byte>([&](auto E) {
                q[E] = (uint8_t)query[i + E * packed_len];
            });
            apply_indexed<batches>([&](auto I) {
                uint8_t doc_byte = (uint8_t)doc_ptrs[I][i];
                apply_indexed<elements_per_byte>([&](auto E) {
                    constexpr int shift = 6 - 2 * E;
                    res[I] += ((doc_byte >> shift) & 0x03) * q[E];
                });
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
