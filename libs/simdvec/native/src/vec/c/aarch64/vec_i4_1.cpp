/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// NEON-vectorized int4 packed-nibble vector operations.
// The "unpacked" vector has 2*packed_len bytes (high nibbles in [0..packed_len),
// low nibbles in [packed_len..2*packed_len)). The "packed" vector has packed_len
// bytes, each holding two 4-bit values.

#include <stddef.h>
#include <arm_neon.h>
#include <algorithm>
#include "vec.h"
#include "vec_common.h"
#include "aarch64/aarch64_vec_common.h"

static inline int32_t doti4_inner(const int8_t* query, const int8_t* doc, int32_t packed_len) {
    const uint8x16_t mask_half_byte = vdupq_n_u8(0x0F);
    uint32x4_t acc_high = vdupq_n_u32(0);
    uint32x4_t acc_low = vdupq_n_u32(0);

    constexpr int stride = sizeof(uint8x16_t);
    const int blk = packed_len & ~(stride - 1);

    for (int i = 0; i < blk; i += stride) {
        uint8x16_t doc_bytes = vld1q_u8((const uint8_t*)(doc + i));
        uint8x16_t doc_high = vshrq_n_u8(doc_bytes, 4);
        uint8x16_t doc_low = vandq_u8(doc_bytes, mask_half_byte);

        uint8x16_t query_high = vld1q_u8((const uint8_t*)(query + i));
        uint8x16_t query_low = vld1q_u8((const uint8_t*)(query + i + packed_len));

        // vdotq_u32 multiplies groups of 4 unsigned 8-bit values and accumulates
        // directly into 32-bit lanes
        acc_high = vdotq_u32(acc_high, doc_high, query_high);
        acc_low = vdotq_u32(acc_low, doc_low, query_low);
    }

    int32_t total = (int32_t)vaddvq_u32(vaddq_u32(acc_high, acc_low));

    for (int i = blk; i < packed_len; i++) {
        uint8_t doc_byte = (uint8_t)doc[i];
        total += (doc_byte >> 4) * query[i];
        total += (doc_byte & 0x0F) * query[i + packed_len];
    }
    return total;
}

EXPORT int32_t vec_doti4(const int8_t* query, const int8_t* doc, int32_t packed_len) {
    return doti4_inner(query, doc, packed_len);
}

template <const int8_t*(*mapper)(const int8_t*, const int32_t, const int32_t*, const int32_t), int batches = 4>
static inline void doti4_bulk_impl(
    const int8_t* docs,
    const int8_t* query,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    const uint8x16_t mask_half_byte = vdupq_n_u8(0x0F);
    constexpr int stride = sizeof(uint8x16_t);
    const int blk = packed_len & ~(stride - 1);

    int c = 0;

    for (; c + batches - 1 < count; c += batches) {
        const int8_t* doc_ptrs[batches];
        uint32x4_t acc_high[batches];
        uint32x4_t acc_low[batches];

        apply_indexed<batches>([&](auto I) {
            doc_ptrs[I] = mapper(docs, c + I, offsets, pitch);
            acc_high[I] = vdupq_n_u32(0);
            acc_low[I] = vdupq_n_u32(0);
        });

        int i = 0;
        for (; i < blk; i += stride) {
            uint8x16_t query_high = vld1q_u8((const uint8_t*)(query + i));
            uint8x16_t query_low = vld1q_u8((const uint8_t*)(query + i + packed_len));

            apply_indexed<batches>([&](auto I) {
                uint8x16_t doc_bytes = vld1q_u8((const uint8_t*)(doc_ptrs[I] + i));
                uint8x16_t doc_high = vshrq_n_u8(doc_bytes, 4);
                uint8x16_t doc_low = vandq_u8(doc_bytes, mask_half_byte);

                acc_high[I] = vdotq_u32(acc_high[I], doc_high, query_high);
                acc_low[I] = vdotq_u32(acc_low[I], doc_low, query_low);
            });
        }

        int32_t res[batches];
        apply_indexed<batches>([&](auto I) {
            res[I] = (int32_t)vaddvq_u32(vaddq_u32(acc_high[I], acc_low[I]));
        });

        for (; i < packed_len; i++) {
            uint8_t query_high_val = (uint8_t)query[i];
            uint8_t query_low_val = (uint8_t)query[i + packed_len];
            apply_indexed<batches>([&](auto I) {
                uint8_t doc_byte = (uint8_t)doc_ptrs[I][i];
                res[I] += (doc_byte >> 4) * query_high_val;
                res[I] += (doc_byte & 0x0F) * query_low_val;
            });
        }

        apply_indexed<batches>([&](auto I) {
            results[c + I] = (f32_t)res[I];
        });
    }

    for (; c < count; c++) {
        const int8_t* doc = mapper(docs, c, offsets, pitch);
        results[c] = (f32_t)doti4_inner(query, doc, packed_len);
    }
}

EXPORT void vec_doti4_bulk(const int8_t* docs, const int8_t* query, int32_t packed_len, int32_t count, f32_t* results) {
    doti4_bulk_impl<sequential_mapper>(docs, query, packed_len, packed_len, NULL, count, results);
}

EXPORT void vec_doti4_bulk_offsets(
    const int8_t* docs,
    const int8_t* query,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    doti4_bulk_impl<offsets_mapper>(docs, query, packed_len, pitch, offsets, count, results);
}
