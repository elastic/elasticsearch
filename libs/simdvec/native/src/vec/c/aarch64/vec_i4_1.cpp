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

static inline int32_t doti4_inner(const int8_t* unpacked, const int8_t* packed, int32_t packed_len) {
    const uint8x16_t mask_0f = vdupq_n_u8(0x0F);

    // 4 accumulators to break dependency chains:
    // hi0/hi1 for high-nibble products, lo0/lo1 for low-nibble products,
    // each split across the low and high halves of the 128-bit vectors.
    uint32x4_t acc_hi0 = vdupq_n_u32(0);
    uint32x4_t acc_hi1 = vdupq_n_u32(0);
    uint32x4_t acc_lo0 = vdupq_n_u32(0);
    uint32x4_t acc_lo1 = vdupq_n_u32(0);

    constexpr int stride = sizeof(uint8x16_t);
    const int blk = packed_len & ~(stride - 1);

    for (int i = 0; i < blk; i += stride) {
        uint8x16_t p = vld1q_u8((const uint8_t*)(packed + i));
        uint8x16_t hi = vshrq_n_u8(p, 4);
        uint8x16_t lo = vandq_u8(p, mask_0f);

        uint8x16_t u_hi = vld1q_u8((const uint8_t*)(unpacked + i));
        uint8x16_t u_lo = vld1q_u8((const uint8_t*)(unpacked + i + packed_len));

        // vmull_u8 widens 8-bit -> 16-bit, vpadalq_u16 pairwise-adds 16-bit -> 32-bit
        acc_hi0 = vpadalq_u16(acc_hi0, vmull_u8(vget_low_u8(hi), vget_low_u8(u_hi)));
        acc_hi1 = vpadalq_u16(acc_hi1, vmull_u8(vget_high_u8(hi), vget_high_u8(u_hi)));
        acc_lo0 = vpadalq_u16(acc_lo0, vmull_u8(vget_low_u8(lo), vget_low_u8(u_lo)));
        acc_lo1 = vpadalq_u16(acc_lo1, vmull_u8(vget_high_u8(lo), vget_high_u8(u_lo)));
    }

    int32_t total = (int32_t)vaddvq_u32(vaddq_u32(vaddq_u32(acc_hi0, acc_hi1), vaddq_u32(acc_lo0, acc_lo1)));

    for (int i = blk; i < packed_len; i++) {
        uint8_t p = (uint8_t)packed[i];
        total += (p >> 4) * unpacked[i];
        total += (p & 0x0F) * unpacked[i + packed_len];
    }
    return total;
}

EXPORT int32_t vec_doti4(const int8_t* unpacked, const int8_t* packed, int32_t packed_len) {
    return doti4_inner(unpacked, packed, packed_len);
}

template <int64_t(*mapper)(const int32_t, const int32_t*), int batches = 4>
static inline void doti4_bulk_impl(
    const int8_t* a,
    const int8_t* b,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    const uint8x16_t mask_0f = vdupq_n_u8(0x0F);
    constexpr int stride = sizeof(uint8x16_t);
    const int blk = packed_len & ~(stride - 1);

    int c = 0;

    for (; c + batches - 1 < count; c += batches) {
        const int8_t* as[batches];
        uint32x4_t acc0[batches];
        uint32x4_t acc1[batches];

        apply_indexed<batches>([&](auto I) {
            as[I] = a + mapper(c + I, offsets) * pitch;
            acc0[I] = vdupq_n_u32(0);
            acc1[I] = vdupq_n_u32(0);
        });

        int i = 0;
        for (; i < blk; i += stride) {
            uint8x16_t u_hi = vld1q_u8((const uint8_t*)(b + i));
            uint8x16_t u_lo = vld1q_u8((const uint8_t*)(b + i + packed_len));

            apply_indexed<batches>([&](auto I) {
                uint8x16_t p = vld1q_u8((const uint8_t*)(as[I] + i));
                uint8x16_t hi = vshrq_n_u8(p, 4);
                uint8x16_t lo = vandq_u8(p, mask_0f);

                acc0[I] = vpadalq_u16(acc0[I], vmull_u8(vget_low_u8(hi), vget_low_u8(u_hi)));
                acc0[I] = vpadalq_u16(acc0[I], vmull_u8(vget_high_u8(hi), vget_high_u8(u_hi)));
                acc1[I] = vpadalq_u16(acc1[I], vmull_u8(vget_low_u8(lo), vget_low_u8(u_lo)));
                acc1[I] = vpadalq_u16(acc1[I], vmull_u8(vget_high_u8(lo), vget_high_u8(u_lo)));
            });
        }

        int32_t res[batches];
        apply_indexed<batches>([&](auto I) {
            res[I] = (int32_t)vaddvq_u32(vaddq_u32(acc0[I], acc1[I]));
        });

        for (; i < packed_len; i++) {
            uint8_t uhi = (uint8_t)b[i];
            uint8_t ulo = (uint8_t)b[i + packed_len];
            apply_indexed<batches>([&](auto I) {
                uint8_t p = (uint8_t)as[I][i];
                res[I] += (p >> 4) * uhi;
                res[I] += (p & 0x0F) * ulo;
            });
        }

        apply_indexed<batches>([&](auto I) {
            results[c + I] = (f32_t)res[I];
        });
    }

    for (; c < count; c++) {
        const int8_t* doc = a + mapper(c, offsets) * pitch;
        results[c] = (f32_t)doti4_inner(b, doc, packed_len);
    }
}

EXPORT void vec_doti4_bulk(const int8_t* a, const int8_t* b, int32_t packed_len, int32_t count, f32_t* results) {
    doti4_bulk_impl<identity_mapper>(a, b, packed_len, packed_len, NULL, count, results);
}

EXPORT void vec_doti4_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    doti4_bulk_impl<array_mapper>(a, b, packed_len, pitch, offsets, count, results);
}
