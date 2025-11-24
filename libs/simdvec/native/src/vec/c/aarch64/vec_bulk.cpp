/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

#include <stddef.h>
#include <stdint.h>
#include <arm_neon.h>
#include <math.h>
extern "C" {
  #include "vec.h"
}

#ifndef DOT7U_STRIDE_BYTES_LEN
#define DOT7U_STRIDE_BYTES_LEN 32 // Must be a power of 2
#endif

#ifndef SQR7U_STRIDE_BYTES_LEN
#define SQR7U_STRIDE_BYTES_LEN 16 // Must be a power of 2
#endif

template <int(*mapper)(int, int32_t*)>
static inline void dot7u_inner_bulk(int8_t* a, int8_t* b, int dims, int32_t* offsets, int count, f32_t* results) {
    size_t blk = dims & ~15;
    size_t c = 0;

    // Process 4 vectors at a time
    for (; c + 3 < count; c += 4) {
        const int8_t* a0 = a + mapper(c, offsets) * dims;
        const int8_t* a1 = a + mapper(c + 1, offsets) * dims;
        const int8_t* a2 = a + mapper(c + 2, offsets) * dims;
        const int8_t* a3 = a + mapper(c + 3, offsets) * dims;

        int32x4_t acc0 = vdupq_n_s32(0);
        int32x4_t acc1 = vdupq_n_s32(0);
        int32x4_t acc2 = vdupq_n_s32(0);
        int32x4_t acc3 = vdupq_n_s32(0);
        int32x4_t acc4 = vdupq_n_s32(0);
        int32x4_t acc5 = vdupq_n_s32(0);
        int32x4_t acc6 = vdupq_n_s32(0);
        int32x4_t acc7 = vdupq_n_s32(0);

        for (size_t i = 0; i < blk; i += 16) {
            int8x16_t vb = vld1q_s8(b + i);

            int8x16_t v0 = vld1q_s8(a0 + i);
            int16x8_t lo0 = vmull_s8(vget_low_s8(v0), vget_low_s8(vb));
            int16x8_t hi0 = vmull_s8(vget_high_s8(v0), vget_high_s8(vb));
            acc0 = vpadalq_s16(acc0, lo0);
            acc1 = vpadalq_s16(acc1, hi0);

            int8x16_t v1 = vld1q_s8(a1 + i);
            int16x8_t lo1 = vmull_s8(vget_low_s8(v1), vget_low_s8(vb));
            int16x8_t hi1 = vmull_s8(vget_high_s8(v1), vget_high_s8(vb));
            acc2 = vpadalq_s16(acc2, lo1);
            acc3 = vpadalq_s16(acc3, hi1);

            int8x16_t v2 = vld1q_s8(a2 + i);
            int16x8_t lo2 = vmull_s8(vget_low_s8(v2), vget_low_s8(vb));
            int16x8_t hi2 = vmull_s8(vget_high_s8(v2), vget_high_s8(vb));
            acc4 = vpadalq_s16(acc4, lo2);
            acc5 = vpadalq_s16(acc5, hi2);

            int8x16_t v3 = vld1q_s8(a3 + i);
            int16x8_t lo3 = vmull_s8(vget_low_s8(v3), vget_low_s8(vb));
            int16x8_t hi3 = vmull_s8(vget_high_s8(v3), vget_high_s8(vb));
            acc6 = vpadalq_s16(acc6, lo3);
            acc7 = vpadalq_s16(acc7, hi3);
        }
        int32x4_t acc01 = vaddq_s32(acc0, acc1);
        int32x4_t acc23 = vaddq_s32(acc2, acc3);
        int32x4_t acc45 = vaddq_s32(acc4, acc5);
        int32x4_t acc67 = vaddq_s32(acc6, acc7);

        int32_t acc_scalar0 = vaddvq_s32(acc01);
        int32_t acc_scalar1 = vaddvq_s32(acc23);
        int32_t acc_scalar2 = vaddvq_s32(acc45);
        int32_t acc_scalar3 = vaddvq_s32(acc67);
        if (blk != dims) {
            // scalar tail
            for (size_t t = blk; t < dims; t++) {
                const int8_t bb = b[t];
                acc_scalar0 += a0[t] * bb;
                acc_scalar1 += a1[t] * bb;
                acc_scalar2 += a2[t] * bb;
                acc_scalar3 += a3[t] * bb;
            }
        }
        results[c + 0] = (f32_t)acc_scalar0;
        results[c + 1] = (f32_t)acc_scalar1;
        results[c + 2] = (f32_t)acc_scalar2;
        results[c + 3] = (f32_t)acc_scalar3;
    }

    // Tail-handling: remaining 0..3 vectors
    for (; c < count; c++) {
        int8_t* a0 = a + mapper(c, offsets) * dims;
        results[c] = (f32_t)dot7u(a0, b, dims);
    }
}

static inline int identity(int i, int32_t* offsets) {
   return i;
}

static inline int index(int i, int32_t* offsets) {
   return offsets[i];
}

extern "C"
EXPORT void dot7u_bulk(int8_t* a, int8_t* b, int dims, int count, f32_t* results) {
    dot7u_inner_bulk<identity>(a, b, dims, NULL, count, results);
}

extern "C"
EXPORT void dot7u_bulk_offsets(int8_t* a, int8_t* b, int dims, int32_t* offsets, int count, f32_t* results) {
    dot7u_inner_bulk<index>(a, b, dims, offsets, count, results);
}

