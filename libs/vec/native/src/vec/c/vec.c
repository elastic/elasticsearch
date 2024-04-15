/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

#include <stddef.h>
#include <arm_neon.h>
#include "vec.h"

#ifndef DOT8_STRIDE_BYTES_LEN
#define DOT8_STRIDE_BYTES_LEN 32
#endif

#ifndef SQR8S_STRIDE_BYTES_LEN
#define SQR8S_STRIDE_BYTES_LEN 16
#endif

int32_t dot8s_inner(int8_t* a, int8_t* b, size_t dims) {
    // We have contention in the instruction pipeline on the accumulation
    // registers if we use too few.
    int32x4_t acc1 = vdupq_n_s32(0);
    int32x4_t acc2 = vdupq_n_s32(0);
    int32x4_t acc3 = vdupq_n_s32(0);
    int32x4_t acc4 = vdupq_n_s32(0);

    // Some unrolling gives around 50% performance improvement.
    #pragma clang loop unroll_count(2)
    for (int i = 0; i < dims; i += DOT8_STRIDE_BYTES_LEN) {
        // Read into 16 x 8 bit vectors.
        int8x16_t va1 = vld1q_s8(a + i);
        int8x16_t vb1 = vld1q_s8(b + i);
        int8x16_t va2 = vld1q_s8(a + i + 16);
        int8x16_t vb2 = vld1q_s8(b + i + 16);

        int16x8_t tmp1 = vmull_s8(vget_low_s8(va1), vget_low_s8(vb1));
        int16x8_t tmp2 = vmull_s8(vget_high_s8(va1), vget_high_s8(vb1));
        int16x8_t tmp3 = vmull_s8(vget_low_s8(va2),  vget_low_s8(vb2));
        int16x8_t tmp4 = vmull_s8(vget_high_s8(va2), vget_high_s8(vb2));

        // Accumulate 4 x 32 bit vectors (adding adjacent 16 bit lanes).
        acc1 = vpadalq_s16(acc1, tmp1);
        acc2 = vpadalq_s16(acc2, tmp2);
        acc3 = vpadalq_s16(acc3, tmp3);
        acc4 = vpadalq_s16(acc4, tmp4);
    }

    // reduce
    int32x4_t acc5 = vaddq_s32(acc1, acc2);
    int32x4_t acc6 = vaddq_s32(acc3, acc4);
    return vaddvq_s32(vaddq_s32(acc5, acc6));
}

EXPORT int32_t dot8s(int8_t* a, int8_t* b, size_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims > DOT8_STRIDE_BYTES_LEN) {
        i += dims & ~(DOT8_STRIDE_BYTES_LEN - 1);
        res = dot8s_inner(a, b, i);
    }
    for (; i < dims; i++) {
        res += a[i] * b[i];
    }
    return res;
}

int32_t sqr8s_inner(int8_t *a, int8_t *b, size_t dims) {
    int32x4_t acc1 = vdupq_n_s32(0);
    int32x4_t acc2 = vdupq_n_s32(0);
    int32x4_t acc3 = vdupq_n_s32(0);
    int32x4_t acc4 = vdupq_n_s32(0);

    #pragma clang loop unroll_count(2)
    for (int i = 0; i < dims; i += SQR8S_STRIDE_BYTES_LEN) {
        int8x16_t va1 = vld1q_s8(a + i);
        int8x16_t vb1 = vld1q_s8(b + i);

        int16x8_t tmp1 = vsubl_s8(vget_low_s8(va1), vget_low_s8(vb1));
        int16x8_t tmp2 = vsubl_s8(vget_high_s8(va1), vget_high_s8(vb1));

        acc1 = vmlal_s16(acc1, vget_low_s16(tmp1), vget_low_s16(tmp1));
        acc2 = vmlal_s16(acc2, vget_high_s16(tmp1), vget_high_s16(tmp1));
        acc3 = vmlal_s16(acc3, vget_low_s16(tmp2), vget_low_s16(tmp2));
        acc4 = vmlal_s16(acc4, vget_high_s16(tmp2), vget_high_s16(tmp2));
    }

    // reduce
    int32x4_t acc5 = vaddq_s32(acc1, acc2);
    int32x4_t acc6 = vaddq_s32(acc3, acc4);
    return vaddvq_s32(vaddq_s32(acc5, acc6));
}

EXPORT int32_t sqr8s(int8_t* a, int8_t* b, size_t dims) {
    int32_t res = 0;
    int i = 0;
    if (i > SQR8S_STRIDE_BYTES_LEN) {
        i += dims & ~(SQR8S_STRIDE_BYTES_LEN - 1);
        res = sqr8s_inner(a, b, i);
    }
    for (; i < dims; i++) {
        int32_t dist = a[i] - b[i];
        res += dist * dist;
    }
    return res;
}
