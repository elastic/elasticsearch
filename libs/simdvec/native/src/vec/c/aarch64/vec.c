/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

#include <stddef.h>
#include <arm_neon.h>
#include "vec.h"

#ifndef DOT7U_STRIDE_BYTES_LEN
#define DOT7U_STRIDE_BYTES_LEN 32 // Must be a power of 2
#endif

#ifndef SQR7U_STRIDE_BYTES_LEN
#define SQR7U_STRIDE_BYTES_LEN 16 // Must be a power of 2
#endif

#ifdef __linux__
    #include <sys/auxv.h>
    #include <asm/hwcap.h>
    #ifndef HWCAP_NEON
    #define HWCAP_NEON 0x1000
    #endif
#endif

#ifdef __APPLE__
#include <TargetConditionals.h>
#endif

EXPORT int vec_caps() {
#ifdef __APPLE__
    #ifdef TARGET_OS_OSX
        // All M series Apple silicon support Neon instructions
        return 1;
    #else
        #error "Unsupported Apple platform"
    #endif
#elif __linux__
    int hwcap = getauxval(AT_HWCAP);
    return (hwcap & HWCAP_NEON) != 0;
#else
    #error "Unsupported aarch64 platform"
#endif
}

static inline int32_t dot7u_inner(int8_t* a, int8_t* b, size_t dims) {
    // We have contention in the instruction pipeline on the accumulation
    // registers if we use too few.
    int32x4_t acc1 = vdupq_n_s32(0);
    int32x4_t acc2 = vdupq_n_s32(0);
    int32x4_t acc3 = vdupq_n_s32(0);
    int32x4_t acc4 = vdupq_n_s32(0);

    // Some unrolling gives around 50% performance improvement.
    for (int i = 0; i < dims; i += DOT7U_STRIDE_BYTES_LEN) {
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

EXPORT int32_t dot7u(int8_t* a, int8_t* b, size_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims > DOT7U_STRIDE_BYTES_LEN) {
        i += dims & ~(DOT7U_STRIDE_BYTES_LEN - 1);
        res = dot7u_inner(a, b, i);
    }
    for (; i < dims; i++) {
        res += a[i] * b[i];
    }
    return res;
}

static inline int32_t sqr7u_inner(int8_t *a, int8_t *b, size_t dims) {
    int32x4_t acc1 = vdupq_n_s32(0);
    int32x4_t acc2 = vdupq_n_s32(0);
    int32x4_t acc3 = vdupq_n_s32(0);
    int32x4_t acc4 = vdupq_n_s32(0);

    for (int i = 0; i < dims; i += SQR7U_STRIDE_BYTES_LEN) {
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

EXPORT int32_t sqr7u(int8_t* a, int8_t* b, size_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims > SQR7U_STRIDE_BYTES_LEN) {
        i += dims & ~(SQR7U_STRIDE_BYTES_LEN - 1);
        res = sqr7u_inner(a, b, i);
    }
    for (; i < dims; i++) {
        int32_t dist = a[i] - b[i];
        res += dist * dist;
    }
    return res;
}
