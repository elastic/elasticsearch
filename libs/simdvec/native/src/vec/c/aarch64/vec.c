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
#include <math.h>
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

// --- single precision floats

// const float *a  pointer to the first float vector
// const float *b  pointer to the second float vector
// size_t elementCount  the number of floating point elements
EXPORT float dotf32(const float *a, const float *b, size_t elementCount) {
    float32x4_t sum0 = vdupq_n_f32(0.0f);
    float32x4_t sum1 = vdupq_n_f32(0.0f);
    float32x4_t sum2 = vdupq_n_f32(0.0f);
    float32x4_t sum3 = vdupq_n_f32(0.0f);
    float32x4_t sum4 = vdupq_n_f32(0.0f);
    float32x4_t sum5 = vdupq_n_f32(0.0f);
    float32x4_t sum6 = vdupq_n_f32(0.0f);
    float32x4_t sum7 = vdupq_n_f32(0.0f);

    size_t i = 0;
    // Each float32x4_t holds 4 floats, so unroll 8x = 32 floats per loop
    size_t unrolled_limit = elementCount & ~31UL;
    for (; i < unrolled_limit; i += 32) {
        sum0 = vfmaq_f32(sum0, vld1q_f32(a + i),      vld1q_f32(b + i));
        sum1 = vfmaq_f32(sum1, vld1q_f32(a + i + 4),  vld1q_f32(b + i + 4));
        sum2 = vfmaq_f32(sum2, vld1q_f32(a + i + 8),  vld1q_f32(b + i + 8));
        sum3 = vfmaq_f32(sum3, vld1q_f32(a + i + 12), vld1q_f32(b + i + 12));
        sum4 = vfmaq_f32(sum4, vld1q_f32(a + i + 16), vld1q_f32(b + i + 16));
        sum5 = vfmaq_f32(sum5, vld1q_f32(a + i + 20), vld1q_f32(b + i + 20));
        sum6 = vfmaq_f32(sum6, vld1q_f32(a + i + 24), vld1q_f32(b + i + 24));
        sum7 = vfmaq_f32(sum7, vld1q_f32(a + i + 28), vld1q_f32(b + i + 28));
    }

    float32x4_t total = vaddq_f32(
        vaddq_f32(vaddq_f32(sum0, sum1), vaddq_f32(sum2, sum3)),
        vaddq_f32(vaddq_f32(sum4, sum5), vaddq_f32(sum6, sum7))
    );
    float result = vaddvq_f32(total);

    // Handle remaining elements
    for (; i < elementCount; ++i) {
        result += a[i] * b[i];
    }

    return result;
}

// const float *a  pointer to the first float vector
// const float *b  pointer to the second float vector
// size_t elementCount  the number of floating point elements
EXPORT float cosf32(const float *a, const float *b, size_t elementCount) {
    float32x4_t sum0 = vdupq_n_f32(0.0f);
    float32x4_t sum1 = vdupq_n_f32(0.0f);
    float32x4_t sum2 = vdupq_n_f32(0.0f);
    float32x4_t sum3 = vdupq_n_f32(0.0f);

    float32x4_t norm_a0 = vdupq_n_f32(0.0f);
    float32x4_t norm_a1 = vdupq_n_f32(0.0f);
    float32x4_t norm_a2 = vdupq_n_f32(0.0f);
    float32x4_t norm_a3 = vdupq_n_f32(0.0f);

    float32x4_t norm_b0 = vdupq_n_f32(0.0f);
    float32x4_t norm_b1 = vdupq_n_f32(0.0f);
    float32x4_t norm_b2 = vdupq_n_f32(0.0f);
    float32x4_t norm_b3 = vdupq_n_f32(0.0f);

    size_t i = 0;
    // Each float32x4_t holds 4 floats, so unroll 4x = 16 floats per loop
    size_t unrolled_limit = elementCount & ~15UL;
    for (; i < unrolled_limit; i += 16) {
        float32x4_t va0 = vld1q_f32(a + i);
        float32x4_t vb0 = vld1q_f32(b + i);
        float32x4_t va1 = vld1q_f32(a + i + 4);
        float32x4_t vb1 = vld1q_f32(b + i + 4);
        float32x4_t va2 = vld1q_f32(a + i + 8);
        float32x4_t vb2 = vld1q_f32(b + i + 8);
        float32x4_t va3 = vld1q_f32(a + i + 12);
        float32x4_t vb3 = vld1q_f32(b + i + 12);

        // Dot products
        sum0 = vfmaq_f32(sum0, va0, vb0);
        sum1 = vfmaq_f32(sum1, va1, vb1);
        sum2 = vfmaq_f32(sum2, va2, vb2);
        sum3 = vfmaq_f32(sum3, va3, vb3);

        // Norms
        norm_a0 = vfmaq_f32(norm_a0, va0, va0);
        norm_a1 = vfmaq_f32(norm_a1, va1, va1);
        norm_a2 = vfmaq_f32(norm_a2, va2, va2);
        norm_a3 = vfmaq_f32(norm_a3, va3, va3);

        norm_b0 = vfmaq_f32(norm_b0, vb0, vb0);
        norm_b1 = vfmaq_f32(norm_b1, vb1, vb1);
        norm_b2 = vfmaq_f32(norm_b2, vb2, vb2);
        norm_b3 = vfmaq_f32(norm_b3, vb3, vb3);
    }

    // Combine accumulators
    float32x4_t sums = vaddq_f32(vaddq_f32(sum0, sum1), vaddq_f32(sum2, sum3));
    float32x4_t norms_a = vaddq_f32(vaddq_f32(norm_a0, norm_a1), vaddq_f32(norm_a2, norm_a3));
    float32x4_t norms_b = vaddq_f32(vaddq_f32(norm_b0, norm_b1), vaddq_f32(norm_b2, norm_b3));

    float dot   = vaddvq_f32(sums);
    float norm_a = vaddvq_f32(norms_a);
    float norm_b = vaddvq_f32(norms_b);

    // Handle remaining tail elements
    for (; i < elementCount; ++i) {
        float va = a[i];
        float vb = b[i];
        dot    += va * vb;
        norm_a += va * va;
        norm_b += vb * vb;
    }

    float denom = sqrtf(norm_a) * sqrtf(norm_b);
    if (denom == 0.0f) {
        return 0.0f;
    }
    return dot / denom;
}

EXPORT float sqrf32(const float *a, const float *b, size_t elementCount) {
    float32x4_t sum0 = vdupq_n_f32(0.0f);
    float32x4_t sum1 = vdupq_n_f32(0.0f);
    float32x4_t sum2 = vdupq_n_f32(0.0f);
    float32x4_t sum3 = vdupq_n_f32(0.0f);
    float32x4_t sum4 = vdupq_n_f32(0.0f);
    float32x4_t sum5 = vdupq_n_f32(0.0f);
    float32x4_t sum6 = vdupq_n_f32(0.0f);
    float32x4_t sum7 = vdupq_n_f32(0.0f);

    size_t i = 0;
    // Each float32x4_t holds 4 floats, so unroll 8x = 32 floats per loop
    size_t unrolled_limit = elementCount & ~31UL;
    for (; i < unrolled_limit; i += 32) {
        float32x4_t d0 = vsubq_f32(vld1q_f32(a + i),      vld1q_f32(b + i));
        float32x4_t d1 = vsubq_f32(vld1q_f32(a + i + 4),  vld1q_f32(b + i + 4));
        float32x4_t d2 = vsubq_f32(vld1q_f32(a + i + 8),  vld1q_f32(b + i + 8));
        float32x4_t d3 = vsubq_f32(vld1q_f32(a + i + 12), vld1q_f32(b + i + 12));
        float32x4_t d4 = vsubq_f32(vld1q_f32(a + i + 16), vld1q_f32(b + i + 16));
        float32x4_t d5 = vsubq_f32(vld1q_f32(a + i + 20), vld1q_f32(b + i + 20));
        float32x4_t d6 = vsubq_f32(vld1q_f32(a + i + 24), vld1q_f32(b + i + 24));
        float32x4_t d7 = vsubq_f32(vld1q_f32(a + i + 28), vld1q_f32(b + i + 28));

        sum0 = vmlaq_f32(sum0, d0, d0);
        sum1 = vmlaq_f32(sum1, d1, d1);
        sum2 = vmlaq_f32(sum2, d2, d2);
        sum3 = vmlaq_f32(sum3, d3, d3);
        sum4 = vmlaq_f32(sum4, d4, d4);
        sum5 = vmlaq_f32(sum5, d5, d5);
        sum6 = vmlaq_f32(sum6, d6, d6);
        sum7 = vmlaq_f32(sum7, d7, d7);
    }

    float32x4_t total = vaddq_f32(
        vaddq_f32(vaddq_f32(sum0, sum1), vaddq_f32(sum2, sum3)),
        vaddq_f32(vaddq_f32(sum4, sum5), vaddq_f32(sum6, sum7))
    );
    float result = vaddvq_f32(total);

    // Handle remaining tail elements
    for (; i < elementCount; ++i) {
        float diff = a[i] - b[i];
        result += diff * diff;
    }

    return result;
}
