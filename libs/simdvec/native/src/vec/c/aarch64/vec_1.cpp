/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

 // This file contains implementations for basic vector processing functionalities,
 // including support for "1st tier" vector capabilities; in the case of ARM,
 // this first tier include functions for processors supporting at least the NEON
 // instruction set.

#include <stddef.h>
#include <arm_neon.h>
#include <math.h>
#include <algorithm>
#include "vec.h"
#include "vec_common.h"
#include "aarch64/aarch64_vec_common.h"

#ifndef DOTI8_STRIDE_BYTES_LEN
#define DOTI8_STRIDE_BYTES_LEN 32 // Must be a power of 2
#endif

#ifndef SQRI8_STRIDE_BYTES_LEN
#define SQRI8_STRIDE_BYTES_LEN 16 // Must be a power of 2
#endif

struct cosine_results_t {
    int32_t sum;
    int32_t norm1;
    int32_t norm2;
};

static inline cosine_results_t cosi8_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32x4_t acc_sum1 = vdupq_n_s32(0);
    int32x4_t acc_sum2 = vdupq_n_s32(0);
    int32x4_t acc_norm11 = vdupq_n_s32(0);
    int32x4_t acc_norm12 = vdupq_n_s32(0);
    int32x4_t acc_norm21 = vdupq_n_s32(0);
    int32x4_t acc_norm22 = vdupq_n_s32(0);

    for (int i = 0; i < dims; i += SQRI8_STRIDE_BYTES_LEN) {
        // Read into 16 x 8 bit vectors.
        int8x16_t va = vld1q_s8(a + i);
        int8x16_t vb = vld1q_s8(b + i);

        int16x8_t sum1 = vmull_s8(vget_low_s8(va), vget_low_s8(vb));
        int16x8_t sum2 = vmull_s8(vget_high_s8(va), vget_high_s8(vb));
        int16x8_t norm11 = vmull_s8(vget_low_s8(va), vget_low_s8(va));
        int16x8_t norm12 = vmull_s8(vget_high_s8(va), vget_high_s8(va));
        int16x8_t norm21 = vmull_s8(vget_low_s8(vb), vget_low_s8(vb));
        int16x8_t norm22 = vmull_s8(vget_high_s8(vb), vget_high_s8(vb));

        // Accumulate, adding adjacent 32-bit lanes
        acc_sum1 = vpadalq_s16(acc_sum1, sum1);
        acc_sum2 = vpadalq_s16(acc_sum2, sum2);
        acc_norm11 = vpadalq_s16(acc_norm11, norm11);
        acc_norm12 = vpadalq_s16(acc_norm12, norm12);
        acc_norm21 = vpadalq_s16(acc_norm21, norm21);
        acc_norm22 = vpadalq_s16(acc_norm22, norm22);
    }

    // reduce
    return cosine_results_t {
        vaddvq_s32(vaddq_s32(acc_sum1, acc_sum2)),
        vaddvq_s32(vaddq_s32(acc_norm11, acc_norm12)),
        vaddvq_s32(vaddq_s32(acc_norm21, acc_norm22))
    };
}

EXPORT f32_t vec_cosi8(const int8_t* a, const int8_t* b, const int32_t dims) {
    cosine_results_t res = cosine_results_t { 0, 0, 0 };
    int i = 0;
    if (dims > SQRI8_STRIDE_BYTES_LEN) {
        i += dims & ~(SQRI8_STRIDE_BYTES_LEN - 1);
        res = cosi8_inner(a, b, i);
    }
    for (; i < dims; i++) {
        int32_t ai = (int32_t) a[i];
        int32_t bi = (int32_t) b[i];
        res.sum += ai * bi;
        res.norm1 += ai * ai;
        res.norm2 += bi * bi;
    }

    return (f32_t) ((double) res.sum / sqrt((double) res.norm1 * res.norm2));
}

template <int64_t(*mapper)(const int32_t, const int32_t*)>
static inline void cosi8_inner_bulk(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    const int blk = dims & ~15;

    // First of all, calculate the b norm
    int32x4_t b_norms0 = vdupq_n_s32(0);
    int32x4_t b_norms1 = vdupq_n_s32(0);

    int bi = 0;
    for(; bi < blk; bi += 16) {
        int8x16_t vb8 = vld1q_s8(b + bi);

        int16x8_t lo = vmull_s8(vget_low_s8(vb8),  vget_low_s8(vb8));
        int16x8_t hi = vmull_s8(vget_high_s8(vb8), vget_high_s8(vb8));

        b_norms0 = vpadalq_s16(b_norms0, lo);
        b_norms1 = vpadalq_s16(b_norms1, hi);
    }
    int32_t b_norm = vaddvq_s32(b_norms0) + vaddvq_s32(b_norms1);
    for (; bi < dims; bi++) {
        b_norm += b[bi] * b[bi];
    }

    int c = 0;

    // Process 2 vectors at a time; this helps the CPU scheduler/prefetcher
    for (; c + 1 < count; c += 2) {
        const int8_t* a0 = a + mapper(c, offsets) * pitch;
        const int8_t* a1 = a + mapper(c + 1, offsets) * pitch;

        int32x4_t sums00 = vdupq_n_s32(0);
        int32x4_t sums01 = vdupq_n_s32(0);
        int32x4_t sums10 = vdupq_n_s32(0);
        int32x4_t sums11 = vdupq_n_s32(0);
        int32x4_t a_norms00 = vdupq_n_s32(0);
        int32x4_t a_norms01 = vdupq_n_s32(0);
        int32x4_t a_norms10 = vdupq_n_s32(0);
        int32x4_t a_norms11 = vdupq_n_s32(0);

        for (int i = 0; i < blk; i += 16) {
            int8x16_t vb = vld1q_s8(b + i);

            int8x16_t v0 = vld1q_s8(a0 + i);
            int16x8_t sum_lo0 = vmull_s8(vget_low_s8(v0), vget_low_s8(vb));
            int16x8_t sum_hi0 = vmull_s8(vget_high_s8(v0), vget_high_s8(vb));
            int16x8_t norm_lo0 = vmull_s8(vget_low_s8(v0), vget_low_s8(v0));
            int16x8_t norm_hi0 = vmull_s8(vget_high_s8(v0), vget_high_s8(v0));
            sums00 = vpadalq_s16(sums00, sum_lo0);
            sums01 = vpadalq_s16(sums01, sum_hi0);
            a_norms00 = vpadalq_s16(a_norms00, norm_lo0);
            a_norms01 = vpadalq_s16(a_norms01, norm_hi0);

            int8x16_t v1 = vld1q_s8(a1 + i);
            int16x8_t sum_lo1 = vmull_s8(vget_low_s8(v1), vget_low_s8(vb));
            int16x8_t sum_hi1 = vmull_s8(vget_high_s8(v1), vget_high_s8(vb));
            int16x8_t norm_lo1 = vmull_s8(vget_low_s8(v1), vget_low_s8(v1));
            int16x8_t norm_hi1 = vmull_s8(vget_high_s8(v1), vget_high_s8(v1));
            sums10 = vpadalq_s16(sums10, sum_lo1);
            sums11 = vpadalq_s16(sums11, sum_hi1);
            a_norms10 = vpadalq_s16(a_norms10, norm_lo1);
            a_norms11 = vpadalq_s16(a_norms11, norm_hi1);
        }
        int32x4_t sums0 = vaddq_s32(sums00, sums01);
        int32x4_t sums1 = vaddq_s32(sums10, sums11);
        int32x4_t a_norms0 = vaddq_s32(a_norms00, a_norms01);
        int32x4_t a_norms1 = vaddq_s32(a_norms10, a_norms11);

        int32_t sum0 = vaddvq_s32(sums0);
        int32_t sum1 = vaddvq_s32(sums1);
        int32_t norm0 = vaddvq_s32(a_norms0);
        int32_t norm1 = vaddvq_s32(a_norms1);
        if (blk != dims) {
            // scalar tail
            for (int t = blk; t < dims; t++) {
                int32_t a0i = (int32_t) a0[t];
                int32_t a1i = (int32_t) a1[t];
                int32_t bi = (int32_t) b[t];
                sum0 += a0i * bi;
                sum1 += a1i * bi;
                norm0 += a0i * a0i;
                norm1 += a1i * a1i;
            }
        }

        float32x2_t sum  = vcvt_f32_s32(vcreate_s32(((uint64_t)sum1 << 32) | (uint32_t)sum0));
        float32x2_t a_norm = vcvt_f32_s32(vcreate_s32(((uint64_t)norm1 << 32) | (uint32_t)norm0));
        float32x2_t b_norm_vec = vcvt_f32_s32(vdup_n_s32(b_norm));

        // sum / sqrt(a_norm * b_norm)
        float32x2_t res = vdiv_f32(sum, vsqrt_f32(vmul_f32(a_norm, b_norm_vec)));

        // store directly in results
        vst1_f32(results + c, res);
    }

    // Tail-handling: remaining vectors
    for (; c < count; c++) {
        const int8_t* a0 = a + mapper(c, offsets) * pitch;
        results[c] = vec_cosi8(a0, b, dims);
    }
}

EXPORT void vec_cosi8_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    cosi8_inner_bulk<identity_mapper>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_cosi8_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    cosi8_inner_bulk<array_mapper>(a, b, dims, pitch, offsets, count, results);
}

static inline int32_t doti8_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    // We have contention in the instruction pipeline on the accumulation
    // registers if we use too few.
    int32x4_t acc1 = vdupq_n_s32(0);
    int32x4_t acc2 = vdupq_n_s32(0);
    int32x4_t acc3 = vdupq_n_s32(0);
    int32x4_t acc4 = vdupq_n_s32(0);

    // Some unrolling gives around 50% performance improvement.
    for (int i = 0; i < dims; i += DOTI8_STRIDE_BYTES_LEN) {
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

static inline int32_t doti8_common(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims > DOTI8_STRIDE_BYTES_LEN) {
        i += dims & ~(DOTI8_STRIDE_BYTES_LEN - 1);
        res = doti8_inner(a, b, i);
    }
    for (; i < dims; i++) {
        res += a[i] * b[i];
    }
    return res;
}

EXPORT int32_t vec_doti7u(const int8_t* a, const int8_t* b, const int32_t dims) {
    return doti8_common(a, b, dims);
}

EXPORT f32_t vec_doti8(const int8_t* a, const int8_t* b, const int32_t dims) {
    return (f32_t)doti8_common(a, b, dims);
}

template <int64_t(*mapper)(const int32_t, const int32_t*)>
static inline void doti8_inner_bulk(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    const int blk = dims & ~15;
    int c = 0;

    // Process 4 vectors at a time; this helps the CPU scheduler/prefetcher.
    // Loading multiple memory locations while computing gives the prefetcher
    // information on where the data to load will be next, and keeps the CPU
    // execution units busy.
    // Our benchmarks show that this "hint" is more effective than using
    // explicit prefetch instructions (e.g. __builtin_prefetch) on many ARM
    // processors (e.g. Graviton)
    for (; c + 3 < count; c += 4) {
        const int8_t* a0 = a + mapper(c, offsets) * pitch;
        const int8_t* a1 = a + mapper(c + 1, offsets) * pitch;
        const int8_t* a2 = a + mapper(c + 2, offsets) * pitch;
        const int8_t* a3 = a + mapper(c + 3, offsets) * pitch;

        int32x4_t acc0 = vdupq_n_s32(0);
        int32x4_t acc1 = vdupq_n_s32(0);
        int32x4_t acc2 = vdupq_n_s32(0);
        int32x4_t acc3 = vdupq_n_s32(0);
        int32x4_t acc4 = vdupq_n_s32(0);
        int32x4_t acc5 = vdupq_n_s32(0);
        int32x4_t acc6 = vdupq_n_s32(0);
        int32x4_t acc7 = vdupq_n_s32(0);

        for (int i = 0; i < blk; i += 16) {
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
            for (int t = blk; t < dims; t++) {
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

    // Tail-handling: remaining vectors
    for (; c < count; c++) {
        const int8_t* a0 = a + mapper(c, offsets) * pitch;
        results[c] = (f32_t)vec_doti8(a0, b, dims);
    }
}

EXPORT void vec_doti7u_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    doti8_inner_bulk<identity_mapper>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_doti7u_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    doti8_inner_bulk<array_mapper>(a, b, dims, pitch, offsets, count, results);
}

EXPORT void vec_doti8_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    doti8_inner_bulk<identity_mapper>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_doti8_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    doti8_inner_bulk<array_mapper>(a, b, dims, pitch, offsets, count, results);
}

static inline int32_t sqri8_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32x4_t acc1 = vdupq_n_s32(0);
    int32x4_t acc2 = vdupq_n_s32(0);
    int32x4_t acc3 = vdupq_n_s32(0);
    int32x4_t acc4 = vdupq_n_s32(0);

    for (int i = 0; i < dims; i += SQRI8_STRIDE_BYTES_LEN) {
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

static inline int32_t sqri8_common(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims > SQRI8_STRIDE_BYTES_LEN) {
        i += dims & ~(SQRI8_STRIDE_BYTES_LEN - 1);
        res = sqri8_inner(a, b, i);
    }
    for (; i < dims; i++) {
        int32_t dist = a[i] - b[i];
        res += dist * dist;
    }
    return res;
}

EXPORT int32_t vec_sqri7u(const int8_t* a, const int8_t* b, const int32_t dims) {
    return sqri8_common(a, b, dims);
}

EXPORT f32_t vec_sqri8(const int8_t* a, const int8_t* b, const int32_t dims) {
    return (f32_t)sqri8_common(a, b, dims);
}

template <int64_t(*mapper)(const int32_t, const int32_t*)>
static inline void sqri8_inner_bulk(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    const int blk = dims & ~15;
    int c = 0;

    // Process 4 vectors at a time; this helps the CPU scheduler/prefetcher.
    // Loading multiple memory locations while computing gives the prefetcher
    // information on where the data to load will be next, and keeps the CPU
    // execution units busy.
    // Our benchmarks show that this "hint" is more effective than using
    // explicit prefetch instructions (e.g. __builtin_prefetch) on many ARM
    // processors (e.g. Graviton)
    for (; c + 3 < count; c += 4) {
        const int8_t* a0 = a + mapper(c, offsets) * pitch;
        const int8_t* a1 = a + mapper(c + 1, offsets) * pitch;
        const int8_t* a2 = a + mapper(c + 2, offsets) * pitch;
        const int8_t* a3 = a + mapper(c + 3, offsets) * pitch;

        int32x4_t acc0 = vdupq_n_s32(0);
        int32x4_t acc1 = vdupq_n_s32(0);
        int32x4_t acc2 = vdupq_n_s32(0);
        int32x4_t acc3 = vdupq_n_s32(0);
        int32x4_t acc4 = vdupq_n_s32(0);
        int32x4_t acc5 = vdupq_n_s32(0);
        int32x4_t acc6 = vdupq_n_s32(0);
        int32x4_t acc7 = vdupq_n_s32(0);

        for (int i = 0; i < blk; i += 16) {
            int8x16_t vb = vld1q_s8(b + i);

            int8x16_t v0 = vld1q_s8(a0 + i);
            int16x8_t d0_lo = vsubl_s8(vget_low_s8(v0),  vget_low_s8(vb));
            int16x8_t d0_hi = vsubl_s8(vget_high_s8(v0), vget_high_s8(vb));
            acc0 = vmlal_s16(acc0, vget_low_s16(d0_lo),  vget_low_s16(d0_lo));
            acc1 = vmlal_s16(acc1, vget_high_s16(d0_lo), vget_high_s16(d0_lo));
            acc0 = vmlal_s16(acc0, vget_low_s16(d0_hi),  vget_low_s16(d0_hi));
            acc1 = vmlal_s16(acc1, vget_high_s16(d0_hi), vget_high_s16(d0_hi));

            int8x16_t v1 = vld1q_s8(a1 + i);
            int16x8_t d1_lo = vsubl_s8(vget_low_s8(v1),  vget_low_s8(vb));
            int16x8_t d1_hi = vsubl_s8(vget_high_s8(v1), vget_high_s8(vb));
            acc2 = vmlal_s16(acc2, vget_low_s16(d1_lo),  vget_low_s16(d1_lo));
            acc3 = vmlal_s16(acc3, vget_high_s16(d1_lo), vget_high_s16(d1_lo));
            acc2 = vmlal_s16(acc2, vget_low_s16(d1_hi),  vget_low_s16(d1_hi));
            acc3 = vmlal_s16(acc3, vget_high_s16(d1_hi), vget_high_s16(d1_hi));

            int8x16_t v2 = vld1q_s8(a2 + i);
            int16x8_t d2_lo = vsubl_s8(vget_low_s8(v2),  vget_low_s8(vb));
            int16x8_t d2_hi = vsubl_s8(vget_high_s8(v2), vget_high_s8(vb));
            acc4 = vmlal_s16(acc4, vget_low_s16(d2_lo),  vget_low_s16(d2_lo));
            acc5 = vmlal_s16(acc5, vget_high_s16(d2_lo), vget_high_s16(d2_lo));
            acc4 = vmlal_s16(acc4, vget_low_s16(d2_hi),  vget_low_s16(d2_hi));
            acc5 = vmlal_s16(acc5, vget_high_s16(d2_hi), vget_high_s16(d2_hi));

            int8x16_t v3 = vld1q_s8(a3 + i);
            int16x8_t d3_lo = vsubl_s8(vget_low_s8(v3),  vget_low_s8(vb));
            int16x8_t d3_hi = vsubl_s8(vget_high_s8(v3), vget_high_s8(vb));
            acc6 = vmlal_s16(acc6, vget_low_s16(d3_lo),  vget_low_s16(d3_lo));
            acc7 = vmlal_s16(acc7, vget_high_s16(d3_lo), vget_high_s16(d3_lo));
            acc6 = vmlal_s16(acc6, vget_low_s16(d3_hi),  vget_low_s16(d3_hi));
            acc7 = vmlal_s16(acc7, vget_high_s16(d3_hi), vget_high_s16(d3_hi));
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
            for (int t = blk; t < dims; t++) {
                const int8_t bb = b[t];
                int32_t diff0 = a0[t] - bb;
                int32_t diff1 = a1[t] - bb;
                int32_t diff2 = a2[t] - bb;
                int32_t diff3 = a3[t] - bb;

                acc_scalar0 += diff0 * diff0;
                acc_scalar1 += diff1 * diff1;
                acc_scalar2 += diff2 * diff2;
                acc_scalar3 += diff3 * diff3;
            }
        }
        results[c + 0] = (f32_t)acc_scalar0;
        results[c + 1] = (f32_t)acc_scalar1;
        results[c + 2] = (f32_t)acc_scalar2;
        results[c + 3] = (f32_t)acc_scalar3;
    }

    // Tail-handling: remaining vectors
    for (; c < count; c++) {
        const int8_t* a0 = a + mapper(c, offsets) * pitch;
        results[c] = (f32_t)vec_sqri8(a0, b, dims);
    }
}

EXPORT void vec_sqri7u_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    sqri8_inner_bulk<identity_mapper>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqri7u_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    sqri8_inner_bulk<array_mapper>(a, b, dims, pitch, offsets, count, results);
}

EXPORT void vec_sqri8_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    sqri8_inner_bulk<identity_mapper>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqri8_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    sqri8_inner_bulk<array_mapper>(a, b, dims, pitch, offsets, count, results);
}

// --- single precision floats

/*
 * Float bulk operation. Iterates over 4 sequential vectors at a time.
 *
 * Template parameters:
 * mapper: gets the nth vector from the input array.
 * inner_op: SIMD per-dimension vector operation, takes sum, a, b, returns new sum
 * scalar_op: scalar per-dimension vector operation, takes a, b, returns sum
 * bulk_tail: complete vector comparison on a single vector
 *
 * This should compile to a single inline method, with no function callouts.
 */
template <
    int64_t(*mapper)(int32_t, const int32_t*),
    float32x4_t(*inner_op)(const float32x4_t, const float32x4_t, const float32x4_t),
    f32_t(*scalar_op)(const f32_t, const f32_t),
    f32_t(*bulk_tail)(const f32_t*, const f32_t*, const int32_t),
    int batches = 8
>
static inline void call_f32_bulk(
    const f32_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    const int vec_size = pitch / sizeof(f32_t);
    int c = 0;

    for (; c + batches - 1 < count; c += batches) {
        // Pointers to the current batch of input vectors, resolved via mapper.
        // as[0] points to the vector for index 0, [1] for index 1, etc
        const f32_t* as[batches];
        float32x4_t sums[batches];
        apply_indexed<batches>([&](auto I) {
            as[I] = a + mapper(c + I, offsets) * vec_size;
            sums[I] = vdupq_n_f32(0.0f);
        });

        int32_t i = 0;
        // do <batches> vectors at a time, iterating through the dimensions in parallel
        constexpr int stride = sizeof(float32x4_t) / sizeof(f32_t);
        for (; i < (dims & ~(stride - 1)); i += stride) {
            float32x4_t bi = vld1q_f32(b + i);
            apply_indexed<batches>([&](auto I) {
                sums[I] = inner_op(sums[I], vld1q_f32(as[I] + i), bi);
            });
        }

        f32_t res[batches];
        apply_indexed<batches>([&](auto I) {
            res[I] = vaddvq_f32(sums[I]);
        });

        // dimensions tail
        for (; i < dims; i++) {
            apply_indexed<batches>([&](auto I) {
                res[I] += scalar_op(as[I][i], b[i]);
            });
        }

        // this should be turned into direct value copies by the compiler
        std::copy_n(res, batches, results + c);
    }

    // Tail-handling: remaining vectors
    for (; c < count; c++) {
        const f32_t* a0 = a + mapper(c, offsets) * vec_size;
        results[c] = bulk_tail(a0, b, dims);
    }
}

// const f32_t* a  pointer to the first float vector
// const f32_t* b  pointer to the second float vector
// const int32_t elementCount  the number of floating point elements
EXPORT f32_t vec_dotf32(const f32_t* a, const f32_t* b, const int32_t elementCount) {
    constexpr int batches = 8;

    float32x4_t sums[batches];
    apply_indexed<batches>([&](auto I) {
        sums[I] = vdupq_n_f32(0.0f);
    });

    int i = 0;
    // each value has <elements> floats, and we iterate over <stride> floats at a time
    constexpr int elements = sizeof(float32x4_t) / sizeof(f32_t);
    constexpr int stride = sizeof(float32x4_t) / sizeof(f32_t) * batches;
    for (; i < (elementCount & ~(stride - 1)); i += stride) {
        apply_indexed<batches>([&](auto I) {
            sums[I] = vfmaq_f32(sums[I], vld1q_f32(a + i + I * elements), vld1q_f32(b + i + I * elements));
        });
    }

    float32x4_t total = vaddq_f32(
        vaddq_f32(vaddq_f32(sums[0], sums[1]), vaddq_f32(sums[2], sums[3])),
        vaddq_f32(vaddq_f32(sums[4], sums[5]), vaddq_f32(sums[6], sums[7]))
    );
    f32_t result = vaddvq_f32(total);

    // Handle remaining elements
    for (; i < elementCount; ++i) {
        result += dot_scalar(a[i], b[i]);
    }

    return result;
}

EXPORT void vec_dotf32_bulk(const f32_t* a, const f32_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_f32_bulk<identity_mapper, vfmaq_f32, dot_scalar<f32_t>, vec_dotf32>(a, b, dims, dims * sizeof(f32_t), NULL, count, results);
}

EXPORT void vec_dotf32_bulk_offsets(
    const f32_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_f32_bulk<array_mapper, vfmaq_f32, dot_scalar<f32_t>, vec_dotf32>(a, b, dims, pitch, offsets, count, results);
}

static inline float32x4_t sqrf32_vector(float32x4_t sum, float32x4_t a, float32x4_t b) {
    float32x4_t diff = vsubq_f32(a, b);
    return vmlaq_f32(sum, diff, diff);
}

EXPORT f32_t vec_sqrf32(const f32_t* a, const f32_t* b, const int32_t elementCount) {
    constexpr int batches = 8;

    float32x4_t sums[batches];
    apply_indexed<batches>([&](auto I) {
        sums[I] = vdupq_n_f32(0.0f);
    });

    int i = 0;
    // each value has <elements> floats, and we iterate over <stride> floats at a time
    constexpr int elements = sizeof(float32x4_t) / sizeof(f32_t);
    constexpr int stride = sizeof(float32x4_t) / sizeof(f32_t) * batches;
    for (; i < (elementCount & ~(stride - 1)); i += stride) {
        apply_indexed<batches>([&](auto I) {
            sums[I] = sqrf32_vector(sums[I], vld1q_f32(a + i + I * elements), vld1q_f32(b + i + I * elements));
        });
    }

    float32x4_t total = vaddq_f32(
        vaddq_f32(vaddq_f32(sums[0], sums[1]), vaddq_f32(sums[2], sums[3])),
        vaddq_f32(vaddq_f32(sums[4], sums[5]), vaddq_f32(sums[6], sums[7]))
    );
    f32_t result = vaddvq_f32(total);

    // Handle remaining elements
    for (; i < elementCount; ++i) {
        result += sqr_scalar(a[i], b[i]);
    }

    return result;
}

EXPORT void vec_sqrf32_bulk(const f32_t* a, const f32_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_f32_bulk<identity_mapper, sqrf32_vector, sqr_scalar, vec_sqrf32>(a, b, dims, dims * sizeof(f32_t), NULL, count, results);
}

EXPORT void vec_sqrf32_bulk_offsets(
    const f32_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_f32_bulk<array_mapper, sqrf32_vector, sqr_scalar, vec_sqrf32>(a, b, dims, pitch, offsets, count, results);
}

static inline int32_t reduce_u8x16_neon(uint8x16_t vec) {
    // Split the vector into two halves and widen to `uint16x8_t`
    uint16x8_t low_half = vmovl_u8(vget_low_u8(vec));   // widen lower 8 elements
    uint16x8_t high_half = vmovl_u8(vget_high_u8(vec)); // widen upper 8 elements

    // Sum the widened halves
    uint16x8_t sum16 = vaddq_u16(low_half, high_half);

    // Now reduce the `uint16x8_t` to a single `simsimd_u32_t`
    uint32x4_t sum32 = vpaddlq_u16(sum16);       // pairwise add into 32-bit integers
    uint64x2_t sum64 = vpaddlq_u32(sum32);       // pairwise add into 64-bit integers
    int32_t final_sum = vaddvq_u64(sum64);       // final horizontal add to 32-bit result
    return final_sum;
}

static inline int64_t dotd1q4_inner(const int8_t* a, const int8_t* query, const int32_t length) {
    int64_t subRet0 = 0;
    int64_t subRet1 = 0;
    int64_t subRet2 = 0;
    int64_t subRet3 = 0;
    int r = 0;

    constexpr int chunk_size = sizeof(uint64x2_t);

    const uint8_t* query_j0 = (const uint8_t*)query;
    const uint8_t* query_j1 = (const uint8_t*)query + length;
    const uint8_t* query_j2 = (const uint8_t*)query + 2 * length;
    const uint8_t* query_j3 = (const uint8_t*)query + 3 * length;

    if (length >= chunk_size) {
        uint64_t iters = length / chunk_size;
        uint8x16_t zero = vcombine_u8(vcreate_u8(0), vcreate_u8(0));

        for (int j = 0; j < iters;) {
            uint8x16_t qDot0 = zero;
            uint8x16_t qDot1 = zero;
            uint8x16_t qDot2 = zero;
            uint8x16_t qDot3 = zero;

            /*
            * After every 31 iterations we need to add the
            * temporary sums (qDot0, qDot1, qDot2, qDot3) to the total sum.
            * We must ensure that the temporary sums <= 255
            * and 31 * 8 bits = 248 which is OK.
            */
            uint64_t limit = (j + 31 < iters) ? j + 31 : iters;
            for (; j < limit; j++, r+= chunk_size)  {
                const uint8x16_t qv0 = vld1q_u8(query_j0 + r);
                const uint8x16_t qv1 = vld1q_u8(query_j1 + r);
                const uint8x16_t qv2 = vld1q_u8(query_j2 + r);
                const uint8x16_t qv3 = vld1q_u8(query_j3 + r);
                const uint8x16_t yv = vld1q_u8((const uint8_t*)a + r);

                qDot0 = vaddq_u8(qDot0, vcntq_u8(vandq_u8(qv0,yv)));
                qDot1 = vaddq_u8(qDot1, vcntq_u8(vandq_u8(qv1,yv)));
                qDot2 = vaddq_u8(qDot2, vcntq_u8(vandq_u8(qv2,yv)));
                qDot3 = vaddq_u8(qDot3, vcntq_u8(vandq_u8(qv3,yv)));
            }

            subRet0 += reduce_u8x16_neon(qDot0);
            subRet1 += reduce_u8x16_neon(qDot1);
            subRet2 += reduce_u8x16_neon(qDot2);
            subRet3 += reduce_u8x16_neon(qDot3);
        }
    }

    int upperBound = length & ~(sizeof(int64_t) - 1);
    for (; r < upperBound; r += sizeof(int64_t)) {
        int64_t value = *((int64_t*)(a + r));
        int64_t q0 = *((int64_t*)(query + r));
        subRet0 += __builtin_popcountll(q0 & value);
        int64_t q1 = *((int64_t*)(query + r + length));
        subRet1 += __builtin_popcountll(q1 & value);
        int64_t q2 = *((int64_t*)(query + r + 2 * length));
        subRet2 += __builtin_popcountll(q2 & value);
        int64_t q3 = *((int64_t*)(query + r + 3 * length));
        subRet3 += __builtin_popcountll(q3 & value);
    }
    upperBound = length & ~(sizeof(int32_t) - 1);
    for (; r < upperBound; r += sizeof(int32_t)) {
        int32_t value = *((int32_t*)(a + r));
        int32_t q0 = *((int32_t*)(query + r));
        subRet0 += __builtin_popcount(q0 & value);
        int32_t q1 = *((int32_t*)(query + r + length));
        subRet1 += __builtin_popcount(q1 & value);
        int32_t q2 = *((int32_t*)(query + r + 2 * length));
        subRet2 += __builtin_popcount(q2 & value);
        int32_t q3 = *((int32_t*)(query + r + 3 * length));
        subRet3 += __builtin_popcount(q3 & value);
    }
    for (; r < length; r++) {
        int8_t value = *(a + r);
        int8_t q0 = *(query + r);
        subRet0 += __builtin_popcount(q0 & value & 0xFF);
        int8_t q1 = *(query + r + length);
        subRet1 += __builtin_popcount(q1 & value & 0xFF);
        int8_t q2 = *(query + r + 2 * length);
        subRet2 += __builtin_popcount(q2 & value & 0xFF);
        int8_t q3 = *(query + r + 3 * length);
        subRet3 += __builtin_popcount(q3 & value & 0xFF);
    }
    return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
}

EXPORT int64_t vec_dotd1q4(const int8_t* a, const int8_t* query, const int32_t length) {
    return dotd1q4_inner(a, query, length);
}

template <int64_t(*mapper)(const int32_t, const int32_t*)>
static inline void dotd1q4_inner_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {

    constexpr int chunk_size = sizeof(uint64x2_t);

    const uint8_t* query_j0 = (const uint8_t*)query;
    const uint8_t* query_j1 = (const uint8_t*)query + length;
    const uint8_t* query_j2 = (const uint8_t*)query + 2 * length;
    const uint8_t* query_j3 = (const uint8_t*)query + 3 * length;

    const int iters = length / chunk_size;
    const uint8x16_t zero = vcombine_u8(vcreate_u8(0), vcreate_u8(0));

    int c = 0;

    for (; c + 1 < count; c += 2) {
        const uint8_t* a0 = (const uint8_t*)a + mapper(c, offsets) * pitch;
        const uint8_t* a1 = (const uint8_t*)a + mapper(c + 1, offsets) * pitch;

        int64_t subRet0_0 = 0;
        int64_t subRet1_0 = 0;
        int64_t subRet2_0 = 0;
        int64_t subRet3_0 = 0;

        int64_t subRet0_1 = 0;
        int64_t subRet1_1 = 0;
        int64_t subRet2_1 = 0;
        int64_t subRet3_1 = 0;

        int r = 0;

        if (length >= chunk_size) {
            for (int j = 0; j < iters;) {
                uint8x16_t qDot0_0 = zero;
                uint8x16_t qDot1_0 = zero;
                uint8x16_t qDot2_0 = zero;
                uint8x16_t qDot3_0 = zero;

                uint8x16_t qDot0_1 = zero;
                uint8x16_t qDot1_1 = zero;
                uint8x16_t qDot2_1 = zero;
                uint8x16_t qDot3_1 = zero;

                /*
                * After every 31 iterations we need to add the
                * temporary sums (qDot0, qDot1, qDot2, qDot3) to the total sum.
                * We must ensure that the temporary sums <= 255
                * and 31 * 8 bits = 248 which is OK.
                */
                uint64_t limit = (j + 31 < iters) ? j + 31 : iters;
                for (; j < limit; j++, r+= chunk_size)  {
                    const uint8x16_t qv0 = vld1q_u8(query_j0 + r);
                    const uint8x16_t qv1 = vld1q_u8(query_j1 + r);
                    const uint8x16_t qv2 = vld1q_u8(query_j2 + r);
                    const uint8x16_t qv3 = vld1q_u8(query_j3 + r);

                    const uint8x16_t yv0 = vld1q_u8((const uint8_t*)a0 + r);
                    const uint8x16_t yv1 = vld1q_u8((const uint8_t*)a1 + r);

                    qDot0_0 = vaddq_u8(qDot0_0, vcntq_u8(vandq_u8(qv0,yv0)));
                    qDot1_0 = vaddq_u8(qDot1_0, vcntq_u8(vandq_u8(qv1,yv0)));
                    qDot2_0 = vaddq_u8(qDot2_0, vcntq_u8(vandq_u8(qv2,yv0)));
                    qDot3_0 = vaddq_u8(qDot3_0, vcntq_u8(vandq_u8(qv3,yv0)));

                    qDot0_1 = vaddq_u8(qDot0_1, vcntq_u8(vandq_u8(qv0,yv1)));
                    qDot1_1 = vaddq_u8(qDot1_1, vcntq_u8(vandq_u8(qv1,yv1)));
                    qDot2_1 = vaddq_u8(qDot2_1, vcntq_u8(vandq_u8(qv2,yv1)));
                    qDot3_1 = vaddq_u8(qDot3_1, vcntq_u8(vandq_u8(qv3,yv1)));
                }

                subRet0_0 += reduce_u8x16_neon(qDot0_0);
                subRet1_0 += reduce_u8x16_neon(qDot1_0);
                subRet2_0 += reduce_u8x16_neon(qDot2_0);
                subRet3_0 += reduce_u8x16_neon(qDot3_0);

                subRet0_1 += reduce_u8x16_neon(qDot0_1);
                subRet1_1 += reduce_u8x16_neon(qDot1_1);
                subRet2_1 += reduce_u8x16_neon(qDot2_1);
                subRet3_1 += reduce_u8x16_neon(qDot3_1);
            }
        }

        for (; r < length; r++) {
            int64_t v0 = *((int64_t*)(a0 + r));
            int64_t v1 = *((int64_t*)(a1 + r));

            int64_t q0 = *((int64_t*)(query_j0 + r));
            int64_t q1 = *((int64_t*)(query_j1 + r));
            int64_t q2 = *((int64_t*)(query_j2 + r));
            int64_t q3 = *((int64_t*)(query_j3 + r));

            subRet0_0 += __builtin_popcount(q0 & v0 & 0xFF);
            subRet1_0 += __builtin_popcount(q1 & v0 & 0xFF);
            subRet2_0 += __builtin_popcount(q2 & v0 & 0xFF);
            subRet3_0 += __builtin_popcount(q3 & v0 & 0xFF);

            subRet0_1 += __builtin_popcount(q0 & v1 & 0xFF);
            subRet1_1 += __builtin_popcount(q1 & v1 & 0xFF);
            subRet2_1 += __builtin_popcount(q2 & v1 & 0xFF);
            subRet3_1 += __builtin_popcount(q3 & v1 & 0xFF);
        }
        results[c] = subRet0_0 + (subRet1_0 << 1) + (subRet2_0 << 2) + (subRet3_0 << 3);
        results[c + 1] = subRet0_1 + (subRet1_1 << 1) + (subRet2_1 << 2) + (subRet3_1 << 3);
    }

    for (; c < count; c++) {
        const int8_t* a0 = a + mapper(c, offsets) * pitch;
        results[c] = (f32_t)dotd1q4_inner(a0, query, length);
    }
}

EXPORT void vec_dotd1q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1q4_inner_bulk<identity_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd1q4_bulk_offsets(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dotd1q4_inner_bulk<array_mapper>(a, query, length, pitch, offsets, count, results);
}

EXPORT int64_t vec_dotd2q4(
    const int8_t* a,
    const int8_t* query,
    const int32_t length
) {
    int64_t lower = dotd1q4_inner(a, query, length/2);
    int64_t upper = dotd1q4_inner(a + length/2, query, length/2);
    return lower + (upper << 1);
}

template <int64_t(*mapper)(const int32_t, const int32_t*)>
static inline void dotd2q4_inner_bulk(
    const int8_t* a,
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
        const int8_t* a0 = a + mapper(c, offsets) * pitch;
        int64_t lower = dotd1q4_inner(a0, query, bit_length);
        int64_t upper = dotd1q4_inner(a0 + bit_length, query, bit_length);
        results[c] = (f32_t)(lower + (upper << 1));
    }
}

EXPORT void vec_dotd2q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<identity_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd2q4_bulk_offsets(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<array_mapper>(a, query, length, pitch, offsets, count, results);
}

EXPORT int64_t vec_dotd4q4(const int8_t* a, const int8_t* query, const int32_t length) {
    const int32_t bit_length = length / 4;
    int64_t p0 = dotd1q4_inner(a + 0 * bit_length, query, bit_length);
    int64_t p1 = dotd1q4_inner(a + 1 * bit_length, query, bit_length);
    int64_t p2 = dotd1q4_inner(a + 2 * bit_length, query, bit_length);
    int64_t p3 = dotd1q4_inner(a + 3 * bit_length, query, bit_length);
    return p0 + (p1 << 1) + (p2 << 2) + (p3 << 3);
}

template <int64_t(*mapper)(const int32_t, const int32_t*)>
static inline void dotd4q4_inner_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    const int32_t bit_length = length / 4;

    for (int c = 0; c < count; c++) {
        const int8_t* a0 = a + mapper(c, offsets) * pitch;

        int64_t p0 = dotd1q4_inner(a0 + 0 * bit_length, query, bit_length);
        int64_t p1 = dotd1q4_inner(a0 + 1 * bit_length, query, bit_length);
        int64_t p2 = dotd1q4_inner(a0 + 2 * bit_length, query, bit_length);
        int64_t p3 = dotd1q4_inner(a0 + 3 * bit_length, query, bit_length);

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
    dotd4q4_inner_bulk<identity_mapper>(a, query, length, length, NULL, count, results);
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
    dotd4q4_inner_bulk<array_mapper>(a, query, length, pitch, offsets, count, results);
}
