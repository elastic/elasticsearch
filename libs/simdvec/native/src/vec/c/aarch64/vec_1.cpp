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

struct cosine_results_t {
    int32_t sum;
    int32_t norm1;
    int32_t norm2;
};

// Several ARM operations split the input vector into two vectors,
// the low and high needs to be processed separately.
// These helper methods are to use the ARM x2 types
// to still treat then as single values

template <int16x8_t(*op)(const int8x8_t, const int8x8_t)>
static inline int16x8x2_t create_pair(int8x16_t a, int8x16_t b) {
    int16x8x2_t ret;
    ret.val[0] = op(vget_low_s8(a), vget_low_s8(b));
    ret.val[1] = op(vget_high_s8(a), vget_high_s8(b));
    return ret;
}

template <int32x4_t(*op)(const int32x4_t, const int16x8_t)>
static inline int32x4x2_t apply(int32x4x2_t a, int16x8x2_t b) {
    int32x4x2_t ret;
    ret.val[0] = op(a.val[0], b.val[0]);
    ret.val[1] = op(a.val[1], b.val[1]);
    return ret;
}

template <int32x4_t(*op)(const int32x4_t, const int32x4_t)>
static inline int32x4_t combine(int32x4x2_t a) {
    return op(a.val[0], a.val[1]);
}

static inline cosine_results_t cosi8_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32x4x2_t zero = { .val = { vdupq_n_s32(0), vdupq_n_s32(0) } };
    int32x4x2_t sums = zero;
    int32x4x2_t a_norms = zero;
    int32x4x2_t b_norms = zero;

    constexpr int stride = sizeof(int8x16_t);
    for (int i = 0; i < dims; i += stride) {
        // Read into 16 x 8 bit vectors.
        int8x16_t va = vld1q_s8(a + i);
        int8x16_t vb = vld1q_s8(b + i);

        int16x8x2_t sum = create_pair<vmull_s8>(va, vb);
        int16x8x2_t a_norm = create_pair<vmull_s8>(va, va);
        int16x8x2_t b_norm = create_pair<vmull_s8>(vb, vb);

        // Accumulate, adding adjacent 32-bit lanes
        sums = apply<vpadalq_s16>(sums, sum);
        a_norms = apply<vpadalq_s16>(a_norms, a_norm);
        b_norms = apply<vpadalq_s16>(b_norms, b_norm);
    }

    // reduce
    return cosine_results_t {
        vaddvq_s32(combine<vaddq_s32>(sums)),
        vaddvq_s32(combine<vaddq_s32>(a_norms)),
        vaddvq_s32(combine<vaddq_s32>(b_norms))
    };
}

EXPORT f32_t vec_cosi8(const int8_t* a, const int8_t* b, const int32_t dims) {
    cosine_results_t res = cosine_results_t { 0, 0, 0 };
    int i = 0;
    if (dims > sizeof(int8x16_t)) {
        i += dims & ~(sizeof(int8x16_t) - 1);
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
    constexpr int batches = 2;

    // First of all, calculate the b norm
    int32x4x2_t b_norms = { .val = { vdupq_n_s32(0), vdupq_n_s32(0) } };

    int bi = 0;
    constexpr int stride = sizeof(int8x16_t);
    for(; bi < (dims & ~(stride - 1)); bi += stride) {
        int8x16_t vb8 = vld1q_s8(b + bi);
        int16x8x2_t norms = create_pair<vmull_s8>(vb8, vb8);
        b_norms = apply<vpadalq_s16>(b_norms, norms);
    }
    int32_t b_norm = vaddvq_s32(b_norms.val[0]) + vaddvq_s32(b_norms.val[1]);
    for (; bi < dims; bi++) {
        b_norm += b[bi] * b[bi];
    }

    int c = 0;

    // Process <batches> vectors at a time; this helps the CPU scheduler/prefetcher
    for (; c + batches - 1 < count; c += batches) {
        const int8_t* as[batches];
        int32x4x2_t sums[batches];
        int32x4x2_t a_norms[batches];
        apply_indexed<batches>([&](auto I) {
            as[I] = a + mapper(c + I, offsets) * pitch;
            sums[I] = { .val = { vdupq_n_s32(0), vdupq_n_s32(0) } };
            a_norms[I] = { .val = { vdupq_n_s32(0), vdupq_n_s32(0) } };
        });

        int i=0;
        for (; i < (dims & ~(stride - 1)); i += stride) {
            int8x16_t vb = vld1q_s8(b + i);

            apply_indexed<batches>([&](auto I) {
                int8x16_t va = vld1q_s8(as[I] + i);

                int16x8x2_t sum = create_pair<vmull_s8>(va, vb);
                int16x8x2_t a_norm = create_pair<vmull_s8>(va, va);
                sums[I] = apply<vpadalq_s16>(sums[I], sum);
                a_norms[I] = apply<vpadalq_s16>(a_norms[I], a_norm);
            });
        }

        int32_t sum[batches];
        int32_t a_norm[batches];
        apply_indexed<batches>([&](auto I) {
            sum[I] = vaddvq_s32(combine<vaddq_s32>(sums[I]));
            a_norm[I] = vaddvq_s32(combine<vaddq_s32>(a_norms[I]));
        });
        // scalar tail
        for (; i < dims; i++) {
            int32_t bi = (int32_t) b[i];
            apply_indexed<batches>([&](auto I) {
                sum[I] += as[I][i] * bi;
                a_norm[I] += as[I][i] * as[I][i];
            });
        }

        float32x2_t sum_vec = vcvt_f32_s32(vcreate_s32(((uint64_t)sum[1] << 32) | (uint32_t)sum[0]));
        float32x2_t a_norm_vec = vcvt_f32_s32(vcreate_s32(((uint64_t)a_norm[1] << 32) | (uint32_t)a_norm[0]));
        float32x2_t b_norm_vec = vcvt_f32_s32(vdup_n_s32(b_norm));

        // sum / sqrt(a_norm * b_norm)
        float32x2_t res = vdiv_f32(sum_vec, vsqrt_f32(vmul_f32(a_norm_vec, b_norm_vec)));

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
    int32x4x2_t acc0 = { .val = { vdupq_n_s32(0), vdupq_n_s32(0) } };
    int32x4x2_t acc1 = { .val = { vdupq_n_s32(0), vdupq_n_s32(0) } };

    // Some unrolling gives around 50% performance improvement.
    constexpr int stride = sizeof(int8x16x2_t);
    for (int i = 0; i < dims; i += stride) {
        // Read into 16 x 8 bit vectors.
        int8x16x2_t va = vld1q_s8_x2(a + i);
        int8x16x2_t vb = vld1q_s8_x2(b + i);

        int16x8x2_t dot0 = create_pair<vmull_s8>(va.val[0], vb.val[0]);
        int16x8x2_t dot1 = create_pair<vmull_s8>(va.val[1], vb.val[1]);

        // Accumulate 4 x 32 bit vectors (adding adjacent 16 bit lanes).
        acc0 = apply<vpadalq_s16>(acc0, dot0);
        acc1 = apply<vpadalq_s16>(acc1, dot1);
    }

    // reduce
    return vaddvq_s32(vaddq_s32(combine<vaddq_s32>(acc0), combine<vaddq_s32>(acc1)));
}

static inline int32_t doti8_common(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims > sizeof(int8x16x2_t)) {
        i += dims & ~(sizeof(int8x16x2_t) - 1);
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

/*
 * Byte bulk operation. Iterates over 4 sequential vectors at a time.
 *
 * Because ARM intrinsics handle arithmetic overflow by calculating half a vector
 * at a time, this multiplies all our resulting accumulators by 2.
 * This is what the x2 types are for. This does make the signatures of some of the ops
 * a bit fiddly.
 *
 * Template parameters:
 * mapper: gets the nth vector from the input array.
 * inner_op: SIMD vectorised comparison operation, sum, a, b, returns new sum
 * scalar_op: scalar per-dimension vector operation, takes a, b, returns sum
 * bulk_tail: complete vector comparison on a single vector
 *
 * This should compile to a single inline method, with no function callouts.
 */
template <
    int64_t(*mapper)(const int32_t, const int32_t*),
    int32x4x2_t(*inner_op)(const int32x4x2_t, const int8x16_t, const int8x16_t),
    int32_t(*scalar_op)(const int8_t, const int8_t),
    f32_t(*bulk_tail)(const int8_t*, const int8_t*, const int32_t),
    int batches = 4>
static inline void call_i8_bulk(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    constexpr int stride = sizeof(int8x16_t);
    const int blk = dims & ~(stride - 1);
    int c = 0;

    // Process <batches> vectors at a time; this helps the CPU scheduler/prefetcher.
    // Loading multiple memory locations while computing gives the prefetcher
    // information on where the data to load will be next, and keeps the CPU
    // execution units busy.
    // Our benchmarks show that this "hint" is more effective than using
    // explicit prefetch instructions (e.g. __builtin_prefetch) on many ARM
    // processors (e.g. Graviton)
    for (; c + batches - 1 < count; c += batches) {
        const int8_t* as[batches];
        int32x4x2_t acc[batches];
        apply_indexed<batches>([&](auto I) {
            as[I] = a + mapper(c + I, offsets) * pitch;
            acc[I] = { .val = { vdupq_n_s32(0), vdupq_n_s32(0) } };
        });

        int i=0;
        for (; i < blk; i += stride) {
            int8x16_t vb = vld1q_s8(b + i);

            apply_indexed<batches>([&](auto I) {
                int8x16_t va = vld1q_s8(as[I] + i);
                acc[I] = inner_op(acc[I], va, vb);
            });
        }

        int32_t res[batches];
        apply_indexed<batches>([&](auto I) {
            res[I] = vaddvq_s32(combine<vaddq_s32>(acc[I]));
        });
        // scalar tail
        for (; i < dims; i++) {
            const int8_t bb = b[i];
            apply_indexed<batches>([&](auto I) {
                res[I] += scalar_op(as[I][i], bb);
            });
        }

        apply_indexed<batches>([&](auto I) {
            results[c + I] = (f32_t)res[I];
        });
    }

    // Tail-handling: remaining vectors
    for (; c < count; c++) {
        const int8_t* a0 = a + mapper(c, offsets) * pitch;
        results[c] = (f32_t)bulk_tail(a0, b, dims);
    }
}

static inline int32x4x2_t doti8_vector(const int32x4x2_t acc, const int8x16_t va, const int8x16_t vb) {
    // a * b, creating a int16x8 for the low/high 8 bytes of each int8x16
    int16x8x2_t vals = create_pair<vmull_s8>(va, vb);
    // accumulate the int16x8x2 into int32x4x2s, adding pairwise
    return apply<vpadalq_s16>(acc, vals);
}

EXPORT void vec_doti7u_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<identity_mapper, doti8_vector, dot_scalar<int8_t>, vec_doti8>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_doti7u_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<array_mapper, doti8_vector, dot_scalar<int8_t>, vec_doti8>(a, b, dims, pitch, offsets, count, results);
}

EXPORT void vec_doti8_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<identity_mapper, doti8_vector, dot_scalar<int8_t>, vec_doti8>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_doti8_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<array_mapper, doti8_vector, dot_scalar<int8_t>, vec_doti8>(a, b, dims, pitch, offsets, count, results);
}

static inline int32x4x2_t sqri8_vector_acc(const int32x4x2_t acc, const int16x8_t diff) {
    // acc += diff * diff
    int32x4x2_t ret;
    ret.val[0] = vmlal_s16(acc.val[0], vget_low_s16(diff), vget_low_s16(diff));
    ret.val[1] = vmlal_s16(acc.val[1], vget_high_s16(diff), vget_high_s16(diff));
    return ret;
}

static inline int32_t sqri8_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32x4x2_t acc0 = { .val = { vdupq_n_s32(0), vdupq_n_s32(0) } };
    int32x4x2_t acc1 = { .val = { vdupq_n_s32(0), vdupq_n_s32(0) } };

    constexpr int stride = sizeof(int8x16_t);
    for (int i = 0; i < dims; i += stride) {
        int8x16_t va = vld1q_s8(a + i);
        int8x16_t vb = vld1q_s8(b + i);

        int16x8x2_t diff = create_pair<vsubl_s8>(va, vb);

        acc0 = sqri8_vector_acc(acc0, diff.val[0]);
        acc1 = sqri8_vector_acc(acc1, diff.val[1]);
    }

    // reduce
    return vaddvq_s32(vaddq_s32(combine<vaddq_s32>(acc0), combine<vaddq_s32>(acc1)));
}

static inline int32_t sqri8_common(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims > sizeof(int8x16_t)) {
        i += dims & ~(sizeof(int8x16_t) - 1);
        res = sqri8_inner(a, b, i);
    }
    for (; i < dims; i++) {
        res += sqr_scalar(a[i], b[i]);
    }
    return res;
}

EXPORT int32_t vec_sqri7u(const int8_t* a, const int8_t* b, const int32_t dims) {
    return sqri8_common(a, b, dims);
}

EXPORT f32_t vec_sqri8(const int8_t* a, const int8_t* b, const int32_t dims) {
    return (f32_t)sqri8_common(a, b, dims);
}

static inline int32x4_t sqri8_vector_combine(const int32x4_t acc, const int16x8_t diff) {
    // acc += diff * diff, handling ARM explosion of vector values after multiplication
    int32x4_t ret = vmlal_s16(acc, vget_low_s16(diff), vget_low_s16(diff));
    return vmlal_s16(ret, vget_high_s16(diff), vget_high_s16(diff));
}

static inline int32x4x2_t sqri8_vector(const int32x4x2_t acc, const int8x16_t va, const int8x16_t vb) {
    // int diff = a - b
    int16x8x2_t diffs = create_pair<vsubl_s8>(va, vb);
    // acc += diff * diff
    return apply<sqri8_vector_combine>(acc, diffs);
}

EXPORT void vec_sqri7u_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<identity_mapper, sqri8_vector, sqr_scalar<int8_t>, vec_sqri8>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqri7u_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<array_mapper, sqri8_vector, sqr_scalar<int8_t>, vec_sqri8>(a, b, dims, pitch, offsets, count, results);
}

EXPORT void vec_sqri8_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<identity_mapper, sqri8_vector, sqr_scalar<int8_t>, vec_sqri8>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqri8_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<array_mapper, sqri8_vector, sqr_scalar<int8_t>, vec_sqri8>(a, b, dims, pitch, offsets, count, results);
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

    float32x4_t total = tree_reduce<batches, float32x4_t, vaddq_f32>(sums);
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

    float32x4_t total = tree_reduce<batches, float32x4_t, vaddq_f32>(sums);
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

static inline int64_t dotd1q4_inner(const int8_t* a, const int8_t* q, const int32_t length) {
    constexpr int query_bits = 4;

    int64_t bit_result[query_bits] = {};

    const uint8_t* query[query_bits];
    apply_indexed<query_bits>([&](auto I) {
        query[I] = (const uint8_t*)q + I * length;
    });

    int r = 0;
    constexpr int chunk_size = sizeof(uint64x2_t);
    if (length >= chunk_size) {
        int iters = length / chunk_size;
        uint8x16_t zero = vcombine_u8(vcreate_u8(0), vcreate_u8(0));

        for (int j = 0; j < iters;) {
            uint8x16_t bit_sum[query_bits];
            apply_indexed<query_bits>([&](auto I) {
                bit_sum[I] = zero;
            });

            /*
            * After every 31 iterations we need to add the
            * bit sums to the total sum.
            * We must ensure that the temporary sums <= 255
            * and 31 * 8 bits = 248 which is OK.
            */
            uint64_t limit = std::min(iters, j + 31);
            for (; j < limit; j++, r += chunk_size)  {
                const uint8x16_t yv = vld1q_u8((const uint8_t*)a + r);
                apply_indexed<query_bits>([&](auto I) {
                    bit_sum[I] = vaddq_u8(bit_sum[I], vcntq_u8(vandq_u8(vld1q_u8(query[I] + r), yv)));
                });
            }

            apply_indexed<query_bits>([&](auto I) {
                bit_result[I] += reduce_u8x16_neon(bit_sum[I]);
            });
        }
    }

    // switch to single 64-bit ops
    int upperBound = length & ~(sizeof(int64_t) - 1);
    for (; r < upperBound; r += sizeof(int64_t)) {
        int64_t value = *((int64_t*)(a + r));
        apply_indexed<query_bits>([&](auto I) {
            int64_t bits = *((int64_t*)(query[I] + r));
            bit_result[I] += __builtin_popcountll(bits & value);
        });
    }

    // then 32-bit ops
    upperBound = length & ~(sizeof(int32_t) - 1);
    for (; r < upperBound; r += sizeof(int32_t)) {
        int32_t value = *((int32_t*)(a + r));
        apply_indexed<query_bits>([&](auto I) {
            int32_t bits = *((int32_t*)(query[I] + r));
            bit_result[I] += __builtin_popcount(bits & value);
        });
    }

    // then single bytes
    for (; r < length; r++) {
        int8_t value = *(a + r);
        apply_indexed<query_bits>([&](auto I) {
            int32_t bits = *(query[I] + r);
            bit_result[I] += __builtin_popcount(bits & value & 0xFF);
        });
    }
    int sum = 0;
    apply_indexed<query_bits>([&](auto I) {
        sum += (bit_result[I] << I);
    });
    return sum;
}

EXPORT int64_t vec_dotd1q4(const int8_t* a, const int8_t* query, const int32_t length) {
    return dotd1q4_inner(a, query, length);
}

template <int64_t(*mapper)(const int32_t, const int32_t*)>
static inline void dotd1q4_inner_bulk(
    const int8_t* a,
    const int8_t* q,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    constexpr int batches = 2;
    constexpr int query_bits = 4;
    constexpr int chunk_size = sizeof(uint64x2_t);

    const uint8_t* query[query_bits];
    apply_indexed<query_bits>([&](auto I) {
        query[I] = (const uint8_t*)q + I * length;
    });

    const int iters = length / chunk_size;
    const uint8x16_t zero = vcombine_u8(vcreate_u8(0), vcreate_u8(0));

    int c = 0;

    for (; c + batches < count; c += batches) {
        const uint8_t* as[batches];
        apply_indexed<batches>([&](auto I) {
            as[I] = (const uint8_t*)a + mapper(c + I, offsets) * pitch;
        });

        int64_t bit_result[batches * query_bits] = {};

        int r = 0;

        if (length >= chunk_size) {
            for (int j = 0; j < iters;) {
                uint8x16_t bit_sum[batches * query_bits];
                apply_indexed<batches * query_bits>([&](auto B) {
                    bit_sum[B] = zero;
                });

                /*
                * After every 31 iterations we need to add the
                * bit_sum to the total sum.
                * We must ensure that the temporary sums <= 255
                * and 31 * 8 bits = 248 which is OK.
                */
                uint64_t limit = std::min(j + 31, iters);
                for (; j < limit; j++, r+= chunk_size)  {
                    uint8x16_t qv[query_bits];
                    apply_indexed<query_bits>([&](auto I) {
                        qv[I] = vld1q_u8(query[I] + r);
                    });

                    uint8x16_t yv[batches];
                    apply_indexed<batches>([&](auto I) {
                        yv[I] = vld1q_u8(as[I] + r);
                    });

                    apply_indexed<batches>([&](auto B) {
                        apply_indexed<query_bits>([&](auto Q) {
                            bit_sum[B * Q] = vaddq_u8(bit_sum[B * Q], vcntq_u8(vandq_u8(qv[Q], yv[B])));
                        });
                    });
                }

                apply_indexed<batches * query_bits>([&](auto I) {
                    bit_result[I] += reduce_u8x16_neon(bit_sum[I]);
                });
            }
        }

        // complete using byte ops
        for (; r < length; r++) {
            int64_t vs[batches];
            apply_indexed<batches>([&](auto I) {
                vs[I] = *(as[I] + r);
            });

            int64_t qs[query_bits];
            apply_indexed<query_bits>([&](auto I) {
                qs[I] = *(query[I] + r);
            });

            apply_indexed<batches>([&](auto B) {
                apply_indexed<query_bits>([&](auto Q) {
                    bit_result[B * Q] = __builtin_popcount(qs[Q] & vs[B] & 0xFF);
                });
            });
        }
        apply_indexed<batches>([&](auto I) {
            int32_t res = 0;
            apply_indexed<query_bits>([&](auto Q) {
                res += (bit_result[I * Q] << Q);
            });
            results[c + I] = (f32_t)res;
        });
    }

    for (; c < count; c++) {
        const int8_t* a0 = a + mapper(c, offsets) * pitch;
        results[c] = (f32_t)dotd1q4_inner(a0, q, length);
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
