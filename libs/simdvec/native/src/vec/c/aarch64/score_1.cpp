/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// This file contains implementations for transforming distances into scores,
// applying additional corrections. It includes support for "1st tier" vector
// capabilities; in the case of ARM, this first tier include functions for
// processors supporting at least the NEON instruction set.

#include <stddef.h>
#include <stdint.h>
#include <arm_neon.h>
#include <limits>
#include "vec.h"
#include "vec_common.h"
#include "aarch64/aarch64_vec_common.h"

#include "score_common.h"

// Compute (1 / x) using the NEON reciprocal estimate plus one Newton-Raphson
// step. Precision is ~16 bits, comparable to AVX-512 _mm512_rcp14_ps and a bit
// better than AVX2 _mm256_rcp_ps; well within the tolerance of the scalar path
// that uses a single full-precision divide.
static inline float32x4_t neon_rcp_ps(const float32x4_t x) {
    float32x4_t r = vrecpeq_f32(x);
    r = vmulq_f32(r, vrecpsq_f32(x, r));
    return r;
}

// Computes ax * ay * dimensions + ay * lx * (float)targetComponentSum
//        + ax * ly * y1 + lx * ly * qcDist, where lx = (ui - ax) * indexBitScale.
static inline float32x4_t apply_base_corrections(
    const float32x4_t ax,
    const float32x4_t ui,
    const int32x4_t targetComponentSum,
    const float32x4_t qcDist,
    const f32_t ay,
    const f32_t ly,
    const f32_t y1,
    const f32_t dimensions,
    const f32_t indexBitScale
) {
    const float32x4_t lx = vmulq_f32(vsubq_f32(ui, ax), vdupq_n_f32(indexBitScale));
    const float32x4_t tcs = vcvtq_f32_s32(targetComponentSum);

    // ax * ay * dimensions
    const float32x4_t res1 = vmulq_f32(ax, vdupq_n_f32(ay * dimensions));
    // ay * lx * (float) targetComponentSum
    const float32x4_t res2 = vmulq_f32(vmulq_f32(lx, vdupq_n_f32(ay)), tcs);
    // ax * ly * y1
    const float32x4_t res3 = vmulq_f32(ax, vdupq_n_f32(ly * y1));
    // lx * ly * qcDist
    const float32x4_t res4 = vmulq_f32(vmulq_f32(lx, vdupq_n_f32(ly)), qcDist);
    return vaddq_f32(vaddq_f32(res1, res2), vaddq_f32(res3, res4));
}

// BBQ inline correction layout: corrections are stored after each vector's quantized bytes.
// Per-vector layout at offset (vectorSizeInBytes) from each address:
//   float lowerInterval, float upperInterval, float additionalCorrection, short targetComponentSum
// Since vectors may be at arbitrary addresses, we load corrections individually and pack into SIMD registers.
static inline void bbq_load_corrections_4(
    const void* const* addresses,
    const int32_t vectorSizeInBytes,
    float32x4_t& lowerInterval,
    float32x4_t& upperInterval,
    float32x4_t& additionalCorrection,
    int32x4_t& targetComponentSum
) {
    // Stack-allocated, naturally aligned arrays so the NEON loads are well-formed.
    alignas(16) f32_t li[4], ui[4], ac[4];
    alignas(16) int32_t tcs[4];
    for (int j = 0; j < 4; ++j) {
        const int8_t* base = (const int8_t*)addresses[j] + vectorSizeInBytes;
        li[j] = *(const f32_t*)base;
        ui[j] = *(const f32_t*)(base + sizeof(f32_t));
        ac[j] = *(const f32_t*)(base + 2 * sizeof(f32_t));
        tcs[j] = (int32_t)(*(const uint16_t*)(base + 3 * sizeof(f32_t)));
    }
    lowerInterval = vld1q_f32(li);
    upperInterval = vld1q_f32(ui);
    additionalCorrection = vld1q_f32(ac);
    targetComponentSum = vld1q_s32(tcs);
}

EXPORT f32_t bbq_apply_corrections_euclidean_bulk(
        const void* const* addresses,
        const int32_t bulkSize,
        const int32_t vectorSizeInBytes,
        const int32_t pitchInBytes,
        const int32_t dimensions,
        const f32_t queryLowerInterval,
        const f32_t queryUpperInterval,
        const int32_t queryComponentSum,
        const f32_t queryAdditionalCorrection,
        const f32_t queryBitScale,
        const f32_t indexBitScale,
        const f32_t centroidDp,
        f32_t* scores
) {
    const f32_t ay = queryLowerInterval;
    const f32_t ly = (queryUpperInterval - ay) * queryBitScale;
    const f32_t y1 = queryComponentSum;
    // Keep the running max in a SIMD register and reduce once after the loop.
    // vmaxvq_f32 inside the loop forces a vector->GPR transfer + 6-cycle horizontal
    // reduction every iteration, serializing through a scalar dependency.
    float32x4_t maxVec = vdupq_n_f32(-std::numeric_limits<f32_t>::infinity());

    int i = 0;
    constexpr int floats_per_cycle = sizeof(float32x4_t) / sizeof(f32_t);
    const int upperBound = bulkSize & ~(floats_per_cycle - 1);
    for (; i < upperBound; i += floats_per_cycle) {
        float32x4_t lowerInterval, upperInterval, additionalCorrection;
        int32x4_t targetComponentSum;
        bbq_load_corrections_4(addresses + i, vectorSizeInBytes,
            lowerInterval, upperInterval, additionalCorrection, targetComponentSum);

        float32x4_t res = apply_base_corrections(
            lowerInterval, upperInterval, targetComponentSum,
            vld1q_f32(scores + i),
            ay, ly, y1, dimensions, indexBitScale
        );

        // res = additionalCorrection - 2 * res + (queryAdditionalCorrection + 1)
        res = vaddq_f32(
            vfmsq_f32(additionalCorrection, vdupq_n_f32(2.0f), res),
            vdupq_n_f32(queryAdditionalCorrection + 1.0f)
        );
        res = vmaxq_f32(neon_rcp_ps(res), vdupq_n_f32(0.0f));

        maxVec = vmaxq_f32(maxVec, res);
        vst1q_f32(scores + i, res);
    }
    f32_t maxScore = vmaxvq_f32(maxVec);
    for (; i < bulkSize; ++i) {
        const bbq_correction_t c = bbq_read_corrections(addresses[i], vectorSizeInBytes);
        f32_t score = apply_corrections_euclidean_inner(
            dimensions, queryLowerInterval, queryUpperInterval, queryComponentSum,
            queryAdditionalCorrection, queryBitScale, indexBitScale, centroidDp,
            c.lowerInterval, c.upperInterval, c.targetComponentSum, c.additionalCorrection, scores[i]
        );
        scores[i] = score;
        maxScore = __builtin_fmaxf(maxScore, score);
    }
    return maxScore;
}

EXPORT f32_t bbq_apply_corrections_maximum_inner_product_bulk(
        const void* const* addresses,
        const int32_t bulkSize,
        const int32_t vectorSizeInBytes,
        const int32_t pitchInBytes,
        const int32_t dimensions,
        const f32_t queryLowerInterval,
        const f32_t queryUpperInterval,
        const int32_t queryComponentSum,
        const f32_t queryAdditionalCorrection,
        const f32_t queryBitScale,
        const f32_t indexBitScale,
        const f32_t centroidDp,
        f32_t* scores
) {
    const f32_t ay = queryLowerInterval;
    const f32_t ly = (queryUpperInterval - ay) * queryBitScale;
    const f32_t y1 = queryComponentSum;
    float32x4_t maxVec = vdupq_n_f32(-std::numeric_limits<f32_t>::infinity());

    int i = 0;
    constexpr int floats_per_cycle = sizeof(float32x4_t) / sizeof(f32_t);
    const int upperBound = bulkSize & ~(floats_per_cycle - 1);
    for (; i < upperBound; i += floats_per_cycle) {
        float32x4_t lowerInterval, upperInterval, additionalCorrection;
        int32x4_t targetComponentSum;
        bbq_load_corrections_4(addresses + i, vectorSizeInBytes,
            lowerInterval, upperInterval, additionalCorrection, targetComponentSum);

        float32x4_t res = apply_base_corrections(
            lowerInterval, upperInterval, targetComponentSum,
            vld1q_f32(scores + i),
            ay, ly, y1, dimensions, indexBitScale
        );

        res = vaddq_f32(
            vaddq_f32(res, additionalCorrection),
            vdupq_n_f32(queryAdditionalCorrection - centroidDp)
        );

        // negative branch: 1 / (1 + -res) == 1 / (1 - res); use FMS to avoid rounding twice.
        const float32x4_t one = vdupq_n_f32(1.0f);
        const float32x4_t negative_scaled = neon_rcp_ps(vfmsq_f32(one, one, res));
        const float32x4_t positive_scaled = vaddq_f32(one, res);

        const uint32x4_t is_neg = vcltq_f32(res, vdupq_n_f32(0.0f));
        res = vbslq_f32(is_neg, negative_scaled, positive_scaled);

        maxVec = vmaxq_f32(maxVec, res);
        vst1q_f32(scores + i, res);
    }
    f32_t maxScore = vmaxvq_f32(maxVec);
    for (; i < bulkSize; ++i) {
        const bbq_correction_t c = bbq_read_corrections(addresses[i], vectorSizeInBytes);
        f32_t score = apply_corrections_maximum_inner_product_inner(
            dimensions, queryLowerInterval, queryUpperInterval, queryComponentSum,
            queryAdditionalCorrection, queryBitScale, indexBitScale, centroidDp,
            c.lowerInterval, c.upperInterval, c.targetComponentSum, c.additionalCorrection, scores[i]
        );
        scores[i] = score;
        maxScore = __builtin_fmaxf(maxScore, score);
    }
    return maxScore;
}

EXPORT f32_t bbq_apply_corrections_dot_product_bulk(
        const void* const* addresses,
        const int32_t bulkSize,
        const int32_t vectorSizeInBytes,
        const int32_t pitchInBytes,
        const int32_t dimensions,
        const f32_t queryLowerInterval,
        const f32_t queryUpperInterval,
        const int32_t queryComponentSum,
        const f32_t queryAdditionalCorrection,
        const f32_t queryBitScale,
        const f32_t indexBitScale,
        const f32_t centroidDp,
        f32_t* scores
) {
    const f32_t ay = queryLowerInterval;
    const f32_t ly = (queryUpperInterval - ay) * queryBitScale;
    const f32_t y1 = queryComponentSum;
    float32x4_t maxVec = vdupq_n_f32(-std::numeric_limits<f32_t>::infinity());

    int i = 0;
    constexpr int floats_per_cycle = sizeof(float32x4_t) / sizeof(f32_t);
    const int upperBound = bulkSize & ~(floats_per_cycle - 1);
    for (; i < upperBound; i += floats_per_cycle) {
        float32x4_t lowerInterval, upperInterval, additionalCorrection;
        int32x4_t targetComponentSum;
        bbq_load_corrections_4(addresses + i, vectorSizeInBytes,
            lowerInterval, upperInterval, additionalCorrection, targetComponentSum);

        float32x4_t res = apply_base_corrections(
            lowerInterval, upperInterval, targetComponentSum,
            vld1q_f32(scores + i),
            ay, ly, y1, dimensions, indexBitScale
        );

        res = vaddq_f32(
            vaddq_f32(res, additionalCorrection),
            vdupq_n_f32(queryAdditionalCorrection - centroidDp + 1.0f)
        );

        res = vmaxq_f32(vmulq_f32(res, vdupq_n_f32(0.5f)), vdupq_n_f32(0.0f));

        maxVec = vmaxq_f32(maxVec, res);
        vst1q_f32(scores + i, res);
    }
    f32_t maxScore = vmaxvq_f32(maxVec);
    for (; i < bulkSize; ++i) {
        const bbq_correction_t c = bbq_read_corrections(addresses[i], vectorSizeInBytes);
        f32_t score = apply_corrections_dot_product_inner(
            dimensions, queryLowerInterval, queryUpperInterval, queryComponentSum,
            queryAdditionalCorrection, queryBitScale, indexBitScale, centroidDp,
            c.lowerInterval, c.upperInterval, c.targetComponentSum, c.additionalCorrection, scores[i]
        );
        scores[i] = score;
        maxScore = __builtin_fmaxf(maxScore, score);
    }
    return maxScore;
}

EXPORT f32_t diskbbq_apply_corrections_euclidean_bulk(
        const int8_t* corrections,
		const int32_t bulkSize,
        const int32_t dimensions,
        const f32_t queryLowerInterval,
        const f32_t queryUpperInterval,
        const int32_t queryComponentSum,
        const f32_t queryAdditionalCorrection,
        const f32_t queryBitScale,
        const f32_t indexBitScale,
        const f32_t centroidDp,
        f32_t* scores
) {
    const f32_t ay = queryLowerInterval;
    const f32_t ly = (queryUpperInterval - ay) * queryBitScale;
    const f32_t y1 = queryComponentSum;
    float32x4_t maxVec = vdupq_n_f32(-std::numeric_limits<f32_t>::infinity());

    const corrections_t c = unpack_corrections(corrections, bulkSize);

    int i = 0;
    constexpr int floats_per_cycle = sizeof(float32x4_t) / sizeof(f32_t);
    const int upperBound = bulkSize & ~(floats_per_cycle - 1);
    for (; i < upperBound; i += floats_per_cycle) {
        const float32x4_t additionalCorrection = vld1q_f32(c.additionalCorrections + i);
        float32x4_t res = apply_base_corrections(
            vld1q_f32(c.lowerIntervals + i),
            vld1q_f32(c.upperIntervals + i),
            vld1q_s32(c.targetComponentSums + i),
            vld1q_f32(scores + i),
            ay,
            ly,
            y1,
            dimensions,
            indexBitScale
        );

        // For euclidean, we need to invert the score and apply the additional correction, which is
        // assumed to be the squared l2norm of the centroid centered vectors.
        res = vaddq_f32(
            vfmsq_f32(additionalCorrection, vdupq_n_f32(2.0f), res),
            vdupq_n_f32(queryAdditionalCorrection + 1.0f)
        );
        res = vmaxq_f32(neon_rcp_ps(res), vdupq_n_f32(0.0f));

        maxVec = vmaxq_f32(maxVec, res);
        vst1q_f32(scores + i, res);
    }
    f32_t maxScore = vmaxvq_f32(maxVec);
    for (; i < bulkSize; ++i) {
        f32_t score = apply_corrections_euclidean_inner(
            dimensions,
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryAdditionalCorrection,
            queryBitScale,
            indexBitScale,
            centroidDp,
            c.lowerIntervals[i],
            c.upperIntervals[i],
            c.targetComponentSums[i],
            c.additionalCorrections[i],
            scores[i]
        );
        scores[i] = score;
        maxScore = __builtin_fmaxf(maxScore, score);
    }

    return maxScore;
}

EXPORT f32_t diskbbq_apply_corrections_maximum_inner_product_bulk(
        const int8_t* corrections,
		const int32_t bulkSize,
        const int32_t dimensions,
        const f32_t queryLowerInterval,
        const f32_t queryUpperInterval,
        const int32_t queryComponentSum,
        const f32_t queryAdditionalCorrection,
        const f32_t queryBitScale,
        const f32_t indexBitScale,
        const f32_t centroidDp,
        f32_t* scores
) {
    const f32_t ay = queryLowerInterval;
    const f32_t ly = (queryUpperInterval - ay) * queryBitScale;
    const f32_t y1 = queryComponentSum;
    float32x4_t maxVec = vdupq_n_f32(-std::numeric_limits<f32_t>::infinity());

    const corrections_t c = unpack_corrections(corrections, bulkSize);

    int i = 0;
    constexpr int floats_per_cycle = sizeof(float32x4_t) / sizeof(f32_t);
    const int upperBound = bulkSize & ~(floats_per_cycle - 1);
    for (; i < upperBound; i += floats_per_cycle) {
        float32x4_t res = apply_base_corrections(
            vld1q_f32(c.lowerIntervals + i),
            vld1q_f32(c.upperIntervals + i),
            vld1q_s32(c.targetComponentSums + i),
            vld1q_f32(scores + i),
            ay,
            ly,
            y1,
            dimensions,
            indexBitScale
        );

        // For max inner product, we need to apply the additional correction, which is
        // assumed to be the non-centered dot-product between the vector and the centroid
        const float32x4_t additionalCorrection = vld1q_f32(c.additionalCorrections + i);
        res = vaddq_f32(
            vaddq_f32(res, additionalCorrection),
            vdupq_n_f32(queryAdditionalCorrection - centroidDp)
        );

        // In scalar code, this is an if-else branch on res (or res elements) being positive or negative.
        // Vectorized, we compute both branches and bit-select on the negativity mask: NEON has a clean
        // vbslq_f32, no need for the AND/ANDNOT trick used on AVX2.
        const float32x4_t one = vdupq_n_f32(1.0f);
        const float32x4_t negative_scaled = neon_rcp_ps(vfmsq_f32(one, one, res));
        const float32x4_t positive_scaled = vaddq_f32(one, res);

        const uint32x4_t is_neg = vcltq_f32(res, vdupq_n_f32(0.0f));
        res = vbslq_f32(is_neg, negative_scaled, positive_scaled);

        maxVec = vmaxq_f32(maxVec, res);
        vst1q_f32(scores + i, res);
    }
    f32_t maxScore = vmaxvq_f32(maxVec);

    for (; i < bulkSize; ++i) {
        f32_t score = apply_corrections_maximum_inner_product_inner(
            dimensions,
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryAdditionalCorrection,
            queryBitScale,
            indexBitScale,
            centroidDp,
            c.lowerIntervals[i],
            c.upperIntervals[i],
            c.targetComponentSums[i],
            c.additionalCorrections[i],
            scores[i]
        );
        scores[i] = score;
        maxScore = __builtin_fmaxf(maxScore, score);
    }

    return maxScore;
}

EXPORT f32_t diskbbq_apply_corrections_dot_product_bulk(
        const int8_t* corrections,
		const int32_t bulkSize,
        const int32_t dimensions,
        const f32_t queryLowerInterval,
        const f32_t queryUpperInterval,
        const int32_t queryComponentSum,
        const f32_t queryAdditionalCorrection,
        const f32_t queryBitScale,
        const f32_t indexBitScale,
        const f32_t centroidDp,
        f32_t* scores
) {
    const f32_t ay = queryLowerInterval;
    const f32_t ly = (queryUpperInterval - ay) * queryBitScale;
    const f32_t y1 = queryComponentSum;
    float32x4_t maxVec = vdupq_n_f32(-std::numeric_limits<f32_t>::infinity());

    const corrections_t c = unpack_corrections(corrections, bulkSize);

    int i = 0;
    constexpr int floats_per_cycle = sizeof(float32x4_t) / sizeof(f32_t);
    const int upperBound = bulkSize & ~(floats_per_cycle - 1);
    for (; i < upperBound; i += floats_per_cycle) {
        float32x4_t res = apply_base_corrections(
            vld1q_f32(c.lowerIntervals + i),
            vld1q_f32(c.upperIntervals + i),
            vld1q_s32(c.targetComponentSums + i),
            vld1q_f32(scores + i),
            ay,
            ly,
            y1,
            dimensions,
            indexBitScale
        );

        const float32x4_t additionalCorrection = vld1q_f32(c.additionalCorrections + i);
        // For dot product we need to apply the additional correction, which is
        // assumed to be the non-centered dot-product between the vector and the centroid

        // res = res + additionalCorrection + queryAdditionalCorrection - centroidDp + 1.0f;
        res = vaddq_f32(
            vaddq_f32(res, additionalCorrection),
            vdupq_n_f32(queryAdditionalCorrection - centroidDp + 1.0f)
        );

        // res = max(res / 2.0f, 0.0f);
        res = vmaxq_f32(vmulq_f32(res, vdupq_n_f32(0.5f)), vdupq_n_f32(0.0f));

        maxVec = vmaxq_f32(maxVec, res);
        vst1q_f32(scores + i, res);
    }
    f32_t maxScore = vmaxvq_f32(maxVec);

    for (; i < bulkSize; ++i) {
        f32_t score = apply_corrections_dot_product_inner(
            dimensions,
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryAdditionalCorrection,
            queryBitScale,
            indexBitScale,
            centroidDp,
            c.lowerIntervals[i],
            c.upperIntervals[i],
            c.targetComponentSums[i],
            c.additionalCorrections[i],
            scores[i]
        );
        scores[i] = score;
        maxScore = __builtin_fmaxf(maxScore, score);
    }

    return maxScore;
}
