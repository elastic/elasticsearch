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
#include <limits>
#include "vec.h"
#include "vec_common.h"
#include "aarch64/aarch64_vec_common.h"

#include "score_common.h"

// BQ corrections for different formats/layouts (DiskBBQ/BBQ).
// The diskbbq dot-product variant is NEON-vectorized below; the remaining variants (euclidean, MIP)
// and all bbq_* variants are kept scalar for now as they are not observed hot-spots.


// BBQ inline correction layout: corrections are stored after each vector's quantized bytes.
// Per-vector layout at offset (node[i] * pitchInBytes + vectorSizeInBytes):
//   float lowerInterval, float upperInterval, float additionalCorrection, short targetComponentSum
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
        const int8_t readComponentSumAsInt,
        f32_t* scores
) {
    f32_t maxScore = -std::numeric_limits<f32_t>::infinity();
    for (int i = 0; i < bulkSize; ++i) {
        const bbq_correction_t c = bbq_read_corrections(addresses[i], vectorSizeInBytes, readComponentSumAsInt);
        const f32_t score = apply_base_corrections_common(
            dimensions,
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryBitScale,
            indexBitScale,
            c.lowerInterval,
            c.upperInterval,
            c.targetComponentSum,
            scores[i]
        );
        scores[i] = euclidean_correction(score, queryAdditionalCorrection, c.additionalCorrection);
        maxScore = __builtin_fmaxf(maxScore, scores[i]);
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
        const int8_t readComponentSumAsInt,
        f32_t* scores
) {
    f32_t maxScore = -std::numeric_limits<f32_t>::infinity();
    for (int i = 0; i < bulkSize; ++i) {
        const bbq_correction_t c = bbq_read_corrections(addresses[i], vectorSizeInBytes, readComponentSumAsInt);
        const f32_t score = apply_base_corrections_common(
            dimensions,
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryBitScale,
            indexBitScale,
            c.lowerInterval,
            c.upperInterval,
            c.targetComponentSum,
            scores[i]
        );
        scores[i] = maximum_inner_product_correction(
            score,
            queryAdditionalCorrection,
            c.additionalCorrection,
            centroidDp
        );
        maxScore = __builtin_fmaxf(maxScore, scores[i]);
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
        const int8_t readComponentSumAsInt,
        f32_t* scores
) {
    f32_t maxScore = -std::numeric_limits<f32_t>::infinity();
    for (int i = 0; i < bulkSize; ++i) {
        const bbq_correction_t c = bbq_read_corrections(addresses[i], vectorSizeInBytes, readComponentSumAsInt);
        const f32_t score = apply_base_corrections_common(
            dimensions,
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryBitScale,
            indexBitScale,
            c.lowerInterval,
            c.upperInterval,
            c.targetComponentSum,
            scores[i]
        );
        scores[i] = dot_product_correction(
            score,
            queryAdditionalCorrection,
            c.additionalCorrection,
            centroidDp
        );
        maxScore = __builtin_fmaxf(maxScore, scores[i]);
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
    f32_t maxScore = -std::numeric_limits<f32_t>::infinity();

    const corrections_t c = unpack_corrections(corrections, bulkSize);

    int i = 0;
    for (; i < bulkSize; ++i) {
        const f32_t score = apply_base_corrections_common(
            dimensions,
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryBitScale,
            indexBitScale,
            c.lowerIntervals[i],
            c.upperIntervals[i],
            c.targetComponentSums[i],
            scores[i]
        );
        scores[i] = legacy_euclidean_correction(score, queryAdditionalCorrection, c.additionalCorrections[i]);
        maxScore = __builtin_fmaxf(maxScore, scores[i]);
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
    f32_t maxScore = -std::numeric_limits<f32_t>::infinity();

    const corrections_t c = unpack_corrections(corrections, bulkSize);

    int i = 0;
    for (; i < bulkSize; ++i) {
        const f32_t score = apply_base_corrections_common(
            dimensions,
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryBitScale,
            indexBitScale,
            c.lowerIntervals[i],
            c.upperIntervals[i],
            c.targetComponentSums[i],
            scores[i]
        );
        scores[i] = maximum_inner_product_correction(
            score,
            queryAdditionalCorrection,
            c.additionalCorrections[i],
            centroidDp
        );
        maxScore = __builtin_fmaxf(maxScore, scores[i]);
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
    const corrections_t c = unpack_corrections(corrections, bulkSize);

    // Precompute query constants
    const f32_t ly = (queryUpperInterval - queryLowerInterval) * queryBitScale;
    const f32_t ay = queryLowerInterval;
    const f32_t dims_f = (f32_t)dimensions;
    const f32_t y1 = (f32_t)queryComponentSum;
    // Combined constant: queryAdditionalCorrection - centroidDp
    const f32_t qac_minus_cdp = queryAdditionalCorrection - centroidDp;

    // Broadcast constants into NEON registers
    const float32x4_t vDimsAy = vdupq_n_f32(dims_f * ay);
    const float32x4_t vAy = vdupq_n_f32(ay);
    const float32x4_t vLy = vdupq_n_f32(ly);
    const float32x4_t vLyY1 = vdupq_n_f32(ly * y1);
    const float32x4_t vIndexBitScale = vdupq_n_f32(indexBitScale);
    const float32x4_t vQacMinusCdp = vdupq_n_f32(qac_minus_cdp);
    const float32x4_t vHalf = vdupq_n_f32(0.5f);
    const float32x4_t vOne = vdupq_n_f32(1.0f);
    const float32x4_t vZero = vdupq_n_f32(0.0f);

    float32x4_t vMax = vdupq_n_f32(-std::numeric_limits<f32_t>::infinity());

    int i = 0;
    for (; i + 4 <= bulkSize; i += 4) {
        float32x4_t vAx = vld1q_f32(&c.lowerIntervals[i]);
        float32x4_t vUpper = vld1q_f32(&c.upperIntervals[i]);
        int32x4_t vTCS = vld1q_s32(&c.targetComponentSums[i]);
        float32x4_t vAC = vld1q_f32(&c.additionalCorrections[i]);
        float32x4_t vQcDist = vld1q_f32(&scores[i]);

        // lx = (upper - ax) * indexBitScale
        float32x4_t vLx = vmulq_f32(vsubq_f32(vUpper, vAx), vIndexBitScale);

        // score = ax*ay*dims + ay*lx*tcs + ax*ly*y1 + lx*ly*qcDist
        float32x4_t vTCSf = vcvtq_f32_s32(vTCS);
        float32x4_t score = vmulq_f32(vAx, vDimsAy);          // ax * (dims*ay)
        score = vfmaq_f32(score, vmulq_f32(vAy, vLx), vTCSf); // + ay*lx*tcs
        score = vfmaq_f32(score, vAx, vLyY1);                 // + ax*(ly*y1)
        score = vfmaq_f32(score, vmulq_f32(vLx, vLy), vQcDist); // + lx*ly*qcDist

        // finalScore = score + qac - cdp + ac = score + (qac-cdp) + ac
        float32x4_t finalScore = vaddq_f32(vaddq_f32(score, vQacMinusCdp), vAC);

        // result = max((1 + finalScore) / 2, 0)
        float32x4_t result = vmaxq_f32(vmulq_f32(vaddq_f32(vOne, finalScore), vHalf), vZero);

        vst1q_f32(&scores[i], result);
        vMax = vmaxq_f32(vMax, result);
    }

    // Reduce vMax to scalar
    f32_t maxScore = vmaxvq_f32(vMax);

    // Scalar tail
    for (; i < bulkSize; ++i) {
        const f32_t score = apply_base_corrections_common(
            dimensions,
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryBitScale,
            indexBitScale,
            c.lowerIntervals[i],
            c.upperIntervals[i],
            c.targetComponentSums[i],
            scores[i]
        );
        scores[i] = legacy_dot_product_correction(
            score,
            queryAdditionalCorrection,
            c.additionalCorrections[i],
            centroidDp
        );
        maxScore = __builtin_fmaxf(maxScore, scores[i]);
    }

    return maxScore;
}
