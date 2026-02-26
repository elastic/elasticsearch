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
// capabilities; in the case of x64, this first tier include functions for processors
// supporting at least AVX2.

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <math.h>
#include <limits>

#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"
#include "score_common.h"

static inline __m256 apply_base_corrections(
    const f32_t* lowerInterval,
    const f32_t* upperInterval,
    const int32_t* targetComponentSum,
    const f32_t* score,
    const f32_t ay,
    const f32_t ly,
    const f32_t y1,
    const f32_t dimensions,
    const f32_t indexBitScale
) {
    const __m256 ax = _mm256_loadu_ps(lowerInterval);
    const __m256 lx = _mm256_mul_ps(_mm256_sub_ps(_mm256_loadu_ps(upperInterval), ax), _mm256_set1_ps(indexBitScale));
    const __m256 tcs = _mm256_cvtepi32_ps(_mm256_lddqu_si256((const __m256i*)(targetComponentSum)));

    const __m256 qcDist = _mm256_loadu_ps(score);

    // ax * ay * dimensions + ay * lx * (float) targetComponentSum + ax * ly * y1 + lx * ly * qcDist;
    // ax * ay * dimensions
    const __m256 res1 = _mm256_mul_ps(ax, _mm256_set1_ps(ay * dimensions));
    // ay * lx * (float) targetComponentSum
    const __m256 res2 = _mm256_mul_ps(_mm256_mul_ps(lx, _mm256_set1_ps(ay)), tcs);
    // ax * ly * y1
    const __m256 res3 = _mm256_mul_ps(ax, _mm256_set1_ps(ly * y1));
    // lx * ly * qcDist
    const __m256 res4 = _mm256_mul_ps(_mm256_mul_ps(lx, _mm256_set1_ps(ly)), qcDist);
    return _mm256_add_ps(_mm256_add_ps(res1, res2), _mm256_add_ps(res3, res4));
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
    f32_t maxScore = -std::numeric_limits<f32_t>::infinity();

    const corrections_t c = unpack_corrections(corrections, bulkSize);

    int i = 0;
    constexpr int floats_per_cycle = sizeof(__m256) / sizeof(f32_t);
    const int upperBound = bulkSize & ~(floats_per_cycle - 1);
    for (; i < upperBound; i += floats_per_cycle) {
        __m256 additionalCorrection = _mm256_loadu_ps(c.additionalCorrections + i);
        __m256 res = apply_base_corrections(
            c.lowerIntervals + i,
            c.upperIntervals + i,
            c.targetComponentSums + i,
            scores + i,
            ay,
            ly,
            y1,
            dimensions,
            indexBitScale
        );

        // For euclidean, we need to invert the score and apply the additional correction, which is
        // assumed to be the squared l2norm of the centroid centered vectors.
        res = _mm256_add_ps(
            _mm256_fnmadd_ps(_mm256_set1_ps(2.0f), res, additionalCorrection),
            _mm256_set1_ps(queryAdditionalCorrection + 1.0f)
        );
        res = _mm256_max_ps(_mm256_rcp_ps(res), _mm256_setzero_ps());

        maxScore = fmax(maxScore, mm256_reduce_ps<_mm_max_ps>(res));
        _mm256_storeu_ps(scores + i, res);
    }
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
            *(c.lowerIntervals + i),
            *(c.upperIntervals + i),
            *(c.targetComponentSums + i),
            *(c.additionalCorrections + i),
            *(scores + i)
        );
        *(scores + i) = score;
        maxScore = fmax(maxScore, score);
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
    f32_t maxScore = -std::numeric_limits<f32_t>::infinity();

    const corrections_t c = unpack_corrections(corrections, bulkSize);

    int i = 0;
    constexpr int floats_per_cycle = sizeof(__m256) / sizeof(f32_t);
    const int upperBound = bulkSize & ~(floats_per_cycle - 1);
    for (; i < upperBound; i += floats_per_cycle) {
        __m256 res = apply_base_corrections(
            c.lowerIntervals + i,
            c.upperIntervals + i,
            c.targetComponentSums + i,
            scores + i,
            ay,
            ly,
            y1,
            dimensions,
            indexBitScale
        );

        // For max inner product, we need to apply the additional correction, which is
        // assumed to be the non-centered dot-product between the vector and the centroid
        __m256 additionalCorrection = _mm256_loadu_ps(c.additionalCorrections + i);
        res = _mm256_add_ps(
            _mm256_add_ps(res, additionalCorrection),
            _mm256_set1_ps(queryAdditionalCorrection - centroidDp)
        );

        // In scalar code, this is a if-else branch on res (or res elements) being positive or negative.
        // Vectorized, this means computing both branches in 2 vector registers, and then combine them into one,
        // masking for positive/negative. Computing both with SIMD operations is faster than computing only the
        // right branch with scalar operations.
        __m256 negative_scaled = _mm256_rcp_ps(_mm256_fnmadd_ps(_mm256_set1_ps(1.0f), res, _mm256_set1_ps(1.0f)));
        __m256 positive_scaled = _mm256_add_ps(_mm256_set1_ps(1.0f), res);

        // We do not have masking ops on AVX2, so we mimic them with AND + ADD
        __m256 is_neg = _mm256_cmp_ps(res, _mm256_setzero_ps(), _CMP_LT_OQ);
        res = _mm256_add_ps(_mm256_and_ps(is_neg, negative_scaled), _mm256_andnot_ps(is_neg, positive_scaled));

        maxScore = fmax(maxScore, mm256_reduce_ps<_mm_max_ps>(res));
        _mm256_storeu_ps(scores + i, res);
    }

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
            *(c.lowerIntervals + i),
            *(c.upperIntervals + i),
            *(c.targetComponentSums + i),
            *(c.additionalCorrections + i),
            *(scores + i)
        );
        *(scores + i) = score;
        maxScore = fmax(maxScore, score);
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
    f32_t maxScore = -std::numeric_limits<f32_t>::infinity();

    const corrections_t c = unpack_corrections(corrections, bulkSize);

    int i = 0;
    constexpr int floats_per_cycle = sizeof(__m256) / sizeof(f32_t);
    const int upperBound = bulkSize & ~(floats_per_cycle - 1);
    for (; i < upperBound; i += floats_per_cycle) {
        __m256 res = apply_base_corrections(
            c.lowerIntervals + i,
            c.upperIntervals + i,
            c.targetComponentSums + i,
            scores + i,
            ay,
            ly,
            y1,
            dimensions,
            indexBitScale
        );

        __m256 additionalCorrection = _mm256_loadu_ps(c.additionalCorrections + i);
        // For dot product we need to apply the additional correction, which is
        // assumed to be the non-centered dot-product between the vector and the centroid

        // res = res + additionalCorrection + queryAdditionalCorrection - centroidDp (+ 1.0f);
        res = _mm256_add_ps(
            _mm256_add_ps(res, additionalCorrection),
            _mm256_set1_ps(queryAdditionalCorrection - centroidDp + 1.0f)
        );

        // res = max(res / 2.0f, 0.0f);
        res = _mm256_max_ps(_mm256_mul_ps(res, _mm256_set1_ps(0.5f)), _mm256_setzero_ps());

        maxScore = fmax(maxScore, mm256_reduce_ps<_mm_max_ps>(res));
        _mm256_storeu_ps(scores + i, res);
    }

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
            *(c.lowerIntervals + i),
            *(c.upperIntervals + i),
            *(c.targetComponentSums + i),
            *(c.additionalCorrections + i),
            *(scores + i)
        );
        *(scores + i) = score;
        maxScore = fmax(maxScore, score);
    }

    return maxScore;
}
