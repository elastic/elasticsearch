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
#include <math.h>
#include <limits>

#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"

static inline __m256 score_inner(const f32_t* lowerInterval, const f32_t* upperInterval, const int16_t* targetComponentSum,
    const f32_t* score, const f32_t ay, const f32_t ly, const f32_t y1, const f32_t dimensions) {

    __m256 ax = _mm256_loadu_ps(lowerInterval);
    // Here we assume `lx` is simply bit vectors, so the scaling isn't necessary
    __m256 lx = _mm256_sub_ps(_mm256_loadu_ps(upperInterval), ax);
    __m256 tcs = _mm256_cvtepi32_ps(
        _mm256_cvtepi16_epi32(
            _mm_lddqu_si128((const __m128i*)(targetComponentSum))
        )
    );

    __m256 qcDist = _mm256_loadu_ps(score);

    // ax * ay * dimensions + ay * lx * (float) targetComponentSum + ax * ly * y1 + lx * ly * qcDist;
    __m256 res1 = _mm256_mul_ps(ax, _mm256_set1_ps(ay * dimensions));
    __m256 res2 = _mm256_mul_ps(tcs, _mm256_mul_ps(lx, _mm256_set1_ps(ay)));
    __m256 res3 = _mm256_mul_ps(ax, _mm256_set1_ps(ly * y1));
    __m256 res4 = _mm256_mul_ps(qcDist, _mm256_mul_ps(lx, _mm256_set1_ps(ly)));
    return _mm256_add_ps(_mm256_add_ps(res1, res2), _mm256_add_ps(res3, res4));
}

EXPORT f32_t score_euclidean_bulk(
        const int8_t* corrections,
		int32_t bulkSize,
        int32_t dimensions,
        f32_t queryLowerInterval,
        f32_t queryUpperInterval,
        int32_t queryComponentSum,
        f32_t queryAdditionalCorrection,
        f32_t queryBitScale,
        f32_t centroidDp,
        f32_t* scores
) {
    f32_t ay = queryLowerInterval;
    f32_t ly = (queryUpperInterval - ay) * queryBitScale;
    f32_t y1 = queryComponentSum;
    f32_t maxScore = -std::numeric_limits<f32_t>::infinity();

    f32_t* lowerIntervals = (f32_t*)corrections;
    f32_t* upperIntervals = (f32_t*)(corrections + 4 * bulkSize);
    int16_t* targetComponentSums = (int16_t*)(corrections + 8 * bulkSize);
    f32_t* additionalCorrections = (f32_t*)(corrections + 10 * bulkSize);

    int i = 0;
    int upperBound = bulkSize & ~(sizeof(__m256) - 1);
    for (; i < upperBound; i += sizeof(__m256)) {
        __m256 additionalCorrection = _mm256_loadu_ps(additionalCorrections + i);
        __m256 res = score_inner(lowerIntervals + i, upperIntervals + i, targetComponentSums + i, scores + i, ay, ly, y1, dimensions);

        // For euclidean, we need to invert the score and apply the additional correction, which is
        // assumed to be the squared l2norm of the centroid centered vectors.
        //res = _mm256_add_ps(_mm256_add_ps(_mm256_mul_ps(res, _mm256_set1_ps(-2.0f)), additionalCorrection), _mm256_set1_ps(queryAdditionalCorrection + 1.0f));
        res = _mm256_add_ps(_mm256_fnmadd_ps(_mm256_set1_ps(2.0f), res, additionalCorrection), _mm256_set1_ps(queryAdditionalCorrection + 1.0f));
        res = _mm256_max_ps(_mm256_rcp_ps(res), _mm256_setzero_ps());

        maxScore = fmax(maxScore, mm256_reduce_ps<_mm_max_ps>(res));
        _mm256_storeu_ps(scores + i, res);
    }
    for (; i < bulkSize; ++i) {
        f32_t score = score_euclidean_inner(
            dimensions,
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryAdditionalCorrection,
            queryBitScale,
            centroidDp,
            *(lowerIntervals + i),
            *(upperIntervals + i),
            *(targetComponentSums + i),
            *(additionalCorrections + i),
            *(scores + i)
        );
        *(scores + i) = score;
        maxScore = fmax(maxScore, score);
    }

    return maxScore;
}

EXPORT f32_t score_maximum_inner_product_bulk(
        const int8_t* corrections,
		int32_t bulkSize,
        int32_t dimensions,
        f32_t queryLowerInterval,
        f32_t queryUpperInterval,
        int32_t queryComponentSum,
        f32_t queryAdditionalCorrection,
        f32_t queryBitScale,
        f32_t centroidDp,
        f32_t* scores
) {
    f32_t ay = queryLowerInterval;
    f32_t ly = (queryUpperInterval - ay) * queryBitScale;
    f32_t y1 = queryComponentSum;
    f32_t maxScore = -std::numeric_limits<f32_t>::infinity();

    f32_t* lowerIntervals = (f32_t*)corrections;
    f32_t* upperIntervals = (f32_t*)(corrections + 4 * bulkSize);
    int16_t* targetComponentSums = (int16_t*)(corrections + 8 * bulkSize);
    f32_t* additionalCorrections = (f32_t*)(corrections + 10 * bulkSize);

    int i = 0;
    int upperBound = bulkSize & ~(sizeof(__m256) - 1);
    for (; i < upperBound; i += sizeof(__m256)) {

        __m256 additionalCorrection = _mm256_loadu_ps(additionalCorrections + i);
        __m256 res = score_inner(lowerIntervals + i, upperIntervals + i, targetComponentSums + i, scores + i, ay, ly, y1, dimensions);

        // For max inner product, we need to apply the additional correction, which is
        // assumed to be the non-centered dot-product between the vector and the centroid
        res = _mm256_add_ps(_mm256_add_ps(res, additionalCorrection), _mm256_set1_ps(queryAdditionalCorrection - centroidDp));

        // On AVX-512, use ternary logic + mask
        //__m256 negative_scaled = _mm256_rcp_ps(_mm256_add_ps(_mm256_set1_ps(1.0f), _mm256_mul_ps(_mm256_set1_ps(-1.0f), res)));
        __m256 negative_scaled = _mm256_rcp_ps(_mm256_fnmadd_ps(_mm256_set1_ps(1.0f), res, _mm256_set1_ps(1.0f)));
        __m256 positive_scaled = _mm256_add_ps(_mm256_set1_ps(1.0f), res);

        __m256 is_neg = _mm256_cmp_ps(res, _mm256_setzero_ps(), _CMP_LT_OQ);
        res = _mm256_add_ps(_mm256_and_ps(is_neg, negative_scaled), _mm256_andnot_ps(is_neg, positive_scaled));

        maxScore = fmax(maxScore, mm256_reduce_ps<_mm_max_ps>(res));
        _mm256_storeu_ps(scores + i, res);
    }

    for (; i < bulkSize; ++i) {
        f32_t score = score_maximum_inner_product_inner(
            dimensions,
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryAdditionalCorrection,
            queryBitScale,
            centroidDp,
            *(lowerIntervals + i),
            *(upperIntervals + i),
            *(targetComponentSums + i),
            *(additionalCorrections + i),
            *(scores + i)
        );
        *(scores + i) = score;
        maxScore = fmax(maxScore, score);
    }

    return maxScore;
}

EXPORT f32_t score_others_bulk(
        const int8_t* corrections,
		int32_t bulkSize,
        int32_t dimensions,
        f32_t queryLowerInterval,
        f32_t queryUpperInterval,
        int32_t queryComponentSum,
        f32_t queryAdditionalCorrection,
        f32_t queryBitScale,
        f32_t centroidDp,
        f32_t* scores
) {
    f32_t ay = queryLowerInterval;
    f32_t ly = (queryUpperInterval - ay) * queryBitScale;
    f32_t y1 = queryComponentSum;
    f32_t maxScore = -std::numeric_limits<f32_t>::infinity();

    f32_t* lowerIntervals = (f32_t*)corrections;
    f32_t* upperIntervals = (f32_t*)(corrections + 4 * bulkSize);
    int16_t* targetComponentSums = (int16_t*)(corrections + 8 * bulkSize);
    f32_t* additionalCorrections = (f32_t*)(corrections + 10 * bulkSize);

    int i = 0;
    int upperBound = bulkSize & ~(sizeof(__m256) - 1);
    for (; i < upperBound; i += sizeof(__m256)) {

        __m256 additionalCorrection = _mm256_loadu_ps(additionalCorrections + i);
        __m256 res = score_inner(lowerIntervals + i, upperIntervals + i, targetComponentSums + i, scores + i, ay, ly, y1, dimensions);

        // For cosine, we need to apply the additional correction, which is
        // assumed to be the non-centered dot-product between the vector and the centroid
        res = _mm256_add_ps(_mm256_add_ps(res, additionalCorrection), _mm256_set1_ps(queryAdditionalCorrection - centroidDp));

        res = _mm256_max_ps(_mm256_mul_ps(_mm256_add_ps(_mm256_set1_ps(1.0f), res), _mm256_set1_ps(0.5f)), _mm256_setzero_ps());

        maxScore = fmax(maxScore, mm256_reduce_ps<_mm_max_ps>(res));
        _mm256_storeu_ps(scores + i, res);
    }

    for (; i < bulkSize; ++i) {
        f32_t score = score_others_inner(
            dimensions,
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryAdditionalCorrection,
            queryBitScale,
            centroidDp,
            *(lowerIntervals + i),
            *(upperIntervals + i),
            *(targetComponentSums + i),
            *(additionalCorrections + i),
            *(scores + i)
        );
        *(scores + i) = score;
        maxScore = fmax(maxScore, score);
    }

    return maxScore;
}
