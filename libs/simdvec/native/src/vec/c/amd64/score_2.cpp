/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// This file contains implementations for transforming distances into scores,
// applying additional corrections. It includes support for "2nd tier" vector
// capabilities; in the case of x64, this tier include functions for processors
// supporting at least AVX-512 with VNNI and VPOPCNT.

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <math.h>
#include <limits>

// Force the preprocessor to pick up AVX-512 intrinsics, and the compiler to emit AVX-512 code
#ifdef __clang__
#pragma clang attribute push(__attribute__((target("arch=icelake-client"))), apply_to=function)
#elif __GNUC__
#pragma GCC push_options
#pragma GCC target ("arch=icelake-client")
#endif

#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"
#include "score_common.h"

static inline __m512 apply_base_corrections(
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
    const __m512 ax = _mm512_loadu_ps(lowerInterval);
    const __m512 lx = _mm512_mul_ps(_mm512_sub_ps(_mm512_loadu_ps(upperInterval), ax), _mm512_set1_ps(indexBitScale));
    const __m512 tcs = _mm512_cvtepi32_ps(_mm512_loadu_si512((const __m512i*)(targetComponentSum)));

    const __m512 qcDist = _mm512_loadu_ps(score);

    // ax * ay * dimensions + ay * lx * (float) targetComponentSum + ax * ly * y1 + lx * ly * qcDist;
    // ax * ay * dimensions
    const __m512 res1 = _mm512_mul_ps(ax, _mm512_set1_ps(ay * dimensions));
    // ay * lx * (float) targetComponentSum
    const __m512 res2 = _mm512_mul_ps(_mm512_mul_ps(lx, _mm512_set1_ps(ay)), tcs);
    // ax * ly * y1
    const __m512 res3 = _mm512_mul_ps(ax, _mm512_set1_ps(ly * y1));
    // lx * ly * qcDist
    const __m512 res4 = _mm512_mul_ps(_mm512_mul_ps(lx, _mm512_set1_ps(ly)), qcDist);
    return _mm512_add_ps(_mm512_add_ps(res1, res2), _mm512_add_ps(res3, res4));
}

EXPORT f32_t diskbbq_apply_corrections_maximum_inner_product_bulk_2(
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

    __m512 max_score = _mm512_set1_ps(-std::numeric_limits<f32_t>::infinity());

    const corrections_t c = unpack_corrections(corrections, bulkSize);

    int i = 0;
    constexpr int floats_per_cycle = sizeof(__m512) / sizeof(f32_t);
    const int upperBound = bulkSize & ~(floats_per_cycle - 1);
    for (; i < upperBound; i += floats_per_cycle) {
        __m512 res = apply_base_corrections(
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
        __m512 additionalCorrection = _mm512_loadu_ps(c.additionalCorrections + i);
        res = _mm512_add_ps(
            _mm512_add_ps(res, additionalCorrection),
            _mm512_set1_ps(queryAdditionalCorrection - centroidDp)
        );

        __mmask16 is_neg_mask = _mm512_fpclass_ps_mask(res, 0x50);
        __m512 negative_scaled = _mm512_rcp14_ps(_mm512_fnmadd_ps(_mm512_set1_ps(1.0f), res, _mm512_set1_ps(1.0f)));
        __m512 positive_scaled = _mm512_add_ps(_mm512_set1_ps(1.0f), res);

        res = _mm512_mask_mov_ps(positive_scaled, is_neg_mask, negative_scaled);

        max_score = _mm512_max_ps(max_score, res);
        _mm512_storeu_ps(scores + i, res);
    }
    f32_t maxScore = _mm512_reduce_max_ps(max_score);

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

#ifdef __clang__
#pragma clang attribute pop
#elif __GNUC__
#pragma GCC pop_options
#endif

