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
#include <limits>
#include "vec.h"
#include "vec_common.h"
#include "aarch64/aarch64_vec_common.h"

#include "score_common.h"

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
    f32_t maxScore = -std::numeric_limits<f32_t>::infinity();

    f32_t* lowerIntervals = (f32_t*)corrections;
    f32_t* upperIntervals = (f32_t*)(corrections + 4 * bulkSize);
    int16_t* targetComponentSums = (int16_t*)(corrections + 8 * bulkSize);
    f32_t* additionalCorrections = (f32_t*)(corrections + 10 * bulkSize);

    int i = 0;
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
    f32_t maxScore = -std::numeric_limits<f32_t>::infinity();

    f32_t* lowerIntervals = (f32_t*)corrections;
    f32_t* upperIntervals = (f32_t*)(corrections + 4 * bulkSize);
    int16_t* targetComponentSums = (int16_t*)(corrections + 8 * bulkSize);
    f32_t* additionalCorrections = (f32_t*)(corrections + 10 * bulkSize);

    int i = 0;
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
    f32_t maxScore = -std::numeric_limits<f32_t>::infinity();

    f32_t* lowerIntervals = (f32_t*)corrections;
    f32_t* upperIntervals = (f32_t*)(corrections + 4 * bulkSize);
    int16_t* targetComponentSums = (int16_t*)(corrections + 8 * bulkSize);
    f32_t* additionalCorrections = (f32_t*)(corrections + 10 * bulkSize);

    int i = 0;
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
