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
        f32_t* scores
) {
    f32_t maxScore = -std::numeric_limits<f32_t>::infinity();
    for (int i = 0; i < bulkSize; ++i) {
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
    f32_t maxScore = -std::numeric_limits<f32_t>::infinity();
    for (int i = 0; i < bulkSize; ++i) {
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
    f32_t maxScore = -std::numeric_limits<f32_t>::infinity();
    for (int i = 0; i < bulkSize; ++i) {
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
    f32_t maxScore = -std::numeric_limits<f32_t>::infinity();

    const corrections_t c = unpack_corrections(corrections, bulkSize);

    int i = 0;
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
    f32_t maxScore = -std::numeric_limits<f32_t>::infinity();

    const corrections_t c = unpack_corrections(corrections, bulkSize);

    int i = 0;
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
    f32_t maxScore = -std::numeric_limits<f32_t>::infinity();

    const corrections_t c = unpack_corrections(corrections, bulkSize);

    int i = 0;
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
