
#ifndef SCORE_COMMON_H
#define SCORE_COMMON_H

#include "vec.h"

struct corrections_t {
    const f32_t* lowerIntervals;
    const f32_t* upperIntervals;
    const int32_t* targetComponentSums;
    const f32_t* additionalCorrections;
};

static inline corrections_t unpack_corrections(const int8_t* corrections, const int32_t bulkSize) {
    const f32_t* lowerIntervals = (f32_t*)corrections;
    const f32_t* upperIntervals = (f32_t*)(lowerIntervals + bulkSize);
    const int32_t* targetComponentSums = (int32_t*)(upperIntervals + bulkSize);
    const f32_t* additionalCorrections = (f32_t*)(targetComponentSums + bulkSize);

    return corrections_t { lowerIntervals, upperIntervals, targetComponentSums, additionalCorrections };
}

static inline f32_t apply_corrections_euclidean_inner(
    const int32_t dimensions,
    const f32_t queryLowerInterval,
    const f32_t queryUpperInterval,
    const int32_t queryComponentSum,
    const f32_t queryAdditionalCorrection,
    const f32_t queryBitScale,
    const f32_t indexBitScale,
    const f32_t centroidDp,
    const f32_t lowerInterval,
    const f32_t upperInterval,
    const int32_t targetComponentSum,
    const f32_t additionalCorrection,
    const f32_t qcDist
) {
    const float ax = lowerInterval;
    const float lx = (upperInterval - ax) * indexBitScale;
    const float ay = queryLowerInterval;
    const float ly = (queryUpperInterval - ay) * queryBitScale;
    const float y1 = queryComponentSum;
    float score = ax * ay * dimensions + ay * lx * (float) targetComponentSum + ax * ly * y1 + lx * ly * qcDist;
    // For euclidean, we need to invert the score and apply the additional correction, which is
    // assumed to be the squared l2norm of the centroid centered vectors.
    score = queryAdditionalCorrection + additionalCorrection - 2 * score;
    return fmax(1.0f / (1.0f + score), 0.0f);
}

static inline f32_t apply_corrections_maximum_inner_product_inner(
    const int32_t dimensions,
    const f32_t queryLowerInterval,
    const f32_t queryUpperInterval,
    const int32_t queryComponentSum,
    const f32_t queryAdditionalCorrection,
    const f32_t queryBitScale,
    const f32_t indexBitScale,
    const f32_t centroidDp,
    const f32_t lowerInterval,
    const f32_t upperInterval,
    const int32_t targetComponentSum,
    const f32_t additionalCorrection,
    const f32_t qcDist
) {
    const float ax = lowerInterval;
    const float lx = (upperInterval - ax) * indexBitScale;
    const float ay = queryLowerInterval;
    const float ly = (queryUpperInterval - ay) * queryBitScale;
    const float y1 = queryComponentSum;
    float score = ax * ay * dimensions + ay * lx * (float) targetComponentSum + ax * ly * y1 + lx * ly * qcDist;

    // For max inner product, we need to apply the additional correction, which is
    // assumed to be the non-centered dot-product between the vector and the centroid
    score += queryAdditionalCorrection + additionalCorrection - centroidDp;

    if (score < 0.0f) {
        return 1.0f / (1.0f + -1.0f * score);
    }
    return score + 1.0f;
}

static inline f32_t apply_corrections_dot_product_inner(
    const int32_t dimensions,
    const f32_t queryLowerInterval,
    const f32_t queryUpperInterval,
    const int32_t queryComponentSum,
    const f32_t queryAdditionalCorrection,
    const f32_t queryBitScale,
    const f32_t indexBitScale,
    const f32_t centroidDp,
    const f32_t lowerInterval,
    const f32_t upperInterval,
    const int32_t targetComponentSum,
    const f32_t additionalCorrection,
    const f32_t qcDist
) {
    const f32_t ax = lowerInterval;
    const f32_t lx = (upperInterval - ax) * indexBitScale;
    const f32_t ay = queryLowerInterval;
    const f32_t ly = (queryUpperInterval - ay) * queryBitScale;
    const f32_t y1 = queryComponentSum;
    f32_t score = ax * ay * dimensions + ay * lx * (f32_t) targetComponentSum + ax * ly * y1 + lx * ly * qcDist;

    // For dot product we need to apply the additional correction, which is
    // assumed to be the non-centered dot-product between the vector and the centroid
    score += queryAdditionalCorrection + additionalCorrection - centroidDp;
    return fmax((1.0f + score) / 2.0f, 0.0f);
}

#endif //SCORE_COMMON_H
