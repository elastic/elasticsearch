
#ifndef SCORE_COMMON_H
#define SCORE_COMMON_H

#include "vec.h"

static inline f32_t score_euclidean_inner(
    int32_t dimensions,
    f32_t queryLowerInterval,
    f32_t queryUpperInterval,
    int32_t queryComponentSum,
    f32_t queryAdditionalCorrection,
    f32_t queryBitScale,
    f32_t centroidDp,
    f32_t lowerInterval,
    f32_t upperInterval,
    int32_t targetComponentSum,
    f32_t additionalCorrection,
    f32_t qcDist
) {
    float ax = lowerInterval;
    // Here we assume `lx` is simply bit vectors, so the scaling isn't necessary
    float lx = (upperInterval - ax);
    float ay = queryLowerInterval;
    float ly = (queryUpperInterval - ay) * queryBitScale;
    float y1 = queryComponentSum;
    float score = ax * ay * dimensions + ay * lx * (float) targetComponentSum + ax * ly * y1 + lx * ly * qcDist;
    // For euclidean, we need to invert the score and apply the additional correction, which is
    // assumed to be the squared l2norm of the centroid centered vectors.
    score = queryAdditionalCorrection + additionalCorrection - 2 * score;
    return fmax(1.0f / (1.0f + score), 0.0f);
}

static inline f32_t score_maximum_inner_product_inner(
    int32_t dimensions,
    f32_t queryLowerInterval,
    f32_t queryUpperInterval,
    int32_t queryComponentSum,
    f32_t queryAdditionalCorrection,
    f32_t queryBitScale,
    f32_t centroidDp,
    f32_t lowerInterval,
    f32_t upperInterval,
    int32_t targetComponentSum,
    f32_t additionalCorrection,
    f32_t qcDist
) {
    float ax = lowerInterval;
    // Here we assume `lx` is simply bit vectors, so the scaling isn't necessary
    float lx = (upperInterval - ax);
    float ay = queryLowerInterval;
    float ly = (queryUpperInterval - ay) * queryBitScale;
    float y1 = queryComponentSum;
    float score = ax * ay * dimensions + ay * lx * (float) targetComponentSum + ax * ly * y1 + lx * ly * qcDist;

    // For max inner product, we need to apply the additional correction, which is
    // assumed to be the non-centered dot-product between the vector and the centroid
    score += queryAdditionalCorrection + additionalCorrection - centroidDp;

    if (score < 0.0f) {
        return 1.0f / (1.0f + -1.0f * score);
    }
    return score + 1.0f;
}

static inline f32_t score_dot_product_inner(
    int32_t dimensions,
    f32_t queryLowerInterval,
    f32_t queryUpperInterval,
    int32_t queryComponentSum,
    f32_t queryAdditionalCorrection,
    f32_t queryBitScale,
    f32_t centroidDp,
    f32_t lowerInterval,
    f32_t upperInterval,
    int32_t targetComponentSum,
    f32_t additionalCorrection,
    f32_t qcDist
) {
    f32_t ax = lowerInterval;
    // Here we assume `lx` is simply bit vectors, so the scaling isn't necessary
    f32_t lx = (upperInterval - ax);
    f32_t ay = queryLowerInterval;
    f32_t ly = (queryUpperInterval - ay) * queryBitScale;
    f32_t y1 = queryComponentSum;
    f32_t score = ax * ay * dimensions + ay * lx * (f32_t) targetComponentSum + ax * ly * y1 + lx * ly * qcDist;

    // For cosine, we need to apply the additional correction, which is
    // assumed to be the non-centered dot-product between the vector and the centroid
    score += queryAdditionalCorrection + additionalCorrection - centroidDp;
    return fmax((1.0f + score) / 2.0f, 0.0f);
}

#endif //SCORE_COMMON_H
