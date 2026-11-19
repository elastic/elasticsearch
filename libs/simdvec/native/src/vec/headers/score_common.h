
#ifndef SCORE_COMMON_H
#define SCORE_COMMON_H

#include "vec.h"

struct corrections_t {
    const f32_t* lowerIntervals;
    const f32_t* upperIntervals;
    const int32_t* targetComponentSums;
    const f32_t* additionalCorrections;
};

struct bbq_correction_t {
    f32_t lowerInterval;
    f32_t upperInterval;
    f32_t additionalCorrection;
    int32_t targetComponentSum;
};

// Reads corrections from a dataset.
// Per-vector trailer layout where corrections are at offset (vectorSizeInBytes) for each address:
// (float lowerInterval, float upperInterval, float additionalCorrection, <targetComponentSum>)
// where <targetComponentSum> is either:
//   - 4 bytes (int32) when readComponentSumAsInt != 0, or
//   - 2 bytes (uint16, zero-extended to int32) when readComponentSumAsInt == 0 (legacy binary BBQ layout).
static inline bbq_correction_t bbq_read_corrections(
    const void *const data,
    int32_t vectorSizeInBytes,
    int8_t readComponentSumAsInt
) {
    const int8_t* base = (const int8_t*)data + vectorSizeInBytes;
    const int8_t* sumBase = base + 3 * sizeof(f32_t);
    return bbq_correction_t {
        *(const f32_t*)base,
        *(const f32_t*)(base + sizeof(f32_t)),
        *(const f32_t*)(base + 2 * sizeof(f32_t)),
        readComponentSumAsInt
            ? *(const int32_t*)sumBase
            : (int32_t)(*(const uint16_t*)sumBase)
    };
}

static inline corrections_t unpack_corrections(const int8_t* corrections, const int32_t bulkSize) {
    const f32_t* lowerIntervals = (f32_t*)corrections;
    const f32_t* upperIntervals = (f32_t*)(lowerIntervals + bulkSize);
    const int32_t* targetComponentSums = (int32_t*)(upperIntervals + bulkSize);
    const f32_t* additionalCorrections = (f32_t*)(targetComponentSums + bulkSize);

    return corrections_t { lowerIntervals, upperIntervals, targetComponentSums, additionalCorrections };
}

static inline f32_t apply_base_corrections_common(
    const int32_t dimensions,
    const f32_t queryLowerInterval,
    const f32_t queryUpperInterval,
    const int32_t queryComponentSum,
    const f32_t queryBitScale,
    const f32_t indexBitScale,
    const f32_t lowerInterval,
    const f32_t upperInterval,
    const int32_t targetComponentSum,
    const f32_t qcDist
) {
    const float ax = lowerInterval;
    const float lx = (upperInterval - ax) * indexBitScale;
    const float ay = queryLowerInterval;
    const float ly = (queryUpperInterval - ay) * queryBitScale;
    const float y1 = queryComponentSum;
    return ax * ay * dimensions + ay * lx * (float) targetComponentSum + ax * ly * y1 + lx * ly * qcDist;
}

// For euclidean, we need to invert the score and apply the additional correction, which is
// assumed to be the squared l2norm of the centroid centered vectors. The clamp to >= 0
// matches Lucene's reference scorer (VectorUtil.normalizeDistanceToUnitInterval(max(d, 0)))
// and guards against floating point error producing tiny negatives.
static inline f32_t euclidean_correction(
    const f32_t score,
    const f32_t queryAdditionalCorrection,
    const f32_t additionalCorrection
) {
    const f32_t finalScore = queryAdditionalCorrection + additionalCorrection - 2 * score;
    return 1.0f / (1.0f + __builtin_fmaxf(finalScore, 0.0f));
}

// TODO: align the Java diskbbq scorers with Lucene 10.4 (PR #15411) and drop this.
static inline f32_t legacy_euclidean_correction(
    const f32_t score,
    const f32_t queryAdditionalCorrection,
    const f32_t additionalCorrection
) {
    const f32_t finalScore = queryAdditionalCorrection + additionalCorrection - 2 * score;
    return __builtin_fmaxf(1.0f / (1.0f + finalScore), 0.0f);
}

// For max inner product, we need to apply the additional correction, which is
// assumed to be the non-centered dot-product between the vector and the centroid
static inline f32_t maximum_inner_product_correction(
    const f32_t score,
    const f32_t queryAdditionalCorrection,
    const f32_t additionalCorrection,
    const f32_t centroidDp
) {
    const f32_t finalScore = score + queryAdditionalCorrection + additionalCorrection - centroidDp;
    if (finalScore < 0.0f) {
        return 1.0f / (1.0f + -1.0f * finalScore);
    }
    return finalScore + 1.0f;
}

// For dot product we need to apply the additional correction, which is
// assumed to be the non-centered dot-product between the vector and the centroid.
// The clamp of score to [-1, 1] before normalization matches Lucene's reference
// (VectorUtil.normalizeToUnitInterval(clamp(dp, -1, 1))) and guards against
// floating point error producing values slightly outside [0, 1].
static inline f32_t dot_product_correction(
    const f32_t score,
    const f32_t queryAdditionalCorrection,
    const f32_t additionalCorrection,
    const f32_t centroidDp
) {
    const f32_t finalScore = score + queryAdditionalCorrection + additionalCorrection - centroidDp;
    const f32_t clampedScore = __builtin_fminf(__builtin_fmaxf(finalScore, -1.0f), 1.0f);
    return (1.0f + clampedScore) / 2.0f;
}

// Scalar Dot Product correction used by diskbbq_apply_corrections_dot_product_bulk
// in both architectures. Preserves the existing diskbbq normalization.
//
// TODO: align the Java diskbbq scorers with Lucene 10.4 (PR #15411) and drop this.
static inline f32_t legacy_dot_product_correction(
    const f32_t score,
    const f32_t queryAdditionalCorrection,
    const f32_t additionalCorrection,
    const f32_t centroidDp
) {
    const f32_t finalScore = score + queryAdditionalCorrection + additionalCorrection - centroidDp;
    return __builtin_fmaxf((1.0f + finalScore) / 2.0f, 0.0f);
}

#endif //SCORE_COMMON_H
