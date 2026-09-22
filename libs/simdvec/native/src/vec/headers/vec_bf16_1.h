
#ifndef VEC_BF16_1_H
#define VEC_BF16_1_H

#include "vec.h"

/*
 * Declaration of the base bf16 x bf16 squared-distance kernel defined in vec_bf16_1.cpp. The higher-capability
 * kernels evaluate |a-b|^2 as a.a + b.b - 2a.b with bf16 dot products, which loses precision for near-duplicate
 * pairs (the difference is dominated by rounding of a.a + b.b and can go negative). They fall back to this
 * kernel, which computes (a-b)^2 directly in f32 and is exact, to recompute the near-duplicate pairs.
 */
extern "C" f32_t vec_sqrDbf16Qbf16(const bf16_t* a, const bf16_t* b, const int32_t elementCount);

// Each term of a.a + b.b - 2a.b is rounded to f32 at the magnitude of a.a + b.b, so the difference carries an
// absolute error of a few ulps of that sum regardless of the true distance. A result below this fraction of
// a.a + b.b has more than roughly 2^-12 relative error and can even be negative, so it is recomputed exactly.
static constexpr f32_t SQR_BF16_CANCELLATION_GUARD = 1.0f / 4096.0f;

// Returns `result` unless it fell below the guard relative to `self` (a.a + b.b), in which case the exact base
// kernel recomputes the pair from `a` and `b`.
static inline f32_t sqr_bf16_recompute_if_needed(
    const f32_t self,
    const f32_t result,
    const bf16_t* a,
    const bf16_t* b,
    const int32_t elementCount
) {
    return result < self * SQR_BF16_CANCELLATION_GUARD ? vec_sqrDbf16Qbf16(a, b, elementCount) : result;
}

#endif // VEC_BF16_1_H
