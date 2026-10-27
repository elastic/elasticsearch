/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

#include <stddef.h>
#include <stdint.h>
#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"

/*
 * Int8 matrix-vector multiplication, result[rows] = A[rows x cols] @ v[cols], using AVX2.
 * AVX2 has no exact signed 8-bit dot product, so values are widened to int16,
 * and the VPMADDWD instruction multiplies pairs of int16 values and adds each pair into an int32 lane.
 *
 * Rows are processed ROWS at a time, with one accumulator per row. Each widened chunk of v is used by every row
 * in the group, and the accumulators are independent, so consecutive multiply-adds do not wait on each other.
 */

// number of rows processed at once - this determines the number of accumulators
constexpr int ROWS = 8;
// int8 values widened into one __m256i
constexpr int STEP = sizeof(__m128i);

// Computes rows [i, i + R) of the result.
template <int R>
static inline void multiply_rows(const int8_t* a, const int8_t* v, const int32_t cols, int32_t* result, const int i) {
    const int8_t* aRows[R];
    __m256i acc[R];
    apply_indexed<R>([&](auto I) {
        aRows[I] = a + (int64_t)(i + I) * cols;
        acc[I] = _mm256_setzero_si256();
    });

    int j = 0;
    for (; j + STEP <= cols; j += STEP) {
        const __m256i v16 = _mm256_cvtepi8_epi16(_mm_loadu_si128((const __m128i*)(v + j)));
        apply_indexed<R>([&](auto I) {
            const __m256i a16 = _mm256_cvtepi8_epi16(_mm_loadu_si128((const __m128i*)(aRows[I] + j)));
            acc[I] = _mm256_add_epi32(_mm256_madd_epi16(a16, v16), acc[I]);
        });
    }

    apply_indexed<R>([&](auto I) {
        int32_t sum = mm256_reduce_epi32<_mm_add_epi32>(acc[I]);
        // column tail
        for (int t = j; t < cols; ++t) {
            sum += aRows[I][t] * v[t];
        }
        result[i + I] = sum;
    });
}

EXPORT void vec_sgemv_i8(const int8_t* a, const int8_t* v, const int32_t rows, const int32_t cols, int32_t* result) {
    int i = 0;
    for (; i + ROWS <= rows; i += ROWS) {
        multiply_rows<ROWS>(a, v, cols, result, i);
    }
    // row tail, one row at a time to avoid instantiating every smaller group size
    for (; i < rows; ++i) {
        multiply_rows<1>(a, v, cols, result, i);
    }
}
