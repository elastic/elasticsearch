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
#include <arm_neon.h>
#include "vec.h"
#include "vec_common.h"
#include "aarch64/aarch64_vec_common.h"

/*
 * Int8 matrix-vector multiplication, result[rows] = A[rows x cols] @ v[cols], using the SDOT (dotprod) instruction,
 * which multiplies groups of 4 int8 values and accumulates them into int32 lanes.
 *
 * Rows are processed ROWS at a time, with one accumulator per row. Each load of v is used by every row in the group,
 * and the accumulators are independent, so consecutive SDOT instructions do not wait on each other.
 */

// number of rows processed at once - this determines the number of accumulators
constexpr int ROWS = 8;
// int8 values in one int8x16 value
constexpr int STEP = 16;

// Computes rows [i, i + R) of the result.
template <int R>
static inline void multiply_rows(const int8_t* a, const int8_t* v, const int32_t cols, int32_t* result, const int i) {
    const int8_t* aRows[R];
    int32x4_t acc[R];
    apply_indexed<R>([&](auto I) {
        aRows[I] = a + (int64_t)(i + I) * cols;
        acc[I] = vdupq_n_s32(0);
    });

    int j = 0;
    for (; j + STEP <= cols; j += STEP) {
        const int8x16_t vv = vld1q_s8(v + j);
        apply_indexed<R>([&](auto I) {
            acc[I] = vdotq_s32(acc[I], vld1q_s8(aRows[I] + j), vv);
        });
    }

    apply_indexed<R>([&](auto I) {
        int32_t sum = vaddvq_s32(acc[I]);
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
