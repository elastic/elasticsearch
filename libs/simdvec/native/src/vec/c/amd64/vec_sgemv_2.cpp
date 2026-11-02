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
 * Int8 matrix-vector multiplication, result[rows] = A[rows x cols] @ v[cols], using AVX-512 VNNI.
 * VPDPBUSD multiplies unsigned bytes by signed bytes, so we use the identity
 *   a * v = (a + 128) * v - 128 * v
 * Each row of A is biased to unsigned by flipping the top bit (a ^ 0x80 == a + 128), and the 128 * sum(v) correction
 * is computed once per call and subtracted from every row. Arithmetic is modulo 2^32, so the result is exact.
 *
 * Rows are processed ROWS at a time, with one accumulator per row. Each load of v is used by every row in the group,
 * and the accumulators are independent, so consecutive VPDPBUSD instructions do not wait on each other.
 */

// number of rows processed at once - this determines the number of accumulators
constexpr int ROWS = 8;
// int8 values in one __m512i
constexpr int STEP = sizeof(__m512i);

// Returns 128 * sum(v), using the same XOR + SAD approach as the bulk i8 dot product.
static inline int32_t vector_correction(const int8_t* v, const int32_t cols) {
    const __m512i xor_mask = _mm512_set1_epi8((char)0x80);
    const __m512i zeros = _mm512_setzero_si512();
    __m512i sum_biased = _mm512_setzero_si512();
    int j = 0;
    for (; j + STEP <= cols; j += STEP) {
        const __m512i vv = _mm512_loadu_si512(v + j);
        sum_biased = _mm512_add_epi64(sum_biased, _mm512_sad_epu8(_mm512_xor_si512(vv, xor_mask), zeros));
    }
    if (j < cols) {
        const __mmask64 mask = (__mmask64)_bzhi_u64(0xFFFFFFFFFFFFFFFFULL, cols - j);
        const __m512i vv = _mm512_maskz_loadu_epi8(mask, v + j);
        // the masked XOR leaves unused lanes at zero, so they add nothing
        sum_biased = _mm512_add_epi64(
            sum_biased,
            _mm512_sad_epu8(_mm512_xor_si512(vv, _mm512_maskz_set1_epi8(mask, (char)0x80)), zeros)
        );
    }
    // sum(v + 128) - 128 * cols == sum(v)
    const int64_t sum_v = _mm512_reduce_add_epi64(sum_biased) - 128LL * cols;
    return (int32_t)(128LL * sum_v);
}

// Computes rows [i, i + R) of the result.
template <int R>
static inline void multiply_rows(
    const int8_t* a,
    const int8_t* v,
    const int32_t cols,
    const int32_t correction,
    int32_t* result,
    const int i
) {
    const __m512i xor_mask = _mm512_set1_epi8((char)0x80);
    const int8_t* aRows[R];
    __m512i acc[R];
    apply_indexed<R>([&](auto I) {
        aRows[I] = a + (int64_t)(i + I) * cols;
        acc[I] = _mm512_setzero_si512();
    });

    int j = 0;
    for (; j + STEP <= cols; j += STEP) {
        const __m512i vv = _mm512_loadu_si512(v + j);
        apply_indexed<R>([&](auto I) {
            const __m512i av = _mm512_loadu_si512(aRows[I] + j);
            acc[I] = _mm512_dpbusd_epi32(acc[I], _mm512_xor_si512(av, xor_mask), vv);
        });
    }

    // Column tail. Unused lanes of v are zero, so whatever bias the masked-off lanes of a get contributes nothing.
    if (j < cols) {
        const __mmask64 mask = (__mmask64)_bzhi_u64(0xFFFFFFFFFFFFFFFFULL, cols - j);
        const __m512i vv = _mm512_maskz_loadu_epi8(mask, v + j);
        apply_indexed<R>([&](auto I) {
            const __m512i av = _mm512_maskz_loadu_epi8(mask, aRows[I] + j);
            acc[I] = _mm512_dpbusd_epi32(acc[I], _mm512_xor_si512(av, xor_mask), vv);
        });
    }

    apply_indexed<R>([&](auto I) {
        result[i + I] = _mm512_reduce_add_epi32(acc[I]) - correction;
    });
}

EXPORT void vec_sgemv_i8_2(const int8_t* a, const int8_t* v, const int32_t rows, const int32_t cols, int32_t* result) {
    const int32_t correction = vector_correction(v, cols);
    int i = 0;
    for (; i + ROWS <= rows; i += ROWS) {
        multiply_rows<ROWS>(a, v, cols, correction, result, i);
    }
    // row tail, one row at a time to avoid instantiating every smaller group size
    for (; i < rows; ++i) {
        multiply_rows<1>(a, v, cols, correction, result, i);
    }
}
