/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// This file contains implementations for BBQ vector operations,
// including support for "2nd tier" vector capabilities; in the case of x64,
// this tier include functions for processors supporting at least AVX-512 with VPOPCNTDQ.

#include <stdint.h>
#include <algorithm>
#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"
#include "amd64/amd64_bbq_common.h"

static inline int64_t dotd1q4_inner_avx512(const int8_t* a, const int8_t* q, const int32_t length) {
    constexpr int query_bits = 4;

    __m512i acc[query_bits];
    apply_indexed<query_bits>([&](auto I) {
        acc[I] = _mm512_setzero_si512();
    });

    int r = 0;
    int upperBound = length & ~(sizeof(__m512i) - 1);
    for (; r < upperBound; r += sizeof(__m512i)) {
        __m512i value = _mm512_loadu_si512((const __m512i_u*)(a + r));
        apply_indexed<query_bits>([&](auto I) {
            __m512i res = _mm512_popcnt_epi64(
                _mm512_and_si512(
                    value,
                    _mm512_loadu_si512((const __m512i_u*)(q + r + I * length))
                )
            );
            acc[I] = _mm512_add_epi64(acc[I], res);
        });
    }

    // Handle remaining bytes with masked ops
    const int remaining = length - r;
    if (remaining > 0) {
        const __mmask64 mask = (1ULL << remaining) - 1;
        __m512i value = _mm512_maskz_loadu_epi8(mask, a + r);
        apply_indexed<query_bits>([&](auto I) {
            __m512i q_val = _mm512_maskz_loadu_epi8(mask, q + r + I * length);
            acc[I] = _mm512_add_epi64(acc[I],
                _mm512_popcnt_epi64(_mm512_and_si512(value, q_val)));
        });
    }

    int sum = 0;
    apply_indexed<query_bits>([&](auto I) {
        sum += (_mm512_reduce_add_epi64(acc[I]) << I);
    });
    return sum;
}

EXPORT int64_t vec_dotd1q4_2(
    const int8_t* a_ptr,
    const int8_t* query_ptr,
    const int32_t length
) {
    return dotd1q4_inner_avx512(a_ptr, query_ptr, length);
}

EXPORT void vec_dotd1q4_bulk_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1q4_inner_bulk<int8_t, sequential_mapper, dotd1q4_inner_avx512>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd1q4_bulk_offsets_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dotd1q4_inner_bulk<int8_t, offsets_mapper, dotd1q4_inner_avx512>(a, query, length, pitch, offsets, count, results);
}

EXPORT void vec_dotd1q4_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1q4_inner_bulk<const int8_t*, sparse_mapper, dotd1q4_inner_avx512>
        ((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT int64_t vec_dotd2q4_2(
    const int8_t* a_ptr,
    const int8_t* query_ptr,
    const int32_t length
) {
    int64_t lower = dotd1q4_inner_avx512(a_ptr, query_ptr, length/2);
    int64_t upper = dotd1q4_inner_avx512(a_ptr + length/2, query_ptr, length/2);
    return lower + (upper << 1);
}

EXPORT void vec_dotd2q4_bulk_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<int8_t, sequential_mapper, dotd1q4_inner_avx512>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd2q4_bulk_offsets_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<int8_t, offsets_mapper, dotd1q4_inner_avx512>(a, query, length, pitch, offsets, count, results);
}

EXPORT void vec_dotd2q4_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<const int8_t*, sparse_mapper, dotd1q4_inner_avx512>
        ((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT int64_t vec_dotd4q4_2(const int8_t* a, const int8_t* query, const int32_t length) {
    const int32_t bit_length = length / 4;
    int64_t p0 = dotd1q4_inner_avx512(a + 0 * bit_length, query, bit_length);
    int64_t p1 = dotd1q4_inner_avx512(a + 1 * bit_length, query, bit_length);
    int64_t p2 = dotd1q4_inner_avx512(a + 2 * bit_length, query, bit_length);
    int64_t p3 = dotd1q4_inner_avx512(a + 3 * bit_length, query, bit_length);
    return p0 + (p1 << 1) + (p2 << 2) + (p3 << 3);
}

EXPORT void vec_dotd4q4_bulk_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<int8_t, sequential_mapper, dotd1q4_inner_avx512>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd4q4_bulk_offsets_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<int8_t, offsets_mapper, dotd1q4_inner_avx512>(a, query, length, pitch, offsets, count, results);
}

EXPORT void vec_dotd4q4_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<const int8_t*, sparse_mapper, dotd1q4_inner_avx512>
        ((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}
