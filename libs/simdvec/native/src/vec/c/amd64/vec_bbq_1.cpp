/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// This file contains implementations for BBQ vector operations,
// including support for "1st tier" vector capabilities; in the case of x64,
// this first tier include functions for processors supporting at least AVX2.

#include <stdint.h>
#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"
#include "amd64/amd64_bbq_common.h"

// Fast AVX2 popcount, based on "Faster Population Counts Using AVX2 Instructions"
// See https://arxiv.org/abs/1611.07612 and https://github.com/WojciechMula/sse-popcount
static inline __m256i dot_bit_256(const __m256i a, const int8_t* b) {
    const __m256i lookup = _mm256_setr_epi8(
        /* 0 */ 0, /* 1 */ 1, /* 2 */ 1, /* 3 */ 2,
        /* 4 */ 1, /* 5 */ 2, /* 6 */ 2, /* 7 */ 3,
        /* 8 */ 1, /* 9 */ 2, /* a */ 2, /* b */ 3,
        /* c */ 2, /* d */ 3, /* e */ 3, /* f */ 4,

        /* 0 */ 0, /* 1 */ 1, /* 2 */ 1, /* 3 */ 2,
        /* 4 */ 1, /* 5 */ 2, /* 6 */ 2, /* 7 */ 3,
        /* 8 */ 1, /* 9 */ 2, /* a */ 2, /* b */ 3,
        /* c */ 2, /* d */ 3, /* e */ 3, /* f */ 4
    );

    const __m256i low_mask = _mm256_set1_epi8(0x0f);

    __m256i local = _mm256_setzero_si256();
    __m256i q0 = _mm256_loadu_si256((const __m256i_u*)b);
    __m256i vec = _mm256_and_si256(q0, a);

   const __m256i lo  = _mm256_and_si256(vec, low_mask);
   const __m256i hi  = _mm256_and_si256(_mm256_srli_epi16(vec, 4), low_mask);
   const __m256i popcnt1 = _mm256_shuffle_epi8(lookup, lo);
   const __m256i popcnt2 = _mm256_shuffle_epi8(lookup, hi);
   local = _mm256_add_epi8(local, popcnt1);
   local = _mm256_add_epi8(local, popcnt2);
   return local;
}

static inline int64_t dotd1q4_inner(const int8_t* a, const int8_t* q, const int32_t length) {
    constexpr int query_bits = 4;

    __m256i acc[query_bits];
    apply_indexed<query_bits>([&](auto I) {
        acc[I] = _mm256_setzero_si256();
    });

    int r = 0;
    int upperBound = length & ~(sizeof(__m256i) - 1);
    for (; r < upperBound; r += sizeof(__m256i)) {
        __m256i value = _mm256_loadu_si256((const __m256i_u*)(a + r));
        apply_indexed<query_bits>([&](auto I) {
            __m256i res = dot_bit_256(value, q + r + I * length);
            acc[I] = _mm256_add_epi64(acc[I], _mm256_sad_epu8(res, _mm256_setzero_si256()));
        });
    }

    int64_t bit_result[query_bits];
    apply_indexed<query_bits>([&](auto I) {
        bit_result[I] = mm256_reduce_epi64<_mm_add_epi64>(acc[I]);
    });

    // switch to 32-bit ops
    upperBound = length & ~(sizeof(int32_t) - 1);
    for (; r < upperBound; r += sizeof(int32_t)) {
        int32_t value = *((int32_t*)(a + r));
        apply_indexed<query_bits>([&](auto I) {
            int32_t bits = *((int32_t*)(q + r + I * length));
            bit_result[I] += __builtin_popcount(bits & value);
        });
    }
    // then single bytes
    for (; r < length; r++) {
        int8_t value = *(a + r);
        apply_indexed<query_bits>([&](auto I) {
            int32_t bits = *(q + r + I * length);
            bit_result[I] += __builtin_popcount(bits & value & 0xFF);
        });
    }
    int sum = 0;
    apply_indexed<query_bits>([&](auto I) {
        sum += (bit_result[I] << I);
    });
    return sum;
}

EXPORT int64_t vec_dotd1q4(
    const int8_t* a_ptr,
    const int8_t* query_ptr,
    const int32_t length
) {
    return dotd1q4_inner(a_ptr, query_ptr, length);
}

EXPORT void vec_dotd1q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1q4_inner_bulk<int8_t, sequential_mapper, dotd1q4_inner>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd1q4_bulk_offsets(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dotd1q4_inner_bulk<int8_t, offsets_mapper, dotd1q4_inner>(a, query, length, pitch, offsets, count, results);
}

EXPORT void vec_dotd1q4_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1q4_inner_bulk<const int8_t*, sparse_mapper, dotd1q4_inner>
        ((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT int64_t vec_dotd2q4(
    const int8_t* a_ptr,
    const int8_t* query_ptr,
    const int32_t length
) {
    int64_t lower = dotd1q4_inner(a_ptr, query_ptr, length/2);
    int64_t upper = dotd1q4_inner(a_ptr + length/2, query_ptr, length/2);
    return lower + (upper << 1);
}

EXPORT void vec_dotd2q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<int8_t, sequential_mapper, dotd1q4_inner>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd2q4_bulk_offsets(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<int8_t, offsets_mapper, dotd1q4_inner>(a, query, length, pitch, offsets, count, results);
}

EXPORT void vec_dotd2q4_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<const int8_t*, sparse_mapper, dotd1q4_inner>
        ((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT int64_t vec_dotd4q4(const int8_t* a, const int8_t* query, const int32_t length) {
    const int32_t bit_length = length / 4;
    int64_t p0 = dotd1q4_inner(a + 0 * bit_length, query, bit_length);
    int64_t p1 = dotd1q4_inner(a + 1 * bit_length, query, bit_length);
    int64_t p2 = dotd1q4_inner(a + 2 * bit_length, query, bit_length);
    int64_t p3 = dotd1q4_inner(a + 3 * bit_length, query, bit_length);
    return p0 + (p1 << 1) + (p2 << 2) + (p3 << 3);
}

EXPORT void vec_dotd4q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<int8_t, sequential_mapper, dotd1q4_inner>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd4q4_bulk_offsets(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<int8_t, offsets_mapper, dotd1q4_inner>(a, query, length, pitch, offsets, count, results);
}

EXPORT void vec_dotd4q4_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<const int8_t*, sparse_mapper, dotd1q4_inner>
        ((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}
