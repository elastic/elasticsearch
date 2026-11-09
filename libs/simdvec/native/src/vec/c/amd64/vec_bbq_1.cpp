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
// Per-byte popcount via a nibble lookup.
static inline __m256i popcnt_bytes_256(const __m256i v) {
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
    const __m256i lo = _mm256_and_si256(v, low_mask);
    const __m256i hi = _mm256_and_si256(_mm256_srli_epi16(v, 4), low_mask);
    return _mm256_add_epi8(_mm256_shuffle_epi8(lookup, lo), _mm256_shuffle_epi8(lookup, hi));
}

static inline __m128i popcnt_bytes_128(const __m128i v) {
    const __m128i lookup = _mm_setr_epi8(0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4);
    const __m128i low_mask = _mm_set1_epi8(0x0f);
    const __m128i lo = _mm_and_si128(v, low_mask);
    const __m128i hi = _mm_and_si128(_mm_srli_epi16(v, 4), low_mask);
    return _mm_add_epi8(_mm_shuffle_epi8(lookup, lo), _mm_shuffle_epi8(lookup, hi));
}

static inline __m256i dot_bit_256(const __m256i a, const int8_t* b) {
    return popcnt_bytes_256(_mm256_and_si256(_mm256_loadu_si256((const __m256i_u*)b), a));
}

template<int query_bits>
static inline int64_t dotd1qN_inner(const int8_t* a, const int8_t* q, const int32_t length) {
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

// 2-bit doc x 4-bit query, both doc bit planes in one pass. The doc is two contiguous planes of
// length/2 bytes (low bit, then high bit), the query four planes of length/2 bytes. Per byte, the doc
// planes combine as popcnt(lo & q) + 2 * popcnt(hi & q) <= 24 and two query planes as p0 + 2 * p1 <= 72,
// so the weighting happens in u8 before the SAD: two accumulators, two SADs per step, and a single
// horizontal reduction per vector.
static inline int64_t dotd2q4_inner_avx2(const int8_t* a, const int8_t* q, const int32_t length) {
    constexpr int query_bits = 4;
    constexpr int m128_bytes = sizeof(__m128i);
    const int32_t bit_length = length / 2;
    const int8_t* a_hi = a + bit_length;
    const int8_t* q0 = q;
    const int8_t* q1 = q + bit_length;
    const int8_t* q2 = q + 2 * bit_length;
    const int8_t* q3 = q + 3 * bit_length;

    __m256i acc01 = _mm256_setzero_si256();
    __m256i acc23 = _mm256_setzero_si256();

    int r = 0;
    const int upperBound = bit_length & ~(sizeof(__m256i) - 1);
    for (; r < upperBound; r += sizeof(__m256i)) {
        const __m256i doc_lo = _mm256_loadu_si256((const __m256i_u*)(a + r));
        const __m256i doc_hi = _mm256_loadu_si256((const __m256i_u*)(a_hi + r));
        const auto plane = [&](const int8_t* qp) {
            const __m256i q_val = _mm256_loadu_si256((const __m256i_u*)(qp + r));
            const __m256i pl = popcnt_bytes_256(_mm256_and_si256(doc_lo, q_val));
            const __m256i ph = popcnt_bytes_256(_mm256_and_si256(doc_hi, q_val));
            return _mm256_add_epi8(pl, _mm256_add_epi8(ph, ph));
        };
        const __m256i p0 = plane(q0);
        const __m256i p1 = plane(q1);
        const __m256i p2 = plane(q2);
        const __m256i p3 = plane(q3);
        acc01 = _mm256_add_epi64(acc01, _mm256_sad_epu8(_mm256_add_epi8(p0, _mm256_add_epi8(p1, p1)), _mm256_setzero_si256()));
        acc23 = _mm256_add_epi64(acc23, _mm256_sad_epu8(_mm256_add_epi8(p2, _mm256_add_epi8(p3, p3)), _mm256_setzero_si256()));
    }

    if (bit_length - r >= m128_bytes) {
        const __m128i doc_lo = _mm_loadu_si128((const __m128i_u*)(a + r));
        const __m128i doc_hi = _mm_loadu_si128((const __m128i_u*)(a_hi + r));
        const auto plane = [&](const int8_t* qp) {
            const __m128i q_val = _mm_loadu_si128((const __m128i_u*)(qp + r));
            const __m128i pl = popcnt_bytes_128(_mm_and_si128(doc_lo, q_val));
            const __m128i ph = popcnt_bytes_128(_mm_and_si128(doc_hi, q_val));
            return _mm_add_epi8(pl, _mm_add_epi8(ph, ph));
        };
        const __m128i p0 = plane(q0);
        const __m128i p1 = plane(q1);
        const __m128i p2 = plane(q2);
        const __m128i p3 = plane(q3);
        acc01 = _mm256_add_epi64(acc01, _mm256_zextsi128_si256(_mm_sad_epu8(_mm_add_epi8(p0, _mm_add_epi8(p1, p1)), _mm_setzero_si128())));
        acc23 = _mm256_add_epi64(acc23, _mm256_zextsi128_si256(_mm_sad_epu8(_mm_add_epi8(p2, _mm_add_epi8(p3, p3)), _mm_setzero_si128())));
        r += m128_bytes;
    }

    int64_t sum = mm256_reduce_epi64<_mm_add_epi64>(_mm256_add_epi64(acc01, _mm256_slli_epi64(acc23, 2)));

    // switch to 32-bit ops
    const int upperBound32 = bit_length & ~(sizeof(int32_t) - 1);
    for (; r < upperBound32; r += sizeof(int32_t)) {
        const int32_t value_lo = *((int32_t*)(a + r));
        const int32_t value_hi = *((int32_t*)(a_hi + r));
        apply_indexed<query_bits>([&](auto I) {
            const int32_t bits = *((int32_t*)(q + r + I * bit_length));
            sum += (int64_t)(__builtin_popcount(bits & value_lo) + (__builtin_popcount(bits & value_hi) << 1)) << I;
        });
    }
    // then single bytes
    for (; r < bit_length; r++) {
        const int8_t value_lo = *(a + r);
        const int8_t value_hi = *(a_hi + r);
        apply_indexed<query_bits>([&](auto I) {
            const int32_t bits = *(q + r + I * bit_length);
            sum += (int64_t)(__builtin_popcount(bits & value_lo & 0xFF) + (__builtin_popcount(bits & value_hi & 0xFF) << 1)) << I;
        });
    }
    return sum;
}

EXPORT int64_t vec_dotd1q4(
    const int8_t* a_ptr,
    const int8_t* query_ptr,
    const int32_t length
) {
    return dotd1qN_inner<4>(a_ptr, query_ptr, length);
}

EXPORT int64_t vec_dotd1q1(
    const int8_t* a_ptr,
    const int8_t* query_ptr,
    const int32_t length
) {
    return dotd1qN_inner<1>(a_ptr, query_ptr, length);
}

EXPORT void vec_dotd1q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1q_inner_bulk<int8_t, sequential_mapper, dotd1qN_inner<4>>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd1q4_bulk_masked(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t mask,
    const int32_t count,
    f32_t* results) {
    dotd1q_inner_bulk<int8_t, masked_mapper, dotd1qN_inner<4>, masked_output>(
        a, query, length, pitch, &mask, __builtin_popcount((uint32_t)mask), results);
}

EXPORT void vec_dotd1q4_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1q_inner_bulk<const int8_t*, sparse_mapper, dotd1qN_inner<4>>
        ((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT void vec_dotd1q1_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1q_inner_bulk<int8_t, sequential_mapper, dotd1qN_inner<1>>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd1q1_bulk_masked(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t mask,
    const int32_t count,
    f32_t* results) {
    dotd1q_inner_bulk<int8_t, masked_mapper, dotd1qN_inner<1>, masked_output>(
        a, query, length, pitch, &mask, __builtin_popcount((uint32_t)mask), results);
}

EXPORT void vec_dotd1q1_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1q_inner_bulk<const int8_t*, sparse_mapper, dotd1qN_inner<1>>
        ((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT int64_t vec_dotd2q2(
    const int8_t* a_ptr,
    const int8_t* query_ptr,
    const int32_t length
) {
    int64_t lower = dotd1qN_inner<2>(a_ptr, query_ptr, length/2);
    int64_t upper = dotd1qN_inner<2>(a_ptr + length/2, query_ptr, length/2);
    return lower + (upper << 1);
}

EXPORT void vec_dotd2q2_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q_inner_bulk<int8_t, sequential_mapper, dotd1qN_inner<2>>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd2q2_bulk_masked(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t mask,
    const int32_t count,
    f32_t* results) {
    dotd2q_inner_bulk<int8_t, masked_mapper, dotd1qN_inner<2>, masked_output>(
        a, query, length, pitch, &mask, __builtin_popcount((uint32_t)mask), results);
}

EXPORT void vec_dotd2q2_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q_inner_bulk<const int8_t*, sparse_mapper, dotd1qN_inner<2>>
        ((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT int64_t vec_dotd2q4(
    const int8_t* a_ptr,
    const int8_t* query_ptr,
    const int32_t length
) {
    return dotd2q4_inner_avx2(a_ptr, query_ptr, length);
}

EXPORT void vec_dotd2q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<int8_t, sequential_mapper, dotd2q4_inner_avx2>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd2q4_bulk_masked(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t mask,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<int8_t, masked_mapper, dotd2q4_inner_avx2, masked_output>(
        a, query, length, pitch, &mask, __builtin_popcount((uint32_t)mask), results);
}

EXPORT void vec_dotd2q4_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<const int8_t*, sparse_mapper, dotd2q4_inner_avx2>
        ((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT int64_t vec_dotd4q4(const int8_t* a, const int8_t* query, const int32_t length) {
    const int32_t bit_length = length / 4;
    int64_t p0 = dotd1qN_inner<4>(a + 0 * bit_length, query, bit_length);
    int64_t p1 = dotd1qN_inner<4>(a + 1 * bit_length, query, bit_length);
    int64_t p2 = dotd1qN_inner<4>(a + 2 * bit_length, query, bit_length);
    int64_t p3 = dotd1qN_inner<4>(a + 3 * bit_length, query, bit_length);
    return p0 + (p1 << 1) + (p2 << 2) + (p3 << 3);
}

EXPORT void vec_dotd4q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<int8_t, sequential_mapper, dotd1qN_inner<4>>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd4q4_bulk_masked(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t mask,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<int8_t, masked_mapper, dotd1qN_inner<4>, masked_output>(
        a, query, length, pitch, &mask, __builtin_popcount((uint32_t)mask), results);
}

EXPORT void vec_dotd4q4_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<const int8_t*, sparse_mapper, dotd1qN_inner<4>>
        ((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}
