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

// Packed bulk: process 4 vectors at a time for length<=16 (dims<=128)
static inline void dotd1q4_bulk_packed4(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    constexpr int query_bits = 4;
    constexpr int vecs_per_reg = 4;

    const uint64_t mask = (1ULL << length) - 1;
    const __mmask16 queryByteMask = mask;
    const __mmask64 dataByteMask = mask | (mask << 16) | (mask << 32) | (mask << 48);

    // Broadcast each query bit plane x4 to fill a full 512-bit register
    __m512i bq[query_bits];
    apply_indexed<query_bits>([&](auto I) {
        bq[I] = _mm512_broadcast_i32x4(_mm_maskz_expandloadu_epi8(queryByteMask, query + I * length));
    });

    int c = 0;
    for (; c + vecs_per_reg - 1 < count; c += vecs_per_reg) {
        // Load 4 contiguous doc vectors in one 512-bit load
        __m512i docs = _mm512_maskz_expandloadu_epi8(dataByteMask, a + (int64_t)c * length);

        // Prefetch next batch
        if (c + 2 * vecs_per_reg - 1 < count) {
            prefetch(a + (int64_t)(c + vecs_per_reg) * length, 1);
        }

        // Accumulate across all 4 query bit planes with bit-plane shifting
        // no need to mask here, the extra bytes are already zero
        __m512i acc = _mm512_setzero_si512();
        apply_indexed<query_bits>([&](auto I) {
            __m512i res = _mm512_popcnt_epi64(_mm512_and_si512(docs, bq[I]));
            acc = _mm512_add_epi64(acc, _mm512_slli_epi64(res, I));
        });

        // Reduce adjacent 64-bit lane pairs to get per-vector results
        // acc = [v0a, v0b, v1a, v1b, v2a, v2b, v3a, v3b]
        __m512i swapped = _mm512_permutex_epi64(acc, _MM_SHUFFLE(2, 3, 0, 1));
        __m512i summed = _mm512_add_epi64(acc, swapped);
        // summed = [v0, v0, v1, v1, v2, v2, v3, v3] (results at even positions)

        // Extract 4 results and store as f32
        results[c + 0] = (f32_t)_mm256_extract_epi64(_mm512_castsi512_si256(summed), 0);
        results[c + 1] = (f32_t)_mm256_extract_epi64(_mm512_castsi512_si256(summed), 2);
        results[c + 2] = (f32_t)_mm256_extract_epi64(_mm512_extracti64x4_epi64(summed, 1), 0);
        results[c + 3] = (f32_t)_mm256_extract_epi64(_mm512_extracti64x4_epi64(summed, 1), 2);
    }

    // Tail: remaining vectors
    for (; c < count; c++) {
        results[c] = (f32_t)dotd1q4_inner_avx512(a + (int64_t)c * length, query, length);
    }
}

// Packed bulk: process 2 vectors at a time for length<=32 (dims<=256)
static inline void dotd1q4_bulk_packed2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    constexpr int query_bits = 4;
    constexpr int vecs_per_reg = 2;

    const uint64_t mask = (1ULL << length) - 1;
    const __mmask32 queryByteMask = mask;
    const __mmask64 dataByteMask = mask | (mask << 32);

    // Broadcast each query bit plane to fill a full 512-bit register
    __m512i bq[query_bits];
    apply_indexed<query_bits>([&](auto I) {
        bq[I] = _mm512_broadcast_i64x4(_mm256_maskz_expandloadu_epi8(queryByteMask, query + I * length));
    });

    int c = 0;
    for (; c + vecs_per_reg - 1 < count; c += vecs_per_reg) {
        // Load 2 contiguous doc vectors in one 512-bit load
        __m512i docs = _mm512_maskz_expandloadu_epi8(dataByteMask, a + (int64_t)c * length);

        // Prefetch next batch
        if (c + 2 * vecs_per_reg - 1 < count) {
            prefetch(a + (int64_t)(c + vecs_per_reg) * length, 1);
        }

        // Accumulate across all 4 query bit planes with bit-plane shifting
        // no need to mask here, the extra bytes are already zero
        __m512i acc = _mm512_setzero_si512();
        apply_indexed<query_bits>([&](auto I) {
            __m512i res = _mm512_popcnt_epi64(_mm512_and_si512(docs, bq[I]));
            acc = _mm512_add_epi64(acc, _mm512_slli_epi64(res, I));
        });

        // Reduce groups of 4 lanes to get per-vector results
        // acc = [v0a, v0b, v0c, v0d, v1a, v1b, v1c, v1d]
        // Step 1: reduce pairs
        __m512i swapped1 = _mm512_permutex_epi64(acc, _MM_SHUFFLE(2, 3, 0, 1));
        __m512i sum1 = _mm512_add_epi64(acc, swapped1);
        // sum1 = [v0ab, *, v0cd, *, v1ab, *, v1cd, *]
        // Step 2: reduce across 128-bit halves
        __m512i swapped2 = _mm512_shuffle_i64x2(sum1, sum1, _MM_SHUFFLE(2, 3, 0, 1));
        __m512i sum2 = _mm512_add_epi64(sum1, swapped2);
        // Results at positions 0 and 4

        results[c + 0] = (f32_t)_mm256_extract_epi64(_mm512_castsi512_si256(sum2), 0);
        results[c + 1] = (f32_t)_mm256_extract_epi64(_mm512_extracti64x4_epi64(sum2, 1), 0);
    }

    // Tail: remaining vector
    for (; c < count; c++) {
        results[c] = (f32_t)dotd1q4_inner_avx512(a + (int64_t)c * length, query, length);
    }
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
    if (length <= 16) {
        dotd1q4_bulk_packed4(a, query, length, count, results);
    } else if (length <= 32) {
        dotd1q4_bulk_packed2(a, query, length, count, results);
    } else {
        dotd1q4_inner_bulk<int8_t, sequential_mapper, dotd1q4_inner_avx512>(a, query, length, length, NULL, count, results);
    }
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
