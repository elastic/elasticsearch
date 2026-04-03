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

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"

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

template <typename TData, const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t)>
static inline void dotd1q4_inner_bulk(
    const TData* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    constexpr int batches = 4;

    const int lines_to_fetch = length / CACHE_LINE_SIZE + 1;
    int c = 0;

    const int8_t* current_vecs[batches];
    init_pointers<batches, TData, int8_t, mapper>(current_vecs, a, pitch, offsets, 0, count);

    // Process a batch of 4 vectors at a time, after instructing the CPU to
    // prefetch the next batch.
    // Prefetching multiple memory locations while computing keeps the CPU
    // execution units busy.
    for (; c + batches - 1 < count; c += batches) {
        const int8_t* next_vecs[batches];
        const bool has_next = c + 2 * batches - 1 < count;
        if (has_next) {
            apply_indexed<batches>([&](auto I) {
                next_vecs[I] = mapper(a, c + batches + I, offsets, pitch);
                prefetch(next_vecs[I], lines_to_fetch);
            });
        }

        apply_indexed<batches>([&](auto I) {
            results[c + I] = (f32_t)dotd1q4_inner(current_vecs[I], query, length);
        });

        if (has_next) {
            std::copy_n(next_vecs, batches, current_vecs);
        }
    }

    // Tail-handling: remaining vectors
    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = (f32_t)dotd1q4_inner(a0, query, length);
    }
}

EXPORT void vec_dotd1q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1q4_inner_bulk<int8_t, sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd1q4_bulk_offsets(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dotd1q4_inner_bulk<int8_t, offsets_mapper>(a, query, length, pitch, offsets, count, results);
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

template <typename TData, const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t)>
static inline void dotd2q4_inner_bulk(
    const TData* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    constexpr int batches = 2;

    const int lines_to_fetch = length / CACHE_LINE_SIZE + 1;
    const int bit_length = length/2;
    int c = 0;

    const int8_t* current_vecs[batches];
    init_pointers<batches, TData, int8_t, mapper>(current_vecs, a, pitch, offsets, 0, count);

    // Process 2 vectors at a time, after instructing the CPU to
    // prefetch the next vectors (both stripes).
    for (; c + batches - 1 < count; c += batches) {
        const int8_t* next_vecs[batches];
        const bool has_next = c + 2 * batches - 1 < count;
        if (has_next) {
            apply_indexed<batches>([&](auto I) {
                next_vecs[I] = mapper(a, c + batches + I, offsets, pitch);
                prefetch(next_vecs[I], lines_to_fetch);
                prefetch(next_vecs[I] + bit_length, lines_to_fetch);
            });
        }

        apply_indexed<batches>([&](auto I) {
            results[c + I] = (f32_t)(
                dotd1q4_inner(current_vecs[I], query, bit_length)
                + (dotd1q4_inner(current_vecs[I] + bit_length, query, bit_length) << 1));
        });

        if (has_next) {
            std::copy_n(next_vecs, batches, current_vecs);
        }
    }

    // Tail-handling: remaining vectors
    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        int64_t lower = dotd1q4_inner(a0, query, bit_length);
        int64_t upper = dotd1q4_inner(a0 + bit_length, query, bit_length);
        results[c] = (f32_t)(lower + (upper << 1));
    }
}

EXPORT void vec_dotd2q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<int8_t, sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd2q4_bulk_offsets(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<int8_t, offsets_mapper>(a, query, length, pitch, offsets, count, results);
}

EXPORT int64_t vec_dotd4q4(const int8_t* a, const int8_t* query, const int32_t length) {
    const int32_t bit_length = length / 4;
    int64_t p0 = dotd1q4_inner(a + 0 * bit_length, query, bit_length);
    int64_t p1 = dotd1q4_inner(a + 1 * bit_length, query, bit_length);
    int64_t p2 = dotd1q4_inner(a + 2 * bit_length, query, bit_length);
    int64_t p3 = dotd1q4_inner(a + 3 * bit_length, query, bit_length);
    return p0 + (p1 << 1) + (p2 << 2) + (p3 << 3);
}

template <typename TData, const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t)>
static inline void dotd4q4_inner_bulk(
    const TData* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    const int lines_to_fetch = length / CACHE_LINE_SIZE + 1;
    const int32_t bit_length = length / 4;
    int c = 0;

    const int8_t* a0 = count > 0 ? mapper(a, 0, offsets, pitch) : nullptr;

    // Process one vector, after instructing the CPU to prefetch the next vector
    for (; c + 1 < count; c++) {
        const int8_t* next_a0 = mapper(a, c + 1, offsets, pitch);

        // prefetch stripes 2 and 3 now
        prefetch(a0 + 2 * bit_length, lines_to_fetch);
        prefetch(a0 + 3 * bit_length, lines_to_fetch);

        int64_t p0 = dotd1q4_inner(a0, query, bit_length);
        int64_t p1 = dotd1q4_inner(a0 + bit_length, query, bit_length);

        // and 0 and 1 of the next vector
        prefetch(next_a0, lines_to_fetch);
        prefetch(next_a0 + bit_length, lines_to_fetch);

        int64_t p2 = dotd1q4_inner(a0 + 2 * bit_length, query, bit_length);
        int64_t p3 = dotd1q4_inner(a0 + 3 * bit_length, query, bit_length);

        results[c] = (f32_t)(p0 + (p1 << 1) + (p2 << 2) + (p3 << 3));

        a0 = next_a0;
    }

    // Tail-handling: remaining vector
    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);

        int64_t p0 = dotd1q4_inner(a0 + 0 * bit_length, query, bit_length);
        int64_t p1 = dotd1q4_inner(a0 + 1 * bit_length, query, bit_length);
        int64_t p2 = dotd1q4_inner(a0 + 2 * bit_length, query, bit_length);
        int64_t p3 = dotd1q4_inner(a0 + 3 * bit_length, query, bit_length);

        results[c] = (f32_t)(p0 + (p1 << 1) + (p2 << 2) + (p3 << 3));
    }
}

EXPORT void vec_dotd4q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<int8_t, sequential_mapper>(a, query, length, length, NULL, count, results);
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
    dotd4q4_inner_bulk<int8_t, offsets_mapper>(a, query, length, pitch, offsets, count, results);
}
