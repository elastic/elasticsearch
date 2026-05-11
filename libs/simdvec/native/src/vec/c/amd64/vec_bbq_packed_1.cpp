/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// AVX2-vectorized N-bit packed doc BBQ kernels

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"

// --- D2Q4 ---

// Doc layout: packed_len bytes, four 2-bit values per byte:
//   [v0:7..6 | v1:5..4 | v2:3..2 | v3:1..0]
// Query layout: 4*packed_len bytes, one 4-bit value per byte (range 0..15), partitioned
// into four contiguous stripes that align with the doc's 2-bit fields:
//   [0..packed_len)            -> stripe 0 (pairs with bits 7:6 of packed)
//   [packed_len..2*packed_len) -> stripe 1 (bits 5:4)
//   [2*packed_len..3*packed_len) -> stripe 2 (bits 3:2)
//   [3*packed_len..4*packed_len) -> stripe 3 (bits 1:0)

static inline int32_t dotd2q4_packed_inner(const int8_t* a, const int8_t* query, int32_t packed_len) {
    const __m256i mask_two_bits = _mm256_set1_epi8(0x03);
    const __m256i ones = _mm256_set1_epi16(1);
    __m256i acc = _mm256_setzero_si256();

    constexpr int stride = sizeof(__m256i);
    const int blk = packed_len & ~(stride - 1);

    // maddubs with 2-bit doc (0..3) and 4-bit query (0..15): max per 16-bit lane = 3*15 + 3*15 = 90.
    // Safe to accumulate floor(32767/90) = 364 iterations before signed 16-bit overflow.
    // Pick 256 (well below the limit; matches int4 packed-nibble kernel for round numbers).
    constexpr int chunk = 256 * stride;

    int i = 0;
    while (i < blk) {
        // We are avoiding templates here because the masks required to extract the 4 elements from a byte
        // are different enough to make templated code look less readable.
        // See commit ddb46a776397ce4293fdff6667eacc0781297119
        __m256i acc_s0_16 = _mm256_setzero_si256();
        __m256i acc_s1_16 = _mm256_setzero_si256();
        __m256i acc_s2_16 = _mm256_setzero_si256();
        __m256i acc_s3_16 = _mm256_setzero_si256();
        const int end = std::min(i + chunk, blk);

        for (; i < end; i += stride) {
            __m256i doc_bytes = _mm256_loadu_si256((const __m256i*)(a + i));

            // Extract the four 2-bit planes via 16-bit-lane shifts; the 0x03 mask cleans cross-byte leakage.
            __m256i doc_s0 = _mm256_and_si256(_mm256_srli_epi16(doc_bytes, 6), mask_two_bits);
            __m256i doc_s1 = _mm256_and_si256(_mm256_srli_epi16(doc_bytes, 4), mask_two_bits);
            __m256i doc_s2 = _mm256_and_si256(_mm256_srli_epi16(doc_bytes, 2), mask_two_bits);
            __m256i doc_s3 = _mm256_and_si256(doc_bytes, mask_two_bits);

            __m256i query_s0 = _mm256_loadu_si256((const __m256i*)(query + i));
            __m256i query_s1 = _mm256_loadu_si256((const __m256i*)(query + i + packed_len));
            __m256i query_s2 = _mm256_loadu_si256((const __m256i*)(query + i + 2 * packed_len));
            __m256i query_s3 = _mm256_loadu_si256((const __m256i*)(query + i + 3 * packed_len));

            // Doc 0..3, query 0..15 — both fit in unsigned bytes; signedness of maddubs is irrelevant here.
            // Accumulate in 16-bit; widen to 32-bit after the chunk.
            acc_s0_16 = _mm256_add_epi16(acc_s0_16, _mm256_maddubs_epi16(doc_s0, query_s0));
            acc_s1_16 = _mm256_add_epi16(acc_s1_16, _mm256_maddubs_epi16(doc_s1, query_s1));
            acc_s2_16 = _mm256_add_epi16(acc_s2_16, _mm256_maddubs_epi16(doc_s2, query_s2));
            acc_s3_16 = _mm256_add_epi16(acc_s3_16, _mm256_maddubs_epi16(doc_s3, query_s3));
        }

        // Widen 16->32 bit and accumulate into the running 32-bit total.
        acc = _mm256_add_epi32(acc, _mm256_madd_epi16(ones, acc_s0_16));
        acc = _mm256_add_epi32(acc, _mm256_madd_epi16(ones, acc_s1_16));
        acc = _mm256_add_epi32(acc, _mm256_madd_epi16(ones, acc_s2_16));
        acc = _mm256_add_epi32(acc, _mm256_madd_epi16(ones, acc_s3_16));
    }

    int32_t total = mm256_reduce_epi32<_mm_add_epi32>(acc);

    for (int i = blk; i < packed_len; i++) {
        uint8_t doc_byte = (uint8_t)a[i];
        total += ((doc_byte >> 6) & 0x03) * query[i];
        total += ((doc_byte >> 4) & 0x03) * query[i + packed_len];
        total += ((doc_byte >> 2) & 0x03) * query[i + 2 * packed_len];
        total += (doc_byte & 0x03) * query[i + 3 * packed_len];
    }
    return total;
}

EXPORT int64_t vec_dotd2q4_packed(const int8_t* a, const int8_t* query, int32_t packed_len) {
    return (int64_t)dotd2q4_packed_inner(a, query, packed_len);
}

template <typename TData, const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t), int batches = 2>
static inline void dotd2q4_packed_bulk_impl(
    const TData* docs,
    const int8_t* query,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    const __m256i mask_two_bits = _mm256_set1_epi8(0x03);
    const __m256i ones = _mm256_set1_epi16(1);
    constexpr int stride = sizeof(__m256i);
    const int blk = packed_len & ~(stride - 1);
    // See dotd2q4_packed_inner above for the int16 headroom math; 256 is well below the 364 safe limit.
    constexpr int chunk = 256 * stride;
    const int lines_to_fetch = packed_len / CACHE_LINE_SIZE + 1;

    int c = 0;

    const int8_t* current_doc_ptrs[batches];
    init_pointers<batches, TData, int8_t, mapper>(current_doc_ptrs, docs, pitch, offsets, 0, count);

    for (; c + batches - 1 < count; c += batches) {
        const int8_t* next_doc_ptrs[batches];
        const bool has_next = c + 2 * batches - 1 < count;
        if (has_next) {
            apply_indexed<batches>([&](auto I) {
                next_doc_ptrs[I] = mapper(docs, c + batches + I, offsets, pitch);
                prefetch(next_doc_ptrs[I], lines_to_fetch);
            });
        }

        __m256i acc32[batches];
        apply_indexed<batches>([&](auto I) {
            acc32[I] = _mm256_setzero_si256();
        });

        int i = 0;
        while (i < blk) {
            __m256i acc_s0_16[batches];
            __m256i acc_s1_16[batches];
            __m256i acc_s2_16[batches];
            __m256i acc_s3_16[batches];
            apply_indexed<batches>([&](auto I) {
                acc_s0_16[I] = _mm256_setzero_si256();
                acc_s1_16[I] = _mm256_setzero_si256();
                acc_s2_16[I] = _mm256_setzero_si256();
                acc_s3_16[I] = _mm256_setzero_si256();
            });

            const int end = std::min(i + chunk, blk);

            for (; i < end; i += stride) {
                __m256i query_s0 = _mm256_loadu_si256((const __m256i*)(query + i));
                __m256i query_s1 = _mm256_loadu_si256((const __m256i*)(query + i + packed_len));
                __m256i query_s2 = _mm256_loadu_si256((const __m256i*)(query + i + 2 * packed_len));
                __m256i query_s3 = _mm256_loadu_si256((const __m256i*)(query + i + 3 * packed_len));

                apply_indexed<batches>([&](auto I) {
                    __m256i doc_bytes = _mm256_loadu_si256((const __m256i*)(current_doc_ptrs[I] + i));
                    __m256i doc_s0 = _mm256_and_si256(_mm256_srli_epi16(doc_bytes, 6), mask_two_bits);
                    __m256i doc_s1 = _mm256_and_si256(_mm256_srli_epi16(doc_bytes, 4), mask_two_bits);
                    __m256i doc_s2 = _mm256_and_si256(_mm256_srli_epi16(doc_bytes, 2), mask_two_bits);
                    __m256i doc_s3 = _mm256_and_si256(doc_bytes, mask_two_bits);

                    acc_s0_16[I] = _mm256_add_epi16(acc_s0_16[I], _mm256_maddubs_epi16(doc_s0, query_s0));
                    acc_s1_16[I] = _mm256_add_epi16(acc_s1_16[I], _mm256_maddubs_epi16(doc_s1, query_s1));
                    acc_s2_16[I] = _mm256_add_epi16(acc_s2_16[I], _mm256_maddubs_epi16(doc_s2, query_s2));
                    acc_s3_16[I] = _mm256_add_epi16(acc_s3_16[I], _mm256_maddubs_epi16(doc_s3, query_s3));
                });
            }

            apply_indexed<batches>([&](auto I) {
                acc32[I] = _mm256_add_epi32(acc32[I], _mm256_madd_epi16(ones, acc_s0_16[I]));
                acc32[I] = _mm256_add_epi32(acc32[I], _mm256_madd_epi16(ones, acc_s1_16[I]));
                acc32[I] = _mm256_add_epi32(acc32[I], _mm256_madd_epi16(ones, acc_s2_16[I]));
                acc32[I] = _mm256_add_epi32(acc32[I], _mm256_madd_epi16(ones, acc_s3_16[I]));
            });
        }

        int32_t res[batches];
        apply_indexed<batches>([&](auto I) {
            res[I] = mm256_reduce_epi32<_mm_add_epi32>(acc32[I]);
        });

        for (; i < packed_len; i++) {
            uint8_t q0 = (uint8_t)query[i];
            uint8_t q1 = (uint8_t)query[i + packed_len];
            uint8_t q2 = (uint8_t)query[i + 2 * packed_len];
            uint8_t q3 = (uint8_t)query[i + 3 * packed_len];
            apply_indexed<batches>([&](auto I) {
                uint8_t doc_byte = (uint8_t)current_doc_ptrs[I][i];
                res[I] += ((doc_byte >> 6) & 0x03) * q0;
                res[I] += ((doc_byte >> 4) & 0x03) * q1;
                res[I] += ((doc_byte >> 2) & 0x03) * q2;
                res[I] += (doc_byte & 0x03) * q3;
            });
        }

        apply_indexed<batches>([&](auto I) {
            results[c + I] = (f32_t)res[I];
        });
        if (has_next) {
            std::copy_n(next_doc_ptrs, batches, current_doc_ptrs);
        }
    }

    for (; c < count; c++) {
        const int8_t* doc = mapper(docs, c, offsets, pitch);
        results[c] = (f32_t)dotd2q4_packed_inner(doc, query, packed_len);
    }
}

EXPORT void vec_dotd2q4_packed_bulk(
    const int8_t* docs,
    const int8_t* query,
    int32_t packed_len,
    int32_t count,
    f32_t* results
) {
    dotd2q4_packed_bulk_impl<int8_t, sequential_mapper>(docs, query, packed_len, packed_len, NULL, count, results);
}

EXPORT void vec_dotd2q4_packed_bulk_offsets(
    const int8_t* docs,
    const int8_t* query,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    dotd2q4_packed_bulk_impl<int8_t, offsets_mapper>(docs, query, packed_len, pitch, offsets, count, results);
}

EXPORT void vec_dotd2q4_packed_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    int32_t packed_len,
    int32_t count,
    f32_t* results
) {
    dotd2q4_packed_bulk_impl<const int8_t*, sparse_mapper>(
        (const int8_t* const*)addresses, query, packed_len, 0, NULL, count, results
    );
}
