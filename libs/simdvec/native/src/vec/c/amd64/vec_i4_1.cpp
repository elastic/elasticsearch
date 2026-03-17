/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// AVX2-vectorized int4 packed-nibble vector operations.
// The "unpacked" vector has 2*packed_len bytes (high nibbles in [0..packed_len),
// low nibbles in [packed_len..2*packed_len)). The "packed" vector has packed_len
// bytes, each holding two 4-bit values.

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"

static inline int32_t doti4_inner(const int8_t* query, const int8_t* doc, int32_t packed_len) {
    const __m128i mask_0f = _mm_set1_epi8(0x0F);
    __m256i acc_high = _mm256_setzero_si256();
    __m256i acc_low = _mm256_setzero_si256();

    constexpr int stride = sizeof(__m128i);
    const int blk = packed_len & ~(stride - 1);

    for (int i = 0; i < blk; i += stride) {
        __m128i doc_bytes = _mm_loadu_si128((const __m128i*)(doc + i));

        // Extract nibbles: shift+mask for high, mask for low.
        // _mm_srli_epi16 shifts 16-bit lanes; the 0x0F mask cleans cross-byte leakage.
        __m128i doc_high = _mm_and_si128(_mm_srli_epi16(doc_bytes, 4), mask_0f);
        __m128i doc_low = _mm_and_si128(doc_bytes, mask_0f);

        __m128i query_high = _mm_loadu_si128((const __m128i*)(query + i));
        __m128i query_low = _mm_loadu_si128((const __m128i*)(query + i + packed_len));

        // Zero-extend 16 bytes to 16 shorts, then madd pairs into 8 int32s
        __m256i doc_high_16 = _mm256_cvtepu8_epi16(doc_high);
        __m256i query_high_16 = _mm256_cvtepu8_epi16(query_high);
        __m256i doc_low_16 = _mm256_cvtepu8_epi16(doc_low);
        __m256i query_low_16 = _mm256_cvtepu8_epi16(query_low);

        acc_high = _mm256_add_epi32(acc_high, _mm256_madd_epi16(doc_high_16, query_high_16));
        acc_low = _mm256_add_epi32(acc_low, _mm256_madd_epi16(doc_low_16, query_low_16));
    }

    int32_t total = mm256_reduce_epi32<_mm_add_epi32>(_mm256_add_epi32(acc_high, acc_low));

    for (int i = blk; i < packed_len; i++) {
        uint8_t doc_byte = (uint8_t)doc[i];
        total += (doc_byte >> 4) * query[i];
        total += (doc_byte & 0x0F) * query[i + packed_len];
    }
    return total;
}

EXPORT int32_t vec_doti4(const int8_t* query, const int8_t* doc, int32_t packed_len) {
    return doti4_inner(query, doc, packed_len);
}

template <int64_t(*mapper)(const int32_t, const int32_t*), int batches = 2>
static inline void doti4_bulk_impl(
    const int8_t* docs,
    const int8_t* query,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    const __m128i mask_0f = _mm_set1_epi8(0x0F);
    constexpr int stride = sizeof(__m128i);
    const int blk = packed_len & ~(stride - 1);
    const int lines_to_fetch = packed_len / CACHE_LINE_SIZE + 1;

    int c = 0;

    const int8_t* current_doc_ptrs[batches];
    init_offsets<batches, int8_t, mapper>(current_doc_ptrs, docs, pitch, offsets, count);

    for (; c + 2 * batches - 1 < count; c += batches) {
        const int8_t* next_doc_ptrs[batches];
        apply_indexed<batches>([&](auto I) {
            next_doc_ptrs[I] = docs + mapper(c + batches + I, offsets) * pitch;
            prefetch(next_doc_ptrs[I], lines_to_fetch);
        });

        __m256i acc_high[batches];
        __m256i acc_low[batches];
        apply_indexed<batches>([&](auto I) {
            acc_high[I] = _mm256_setzero_si256();
            acc_low[I] = _mm256_setzero_si256();
        });

        int i = 0;
        for (; i < blk; i += stride) {
            __m128i query_high = _mm_loadu_si128((const __m128i*)(query + i));
            __m128i query_low = _mm_loadu_si128((const __m128i*)(query + i + packed_len));
            __m256i query_high_16 = _mm256_cvtepu8_epi16(query_high);
            __m256i query_low_16 = _mm256_cvtepu8_epi16(query_low);

            apply_indexed<batches>([&](auto I) {
                __m128i doc_bytes = _mm_loadu_si128((const __m128i*)(current_doc_ptrs[I] + i));
                __m128i doc_high = _mm_and_si128(_mm_srli_epi16(doc_bytes, 4), mask_0f);
                __m128i doc_low = _mm_and_si128(doc_bytes, mask_0f);

                acc_high[I] = _mm256_add_epi32(acc_high[I], _mm256_madd_epi16(_mm256_cvtepu8_epi16(doc_high), query_high_16));
                acc_low[I] = _mm256_add_epi32(acc_low[I], _mm256_madd_epi16(_mm256_cvtepu8_epi16(doc_low), query_low_16));
            });
        }

        int32_t res[batches];
        apply_indexed<batches>([&](auto I) {
            res[I] = mm256_reduce_epi32<_mm_add_epi32>(_mm256_add_epi32(acc_high[I], acc_low[I]));
        });

        for (; i < packed_len; i++) {
            uint8_t query_high_val = (uint8_t)query[i];
            uint8_t query_low_val = (uint8_t)query[i + packed_len];
            apply_indexed<batches>([&](auto I) {
                uint8_t doc_byte = (uint8_t)current_doc_ptrs[I][i];
                res[I] += (doc_byte >> 4) * query_high_val;
                res[I] += (doc_byte & 0x0F) * query_low_val;
            });
        }

        apply_indexed<batches>([&](auto I) {
            results[c + I] = (f32_t)res[I];
        });
        std::copy_n(next_doc_ptrs, batches, current_doc_ptrs);
    }

    for (; c < count; c++) {
        const int8_t* doc = docs + mapper(c, offsets) * pitch;
        results[c] = (f32_t)doti4_inner(query, doc, packed_len);
    }
}

EXPORT void vec_doti4_bulk(const int8_t* docs, const int8_t* query, int32_t packed_len, int32_t count, f32_t* results) {
    doti4_bulk_impl<identity_mapper>(docs, query, packed_len, packed_len, NULL, count, results);
}

EXPORT void vec_doti4_bulk_offsets(
    const int8_t* docs,
    const int8_t* query,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    doti4_bulk_impl<array_mapper>(docs, query, packed_len, pitch, offsets, count, results);
}
