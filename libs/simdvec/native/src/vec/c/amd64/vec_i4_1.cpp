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
    const __m256i mask_half_byte = _mm256_set1_epi8(0x0F);
    const __m256i ones = _mm256_set1_epi16(1);
    __m256i acc = _mm256_setzero_si256();

    constexpr int stride = sizeof(__m256i);
    const int blk = packed_len & ~(stride - 1);

    // maddubs with int4 values produces at most 15*15+15*15 = 450 per 16-bit lane.
    // Safe to accumulate floor(32767/450) = 72 iterations before signed 16-bit overflow.
    constexpr int chunk = 64 * stride;

    int i = 0;
    while (i < blk) {
        __m256i acc_high16 = _mm256_setzero_si256();
        __m256i acc_low16 = _mm256_setzero_si256();
        const int end = std::min(i + chunk, blk);

        for (; i < end; i += stride) {
            __m256i doc_bytes = _mm256_loadu_si256((const __m256i*)(doc + i));

            // Extract nibbles at 256-bit width.
            // _mm256_srli_epi16 shifts 16-bit lanes; the 0x0F mask cleans cross-byte leakage.
            __m256i doc_high = _mm256_and_si256(_mm256_srli_epi16(doc_bytes, 4), mask_half_byte);
            __m256i doc_low = _mm256_and_si256(doc_bytes, mask_half_byte);

            __m256i query_high = _mm256_loadu_si256((const __m256i*)(query + i));
            __m256i query_low = _mm256_loadu_si256((const __m256i*)(query + i + packed_len));

            // _mm256_maddubs_epi16 multiplies unsigned*signed byte pairs and horizontally
            // adds adjacent products into 16-bit results. Both operands are in [0,15] so
            // signedness doesn't matter. Accumulate in 16-bit; widen to 32-bit after the chunk.
            acc_high16 = _mm256_add_epi16(acc_high16, _mm256_maddubs_epi16(doc_high, query_high));
            acc_low16 = _mm256_add_epi16(acc_low16, _mm256_maddubs_epi16(doc_low, query_low));
        }

        // Widen 16→32 bit: _mm256_madd_epi16 with ones horizontally adds pairs of
        // signed 16-bit values into 32-bit results, then accumulate into the 32-bit total.
        acc = _mm256_add_epi32(acc, _mm256_madd_epi16(ones, acc_high16));
        acc = _mm256_add_epi32(acc, _mm256_madd_epi16(ones, acc_low16));
    }

    int32_t total = mm256_reduce_epi32<_mm_add_epi32>(acc);

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

template <const int8_t*(*mapper)(const int8_t*, const int32_t, const int32_t*, const int32_t), int batches = 2>
static inline void doti4_bulk_impl(
    const int8_t* docs,
    const int8_t* query,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    const __m256i mask_half_byte = _mm256_set1_epi8(0x0F);
    const __m256i ones = _mm256_set1_epi16(1);
    constexpr int stride = sizeof(__m256i);
    const int blk = packed_len & ~(stride - 1);
    constexpr int chunk = 64 * stride;
    const int lines_to_fetch = packed_len / CACHE_LINE_SIZE + 1;

    int c = 0;

    const int8_t* current_doc_ptrs[batches];
    init_pointers<batches, int8_t, int8_t, mapper>(current_doc_ptrs, docs, pitch, offsets, 0, count);

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
            __m256i acc_high16[batches];
            __m256i acc_low16[batches];
            apply_indexed<batches>([&](auto I) {
                acc_high16[I] = _mm256_setzero_si256();
                acc_low16[I] = _mm256_setzero_si256();
            });

            const int end = std::min(i + chunk, blk);

            for (; i < end; i += stride) {
                __m256i query_high = _mm256_loadu_si256((const __m256i*)(query + i));
                __m256i query_low = _mm256_loadu_si256((const __m256i*)(query + i + packed_len));

                apply_indexed<batches>([&](auto I) {
                    __m256i doc_bytes = _mm256_loadu_si256((const __m256i*)(current_doc_ptrs[I] + i));
                    __m256i doc_high = _mm256_and_si256(_mm256_srli_epi16(doc_bytes, 4), mask_half_byte);
                    __m256i doc_low = _mm256_and_si256(doc_bytes, mask_half_byte);

                    acc_high16[I] = _mm256_add_epi16(acc_high16[I], _mm256_maddubs_epi16(doc_high, query_high));
                    acc_low16[I] = _mm256_add_epi16(acc_low16[I], _mm256_maddubs_epi16(doc_low, query_low));
                });
            }

            apply_indexed<batches>([&](auto I) {
                acc32[I] = _mm256_add_epi32(acc32[I], _mm256_madd_epi16(ones, acc_high16[I]));
                acc32[I] = _mm256_add_epi32(acc32[I], _mm256_madd_epi16(ones, acc_low16[I]));
            });
        }

        int32_t res[batches];
        apply_indexed<batches>([&](auto I) {
            res[I] = mm256_reduce_epi32<_mm_add_epi32>(acc32[I]);
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
        if (has_next) {
            std::copy_n(next_doc_ptrs, batches, current_doc_ptrs);
        }
    }

    for (; c < count; c++) {
        const int8_t* doc = mapper(docs, c, offsets, pitch);
        results[c] = (f32_t)doti4_inner(query, doc, packed_len);
    }
}

EXPORT void vec_doti4_bulk(const int8_t* docs, const int8_t* query, int32_t packed_len, int32_t count, f32_t* results) {
    doti4_bulk_impl<sequential_mapper>(docs, query, packed_len, packed_len, NULL, count, results);
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
    doti4_bulk_impl<offsets_mapper>(docs, query, packed_len, pitch, offsets, count, results);
}
