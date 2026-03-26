/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// AVX-512 vectorized int4 packed-nibble vector operations.
// The "unpacked" vector has 2*packed_len bytes (high nibbles in [0..packed_len),
// low nibbles in [packed_len..2*packed_len)). The "packed" vector has packed_len
// bytes, each holding two 4-bit values.

#include <stddef.h>
#include <stdint.h>

#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"

static inline int32_t doti4_inner_avx512(const int8_t* query, const int8_t* doc, int32_t packed_len) {
    const __m512i mask_half_byte = _mm512_set1_epi8(0x0F);
    const __m512i ones = _mm512_set1_epi16(1);
    __m512i acc = _mm512_setzero_si512();

    constexpr int stride = sizeof(__m512i);
    const int blk = packed_len & ~(stride - 1);

    // maddubs with int4 values produces at most 15*15+15*15 = 450 per 16-bit lane.
    // Safe to accumulate floor(32767/450) = 72 iterations before signed 16-bit overflow.
    constexpr int chunk = 64 * stride;

    int i = 0;
    while (i < blk) {
        __m512i acc_high16 = _mm512_setzero_si512();
        __m512i acc_low16 = _mm512_setzero_si512();
        const int end_raw = i + chunk;
        // TODO: replace with std::min when we solve the gcc #pragma target inline bug
        const int end = end_raw < blk ? end_raw : blk;

        for (; i < end; i += stride) {
            __m512i doc_bytes = _mm512_loadu_si512((const __m512i*)(doc + i));

            __m512i doc_high = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 4), mask_half_byte);
            __m512i doc_low = _mm512_and_si512(doc_bytes, mask_half_byte);

            __m512i query_high = _mm512_loadu_si512((const __m512i*)(query + i));
            __m512i query_low = _mm512_loadu_si512((const __m512i*)(query + i + packed_len));

            acc_high16 = _mm512_add_epi16(acc_high16, _mm512_maddubs_epi16(doc_high, query_high));
            acc_low16 = _mm512_add_epi16(acc_low16, _mm512_maddubs_epi16(doc_low, query_low));
        }

        acc = _mm512_add_epi32(acc, _mm512_madd_epi16(ones, acc_high16));
        acc = _mm512_add_epi32(acc, _mm512_madd_epi16(ones, acc_low16));
    }

    int32_t total = _mm512_reduce_add_epi32(acc);

    // Masked tail: handle remaining bytes that don't fill a full 512-bit register.
    // Masked-off lanes load as zero, contributing nothing to the dot product.
    const int rem = packed_len - blk;
    if (rem > 0) {
        __mmask64 mask = (__mmask64)((1ULL << rem) - 1);

        __m512i doc_bytes = _mm512_maskz_loadu_epi8(mask, doc + blk);
        __m512i doc_high = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 4), mask_half_byte);
        __m512i doc_low = _mm512_and_si512(doc_bytes, mask_half_byte);

        __m512i query_high = _mm512_maskz_loadu_epi8(mask, query + blk);
        __m512i query_low = _mm512_maskz_loadu_epi8(mask, query + blk + packed_len);

        __m512i wide = _mm512_add_epi32(
            _mm512_madd_epi16(ones, _mm512_maddubs_epi16(doc_high, query_high)),
            _mm512_madd_epi16(ones, _mm512_maddubs_epi16(doc_low, query_low))
        );
        total += _mm512_reduce_add_epi32(wide);
    }

    return total;
}

EXPORT int32_t vec_doti4_2(const int8_t* query, const int8_t* doc, int32_t packed_len) {
    return doti4_inner_avx512(query, doc, packed_len);
}

// batches=2 rather than 4: most CPUs have only 1 port for 512-bit integer
// multiply (vpmaddubsw zmm), so batches>2 saturates that port without
// increasing per-doc throughput, while adding instruction overhead.
template <const int8_t*(*mapper)(const int8_t*, const int32_t, const int32_t*, const int32_t), int batches = 2>
static inline void doti4_bulk_impl_avx512(
    const int8_t* docs,
    const int8_t* query,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    const __m512i mask_half_byte = _mm512_set1_epi8(0x0F);
    const __m512i ones = _mm512_set1_epi16(1);
    constexpr int stride = sizeof(__m512i);
    const int blk = packed_len & ~(stride - 1);
    constexpr int chunk = 64 * stride;
    const int lines_to_fetch = packed_len / CACHE_LINE_SIZE + 1;

    const int rem = packed_len - blk;
    const __mmask64 tail_mask = rem > 0 ? (__mmask64)((1ULL << rem) - 1) : 0;

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

        __m512i acc32[batches];
        apply_indexed<batches>([&](auto I) {
            acc32[I] = _mm512_setzero_si512();
        });

        int i = 0;
        while (i < blk) {
            __m512i acc_high16[batches];
            __m512i acc_low16[batches];
            apply_indexed<batches>([&](auto I) {
                acc_high16[I] = _mm512_setzero_si512();
                acc_low16[I] = _mm512_setzero_si512();
            });

            const int end_raw = i + chunk;
            // TODO: replace with std::min when we solve the gcc #pragma target inline bug
            const int end = end_raw < blk ? end_raw : blk;

            for (; i < end; i += stride) {
                __m512i query_high = _mm512_loadu_si512((const __m512i*)(query + i));
                __m512i query_low = _mm512_loadu_si512((const __m512i*)(query + i + packed_len));

                apply_indexed<batches>([&](auto I) {
                    __m512i doc_bytes = _mm512_loadu_si512((const __m512i*)(current_doc_ptrs[I] + i));
                    __m512i doc_high = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 4), mask_half_byte);
                    __m512i doc_low = _mm512_and_si512(doc_bytes, mask_half_byte);

                    acc_high16[I] = _mm512_add_epi16(acc_high16[I], _mm512_maddubs_epi16(doc_high, query_high));
                    acc_low16[I] = _mm512_add_epi16(acc_low16[I], _mm512_maddubs_epi16(doc_low, query_low));
                });
            }

            apply_indexed<batches>([&](auto I) {
                acc32[I] = _mm512_add_epi32(acc32[I], _mm512_madd_epi16(ones, acc_high16[I]));
                acc32[I] = _mm512_add_epi32(acc32[I], _mm512_madd_epi16(ones, acc_low16[I]));
            });
        }

        int32_t res[batches];
        apply_indexed<batches>([&](auto I) {
            res[I] = _mm512_reduce_add_epi32(acc32[I]);
        });

        if (tail_mask) {
            __m512i query_high = _mm512_maskz_loadu_epi8(tail_mask, query + blk);
            __m512i query_low = _mm512_maskz_loadu_epi8(tail_mask, query + blk + packed_len);

            apply_indexed<batches>([&](auto I) {
                __m512i doc_bytes = _mm512_maskz_loadu_epi8(tail_mask, current_doc_ptrs[I] + blk);
                __m512i doc_high = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 4), mask_half_byte);
                __m512i doc_low = _mm512_and_si512(doc_bytes, mask_half_byte);

                __m512i wide = _mm512_add_epi32(
                    _mm512_madd_epi16(ones, _mm512_maddubs_epi16(doc_high, query_high)),
                    _mm512_madd_epi16(ones, _mm512_maddubs_epi16(doc_low, query_low))
                );
                res[I] += _mm512_reduce_add_epi32(wide);
            });
        }

        // TODO: consider replacing with std::copy_n when we solve the gcc #pragma target inline bug
        apply_indexed<batches>([&](auto I) {
            results[c + I] = (f32_t)res[I];
        });
        if (has_next) {
            apply_indexed<batches>([&](auto I) {
                current_doc_ptrs[I] = next_doc_ptrs[I];
            });
        }
    }

    for (; c < count; c++) {
        const int8_t* doc = mapper(docs, c, offsets, pitch);
        results[c] = (f32_t)doti4_inner_avx512(query, doc, packed_len);
    }
}

EXPORT void vec_doti4_bulk_2(const int8_t* docs, const int8_t* query, int32_t packed_len, int32_t count, f32_t* results) {
    doti4_bulk_impl_avx512<sequential_mapper>(docs, query, packed_len, packed_len, NULL, count, results);
}

EXPORT void vec_doti4_bulk_offsets_2(
    const int8_t* docs,
    const int8_t* query,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    doti4_bulk_impl_avx512<offsets_mapper>(docs, query, packed_len, pitch, offsets, count, results);
}
