/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// AVX-512 (+ VNNI) tier-2 N-bit packed-doc BBQ kernels.
// Mirrors vec_bbq_packed_1.cpp at 512-bit width and uses VNNI vpdpbusd to
// accumulate directly into 32-bit lanes; both operands are in 0..15 (query)
// and 0..3 (doc) so the unsigned/signed asymmetry of vpdpbusd is irrelevant.
// Max per-lane contribution per stride is 3*15*4 = 180, never overflows int32,
// so no chunked 16-bit accumulator is required.

#include <stddef.h>
#include <stdint.h>

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

static inline int32_t dotd2q4_packed_inner_avx512(const int8_t* a, const int8_t* query, int32_t packed_len) {
    const __m512i mask_two_bits = _mm512_set1_epi8(0x03);
    __m512i acc_s0 = _mm512_setzero_si512();
    __m512i acc_s1 = _mm512_setzero_si512();
    __m512i acc_s2 = _mm512_setzero_si512();
    __m512i acc_s3 = _mm512_setzero_si512();

    constexpr int stride = sizeof(__m512i);
    const int blk = packed_len & ~(stride - 1);

    for (int i = 0; i < blk; i += stride) {
        __m512i doc_bytes = _mm512_loadu_si512((const __m512i*)(a + i));
        __m512i doc_s0 = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 6), mask_two_bits);
        __m512i doc_s1 = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 4), mask_two_bits);
        __m512i doc_s2 = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 2), mask_two_bits);
        __m512i doc_s3 = _mm512_and_si512(doc_bytes, mask_two_bits);

        __m512i query_s0 = _mm512_loadu_si512((const __m512i*)(query + i));
        __m512i query_s1 = _mm512_loadu_si512((const __m512i*)(query + i + packed_len));
        __m512i query_s2 = _mm512_loadu_si512((const __m512i*)(query + i + 2 * packed_len));
        __m512i query_s3 = _mm512_loadu_si512((const __m512i*)(query + i + 3 * packed_len));

        acc_s0 = _mm512_dpbusd_epi32(acc_s0, doc_s0, query_s0);
        acc_s1 = _mm512_dpbusd_epi32(acc_s1, doc_s1, query_s1);
        acc_s2 = _mm512_dpbusd_epi32(acc_s2, doc_s2, query_s2);
        acc_s3 = _mm512_dpbusd_epi32(acc_s3, doc_s3, query_s3);
    }

    // Masked tail: remaining bytes that don't fill a full 512-bit register.
    // Masked-off lanes load as zero, contributing nothing to the dot product.
    const int rem = packed_len - blk;
    if (rem > 0) {
        __mmask64 mask = (__mmask64)((1ULL << rem) - 1);

        __m512i doc_bytes = _mm512_maskz_loadu_epi8(mask, a + blk);
        __m512i doc_s0 = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 6), mask_two_bits);
        __m512i doc_s1 = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 4), mask_two_bits);
        __m512i doc_s2 = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 2), mask_two_bits);
        __m512i doc_s3 = _mm512_and_si512(doc_bytes, mask_two_bits);

        __m512i query_s0 = _mm512_maskz_loadu_epi8(mask, query + blk);
        __m512i query_s1 = _mm512_maskz_loadu_epi8(mask, query + blk + packed_len);
        __m512i query_s2 = _mm512_maskz_loadu_epi8(mask, query + blk + 2 * packed_len);
        __m512i query_s3 = _mm512_maskz_loadu_epi8(mask, query + blk + 3 * packed_len);

        acc_s0 = _mm512_dpbusd_epi32(acc_s0, doc_s0, query_s0);
        acc_s1 = _mm512_dpbusd_epi32(acc_s1, doc_s1, query_s1);
        acc_s2 = _mm512_dpbusd_epi32(acc_s2, doc_s2, query_s2);
        acc_s3 = _mm512_dpbusd_epi32(acc_s3, doc_s3, query_s3);
    }

    return _mm512_reduce_add_epi32(
        _mm512_add_epi32(_mm512_add_epi32(acc_s0, acc_s1), _mm512_add_epi32(acc_s2, acc_s3))
    );
}

EXPORT int64_t vec_dotd2q4_packed_2(const int8_t* a, const int8_t* query, int32_t packed_len) {
    return (int64_t)dotd2q4_packed_inner_avx512(a, query, packed_len);
}

// `batches` tuned per export below, mirroring the int4 tier-2 design (#148287):
// BULK uses the most conservative count to avoid L1D set aliasing on the contiguous
// layout; OFFSETS and SPARSE scatter their streams across memory, so a higher count
// further hides vpdpbusd latency without aliasing risk. The four stripe accumulators
// already provide twice the independent chains of int4's two, so unroll_dim is not
// needed (also matches #148287 finding that ideal unroll_dim is always 1).
template <
    typename TData,
    const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int batches = 2
>
static inline void dotd2q4_packed_bulk_impl_avx512(
    const TData* docs,
    const int8_t* query,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    const __m512i mask_two_bits = _mm512_set1_epi8(0x03);
    constexpr int stride = sizeof(__m512i);
    const int blk = packed_len & ~(stride - 1);
    const int lines_to_fetch = packed_len / CACHE_LINE_SIZE + 1;

    const int rem = packed_len - blk;
    const __mmask64 tail_mask = rem > 0 ? (__mmask64)((1ULL << rem) - 1) : 0;

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

        __m512i acc_s0[batches];
        __m512i acc_s1[batches];
        __m512i acc_s2[batches];
        __m512i acc_s3[batches];
        apply_indexed<batches>([&](auto I) {
            acc_s0[I] = _mm512_setzero_si512();
            acc_s1[I] = _mm512_setzero_si512();
            acc_s2[I] = _mm512_setzero_si512();
            acc_s3[I] = _mm512_setzero_si512();
        });

        int i = 0;
        for (; i < blk; i += stride) {
            __m512i query_s0 = _mm512_loadu_si512((const __m512i*)(query + i));
            __m512i query_s1 = _mm512_loadu_si512((const __m512i*)(query + i + packed_len));
            __m512i query_s2 = _mm512_loadu_si512((const __m512i*)(query + i + 2 * packed_len));
            __m512i query_s3 = _mm512_loadu_si512((const __m512i*)(query + i + 3 * packed_len));

            apply_indexed<batches>([&](auto I) {
                __m512i doc_bytes = _mm512_loadu_si512((const __m512i*)(current_doc_ptrs[I] + i));
                __m512i doc_s0 = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 6), mask_two_bits);
                __m512i doc_s1 = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 4), mask_two_bits);
                __m512i doc_s2 = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 2), mask_two_bits);
                __m512i doc_s3 = _mm512_and_si512(doc_bytes, mask_two_bits);

                acc_s0[I] = _mm512_dpbusd_epi32(acc_s0[I], doc_s0, query_s0);
                acc_s1[I] = _mm512_dpbusd_epi32(acc_s1[I], doc_s1, query_s1);
                acc_s2[I] = _mm512_dpbusd_epi32(acc_s2[I], doc_s2, query_s2);
                acc_s3[I] = _mm512_dpbusd_epi32(acc_s3[I], doc_s3, query_s3);
            });
        }

        if (tail_mask) {
            __m512i query_s0 = _mm512_maskz_loadu_epi8(tail_mask, query + blk);
            __m512i query_s1 = _mm512_maskz_loadu_epi8(tail_mask, query + blk + packed_len);
            __m512i query_s2 = _mm512_maskz_loadu_epi8(tail_mask, query + blk + 2 * packed_len);
            __m512i query_s3 = _mm512_maskz_loadu_epi8(tail_mask, query + blk + 3 * packed_len);

            apply_indexed<batches>([&](auto I) {
                __m512i doc_bytes = _mm512_maskz_loadu_epi8(tail_mask, current_doc_ptrs[I] + blk);
                __m512i doc_s0 = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 6), mask_two_bits);
                __m512i doc_s1 = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 4), mask_two_bits);
                __m512i doc_s2 = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 2), mask_two_bits);
                __m512i doc_s3 = _mm512_and_si512(doc_bytes, mask_two_bits);

                acc_s0[I] = _mm512_dpbusd_epi32(acc_s0[I], doc_s0, query_s0);
                acc_s1[I] = _mm512_dpbusd_epi32(acc_s1[I], doc_s1, query_s1);
                acc_s2[I] = _mm512_dpbusd_epi32(acc_s2[I], doc_s2, query_s2);
                acc_s3[I] = _mm512_dpbusd_epi32(acc_s3[I], doc_s3, query_s3);
            });
        }

        apply_indexed<batches>([&](auto I) {
            results[c + I] = (f32_t)_mm512_reduce_add_epi32(
                _mm512_add_epi32(_mm512_add_epi32(acc_s0[I], acc_s1[I]), _mm512_add_epi32(acc_s2[I], acc_s3[I]))
            );
        });

        if (has_next) {
            std::copy_n(next_doc_ptrs, batches, current_doc_ptrs);
        }
    }

    for (; c < count; c++) {
        const int8_t* doc = mapper(docs, c, offsets, pitch);
        results[c] = (f32_t)dotd2q4_packed_inner_avx512(doc, query, packed_len);
    }
}

EXPORT void vec_dotd2q4_packed_bulk_2(
    const int8_t* docs,
    const int8_t* query,
    int32_t packed_len,
    int32_t count,
    f32_t* results
) {
    dotd2q4_packed_bulk_impl_avx512<int8_t, sequential_mapper, 2>(docs, query, packed_len, packed_len, NULL, count, results);
}

EXPORT void vec_dotd2q4_packed_bulk_offsets_2(
    const int8_t* docs,
    const int8_t* query,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    dotd2q4_packed_bulk_impl_avx512<int8_t, offsets_mapper, 4>(docs, query, packed_len, pitch, offsets, count, results);
}

EXPORT void vec_dotd2q4_packed_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* query,
    int32_t packed_len,
    int32_t count,
    f32_t* results
) {
    dotd2q4_packed_bulk_impl_avx512<const int8_t*, sparse_mapper, 4>(
        (const int8_t* const*)addresses, query, packed_len, 0, NULL, count, results
    );
}
