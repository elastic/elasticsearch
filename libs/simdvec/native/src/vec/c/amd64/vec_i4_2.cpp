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
//
// Uses VNNI vpdpbusd to accumulate directly into 32-bit lanes. Both operands are
// in 0..15 so the unsigned/signed asymmetry of vpdpbusd is irrelevant. Max
// per-lane contribution is 15*15*4 = 900, never overflows int32, so no chunked
// 16-bit accumulator is required.
//
// Two independent unroll axes (mirrors vec_bf16_3.cpp):
//   batches    — number of distinct vector pairs in flight; amortises the query
//                load across vectors. Constrained on sequential layouts at very
//                high power-of-2 dims by L1D set aliasing.
//   unroll_dim — consecutive dim blocks per pass with independent accumulators;
//                fills the multi-cycle vpdpbusd latency window. The high/low
//                nibble streams already give two natural accumulators per pass;
//                unroll_dim multiplies that.

#include <stddef.h>
#include <stdint.h>

#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"

template<int unroll_dim = 1>
static inline int32_t doti4_inner_avx512(const int8_t* query, const int8_t* doc, int32_t packed_len) {
    const __m512i mask_half_byte = _mm512_set1_epi8(0x0F);

    __m512i acc_high[unroll_dim];
    __m512i acc_low[unroll_dim];
    apply_indexed<unroll_dim>([&](auto U) {
        acc_high[U] = _mm512_setzero_si512();
        acc_low[U] = _mm512_setzero_si512();
    });

    constexpr int stride = sizeof(__m512i);
    constexpr int dimStride = stride * unroll_dim;
    const int blk = packed_len & ~(stride - 1);

    int i = 0;
    for (; i + dimStride <= blk; i += dimStride) {
        apply_indexed<unroll_dim>([&](auto U) {
            __m512i doc_bytes = _mm512_loadu_si512((const __m512i*)(doc + i + U * stride));
            __m512i doc_high = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 4), mask_half_byte);
            __m512i doc_low = _mm512_and_si512(doc_bytes, mask_half_byte);

            __m512i query_high = _mm512_loadu_si512((const __m512i*)(query + i + U * stride));
            __m512i query_low = _mm512_loadu_si512((const __m512i*)(query + i + U * stride + packed_len));

            acc_high[U] = _mm512_dpbusd_epi32(acc_high[U], doc_high, query_high);
            acc_low[U] = _mm512_dpbusd_epi32(acc_low[U], doc_low, query_low);
        });
    }

    // Fold the unroll_dim accumulators back into a single pair (acc_high[0], acc_low[0]).
    // The constexpr branch is skipped at unroll_dim=1, where the main loop already
    // wrote acc_high[0]/acc_low[0] directly -- byte-equivalent to the unroll-1 code.
    if constexpr (unroll_dim > 1) {
        acc_high[0] = tree_reduce<unroll_dim, __m512i, _mm512_add_epi32>(acc_high);
        acc_low[0] = tree_reduce<unroll_dim, __m512i, _mm512_add_epi32>(acc_low);

        // Dim-unroll-1 tail: full strides not consumed by the main loop.
        for (; i < blk; i += stride) {
            __m512i doc_bytes = _mm512_loadu_si512((const __m512i*)(doc + i));
            __m512i doc_high = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 4), mask_half_byte);
            __m512i doc_low = _mm512_and_si512(doc_bytes, mask_half_byte);

            __m512i query_high = _mm512_loadu_si512((const __m512i*)(query + i));
            __m512i query_low = _mm512_loadu_si512((const __m512i*)(query + i + packed_len));

            acc_high[0] = _mm512_dpbusd_epi32(acc_high[0], doc_high, query_high);
            acc_low[0] = _mm512_dpbusd_epi32(acc_low[0], doc_low, query_low);
        }
    }

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

        acc_high[0] = _mm512_dpbusd_epi32(acc_high[0], doc_high, query_high);
        acc_low[0] = _mm512_dpbusd_epi32(acc_low[0], doc_low, query_low);
    }

    return _mm512_reduce_add_epi32(_mm512_add_epi32(acc_high[0], acc_low[0]));
}

EXPORT int32_t vec_doti4_2(const int8_t* query, const int8_t* doc, int32_t packed_len) {
    return doti4_inner_avx512<1>(query, doc, packed_len);
}

template <
    typename TData,
    const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int batches = 2,
    int unroll_dim = 1
>
static inline void doti4_bulk_impl_avx512(
    const TData* docs,
    const int8_t* query,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    const __m512i mask_half_byte = _mm512_set1_epi8(0x0F);
    constexpr int stride = sizeof(__m512i);
    constexpr int dimStride = stride * unroll_dim;
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

        // Row-major layout: acc_*[I * unroll_dim + U] keeps the unroll_dim accumulators
        // for batch member I contiguous, so tree_reduce can fold them in one call.
        __m512i acc_high[batches * unroll_dim];
        __m512i acc_low[batches * unroll_dim];
        apply_indexed<batches * unroll_dim>([&](auto I) {
            acc_high[I] = _mm512_setzero_si512();
            acc_low[I] = _mm512_setzero_si512();
        });

        // Main loop: each iteration advances the dim cursor by unroll_dim * stride
        // bytes, issuing 2 * unroll_dim * batches independent vpdpbusd's into
        // distinct accumulators to fill the multi-cycle vpdpbusd latency window.
        int i = 0;
        for (; i + dimStride <= blk; i += dimStride) {
            apply_indexed<unroll_dim>([&](auto U) {
                __m512i query_high = _mm512_loadu_si512((const __m512i*)(query + i + U * stride));
                __m512i query_low = _mm512_loadu_si512((const __m512i*)(query + i + U * stride + packed_len));

                apply_indexed<batches>([&](auto I) {
                    __m512i doc_bytes = _mm512_loadu_si512((const __m512i*)(current_doc_ptrs[I] + i + U * stride));
                    __m512i doc_high = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 4), mask_half_byte);
                    __m512i doc_low = _mm512_and_si512(doc_bytes, mask_half_byte);

                    acc_high[I * unroll_dim + U] = _mm512_dpbusd_epi32(acc_high[I * unroll_dim + U], doc_high, query_high);
                    acc_low[I * unroll_dim + U] = _mm512_dpbusd_epi32(acc_low[I * unroll_dim + U], doc_low, query_low);
                });
            });
        }

        // Fold unroll_dim accumulators per batch back into acc_*[I]. Skipped at
        // unroll_dim=1, where the main loop already wrote acc_*[I*1+0] = acc_*[I]
        // -- byte-equivalent to the unroll-1 code.
        if constexpr (unroll_dim > 1) {
            apply_indexed<batches>([&](auto I) {
                acc_high[I] = tree_reduce<unroll_dim, __m512i, _mm512_add_epi32>(&acc_high[I * unroll_dim]);
                acc_low[I] = tree_reduce<unroll_dim, __m512i, _mm512_add_epi32>(&acc_low[I * unroll_dim]);
            });

            // Dim-unroll-1 tail: full strides not consumed by the main loop.
            for (; i < blk; i += stride) {
                __m512i query_high = _mm512_loadu_si512((const __m512i*)(query + i));
                __m512i query_low = _mm512_loadu_si512((const __m512i*)(query + i + packed_len));

                apply_indexed<batches>([&](auto I) {
                    __m512i doc_bytes = _mm512_loadu_si512((const __m512i*)(current_doc_ptrs[I] + i));
                    __m512i doc_high = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 4), mask_half_byte);
                    __m512i doc_low = _mm512_and_si512(doc_bytes, mask_half_byte);

                    acc_high[I] = _mm512_dpbusd_epi32(acc_high[I], doc_high, query_high);
                    acc_low[I] = _mm512_dpbusd_epi32(acc_low[I], doc_low, query_low);
                });
            }
        }

        if (tail_mask) {
            __m512i query_high = _mm512_maskz_loadu_epi8(tail_mask, query + blk);
            __m512i query_low = _mm512_maskz_loadu_epi8(tail_mask, query + blk + packed_len);

            apply_indexed<batches>([&](auto I) {
                __m512i doc_bytes = _mm512_maskz_loadu_epi8(tail_mask, current_doc_ptrs[I] + blk);
                __m512i doc_high = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, 4), mask_half_byte);
                __m512i doc_low = _mm512_and_si512(doc_bytes, mask_half_byte);

                acc_high[I] = _mm512_dpbusd_epi32(acc_high[I], doc_high, query_high);
                acc_low[I] = _mm512_dpbusd_epi32(acc_low[I], doc_low, query_low);
            });
        }

        apply_indexed<batches>([&](auto I) {
            results[c + I] = (f32_t)_mm512_reduce_add_epi32(_mm512_add_epi32(acc_high[I], acc_low[I]));
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
    doti4_bulk_impl_avx512<int8_t, sequential_mapper, 2, 1>(docs, query, packed_len, packed_len, NULL, count, results);
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
    doti4_bulk_impl_avx512<int8_t, offsets_mapper, 2, 1>(docs, query, packed_len, pitch, offsets, count, results);
}

EXPORT void vec_doti4_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* query,
    int32_t packed_len,
    int32_t count,
    f32_t* results
) {
    doti4_bulk_impl_avx512<const int8_t*, sparse_mapper, 4, 1>(
        (const int8_t* const*)addresses, query, packed_len, 0, NULL, count, results);
}
