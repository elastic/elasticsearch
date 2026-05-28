/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// AVX-512/VNNI packed-int dot product kernels, parameterised on the
// per-value bit width. See amd64_packed_int_traits.h for the layout.
//
// Uses VNNI vpdpbusd to accumulate directly into 32-bit lanes. Both operands
// are in [0, 2^BITS - 1] so the unsigned/signed asymmetry of vpdpbusd is
// irrelevant. Per-iteration per-lane max is 4*(2^BITS-1)^2, and the running
// sum over the whole vector is BITS-dependent but stays well below int32 for
// any practical packed_len, so no chunked 16-bit accumulator is required.

#ifndef AMD64_PACKED_INT_AVX512_INCLUDED
#define AMD64_PACKED_INT_AVX512_INCLUDED

#include <stddef.h>
#include <stdint.h>
#include <algorithm>

#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"
#include "amd64/amd64_packed_int_traits.h"

template <int BITS>
static inline int32_t doti_inner_avx512(const int8_t* query, const int8_t* doc, int32_t packed_len) {
    using T = packed_int_traits<BITS>;
    constexpr int N = T::stripes;
    const __m512i value_mask = _mm512_set1_epi8(T::mask);
    __m512i acc[N];
    apply_indexed<N>([&](auto K) { acc[K] = _mm512_setzero_si512(); });

    constexpr int stride = sizeof(__m512i);
    const int blk = packed_len & ~(stride - 1);

    for (int i = 0; i < blk; i += stride) {
        __m512i doc_bytes = _mm512_loadu_si512((const __m512i*)(doc + i));
        apply_indexed<N>([&](auto K) {
            // _mm512_srli_epi16 shifts 16-bit lanes; the value_mask cleans cross-byte leakage.
            __m512i doc_s;
            if constexpr (T::is_last_stripe(K)) {
                doc_s = _mm512_and_si512(doc_bytes, value_mask);
            } else {
                doc_s = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, T::shift(K)), value_mask);
            }
            __m512i query_s = _mm512_loadu_si512((const __m512i*)(query + i + K * packed_len));
            acc[K] = _mm512_dpbusd_epi32(acc[K], doc_s, query_s);
        });
    }

    // Masked tail: handle remaining bytes that don't fill a full 512-bit register.
    // Masked-off lanes load as zero, contributing nothing to the dot product.
    const int rem = packed_len - blk;
    if (rem > 0) {
        __mmask64 mask = (__mmask64)((1ULL << rem) - 1);
        __m512i doc_bytes = _mm512_maskz_loadu_epi8(mask, doc + blk);
        apply_indexed<N>([&](auto K) {
            __m512i doc_s;
            if constexpr (T::is_last_stripe(K)) {
                doc_s = _mm512_and_si512(doc_bytes, value_mask);
            } else {
                doc_s = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, T::shift(K)), value_mask);
            }
            __m512i query_s = _mm512_maskz_loadu_epi8(mask, query + blk + K * packed_len);
            acc[K] = _mm512_dpbusd_epi32(acc[K], doc_s, query_s);
        });
    }

    __m512i sum = acc[0];
    apply_indexed<N - 1>([&](auto K) {
        sum = _mm512_add_epi32(sum, acc[K + 1]);
    });
    return _mm512_reduce_add_epi32(sum);
}

template <
    int BITS,
    typename TData,
    const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int batches = 2
>
static inline void doti_bulk_impl_avx512(
    const TData* docs,
    const int8_t* query,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    using T = packed_int_traits<BITS>;
    constexpr int N = T::stripes;
    const __m512i value_mask = _mm512_set1_epi8(T::mask);
    constexpr int stride = sizeof(__m512i);
    static_assert(stride == CACHE_LINE_SIZE,
        "spread_prefetch_step<lines_per_iter=1> below assumes one full cache line per iter");
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
            });
            head_prefetch<batches, 1>(next_doc_ptrs);
        }

        __m512i acc[N][batches];
        apply_indexed<N>([&](auto K) {
            apply_indexed<batches>([&](auto I) {
                acc[K][I] = _mm512_setzero_si512();
            });
        });

        int i = 0;
        for (; i < blk; i += stride) {
            if (has_next) {
                spread_prefetch_step<batches, 1, stride>(next_doc_ptrs, i, lines_to_fetch);
            }
            __m512i query_s[N];
            apply_indexed<N>([&](auto K) {
                query_s[K] = _mm512_loadu_si512((const __m512i*)(query + i + K * packed_len));
            });

            apply_indexed<batches>([&](auto I) {
                __m512i doc_bytes = _mm512_loadu_si512((const __m512i*)(current_doc_ptrs[I] + i));
                apply_indexed<N>([&](auto K) {
                    __m512i doc_s;
                    if constexpr (T::is_last_stripe(K)) {
                        doc_s = _mm512_and_si512(doc_bytes, value_mask);
                    } else {
                        doc_s = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, T::shift(K)), value_mask);
                    }
                    acc[K][I] = _mm512_dpbusd_epi32(acc[K][I], doc_s, query_s[K]);
                });
            });
        }

        // Masked tail: same trick as the inner kernel — masked-off lanes load
        // as zero and contribute nothing to the dot product.
        if (tail_mask) {
            __m512i query_s[N];
            apply_indexed<N>([&](auto K) {
                query_s[K] = _mm512_maskz_loadu_epi8(tail_mask, query + blk + K * packed_len);
            });

            apply_indexed<batches>([&](auto I) {
                __m512i doc_bytes = _mm512_maskz_loadu_epi8(tail_mask, current_doc_ptrs[I] + blk);
                apply_indexed<N>([&](auto K) {
                    __m512i doc_s;
                    if constexpr (T::is_last_stripe(K)) {
                        doc_s = _mm512_and_si512(doc_bytes, value_mask);
                    } else {
                        doc_s = _mm512_and_si512(_mm512_srli_epi16(doc_bytes, T::shift(K)), value_mask);
                    }
                    acc[K][I] = _mm512_dpbusd_epi32(acc[K][I], doc_s, query_s[K]);
                });
            });
        }

        apply_indexed<batches>([&](auto I) {
            __m512i sum = acc[0][I];
            apply_indexed<N - 1>([&](auto K) {
                sum = _mm512_add_epi32(sum, acc[K + 1][I]);
            });
            results[c + I] = (f32_t)_mm512_reduce_add_epi32(sum);
        });

        if (has_next) {
            std::copy_n(next_doc_ptrs, batches, current_doc_ptrs);
        }
    }

    for (; c < count; c++) {
        const int8_t* doc = mapper(docs, c, offsets, pitch);
        results[c] = (f32_t)doti_inner_avx512<BITS>(query, doc, packed_len);
    }
}

#endif // AMD64_PACKED_INT_AVX512_INCLUDED
