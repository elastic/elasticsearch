/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// AVX2-vectorized packed-int dot product kernels, parameterised on the
// per-value bit width. See amd64_packed_int_traits.h for the layout.
//
// _mm256_maddubs_epi16 multiplies unsigned*signed byte pairs and horizontally
// adds adjacent products into 16-bit results. Both operands are in
// [0, 2^BITS - 1] so signedness doesn't matter. We accumulate in 16-bit
// inside chunked inner loops and widen to 32-bit between chunks; chunk size
// is taken from the traits and reflects the headroom before 16-bit overflow.

#ifndef AMD64_PACKED_INT_AVX2_INCLUDED
#define AMD64_PACKED_INT_AVX2_INCLUDED

#include <stddef.h>
#include <stdint.h>
#include <algorithm>

#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"
#include "amd64/amd64_packed_int_traits.h"

template <int BITS>
static inline int32_t doti_inner_avx2(const int8_t* query, const int8_t* doc, int32_t packed_len) {
    using T = packed_int_traits<BITS>;
    constexpr int N = T::stripes;
    const __m256i value_mask = _mm256_set1_epi8(T::mask);
    const __m256i ones = _mm256_set1_epi16(1);
    __m256i acc = _mm256_setzero_si256();

    constexpr int stride = sizeof(__m256i);
    const int blk = packed_len & ~(stride - 1);
    constexpr int chunk = T::chunk_iters * stride;

    int i = 0;
    while (i < blk) {
        __m256i acc16[N];
        apply_indexed<N>([&](auto K) { acc16[K] = _mm256_setzero_si256(); });
        const int end = std::min(i + chunk, blk);

        for (; i < end; i += stride) {
            __m256i doc_bytes = _mm256_loadu_si256((const __m256i*)(doc + i));
            apply_indexed<N>([&](auto K) {
                // _mm256_srli_epi16 shifts 16-bit lanes; the value_mask cleans cross-byte leakage.
                __m256i doc_s;
                if constexpr (T::is_last_stripe(K)) {
                    doc_s = _mm256_and_si256(doc_bytes, value_mask);
                } else {
                    doc_s = _mm256_and_si256(_mm256_srli_epi16(doc_bytes, T::shift(K)), value_mask);
                }
                __m256i query_s = _mm256_loadu_si256((const __m256i*)(query + i + K * packed_len));
                acc16[K] = _mm256_add_epi16(acc16[K], _mm256_maddubs_epi16(doc_s, query_s));
            });
        }

        // Widen 16->32 bit: _mm256_madd_epi16 with ones horizontally adds pairs of
        // signed 16-bit values into 32-bit results, then accumulate into the 32-bit total.
        apply_indexed<N>([&](auto K) {
            acc = _mm256_add_epi32(acc, _mm256_madd_epi16(ones, acc16[K]));
        });
    }

    int32_t total = mm256_reduce_epi32<_mm_add_epi32>(acc);

    for (int i = blk; i < packed_len; i++) {
        uint8_t doc_byte = (uint8_t)doc[i];
        apply_indexed<N>([&](auto K) {
            total += ((doc_byte >> T::shift(K)) & T::mask) * (uint8_t)query[i + K * packed_len];
        });
    }
    return total;
}

template <
    int BITS,
    typename TData,
    const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int batches = 2
>
static inline void doti_bulk_impl_avx2(
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
    const __m256i value_mask = _mm256_set1_epi8(T::mask);
    const __m256i ones = _mm256_set1_epi16(1);
    constexpr int stride = sizeof(__m256i);
    const int blk = packed_len & ~(stride - 1);
    constexpr int chunk = T::chunk_iters * stride;
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
            __m256i acc16[N][batches];
            apply_indexed<N>([&](auto K) {
                apply_indexed<batches>([&](auto I) {
                    acc16[K][I] = _mm256_setzero_si256();
                });
            });

            const int end = std::min(i + chunk, blk);

            for (; i < end; i += stride) {
                __m256i query_s[N];
                apply_indexed<N>([&](auto K) {
                    query_s[K] = _mm256_loadu_si256((const __m256i*)(query + i + K * packed_len));
                });

                apply_indexed<batches>([&](auto I) {
                    __m256i doc_bytes = _mm256_loadu_si256((const __m256i*)(current_doc_ptrs[I] + i));
                    apply_indexed<N>([&](auto K) {
                        __m256i doc_s;
                        if constexpr (T::is_last_stripe(K)) {
                            doc_s = _mm256_and_si256(doc_bytes, value_mask);
                        } else {
                            doc_s = _mm256_and_si256(_mm256_srli_epi16(doc_bytes, T::shift(K)), value_mask);
                        }
                        acc16[K][I] = _mm256_add_epi16(acc16[K][I], _mm256_maddubs_epi16(doc_s, query_s[K]));
                    });
                });
            }

            apply_indexed<batches>([&](auto I) {
                apply_indexed<N>([&](auto K) {
                    acc32[I] = _mm256_add_epi32(acc32[I], _mm256_madd_epi16(ones, acc16[K][I]));
                });
            });
        }

        int32_t res[batches];
        apply_indexed<batches>([&](auto I) {
            res[I] = mm256_reduce_epi32<_mm_add_epi32>(acc32[I]);
        });

        for (; i < packed_len; i++) {
            uint8_t q[N];
            apply_indexed<N>([&](auto K) {
                q[K] = (uint8_t)query[i + K * packed_len];
            });
            apply_indexed<batches>([&](auto I) {
                uint8_t doc_byte = (uint8_t)current_doc_ptrs[I][i];
                apply_indexed<N>([&](auto K) {
                    res[I] += ((doc_byte >> T::shift(K)) & T::mask) * q[K];
                });
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
        results[c] = (f32_t)doti_inner_avx2<BITS>(query, doc, packed_len);
    }
}

#endif // AMD64_PACKED_INT_AVX2_INCLUDED
