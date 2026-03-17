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

static inline int32_t doti4_inner(const int8_t* unpacked, const int8_t* packed, int32_t packed_len) {
    const __m128i mask_0f = _mm_set1_epi8(0x0F);
    __m256i acc0 = _mm256_setzero_si256();
    __m256i acc1 = _mm256_setzero_si256();

    constexpr int stride = sizeof(__m128i);
    const int blk = packed_len & ~(stride - 1);

    for (int i = 0; i < blk; i += stride) {
        __m128i p = _mm_loadu_si128((const __m128i*)(packed + i));

        // Extract nibbles: shift+mask for high, mask for low.
        // _mm_srli_epi16 shifts 16-bit lanes; the 0x0F mask cleans cross-byte leakage.
        __m128i hi = _mm_and_si128(_mm_srli_epi16(p, 4), mask_0f);
        __m128i lo = _mm_and_si128(p, mask_0f);

        __m128i u_hi = _mm_loadu_si128((const __m128i*)(unpacked + i));
        __m128i u_lo = _mm_loadu_si128((const __m128i*)(unpacked + i + packed_len));

        // Zero-extend 16 bytes to 16 shorts, then madd pairs into 8 int32s
        __m256i hi16 = _mm256_cvtepu8_epi16(hi);
        __m256i u_hi16 = _mm256_cvtepu8_epi16(u_hi);
        __m256i lo16 = _mm256_cvtepu8_epi16(lo);
        __m256i u_lo16 = _mm256_cvtepu8_epi16(u_lo);

        acc0 = _mm256_add_epi32(acc0, _mm256_madd_epi16(hi16, u_hi16));
        acc1 = _mm256_add_epi32(acc1, _mm256_madd_epi16(lo16, u_lo16));
    }

    int32_t total = mm256_reduce_epi32<_mm_add_epi32>(_mm256_add_epi32(acc0, acc1));

    for (int i = blk; i < packed_len; i++) {
        uint8_t p = (uint8_t)packed[i];
        total += (p >> 4) * unpacked[i];
        total += (p & 0x0F) * unpacked[i + packed_len];
    }
    return total;
}

EXPORT int32_t vec_doti4(const int8_t* unpacked, const int8_t* packed, int32_t packed_len) {
    return doti4_inner(unpacked, packed, packed_len);
}

template <int64_t(*mapper)(const int32_t, const int32_t*), int batches = 2>
static inline void doti4_bulk_impl(
    const int8_t* a,
    const int8_t* b,
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

    const int8_t* current_vecs[batches];
    init_offsets<batches, int8_t, mapper>(current_vecs, a, pitch, offsets, count);

    for (; c + 2 * batches - 1 < count; c += batches) {
        const int8_t* next_vecs[batches];
        apply_indexed<batches>([&](auto I) {
            next_vecs[I] = a + mapper(c + batches + I, offsets) * pitch;
            prefetch(next_vecs[I], lines_to_fetch);
        });

        __m256i acc0[batches];
        __m256i acc1[batches];
        apply_indexed<batches>([&](auto I) {
            acc0[I] = _mm256_setzero_si256();
            acc1[I] = _mm256_setzero_si256();
        });

        int i = 0;
        for (; i < blk; i += stride) {
            __m128i u_hi = _mm_loadu_si128((const __m128i*)(b + i));
            __m128i u_lo = _mm_loadu_si128((const __m128i*)(b + i + packed_len));
            __m256i u_hi16 = _mm256_cvtepu8_epi16(u_hi);
            __m256i u_lo16 = _mm256_cvtepu8_epi16(u_lo);

            apply_indexed<batches>([&](auto I) {
                __m128i p = _mm_loadu_si128((const __m128i*)(current_vecs[I] + i));
                __m128i hi = _mm_and_si128(_mm_srli_epi16(p, 4), mask_0f);
                __m128i lo = _mm_and_si128(p, mask_0f);

                acc0[I] = _mm256_add_epi32(acc0[I], _mm256_madd_epi16(_mm256_cvtepu8_epi16(hi), u_hi16));
                acc1[I] = _mm256_add_epi32(acc1[I], _mm256_madd_epi16(_mm256_cvtepu8_epi16(lo), u_lo16));
            });
        }

        int32_t res[batches];
        apply_indexed<batches>([&](auto I) {
            res[I] = mm256_reduce_epi32<_mm_add_epi32>(_mm256_add_epi32(acc0[I], acc1[I]));
        });

        for (; i < packed_len; i++) {
            uint8_t uhi = (uint8_t)b[i];
            uint8_t ulo = (uint8_t)b[i + packed_len];
            apply_indexed<batches>([&](auto I) {
                uint8_t p = (uint8_t)current_vecs[I][i];
                res[I] += (p >> 4) * uhi;
                res[I] += (p & 0x0F) * ulo;
            });
        }

        apply_indexed<batches>([&](auto I) {
            results[c + I] = (f32_t)res[I];
        });
        std::copy_n(next_vecs, batches, current_vecs);
    }

    for (; c < count; c++) {
        const int8_t* doc = a + mapper(c, offsets) * pitch;
        results[c] = (f32_t)doti4_inner(b, doc, packed_len);
    }
}

EXPORT void vec_doti4_bulk(const int8_t* a, const int8_t* b, int32_t packed_len, int32_t count, f32_t* results) {
    doti4_bulk_impl<identity_mapper>(a, b, packed_len, packed_len, NULL, count, results);
}

EXPORT void vec_doti4_bulk_offsets(
    const int8_t* a,
    const int8_t* b,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    doti4_bulk_impl<array_mapper>(a, b, packed_len, pitch, offsets, count, results);
}
