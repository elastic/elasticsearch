/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

#include <stddef.h>
#include <stdint.h>

#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"

// Unlike i7u which uses maddubs (unsigned x signed), i8 needs sign-extension to 16-bit
// before multiply, because both operands can be negative. Each iteration loads 32 bytes
// (into __m256i), sign-extends to __m512i, then uses signed madd_epi16.

// Accumulates acc += dot(pa, pb) for signed int8. Loads 32 bytes at compile-time
// offset, sign-extends to 16-bit, then signed multiply-accumulate.
template<int offsetRegs>
inline void fmai8(__m512i& acc, const int8_t* pa, const int8_t* pb) {
    constexpr int lanes = offsetRegs * sizeof(__m256i);  // 32 bytes per step
    const __m512i a16 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(pa + lanes)));
    const __m512i b16 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(pb + lanes)));
    acc = _mm512_add_epi32(_mm512_madd_epi16(a16, b16), acc);
}

static inline int32_t doti8_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    int i = 0;
    __m512i total_sum = _mm512_setzero_si512();
    if (dims >= sizeof(__m256i)) {
        i = dims & ~(sizeof(__m256i) - 1);

        constexpr int batches = 4;
        static_assert(batches % 2 == 0, "batches must be even for vectorized AVX-512 operations");
        constexpr int half_batches = batches / 2;
        constexpr int batch_stride = batches * sizeof(__m256i);
        constexpr int half_batch_stride = half_batches * sizeof(__m256i);
        const int8_t* pa = a;
        const int8_t* pb = b;

        __m512i acc[batches];
        apply_indexed<batches>([&](auto I) {
            acc[I] = _mm512_setzero_si512();
        });

        const int8_t* pa_end = a + (i & ~(batch_stride - 1));
        while (pa < pa_end) {
            apply_indexed<batches>([&](auto I) {
                fmai8<I>(acc[I], pa, pb);
            });
            pa += batch_stride;
            pb += batch_stride;
        }

        pa_end = a + (i & ~(half_batch_stride - 1));
        while (pa < pa_end) {
            apply_indexed<half_batches>([&](auto I) {
                fmai8<I>(acc[I], pa, pb);
            });
            pa += half_batch_stride;
            pb += half_batch_stride;
        }

        pa_end = a + i;
        while (pa < pa_end) {
            fmai8<0>(acc[0], pa, pb);
            pa += sizeof(__m256i);
            pb += sizeof(__m256i);
        }

        total_sum = tree_reduce<batches, __m512i, _mm512_add_epi32>(acc);
    }
    // Masked tail: sign-extend remaining elements (< 32) with a single masked SIMD iteration.
    // Zeroed lanes from maskz_loadu produce zeros in madd_epi16, contributing nothing to the sum.
    const int remaining = dims - i;
    if (remaining > 0) {
        const __mmask32 mask = (__mmask32)((1ULL << remaining) - 1);
        const __m512i a16 = _mm512_cvtepi8_epi16(_mm256_maskz_loadu_epi8(mask, a + i));
        const __m512i b16 = _mm512_cvtepi8_epi16(_mm256_maskz_loadu_epi8(mask, b + i));
        total_sum = _mm512_add_epi32(total_sum, _mm512_madd_epi16(a16, b16));
    }
    return _mm512_reduce_add_epi32(total_sum);
}

EXPORT f32_t vec_doti8_2(const int8_t* a, const int8_t* b, const int32_t dims) {
    return (f32_t)doti8_inner(a, b, dims);
}

// Bulk-optimized dot i8
// Uses the algebraic identity: a*b = (a+128)*b - 128*sum(b), where (a+128) is
// unsigned. This is to work around x64 asymmetric dot-product ops (DPBUSD computes
// unsigned*signed multiply-accumulate).
// Precompute the query correction once, then each document
// vector only needs XOR + DPBUSD (no SAD per vector).

// Lean fmai8 for bulk: only XOR on a + DPBUSD, no b-side correction work.
template<int offsetRegs>
inline void fmai8_bulk(__m512i& acc_ab, const int8_t* pa, const int8_t* pb) {
    constexpr int lanes = offsetRegs * sizeof(__m512i);  // 64 bytes per step
    const __m512i xor_mask = _mm512_set1_epi8((char)0x80);
    const __m512i a = _mm512_loadu_si512(pa + lanes);
    const __m512i b = _mm512_loadu_si512(pb + lanes);
    acc_ab = _mm512_dpbusd_epi32(acc_ab, _mm512_xor_si512(a, xor_mask), b);
}

// Precompute the correction term from the query vector b.
// Returns 128 * sum(b), computed via the XOR+SAD trick.
static inline int64_t precompute_b_correction(const int8_t* b, const int32_t dims) {
    const __m512i xor_mask = _mm512_set1_epi8((char)0x80);
    const __m512i zeros = _mm512_setzero_si512();
    __m512i sum_b_bias = _mm512_setzero_si512();
    int i = 0;
    for (; i + 64 <= dims; i += 64) {
        const __m512i bv = _mm512_loadu_si512(b + i);
        sum_b_bias = _mm512_add_epi64(sum_b_bias, _mm512_sad_epu8(_mm512_xor_si512(bv, xor_mask), zeros));
    }
    if (i < dims) {
        const __mmask64 mask = (__mmask64)_bzhi_u64(0xFFFFFFFFFFFFFFFFULL, dims - i);
        const __m512i bv = _mm512_maskz_loadu_epi8(mask, b + i);
        sum_b_bias = _mm512_add_epi64(sum_b_bias, _mm512_sad_epu8(
            _mm512_xor_si512(bv, _mm512_maskz_set1_epi8(mask, (char)0x80)), zeros));
    }
    int64_t sum_b_biased = _mm512_reduce_add_epi64(sum_b_bias);
    // correction = 128 * sum(b) = 128 * (sum(b+128) - 128*dims)
    // No phantom values thanks to masked XOR in tail.
    return 128LL * sum_b_biased - 16384LL * dims;
}

// Inner op for bulk: DPBUSD only, applies precomputed correction at the end.
static inline int32_t doti8_inner_bulk(const int8_t* a, const int8_t* b, const int32_t dims, const int64_t b_correction) {
    int i = 0;
    __m512i total_ab = _mm512_setzero_si512();
    if (dims >= sizeof(__m512i)) {
        i = dims & ~(sizeof(__m512i) - 1);

        constexpr int batches = 4;
        constexpr int half_batches = batches / 2;
        constexpr int batch_stride = batches * sizeof(__m512i);
        constexpr int half_batch_stride = half_batches * sizeof(__m512i);
        const int8_t* pa = a;
        const int8_t* pb = b;

        __m512i acc_ab[batches];
        apply_indexed<batches>([&](auto I) {
            acc_ab[I] = _mm512_setzero_si512();
        });

        const int8_t* pa_end = a + (i & ~(batch_stride - 1));
        while (pa < pa_end) {
            apply_indexed<batches>([&](auto I) {
                fmai8_bulk<I>(acc_ab[I], pa, pb);
            });
            pa += batch_stride;
            pb += batch_stride;
        }

        pa_end = a + (i & ~(half_batch_stride - 1));
        while (pa < pa_end) {
            apply_indexed<half_batches>([&](auto I) {
                fmai8_bulk<I>(acc_ab[I], pa, pb);
            });
            pa += half_batch_stride;
            pb += half_batch_stride;
        }

        pa_end = a + i;
        while (pa < pa_end) {
            fmai8_bulk<0>(acc_ab[0], pa, pb);
            pa += sizeof(__m512i);
            pb += sizeof(__m512i);
        }

        total_ab = tree_reduce<batches, __m512i, _mm512_add_epi32>(acc_ab);
    }
    const int remaining = dims - i;
    if (remaining > 0) {
        const __m512i xor_mask = _mm512_set1_epi8((char)0x80);
        const __mmask64 mask = (__mmask64)_bzhi_u64(0xFFFFFFFFFFFFFFFFULL, remaining);
        const __m512i a_raw = _mm512_maskz_loadu_epi8(mask, a + i);
        const __m512i b_raw = _mm512_maskz_loadu_epi8(mask, b + i);
        total_ab = _mm512_dpbusd_epi32(total_ab, _mm512_xor_si512(a_raw, xor_mask), b_raw);
    }
    return (int32_t)(_mm512_reduce_add_epi32(total_ab) - b_correction);
}

// Bulk dot i8 with prefetch. Precomputes query correction once, then scores
// each document with the lean DPBUSD-only inner loop.
template <typename TData, const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t)>
static inline void doti8_bulk(
    const TData* a, const int8_t* b, const int32_t dims, const int32_t pitch,
    const int32_t* offsets, const int32_t count, f32_t* results
) {
    constexpr int batches = 4;
    const int lines_to_fetch = dims / CACHE_LINE_SIZE + 1;
    const int64_t b_correction = precompute_b_correction(b, dims);
    int c = 0;

    const int8_t* current_vecs[batches];
    init_pointers<batches, TData, int8_t, mapper>(current_vecs, a, pitch, offsets, 0, count);

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
            results[c + I] = (f32_t)doti8_inner_bulk(current_vecs[I], b, dims, b_correction);
        });

        if (has_next) {
            std::copy_n(next_vecs, batches, current_vecs);
        }
    }

    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = (f32_t)doti8_inner_bulk(a0, b, dims, b_correction);
    }
}

EXPORT void vec_doti8_bulk_2(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    doti8_bulk<int8_t, sequential_mapper>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_doti8_bulk_offsets_2(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    doti8_bulk<int8_t, offsets_mapper>(a, b, dims, pitch, offsets, count, results);
}

EXPORT void vec_doti8_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results) {
    doti8_bulk<const int8_t*, sparse_mapper>((const int8_t* const*)addresses, b, dims, 0, NULL, count, results);
}

// Accumulates acc += sqr_distance(pa, pb) for signed int8. Sign-extends to 16-bit,
// subtracts, then madd for squared accumulation.
template<int offsetRegs>
inline void sqri8(__m512i& acc, const int8_t* pa, const int8_t* pb) {
    constexpr int lanes = offsetRegs * sizeof(__m256i);
    const __m512i a16 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(pa + lanes)));
    const __m512i b16 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(pb + lanes)));
    const __m512i dist = _mm512_sub_epi16(a16, b16);
    acc = _mm512_add_epi32(_mm512_madd_epi16(dist, dist), acc);
}

static inline int32_t sqri8_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    int i = 0;
    __m512i total_sum = _mm512_setzero_si512();
    if (dims >= sizeof(__m256i)) {
        i = dims & ~(sizeof(__m256i) - 1);

        constexpr int batches = 4;
        static_assert(batches % 2 == 0, "batches must be even for vectorized AVX-512 operations");
        constexpr int half_batches = batches / 2;
        constexpr int batch_stride = batches * sizeof(__m256i);
        constexpr int half_batch_stride = half_batches * sizeof(__m256i);
        const int8_t* pa = a;
        const int8_t* pb = b;

        __m512i acc[batches];
        apply_indexed<batches>([&](auto I) {
            acc[I] = _mm512_setzero_si512();
        });

        const int8_t* pa_end = a + (i & ~(batch_stride - 1));
        while (pa < pa_end) {
            apply_indexed<batches>([&](auto I) {
                sqri8<I>(acc[I], pa, pb);
            });
            pa += batch_stride;
            pb += batch_stride;
        }

        pa_end = a + (i & ~(half_batch_stride - 1));
        while (pa < pa_end) {
            apply_indexed<half_batches>([&](auto I) {
                sqri8<I>(acc[I], pa, pb);
            });
            pa += half_batch_stride;
            pb += half_batch_stride;
        }

        pa_end = a + i;
        while (pa < pa_end) {
            sqri8<0>(acc[0], pa, pb);
            pa += sizeof(__m256i);
            pb += sizeof(__m256i);
        }

        total_sum = tree_reduce<batches, __m512i, _mm512_add_epi32>(acc);
    }
    // Masked tail: zeroed lanes from maskz_loadu produce zeros in sub/madd, contributing nothing.
    const int remaining = dims - i;
    if (remaining > 0) {
        const __mmask32 mask = (__mmask32)((1ULL << remaining) - 1);
        const __m512i a16 = _mm512_cvtepi8_epi16(_mm256_maskz_loadu_epi8(mask, a + i));
        const __m512i b16 = _mm512_cvtepi8_epi16(_mm256_maskz_loadu_epi8(mask, b + i));
        const __m512i dist = _mm512_sub_epi16(a16, b16);
        total_sum = _mm512_add_epi32(total_sum, _mm512_madd_epi16(dist, dist));
    }
    return _mm512_reduce_add_epi32(total_sum);
}

EXPORT f32_t vec_sqri8_2(const int8_t* a, const int8_t* b, const int32_t dims) {
    return (f32_t)sqri8_inner(a, b, dims);
}

EXPORT void vec_sqri8_bulk_2(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<int8_t, sequential_mapper, sqri8_inner, 4>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqri8_bulk_offsets_2(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<int8_t, offsets_mapper, sqri8_inner, 4>(a, b, dims, pitch, offsets, count, results);
}

EXPORT void vec_sqri8_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<const int8_t*, sparse_mapper, sqri8_inner, 4>((const int8_t* const*)addresses, b, dims, 0, NULL, count, results);
}

// --- Cosine i8 (signed) AVX-512 ---
// Computes cosine similarity: sum(a*b) / sqrt(sum(a*a) * sum(b*b))
// Uses sign-extension like dot/sqr i8, with 3 accumulators (sum, a_norm, b_norm).
static inline f32_t cosi8_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    __m512i sum = _mm512_setzero_si512();
    __m512i a_norm = _mm512_setzero_si512();
    __m512i b_norm = _mm512_setzero_si512();

    int i = 0;
    const int blk = dims & ~(sizeof(__m256i) - 1);
    for (; i < blk; i += sizeof(__m256i)) {
        const __m512i a16 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(a + i)));
        const __m512i b16 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(b + i)));
        sum = _mm512_add_epi32(_mm512_madd_epi16(a16, b16), sum);
        a_norm = _mm512_add_epi32(_mm512_madd_epi16(a16, a16), a_norm);
        b_norm = _mm512_add_epi32(_mm512_madd_epi16(b16, b16), b_norm);
    }

    // Masked tail
    const int remaining = dims - i;
    if (remaining > 0) {
        const __mmask32 mask = (__mmask32)((1ULL << remaining) - 1);
        const __m512i a16 = _mm512_cvtepi8_epi16(_mm256_maskz_loadu_epi8(mask, a + i));
        const __m512i b16 = _mm512_cvtepi8_epi16(_mm256_maskz_loadu_epi8(mask, b + i));
        sum = _mm512_add_epi32(_mm512_madd_epi16(a16, b16), sum);
        a_norm = _mm512_add_epi32(_mm512_madd_epi16(a16, a16), a_norm);
        b_norm = _mm512_add_epi32(_mm512_madd_epi16(b16, b16), b_norm);
    }

    int32_t sum_i32 = _mm512_reduce_add_epi32(sum);
    int32_t a_norm_i32 = _mm512_reduce_add_epi32(a_norm);
    int32_t b_norm_i32 = _mm512_reduce_add_epi32(b_norm);
    return (f32_t) ((double) sum_i32 / __builtin_sqrt((double) a_norm_i32 * b_norm_i32));
}

EXPORT f32_t vec_cosi8_2(const int8_t* a, const int8_t* b, const int32_t dims) {
    return cosi8_inner(a, b, dims);
}

template <typename TData, const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t), int batches = 4>
static inline void cosi8_inner_bulk(
    const TData* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    const int blk = dims & ~(sizeof(__m256i) - 1);
    const int lines_to_fetch = dims / CACHE_LINE_SIZE + 1;
    int c = 0;

    // Precompute b norm
    __m512i b_norms = _mm512_setzero_si512();
    int bi = 0;
    for (; bi < blk; bi += sizeof(__m256i)) {
        const __m512i vb16 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(b + bi)));
        b_norms = _mm512_add_epi32(_mm512_madd_epi16(vb16, vb16), b_norms);
    }
    // Masked tail for b norm
    const int b_remaining = dims - bi;
    if (b_remaining > 0) {
        const __mmask32 mask = (__mmask32)((1ULL << b_remaining) - 1);
        const __m512i vb16 = _mm512_cvtepi8_epi16(_mm256_maskz_loadu_epi8(mask, b + bi));
        b_norms = _mm512_add_epi32(_mm512_madd_epi16(vb16, vb16), b_norms);
    }
    int32_t b_norm = _mm512_reduce_add_epi32(b_norms);

    const int8_t* current_vecs[batches];
    init_pointers<batches, TData, int8_t, mapper>(current_vecs, a, pitch, offsets, 0, count);

    static_assert(batches % 2 == 0, "batches must be even for vectorized cosine");
    for (; c + batches - 1 < count; c += batches) {
        const int8_t* next_vecs[batches];
        const bool has_next = c + 2 * batches - 1 < count;
        if (has_next) {
            apply_indexed<batches>([&](auto I) {
                next_vecs[I] = mapper(a, c + batches + I, offsets, pitch);
                prefetch(next_vecs[I], lines_to_fetch);
            });
        }

        __m512i sums[batches];
        __m512i a_norms[batches];
        apply_indexed<batches>([&](auto I) {
            sums[I] = _mm512_setzero_si512();
            a_norms[I] = _mm512_setzero_si512();
        });

        int i = 0;
        for (; i < blk; i += sizeof(__m256i)) {
            const __m512i vb16 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(b + i)));

            apply_indexed<batches>([&](auto I) {
                const __m512i va16 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(current_vecs[I] + i)));
                sums[I] = _mm512_add_epi32(_mm512_madd_epi16(va16, vb16), sums[I]);
                a_norms[I] = _mm512_add_epi32(_mm512_madd_epi16(va16, va16), a_norms[I]);
            });
        }

        // Masked tail
        const int remaining = dims - i;
        if (remaining > 0) {
            const __mmask32 mask = (__mmask32)((1ULL << remaining) - 1);
            const __m512i vb16 = _mm512_cvtepi8_epi16(_mm256_maskz_loadu_epi8(mask, b + i));
            apply_indexed<batches>([&](auto I) {
                const __m512i va16 = _mm512_cvtepi8_epi16(_mm256_maskz_loadu_epi8(mask, current_vecs[I] + i));
                sums[I] = _mm512_add_epi32(_mm512_madd_epi16(va16, vb16), sums[I]);
                a_norms[I] = _mm512_add_epi32(_mm512_madd_epi16(va16, va16), a_norms[I]);
            });
        }

        // Vectorized cosine finalization: results[i] = sum[i] / sqrt(a_norm[i] * b_norm)
        // batches=4 fits exactly in one __m128
        __m128 sum_ps = _mm_setr_ps(
            _mm512_reduce_add_epi32(sums[0]), _mm512_reduce_add_epi32(sums[1]),
            _mm512_reduce_add_epi32(sums[2]), _mm512_reduce_add_epi32(sums[3]));
        __m128 a_norm_ps = _mm_setr_ps(
            _mm512_reduce_add_epi32(a_norms[0]), _mm512_reduce_add_epi32(a_norms[1]),
            _mm512_reduce_add_epi32(a_norms[2]), _mm512_reduce_add_epi32(a_norms[3]));
        __m128 b_norm_ps = _mm_set1_ps(b_norm);
        __m128 res = _mm_div_ps(sum_ps, _mm_sqrt_ps(_mm_mul_ps(a_norm_ps, b_norm_ps)));
        _mm_storeu_ps(results + c, res);

        if (has_next) {
            std::copy_n(next_vecs, batches, current_vecs);
        }
    }

    // Tail-handling: remaining vectors
    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = cosi8_inner(a0, b, dims);
    }
}

EXPORT void vec_cosi8_bulk_2(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    cosi8_inner_bulk<int8_t, sequential_mapper>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_cosi8_bulk_offsets_2(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    cosi8_inner_bulk<int8_t, offsets_mapper>(a, b, dims, pitch, offsets, count, results);
}

EXPORT void vec_cosi8_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results) {
    cosi8_inner_bulk<const int8_t*, sparse_mapper>((const int8_t* const*)addresses, b, dims, 0, NULL, count, results);
}
