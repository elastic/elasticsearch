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

// Accumulates acc += dot(pa, pb) for unsigned 7-bit int lanes (64 bytes per step).
template<int offsetRegs>
inline void fmai7u(__m512i& acc, const int8_t* pa, const int8_t* pb) {
    constexpr int lanes = offsetRegs * sizeof(__m512i);
    const __m512i a = _mm512_loadu_si512((const __m512i*)(pa + lanes));
    const __m512i b = _mm512_loadu_si512((const __m512i*)(pb + lanes));
    // Vertically multiply each unsigned 8-bit integer from a with the corresponding
    // signed 8-bit integer from b, producing intermediate signed 16-bit integers.
    const __m512i dot = _mm512_maddubs_epi16(a, b);
    const __m512i ones = _mm512_set1_epi16(1);
    // Horizontally add adjacent pairs of intermediate signed 16-bit ints, and pack the results in 32-bit ints.
    acc = _mm512_add_epi32(_mm512_madd_epi16(ones, dot), acc);
}

static inline int32_t dot7u_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    int i = 0;
    __m512i total_sum = _mm512_setzero_si512();
    if (dims >= sizeof(__m512i)) {
        i = dims & ~(sizeof(__m512i) - 1);

        constexpr int batches = 4;
        static_assert(batches % 2 == 0, "batches must be even for vectorized AVX-512 operations");
        constexpr int half_batches = batches / 2;
        constexpr int batch_stride = batches * sizeof(__m512i);
        constexpr int half_batch_stride = half_batches * sizeof(__m512i);
        const int8_t* pa = a;
        const int8_t* pb = b;

        __m512i acc[batches];
        // Init accumulator(s) with 0
        apply_indexed<batches>([&](auto I) {
            acc[I] = _mm512_setzero_si512();
        });

        const int8_t* pa_end = a + (i & ~(batch_stride - 1));
        while (pa < pa_end) {
            apply_indexed<batches>([&](auto I) {
                fmai7u<I>(acc[I], pa, pb);
            });
            pa += batch_stride;
            pb += batch_stride;
        }

        pa_end = a + (i & ~(half_batch_stride - 1));
        while (pa < pa_end) {
            apply_indexed<half_batches>([&](auto I) {
                fmai7u<I>(acc[I], pa, pb);
            });
            pa += half_batch_stride;
            pb += half_batch_stride;
        }

        pa_end = a + i;
        while (pa < pa_end) {
            fmai7u<0>(acc[0], pa, pb);
            pa += sizeof(__m512i);
            pb += sizeof(__m512i);
        }

        total_sum = tree_reduce<batches, __m512i, _mm512_add_epi32>(acc);
    }
    // Masked tail: handle remaining elements (< 64) with a single masked SIMD iteration.
    // Zeroed lanes from maskz_loadu produce zeros in maddubs, contributing nothing to the sum.
    const int remaining = dims - i;
    if (remaining > 0) {
        const __mmask64 mask = (__mmask64)((1ULL << remaining) - 1);
        const __m512i va = _mm512_maskz_loadu_epi8(mask, a + i);
        const __m512i vb = _mm512_maskz_loadu_epi8(mask, b + i);
        const __m512i vab = _mm512_maddubs_epi16(va, vb);
        const __m512i ones = _mm512_set1_epi16(1);
        total_sum = _mm512_add_epi32(total_sum, _mm512_madd_epi16(ones, vab));
    }
    return _mm512_reduce_add_epi32(total_sum);
}

EXPORT int32_t vec_doti7u_2(const int8_t* a, const int8_t* b, const int32_t dims) {
    return dot7u_inner(a, b, dims);
}

/*
 * Bulk dot product for 7-bit unsigned int lanes with shared query loads.
 *
 * Iterates over `batches` document vectors in parallel per dimension step,
 * loading the query once per step and reusing it across all batched documents
 * to amortise b-side L1D bandwidth across the batch.
 *
 * Two independent unroll axes:
 *   batches    - number of distinct document vectors scored in parallel;
 *                amortises the query load and adds independent madd chains
 *                across documents.
 *   unroll_dim - number of consecutive 64-byte blocks of the same document
 *                processed per inner-loop iteration, each into its own
 *                accumulator. Hides the maddubs/madd port-5 latency on hosts
 *                whose throughput exceeds what `batches` chains alone can
 *                saturate.
 *
 * The single-pair scorer `dot7u_inner` above uses the same dim-axis pattern
 * under its local `batches` constant.
 *
 * Prefetch strategy (head + spread):
 *   - At each batch boundary, software-prefetches the first `unroll_dim`
 *     cache lines of every next-batch vector so the very first inner-loop
 *     iter never waits on a demand miss.
 *   - At each inner iter, software-prefetches the next `unroll_dim` lines
 *     (the lines that the *next* outer iter will consume) of every
 *     next-batch vector. Spreading the issues across the inner loop keeps
 *     the L1d fill buffer occupancy below the per-core ceiling and lets the
 *     prefetched lines arrive ~1 outer iter before they are used.
 */
template <
    typename TData,
    const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int batches = 4,
    int unroll_dim = 1
>
static inline void dot7u_bulk_avx512(
    const TData* a, const int8_t* b, const int32_t dims,
    const int32_t pitch, const int32_t* offsets,
    const int32_t count, f32_t* results
) {
    constexpr int stride = sizeof(__m512i);              // 64 bytes
    constexpr int dimStride = stride * unroll_dim;
    const int blk = dims & ~(stride - 1);
    const __m512i ones = _mm512_set1_epi16(1);
    const int lines_to_fetch = dims / CACHE_LINE_SIZE + 1;
    int c = 0;

    const int8_t* current_vecs[batches];
    init_pointers<batches, TData, int8_t, mapper>(current_vecs, a, pitch, offsets, 0, count);

    for (; c + batches - 1 < count; c += batches) {
        const int8_t* next_vecs[batches];
        const bool has_next = c + 2 * batches - 1 < count;
        if (has_next) {
            apply_indexed<batches>([&](auto I) {
                next_vecs[I] = mapper(a, c + batches + I, offsets, pitch);
            });
            head_prefetch<batches, unroll_dim>(next_vecs);
        }

        // Row-major layout: acc[I * unroll_dim + U] keeps the unroll_dim
        // accumulators for batch member I contiguous, so tree_reduce can fold
        // them in one call.
        __m512i acc[batches * unroll_dim];
        apply_indexed<batches * unroll_dim>([&](auto I) {
            acc[I] = _mm512_setzero_si512();
        });

        int i = 0;
        for (; i + dimStride <= blk; i += dimStride) {
            if (has_next) {
                spread_prefetch<batches, unroll_dim>(next_vecs, i, lines_to_fetch);
            }
            apply_indexed<unroll_dim>([&](auto U) {
                __m512i bv = _mm512_loadu_si512((const __m512i*)(b + i + U * stride));
                apply_indexed<batches>([&](auto I) {
                    __m512i av = _mm512_loadu_si512((const __m512i*)(current_vecs[I] + i + U * stride));
                    acc[I * unroll_dim + U] = _mm512_add_epi32(
                        acc[I * unroll_dim + U],
                        _mm512_madd_epi16(ones, _mm512_maddubs_epi16(av, bv)));
                });
            });
        }

        // Fold the unroll_dim accumulators per batch back into acc[I] before
        // the unroll_dim=1 tail and the masked tail. Skipped at unroll_dim=1,
        // where the main loop already wrote into acc[I*1+0] = acc[I].
        if constexpr (unroll_dim > 1) {
            apply_indexed<batches>([&](auto I) {
                acc[I] = tree_reduce<unroll_dim, __m512i, _mm512_add_epi32>(&acc[I * unroll_dim]);
            });
            for (; i + stride <= blk; i += stride) {
                if (has_next) {
                    spread_prefetch<batches, 1>(next_vecs, i, lines_to_fetch);
                }
                __m512i bv = _mm512_loadu_si512((const __m512i*)(b + i));
                apply_indexed<batches>([&](auto I) {
                    __m512i av = _mm512_loadu_si512((const __m512i*)(current_vecs[I] + i));
                    acc[I] = _mm512_add_epi32(
                        acc[I], _mm512_madd_epi16(ones, _mm512_maddubs_epi16(av, bv)));
                });
            }
        }

        // Masked tail: zeroed lanes from maskz_loadu contribute nothing to the sum.
        const int rem = dims - i;
        if (rem > 0) {
            __mmask64 mask = (__mmask64)((1ULL << rem) - 1);
            __m512i bv = _mm512_maskz_loadu_epi8(mask, b + i);
            apply_indexed<batches>([&](auto I) {
                __m512i av = _mm512_maskz_loadu_epi8(mask, current_vecs[I] + i);
                acc[I] = _mm512_add_epi32(
                    acc[I], _mm512_madd_epi16(ones, _mm512_maddubs_epi16(av, bv)));
            });
        }

        apply_indexed<batches>([&](auto I) {
            results[c + I] = (f32_t)_mm512_reduce_add_epi32(acc[I]);
        });

        if (has_next) {
            std::copy_n(next_vecs, batches, current_vecs);
        }
    }

    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = (f32_t)dot7u_inner(a0, b, dims);
    }
}

EXPORT void vec_doti7u_bulk_2(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    dot7u_bulk_avx512<int8_t, sequential_mapper, 4, 1>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_doti7u_bulk_offsets_2(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dot7u_bulk_avx512<int8_t, offsets_mapper, 4, 1>(a, b, dims, pitch, offsets, count, results);
}

EXPORT void vec_doti7u_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results) {
    dot7u_bulk_avx512<const int8_t*, sparse_mapper, 4, 1>((const int8_t* const*)addresses, b, dims, 0, NULL, count, results);
}

// Accumulates acc += sqr_distance(a, b) for unsigned 7-bit int lanes already
// loaded into 64-byte SIMD registers.
inline void sqri7u(__m512i& acc, __m512i a, __m512i b) {
    const __m512i dist = _mm512_sub_epi8(a, b);
    // abs_epi8 makes the difference non-negative so it fits the unsigned
    // slot of maddubs_epi16, which treats its first operand as unsigned and
    // its second as signed. Squared value is unchanged: (a-b)^2 == |a-b|^2.
    const __m512i abs_dist = _mm512_abs_epi8(dist);
    const __m512i sqr_add = _mm512_maddubs_epi16(abs_dist, abs_dist);
    const __m512i ones = _mm512_set1_epi16(1);
    acc = _mm512_add_epi32(_mm512_madd_epi16(ones, sqr_add), acc);
}

// Accumulates acc += sqr_distance(pa, pb) for unsigned 7-bit int lanes (64 bytes per step).
template<int offsetRegs>
inline void sqri7u(__m512i& acc, const int8_t* pa, const int8_t* pb) {
    constexpr int lanes = offsetRegs * sizeof(__m512i);
    const __m512i a = _mm512_loadu_si512((const __m512i*)(pa + lanes));
    const __m512i b = _mm512_loadu_si512((const __m512i*)(pb + lanes));
    sqri7u(acc, a, b);
}

static inline int32_t sqr7u_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    int i = 0;
    __m512i total_sum = _mm512_setzero_si512();
    if (dims >= sizeof(__m512i)) {
        i = dims & ~(sizeof(__m512i) - 1);

        constexpr int batches = 4;
        static_assert(batches % 2 == 0, "batches must be even for vectorized AVX-512 operations");
        constexpr int half_batches = batches / 2;
        constexpr int batch_stride = batches * sizeof(__m512i);
        constexpr int half_batch_stride = half_batches * sizeof(__m512i);
        const int8_t* pa = a;
        const int8_t* pb = b;

        __m512i acc[batches];
        // Init accumulator(s) with 0
        apply_indexed<batches>([&](auto I) {
            acc[I] = _mm512_setzero_si512();
        });

        const int8_t* pa_end = a + (i & ~(batch_stride - 1));
        while (pa < pa_end) {
            apply_indexed<batches>([&](auto I) {
                sqri7u<I>(acc[I], pa, pb);
            });
            pa += batch_stride;
            pb += batch_stride;
        }

        pa_end = a + (i & ~(half_batch_stride - 1));
        while (pa < pa_end) {
            apply_indexed<half_batches>([&](auto I) {
                sqri7u<I>(acc[I], pa, pb);
            });
            pa += half_batch_stride;
            pb += half_batch_stride;
        }

        pa_end = a + i;
        while (pa < pa_end) {
            sqri7u<0>(acc[0], pa, pb);
            pa += sizeof(__m512i);
            pb += sizeof(__m512i);
        }

        total_sum = tree_reduce<batches, __m512i, _mm512_add_epi32>(acc);
    }
    // Masked tail: handle remaining elements (< 64) with a single masked SIMD iteration.
    // Zeroed lanes from maskz_loadu produce zeros in sub/abs/maddubs, contributing nothing to the sum.
    const int remaining = dims - i;
    if (remaining > 0) {
        const __mmask64 mask = (__mmask64)((1ULL << remaining) - 1);
        const __m512i va = _mm512_maskz_loadu_epi8(mask, a + i);
        const __m512i vb = _mm512_maskz_loadu_epi8(mask, b + i);
        const __m512i dist = _mm512_sub_epi8(va, vb);
        const __m512i abs_dist = _mm512_abs_epi8(dist);
        const __m512i sqr_add = _mm512_maddubs_epi16(abs_dist, abs_dist);
        const __m512i ones = _mm512_set1_epi16(1);
        total_sum = _mm512_add_epi32(total_sum, _mm512_madd_epi16(ones, sqr_add));
    }
    return _mm512_reduce_add_epi32(total_sum);
}

EXPORT int32_t vec_sqri7u_2(const int8_t* a, const int8_t* b, const int32_t dims) {
    return sqr7u_inner(a, b, dims);
}

/*
 * Bulk squared distance for 7-bit unsigned int lanes with shared query loads.
 *
 * Same shape as dot7u_bulk_avx512 (parallel batches, shared b per dimension
 * step) but with sub_epi8 + abs_epi8 + maddubs_epi16(abs,abs) per step. The
 * per-step op count (5 ops on ports 0/5: sub, abs, maddubs, madd, add)
 * already saturates the issue ports at one accumulator per batch on Sapphire
 * Rapids, so a dim-axis unroll would only add register pressure without
 * throughput. Only the batch-axis (shared-b) parallelism is applied.
 *
 * Prefetch strategy: see dot7u_bulk_avx512. Inner step is one cache line per
 * iter, so this kernel uses head=1 + 1-line spread per iter.
 */
template <
    typename TData,
    const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int batches = 4
>
static inline void sqr7u_bulk_avx512(
    const TData* a, const int8_t* b, const int32_t dims,
    const int32_t pitch, const int32_t* offsets,
    const int32_t count, f32_t* results
) {
    constexpr int stride = sizeof(__m512i);
    const int blk = dims & ~(stride - 1);
    const int lines_to_fetch = dims / CACHE_LINE_SIZE + 1;
    int c = 0;

    const int8_t* current_vecs[batches];
    init_pointers<batches, TData, int8_t, mapper>(current_vecs, a, pitch, offsets, 0, count);

    for (; c + batches - 1 < count; c += batches) {
        const int8_t* next_vecs[batches];
        const bool has_next = c + 2 * batches - 1 < count;
        if (has_next) {
            apply_indexed<batches>([&](auto I) {
                next_vecs[I] = mapper(a, c + batches + I, offsets, pitch);
            });
            head_prefetch<batches, 1>(next_vecs);
        }

        __m512i acc[batches];
        apply_indexed<batches>([&](auto I) {
            acc[I] = _mm512_setzero_si512();
        });

        int i = 0;
        for (; i + stride <= blk; i += stride) {
            if (has_next) {
                spread_prefetch<batches, 1>(next_vecs, i, lines_to_fetch);
            }
            __m512i bv = _mm512_loadu_si512((const __m512i*)(b + i));
            apply_indexed<batches>([&](auto I) {
                __m512i av = _mm512_loadu_si512((const __m512i*)(current_vecs[I] + i));
                sqri7u(acc[I], av, bv);
            });
        }

        const int rem = dims - i;
        if (rem > 0) {
            __mmask64 mask = (__mmask64)((1ULL << rem) - 1);
            __m512i bv = _mm512_maskz_loadu_epi8(mask, b + i);
            apply_indexed<batches>([&](auto I) {
                __m512i av = _mm512_maskz_loadu_epi8(mask, current_vecs[I] + i);
                sqri7u(acc[I], av, bv);
            });
        }

        apply_indexed<batches>([&](auto I) {
            results[c + I] = (f32_t)_mm512_reduce_add_epi32(acc[I]);
        });

        if (has_next) {
            std::copy_n(next_vecs, batches, current_vecs);
        }
    }

    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = (f32_t)sqr7u_inner(a0, b, dims);
    }
}

EXPORT void vec_sqri7u_bulk_2(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    sqr7u_bulk_avx512<int8_t, sequential_mapper>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqri7u_bulk_offsets_2(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    sqr7u_bulk_avx512<int8_t, offsets_mapper>(a, b, dims, pitch, offsets, count, results);
}

EXPORT void vec_sqri7u_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results) {
    sqr7u_bulk_avx512<const int8_t*, sparse_mapper>((const int8_t* const*)addresses, b, dims, 0, NULL, count, results);
}
