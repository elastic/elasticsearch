/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

 // This file contains implementations for processors supporting "2nd level" vector
 // capabilities; in the case of x64, this second level is support for AVX-512
 // instructions.

#include <stddef.h>
#include <stdint.h>

#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"

// Includes for intrinsics
#ifdef _MSC_VER
#include <intrin.h>
#elif __clang__
#include <x86intrin.h>
#elif __GNUC__
#include <x86intrin.h>
#endif

#include <emmintrin.h>
#include <immintrin.h>

// Returns acc + ( pa * pb ), for 64-wide int lanes.
template<int offsetRegs>
inline __m512i fma8(__m512i acc, const int8_t* pa, const int8_t* pb) {
    constexpr int lanes = offsetRegs * sizeof(__m512i);
    const __m512i a = _mm512_loadu_si512((const __m512i*)(pa + lanes));
    const __m512i b = _mm512_loadu_si512((const __m512i*)(pb + lanes));
    // Perform multiplication and create 16-bit values
    // Vertically multiply each unsigned 8-bit integer from a with the corresponding
    // signed 8-bit integer from b, producing intermediate signed 16-bit integers.
    // These values will be at max 32385, at min −32640
    const __m512i dot = _mm512_maddubs_epi16(a, b);
    const __m512i ones = _mm512_set1_epi16(1);
    // Horizontally add adjacent pairs of intermediate signed 16-bit ints, and pack the results in 32-bit ints.
    // Using madd with 1, as this is faster than extract 2 halves, add 16-bit ints, and convert to 32-bit ints.
    return _mm512_add_epi32(_mm512_madd_epi16(ones, dot), acc);
}

static inline int32_t dot7u_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims >= sizeof(__m512i)) {
        i = dims & ~(sizeof(__m512i) - 1);

        constexpr int batches = 8;
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
                acc[I] = fma8<I>(acc[I], pa, pb);
            });
            pa += batch_stride;
            pb += batch_stride;
        }

        pa_end = a + (i & ~(half_batch_stride - 1));
        while (pa < pa_end) {
            apply_indexed<half_batches>([&](auto I) {
                acc[I] = fma8<I>(acc[I], pa, pb);
            });
            pa += half_batch_stride;
            pb += half_batch_stride;
        }

        pa_end = a + i;
        while (pa < pa_end) {
            acc[0] = fma8<0>(acc[0], pa, pb);
            pa += sizeof(__m512i);
            pb += sizeof(__m512i);
        }

        // reduce (accumulate all)
        __m512i total_sum = tree_reduce<batches, __m512i, _mm512_add_epi32>(acc);
        res = _mm512_reduce_add_epi32(total_sum);
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
        res += _mm512_reduce_add_epi32(_mm512_madd_epi16(ones, vab));
    }
    return res;
}

EXPORT int32_t vec_dot7u_2(const int8_t* a, const int8_t* b, const int32_t dims) {
    return dot7u_inner(a, b, dims);
}

EXPORT void vec_dot7u_bulk_2(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<int8_t, sequential_mapper, dot7u_inner, 4>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_dot7u_bulk_offsets_2(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<int8_t, offsets_mapper, dot7u_inner, 4>(a, b, dims, pitch, offsets, count, results);
}

template<int offsetRegs>
inline __m512i sqr8(__m512i acc, const int8_t* pa, const int8_t* pb) {
    constexpr int lanes = offsetRegs * sizeof(__m512i);
    const __m512i a = _mm512_loadu_si512((const __m512i*)(pa + lanes));
    const __m512i b = _mm512_loadu_si512((const __m512i*)(pb + lanes));

    const __m512i dist = _mm512_sub_epi8(a, b);
    const __m512i abs_dist = _mm512_abs_epi8(dist);
    const __m512i sqr_add = _mm512_maddubs_epi16(abs_dist, abs_dist);
    const __m512i ones = _mm512_set1_epi16(1);
    // Horizontally add adjacent pairs of intermediate signed 16-bit integers, and pack the results.
    return _mm512_add_epi32(_mm512_madd_epi16(ones, sqr_add), acc);
}

static inline int32_t sqr7u_inner(const int8_t* a, const int8_t* b, const int32_t dims) {
    int32_t res = 0;
    int i = 0;
    if (dims >= sizeof(__m512i)) {
        i = dims & ~(sizeof(__m512i) - 1);

        constexpr int batches = 8;
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
                acc[I] = sqr8<I>(acc[I], pa, pb);
            });
            pa += batch_stride;
            pb += batch_stride;
        }

        pa_end = a + (i & ~(half_batch_stride - 1));
        while (pa < pa_end) {
            apply_indexed<half_batches>([&](auto I) {
                acc[I] = sqr8<I>(acc[I], pa, pb);
            });
            pa += half_batch_stride;
            pb += half_batch_stride;
        }

        pa_end = a + i;
        while (pa < pa_end) {
            acc[0] = sqr8<0>(acc[0], pa, pb);
            pa += sizeof(__m512i);
            pb += sizeof(__m512i);
        }

        // reduce (accumulate all)
        __m512i total_sum = tree_reduce<batches, __m512i, _mm512_add_epi32>(acc);
        res = _mm512_reduce_add_epi32(total_sum);
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
        res += _mm512_reduce_add_epi32(_mm512_madd_epi16(ones, sqr_add));
    }
    return res;
}

EXPORT int32_t vec_sqr7u_2(const int8_t* a, const int8_t* b, const int32_t dims) {
    return sqr7u_inner(a, b, dims);
}

EXPORT void vec_sqr7u_bulk_2(const int8_t* a, const int8_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_i8_bulk<int8_t, sequential_mapper, sqr7u_inner, 4>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqr7u_bulk_offsets_2(
    const int8_t* a,
    const int8_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_i8_bulk<int8_t, offsets_mapper, sqr7u_inner, 4>(a, b, dims, pitch, offsets, count, results);
}

// --- single precision floats (AVX-512) ---

static inline __m512 sqrf32_512(__m512 a, __m512 b, __m512 sum) {
    __m512 diff = _mm512_sub_ps(a, b);
    return _mm512_fmadd_ps(diff, diff, sum);
}

EXPORT f32_t vec_dotf32_2(const f32_t* a, const f32_t* b, const int32_t elementCount) {
    constexpr int batches = 4;

    __m512 sums[batches];
    apply_indexed<batches>([&](auto I) {
        sums[I] = _mm512_setzero_ps();
    });

    int i = 0;
    constexpr int elements = sizeof(__m512) / sizeof(f32_t);  // 16
    constexpr int stride = elements * batches;                 // 64
    for (; i < (elementCount & ~(stride - 1)); i += stride) {
        apply_indexed<batches>([&](auto I) {
            sums[I] = _mm512_fmadd_ps(_mm512_loadu_ps(a + i + I * elements), _mm512_loadu_ps(b + i + I * elements), sums[I]);
        });
    }

    __m512 total_sum = tree_reduce<batches, __m512, _mm512_add_ps>(sums);

    // Non-batched tail: accumulate into the same vector register
    for (; i + elements <= elementCount; i += elements) {
        total_sum = _mm512_fmadd_ps(_mm512_loadu_ps(a + i), _mm512_loadu_ps(b + i), total_sum);
    }
    // Masked tail: zeroed lanes from maskz_loadu contribute nothing via fmadd
    const int remaining = elementCount - i;
    if (remaining > 0) {
        const __mmask16 mask = (__mmask16)((1 << remaining) - 1);
        total_sum = _mm512_fmadd_ps(_mm512_maskz_loadu_ps(mask, a + i), _mm512_maskz_loadu_ps(mask, b + i), total_sum);
    }

    return _mm512_reduce_add_ps(total_sum);
}

EXPORT f32_t vec_sqrf32_2(const f32_t* a, const f32_t* b, const int32_t elementCount) {
    constexpr int batches = 4;

    __m512 sums[batches];
    apply_indexed<batches>([&](auto I) {
        sums[I] = _mm512_setzero_ps();
    });

    int i = 0;
    constexpr int elements = sizeof(__m512) / sizeof(f32_t);  // 16
    constexpr int stride = elements * batches;                 // 64
    for (; i < (elementCount & ~(stride - 1)); i += stride) {
        apply_indexed<batches>([&](auto I) {
            sums[I] = sqrf32_512(_mm512_loadu_ps(a + i + I * elements), _mm512_loadu_ps(b + i + I * elements), sums[I]);
        });
    }

    __m512 total_sum = tree_reduce<batches, __m512, _mm512_add_ps>(sums);

    // Non-batched tail: accumulate into the same vector register
    for (; i + elements <= elementCount; i += elements) {
        total_sum = sqrf32_512(_mm512_loadu_ps(a + i), _mm512_loadu_ps(b + i), total_sum);
    }
    // Masked tail: zeroed lanes from maskz_loadu contribute nothing via fmadd
    const int remaining = elementCount - i;
    if (remaining > 0) {
        const __mmask16 mask = (__mmask16)((1 << remaining) - 1);
        total_sum = sqrf32_512(_mm512_maskz_loadu_ps(mask, a + i), _mm512_maskz_loadu_ps(mask, b + i), total_sum);
    }

    return _mm512_reduce_add_ps(total_sum);
}

/*
 * Float bulk operation (AVX-512). Iterates over 4 sequential vectors at a time.
 */
template <
    typename TData,
    const f32_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    __m512(*inner_op)(const __m512, const __m512, const __m512),
    f32_t(*scalar_tail)(const f32_t*, const f32_t*, const int32_t),
    int batches = 4
>
static inline void call_f32_bulk_512(
    const TData* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    int c = 0;

    for (; c + batches - 1 < count; c += batches) {
        const f32_t* as[batches];
        __m512 sums[batches];
        apply_indexed<batches>([&](auto I) {
            as[I] = mapper(a, c + I, offsets, pitch);
            sums[I] = _mm512_setzero_ps();
        });

        int32_t i = 0;
        constexpr int stride = sizeof(__m512) / sizeof(f32_t);  // 16
        for (; i < (dims & ~(stride - 1)); i += stride) {
            __m512 bi = _mm512_loadu_ps(b + i);
            apply_indexed<batches>([&](auto I) {
                sums[I] = inner_op(_mm512_loadu_ps(as[I] + i), bi, sums[I]);
            });
        }

        f32_t res[batches];
        apply_indexed<batches>([&](auto I) {
            res[I] = _mm512_reduce_add_ps(sums[I]);
        });

        // Masked tail for remaining dimensions
        const int remaining = dims - i;
        if (remaining > 0) {
            const __mmask16 mask = (__mmask16)((1 << remaining) - 1);
            __m512 bi = _mm512_maskz_loadu_ps(mask, b + i);
            apply_indexed<batches>([&](auto I) {
                __m512 ai = _mm512_maskz_loadu_ps(mask, as[I] + i);
                __m512 partial = inner_op(ai, bi, _mm512_setzero_ps());
                res[I] += _mm512_reduce_add_ps(partial);
            });
        }

        std::copy_n(res, batches, results + c);
    }

    for (; c < count; c++) {
        const f32_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = scalar_tail(a0, b, dims);
    }
}

EXPORT void vec_dotf32_bulk_2(const f32_t* a, const f32_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_f32_bulk_512<f32_t, sequential_mapper, _mm512_fmadd_ps, vec_dotf32_2>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_dotf32_bulk_offsets_2(
    const f32_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_f32_bulk_512<f32_t, offsets_mapper, _mm512_fmadd_ps, vec_dotf32_2>(a, b, dims, pitch / sizeof(f32_t), offsets, count, results);
}

EXPORT void vec_sqrf32_bulk_2(const f32_t* a, const f32_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    call_f32_bulk_512<f32_t, sequential_mapper, sqrf32_512, vec_sqrf32_2>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqrf32_bulk_offsets_2(
    const f32_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    call_f32_bulk_512<f32_t, offsets_mapper, sqrf32_512, vec_sqrf32_2>(a, b, dims, pitch / sizeof(f32_t), offsets, count, results);
}
