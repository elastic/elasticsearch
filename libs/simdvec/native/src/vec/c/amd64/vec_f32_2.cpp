/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

 // --- single precision floats (AVX-512) ---

 #include <stddef.h>
 #include <stdint.h>
 #include <algorithm>
 #include "vec.h"
 #include "vec_common.h"
 #include "amd64/amd64_vec_common.h"

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

        // Masked tail for remaining dimensions
        const int remaining = dims - i;
        if (remaining > 0) {
            const __mmask16 mask = (__mmask16)((1 << remaining) - 1);
            __m512 bi = _mm512_maskz_loadu_ps(mask, b + i);
            apply_indexed<batches>([&](auto I) {
                __m512 ai = _mm512_maskz_loadu_ps(mask, as[I] + i);
                sums[I] = inner_op(ai, bi, sums[I]);
            });
        }

        apply_indexed<batches>([&](auto I) {
            results[c + I] = _mm512_reduce_add_ps(sums[I]);
        });
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
