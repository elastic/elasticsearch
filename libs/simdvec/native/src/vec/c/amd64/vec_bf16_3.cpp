/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// AVX-512 vectorized BF16 vector operations

#include <stddef.h>
#include <stdint.h>

#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"

static inline __m512 bf16_to_f32(__m256i bf16) {
    __m512i widened = _mm512_cvtepu16_epi32(bf16);
    __m512i shifted = _mm512_slli_epi32(widened, 16);
    return _mm512_castsi512_ps(shifted);
}

static inline __m512 load_bf16(const bf16_t* ptr, const int elements) {
    return bf16_to_f32(_mm256_lddqu_si256((const __m256i*)(ptr + elements)));
}

static inline __m512 load_f32(const f32_t* ptr, const int elements) {
    return _mm512_loadu_ps(ptr + elements);
}

/*
 * Specialization for dot product of 2 vectors of BFloat16s using the dpbf16 instructions.
 */
static inline f32_t dotDbf16Qbf16_inner_avx512(const bf16_t* d, const bf16_t* q, int32_t elementCount) {
    constexpr int batches = 8;

    __m512 sums[batches];
    apply_indexed<batches>([&](auto I) {
        sums[I] = _mm512_setzero_ps();
    });

    int i = 0;
    constexpr int elements = sizeof(__m512bh) / sizeof(bf16_t);
    constexpr int stride512 = sizeof(__m512bh) / sizeof(bf16_t) * batches;
    for (; i < (elementCount & ~(stride512 - 1)); i += stride512) {
        apply_indexed<batches>([&](auto I) {
            sums[I] = _mm512_dpbf16_ps(sums[I],
                (__m512bh)_mm512_loadu_epi16(d + i + I * elements),
                (__m512bh)_mm512_loadu_epi16(q + i + I * elements));
        });
    }

    __m512 total = tree_reduce<batches, __m512, _mm512_add_ps>(sums);

    // another complete register at the end
    if (elementCount - i > elements) {
        total = _mm512_dpbf16_ps(total,
            (__m512bh)_mm512_loadu_epi16(d + i),
            (__m512bh)_mm512_loadu_epi16(q + i));
        i += elements;
    }

    // Masked tail: handle remaining bytes that don't fill a full 512-bit register.
    // Masked-off lanes load as zero, contributing nothing to the dot product.
    const int maskRem = elementCount - i;
    if (maskRem > 0) {
        __mmask32 readMask = (__mmask32)((1UL << maskRem) - 1);
        __mmask16 dpMask = (__mmask16)((1U << (maskRem/2)) - 1);
        __m512bh d_rem = (__m512bh)_mm512_maskz_loadu_epi16(readMask, d + i);
        __m512bh q_rem = (__m512bh)_mm512_maskz_loadu_epi16(readMask, q + i);
        total = _mm512_mask_dpbf16_ps(total, dpMask, d_rem, q_rem);
    }

    f32_t result = _mm512_reduce_add_ps(total);

    if ((maskRem & 1) != 0) {
        // odd number of dimensions
        result += dot_scalar(d[elementCount - 1], q[elementCount - 1]);
    }

    return result;
}

/*
 * BFloat16 single operation with manual bf16->f32 conversion + fmadd.
 * Processes 16 bf16 elements per batch (256-bit bf16 load -> 512-bit f32).
 *
 * Template parameters:
 * Q: the type of query vector
 * load_q: loads the query vector as a __m512 of 32-bit floats
 * inner_op: SIMD per-dimension vector operation, takes a, b, sum, returns new sum
 * scalar_op: scalar per-dimension vector operation, takes a, b, returns sum
 *
 * This should compile to a single inline method, with no function callouts.
 */
template<
    typename TQuery,
    __m512(*load_q)(const TQuery*, const int),
    __m512(*vector_op)(const __m512, const __m512, const __m512),
    f32_t(*scalar_op)(const bf16_t, const TQuery)
>
static inline f32_t bf16_inner_avx512(const bf16_t* d, const TQuery* q, const int32_t elementCount) {
    constexpr int batches = 4;

    __m512 sums[batches];
    apply_indexed<batches>([&](auto I) {
        sums[I] = _mm512_setzero_ps();
    });

    int i = 0;
    // each __m256i holds 16 bf16 values, widened to 16 f32 in a __m512
    constexpr int elements = sizeof(__m256) / sizeof(bf16_t);
    constexpr int stride = elements * batches;
    for (; i < (elementCount & ~(stride - 1)); i += stride) {
        apply_indexed<batches>([&](auto I) {
            sums[I] = vector_op(load_bf16(d, i + I * elements), load_q(q, i + I * elements), sums[I]);
        });
    }

    // Combine all partial sums
    __m512 total_sum = tree_reduce<batches, __m512, _mm512_add_ps>(sums);
    f32_t result = _mm512_reduce_add_ps(total_sum);

    for (; i < elementCount; ++i) {
        result += scalar_op(d[i], q[i]);
    }

    return result;
}

EXPORT f32_t vec_dotDbf16Qbf16_3(const bf16_t* a, const bf16_t* b, const int32_t elementCount) {
    return dotDbf16Qbf16_inner_avx512(a, b, elementCount);
}

EXPORT f32_t vec_dotDbf16Qf32_3(const bf16_t* a, const f32_t* b, const int32_t elementCount) {
    return bf16_inner_avx512<f32_t, load_f32, _mm512_fmadd_ps, dot_scalar>(a, b, elementCount);
}

static inline __m512 sqrf32_vector(const __m512 a, const __m512 b, const __m512 sum) {
    __m512 diff = _mm512_sub_ps(a, b);
    return _mm512_fmadd_ps(diff, diff, sum);
}

EXPORT f32_t vec_sqrDbf16Qf32_3(const bf16_t* a, const f32_t* b, const int32_t elementCount) {
    return bf16_inner_avx512<f32_t, load_f32, sqrf32_vector, sqr_scalar>(a, b, elementCount);
}

/*
 * Squared distance of 2 bf16 vectors using dpbf16 for subtraction.
 *
 * Strategy: interleave a and b into [a₀,b₀,a₁,b₁,...], then dpbf16
 * against a constant [1,-1,1,-1,...] to compute (a_i - b_i) in f32.
 * Then square with regular FMA.
 *
 * We load 32 bf16 from each vector (512-bit loads), then unpacklo/hi
 * to get two registers of 16 interleaved pairs each. Each dpbf16
 * produces 16 f32 differences; FMA squares and accumulates them.
 * This processes 32 elements per batch with 2 loads + 2 shuffles +
 * 2 dpbf16 + 2 FMA — no bf16→f32 integer conversion needed.
 */
static inline f32_t sqrDbf16Qbf16_inner_avx512(const bf16_t* a, const bf16_t* b, int32_t elementCount) {
    // bf16 constant [1.0, -1.0, 1.0, -1.0, ...] — 32 bf16 values
    // bf16 representation: 1.0 = 0x3F80, -1.0 = 0xBF80
    static const __m512bh ones_neg_ones = (__m512bh)_mm512_set1_epi32(0xBF803F80);

    constexpr int batches = 4;

    __m512 sums[batches];
    apply_indexed<batches>([&](auto I) {
        sums[I] = _mm512_setzero_ps();
    });

    int i = 0;
    // 32 bf16 elements per 512-bit load; we process 32 elements per batch
    // (unpacklo handles the even-indexed 128-bit lane halves,
    //  unpackhi handles the odd-indexed halves — together they cover all 32)
    constexpr int elements = sizeof(__m512bh) / sizeof(bf16_t);
    constexpr int stride = elements * batches;
    for (; i < (elementCount & ~(stride - 1)); i += stride) {
        apply_indexed<batches>([&](auto I) {
            __m512i av = _mm512_loadu_epi16(a + i + I * elements);
            __m512i bv = _mm512_loadu_epi16(b + i + I * elements);
            // Interleave into [a₀,b₀,a₁,b₁,...] pairs within each 128-bit lane
            __m512bh lo = (__m512bh)_mm512_unpacklo_epi16(av, bv);
            __m512bh hi = (__m512bh)_mm512_unpackhi_epi16(av, bv);
            // dpbf16 against [1,-1,...] computes a_i*1 + b_i*(-1) = a_i - b_i per lane
            __m512 diff_lo = _mm512_dpbf16_ps(_mm512_setzero_ps(), lo, ones_neg_ones);
            __m512 diff_hi = _mm512_dpbf16_ps(_mm512_setzero_ps(), hi, ones_neg_ones);
            // Square and accumulate
            sums[I] = _mm512_fmadd_ps(diff_lo, diff_lo, sums[I]);
            sums[I] = _mm512_fmadd_ps(diff_hi, diff_hi, sums[I]);
        });
    }

    __m512 total = tree_reduce<batches, __m512, _mm512_add_ps>(sums);

    // Non-batched tail: one full 512-bit register at a time
    for (; i + elements <= elementCount; i += elements) {
        __m512i av = _mm512_loadu_epi16(a + i);
        __m512i bv = _mm512_loadu_epi16(b + i);
        __m512bh lo = (__m512bh)_mm512_unpacklo_epi16(av, bv);
        __m512bh hi = (__m512bh)_mm512_unpackhi_epi16(av, bv);
        __m512 diff_lo = _mm512_dpbf16_ps(_mm512_setzero_ps(), lo, ones_neg_ones);
        __m512 diff_hi = _mm512_dpbf16_ps(_mm512_setzero_ps(), hi, ones_neg_ones);
        total = _mm512_fmadd_ps(diff_lo, diff_lo, total);
        total = _mm512_fmadd_ps(diff_hi, diff_hi, total);
    }

    f32_t result = _mm512_reduce_add_ps(total);

    // Scalar tail for remaining elements
    for (; i < elementCount; ++i) {
        result += sqr_scalar(a[i], b[i]);
    }

    return result;
}

EXPORT f32_t vec_sqrDbf16Qbf16_3(const bf16_t* a, const bf16_t* b, const int32_t elementCount) {
    return sqrDbf16Qbf16_inner_avx512(a, b, elementCount);
}
