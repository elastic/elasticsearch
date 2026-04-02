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

#include <stddef.h>
#include <stdint.h>

#ifdef __clang__
#pragma clang attribute push(__attribute__((target("arch=cooperlake"))), apply_to=function)
#elif __GNUC__
#pragma GCC push_options
#pragma GCC target ("arch=cooperlake")
#endif

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
    constexpr int batches = 2;

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
        total = _mm512_maskz_dpbf16_ps(dpMask, total, d_rem, q_rem);
    }

    f32_t result = _mm512_reduce_add_ps(total);

    if (maskRem & 1 != 0) {
        // odd number of dimensions
        result += dot_scalar(d[elementCount - 1], q[elementCount - 1]);
    }

    return result;
}

/*
 * BFloat16 single operation. Processes 8 dimensions at a time.
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
    constexpr int batches = 2;

    __m512 sums[batches];
    apply_indexed<batches>([&](auto I) {
        sums[I] = _mm512_setzero_ps();
    });

    int i = 0;
    // each value has <elements> floats, and we iterate over <stride> floats at a time
    constexpr int elements = sizeof(__m256) / sizeof(bf16_t);
    constexpr int stride = sizeof(__m256) / sizeof(bf16_t) * batches;
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

EXPORT f32_t vec_dotDbf16Qbf16_2(const bf16_t* a, const bf16_t* b, const int32_t elementCount) {
    return dotDbf16Qbf16_inner_avx512(a, b, elementCount);
}

EXPORT f32_t vec_dotDbf16Qf32_2(const bf16_t* a, const f32_t* b, const int32_t elementCount) {
    return bf16_inner_avx512<f32_t, load_f32, _mm512_fmadd_ps, dot_scalar>(a, b, elementCount);
}

static inline __m512 sqrf32_vector(const __m512 a, const __m512 b, const __m512 sum) {
    __m512 diff = _mm512_sub_ps(a, b);
    return _mm512_fmadd_ps(diff, diff, sum);
}

EXPORT f32_t vec_sqrDbf16Qf32_2(const bf16_t* a, const f32_t* b, const int32_t elementCount) {
    return bf16_inner_avx512<f32_t, load_f32, sqrf32_vector, sqr_scalar>(a, b, elementCount);
}

EXPORT f32_t vec_sqrDbf16Qbf16_2(const bf16_t* a, const bf16_t* b, const int32_t elementCount) {
    return bf16_inner_avx512<bf16_t, load_bf16, sqrf32_vector, sqr_scalar>(a, b, elementCount);
}

#ifdef __clang__
#pragma clang attribute pop
#elif __GNUC__
#pragma GCC pop_options
#endif
