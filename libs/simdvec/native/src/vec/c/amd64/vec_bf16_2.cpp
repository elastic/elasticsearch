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

    f32_t total = _mm512_reduce_add_ps(tree_reduce<batches, __m512, _mm512_add_ps>(sums));

    constexpr int stride256 = sizeof(__m256bh) / sizeof(bf16_t);
    __m256 acc256 = _mm256_setzero_ps();
    for (; i < (elementCount & ~(stride256 - 1)); i += stride256) {
        acc256 = _mm256_dpbf16_ps(acc256,
            (__m256bh)_mm256_loadu_epi16(d + i),
            (__m256bh)_mm256_loadu_epi16(q + i));
    }

    total += mm256_reduce_ps<_mm_add_ps>(acc256);

    // finish scalar
    for (; i < elementCount; ++i) {
        total += dot_scalar(d[i], q[i]);
    }

    return total;
}

EXPORT f32_t vec_dotDbf16Qbf16_2(const bf16_t* a, const bf16_t* b, const int32_t elementCount) {
    return dotDbf16Qbf16_inner_avx512(a, b, elementCount);
}

#ifdef __clang__
#pragma clang attribute pop
#elif __GNUC__
#pragma GCC pop_options
#endif
