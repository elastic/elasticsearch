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
    __m512 acc = _mm512_setzero_ps();

    int i = 0;
    constexpr int stride = sizeof(__m512bh) / sizeof(bf16_t);
    for (; i < (elementCount & ~(stride - 1)); i += stride) {
        acc = _mm512_dpbf16_ps(acc,
          (__m512bh)_mm512_loadu_epi16(d + i),
          (__m512bh)_mm512_loadu_epi16(q + i));
    }

    // Masked tail: handle remaining bytes that don't fill a full 512-bit register.
    // Masked-off lanes load as zero, contributing nothing to the dot product.
    const int rem = elementCount - i;
    if (rem > 0) {
        __mmask32 readMask = (__mmask32)((1UL << rem) - 1);
        __mmask16 dpMask = (__mmask16)((1U << (rem/2)) - 1);

        __m512bh d_rem = (__m512bh)_mm512_maskz_loadu_epi16(readMask, d + i);
        __m512bh q_rem = (__m512bh)_mm512_maskz_loadu_epi16(readMask, q + i);
        acc = _mm512_maskz_dpbf16_ps(dpMask, acc, d_rem, q_rem);
    }

    f32_t total = _mm512_reduce_add_ps(acc);

    if (rem & 1) {
        // odd number of elements at the end
        total += dot_scalar(d[i + rem - 1], q[i + rem - 1]);
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
