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
#include <math.h>
#include "vec.h"

#ifdef _MSC_VER
#include <intrin.h>
#elif __clang__
#include <x86intrin.h>
#elif __GNUC__
#include <x86intrin.h>
#endif

#include <emmintrin.h>
#include <immintrin.h>

#ifndef STRIDE_BYTES_LEN
#define STRIDE_BYTES_LEN sizeof(__m256i) // Must be a power of 2
#endif

template <int(*mapper)(int, int32_t*)>
static inline void dot7u_inner_bulk(int8_t* a, int8_t* b, int dims, int32_t* offsets, int count, f32_t* results) {
    int32_t res = 0;
    if (dims > STRIDE_BYTES_LEN) {
        const int limit = dims & ~(STRIDE_BYTES_LEN - 1);
        for (int32_t c = 0; c < count; c++) {
            const int8_t* a0 = a + mapper(c, offsets) * dims;
            int i = limit;
            res = dot7u_inner(a0, b, i);
            results[c] = (f32_t)res;
        }
    } else {
        for (int32_t c = 0; c < count; c++) {
            const int8_t* a0 = a + mapper(c, offsets) * dims;
            res = 0;
            for (int32_t i = 0; i < dims; i++) {
                res += a0[i] * b[i];
            }
            results[c] = (f32_t)res;
        }
    }
}

static inline int identity(int i, int32_t* offsets) {
   return i;
}

static inline int index(int i, int32_t* offsets) {
   return offsets[i];
}

extern "C"
EXPORT void dot7u_bulk(int8_t* a, int8_t* b, int dims, int count, f32_t* results) {
    dot7u_inner_bulk<identity>(a, b, dims, NULL, count, results);
}

extern "C"
EXPORT void dot7u_bulk_offsets(int8_t* a, int8_t* b, int dims, int32_t* offsets, int count, f32_t* results) {
    dot7u_inner_bulk<index>(a, b, dims, offsets, count, results);
}
