/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

#include <stddef.h>
#include <stdint.h>
#include "vec.h"

#ifdef _MSC_VER
#include <intrin.h>
#elif __clang__
#pragma clang attribute push(__attribute__((target("arch=skylake-avx512"))), apply_to=function)
#include <x86intrin.h>
#elif __GNUC__
#pragma GCC push_options
#pragma GCC target ("arch=skylake-avx512")
#include <x86intrin.h>
#endif

#include <emmintrin.h>
#include <immintrin.h>

#ifndef STRIDE_BYTES_LEN
#define STRIDE_BYTES_LEN sizeof(__m512i) // Must be a power of 2
#endif

template<int32_t (*F)(int8_t*, int8_t*, size_t)>
inline int32_t call_vector_function(int8_t* a, int8_t* b, size_t dims) {
   int32_t res = 0;
   int i = 0;
   if (dims > STRIDE_BYTES_LEN) {
       i += dims & ~(STRIDE_BYTES_LEN - 1);
       res = F(a, b, i);
   }
   for (; i < dims; i++) {
       res += a[i] * b[i];
   }
   return res;
}

// Returns acc + ( p1 * p2 ), for 64-wide int lanes.
template<int offsetRegs>
inline __m512i fma8(__m512i acc, const int8_t* p1, const int8_t* p2) {
    constexpr int lanes = offsetRegs * STRIDE_BYTES_LEN;
    const __m512i a = _mm512_loadu_si512((const __m512i*)(p1 + lanes));
    const __m512i b = _mm512_loadu_si512((const __m512i*)(p2 + lanes));
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

static inline int32_t dot7u_inner_avx512(int8_t* a, int8_t* b, size_t dims) {
    constexpr int stride8 = 8 * STRIDE_BYTES_LEN;
    constexpr int stride4 = 4 * STRIDE_BYTES_LEN;
    const int8_t* p1 = a;
    const int8_t* p2 = b;

    const ptrdiff_t rem = (( dims - 1 ) % STRIDE_BYTES_LEN) + 1;
    const int8_t* const p1End = p1 + dims - rem;

    // Init accumulator(s) with 0
    __m512i acc0 = _mm512_setzero_si512();
    __m512i acc1 = _mm512_setzero_si512();
    __m512i acc2 = _mm512_setzero_si512();
    __m512i acc3 = _mm512_setzero_si512();
    __m512i acc4 = _mm512_setzero_si512();
    __m512i acc5 = _mm512_setzero_si512();
    __m512i acc6 = _mm512_setzero_si512();
    __m512i acc7 = _mm512_setzero_si512();

    while (p1 < p1End) {
        acc0 = fma8<0>(acc0, p1, p2);
        acc1 = fma8<1>(acc1, p1, p2);
        acc2 = fma8<2>(acc2, p1, p2);
        acc3 = fma8<3>(acc3, p1, p2);
        acc4 = fma8<4>(acc4, p1, p2);
        acc5 = fma8<5>(acc5, p1, p2);
        acc6 = fma8<6>(acc6, p1, p2);
        acc7 = fma8<7>(acc7, p1, p2);
        p1 += stride8;
        p2 += stride8;
    }

    while (p1 < p1End) {
        acc0 = fma8<0>(acc0, p1, p2);
        acc1 = fma8<1>(acc1, p1, p2);
        acc2 = fma8<2>(acc2, p1, p2);
        acc3 = fma8<3>(acc3, p1, p2);
        p1 += stride4;
        p2 += stride4;
    }

    while (p1 < p1End) {
        acc0 = fma8<0>(acc0, p1, p2);
        p1 += STRIDE_BYTES_LEN;
        p2 += STRIDE_BYTES_LEN;
    }

    // reduce (accumulate all)
    acc0 = _mm512_add_epi32(_mm512_add_epi32(acc0, acc1), _mm512_add_epi32(acc2, acc3));
    acc4 = _mm512_add_epi32(_mm512_add_epi32(acc4, acc5), _mm512_add_epi32(acc6, acc7));
    return _mm512_reduce_add_epi32(_mm512_add_epi32(acc0, acc4));
}

extern "C"
EXPORT int32_t dot7u_2(int8_t* a, int8_t* b, size_t dims) {
    return call_vector_function<dot7u_inner_avx512>(a, b, dims);
}

// Returns acc + ( p1 * p2 ), for 64-wide byte lanes.
// Data in p1 is considered as packed unsigned int4 (0x0 to 0xF); data in p2 is considered as unpacked unsigned int4 (0x00 to 0x0F)
// Data in acc/retured data is 32-wide short (16-bit) lanes.
template<int offsetRegs>
inline __m512i fma4(__m512i acc, const int8_t* p1, const int8_t* p2) {
  constexpr int a1Offset = 2 * offsetRegs * STRIDE_BYTES_LEN;
  constexpr int a2Offset = (2 * offsetRegs + 1) * STRIDE_BYTES_LEN;
  constexpr int bOffset = offsetRegs * STRIDE_BYTES_LEN;
  const __m512i a1 = _mm512_loadu_si512((const __m512i*)(p1 + a1Offset));
  const __m512i a2 = _mm512_loadu_si512((const __m512i*)(p1 + a2Offset));
  const __m512i b = _mm512_loadu_si512((const __m512i*)(p2 + bOffset));

  const __m512i lomask = _mm512_set1_epi8(0x0F);
  const __m512i lob = _mm512_and_si512(b, lomask);
  const __m512i dot1 = _mm512_maddubs_epi16(a1, lob);
  const __m512i acc1 = _mm512_add_epi16(dot1, acc);

  const __m512i himask = _mm512_set1_epi32(0xF0F0F0F0);
  const __m512i hib = _mm512_srli_epi32(_mm512_and_si512(b, himask), 4);
  const __m512i dot2 = _mm512_maddubs_epi16(a2, hib);

  return _mm512_add_epi16(acc1, dot2);
}

// Note: dims (sizeof(a)) must be even
static inline int32_t dot4u_inner_avx512(int8_t* a, int8_t* b, size_t dims) {
  constexpr int stride16 = 16 * STRIDE_BYTES_LEN;
  constexpr int stride8 = 8 * STRIDE_BYTES_LEN;
  const int8_t* p1 = a;
  const int8_t* p2 = b;

  const ptrdiff_t rem = (( dims - 1 ) % STRIDE_BYTES_LEN) + 1;
  const int8_t* const p1End = p1 + dims - rem;

  // Init accumulator(s) with 0
  __m512i acc0 = _mm512_setzero_si512();
  __m512i acc1 = _mm512_setzero_si512();
  __m512i acc2 = _mm512_setzero_si512();
  __m512i acc3 = _mm512_setzero_si512();
  __m512i acc4 = _mm512_setzero_si512();
  __m512i acc5 = _mm512_setzero_si512();
  __m512i acc6 = _mm512_setzero_si512();
  __m512i acc7 = _mm512_setzero_si512();

  while (p1 < p1End) {
      __m512i acc16_0 = _mm512_setzero_si512();
      __m512i acc16_1 = _mm512_setzero_si512();
      __m512i acc16_2 = _mm512_setzero_si512();
      __m512i acc16_3 = _mm512_setzero_si512();
      __m512i acc16_4 = _mm512_setzero_si512();
      __m512i acc16_5 = _mm512_setzero_si512();
      __m512i acc16_6 = _mm512_setzero_si512();
      __m512i acc16_7 = _mm512_setzero_si512();

      int i = 0;
      while (i < 256 && p1 < p1End) {
          acc16_0 = fma4<0>(acc16_0, p1, p2);
          acc16_1 = fma4<1>(acc16_1, p1, p2);
          acc16_2 = fma4<2>(acc16_2, p1, p2);
          acc16_3 = fma4<3>(acc16_3, p1, p2);
          acc16_4 = fma4<4>(acc16_4, p1, p2);
          acc16_5 = fma4<5>(acc16_5, p1, p2);
          acc16_6 = fma4<6>(acc16_6, p1, p2);
          acc16_7 = fma4<7>(acc16_7, p1, p2);
          ++i;
          p1 += stride16;
          p2 += stride8;
      }

      const __m512i ones = _mm512_set1_epi16(1);
      // Horizontally add adjacent pairs of intermediate signed 16-bit integers, and pack the results.
      acc0 = _mm512_add_epi32(_mm512_madd_epi16(ones, acc16_0), acc0);
      acc1 = _mm512_add_epi32(_mm512_madd_epi16(ones, acc16_1), acc1);
      acc2 = _mm512_add_epi32(_mm512_madd_epi16(ones, acc16_2), acc2);
      acc3 = _mm512_add_epi32(_mm512_madd_epi16(ones, acc16_3), acc3);
      acc4 = _mm512_add_epi32(_mm512_madd_epi16(ones, acc16_4), acc4);
      acc5 = _mm512_add_epi32(_mm512_madd_epi16(ones, acc16_5), acc5);
      acc6 = _mm512_add_epi32(_mm512_madd_epi16(ones, acc16_6), acc6);
      acc7 = _mm512_add_epi32(_mm512_madd_epi16(ones, acc16_7), acc7);
  }

  if (p1 < p1End) {
      __m512i acc16_0 = _mm512_setzero_si512();
      while (p1 < p1End) {
          acc16_0 = fma4<0>(acc16_0, p1, p2);
          p1 += (2 * STRIDE_BYTES_LEN);
          p2 += STRIDE_BYTES_LEN;
      }
      const __m512i ones = _mm512_set1_epi16(1);
      acc0 = _mm512_add_epi32(_mm512_madd_epi16(ones, acc16_0), acc0);
  }

  // reduce (accumulate all)
  acc0 = _mm512_add_epi32(_mm512_add_epi32(acc0, acc1), _mm512_add_epi32(acc2, acc3));
  acc4 = _mm512_add_epi32(_mm512_add_epi32(acc4, acc5), _mm512_add_epi32(acc6, acc7));
  return _mm512_reduce_add_epi32(_mm512_add_epi32(acc0, acc4));
}

extern "C"
EXPORT int32_t dot4u_2(int8_t* a, int8_t* b, size_t dims) {
    return call_vector_function<dot4u_inner_avx512>(a, b, dims);
}

template<int offsetRegs>
inline __m512i sqr8(__m512i acc, const int8_t* p1, const int8_t* p2) {
    constexpr int lanes = offsetRegs * STRIDE_BYTES_LEN;
    const __m512i a = _mm512_loadu_si512((const __m512i*)(p1 + lanes));
    const __m512i b = _mm512_loadu_si512((const __m512i*)(p2 + lanes));

    const __m512i dist = _mm512_sub_epi8(a, b);
    const __m512i abs_dist = _mm512_abs_epi8(dist);
    const __m512i sqr_add = _mm512_maddubs_epi16(abs_dist, abs_dist);
    const __m512i ones = _mm512_set1_epi16(1);
    // Horizontally add adjacent pairs of intermediate signed 16-bit integers, and pack the results.
    return _mm512_add_epi32(_mm512_madd_epi16(ones, sqr_add), acc);
}

static inline int32_t sqr7u_inner_avx512(int8_t *a, int8_t *b, size_t dims) {
    constexpr int stride8 = 8 * STRIDE_BYTES_LEN;
    constexpr int stride4 = 4 * STRIDE_BYTES_LEN;
    const int8_t* p1 = a;
    const int8_t* p2 = b;

    const ptrdiff_t rem = (( dims - 1 ) % STRIDE_BYTES_LEN) + 1;
    const int8_t* const p1End = p1 + dims - rem;

    // Init accumulator(s) with 0
    __m512i acc0 = _mm512_setzero_si512();
    __m512i acc1 = _mm512_setzero_si512();
    __m512i acc2 = _mm512_setzero_si512();
    __m512i acc3 = _mm512_setzero_si512();
    __m512i acc4 = _mm512_setzero_si512();
    __m512i acc5 = _mm512_setzero_si512();
    __m512i acc6 = _mm512_setzero_si512();
    __m512i acc7 = _mm512_setzero_si512();

    while (p1 < p1End) {
        acc0 = sqr8<0>(acc0, p1, p2);
        acc1 = sqr8<1>(acc1, p1, p2);
        acc2 = sqr8<2>(acc2, p1, p2);
        acc3 = sqr8<3>(acc3, p1, p2);
        acc4 = sqr8<4>(acc4, p1, p2);
        acc5 = sqr8<5>(acc5, p1, p2);
        acc6 = sqr8<6>(acc6, p1, p2);
        acc7 = sqr8<7>(acc7, p1, p2);
        p1 += stride8;
        p2 += stride8;
    }

    while (p1 < p1End) {
        acc0 = sqr8<0>(acc0, p1, p2);
        acc1 = sqr8<1>(acc1, p1, p2);
        acc2 = sqr8<2>(acc2, p1, p2);
        acc3 = sqr8<3>(acc3, p1, p2);
        p1 += stride4;
        p2 += stride4;
    }

    while (p1 < p1End) {
        acc0 = sqr8<0>(acc0, p1, p2);
        p1 += STRIDE_BYTES_LEN;
        p2 += STRIDE_BYTES_LEN;
    }

    // reduce (accumulate all)
    acc0 = _mm512_add_epi32(_mm512_add_epi32(acc0, acc1), _mm512_add_epi32(acc2, acc3));
    acc4 = _mm512_add_epi32(_mm512_add_epi32(acc4, acc5), _mm512_add_epi32(acc6, acc7));
    return _mm512_reduce_add_epi32(_mm512_add_epi32(acc0, acc4));
}

extern "C"
EXPORT int32_t sqr7u_2(int8_t* a, int8_t* b, size_t dims) {
    return call_vector_function<sqr7u_inner_avx512>(a, b, dims);
}

#ifdef __clang__
#pragma clang attribute pop
#elif __GNUC__
#pragma GCC pop_options
#endif
