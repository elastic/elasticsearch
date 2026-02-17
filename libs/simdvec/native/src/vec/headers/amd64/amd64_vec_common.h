#ifndef AMD64_VEC_COMMON_INCLUDED
#define AMD64_VEC_COMMON_INCLUDED

#include "vec_common.h"
#include <emmintrin.h>
#include <immintrin.h>

#define CACHE_LINE_SIZE 64

static inline void prefetch(const void* ptr, int lines) {
    const uintptr_t base = align_downwards<CACHE_LINE_SIZE>(ptr);
    for (int k = 0; k < lines; ++k) {
        _mm_prefetch((void*)(base + k * CACHE_LINE_SIZE), _MM_HINT_T0);
    }
}

/* Utility functions to perform reduce operations (horizontal ops) over a vector
 * It works by narrowing in half until you're down to 1 element.
 * Schematically, this works as depicted: from a starting vector whose elements
 * are labelled as |1|2|3|4|5|6|7|8| we perform "op" over half vectors multiple
 * times, returning op(op(op(1, 5), op(3, 7)), op(2, 6), op(4, 8))).
 * "op" is the template argument mm_op_epi32, which is an intrinsic with the template
 * argument performing the binary "op" (e.g. _mm_add_epi32 for +, _mm_max_epi32 for max, etc.)
 *
 * |1|2|3|4|5|6|7|8|
 * extract
 * =
 * |5|6|7|8|
 *
 * |1|2|3|4|
 * op
 * |5|6|7|8|
 * =
 * |15|26|37|48|	:op128
 *
 * |15|26|37|48|
 * unpackhi
 * |15|26|37|48|
 * =
 * |37|48|37|48|	:hi64
 * op
 * |15|26|37|48|	:op128
 * =
 * |1537|2648|3737|4848|	:op64
 * _MM_SHUFFLE(2, 3, 0, 1));
 * |2648|1537|4848|3737|	:hi32
 *
 * |1537|2648|3737|4848|
 * op
 * |2648|1537|4848|3737|
 * =
 * |15372648|...
 */
template <__m128i(*mm_op_epi32)(const __m128i, const __m128i)>
static inline int32_t mm256_reduce_epi32(const __m256i a) {
    const __m128i op128 = mm_op_epi32(_mm256_castsi256_si128(a), _mm256_extractf128_si256(a, 1));
    const __m128i hi64 = _mm_unpackhi_epi64(op128, op128);
    const __m128i op64 = mm_op_epi32(hi64, op128);
    const __m128i hi32  = _mm_shuffle_epi32(op64, _MM_SHUFFLE(2, 3, 0, 1));
    return _mm_cvtsi128_si32(mm_op_epi32(op64, hi32));
}

template <__m128(*mm_op_ps)(const __m128, const __m128)>
static inline f32_t mm256_reduce_ps(const __m256 a) {
    const __m128 op128 = mm_op_ps(_mm256_castps256_ps128(a), _mm256_extractf128_ps(a, 1));
    const __m128 hi64 = _mm_castsi128_ps(_mm_unpackhi_epi64(_mm_castps_si128(op128), _mm_castps_si128(op128)));
    const __m128 op64 = mm_op_ps(hi64, op128);
    const __m128 hi32  = _mm_shuffle_ps(op64, op64, _MM_SHUFFLE(2, 3, 0, 1));
    return _mm_cvtss_f32(mm_op_ps(op64, hi32));
}

template <__m128i(*mm_op_epi64)(const __m128i, const __m128i)>
static inline int64_t mm256_reduce_epi64(const __m256i a) {
    const __m128i op128 = mm_op_epi64(_mm256_castsi256_si128(a), _mm256_extractf128_si256(a, 1));
    const __m128i hi64 = _mm_unpackhi_epi64(op128, op128);
    const __m128i op64 = mm_op_epi64(hi64, op128);
    return _mm_cvtsi128_si64(op64);
}

#endif // AMD64_VEC_COMMON_INCLUDED
