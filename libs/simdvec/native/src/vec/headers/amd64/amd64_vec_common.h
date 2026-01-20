#ifndef AMD64_VEC_COMMON_INCLUDED
#define AMD64_VEC_COMMON_INCLUDED

#include "vec_common.h"
#include <immintrin.h>

#define CACHE_LINE_SIZE 64

static inline void prefetch(const void* ptr, int lines) {
    const uintptr_t base = align_downwards<CACHE_LINE_SIZE>(ptr);
    for (int k = 0; k < lines; ++k) {
        _mm_prefetch((void*)(base + k * CACHE_LINE_SIZE), _MM_HINT_T0);
    }
}

#endif // AMD64_VEC_COMMON_INCLUDED
