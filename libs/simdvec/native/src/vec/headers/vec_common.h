
#ifndef VEC_COMMON_H
#define VEC_COMMON_H

#include <stdint.h>
#include <assert.h>
#include <type_traits>

template <uintptr_t align>
static inline uintptr_t align_downwards(const void* ptr) {
    static_assert(align > 0 && (align & (align - 1)) == 0, "Align must be a power of 2");
    assert(ptr != 0);

    uintptr_t addr = (uintptr_t)ptr;
    // Round down to align-byte boundary
    addr &= -align;
    assert(addr <= (uintptr_t)ptr);
    return addr;
}

static inline int64_t identity_mapper(const int32_t i, const int32_t* offsets) {
   return i;
}

static inline int64_t array_mapper(const int32_t i, const int32_t* offsets) {
   return offsets[i];
}

template <typename T, int offset, int64_t(*mapper)(const int32_t, const int32_t*)>
static inline const T* safe_mapper_offset(
    const T* a,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count
) {
    return count > offset ? a + mapper(offset, offsets) * pitch : nullptr;
}

// Populates out[0..N-1] with safe_mapper_offset<T, 0..N-1, mapper>(...),
// using recursive template instantiation to supply each array index as a
// compile-time constant (required by safe_mapper_offset's template parameter).
template <int I, int N, typename T, int64_t(*mapper)(const int32_t, const int32_t*)>
static inline void init_offsets(const T** out, const T* a, int32_t pitch,
                                const int32_t* offsets, int32_t count) {
    if constexpr (I < N) {
        out[I] = safe_mapper_offset<T, I, mapper>(a, pitch, offsets, count);
        init_offsets<I + 1, N, T, mapper>(out, a, pitch, offsets, count);
    }
}

// Compile-time loop: calls f(integral_constant<int, 0>{}), f(...<1>{}), ...,
// f(...<N-1>{}). Each index is a compile-time constant, so it can be used as
// a template argument or array index that the compiler fully unrolls.
template <int N, typename F, int I = 0>
static inline void apply_indexed(F&& f) {
    if constexpr (I < N) {
        f(std::integral_constant<int, I>{});
        apply_indexed<N, F, I + 1>(std::forward<F>(f));
    }
}

#endif // VEC_COMMON_H
