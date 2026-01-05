
#ifndef VEC_COMMON_H
#define VEC_COMMON_H

#include <stdint.h>
#include <assert.h>

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

template <int offset, int64_t(*mapper)(const int32_t, const int32_t*)>
static inline const int8_t* safe_mapper_offset(
    const int8_t* a,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count
) {
    return count > offset ? a + mapper(offset, offsets) * pitch : nullptr;
}

#endif // VEC_COMMON_H
