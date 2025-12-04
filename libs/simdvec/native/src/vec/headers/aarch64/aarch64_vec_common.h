#ifndef AARCH64_VEC_COMMON_INCLUDED
#define AARCH64_VEC_COMMON_INCLUDED

#include "vec_common.h"

#ifdef __APPLE__
    // Cache line size is 128 bytes on Apple M silicon
    // Source: sysctl -a hw machdep.cpu | grep hw.cachelinesize
    #define CACHE_LINE_SIZE 128
    #else
        #error "Unsupported Apple platform"
    #endif
#elif __linux__
    // We mostly care about ARMv8a like Neoverse N1 (e.g. Graviton 2) and V1 (e.g. Graviton 3), and ARMv9a
    // like Neoverse V2 (e.g. Graviton 4) architectures.
    // They all have cache lines of 64 bytes. See:
    // - https://developer.arm.com/documentation/100616/0401/L2-memory-system/About-the-L2-memory-system Graviton CPUs
    // - https://documentation-service.arm.com/static/66ace927882fec713ef4819f
    // - https://developer.arm.com/documentation/102375/latest
    #define CACHE_LINE_SIZE 64
#else
    #error "Unsupported aarch64 platform"
#endif

static inline void prefetch(const void* ptr, int lines) {
    const uintptr_t base = align_downwards<CACHE_LINE_SIZE>(ptr);
    for (int k = 0; k < lines; ++k) {
        __builtin_prefetch((void*)(base + k * CACHE_LINE_SIZE));
    }
}

#endif // AARCH64_VEC_COMMON_INCLUDED
