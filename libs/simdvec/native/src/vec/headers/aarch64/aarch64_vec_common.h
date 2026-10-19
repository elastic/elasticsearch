#ifndef AARCH64_VEC_COMMON_INCLUDED
#define AARCH64_VEC_COMMON_INCLUDED

#include "vec_common.h"

#ifdef __APPLE__
    // Cache line size is 128 bytes on Apple M silicon
    // Source: sysctl -a hw machdep.cpu | grep hw.cachelinesize
    #define CACHE_LINE_SIZE 128
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

// positions[b][r] is the position of the r-th (0-based) set bit of the byte value b
struct select_in_byte_table {
    uint8_t positions[256][8];

    constexpr select_in_byte_table() : positions() {
        for (int b = 0; b < 256; b++) {
            int r = 0;
            for (int p = 0; p < 8; p++) {
                if (b & (1 << p)) {
                    positions[b][r++] = (uint8_t)p;
                }
            }
        }
    }
};

static constexpr select_in_byte_table SELECT_IN_BYTE{};

// NEON has no bit-deposit instruction (x86 pdep), so this first finds the byte that holds the
// k-th set bit, then the bit within that byte.
// Broadword select after Lucene's BitUtil.select (LUCENE-6040, apache/lucene#7102), reduced to 32
// bits: S. Vigna, "Broadword Implementation of Rank/Select Queries", WEA 2008, with the lookups of
// S. Gog and M. Petri, "Optimized Succinct Data Structures for Massive Data", Softw. Pract. Exper., 2014.
static inline int32_t mask_select(const uint32_t mask, const int32_t k) {
    // 1. Per-byte popcounts: the first part of packed SIMD popcount, stopped before the bytes
    // are summed. Byte j of `counts` is the number of set bits in byte j of `mask`.
    uint32_t counts = mask - ((mask >> 1) & 0x55555555);
    counts = (counts & 0x33333333) + ((counts >> 2) & 0x33333333);
    counts = (counts + (counts >> 4)) & 0x0F0F0F0F;
    // 2. Prefix sums: multiplying by 0x01010101 adds each byte into every byte above it, so
    // byte j of `prefix` is the number of set bits in bytes 0..j (at most 32: no overflow).
    const uint32_t prefix = counts * 0x01010101;
    // 3. Find the byte that holds the k-th set bit: byte t, the first byte of `prefix` whose
    // value is > k.
    // All four bytes are compared at once: in each byte j, prefix[j] + (0x7F - k) has its high
    // bit set exactly when prefix[j] > k. The sum is below 0x100, so no byte carries into its
    // neighbour.
    // The k-th set bit exists (k < popcount(mask)), so at least the top byte has its high bit
    // set; the lowest high bit set is bit 8 * t + 7, found by counting the trailing zeros.
    const uint32_t gt = (prefix + (uint32_t)(0x7F - k) * 0x01010101) & 0x80808080;
    const int32_t t = __builtin_ctz(gt) >> 3;
    // 4. The rank of the bit within byte t is k minus the set bits in the bytes below it, which is
    // byte (t - 1) of `prefix`: shifting `prefix` up by one byte first makes that byte t, and makes
    // it 0 when t is 0. The table maps (byte value, rank) to the bit position.
    const int32_t below = (int32_t)(((prefix << 8) >> (8 * t)) & 0xFF);
    return 8 * t + SELECT_IN_BYTE.positions[(mask >> (8 * t)) & 0xFF][k - below];
}

#endif // AARCH64_VEC_COMMON_INCLUDED
