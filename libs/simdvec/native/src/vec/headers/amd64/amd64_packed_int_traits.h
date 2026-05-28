/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// Shared traits for AVX2/AVX-512 packed-int dot product kernels.
// Parameterises the kernels in amd64_packed_int_avx2.h and
// amd64_packed_int_avx512.h on the per-value bit width.
//
// The packed layout is: one byte holds (8 / BITS) consecutive values; the
// "unpacked" query has (8 / BITS) stripes of packed_len bytes each, where
// stripe K corresponds to the bits at offset shift(K) within each packed
// byte. The first stripe (K=0) is the most significant.

#ifndef AMD64_PACKED_INT_TRAITS_INCLUDED
#define AMD64_PACKED_INT_TRAITS_INCLUDED

#include <bit>
#include <cstdint>

template <int BITS> requires (BITS == 2 || BITS == 4)
struct packed_int_traits {
    static constexpr int bits = BITS;
    static constexpr int stripes = 8 / bits;
    static constexpr int mask = (1 << bits) - 1;

    // Stripe K is at bit position (stripes - 1 - K) * BITS within the byte;
    // stripe 0 is the most significant.
    static consteval int shift(int K) { return (stripes - 1 - K) * bits; }

    // The last stripe occupies the LSBs of the byte (shift==0) and so needs
    // no right shift before masking. Used with `if constexpr` at call sites
    // to elide the no-op shift at compile time.
    static consteval bool is_last_stripe(int K) { return K == stripes - 1; }

private:
    // _mm256_maddubs_epi16 sums two byte-products into each 16-bit lane;
    // each product is at most mask*mask, so the per-iteration per-lane max
    // is 2*mask*mask. Safe iteration limit before signed 16-bit overflow is
    // INT16_MAX / (2*mask*mask): 1820 for BITS=2, 72 for BITS=4.
    static constexpr unsigned max_per_iter_16bit = 2u * mask * mask;
    static constexpr unsigned safe_iters_16bit  = (unsigned)INT16_MAX / max_per_iter_16bit;

public:
    // Tier-1 (AVX2) only: largest power-of-two number of `stride`-sized inner
    // iterations safe to accumulate in 16-bit lanes before widening to 32-bit.
    // Evaluates to 1024 for BITS=2 and 64 for BITS=4, leaving >=50% headroom
    // against accumulator wrap.
    static constexpr int chunk_iters = (int)std::bit_floor(safe_iters_16bit);
};

#endif // AMD64_PACKED_INT_TRAITS_INCLUDED
