/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

 // This file contains implementations for BBQ vector operations,
 // including support for "1st tier" vector capabilities; in the case of ARM,
 // this first tier include functions for processors supporting at least the NEON
 // instruction set.

#include <stddef.h>
#include <arm_neon.h>
#include <algorithm>
#include "vec.h"
#include "vec_common.h"
#include "aarch64/aarch64_vec_common.h"

static inline int32_t reduce_u8x16_neon(uint8x16_t vec) {
    // Split the vector into two halves and widen to `uint16x8_t`
    uint16x8_t low_half = vmovl_u8(vget_low_u8(vec));   // widen lower 8 elements
    uint16x8_t high_half = vmovl_u8(vget_high_u8(vec)); // widen upper 8 elements

    // Sum the widened halves
    uint16x8_t sum16 = vaddq_u16(low_half, high_half);

    // Now reduce the `uint16x8_t` to a single `simsimd_u32_t`
    uint32x4_t sum32 = vpaddlq_u16(sum16);       // pairwise add into 32-bit integers
    uint64x2_t sum64 = vpaddlq_u32(sum32);       // pairwise add into 64-bit integers
    int32_t final_sum = vaddvq_u64(sum64);       // final horizontal add to 32-bit result
    return final_sum;
}

static inline int64_t dotd1q4_inner(const int8_t* a, const int8_t* q, const int32_t length) {
    constexpr int query_bits = 4;

    int64_t bit_result[query_bits] = {};

    const uint8_t* query[query_bits];
    apply_indexed<query_bits>([&](auto I) {
        query[I] = (const uint8_t*)q + I * length;
    });

    int r = 0;
    constexpr int chunk_size = sizeof(uint64x2_t);
    if (length >= chunk_size) {
        int iters = length / chunk_size;
        uint8x16_t zero = vcombine_u8(vcreate_u8(0), vcreate_u8(0));

        for (int j = 0; j < iters;) {
            uint8x16_t bit_sum[query_bits];
            apply_indexed<query_bits>([&](auto I) {
                bit_sum[I] = zero;
            });

            /*
            * After every 31 iterations we need to add the
            * bit sums to the total sum.
            * We must ensure that the temporary sums <= 255
            * and 31 * 8 bits = 248 which is OK.
            */
            uint64_t limit = std::min(j + 31, iters);
            for (; j < limit; j++, r += chunk_size)  {
                uint8x16_t qv[query_bits];
                apply_indexed<query_bits>([&](auto I) {
                    qv[I] = vld1q_u8(query[I] + r);
                });
                const uint8x16_t yv = vld1q_u8((const uint8_t*)a + r);

                apply_indexed<query_bits>([&](auto I) {
                    bit_sum[I] = vaddq_u8(bit_sum[I], vcntq_u8(vandq_u8(qv[I], yv)));
                });
            }

            apply_indexed<query_bits>([&](auto I) {
                bit_result[I] += reduce_u8x16_neon(bit_sum[I]);
            });
        }
    }

    // switch to single 64-bit ops
    int upperBound = length & ~(sizeof(int64_t) - 1);
    for (; r < upperBound; r += sizeof(int64_t)) {
        int64_t value = *((int64_t*)(a + r));
        apply_indexed<query_bits>([&](auto I) {
            int64_t bits = *((int64_t*)(q + r + I * length));
            bit_result[I] += __builtin_popcountll(bits & value);
        });
    }

    // then 32-bit ops
    upperBound = length & ~(sizeof(int32_t) - 1);
    for (; r < upperBound; r += sizeof(int32_t)) {
        int32_t value = *((int32_t*)(a + r));
        apply_indexed<query_bits>([&](auto I) {
            int32_t bits = *((int32_t*)(q + r + I * length));
            bit_result[I] += __builtin_popcount(bits & value);
        });
    }

    // then single bytes
    for (; r < length; r++) {
        int8_t value = *(a + r);
        apply_indexed<query_bits>([&](auto I) {
            int32_t bits = *(q + r + I * length);
            bit_result[I] += __builtin_popcount(bits & value & 0xFF);
        });
    }
    int64_t sum = 0;
    apply_indexed<query_bits>([&](auto I) {
        sum += (bit_result[I] << I);
    });
    return sum;
}

EXPORT int64_t vec_dotd1q4(const int8_t* a, const int8_t* query, const int32_t length) {
    return dotd1q4_inner(a, query, length);
}

template <const int8_t*(*mapper)(const int8_t*, const int32_t, const int32_t*, const int32_t)>
static inline void dotd1q4_inner_bulk(
    const int8_t* a,
    const int8_t* q,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    constexpr int batches = 2;
    constexpr int query_bits = 4;
    constexpr int chunk_size = sizeof(uint64x2_t);

    const uint8_t* query[query_bits];
    apply_indexed<query_bits>([&](auto I) {
        query[I] = (const uint8_t*)q + I * length;
    });

    const int iters = length / chunk_size;
    const uint8x16_t zero = vcombine_u8(vcreate_u8(0), vcreate_u8(0));

    int c = 0;

    for (; c + batches < count; c += batches) {
        const uint8_t* as[batches];
        apply_indexed<batches>([&](auto I) {
            as[I] = (const uint8_t*)mapper(a, c + I, offsets, pitch);
        });

        int64_t bit_result[batches * query_bits] = {};

        int r = 0;

        if (length >= chunk_size) {
            for (int j = 0; j < iters;) {
                uint8x16_t bit_sum[batches * query_bits];
                apply_indexed<batches * query_bits>([&](auto B) {
                    bit_sum[B] = zero;
                });

                /*
                * After every 31 iterations we need to add the
                * bit_sum to the total sum.
                * We must ensure that the temporary sums <= 255
                * and 31 * 8 bits = 248 which is OK.
                */
                uint64_t limit = std::min(j + 31, iters);
                for (; j < limit; j++, r+= chunk_size)  {
                    uint8x16_t qv[query_bits];
                    apply_indexed<query_bits>([&](auto I) {
                        qv[I] = vld1q_u8(query[I] + r);
                    });

                    uint8x16_t yv[batches];
                    apply_indexed<batches>([&](auto I) {
                        yv[I] = vld1q_u8(as[I] + r);
                    });

                    apply_indexed<batches>([&](auto B) {
                        apply_indexed<query_bits>([&](auto Q) {
                            constexpr int idx = B * query_bits + Q;
                            bit_sum[idx] = vaddq_u8(bit_sum[idx], vcntq_u8(vandq_u8(qv[Q], yv[B])));
                        });
                    });
                }

                apply_indexed<batches * query_bits>([&](auto I) {
                    bit_result[I] += reduce_u8x16_neon(bit_sum[I]);
                });
            }
        }

        // complete using byte ops
        for (; r < length; r++) {
            int64_t vs[batches];
            apply_indexed<batches>([&](auto I) {
                vs[I] = *((int64_t*)(as[I] + r));
            });

            int64_t qs[query_bits];
            apply_indexed<query_bits>([&](auto I) {
                qs[I] = *((int64_t*)(query[I] + r));
            });

            apply_indexed<batches>([&](auto B) {
                apply_indexed<query_bits>([&](auto Q) {
                    bit_result[B * query_bits + Q] += __builtin_popcount(qs[Q] & vs[B] & 0xFF);
                });
            });
        }
        apply_indexed<batches>([&](auto B) {
            int32_t res = 0;
            apply_indexed<query_bits>([&](auto Q) {
                res += (bit_result[B * query_bits + Q] << Q);
            });
            results[c + B] = (f32_t)res;
        });
    }

    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = (f32_t)dotd1q4_inner(a0, q, length);
    }
}

EXPORT void vec_dotd1q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1q4_inner_bulk<sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd1q4_bulk_offsets(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dotd1q4_inner_bulk<offsets_mapper>(a, query, length, pitch, offsets, count, results);
}

EXPORT int64_t vec_dotd2q4(
    const int8_t* a,
    const int8_t* query,
    const int32_t length
) {
    int64_t lower = dotd1q4_inner(a, query, length/2);
    int64_t upper = dotd1q4_inner(a + length/2, query, length/2);
    return lower + (upper << 1);
}

template <const int8_t*(*mapper)(const int8_t*, const int32_t, const int32_t*, const int32_t)>
static inline void dotd2q4_inner_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    int c = 0;
    const int bit_length = length/2;
    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        int64_t lower = dotd1q4_inner(a0, query, bit_length);
        int64_t upper = dotd1q4_inner(a0 + bit_length, query, bit_length);
        results[c] = (f32_t)(lower + (upper << 1));
    }
}

EXPORT void vec_dotd2q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd2q4_bulk_offsets(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<offsets_mapper>(a, query, length, pitch, offsets, count, results);
}

EXPORT int64_t vec_dotd4q4(const int8_t* a, const int8_t* query, const int32_t length) {
    const int32_t bit_length = length / 4;
    int64_t p0 = dotd1q4_inner(a + 0 * bit_length, query, bit_length);
    int64_t p1 = dotd1q4_inner(a + 1 * bit_length, query, bit_length);
    int64_t p2 = dotd1q4_inner(a + 2 * bit_length, query, bit_length);
    int64_t p3 = dotd1q4_inner(a + 3 * bit_length, query, bit_length);
    return p0 + (p1 << 1) + (p2 << 2) + (p3 << 3);
}

template <const int8_t*(*mapper)(const int8_t*, const int32_t, const int32_t*, const int32_t)>
static inline void dotd4q4_inner_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    const int32_t bit_length = length / 4;

    for (int c = 0; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);

        int64_t p0 = dotd1q4_inner(a0 + 0 * bit_length, query, bit_length);
        int64_t p1 = dotd1q4_inner(a0 + 1 * bit_length, query, bit_length);
        int64_t p2 = dotd1q4_inner(a0 + 2 * bit_length, query, bit_length);
        int64_t p3 = dotd1q4_inner(a0 + 3 * bit_length, query, bit_length);

        results[c] = (f32_t)(p0 + (p1 << 1) + (p2 << 2) + (p3 << 3));
    }
}

EXPORT void vec_dotd4q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd4q4_bulk_offsets(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<offsets_mapper>(a, query, length, pitch, offsets, count, results);
}
