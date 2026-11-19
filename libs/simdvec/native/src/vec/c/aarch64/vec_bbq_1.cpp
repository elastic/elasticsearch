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
#include "vec.h"
#include "vec_common.h"
#include "aarch64/aarch64_vec_common.h"

// Counts the bits of a & q per byte and folds the counts into acc with UDOT against a constant weight
// vector: one instruction applies the query plane's bit value and sums four bytes into each 32-bit lane, so
// no 8-bit partial sums and no periodic reduction are needed. Per lane a step adds at most
// 4 bytes * 8 bits * weight, far from a u32 overflow.
static inline uint32x4_t dot_bit_weighted_neon(const uint32x4_t acc, const uint8x16_t a, const uint8x16_t q, const uint8x16_t weight) {
    return vdotq_u32(acc, vcntq_u8(vandq_u8(a, q)), weight);
}

template<int query_bits>
static inline int64_t dotd1qN_inner(const int8_t* a, const int8_t* q, const int32_t length) {
    int64_t bit_result[query_bits] = {};
    int64_t sum = 0;

    const uint8_t* query[query_bits];
    apply_indexed<query_bits>([&](auto I) {
        query[I] = (const uint8_t*)q + I * length;
    });

    int r = 0;
    constexpr int chunk_size = sizeof(uint8x16_t);
    if (length >= chunk_size) {
        // one accumulator per query plane keeps the UDOT chains independent
        uint32x4_t acc[query_bits];
        uint8x16_t weight[query_bits];
        apply_indexed<query_bits>([&](auto I) {
            acc[I] = vdupq_n_u32(0);
            weight[I] = vdupq_n_u8(1 << I);
        });

        for (; r + chunk_size <= length; r += chunk_size) {
            const uint8x16_t yv = vld1q_u8((const uint8_t*)a + r);
            apply_indexed<query_bits>([&](auto I) {
                acc[I] = dot_bit_weighted_neon(acc[I], yv, vld1q_u8(query[I] + r), weight[I]);
            });
        }

        apply_indexed<query_bits>([&](auto I) {
            sum += vaddvq_u32(acc[I]);
        });
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
    apply_indexed<query_bits>([&](auto I) {
        sum += (bit_result[I] << I);
    });
    return sum;
}

EXPORT int64_t vec_dotd1q4(const int8_t* a, const int8_t* query, const int32_t length) {
    return dotd1qN_inner<4>(a, query, length);
}

EXPORT int64_t vec_dotd1q1(const int8_t* a, const int8_t* query, const int32_t length) {
    return dotd1qN_inner<1>(a, query, length);
}

template <
    typename TData,
    int query_bits,
    const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int32_t(*out_mapper)(const int32_t*, const int32_t) = compact_output>
static inline void dotd1qN_inner_bulk(
    const TData* a,
    const int8_t* q,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    // four vectors per batch keep twice as many cache misses in flight as two; two query planes share one
    // accumulator, UDOT folds the plane weight in, so the accumulators, the query planes, the weights and the
    // loaded vectors of a batch fit in the 32 NEON registers
    constexpr int batches = 4;
    constexpr int chunk_size = sizeof(uint8x16_t);
    constexpr int accs = (query_bits + 1) / 2;

    const uint8_t* query[query_bits];
    uint8x16_t weight[query_bits];
    apply_indexed<query_bits>([&](auto I) {
        query[I] = (const uint8_t*)q + I * length;
        weight[I] = vdupq_n_u8(1 << I);
    });

    const int blk = length & ~(chunk_size - 1);

    int c = 0;

    for (; c + batches - 1 < count; c += batches) {
        const uint8_t* as[batches];
        apply_indexed<batches>([&](auto I) {
            as[I] = (const uint8_t*)mapper(a, c + I, offsets, pitch);
        });

        uint32x4_t acc[batches * accs];
        apply_indexed<batches * accs>([&](auto I) {
            acc[I] = vdupq_n_u32(0);
        });

        for (int r = 0; r < blk; r += chunk_size) {
            uint8x16_t qv[query_bits];
            apply_indexed<query_bits>([&](auto I) {
                qv[I] = vld1q_u8(query[I] + r);
            });

            apply_indexed<batches>([&](auto B) {
                const uint8x16_t yv = vld1q_u8(as[B] + r);
                apply_indexed<query_bits>([&](auto Q) {
                    constexpr int idx = B * accs + Q / 2;
                    acc[idx] = dot_bit_weighted_neon(acc[idx], yv, qv[Q], weight[Q]);
                });
            });
        }

        int64_t res[batches];
        apply_indexed<batches>([&](auto B) {
            res[B] = 0;
            apply_indexed<accs>([&](auto K) {
                res[B] += vaddvq_u32(acc[B * accs + K]);
            });
        });

        // Byte tail. Single-byte loads only: with sparse addresses a vector can
        // end right before an unmapped page, so wider loads could fault.
        for (int r = blk; r < length; r++) {
            uint8_t vs[batches];
            apply_indexed<batches>([&](auto I) {
                vs[I] = as[I][r];
            });

            uint8_t qs[query_bits];
            apply_indexed<query_bits>([&](auto I) {
                qs[I] = query[I][r];
            });

            apply_indexed<batches>([&](auto B) {
                apply_indexed<query_bits>([&](auto Q) {
                    res[B] += __builtin_popcount(qs[Q] & vs[B]) << Q;
                });
            });
        }
        apply_indexed<batches>([&](auto B) {
            results[out_mapper(offsets, c + B)] = (f32_t)res[B];
        });
    }

    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        results[out_mapper(offsets, c)] = (f32_t)dotd1qN_inner<query_bits>(a0, q, length);
    }
}

EXPORT void vec_dotd1q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1qN_inner_bulk<int8_t, 4, sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd1q4_bulk_masked(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t mask,
    const int32_t count,
    f32_t* results) {
    const int32_t block_mask = clamp_mask(mask, count);
    dotd1qN_inner_bulk<int8_t, 4, masked_mapper, masked_output>(
        a, query, length, pitch, &block_mask, __builtin_popcount((uint32_t)block_mask), results);
}

EXPORT void vec_dotd1q4_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1qN_inner_bulk<const int8_t*, 4, sparse_mapper>((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT void vec_dotd1q1_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1qN_inner_bulk<int8_t, 1, sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd1q1_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1qN_inner_bulk<const int8_t*, 1, sparse_mapper>((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT void vec_dotd1q1_bulk_masked(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t mask,
    const int32_t count,
    f32_t* results) {
    const int32_t block_mask = clamp_mask(mask, count);
    dotd1qN_inner_bulk<int8_t, 1, masked_mapper, masked_output>(
        a, query, length, pitch, &block_mask, __builtin_popcount((uint32_t)block_mask), results);
}

EXPORT int64_t vec_dotd2q2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length
) {
    int64_t lower = dotd1qN_inner<2>(a, query, length/2);
    int64_t upper = dotd1qN_inner<2>(a + length/2, query, length/2);
    return lower + (upper << 1);
}

template <
    typename TData,
    const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int32_t(*out_mapper)(const int32_t*, const int32_t) = compact_output>
static inline void dotd2q2_inner_bulk(
    const TData* a,
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
        int64_t lower = dotd1qN_inner<2>(a0, query, bit_length);
        int64_t upper = dotd1qN_inner<2>(a0 + bit_length, query, bit_length);
        results[out_mapper(offsets, c)] = (f32_t)(lower + (upper << 1));
    }
}

EXPORT void vec_dotd2q2_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q2_inner_bulk<int8_t, sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd2q2_bulk_masked(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t mask,
    const int32_t count,
    f32_t* results) {
    const int32_t block_mask = clamp_mask(mask, count);
    dotd2q2_inner_bulk<int8_t, masked_mapper, masked_output>(
        a, query, length, pitch, &block_mask, __builtin_popcount((uint32_t)block_mask), results);
}

EXPORT void vec_dotd2q2_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q2_inner_bulk<const int8_t*, sparse_mapper>((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

// 2-bit doc x 4-bit query, both doc bit planes in one pass. The doc is two contiguous planes of
// length/2 bytes (low bit, then high bit), the query four planes of length/2 bytes. UDOT applies
// 2^(query bit + doc bit) as a weight, so the eight plane pairs share two accumulators and one reduction.
static inline int64_t dotd2q4_inner(const int8_t* a, const int8_t* q, const int32_t length) {
    constexpr int query_bits = 4;
    const int32_t bit_length = length / 2;
    const int8_t* a_hi = a + bit_length;
    int64_t sum = 0;

    const uint8_t* query[query_bits];
    apply_indexed<query_bits>([&](auto I) {
        query[I] = (const uint8_t*)q + I * bit_length;
    });

    int r = 0;
    constexpr int chunk_size = sizeof(uint8x16_t);
    if (bit_length >= chunk_size) {
        uint32x4_t acc_lo = vdupq_n_u32(0);
        uint32x4_t acc_hi = vdupq_n_u32(0);
        // 2^(query bit + doc bit): the low doc plane uses weight[0..3], the high plane weight[1..4]
        uint8x16_t weight[query_bits + 1];
        apply_indexed<query_bits + 1>([&](auto I) {
            weight[I] = vdupq_n_u8(1 << I);
        });

        for (; r + chunk_size <= bit_length; r += chunk_size) {
            const uint8x16_t lo = vld1q_u8((const uint8_t*)a + r);
            const uint8x16_t hi = vld1q_u8((const uint8_t*)a_hi + r);
            apply_indexed<query_bits>([&](auto I) {
                const uint8x16_t qv = vld1q_u8(query[I] + r);
                acc_lo = dot_bit_weighted_neon(acc_lo, lo, qv, weight[I]);
                acc_hi = dot_bit_weighted_neon(acc_hi, hi, qv, weight[I + 1]);
            });
        }
        sum += vaddvq_u32(acc_lo) + (int64_t)vaddvq_u32(acc_hi);
    }

    // switch to single 64-bit ops
    int upperBound = bit_length & ~(sizeof(int64_t) - 1);
    for (; r < upperBound; r += sizeof(int64_t)) {
        const int64_t value_lo = *((int64_t*)(a + r));
        const int64_t value_hi = *((int64_t*)(a_hi + r));
        apply_indexed<query_bits>([&](auto I) {
            const int64_t bits = *((int64_t*)(query[I] + r));
            sum += (int64_t)(__builtin_popcountll(bits & value_lo) + (__builtin_popcountll(bits & value_hi) << 1)) << I;
        });
    }

    // then single bytes
    for (; r < bit_length; r++) {
        const uint8_t value_lo = a[r];
        const uint8_t value_hi = a_hi[r];
        apply_indexed<query_bits>([&](auto I) {
            const uint8_t bits = query[I][r];
            sum += (int64_t)(__builtin_popcount(bits & value_lo) + (__builtin_popcount(bits & value_hi) << 1)) << I;
        });
    }
    return sum;
}

EXPORT int64_t vec_dotd2q4(
    const int8_t* a,
    const int8_t* query,
    const int32_t length
) {
    return dotd2q4_inner(a, query, length);
}

template <
    typename TData,
    const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int32_t(*out_mapper)(const int32_t*, const int32_t) = compact_output>
static inline void dotd2q4_inner_bulk(
    const TData* a,
    const int8_t* q,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    // Four vectors per batch, each with one accumulator per doc bit plane so the UDOT chains stay
    // independent; the accumulators, the query planes, the weights and the loaded planes of a batch fit
    // in the 32 NEON registers.
    constexpr int batches = 4;
    constexpr int query_bits = 4;
    constexpr int chunk_size = sizeof(uint8x16_t);
    constexpr int word_size = sizeof(uint64_t);
    const int32_t bit_length = length / 2;

    const uint8_t* query[query_bits];
    apply_indexed<query_bits>([&](auto I) {
        query[I] = (const uint8_t*)q + I * bit_length;
    });
    // 2^(query bit + doc bit): the low doc plane uses weight[0..3], the high plane weight[1..4]
    uint8x16_t weight[query_bits + 1];
    apply_indexed<query_bits + 1>([&](auto I) {
        weight[I] = vdupq_n_u8(1 << I);
    });

    const int blk = bit_length & ~(chunk_size - 1);

    int c = 0;

    for (; c + batches - 1 < count; c += batches) {
        const uint8_t* as[batches];
        apply_indexed<batches>([&](auto I) {
            as[I] = (const uint8_t*)mapper(a, c + I, offsets, pitch);
        });

        uint32x4_t acc_lo[batches];
        uint32x4_t acc_hi[batches];
        apply_indexed<batches>([&](auto I) {
            acc_lo[I] = vdupq_n_u32(0);
            acc_hi[I] = vdupq_n_u32(0);
        });

        for (int r = 0; r < blk; r += chunk_size) {
            uint8x16_t qv[query_bits];
            apply_indexed<query_bits>([&](auto I) {
                qv[I] = vld1q_u8(query[I] + r);
            });

            apply_indexed<batches>([&](auto B) {
                const uint8x16_t lo = vld1q_u8(as[B] + r);
                const uint8x16_t hi = vld1q_u8(as[B] + bit_length + r);
                apply_indexed<query_bits>([&](auto Q) {
                    acc_lo[B] = dot_bit_weighted_neon(acc_lo[B], lo, qv[Q], weight[Q]);
                    acc_hi[B] = dot_bit_weighted_neon(acc_hi[B], hi, qv[Q], weight[Q + 1]);
                });
            });
        }

        int64_t res[batches];
        apply_indexed<batches>([&](auto B) {
            res[B] = vaddvq_u32(acc_lo[B]) + (int64_t)vaddvq_u32(acc_hi[B]);
        });

        // Tail of fewer than 16 bytes: at most one 8-byte step, then single bytes. Every load stays within
        // its plane, so none reads past the end of a vector (with sparse addresses, a vector can end right
        // before an unmapped page).
        int r = blk;
        if (bit_length - r >= word_size) {
            uint64_t qs[query_bits];
            apply_indexed<query_bits>([&](auto I) {
                qs[I] = *((const uint64_t*)(query[I] + r));
            });

            apply_indexed<batches>([&](auto B) {
                const uint64_t lo = *((const uint64_t*)(as[B] + r));
                const uint64_t hi = *((const uint64_t*)(as[B] + bit_length + r));
                apply_indexed<query_bits>([&](auto Q) {
                    res[B] += (int64_t)(__builtin_popcountll(qs[Q] & lo) + (__builtin_popcountll(qs[Q] & hi) << 1)) << Q;
                });
            });
            r += word_size;
        }
        for (; r < bit_length; r++) {
            uint8_t qs[query_bits];
            apply_indexed<query_bits>([&](auto I) {
                qs[I] = query[I][r];
            });

            apply_indexed<batches>([&](auto B) {
                const uint8_t lo = as[B][r];
                const uint8_t hi = as[B][bit_length + r];
                apply_indexed<query_bits>([&](auto Q) {
                    res[B] += (__builtin_popcount(qs[Q] & lo) + (__builtin_popcount(qs[Q] & hi) << 1)) << Q;
                });
            });
        }
        apply_indexed<batches>([&](auto B) {
            results[out_mapper(offsets, c + B)] = (f32_t)res[B];
        });
    }

    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        results[out_mapper(offsets, c)] = (f32_t)dotd2q4_inner(a0, q, length);
    }
}

EXPORT void vec_dotd2q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<int8_t, sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd2q4_bulk_masked(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t mask,
    const int32_t count,
    f32_t* results) {
    const int32_t block_mask = clamp_mask(mask, count);
    dotd2q4_inner_bulk<int8_t, masked_mapper, masked_output>(
        a, query, length, pitch, &block_mask, __builtin_popcount((uint32_t)block_mask), results);
}

EXPORT void vec_dotd2q4_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd2q4_inner_bulk<const int8_t*, sparse_mapper>((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}

// 4-bit doc x 4-bit query in one pass. The doc is four contiguous bit planes of length/4 bytes, the
// query four planes of length/4 bytes. Each doc plane has its own accumulator, into which UDOT applies the
// query plane's bit value as a weight; the doc plane's bit value is applied after the reduction, so four
// weights cover the 16 plane pairs.
static inline int64_t dotd4q4_inner(const int8_t* a, const int8_t* q, const int32_t length) {
    constexpr int doc_bits = 4;
    constexpr int query_bits = 4;
    constexpr int u64_bytes = sizeof(uint64_t);
    constexpr int u32_bytes = sizeof(uint32_t);
    constexpr int u16_bytes = sizeof(uint16_t);
    constexpr int chunk_size = sizeof(uint8x16_t);
    const int32_t bit_length = length / 4;
    int64_t sum = 0;

    int r = 0;
    if (bit_length >= chunk_size) {
        uint32x4_t acc[doc_bits];
        uint8x16_t weight[query_bits];
        apply_indexed<doc_bits>([&](auto D) {
            acc[D] = vdupq_n_u32(0);
        });
        apply_indexed<query_bits>([&](auto Q) {
            weight[Q] = vdupq_n_u8(1 << Q);
        });

        for (; r + chunk_size <= bit_length; r += chunk_size) {
            uint8x16_t qv[query_bits];
            apply_indexed<query_bits>([&](auto Q) {
                qv[Q] = vld1q_u8((const uint8_t*)q + Q * bit_length + r);
            });
            apply_indexed<doc_bits>([&](auto D) {
                const uint8x16_t doc_plane = vld1q_u8((const uint8_t*)a + D * bit_length + r);
                apply_indexed<query_bits>([&](auto Q) {
                    acc[D] = dot_bit_weighted_neon(acc[D], doc_plane, qv[Q], weight[Q]);
                });
            });
        }
        apply_indexed<doc_bits>([&](auto D) {
            sum += (int64_t)vaddvq_u32(acc[D]) << D;
        });
    }

    // fewer than 16 bytes are left: at most one step each of 8, 4, 2 and 1 bytes
    if (bit_length - r >= u64_bytes) {
        dot_striped_scalar<doc_bits, query_bits, u64_bytes>(sum, a, q, bit_length, r);
        r += u64_bytes;
    }
    if (bit_length - r >= u32_bytes) {
        dot_striped_scalar<doc_bits, query_bits, u32_bytes>(sum, a, q, bit_length, r);
        r += u32_bytes;
    }
    if (bit_length - r >= u16_bytes) {
        dot_striped_scalar<doc_bits, query_bits, u16_bytes>(sum, a, q, bit_length, r);
        r += u16_bytes;
    }
    if (r < bit_length) {
        dot_striped_scalar<doc_bits, query_bits, 1>(sum, a, q, bit_length, r);
    }
    return sum;
}

EXPORT int64_t vec_dotd4q4(const int8_t* a, const int8_t* query, const int32_t length) {
    return dotd4q4_inner(a, query, length);
}

template <
    typename TData,
    const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int32_t(*out_mapper)(const int32_t*, const int32_t) = compact_output>
static inline void dotd4q4_inner_bulk(
    const TData* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    // Two vectors per batch, each with one accumulator per doc bit plane so the UDOT chains stay
    // independent; the doc planes are loaded and consumed one at a time. Four vectors would not leave
    // enough of the 32 NEON registers for the query planes, the weights and the temporaries.
    constexpr int batches = 2;
    constexpr int doc_bits = 4;
    constexpr int query_bits = 4;
    constexpr int u64_bytes = sizeof(uint64_t);
    constexpr int u32_bytes = sizeof(uint32_t);
    constexpr int u16_bytes = sizeof(uint16_t);
    constexpr int chunk_size = sizeof(uint8x16_t);
    const int32_t bit_length = length / 4;

    uint8x16_t weight[query_bits];
    apply_indexed<query_bits>([&](auto Q) {
        weight[Q] = vdupq_n_u8(1 << Q);
    });
    const int blk = bit_length & ~(chunk_size - 1);

    int c = 0;
    for (; c + batches - 1 < count; c += batches) {
        const uint8_t* as[batches];
        apply_indexed<batches>([&](auto B) {
            as[B] = (const uint8_t*)mapper(a, c + B, offsets, pitch);
        });

        uint32x4_t acc[batches][doc_bits];
        apply_indexed<batches>([&](auto B) {
            apply_indexed<doc_bits>([&](auto D) {
                acc[B][D] = vdupq_n_u32(0);
            });
        });

        for (int r = 0; r < blk; r += chunk_size) {
            uint8x16_t qv[query_bits];
            apply_indexed<query_bits>([&](auto Q) {
                qv[Q] = vld1q_u8((const uint8_t*)query + Q * bit_length + r);
            });
            apply_indexed<batches>([&](auto B) {
                apply_indexed<doc_bits>([&](auto D) {
                    const uint8x16_t doc_plane = vld1q_u8(as[B] + D * bit_length + r);
                    apply_indexed<query_bits>([&](auto Q) {
                        acc[B][D] = dot_bit_weighted_neon(acc[B][D], doc_plane, qv[Q], weight[Q]);
                    });
                });
            });
        }

        int64_t res[batches];
        apply_indexed<batches>([&](auto B) {
            res[B] = 0;
            apply_indexed<doc_bits>([&](auto D) {
                res[B] += (int64_t)vaddvq_u32(acc[B][D]) << D;
            });
        });

        // Tail of fewer than 16 bytes: at most one step each of 8, 4, 2 and 1 bytes. Every load stays within
        // its plane, so none reads past the end of a vector (with sparse addresses, a vector can end right
        // before an unmapped page).
        int r = blk;
        if (bit_length - r >= u64_bytes) {
            apply_indexed<batches>([&](auto B) {
                dot_striped_scalar<doc_bits, query_bits, u64_bytes>(res[B], (const int8_t*)as[B], query, bit_length, r);
            });
            r += u64_bytes;
        }
        if (bit_length - r >= u32_bytes) {
            apply_indexed<batches>([&](auto B) {
                dot_striped_scalar<doc_bits, query_bits, u32_bytes>(res[B], (const int8_t*)as[B], query, bit_length, r);
            });
            r += u32_bytes;
        }
        if (bit_length - r >= u16_bytes) {
            apply_indexed<batches>([&](auto B) {
                dot_striped_scalar<doc_bits, query_bits, u16_bytes>(res[B], (const int8_t*)as[B], query, bit_length, r);
            });
            r += u16_bytes;
        }
        if (r < bit_length) {
            apply_indexed<batches>([&](auto B) {
                dot_striped_scalar<doc_bits, query_bits, 1>(res[B], (const int8_t*)as[B], query, bit_length, r);
            });
        }
        apply_indexed<batches>([&](auto B) {
            results[out_mapper(offsets, c + B)] = (f32_t)res[B];
        });
    }

    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        results[out_mapper(offsets, c)] = (f32_t)dotd4q4_inner(a0, query, length);
    }
}

EXPORT void vec_dotd4q4_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<int8_t, sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd4q4_bulk_masked(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t mask,
    const int32_t count,
    f32_t* results
) {
    const int32_t block_mask = clamp_mask(mask, count);
    dotd4q4_inner_bulk<int8_t, masked_mapper, masked_output>(
        a, query, length, pitch, &block_mask, __builtin_popcount((uint32_t)block_mask), results);
}

EXPORT void vec_dotd4q4_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    dotd4q4_inner_bulk<const int8_t*, sparse_mapper>((const int8_t* const*)addresses, query, length, 0, NULL, count, results);
}
