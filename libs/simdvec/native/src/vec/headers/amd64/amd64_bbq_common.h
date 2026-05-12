/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

#ifndef AMD64_BBQ_COMMON_INCLUDED
#define AMD64_BBQ_COMMON_INCLUDED

#include <stdint.h>
#include <algorithm>
#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"

template<
    typename TData,
    const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int64_t (*dotd1q4_op)(const int8_t* a, const int8_t* q, const int32_t length)
>
static inline void dotd1q4_inner_bulk(
    const TData* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    constexpr int batches = 4;

    const int lines_to_fetch = length / CACHE_LINE_SIZE + 1;
    int c = 0;

    const int8_t* current_vecs[batches];
    init_pointers<batches, TData, int8_t, mapper>(current_vecs, a, pitch, offsets, 0, count);

    // Process a batch of 4 vectors at a time, after instructing the CPU to
    // prefetch the next batch.
    // Prefetching multiple memory locations while computing keeps the CPU
    // execution units busy.
    for (; c + batches - 1 < count; c += batches) {
        const int8_t* next_vecs[batches];
        const bool has_next = c + 2 * batches - 1 < count;
        if (has_next) {
            apply_indexed<batches>([&](auto I) {
                next_vecs[I] = mapper(a, c + batches + I, offsets, pitch);
                prefetch(next_vecs[I], lines_to_fetch);
            });
        }

        apply_indexed<batches>([&](auto I) {
            results[c + I] = (f32_t)dotd1q4_op(current_vecs[I], query, length);
        });

        if (has_next) {
            std::copy_n(next_vecs, batches, current_vecs);
        }
    }

    // Tail-handling: remaining vectors
    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = (f32_t)dotd1q4_op(a0, query, length);
    }
}

template <
    typename TData,
    const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int64_t (*dotd1q4_op)(const int8_t* a, const int8_t* q, const int32_t length)
>
static inline void dotd2q4_inner_bulk(
    const TData* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    constexpr int batches = 2;

    const int lines_to_fetch = length / CACHE_LINE_SIZE + 1;
    const int bit_length = length/2;
    int c = 0;

    const int8_t* current_vecs[batches];
    init_pointers<batches, TData, int8_t, mapper>(current_vecs, a, pitch, offsets, 0, count);

    // Process 2 vectors at a time, after instructing the CPU to
    // prefetch the next vectors (both stripes).
    for (; c + batches - 1 < count; c += batches) {
        const int8_t* next_vecs[batches];
        const bool has_next = c + 2 * batches - 1 < count;
        if (has_next) {
            apply_indexed<batches>([&](auto I) {
                next_vecs[I] = mapper(a, c + batches + I, offsets, pitch);
                prefetch(next_vecs[I], lines_to_fetch);
                prefetch(next_vecs[I] + bit_length, lines_to_fetch);
            });
        }

        apply_indexed<batches>([&](auto I) {
            results[c + I] = (f32_t)(
                dotd1q4_op(current_vecs[I], query, bit_length)
                + (dotd1q4_op(current_vecs[I] + bit_length, query, bit_length) << 1));
        });

        if (has_next) {
            std::copy_n(next_vecs, batches, current_vecs);
        }
    }

    // Tail-handling: remaining vectors
    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        int64_t lower = dotd1q4_op(a0, query, bit_length);
        int64_t upper = dotd1q4_op(a0 + bit_length, query, bit_length);
        results[c] = (f32_t)(lower + (upper << 1));
    }
}

template <
    typename TData,
    const int8_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int64_t (*dotd1q4_op)(const int8_t* a, const int8_t* q, const int32_t length)
>
static inline void dotd4q4_inner_bulk(
    const TData* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    const int lines_to_fetch = length / CACHE_LINE_SIZE + 1;
    const int32_t bit_length = length / 4;
    int c = 0;

    const int8_t* a0 = count > 0 ? mapper(a, 0, offsets, pitch) : nullptr;

    // Process one vector, after instructing the CPU to prefetch the next vector
    for (; c + 1 < count; c++) {
        const int8_t* next_a0 = mapper(a, c + 1, offsets, pitch);

        // prefetch stripes 2 and 3 now
        prefetch(a0 + 2 * bit_length, lines_to_fetch);
        prefetch(a0 + 3 * bit_length, lines_to_fetch);

        int64_t p0 = dotd1q4_op(a0, query, bit_length);
        int64_t p1 = dotd1q4_op(a0 + bit_length, query, bit_length);

        // and 0 and 1 of the next vector
        prefetch(next_a0, lines_to_fetch);
        prefetch(next_a0 + bit_length, lines_to_fetch);

        int64_t p2 = dotd1q4_op(a0 + 2 * bit_length, query, bit_length);
        int64_t p3 = dotd1q4_op(a0 + 3 * bit_length, query, bit_length);

        results[c] = (f32_t)(p0 + (p1 << 1) + (p2 << 2) + (p3 << 3));

        a0 = next_a0;
    }

    // Tail-handling: remaining vector
    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);

        int64_t p0 = dotd1q4_op(a0 + 0 * bit_length, query, bit_length);
        int64_t p1 = dotd1q4_op(a0 + 1 * bit_length, query, bit_length);
        int64_t p2 = dotd1q4_op(a0 + 2 * bit_length, query, bit_length);
        int64_t p3 = dotd1q4_op(a0 + 3 * bit_length, query, bit_length);

        results[c] = (f32_t)(p0 + (p1 << 1) + (p2 << 2) + (p3 << 3));
    }
}

#endif // AMD64_BBQ_COMMON_INCLUDED
