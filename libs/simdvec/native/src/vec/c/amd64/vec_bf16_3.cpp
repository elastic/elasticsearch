/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// AVX-512 vectorized BF16 vector operations

#include <stddef.h>
#include <stdint.h>
#include <algorithm>

#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"

static inline __m512 bf16_to_f32(__m256i bf16) {
    __m512i widened = _mm512_cvtepu16_epi32(bf16);
    __m512i shifted = _mm512_slli_epi32(widened, 16);
    return _mm512_castsi512_ps(shifted);
}

static inline __m512 load_bf16(const bf16_t* ptr, const int elements) {
    return bf16_to_f32(_mm256_lddqu_si256((const __m256i*)(ptr + elements)));
}

static inline __m512 load_f32(const f32_t* ptr, const int elements) {
    return _mm512_loadu_ps(ptr + elements);
}

/*
 * Specialization for dot product of 2 vectors of BFloat16s using the dpbf16 instructions.
 */
static inline f32_t dotDbf16Qbf16_inner_avx512(const bf16_t* d, const bf16_t* q, int32_t elementCount) {
    constexpr int batches = 8;

    __m512 sums[batches];
    apply_indexed<batches>([&](auto I) {
        sums[I] = _mm512_setzero_ps();
    });

    int i = 0;
    constexpr int elements = sizeof(__m512bh) / sizeof(bf16_t);
    constexpr int stride512 = sizeof(__m512bh) / sizeof(bf16_t) * batches;
    for (; i < (elementCount & ~(stride512 - 1)); i += stride512) {
        apply_indexed<batches>([&](auto I) {
            sums[I] = _mm512_dpbf16_ps(sums[I],
                (__m512bh)_mm512_loadu_epi16(d + i + I * elements),
                (__m512bh)_mm512_loadu_epi16(q + i + I * elements));
        });
    }

    __m512 total = tree_reduce<batches, __m512, _mm512_add_ps>(sums);

    // Non-batched tail
    for (; i + elements <= elementCount; i += elements) {
        total = _mm512_dpbf16_ps(total,
            (__m512bh)_mm512_loadu_epi16(d + i),
            (__m512bh)_mm512_loadu_epi16(q + i));
    }

    // Masked tail: handle remaining bytes that don't fill a full 512-bit register.
    // Masked-off lanes load as zero, contributing nothing to the dot product.
    const int maskRem = elementCount - i;
    if (maskRem > 0) {
        __mmask32 readMask = (__mmask32)((1UL << maskRem) - 1);
        __mmask16 dpMask = (__mmask16)((1U << (maskRem/2)) - 1);
        __m512bh d_rem = (__m512bh)_mm512_maskz_loadu_epi16(readMask, d + i);
        __m512bh q_rem = (__m512bh)_mm512_maskz_loadu_epi16(readMask, q + i);
        total = _mm512_mask_dpbf16_ps(total, dpMask, d_rem, q_rem);
    }

    f32_t result = _mm512_reduce_add_ps(total);

    if ((maskRem & 1) != 0) {
        // odd number of dimensions
        result += dot_scalar(d[elementCount - 1], q[elementCount - 1]);
    }

    return result;
}

/*
 * BFloat16 single operation with manual bf16->f32 conversion + fmadd.
 * Processes 16 bf16 elements per batch (256-bit bf16 load -> 512-bit f32).
 * Uses masked SIMD loads for the tail instead of scalar fallback.
 *
 * Template parameter:
 * vector_op: SIMD per-dimension vector operation, takes a, b, sum, returns new sum
 */
template<__m512(*vector_op)(const __m512, const __m512, const __m512)>
static inline f32_t bf16Qf32_inner_avx512(const bf16_t* d, const f32_t* q, const int32_t elementCount) {
    constexpr int batches = 4;

    __m512 sums[batches];
    apply_indexed<batches>([&](auto I) {
        sums[I] = _mm512_setzero_ps();
    });

    int i = 0;
    // each __m256i holds 16 bf16 values, widened to 16 f32 in a __m512
    constexpr int elements = sizeof(__m256) / sizeof(bf16_t);
    constexpr int stride = elements * batches;
    for (; i < (elementCount & ~(stride - 1)); i += stride) {
        apply_indexed<batches>([&](auto I) {
            sums[I] = vector_op(load_bf16(d, i + I * elements), load_f32(q, i + I * elements), sums[I]);
        });
    }

    __m512 total_sum = tree_reduce<batches, __m512, _mm512_add_ps>(sums);

    // Non-batched tail
    for (; i + elements <= elementCount; i += elements) {
        total_sum = vector_op(load_bf16(d, i), load_f32(q, i), total_sum);
    }

    // Masked tail: remaining elements < 16
    const int rem = elementCount - i;
    if (rem > 0) {
        __mmask16 mask = (__mmask16)((1U << rem) - 1);
        __m512 d_rem = bf16_to_f32(_mm256_maskz_loadu_epi16(mask, d + i));
        __m512 q_rem = _mm512_maskz_loadu_ps(mask, q + i);
        total_sum = vector_op(d_rem, q_rem, total_sum);
    }

    return _mm512_reduce_add_ps(total_sum);
}

EXPORT f32_t vec_dotDbf16Qbf16_3(const bf16_t* a, const bf16_t* b, const int32_t elementCount) {
    return dotDbf16Qbf16_inner_avx512(a, b, elementCount);
}

EXPORT f32_t vec_dotDbf16Qf32_3(const bf16_t* a, const f32_t* b, const int32_t elementCount) {
    return bf16Qf32_inner_avx512<_mm512_fmadd_ps>(a, b, elementCount);
}

static inline __m512 sqrf32_vector(const __m512 a, const __m512 b, const __m512 sum) {
    __m512 diff = _mm512_sub_ps(a, b);
    return _mm512_fmadd_ps(diff, diff, sum);
}

EXPORT f32_t vec_sqrDbf16Qf32_3(const bf16_t* a, const f32_t* b, const int32_t elementCount) {
    return bf16Qf32_inner_avx512<sqrf32_vector>(a, b, elementCount);
}

/*
 * Squared distance of 2 bf16 vectors using dpbf16, via the identity:
 * |a - b|^2 = a*a - 2*a*b + b*b
 * Each term is a dot product computed natively with dpbf16, avoiding
 * the costly bf16 -> f32 conversion that makes the generic path ALU-bound.
 */
static inline f32_t sqrDbf16Qbf16_inner_avx512(const bf16_t* a, const bf16_t* b, int32_t elementCount) {
    constexpr int batches = 4;

    __m512 sum_self[batches];
    __m512 sum_cross[batches];
    apply_indexed<batches>([&](auto I) {
        sum_self[I] = _mm512_setzero_ps();
        sum_cross[I] = _mm512_setzero_ps();
    });

    int i = 0;
    constexpr int elements = sizeof(__m512bh) / sizeof(bf16_t);
    constexpr int stride = elements * batches;
    for (; i < (elementCount & ~(stride - 1)); i += stride) {
        apply_indexed<batches>([&](auto I) {
            __m512bh av = (__m512bh)_mm512_loadu_epi16(a + i + I * elements);
            __m512bh bv = (__m512bh)_mm512_loadu_epi16(b + i + I * elements);
            sum_self[I] = _mm512_dpbf16_ps(sum_self[I], av, av);
            sum_self[I] = _mm512_dpbf16_ps(sum_self[I], bv, bv);
            sum_cross[I] = _mm512_dpbf16_ps(sum_cross[I], av, bv);
        });
    }

    __m512 total_self = tree_reduce<batches, __m512, _mm512_add_ps>(sum_self);
    __m512 total_cross = tree_reduce<batches, __m512, _mm512_add_ps>(sum_cross);

    // Non-batched tail
    for (; i + elements <= elementCount; i += elements) {
        __m512bh av = (__m512bh)_mm512_loadu_epi16(a + i);
        __m512bh bv = (__m512bh)_mm512_loadu_epi16(b + i);
        total_self = _mm512_dpbf16_ps(total_self, av, av);
        total_self = _mm512_dpbf16_ps(total_self, bv, bv);
        total_cross = _mm512_dpbf16_ps(total_cross, av, bv);
    }

    // Masked tail
    const int maskRem = elementCount - i;
    if (maskRem > 0) {
        __mmask32 readMask = (__mmask32)((1UL << maskRem) - 1);
        __mmask16 dpMask = (__mmask16)((1U << (maskRem / 2)) - 1);
        __m512bh a_rem = (__m512bh)_mm512_maskz_loadu_epi16(readMask, a + i);
        __m512bh b_rem = (__m512bh)_mm512_maskz_loadu_epi16(readMask, b + i);
        total_self = _mm512_mask_dpbf16_ps(total_self, dpMask, a_rem, a_rem);
        total_self = _mm512_mask_dpbf16_ps(total_self, dpMask, b_rem, b_rem);
        total_cross = _mm512_mask_dpbf16_ps(total_cross, dpMask, a_rem, b_rem);
    }

    // |a - b|^2 = a*a - 2*a*b + b*b
    f32_t result = _mm512_reduce_add_ps(total_self) - 2.0f * _mm512_reduce_add_ps(total_cross);

    // Scalar tail for odd remaining element
    if ((maskRem & 1) != 0) {
        result += sqr_scalar(a[elementCount - 1], b[elementCount - 1]);
    }

    return result;
}

EXPORT f32_t vec_sqrDbf16Qbf16_3(const bf16_t* a, const bf16_t* b, const int32_t elementCount) {
    return sqrDbf16Qbf16_inner_avx512(a, b, elementCount);
}

// --- Bulk operations ---

/*
 * AVX-512 bulk operation for bf16 data with f32 query.
 * Converts bf16 data to f32, then applies vector_op (fmadd for dot, sub+fmadd for sqr).
 * Processes `batches` vectors in parallel per dimension step, loading the f32 query
 * once per step and reusing it across all vectors in the batch.
 * Uses masked loads for the dimension tail instead of scalar fallback.
 *
 * L1D cache set aliasing and the `batches` parameter:
 * When vectors are stored contiguously (sequential access), they are separated by
 * exactly `dims * sizeof(bf16_t)` bytes. For power-of-2 dims (e.g. 1024 -> stride 2048),
 * all vectors in a batch map to the same L1D cache sets at any given dimension offset.
 * With batches=4, four simultaneous load streams fight for the same cache sets, causing
 * evictions and stalls -- despite having fewer total loads than the single-pair baseline.
 * This effect is absent for non-power-of-2 dims (e.g. 768 -> stride 1536) and for
 * random-offset access (bulk_offsets) where vectors are scattered across memory.
 * Sequential callers should use batches=1 to avoid this; offset-based callers can
 * safely use batches=4 since random offsets break the aliasing pattern.
 */
template <
    const bf16_t*(*mapper)(const bf16_t*, const int32_t, const int32_t*, const int32_t),
    __m512(*vector_op)(const __m512, const __m512, const __m512),
    f32_t(*bulk_tail)(const bf16_t*, const f32_t*, const int32_t),
    int batches = 4
>
static inline void bf16Qf32_bulk_avx512(
    const bf16_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    int c = 0;

    // 16 bf16 elements per 256-bit load, widened to 512-bit f32
    constexpr int elements = sizeof(__m256) / sizeof(bf16_t);
    const int lines_to_fetch = dims * sizeof(bf16_t) / CACHE_LINE_SIZE + 1;

    const bf16_t* current_vecs[batches];
    init_pointers<batches, bf16_t, bf16_t, mapper>(current_vecs, a, pitch, offsets, 0, count);

    for (; c + batches - 1 < count; c += batches) {
        const bf16_t* next_vecs[batches];
        const bool has_next = c + 2 * batches - 1 < count;
        if (has_next) {
            apply_indexed<batches>([&](auto I) {
                next_vecs[I] = mapper(a, c + batches + I, offsets, pitch);
                prefetch(next_vecs[I], lines_to_fetch);
            });
        }

        __m512 sums[batches];
        apply_indexed<batches>([&](auto I) {
            sums[I] = _mm512_setzero_ps();
        });

        int i = 0;
        for (; i + elements <= dims; i += elements) {
            __m512 qi = load_f32(b, i);
            apply_indexed<batches>([&](auto I) {
                sums[I] = vector_op(load_bf16(current_vecs[I], i), qi, sums[I]);
            });
        }

        // Masked tail: remaining elements < 16
        const int rem = dims - i;
        if (rem > 0) {
            __mmask16 mask = (__mmask16)((1U << rem) - 1);
            __m512 qi = _mm512_maskz_loadu_ps(mask, b + i);
            apply_indexed<batches>([&](auto I) {
                __m512 ai = bf16_to_f32(_mm256_maskz_loadu_epi16(mask, current_vecs[I] + i));
                sums[I] = vector_op(ai, qi, sums[I]);
            });
        }

        apply_indexed<batches>([&](auto I) {
            results[c + I] = _mm512_reduce_add_ps(sums[I]);
        });

        if (has_next) {
            std::copy_n(next_vecs, batches, current_vecs);
        }
    }

    // Remaining vectors that don't fill a full batch
    for (; c < count; c++) {
        const bf16_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = bulk_tail(a0, b, dims);
    }
}

EXPORT void vec_dotDbf16Qf32_bulk_3(const bf16_t* a, const f32_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    bf16Qf32_bulk_avx512<sequential_mapper, _mm512_fmadd_ps, vec_dotDbf16Qf32_3, 1>(
        a, b, dims, dims, NULL, count, results);
}

// --- Qbf16 bulk: native dpbf16 paths ---

/*
 * Bulk dot product for bf16×bf16 using dpbf16.
 * Loads query once per dimension step, applies to all vectors in the batch.
 * See bf16Qf32_bulk_avx512 for L1D cache set aliasing considerations on `batches`.
 */
template <
    const bf16_t*(*mapper)(const bf16_t*, const int32_t, const int32_t*, const int32_t),
    int batches = 4
>
static inline void dotDbf16Qbf16_bulk_avx512(
    const bf16_t* a,
    const bf16_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    int c = 0;
    constexpr int elements = sizeof(__m512bh) / sizeof(bf16_t);
    const int lines_to_fetch = dims * sizeof(bf16_t) / CACHE_LINE_SIZE + 1;

    const bf16_t* current_vecs[batches];
    init_pointers<batches, bf16_t, bf16_t, mapper>(current_vecs, a, pitch, offsets, 0, count);

    for (; c + batches - 1 < count; c += batches) {
        const bf16_t* next_vecs[batches];
        const bool has_next = c + 2 * batches - 1 < count;
        if (has_next) {
            apply_indexed<batches>([&](auto I) {
                next_vecs[I] = mapper(a, c + batches + I, offsets, pitch);
                prefetch(next_vecs[I], lines_to_fetch);
            });
        }

        __m512 sums[batches];
        apply_indexed<batches>([&](auto I) {
            sums[I] = _mm512_setzero_ps();
        });

        int i = 0;
        for (; i + elements <= dims; i += elements) {
            __m512bh qi = (__m512bh)_mm512_loadu_epi16(b + i);
            apply_indexed<batches>([&](auto I) {
                sums[I] = _mm512_dpbf16_ps(sums[I],
                    (__m512bh)_mm512_loadu_epi16(current_vecs[I] + i), qi);
            });
        }

        // Masked tail
        const int rem = dims - i;
        if (rem > 0) {
            __mmask32 readMask = (__mmask32)((1UL << rem) - 1);
            __mmask16 dpMask = (__mmask16)((1U << (rem / 2)) - 1);
            __m512bh qi = (__m512bh)_mm512_maskz_loadu_epi16(readMask, b + i);
            apply_indexed<batches>([&](auto I) {
                __m512bh ai = (__m512bh)_mm512_maskz_loadu_epi16(readMask, current_vecs[I] + i);
                sums[I] = _mm512_mask_dpbf16_ps(sums[I], dpMask, ai, qi);
            });
        }

        apply_indexed<batches>([&](auto I) {
            f32_t result = _mm512_reduce_add_ps(sums[I]);
            if ((rem & 1) != 0) {
                result += dot_scalar(current_vecs[I][dims - 1], b[dims - 1]);
            }
            results[c + I] = result;
        });

        if (has_next) {
            std::copy_n(next_vecs, batches, current_vecs);
        }
    }

    for (; c < count; c++) {
        const bf16_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = vec_dotDbf16Qbf16_3(a0, b, dims);
    }
}

/*
 * Bulk squared distance for bf16×bf16 using dpbf16 via ||a-b||² = a·a - 2·a·b + b·b.
 * Loads query once per dimension step. Computes q·q once (shared across all vectors
 * in the batch), then per vector accumulates a·a and a·q.
 * See bf16Qf32_bulk_avx512 for L1D cache set aliasing considerations on `batches`.
 */
template <
    const bf16_t*(*mapper)(const bf16_t*, const int32_t, const int32_t*, const int32_t),
    int batches = 4
>
static inline void sqrDbf16Qbf16_bulk_avx512(
    const bf16_t* a,
    const bf16_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    int c = 0;
    constexpr int elements = sizeof(__m512bh) / sizeof(bf16_t);
    const int rem = dims % elements;
    const bool odd_dims = (rem & 1) != 0;
    const int lines_to_fetch = dims * sizeof(bf16_t) / CACHE_LINE_SIZE + 1;

    const bf16_t* current_vecs[batches];
    init_pointers<batches, bf16_t, bf16_t, mapper>(current_vecs, a, pitch, offsets, 0, count);

    // Compute squared distance (|a - b|^2) as its expansion (a dot a + b dot b - 2(a dot b)),
    // using 3x dpbf16 operations, accumulating them in sum_aa, sum_qq, sum_ab
    for (; c + batches - 1 < count; c += batches) {
        const bf16_t* next_vecs[batches];
        const bool has_next = c + 2 * batches - 1 < count;
        if (has_next) {
            apply_indexed<batches>([&](auto I) {
                next_vecs[I] = mapper(a, c + batches + I, offsets, pitch);
                prefetch(next_vecs[I], lines_to_fetch);
            });
        }

        __m512 sum_aa[batches];
        __m512 sum_ab[batches];
        apply_indexed<batches>([&](auto I) {
            sum_aa[I] = _mm512_setzero_ps();
            sum_ab[I] = _mm512_setzero_ps();
        });
        __m512 sum_qq = _mm512_setzero_ps();

        int i = 0;
        for (; i + elements <= dims; i += elements) {
            __m512bh qi = (__m512bh)_mm512_loadu_epi16(b + i);
            sum_qq = _mm512_dpbf16_ps(sum_qq, qi, qi);
            apply_indexed<batches>([&](auto I) {
                __m512bh ai = (__m512bh)_mm512_loadu_epi16(current_vecs[I] + i);
                sum_aa[I] = _mm512_dpbf16_ps(sum_aa[I], ai, ai);
                sum_ab[I] = _mm512_dpbf16_ps(sum_ab[I], ai, qi);
            });
        }

        // Masked tail
        if (rem > 0) {
            __mmask32 readMask = (__mmask32)((1UL << rem) - 1);
            __mmask16 dpMask = (__mmask16)((1U << (rem / 2)) - 1);
            __m512bh qi = (__m512bh)_mm512_maskz_loadu_epi16(readMask, b + i);
            sum_qq = _mm512_mask_dpbf16_ps(sum_qq, dpMask, qi, qi);
            apply_indexed<batches>([&](auto I) {
                __m512bh ai = (__m512bh)_mm512_maskz_loadu_epi16(readMask, current_vecs[I] + i);
                sum_aa[I] = _mm512_mask_dpbf16_ps(sum_aa[I], dpMask, ai, ai);
                sum_ab[I] = _mm512_mask_dpbf16_ps(sum_ab[I], dpMask, ai, qi);
            });
        }

        f32_t qq = _mm512_reduce_add_ps(sum_qq);
        // dpbf16 is a pair-wise instruction, so we may need to consider a lone un-paired element
        if (odd_dims) {
            qq += dot_scalar(b[dims - 1], b[dims - 1]);
            apply_indexed<batches>([&](auto I) {
                f32_t aa = _mm512_reduce_add_ps(sum_aa[I]);
                f32_t ab = _mm512_reduce_add_ps(sum_ab[I]);
                aa += dot_scalar(current_vecs[I][dims - 1], current_vecs[I][dims - 1]);
                ab += dot_scalar(current_vecs[I][dims - 1], b[dims - 1]);
                results[c + I] = aa + qq - 2.0f * ab;
            });
        } else {
            apply_indexed<batches>([&](auto I) {
                f32_t aa = _mm512_reduce_add_ps(sum_aa[I]);
                f32_t ab = _mm512_reduce_add_ps(sum_ab[I]);
                results[c + I] = aa + qq - 2.0f * ab;
            });
        }

        if (has_next) {
            std::copy_n(next_vecs, batches, current_vecs);
        }
    }

    for (; c < count; c++) {
        const bf16_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = vec_sqrDbf16Qbf16_3(a0, b, dims);
    }
}

EXPORT void vec_dotDbf16Qbf16_bulk_3(const bf16_t* a, const bf16_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    dotDbf16Qbf16_bulk_avx512<sequential_mapper, 1>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqrDbf16Qf32_bulk_3(const bf16_t* a, const f32_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    bf16Qf32_bulk_avx512<sequential_mapper, sqrf32_vector, vec_sqrDbf16Qf32_3, 1>(
        a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqrDbf16Qbf16_bulk_3(const bf16_t* a, const bf16_t* b, const int32_t dims, const int32_t count, f32_t* results) {
    sqrDbf16Qbf16_bulk_avx512<sequential_mapper, 1>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_dotDbf16Qf32_bulk_offsets_3(
    const bf16_t* a, const f32_t* b, const int32_t dims, const int32_t pitch,
    const int32_t* offsets, const int32_t count, f32_t* results) {
    bf16Qf32_bulk_avx512<offsets_mapper, _mm512_fmadd_ps, vec_dotDbf16Qf32_3>(
        a, b, dims, pitch / sizeof(bf16_t), offsets, count, results);
}

EXPORT void vec_dotDbf16Qbf16_bulk_offsets_3(
    const bf16_t* a, const bf16_t* b, const int32_t dims, const int32_t pitch,
    const int32_t* offsets, const int32_t count, f32_t* results) {
    dotDbf16Qbf16_bulk_avx512<offsets_mapper>(a, b, dims, pitch / sizeof(bf16_t), offsets, count, results);
}

EXPORT void vec_sqrDbf16Qf32_bulk_offsets_3(
    const bf16_t* a, const f32_t* b, const int32_t dims, const int32_t pitch,
    const int32_t* offsets, const int32_t count, f32_t* results) {
    bf16Qf32_bulk_avx512<offsets_mapper, sqrf32_vector, vec_sqrDbf16Qf32_3>(
        a, b, dims, pitch / sizeof(bf16_t), offsets, count, results);
}

EXPORT void vec_sqrDbf16Qbf16_bulk_offsets_3(
    const bf16_t* a, const bf16_t* b, const int32_t dims, const int32_t pitch,
    const int32_t* offsets, const int32_t count, f32_t* results) {
    sqrDbf16Qbf16_bulk_avx512<offsets_mapper>(a, b, dims, pitch / sizeof(bf16_t), offsets, count, results);
}
