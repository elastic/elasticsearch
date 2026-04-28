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
    typename TData,
    const bf16_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    __m512(*vector_op)(const __m512, const __m512, const __m512),
    f32_t(*bulk_tail)(const bf16_t*, const f32_t*, const int32_t),
    int batches = 4
>
static inline void bf16Qf32_bulk_avx512(
    const TData* a,
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
    init_pointers<batches, TData, bf16_t, mapper>(current_vecs, a, pitch, offsets, 0, count);

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

/*
 * Bulk dot product for bf16×bf16 using dpbf16.
 * Loads query once per dimension step, applies to all vectors in the batch.
 * See bf16Qf32_bulk_avx512 for L1D cache set aliasing considerations on `batches`.
 */
/*
 * `unroll_k` is the number of consecutive K-blocks unrolled inside the inner loop,
 * each with its own accumulator. SPR's vdpbf16ps has latency 4-5 cycles and 0.5-cycle
 * throughput (ports 0+5), so a single accumulator is dependency-bound at ~20% of peak.
 *
 * Sequential paths benefit most from unroll_k > 1 because batches=1 (single load
 * stream — no L1D set aliasing) means only one accumulator without unrolling, which
 * is severely latency-bound. Offsets/sparse already use batches=4 which gives four
 * concurrent accumulators; further K-unrolling there increased reduction overhead
 * without consistently winning, so they stay at unroll_k=1.
 *
 * Picks (Sapphire Rapids, validated 2026-04-27):
 *   sequential dot: batches=1, unroll_k=8  -> 8 accumulators
 *   sequential sqr: batches=1, unroll_k=4  -> 12 accumulators (4 aa + 4 ab + 4 qq)
 *   offsets/sparse: batches=4, unroll_k=1  -> identical to pre-optimization code
 */
template <
    typename TData,
    const bf16_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int batches = 4,
    int unroll_k = 1
>
static inline void dotDbf16Qbf16_bulk_avx512(
    const TData* a,
    const bf16_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    int c = 0;
    constexpr int elements = sizeof(__m512bh) / sizeof(bf16_t);
    constexpr int kStride = elements * unroll_k;
    const int lines_to_fetch = dims * sizeof(bf16_t) / CACHE_LINE_SIZE + 1;

    const bf16_t* current_vecs[batches];
    init_pointers<batches, TData, bf16_t, mapper>(current_vecs, a, pitch, offsets, 0, count);

    for (; c + batches - 1 < count; c += batches) {
        const bf16_t* next_vecs[batches];
        const bool has_next = c + 2 * batches - 1 < count;
        if (has_next) {
            apply_indexed<batches>([&](auto I) {
                next_vecs[I] = mapper(a, c + batches + I, offsets, pitch);
                prefetch(next_vecs[I], lines_to_fetch);
            });
        }

        __m512 sums[batches * unroll_k];
        apply_indexed<batches * unroll_k>([&](auto I) {
            sums[I] = _mm512_setzero_ps();
        });

        // Main loop: K-stride = unroll_k * elements. Each iteration issues
        // unroll_k * batches independent dpbf16ps's into distinct accumulators.
        int i = 0;
        for (; i + kStride <= dims; i += kStride) {
            apply_indexed<unroll_k>([&](auto K) {
                __m512bh qi = (__m512bh)_mm512_loadu_epi16(b + i + K * elements);
                apply_indexed<batches>([&](auto I) {
                    sums[K * batches + I] = _mm512_dpbf16_ps(sums[K * batches + I],
                        (__m512bh)_mm512_loadu_epi16(current_vecs[I] + i + K * elements), qi);
                });
            });
        }

        // Reduce + K-tail-1 only emit when the K-unroll is > 1. At unroll_k=1
        // the main loop already exits exactly when (i + 32 > dims), and the
        // K-tail-1 condition is the same — guarding with constexpr keeps the
        // unroll_k=1 instantiation byte-equivalent to the pre-optimization code.
        if constexpr (unroll_k > 1) {
            // Tree-reduce across unroll_k accumulators per batch member, leaving
            // sums[0..batches-1] holding the per-vector partial that the tail
            // loops feed into.
            apply_indexed<batches>([&](auto I) {
                __m512 acc = sums[I];
                for (int u = 1; u < unroll_k; u++) {
                    acc = _mm512_add_ps(acc, sums[u * batches + I]);
                }
                sums[I] = acc;
            });

            // K-unroll-1 tail (full 32-element blocks not consumed by main loop).
            for (; i + elements <= dims; i += elements) {
                __m512bh qi = (__m512bh)_mm512_loadu_epi16(b + i);
                apply_indexed<batches>([&](auto I) {
                    sums[I] = _mm512_dpbf16_ps(sums[I],
                        (__m512bh)_mm512_loadu_epi16(current_vecs[I] + i), qi);
                });
            }
        }

        // Masked tail (< 32 BF16 elements left).
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
 * q·q is invariant across batches and is computed once before the batch loop, freeing
 * register pressure inside the inner loop. Per-batch we carry only `aa` and `ab`
 * streams. unroll_k applies to both — see `dotDbf16Qbf16_bulk_avx512` for the
 * latency / aliasing rationale.
 */
template <
    typename TData,
    const bf16_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int batches = 4,
    int unroll_k = 1
>
static inline void sqrDbf16Qbf16_bulk_avx512(
    const TData* a,
    const bf16_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    int c = 0;
    constexpr int elements = sizeof(__m512bh) / sizeof(bf16_t);
    constexpr int kStride = elements * unroll_k;
    const int rem = dims % elements;
    const bool odd_dims = (rem & 1) != 0;
    const int lines_to_fetch = dims * sizeof(bf16_t) / CACHE_LINE_SIZE + 1;

    const bf16_t* current_vecs[batches];
    init_pointers<batches, TData, bf16_t, mapper>(current_vecs, a, pitch, offsets, 0, count);

    for (; c + batches - 1 < count; c += batches) {
        const bf16_t* next_vecs[batches];
        const bool has_next = c + 2 * batches - 1 < count;
        if (has_next) {
            apply_indexed<batches>([&](auto I) {
                next_vecs[I] = mapper(a, c + batches + I, offsets, pitch);
                prefetch(next_vecs[I], lines_to_fetch);
            });
        }

        __m512 sum_aa[batches * unroll_k];
        __m512 sum_ab[batches * unroll_k];
        __m512 sum_qq[unroll_k];
        apply_indexed<batches * unroll_k>([&](auto I) {
            sum_aa[I] = _mm512_setzero_ps();
            sum_ab[I] = _mm512_setzero_ps();
        });
        apply_indexed<unroll_k>([&](auto K) {
            sum_qq[K] = _mm512_setzero_ps();
        });

        // Main loop with K-stride = unroll_k * elements.
        int i = 0;
        for (; i + kStride <= dims; i += kStride) {
            apply_indexed<unroll_k>([&](auto K) {
                __m512bh qi = (__m512bh)_mm512_loadu_epi16(b + i + K * elements);
                sum_qq[K] = _mm512_dpbf16_ps(sum_qq[K], qi, qi);
                apply_indexed<batches>([&](auto I) {
                    __m512bh ai = (__m512bh)_mm512_loadu_epi16(current_vecs[I] + i + K * elements);
                    sum_aa[K * batches + I] = _mm512_dpbf16_ps(sum_aa[K * batches + I], ai, ai);
                    sum_ab[K * batches + I] = _mm512_dpbf16_ps(sum_ab[K * batches + I], ai, qi);
                });
            });
        }

        // Reduce + K-tail-1 only emit for unroll_k > 1 (see dot variant for the
        // rationale). At unroll_k=1 the main loop already covers all full
        // 32-element blocks; the constexpr keeps the unroll_k=1 instantiation
        // byte-equivalent to the pre-optimization code.
        if constexpr (unroll_k > 1) {
            apply_indexed<batches>([&](auto I) {
                __m512 acc_aa = sum_aa[I];
                __m512 acc_ab = sum_ab[I];
                for (int u = 1; u < unroll_k; u++) {
                    acc_aa = _mm512_add_ps(acc_aa, sum_aa[u * batches + I]);
                    acc_ab = _mm512_add_ps(acc_ab, sum_ab[u * batches + I]);
                }
                sum_aa[I] = acc_aa;
                sum_ab[I] = acc_ab;
            });
            for (int u = 1; u < unroll_k; u++) {
                sum_qq[0] = _mm512_add_ps(sum_qq[0], sum_qq[u]);
            }

            // K-unroll-1 tail (full 32-element blocks).
            for (; i + elements <= dims; i += elements) {
                __m512bh qi = (__m512bh)_mm512_loadu_epi16(b + i);
                sum_qq[0] = _mm512_dpbf16_ps(sum_qq[0], qi, qi);
                apply_indexed<batches>([&](auto I) {
                    __m512bh ai = (__m512bh)_mm512_loadu_epi16(current_vecs[I] + i);
                    sum_aa[I] = _mm512_dpbf16_ps(sum_aa[I], ai, ai);
                    sum_ab[I] = _mm512_dpbf16_ps(sum_ab[I], ai, qi);
                });
            }
        }

        // Masked tail (< 32 BF16 left)
        if (rem > 0) {
            __mmask32 readMask = (__mmask32)((1UL << rem) - 1);
            __mmask16 dpMask = (__mmask16)((1U << (rem / 2)) - 1);
            __m512bh qi = (__m512bh)_mm512_maskz_loadu_epi16(readMask, b + i);
            sum_qq[0] = _mm512_mask_dpbf16_ps(sum_qq[0], dpMask, qi, qi);
            apply_indexed<batches>([&](auto I) {
                __m512bh ai = (__m512bh)_mm512_maskz_loadu_epi16(readMask, current_vecs[I] + i);
                sum_aa[I] = _mm512_mask_dpbf16_ps(sum_aa[I], dpMask, ai, ai);
                sum_ab[I] = _mm512_mask_dpbf16_ps(sum_ab[I], dpMask, ai, qi);
            });
        }

        f32_t qq = _mm512_reduce_add_ps(sum_qq[0]);
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

EXPORT void vec_dotDbf16Qf32_bulk_3(
    const bf16_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results
) {
    bf16Qf32_bulk_avx512<bf16_t, sequential_mapper, _mm512_fmadd_ps, vec_dotDbf16Qf32_3, 1>(
        a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_dotDbf16Qbf16_bulk_3(
    const bf16_t* a,
    const bf16_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results
) {
    dotDbf16Qbf16_bulk_avx512<bf16_t, sequential_mapper, 1, 8>(a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqrDbf16Qf32_bulk_3(
    const bf16_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results
) {
    bf16Qf32_bulk_avx512<bf16_t, sequential_mapper, sqrf32_vector, vec_sqrDbf16Qf32_3, 1>(
        a, b, dims, dims, NULL, count, results);
}

EXPORT void vec_sqrDbf16Qbf16_bulk_3(
    const bf16_t* a,
    const bf16_t* b,
    const int32_t dims,
    const int32_t count,
    f32_t* results
) {
    sqrDbf16Qbf16_bulk_avx512<bf16_t, sequential_mapper, 1, 4>(a, b, dims, dims, NULL, count, results);
}


EXPORT void vec_dotDbf16Qf32_bulk_sparse_3(
    const void* const* addresses,
    const f32_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    bf16Qf32_bulk_avx512<const bf16_t*, sparse_mapper, _mm512_fmadd_ps, vec_dotDbf16Qf32_3>(
        (const bf16_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT void vec_dotDbf16Qbf16_bulk_sparse_3(
    const void* const* addresses,
    const bf16_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    dotDbf16Qbf16_bulk_avx512<const bf16_t*, sparse_mapper, 4, 1>(
        (const bf16_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT void vec_sqrDbf16Qf32_bulk_sparse_3(
    const void* const* addresses,
    const f32_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    bf16Qf32_bulk_avx512<const bf16_t*, sparse_mapper, sqrf32_vector, vec_sqrDbf16Qf32_3>(
        (const bf16_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT void vec_sqrDbf16Qbf16_bulk_sparse_3(
    const void* const* addresses,
    const bf16_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results
) {
    sqrDbf16Qbf16_bulk_avx512<const bf16_t*, sparse_mapper, 4, 1>(
        (const bf16_t* const*)addresses, query, length, 0, NULL, count, results);
}

EXPORT void vec_dotDbf16Qf32_bulk_offsets_3(
    const bf16_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    bf16Qf32_bulk_avx512<bf16_t, offsets_mapper, _mm512_fmadd_ps, vec_dotDbf16Qf32_3>(
        a, b, dims, pitch / sizeof(bf16_t), offsets, count, results);
}

EXPORT void vec_dotDbf16Qbf16_bulk_offsets_3(
    const bf16_t* a,
    const bf16_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    dotDbf16Qbf16_bulk_avx512<bf16_t, offsets_mapper, 4, 1>(a, b, dims, pitch / sizeof(bf16_t), offsets, count, results);
}

EXPORT void vec_sqrDbf16Qf32_bulk_offsets_3(
    const bf16_t* a,
    const f32_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    bf16Qf32_bulk_avx512<bf16_t, offsets_mapper, sqrf32_vector, vec_sqrDbf16Qf32_3>(
        a, b, dims, pitch / sizeof(bf16_t), offsets, count, results);
}

EXPORT void vec_sqrDbf16Qbf16_bulk_offsets_3(
    const bf16_t* a,
    const bf16_t* b,
    const int32_t dims,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    sqrDbf16Qbf16_bulk_avx512<bf16_t, offsets_mapper, 4, 1>(a, b, dims, pitch / sizeof(bf16_t), offsets, count, results);
}
