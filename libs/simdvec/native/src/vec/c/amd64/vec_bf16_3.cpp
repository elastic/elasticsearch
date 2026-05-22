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
 *
 * `unroll_dim` consecutive blocks per pass with independent accumulators; same idea
 * as the bulk variants below.
 */
static inline f32_t dotDbf16Qbf16_inner_avx512(const bf16_t* d, const bf16_t* q, int32_t elementCount) {
    constexpr int unroll_dim = 8;

    __m512 sums[unroll_dim];
    apply_indexed<unroll_dim>([&](auto I) {
        sums[I] = _mm512_setzero_ps();
    });

    int i = 0;
    constexpr int elements = sizeof(__m512bh) / sizeof(bf16_t);
    constexpr int stride512 = sizeof(__m512bh) / sizeof(bf16_t) * unroll_dim;
    for (; i < (elementCount & ~(stride512 - 1)); i += stride512) {
        apply_indexed<unroll_dim>([&](auto I) {
            sums[I] = _mm512_dpbf16_ps(sums[I],
                (__m512bh)_mm512_loadu_epi16(d + i + I * elements),
                (__m512bh)_mm512_loadu_epi16(q + i + I * elements));
        });
    }

    __m512 total = tree_reduce<unroll_dim, __m512, _mm512_add_ps>(sums);

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
 * Processes 16 bf16 elements per pass (256-bit bf16 load -> 512-bit f32).
 * Uses masked SIMD loads for the tail instead of scalar fallback.
 *
 * `unroll_dim` consecutive blocks per pass with independent accumulators; same idea
 * as the bulk variants below.
 *
 * Template parameter:
 * vector_op: SIMD per-dimension vector operation, takes a, b, sum, returns new sum
 */
template<__m512(*vector_op)(const __m512, const __m512, const __m512)>
static inline f32_t bf16Qf32_inner_avx512(const bf16_t* d, const f32_t* q, const int32_t elementCount) {
    constexpr int unroll_dim = 4;

    __m512 sums[unroll_dim];
    apply_indexed<unroll_dim>([&](auto I) {
        sums[I] = _mm512_setzero_ps();
    });

    int i = 0;
    // each __m256i holds 16 bf16 values, widened to 16 f32 in a __m512
    constexpr int elements = sizeof(__m256) / sizeof(bf16_t);
    constexpr int stride = elements * unroll_dim;
    for (; i < (elementCount & ~(stride - 1)); i += stride) {
        apply_indexed<unroll_dim>([&](auto I) {
            sums[I] = vector_op(load_bf16(d, i + I * elements), load_f32(q, i + I * elements), sums[I]);
        });
    }

    __m512 total_sum = tree_reduce<unroll_dim, __m512, _mm512_add_ps>(sums);

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
 *
 * `unroll_dim` consecutive blocks per pass with independent accumulators; same idea
 * as the bulk variants below.
 */
static inline f32_t sqrDbf16Qbf16_inner_avx512(const bf16_t* a, const bf16_t* b, int32_t elementCount) {
    constexpr int unroll_dim = 4;

    __m512 sum_self[unroll_dim];
    __m512 sum_cross[unroll_dim];
    apply_indexed<unroll_dim>([&](auto I) {
        sum_self[I] = _mm512_setzero_ps();
        sum_cross[I] = _mm512_setzero_ps();
    });

    int i = 0;
    constexpr int elements = sizeof(__m512bh) / sizeof(bf16_t);
    constexpr int stride = elements * unroll_dim;
    for (; i < (elementCount & ~(stride - 1)); i += stride) {
        apply_indexed<unroll_dim>([&](auto I) {
            __m512bh av = (__m512bh)_mm512_loadu_epi16(a + i + I * elements);
            __m512bh bv = (__m512bh)_mm512_loadu_epi16(b + i + I * elements);
            sum_self[I] = _mm512_dpbf16_ps(sum_self[I], av, av);
            sum_self[I] = _mm512_dpbf16_ps(sum_self[I], bv, bv);
            sum_cross[I] = _mm512_dpbf16_ps(sum_cross[I], av, bv);
        });
    }

    __m512 total_self = tree_reduce<unroll_dim, __m512, _mm512_add_ps>(sum_self);
    __m512 total_cross = tree_reduce<unroll_dim, __m512, _mm512_add_ps>(sum_cross);

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
 * exactly `dims * sizeof(bf16_t)` bytes. For power-of-2 dims, all vectors in a batch
 * map to the same L1D cache sets at any given dimension offset. With batches=4, four
 * simultaneous load streams fight for the same cache sets, causing evictions and
 * stalls -- despite having fewer total loads than the single-pair baseline.
 *
 * Concrete example, Sapphire Rapids L1D (48 KiB, 12-way, 64 sets, 64-byte lines):
 *   dims=2048 -> per-vector stride = 4096 bytes = 64 cache lines = exactly 64 sets,
 *   so all four vectors in a batches=4 group land in the same set group at every
 *   dimension offset; with 12-way associativity and four competing streams, the
 *   working set thrashes its way through evictions.
 *   dims=768 -> per-vector stride = 1536 bytes = 24 cache lines, so the four
 *   vectors map to four distinct set groups (offsets 0, 24, 48, 8) and there is
 *   no aliasing.
 *
 * Random-offset access (bulk_offsets, bulk_sparse) scatters the streams unconditionally,
 * so the pathology never arises -- those paths keep batches=4. Sequential callers
 * use batches=1 and instead extract more accumulators along the dim axis (see the
 * comment block above dotDbf16Qbf16_bulk_avx512 below).
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
 * Bulk dot product for bf16×bf16 using vdpbf16ps. Loads the query once per dimension
 * step and applies it to all vectors in the batch.
 *
 * Two independent unroll axes:
 *   batches    — number of distinct vector pairs scored in parallel; amortises the
 *                query load and adds independent FMAs across vectors. Capped at 1
 *                on sequential paths because of the L1D set-aliasing pathology
 *                described above bf16Qf32_bulk_avx512.
 *   unroll_dim — number of consecutive `elements`-wide blocks of the same vector
 *                pair processed inside the inner loop, each with its own accumulator.
 *                Targets the `vdpbf16ps` latency: on current x64 CPUs the instruction
 *                has multi-cycle latency with sub-cycle throughput, so a single
 *                accumulator is dependency-bound at a fraction of peak. Several
 *                independent accumulators give the issue ports enough independent
 *                work to fill the latency window. The single-vector kernels at the
 *                top of this file (e.g. dotDbf16Qbf16_inner_avx512) already use the
 *                same trick under their local `unroll_dim` constant.
 *
 * The two axes are mutually exclusive — see the static_assert inside the template.
 */
template <
    typename TData,
    const bf16_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int batches = 4,
    int unroll_dim = 1
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
    static_assert(batches == 1 || unroll_dim == 1,
        "Combining batches > 1 with unroll_dim > 1 is not a supported tuning point;"
        " see the comment block above for the rationale.");

    int c = 0;
    constexpr int elements = sizeof(__m512bh) / sizeof(bf16_t);
    constexpr int dimStride = elements * unroll_dim;
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

        // Row-major layout: sums[I * unroll_dim + U] keeps the unroll_dim accumulators
        // for batch member I contiguous, so tree_reduce can fold them in one call.
        __m512 sums[batches * unroll_dim];
        apply_indexed<batches * unroll_dim>([&](auto I) {
            sums[I] = _mm512_setzero_ps();
        });

        // Main loop: each iteration advances the dim cursor by unroll_dim * elements
        // bf16 lanes, issuing unroll_dim * batches independent vdpbf16ps's into
        // distinct accumulators to fill the multi-cycle vdpbf16ps latency window.
        int i = 0;
        for (; i + dimStride <= dims; i += dimStride) {
            apply_indexed<unroll_dim>([&](auto U) {
                __m512bh qi = (__m512bh)_mm512_loadu_epi16(b + i + U * elements);
                apply_indexed<batches>([&](auto I) {
                    sums[I * unroll_dim + U] = _mm512_dpbf16_ps(sums[I * unroll_dim + U],
                        (__m512bh)_mm512_loadu_epi16(current_vecs[I] + i + U * elements), qi);
                });
            });
        }

        // After the main loop, fold the unroll_dim accumulators per batch back into
        // sums[I]. The constexpr branch is skipped at unroll_dim=1, where the main
        // loop already wrote sums[I*1+0] = sums[I] and the tail loops just feed it
        // -- byte-equivalent to the pre-optimisation code.
        if constexpr (unroll_dim > 1) {
            apply_indexed<batches>([&](auto I) {
                sums[I] = tree_reduce<unroll_dim, __m512, _mm512_add_ps>(&sums[I * unroll_dim]);
            });

            // Dim-unroll-1 tail: full 32-element blocks not consumed by the main loop.
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
 * Bulk squared distance for bf16×bf16 using vdpbf16ps via ||a-b||² = a·a - 2·a·b + b·b.
 * b·b is invariant across the batches dimension (the query is shared), so it is
 * accumulated once per batch loop iteration and reduced into a single scalar at the
 * end, freeing register pressure inside the inner loop. Per-batch we carry only the
 * `aa` and `ab` streams. `unroll_dim` applies to all three streams — see
 * `dotDbf16Qbf16_bulk_avx512` for the latency / aliasing rationale.
 */
template <
    typename TData,
    const bf16_t*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t),
    int batches = 4,
    int unroll_dim = 1
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
    static_assert(batches == 1 || unroll_dim == 1,
        "Combining batches > 1 with unroll_dim > 1 is not a supported tuning point;"
        " see dotDbf16Qbf16_bulk_avx512 for the rationale.");

    int c = 0;
    constexpr int elements = sizeof(__m512bh) / sizeof(bf16_t);
    constexpr int dimStride = elements * unroll_dim;
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

        // Row-major layout for sum_aa / sum_ab: sum_xx[I * unroll_dim + U] keeps
        // unroll_dim accumulators per batch member contiguous, so tree_reduce can
        // fold them in one call. sum_bb is already contiguous along the unroll axis.
        __m512 sum_aa[batches * unroll_dim];
        __m512 sum_ab[batches * unroll_dim];
        __m512 sum_bb[unroll_dim];
        apply_indexed<batches * unroll_dim>([&](auto I) {
            sum_aa[I] = _mm512_setzero_ps();
            sum_ab[I] = _mm512_setzero_ps();
        });
        apply_indexed<unroll_dim>([&](auto U) {
            sum_bb[U] = _mm512_setzero_ps();
        });

        // Main loop: each iteration advances the dim cursor by unroll_dim * elements
        // bf16 lanes, issuing unroll_dim * (1 + 2 * batches) independent vdpbf16ps's.
        int i = 0;
        for (; i + dimStride <= dims; i += dimStride) {
            apply_indexed<unroll_dim>([&](auto U) {
                __m512bh qi = (__m512bh)_mm512_loadu_epi16(b + i + U * elements);
                sum_bb[U] = _mm512_dpbf16_ps(sum_bb[U], qi, qi);
                apply_indexed<batches>([&](auto I) {
                    __m512bh ai = (__m512bh)_mm512_loadu_epi16(current_vecs[I] + i + U * elements);
                    sum_aa[I * unroll_dim + U] = _mm512_dpbf16_ps(sum_aa[I * unroll_dim + U], ai, ai);
                    sum_ab[I * unroll_dim + U] = _mm512_dpbf16_ps(sum_ab[I * unroll_dim + U], ai, qi);
                });
            });
        }

        // After the main loop, fold the unroll_dim accumulators per stream back
        // into sum_aa[I] / sum_ab[I] / sum_bb[0]. The constexpr branch is skipped
        // at unroll_dim=1, where the main loop already wrote sum_*[I*1+0] = sum_*[I]
        // and sum_bb[0] -- byte-equivalent to the pre-optimisation code.
        if constexpr (unroll_dim > 1) {
            apply_indexed<batches>([&](auto I) {
                sum_aa[I] = tree_reduce<unroll_dim, __m512, _mm512_add_ps>(&sum_aa[I * unroll_dim]);
                sum_ab[I] = tree_reduce<unroll_dim, __m512, _mm512_add_ps>(&sum_ab[I * unroll_dim]);
            });
            sum_bb[0] = tree_reduce<unroll_dim, __m512, _mm512_add_ps>(sum_bb);

            // Dim-unroll-1 tail: full 32-element blocks not consumed by the main loop.
            for (; i + elements <= dims; i += elements) {
                __m512bh qi = (__m512bh)_mm512_loadu_epi16(b + i);
                sum_bb[0] = _mm512_dpbf16_ps(sum_bb[0], qi, qi);
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
            sum_bb[0] = _mm512_mask_dpbf16_ps(sum_bb[0], dpMask, qi, qi);
            apply_indexed<batches>([&](auto I) {
                __m512bh ai = (__m512bh)_mm512_maskz_loadu_epi16(readMask, current_vecs[I] + i);
                sum_aa[I] = _mm512_mask_dpbf16_ps(sum_aa[I], dpMask, ai, ai);
                sum_ab[I] = _mm512_mask_dpbf16_ps(sum_ab[I], dpMask, ai, qi);
            });
        }

        f32_t bb = _mm512_reduce_add_ps(sum_bb[0]);
        if (odd_dims) {
            bb += dot_scalar(b[dims - 1], b[dims - 1]);
            apply_indexed<batches>([&](auto I) {
                f32_t aa = _mm512_reduce_add_ps(sum_aa[I]);
                f32_t ab = _mm512_reduce_add_ps(sum_ab[I]);
                aa += dot_scalar(current_vecs[I][dims - 1], current_vecs[I][dims - 1]);
                ab += dot_scalar(current_vecs[I][dims - 1], b[dims - 1]);
                results[c + I] = aa + bb - 2.0f * ab;
            });
        } else {
            apply_indexed<batches>([&](auto I) {
                f32_t aa = _mm512_reduce_add_ps(sum_aa[I]);
                f32_t ab = _mm512_reduce_add_ps(sum_ab[I]);
                results[c + I] = aa + bb - 2.0f * ab;
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
