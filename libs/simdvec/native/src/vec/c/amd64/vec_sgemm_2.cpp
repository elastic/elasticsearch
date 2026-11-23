/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include "vec.h"
#include "vec_common.h"
#include "amd64/amd64_vec_common.h"

/*
 * Int8 matrix multiplication, C[m x n] += A[m x k] @ B[k x n], using AVX-512 VNNI.
 * VPDPBUSD multiplies unsigned bytes by signed bytes, so we use the identity
 *   a * b = a * (b + 128) - 128 * a
 * B is biased to unsigned by flipping the top bit (b ^ 0x80 == b + 128) when it is packed, so the bias costs nothing
 * in the inner loop. The 128 * sum(a) correction for each A row is subtracted from the C tile before accumulating.
 * Biasing B rather than A lets A be broadcast to VPDPBUSD as-is. Arithmetic is modulo 2^32, so the result is exact.
 *
 * C is computed in tiles of TILE_ROWS A rows x TILE_COLS B cols, held in TILE_ROWS x TILE_VECS __m512i registers.
 * The k values are split into blocks of K_BLOCK. For each block, the K_BLOCK values of TILE_COLS B cols
 * are packed into a panel array, which stays in L1. Then every TILE_ROWS A rows, over the same K_BLOCK values,
 * are multiplied against that panel, and the results are added to the matching C tile.
 *
 * The multiply instruction is _mm512_dpbusd_epi32(acc, __m512i b, __m512i a).
 * Each int32 lane adds the products of GROUP unsigned values of one B col in `b` with GROUP signed values in `a`.
 * `a` has the same GROUP A values broadcast to every lane.
 */

// number of rows processed at once - this determines the number of accumulators
constexpr int TILE_ROWS = 8;
// int32 lanes in one __m512i accumulator vector
constexpr int LANES = 16;
// Accumulator vectors per tile row, also the number of panel vectors per group
constexpr int TILE_VECS = 2;
// B cols per tile
constexpr int TILE_COLS = LANES * TILE_VECS;
// Number of values multiplied together by one _mm512_dpbusd_epi32 lane
constexpr int GROUP = 4;
// uint8 values of packed panel in one vector: LANES B cols x GROUP values from each B col.
constexpr int VEC_VALUES = LANES * GROUP;
// uint8 values of packed panel per group: TILE_COLS B cols x GROUP values from each B col.
constexpr int GROUP_VALUES = TILE_COLS * GROUP;
// k values (A rows, or B cols) per block. A packed panel is K_BLOCK * TILE_COLS bytes, so it stays in L1 across all the rows.
constexpr int K_BLOCK = 512;

static_assert(K_BLOCK % GROUP == 0, "K_BLOCK must be a whole number of groups");
static_assert(TILE_COLS == sizeof(__m256i), "pack_panel loads one __m256i per B row");

// Packs B[lStart .. lStart + lCount) x [j, j + cols) into panel as biased uint8, as [group][B col][GROUP values from that B col].
// Missing B cols, and B col values up to the next whole group, are zero, so they add nothing whatever the A value.
static void pack_panel(const int8_t* b, const int n, const int lStart, const int lCount, const int j, const int cols, uint8_t* panel) {
    // masked loads zero the missing B cols and suppress faults on them, so they do not read past the end of B
    const __mmask32 colMask = (__mmask32)_bzhi_u32(0xFFFFFFFFu, cols);
    const int8_t* src = b + (int64_t)lStart * n + j;

    for (int l = 0; l < lCount; l += GROUP) {
        __m256i rows[GROUP];
        apply_indexed<GROUP>([&](auto T) {
            // B col values past the end of the block are zero
            const bool present = l + T < lCount;
            const __mmask32 mask = present ? colMask : 0;
            const __m256i row = _mm256_maskz_loadu_epi8(mask, present ? src + (int64_t)(l + T) * n : src);
            // only the loaded values are biased, so the missing values stay zero
            rows[T] = _mm256_xor_si256(row, _mm256_maskz_set1_epi8(mask, (char)0x80));
        });

        // interleaves the 4 rows, so each B col has GROUP consecutive values together.
        // unpack works within each 128-bit half, so the halves are put back in column order when storing.
        const __m256i r01lo = _mm256_unpacklo_epi8(rows[0], rows[1]); // columns 0-7 and 16-23
        const __m256i r01hi = _mm256_unpackhi_epi8(rows[0], rows[1]); // columns 8-15 and 24-31
        const __m256i r23lo = _mm256_unpacklo_epi8(rows[2], rows[3]);
        const __m256i r23hi = _mm256_unpackhi_epi8(rows[2], rows[3]);

        const __m256i c0 = _mm256_unpacklo_epi16(r01lo, r23lo); // columns 0-3 and 16-19
        const __m256i c1 = _mm256_unpackhi_epi16(r01lo, r23lo); // columns 4-7 and 20-23
        const __m256i c2 = _mm256_unpacklo_epi16(r01hi, r23hi); // columns 8-11 and 24-27
        const __m256i c3 = _mm256_unpackhi_epi16(r01hi, r23hi); // columns 12-15 and 28-31

        uint8_t* dst = panel + (l / GROUP) * GROUP_VALUES;
        _mm256_storeu_si256((__m256i*)dst, _mm256_permute2x128_si256(c0, c1, 0x20));        // columns 0-7
        _mm256_storeu_si256((__m256i*)(dst + 32), _mm256_permute2x128_si256(c2, c3, 0x20)); // columns 8-15
        _mm256_storeu_si256((__m256i*)(dst + 64), _mm256_permute2x128_si256(c0, c1, 0x31)); // columns 16-23
        _mm256_storeu_si256((__m256i*)(dst + 96), _mm256_permute2x128_si256(c2, c3, 0x31)); // columns 24-31
    }
}

// Returns 128 * sum(a[0 .. count)), using the same XOR + SAD approach as the bulk i8 dot product.
static inline int32_t row_correction(const int8_t* a, const int count) {
    const __m512i xor_mask = _mm512_set1_epi8((char)0x80);
    const __m512i zeros = _mm512_setzero_si512();
    __m512i sum_biased = _mm512_setzero_si512();
    int l = 0;
    for (; l + (int)sizeof(__m512i) <= count; l += sizeof(__m512i)) {
        const __m512i av = _mm512_loadu_si512(a + l);
        sum_biased = _mm512_add_epi64(sum_biased, _mm512_sad_epu8(_mm512_xor_si512(av, xor_mask), zeros));
    }
    if (l < count) {
        const __mmask64 mask = (__mmask64)_bzhi_u64(0xFFFFFFFFFFFFFFFFULL, count - l);
        const __m512i av = _mm512_maskz_loadu_epi8(mask, a + l);
        // the masked XOR leaves unused lanes at zero, so they add nothing
        sum_biased = _mm512_add_epi64(
            sum_biased,
            _mm512_sad_epu8(_mm512_xor_si512(av, _mm512_maskz_set1_epi8(mask, (char)0x80)), zeros)
        );
    }
    // sum(a + 128) - 128 * count == sum(a)
    const int64_t sum_a = _mm512_reduce_add_epi64(sum_biased) - 128LL * count;
    return (int32_t)(128LL * sum_a);
}

// Multiplies GROUP values of each A row against the matching group of the panel.
template <int ROWS>
static inline void multiply_group(__m512i (&acc)[ROWS][TILE_VECS], const __m512i (&aGroups)[ROWS], const uint8_t* panel) {
    apply_indexed<TILE_VECS>([&](auto Q) {
        const __m512i bVec = _mm512_loadu_si512(panel + Q * VEC_VALUES);
        apply_indexed<ROWS>([&](auto R) {
            // column LANES * Q + x adds its GROUP values multiplied by the GROUP values of row R
            acc[R][Q] = _mm512_dpbusd_epi32(acc[R][Q], bVec, aGroups[R]);
        });
    });
}

// Adds ROWS rows x TILE_COLS columns of A @ panel to c. aRows are the starts (already offset to lStart) of the A rows.
// Only the B cols set in colMask are read from and written to c.
template <int ROWS>
static inline void multiply_panel(
    const int8_t* const (&aRows)[ROWS],
    const uint8_t* panel,
    const int lCount,
    int32_t* c,
    const int ldc,
    const __mmask32 colMask
) {
    // starting from c - correction leaves the plain product added to c when the accumulators are written back
    __m512i acc[ROWS][TILE_VECS];
    apply_indexed<ROWS>([&](auto R) {
        const __m512i correction = _mm512_set1_epi32(row_correction(aRows[R], lCount));
        apply_indexed<TILE_VECS>([&](auto Q) {
            const __mmask16 mask = (__mmask16)(colMask >> (Q * LANES));
            acc[R][Q] = _mm512_sub_epi32(_mm512_maskz_loadu_epi32(mask, c + (int64_t)R * ldc + Q * LANES), correction);
        });
    });

    const int kLimit = lCount - lCount % GROUP;
    int l = 0;
    for (; l < kLimit; l += GROUP) {
        // puts the GROUP A values of each row in every int32 lane, to multiply against a group of the panel
        __m512i aGroups[ROWS];
        apply_indexed<ROWS>([&](auto R) {
            aGroups[R] = _mm512_broadcastd_epi32(_mm_loadu_si32(aRows[R] + l));
        });
        multiply_group<ROWS>(acc, aGroups, panel + (l / GROUP) * GROUP_VALUES);
    }

    // tail, the panel is zero for the missing B col values.
    // mask the load on A so it only reads what it should (extra values are zero)
    if (l < lCount) {
        const __mmask16 mask = (__mmask16)((1u << (lCount - l)) - 1);
        __m512i aGroups[ROWS];
        apply_indexed<ROWS>([&](auto R) {
            aGroups[R] = _mm512_broadcastd_epi32(_mm_maskz_loadu_epi8(mask, aRows[R] + l));
        });
        multiply_group<ROWS>(acc, aGroups, panel + (l / GROUP) * GROUP_VALUES);
    }

    // write back accumulators
    apply_indexed<ROWS>([&](auto R) {
        apply_indexed<TILE_VECS>([&](auto Q) {
            const __mmask16 mask = (__mmask16)(colMask >> (Q * LANES));
            _mm512_mask_storeu_epi32(c + (int64_t)R * ldc + Q * LANES, mask, acc[R][Q]);
        });
    });
}

// Adds rows [i, i + ROWS) x columns [j, j + cols) of A @ panel to c.
template <int ROWS>
static inline void multiply_rows(
    const int8_t* a,
    const int k,
    const int lStart,
    const int lCount,
    const uint8_t* panel,
    int32_t* c,
    const int n,
    const int i,
    const int j,
    const int cols
) {
    const int8_t* aRows[ROWS];
    apply_indexed<ROWS>([&](auto R) {
        aRows[R] = a + (int64_t)(i + R) * k + lStart;
    });

    // masked accesses leave the B cols past cols alone, as they can be past the end of C or in another task's range.
    const __mmask32 colMask = (__mmask32)_bzhi_u32(0xFFFFFFFFu, cols);
    multiply_panel<ROWS>(aRows, panel, lCount, c + (int64_t)i * n + j, n, colMask);
}

// Adds A @ B to columns [colStart, colEnd) of C, where A is m x k, B is k x n and C is m x n, all row-major.
// C must be zeroed by the caller to get the plain product.
// Splitting by column range lets the caller run the ranges in parallel, since they write disjoint parts of C.
EXPORT void vec_sgemm_i8_2(
    const int8_t* a,
    const int8_t* b,
    const int32_t m,
    const int32_t k,
    const int32_t n,
    int32_t* c,
    const int32_t colStart,
    const int32_t colEnd
) {
    // allocated directly on the stack, K_BLOCK * TILE_COLS bytes
    alignas(64) uint8_t panel[K_BLOCK * TILE_COLS];

    for (int lStart = 0; lStart < k; lStart += K_BLOCK) {
        const int lCount = std::min(K_BLOCK, k - lStart);

        for (int j = colStart; j < colEnd; j += TILE_COLS) {
            const int cols = std::min(TILE_COLS, colEnd - j);
            // one panel is packed once per block, then every row of A is multiplied against it while it is in L1
            pack_panel(b, n, lStart, lCount, j, cols, panel);

            int i = 0;
            for (; i + TILE_ROWS <= m; i += TILE_ROWS) {
                multiply_rows<TILE_ROWS>(a, k, lStart, lCount, panel, c, n, i, j, cols);
            }

            // row tail, split into powers of two to avoid instantiating every smaller group size
            static_assert(TILE_ROWS == 8, "the row tail handles up to 7 rows");
            const int rowTail = m - i;
            if (rowTail & 4) {
                multiply_rows<4>(a, k, lStart, lCount, panel, c, n, i, j, cols);
                i += 4;
            }
            if (rowTail & 2) {
                multiply_rows<2>(a, k, lStart, lCount, panel, c, n, i, j, cols);
                i += 2;
            }
            if (rowTail & 1) {
                multiply_rows<1>(a, k, lStart, lCount, panel, c, n, i, j, cols);
            }
        }
    }
}
