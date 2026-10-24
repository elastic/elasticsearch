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
 * Int8 matrix multiplication, C[m x n] += A[m x k] @ B[k x n], using AVX2.
 * AVX2 has no exact signed 8-bit dot product, so B is widened to int16 when it is packed,
 * and the VPMADDWD instruction multiplies pairs of int16 values and adds each pair into an int32 lane.
 *
 * C is computed in tiles of TILE_ROWS A rows x TILE_COLS B cols, held in TILE_ROWS x TILE_VECS __m256i registers.
 * The k values are split into blocks of K_BLOCK. For each block, the K_BLOCK values of TILE_COLS B cols
 * are packed into a panel array, which stays in L1. Then every TILE_ROWS A rows, over the same K_BLOCK values,
 * are multiplied against that panel, and the results are added to the matching C tile.
 *
 * The madd instruction is _mm256_madd_epi16(__m256i b, __m256i a).
 * Each int32 lane adds the products of one pair of int16 values in `b` with the same pair in `a`.
 * `a` has the same pair of A values broadcast to every lane.
 */

// number of rows processed at once - this determines the number of accumulators
constexpr int TILE_ROWS = 4;
// 16-byte values are used to load each B row, so 16 columns per value
constexpr int TILE_COLS = 16;
// int32 lanes in one __m256i accumulator vector
constexpr int LANES = 8;
// Accumulator vectors per tile row, also the number of panel vectors per group
constexpr int TILE_VECS = TILE_COLS / LANES;
// Number of values multiplied together by one _mm256_madd_epi16 lane
// we have to use int16 here rather than int8, as AVX2 only has asymmetric int8 madd instructions, and ours are symmetric
constexpr int GROUP = 2;
// int16 values of packed panel in one vector: LANES B cols x GROUP values from each B col.
constexpr int VEC_VALUES = LANES * GROUP;
// int16 values of packed panel per group: TILE_COLS B cols x GROUP values from each B col.
constexpr int GROUP_VALUES = TILE_COLS * GROUP;
// k values (A rows, or B cols) per block. A packed panel is K_BLOCK * TILE_COLS int16 values, so it stays in L1 across all the rows.
constexpr int K_BLOCK = 512;

static_assert(K_BLOCK % GROUP == 0, "K_BLOCK must be a whole number of groups");
static_assert(TILE_VECS == 2, "pack_panel writes exactly 2 panel vectors per group");

// Packs B[lStart .. lStart + lCount) x [j, j + cols) into panel as int16, as [group][B col][GROUP values from that B col].
// Missing B cols, and B col values up to the next whole group, are zero.
static void pack_panel(const int8_t* b, const int n, const int lStart, const int lCount, const int j, const int cols, int16_t* panel) {
    const int kPadded = (lCount + GROUP - 1) / GROUP * GROUP;
    int l = 0;

    if (cols == TILE_COLS) {
        for (; l + GROUP <= lCount; l += GROUP) {
            const int8_t* src = b + (int64_t)(lStart + l) * n + j;
            const __m256i row0 = _mm256_cvtepi8_epi16(_mm_loadu_si128((const __m128i*)src));
            const __m256i row1 = _mm256_cvtepi8_epi16(_mm_loadu_si128((const __m128i*)(src + n)));

            // interleaves the 2 rows, so each B col has GROUP consecutive values together
            // unpack works within each 128-bit half, so the halves are put back in column order when storing.
            const __m256i lo = _mm256_unpacklo_epi16(row0, row1); // columns 0-3 and 8-11
            const __m256i hi = _mm256_unpackhi_epi16(row0, row1); // columns 4-7 and 12-15

            int16_t* dst = panel + (l / GROUP) * GROUP_VALUES;
            _mm256_storeu_si256((__m256i*)dst, _mm256_permute2x128_si256(lo, hi, 0x20));                // columns 0-7
            _mm256_storeu_si256((__m256i*)(dst + VEC_VALUES), _mm256_permute2x128_si256(lo, hi, 0x31)); // columns 8-15
        }
    }

    // partial panel, or the tail of the block
    for (; l < kPadded; l += GROUP) {
        int16_t* dst = panel + (l / GROUP) * GROUP_VALUES;
        for (int c = 0; c < TILE_COLS; ++c) {
            for (int t = 0; t < GROUP; ++t) {
                dst[c * GROUP + t] = (c < cols && l + t < lCount) ? b[(int64_t)(lStart + l + t) * n + j + c] : 0;
            }
        }
    }
}

// Puts the int16 pair (a0, a1) in every int32 lane, to multiply against a group of the panel.
static inline __m256i broadcast_pair(const int8_t a0, const int8_t a1) {
    return _mm256_set1_epi32((int32_t)((uint16_t)a0 | ((uint32_t)a1 << 16)));
}

// Multiplies GROUP values of each A row against the matching group of the panel.
template <int ROWS>
static inline void multiply_group(__m256i (&acc)[ROWS][TILE_VECS], const __m256i (&aPairs)[ROWS], const int16_t* panel) {
    apply_indexed<TILE_VECS>([&](auto Q) {
        const __m256i bVec = _mm256_loadu_si256((const __m256i*)(panel + Q * VEC_VALUES));
        apply_indexed<ROWS>([&](auto R) {
            // column LANES * Q + x adds its GROUP values multiplied by the GROUP values of row R
            acc[R][Q] = _mm256_add_epi32(acc[R][Q], _mm256_madd_epi16(bVec, aPairs[R]));
        });
    });
}

// Adds ROWS rows x TILE_COLS columns of A @ panel to c. aRows are the starts (already offset to lStart) of the A rows.
template <int ROWS>
static inline void multiply_panel(const int8_t* const (&aRows)[ROWS], const int16_t* panel, const int lCount, int32_t* c, const int ldc) {
    __m256i acc[ROWS][TILE_VECS];
    apply_indexed<ROWS>([&](auto R) {
        apply_indexed<TILE_VECS>([&](auto Q) {
            acc[R][Q] = _mm256_loadu_si256((const __m256i*)(c + (int64_t)R * ldc + Q * LANES));
        });
    });

    const int kLimit = lCount - lCount % GROUP;
    int l = 0;
    for (; l < kLimit; l += GROUP) {
        __m256i aPairs[ROWS];
        apply_indexed<ROWS>([&](auto R) {
            aPairs[R] = broadcast_pair(aRows[R][l], aRows[R][l + 1]);
        });
        multiply_group<ROWS>(acc, aPairs, panel + (l / GROUP) * GROUP_VALUES);
    }

    // tail, the panel is zero for the missing B col value
    if (l < lCount) {
        __m256i aPairs[ROWS];
        apply_indexed<ROWS>([&](auto R) {
            aPairs[R] = broadcast_pair(aRows[R][l], 0);
        });
        multiply_group<ROWS>(acc, aPairs, panel + (l / GROUP) * GROUP_VALUES);
    }

    // write back accumulators
    apply_indexed<ROWS>([&](auto R) {
        apply_indexed<TILE_VECS>([&](auto Q) {
            _mm256_storeu_si256((__m256i*)(c + (int64_t)R * ldc + Q * LANES), acc[R][Q]);
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
    const int16_t* panel,
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

    if (cols == TILE_COLS) {
        multiply_panel<ROWS>(aRows, panel, lCount, c + (int64_t)i * n + j, n);
    } else {
        // use a separate array of the full width, leaving zeros in unallocated columns,
        // so we don't access C out-of-bounds
        int32_t tail[ROWS * TILE_COLS] = {};
        for (int r = 0; r < ROWS; ++r) {
            std::copy_n(c + (int64_t)(i + r) * n + j, cols, tail + r * TILE_COLS);
        }
        multiply_panel<ROWS>(aRows, panel, lCount, tail, TILE_COLS);
        for (int r = 0; r < ROWS; ++r) {
            std::copy_n(tail + r * TILE_COLS, cols, c + (int64_t)(i + r) * n + j);
        }
    }
}

// Adds A @ B to columns [colStart, colEnd) of C, where A is m x k, B is k x n and C is m x n, all row-major.
// C must be zeroed by the caller to get the plain product.
// Splitting by column range lets the caller run the ranges in parallel, since they write disjoint parts of C.
EXPORT void vec_sgemm_i8(
    const int8_t* a,
    const int8_t* b,
    const int32_t m,
    const int32_t k,
    const int32_t n,
    int32_t* c,
    const int32_t colStart,
    const int32_t colEnd
) {
    // allocated directly on the stack, K_BLOCK * TILE_COLS * 2 bytes
    alignas(32) int16_t panel[K_BLOCK * TILE_COLS];

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

            // row tail
            const int rowTail = m - i;
            switch (rowTail) {
                case 3:
                    multiply_rows<3>(a, k, lStart, lCount, panel, c, n, i, j, cols);
                    break;
                case 2:
                    multiply_rows<2>(a, k, lStart, lCount, panel, c, n, i, j, cols);
                    break;
                case 1:
                    multiply_rows<1>(a, k, lStart, lCount, panel, c, n, i, j, cols);
                    break;
            }
        }
    }
}
