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
#include <arm_neon.h>
#include <algorithm>
#include "vec.h"
#include "vec_common.h"
#include "aarch64/aarch64_vec_common.h"

/*
 * Int8 matrix multiplication, C[m x n] += A[m x k] @ B[k x n], using the SDOT (dotprod) instruction,
 * which multiplies groups of 4 int8 values and accumulates them into int32 lanes.
 *
 * C is computed in tiles of TILE_ROWS A rows x TILE_COLS B cols, held in TILE_ROWS x TILE_VECS int32x4 registers.
 * The k values are split into blocks of K_BLOCK. For each block, the K_BLOCK values of TILE_COLS B cols
 * are packed into a panel array, which stays in L1. Then every TILE_ROWS A rows, over the same K_BLOCK values,
 * are multiplied against that panel, and the results are added to the matching C tile.
 *
 * The sdot instruction is vdotq_laneq_s32(int32x4 acc, int8x16 b, int8x16 a, int lane).
 * 'lane' selects which set of 4 bytes in `a` is broadcast and multiplied with everything in `b`.
 */

// number of rows processed at once - this determines the number of accumulators
constexpr int TILE_ROWS = 4;
// int8x16 values are used to store each B row, so 16 columns per value
constexpr int TILE_COLS = 16;
// int32 lanes in one accumulator vector
constexpr int LANES = 4;
// Accumulator vectors per tile row, also the number of panel vectors per group
constexpr int TILE_VECS = TILE_COLS / LANES;
// Number of values multiplied together by one vdotq_laneq_s32 lane
constexpr int GROUP = 4;
// Bytes of packed panel per group: TILE_COLS B cols x GROUP values from each B col.
constexpr int GROUP_BYTES = TILE_COLS * GROUP;
// int8 values in one int8x16 value of A
constexpr int STEP = 16;
// k values (A rows, or B cols) per block. A packed panel is K_BLOCK * TILE_COLS bytes, so it stays in L1 across all the rows.
constexpr int K_BLOCK = 1024;

static_assert(K_BLOCK % STEP == 0, "K_BLOCK must be a whole number of steps");
static_assert(TILE_VECS == 4, "vld1q_s8_x4 loads exactly 4 panel vectors per group");

// Packs B[lStart .. lStart + lCount) x [j, j + cols) into panel, as [group][B col][GROUP values from that B col].
// Missing B cols, and B col values up to the next whole step, are zero.
static void pack_panel(const int8_t* b, const int n, const int lStart, const int lCount, const int j, const int cols, int8_t* panel) {
    const int kPadded = (lCount + STEP - 1) / STEP * STEP;
    int l = 0;

    if (cols == TILE_COLS) {
        for (; l + GROUP <= lCount; l += GROUP) {
            const int8_t* src = b + (int64_t)(lStart + l) * n + j;
            int8x16x4_t bRows;
            apply_indexed<GROUP>([&](auto T) {
                bRows.val[T] = vld1q_s8(src + (int64_t)n * T);
            });
            // interleaves the 4 rows, so each B col has GROUP consecutive values together
            // this effectively transposes the panel for each vector value,
            // so each vector value stores sequential columns, not rows.
            vst4q_s8(panel + (l / GROUP) * GROUP_BYTES, bRows);
        }
    }

    // partial panel, or the tail of the block
    for (; l < kPadded; l += GROUP) {
        int8_t* dst = panel + (l / GROUP) * GROUP_BYTES;
        for (int c = 0; c < TILE_COLS; ++c) {
            for (int t = 0; t < GROUP; ++t) {
                dst[c * GROUP + t] = (c < cols && l + t < lCount) ? b[(int64_t)(lStart + l + t) * n + j + c] : 0;
            }
        }
    }
}

// Multiplies STEP values of each A row against the matching groups of the panel.
template <int ROWS>
static inline void multiply_step(int32x4_t (&acc)[ROWS][TILE_VECS], const int8x16_t (&aVecs)[ROWS], const int8_t* panel) {
    apply_indexed<STEP / GROUP>([&](auto G) {
        const int8x16x4_t bVecs = vld1q_s8_x4(panel + G * GROUP_BYTES);
        apply_indexed<ROWS>([&](auto R) {
            apply_indexed<TILE_VECS>([&](auto Q) {
                // column LANES * Q + x accumulates the dot product of its GROUP values with group G of row R
                // the 'lane' parameter selects which set of 4 bytes in aVecs is actually used
                acc[R][Q] = vdotq_laneq_s32(acc[R][Q], bVecs.val[Q], aVecs[R], decltype(G)::value);
            });
        });
    });
}

// Adds ROWS rows x TILE_COLS columns of A @ panel to c. aRows are the starts (already offset to lStart) of the A rows.
template <int ROWS>
static inline void multiply_panel(const int8_t* const (&aRows)[ROWS], const int8_t* panel, const int lCount, int32_t* c, const int ldc) {
    int32x4_t acc[ROWS][TILE_VECS];
    apply_indexed<ROWS>([&](auto R) {
        apply_indexed<TILE_VECS>([&](auto Q) {
            acc[R][Q] = vld1q_s32(c + (int64_t)R * ldc + Q * LANES);
        });
    });

    const int kLimit = lCount - lCount % STEP;
    int l = 0;
    for (; l < kLimit; l += STEP) {
        int8x16_t aVecs[ROWS];
        apply_indexed<ROWS>([&](auto R) {
            aVecs[R] = vld1q_s8(aRows[R] + l);
        });
        multiply_step<ROWS>(acc, aVecs, panel + (l / GROUP) * GROUP_BYTES);
    }

    // tail, padded with zeros so it doesn't read past the end of the A rows
    if (l < lCount) {
        int8_t tail[ROWS][STEP] = {};
        int8x16_t aVecs[ROWS];
        apply_indexed<ROWS>([&](auto R) {
            std::copy_n(aRows[R] + l, lCount - l, tail[R]);
            aVecs[R] = vld1q_s8(tail[R]);
        });
        multiply_step<ROWS>(acc, aVecs, panel + (l / GROUP) * GROUP_BYTES);
    }

    // write back accumulators
    apply_indexed<ROWS>([&](auto R) {
        apply_indexed<TILE_VECS>([&](auto Q) {
            vst1q_s32(c + (int64_t)R * ldc + Q * LANES, acc[R][Q]);
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
    const int8_t* panel,
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
    // allocated directly on the stack. This is probably ok?
    alignas(16) int8_t panel[K_BLOCK * TILE_COLS];

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
