/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// This file contains implementations for vector processing functionalities,
// for the "2nd tier" vector capabilities; in the case of ARM, this second tier
// consist of functions for processors supporting the SVE/SVE2
// instruction set.

#include <stddef.h>
#include <arm_sve.h>
#include "vec.h"
#include "vec_common.h"
#include "aarch64/aarch64_vec_common.h"

static inline svuint64_t dot_bit_sv(const svbool_t pg, const svuint8_t a, const int8_t* b) {
    const svuint8_t q0 = svld1_u8(pg, (const uint8_t*)b);
    // reinterpret the u8 result as u64 so the count doesn't overflow
    return svcnt_u64_x(svptrue_b64(), svreinterpret_u64(svand_u8_z(pg, q0, a)));
}

static inline int64_t dotd1q4_inner(const int8_t* a, const int8_t* query, const int32_t length) {
    int r = 0;

    // Init accumulator(s) with 0
    svuint64_t acc0 = svdup_n_u64(0);
    svuint64_t acc1 = svdup_n_u64(0);
    svuint64_t acc2 = svdup_n_u64(0);
    svuint64_t acc3 = svdup_n_u64(0);

    for (svbool_t pg = svwhilelt_b8(r, length); svptest_any(svptrue_b8(), pg); pg = svwhilelt_b8(r, length)) {
        const svuint8_t value = svld1_u8(pg, (const uint8_t*)(a + r));

        acc0 = svadd_u64_x(svptrue_b64(), acc0, dot_bit_sv(pg, value, query + r));
        acc1 = svadd_u64_x(svptrue_b64(), acc1, dot_bit_sv(pg, value, query + r + length));
        acc2 = svadd_u64_x(svptrue_b64(), acc2, dot_bit_sv(pg, value, query + r + 2 * length));
        acc3 = svadd_u64_x(svptrue_b64(), acc3, dot_bit_sv(pg, value, query + r + 3 * length));

        r += svcntb();
    }

    int64_t subRet0 = svaddv_u64(svptrue_b64(), acc0);
    int64_t subRet1 = svaddv_u64(svptrue_b64(), acc1);
    int64_t subRet2 = svaddv_u64(svptrue_b64(), acc2);
    int64_t subRet3 = svaddv_u64(svptrue_b64(), acc3);

    return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
}

EXPORT int64_t vec_dotd1q4_2(const int8_t* a, const int8_t* query, const int32_t length) {
    return dotd1q4_inner(a, query, length);
}

template <const int8_t*(*mapper)(const int8_t*, const int32_t, const int32_t*, const int32_t)>
static inline void dotd1q4_inner_bulk(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results
) {
    int c = 0;

    for (; c + 3 < count; c += 4) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        const int8_t* a1 = mapper(a, c + 1, offsets, pitch);
        const int8_t* a2 = mapper(a, c + 2, offsets, pitch);
        const int8_t* a3 = mapper(a, c + 3, offsets, pitch);

        int r = 0;
        svuint64_t acc0_0 = svdup_n_u64(0);
        svuint64_t acc1_0 = svdup_n_u64(0);
        svuint64_t acc2_0 = svdup_n_u64(0);
        svuint64_t acc3_0 = svdup_n_u64(0);

        svuint64_t acc0_1 = svdup_n_u64(0);
        svuint64_t acc1_1 = svdup_n_u64(0);
        svuint64_t acc2_1 = svdup_n_u64(0);
        svuint64_t acc3_1 = svdup_n_u64(0);

        svuint64_t acc0_2 = svdup_n_u64(0);
        svuint64_t acc1_2 = svdup_n_u64(0);
        svuint64_t acc2_2 = svdup_n_u64(0);
        svuint64_t acc3_2 = svdup_n_u64(0);

        svuint64_t acc0_3 = svdup_n_u64(0);
        svuint64_t acc1_3 = svdup_n_u64(0);
        svuint64_t acc2_3 = svdup_n_u64(0);
        svuint64_t acc3_3 = svdup_n_u64(0);

        for (svbool_t pg = svwhilelt_b8(r, length); svptest_any(svptrue_b8(), pg); pg = svwhilelt_b8(r, length)) {
            const svuint8_t q0 = svld1_u8(pg, (const uint8_t*)(query + r));
            const svuint8_t q1 = svld1_u8(pg, (const uint8_t*)(query + r + length));
            const svuint8_t q2 = svld1_u8(pg, (const uint8_t*)(query + r + 2 * length));
            const svuint8_t q3 = svld1_u8(pg, (const uint8_t*)(query + r + 3 * length));

            const svuint8_t v0 = svld1_u8(pg, (const uint8_t*)(a0 + r));
            const svuint8_t v1 = svld1_u8(pg, (const uint8_t*)(a1 + r));
            const svuint8_t v2 = svld1_u8(pg, (const uint8_t*)(a2 + r));
            const svuint8_t v3 = svld1_u8(pg, (const uint8_t*)(a3 + r));

            acc0_0 = svadd_u64_x(svptrue_b64(), acc0_0, svcnt_u64_x(svptrue_b64(), svreinterpret_u64(svand_u8_z(pg, v0, q0))));
            acc1_0 = svadd_u64_x(svptrue_b64(), acc1_0, svcnt_u64_x(svptrue_b64(), svreinterpret_u64(svand_u8_z(pg, v0, q1))));
            acc2_0 = svadd_u64_x(svptrue_b64(), acc2_0, svcnt_u64_x(svptrue_b64(), svreinterpret_u64(svand_u8_z(pg, v0, q2))));
            acc3_0 = svadd_u64_x(svptrue_b64(), acc3_0, svcnt_u64_x(svptrue_b64(), svreinterpret_u64(svand_u8_z(pg, v0, q3))));

            acc0_1 = svadd_u64_x(svptrue_b64(), acc0_1, svcnt_u64_x(svptrue_b64(), svreinterpret_u64(svand_u8_z(pg, v1, q0))));
            acc1_1 = svadd_u64_x(svptrue_b64(), acc1_1, svcnt_u64_x(svptrue_b64(), svreinterpret_u64(svand_u8_z(pg, v1, q1))));
            acc2_1 = svadd_u64_x(svptrue_b64(), acc2_1, svcnt_u64_x(svptrue_b64(), svreinterpret_u64(svand_u8_z(pg, v1, q2))));
            acc3_1 = svadd_u64_x(svptrue_b64(), acc3_1, svcnt_u64_x(svptrue_b64(), svreinterpret_u64(svand_u8_z(pg, v1, q3))));

            acc0_2 = svadd_u64_x(svptrue_b64(), acc0_2, svcnt_u64_x(svptrue_b64(), svreinterpret_u64(svand_u8_z(pg, v2, q0))));
            acc1_2 = svadd_u64_x(svptrue_b64(), acc1_2, svcnt_u64_x(svptrue_b64(), svreinterpret_u64(svand_u8_z(pg, v2, q1))));
            acc2_2 = svadd_u64_x(svptrue_b64(), acc2_2, svcnt_u64_x(svptrue_b64(), svreinterpret_u64(svand_u8_z(pg, v2, q2))));
            acc3_2 = svadd_u64_x(svptrue_b64(), acc3_2, svcnt_u64_x(svptrue_b64(), svreinterpret_u64(svand_u8_z(pg, v2, q3))));

            acc0_3 = svadd_u64_x(svptrue_b64(), acc0_3, svcnt_u64_x(svptrue_b64(), svreinterpret_u64(svand_u8_z(pg, v3, q0))));
            acc1_3 = svadd_u64_x(svptrue_b64(), acc1_3, svcnt_u64_x(svptrue_b64(), svreinterpret_u64(svand_u8_z(pg, v3, q1))));
            acc2_3 = svadd_u64_x(svptrue_b64(), acc2_3, svcnt_u64_x(svptrue_b64(), svreinterpret_u64(svand_u8_z(pg, v3, q2))));
            acc3_3 = svadd_u64_x(svptrue_b64(), acc3_3, svcnt_u64_x(svptrue_b64(), svreinterpret_u64(svand_u8_z(pg, v3, q3))));

            r += svcntb();
        }

        int64_t subRet0_0 = svaddv_u64(svptrue_b64(), acc0_0);
        int64_t subRet1_0 = svaddv_u64(svptrue_b64(), acc1_0);
        int64_t subRet2_0 = svaddv_u64(svptrue_b64(), acc2_0);
        int64_t subRet3_0 = svaddv_u64(svptrue_b64(), acc3_0);

        int64_t subRet0_1 = svaddv_u64(svptrue_b64(), acc0_1);
        int64_t subRet1_1 = svaddv_u64(svptrue_b64(), acc1_1);
        int64_t subRet2_1 = svaddv_u64(svptrue_b64(), acc2_1);
        int64_t subRet3_1 = svaddv_u64(svptrue_b64(), acc3_1);

        int64_t subRet0_2 = svaddv_u64(svptrue_b64(), acc0_2);
        int64_t subRet1_2 = svaddv_u64(svptrue_b64(), acc1_2);
        int64_t subRet2_2 = svaddv_u64(svptrue_b64(), acc2_2);
        int64_t subRet3_2 = svaddv_u64(svptrue_b64(), acc3_2);

        int64_t subRet0_3 = svaddv_u64(svptrue_b64(), acc0_3);
        int64_t subRet1_3 = svaddv_u64(svptrue_b64(), acc1_3);
        int64_t subRet2_3 = svaddv_u64(svptrue_b64(), acc2_3);
        int64_t subRet3_3 = svaddv_u64(svptrue_b64(), acc3_3);

        results[c] = subRet0_0 + (subRet1_0 << 1) + (subRet2_0 << 2) + (subRet3_0 << 3);
        results[c + 1] = subRet0_1 + (subRet1_1 << 1) + (subRet2_1 << 2) + (subRet3_1 << 3);
        results[c + 2] = subRet0_2 + (subRet1_2 << 1) + (subRet2_2 << 2) + (subRet3_2 << 3);
        results[c + 3] = subRet0_3 + (subRet1_3 << 1) + (subRet2_3 << 2) + (subRet3_3 << 3);
    }

    // handle the vectors tail
    for (; c < count; c++) {
        const int8_t* a0 = mapper(a, c, offsets, pitch);
        results[c] = (f32_t)dotd1q4_inner(a0, query, length);
    }
}

EXPORT void vec_dotd1q4_bulk_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t count,
    f32_t* results) {
    dotd1q4_inner_bulk<sequential_mapper>(a, query, length, length, NULL, count, results);
}

EXPORT void vec_dotd1q4_bulk_offsets_2(
    const int8_t* a,
    const int8_t* query,
    const int32_t length,
    const int32_t pitch,
    const int32_t* offsets,
    const int32_t count,
    f32_t* results) {
    dotd1q4_inner_bulk<offsets_mapper>(a, query, length, pitch, offsets, count, results);
}
