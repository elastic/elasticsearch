/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// AVX2-vectorized int4 packed-nibble vector operations. Thin EXPORT thunks
// over the BITS-templated kernels in amd64_packed_int_avx2.h.
// See amd64_packed_int_traits.h for the packed layout.

#include "amd64/amd64_packed_int_avx2.h"

EXPORT int32_t vec_doti4(const int8_t* query, const int8_t* doc, int32_t packed_len) {
    return doti_inner_avx2<4>(query, doc, packed_len);
}

EXPORT void vec_doti4_bulk(const int8_t* docs, const int8_t* query, int32_t packed_len, int32_t count, f32_t* results) {
    doti_bulk_impl_avx2<4, int8_t, sequential_mapper>(docs, query, packed_len, packed_len, NULL, count, results);
}

EXPORT void vec_doti4_bulk_offsets(
    const int8_t* docs,
    const int8_t* query,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    doti_bulk_impl_avx2<4, int8_t, offsets_mapper>(docs, query, packed_len, pitch, offsets, count, results);
}

EXPORT void vec_doti4_bulk_sparse(
    const void* const* addresses,
    const int8_t* query,
    int32_t packed_len,
    int32_t count,
    f32_t* results
) {
    doti_bulk_impl_avx2<4, const int8_t*, sparse_mapper>(
        (const int8_t* const*)addresses, query, packed_len, 0, NULL, count, results);
}
