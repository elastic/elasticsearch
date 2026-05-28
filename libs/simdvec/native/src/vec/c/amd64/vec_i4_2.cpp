/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// AVX-512/VNNI int4 packed-nibble vector operations. Thin EXPORT thunks
// over the BITS-templated kernels in amd64_packed_int_avx512.h.
// See amd64_packed_int_traits.h for the packed layout.
//
// `batches` is tuned per export: BULK uses the most conservative count to
// avoid L1D set aliasing on the contiguous layout; OFFSETS and SPARSE scatter
// their streams across memory, so a higher count further hides vpdpbusd
// latency without aliasing risk.

#include "amd64/amd64_packed_int_avx512.h"

EXPORT int32_t vec_doti4_2(const int8_t* query, const int8_t* doc, int32_t packed_len) {
    return doti_inner_avx512<4>(query, doc, packed_len);
}

EXPORT void vec_doti4_bulk_2(const int8_t* docs, const int8_t* query, int32_t packed_len, int32_t count, f32_t* results) {
    doti_bulk_impl_avx512<4, int8_t, sequential_mapper, 2>(docs, query, packed_len, packed_len, NULL, count, results);
}

EXPORT void vec_doti4_bulk_offsets_2(
    const int8_t* docs,
    const int8_t* query,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    doti_bulk_impl_avx512<4, int8_t, offsets_mapper, 4>(docs, query, packed_len, pitch, offsets, count, results);
}

EXPORT void vec_doti4_bulk_sparse_2(
    const void* const* addresses,
    const int8_t* query,
    int32_t packed_len,
    int32_t count,
    f32_t* results
) {
    doti_bulk_impl_avx512<4, const int8_t*, sparse_mapper, 4>(
        (const int8_t* const*)addresses, query, packed_len, 0, NULL, count, results);
}
