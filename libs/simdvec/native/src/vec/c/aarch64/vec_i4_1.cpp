/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// Scalar implementations for int4 packed-nibble vector operations.
// The "unpacked" vector has 2*packed_len bytes (high nibbles in [0..packed_len),
// low nibbles in [packed_len..2*packed_len)). The "packed" vector has packed_len
// bytes, each holding two 4-bit values.

#include <stdint.h>
#include "vec.h"
#include "vec_common.h"

static inline int32_t doti4_inner(const int8_t* unpacked, const uint8_t* packed, int32_t packed_len) {
    int32_t total = 0;
    for (int32_t i = 0; i < packed_len; i++) {
        uint8_t p = packed[i];
        total += (p >> 4) * unpacked[i];
        total += (p & 0x0F) * unpacked[i + packed_len];
    }
    return total;
}

EXPORT int32_t vec_doti4(const int8_t* unpacked, const uint8_t* packed, int32_t packed_len) {
    return doti4_inner(unpacked, packed, packed_len);
}

EXPORT void vec_doti4_bulk(const uint8_t* a, const int8_t* b, int32_t packed_len, int32_t count, f32_t* results) {
    for (int c = 0; c < count; c++) {
        results[c] = (f32_t)doti4_inner(b, a + (int64_t)c * packed_len, packed_len);
    }
}

EXPORT void vec_doti4_bulk_offsets(
    const uint8_t* a,
    const int8_t* b,
    int32_t packed_len,
    int32_t pitch,
    const int32_t* offsets,
    int32_t count,
    f32_t* results
) {
    for (int c = 0; c < count; c++) {
        const uint8_t* doc = a + (int64_t)offsets[c] * pitch;
        results[c] = (f32_t)doti4_inner(b, doc, packed_len);
    }
}
