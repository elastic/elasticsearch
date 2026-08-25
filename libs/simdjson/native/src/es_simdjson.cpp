/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/*
 * Thin C-linkage wrapper around simdjson's stage 1 (structural indexing + UTF-8
 * validation). The caller allocates a context once per thread, runs stage 1 on
 * each buffer, then reads the resulting uint32_t structural index array.
 *
 * simdjson auto-selects the best SIMD backend (AVX-512, AVX2, SSE4.2, NEON)
 * at runtime via its implementation-selection machinery.
 *
 * Pinned to simdjson v4.6.5 (amalgamated single-header distribution).
 */

#include "simdjson.h"

#include <cstdint>
#include <cstdlib>
#include <memory>

using namespace simdjson;

struct es_stage1_ctx {
    std::unique_ptr<internal::dom_parser_implementation> impl;
};

extern "C" {

/*
 * Allocates a reusable stage 1 context sized for buffers up to `capacity`
 * bytes. Returns nullptr on allocation failure.
 */
es_stage1_ctx* es_stage1_create(uint32_t capacity) {
    auto ctx = new (std::nothrow) es_stage1_ctx();
    if (!ctx) return nullptr;

    auto err = get_active_implementation()->create_dom_parser_implementation(
        capacity, 64, ctx->impl);
    if (err) {
        delete ctx;
        return nullptr;
    }
    return ctx;
}

/*
 * Frees the context. Safe to call with nullptr.
 */
void es_stage1_destroy(es_stage1_ctx* ctx) {
    delete ctx;
}

/*
 * Runs stage 1 over buf[offset..offset+len) and writes structural indices into
 * out_buf. Adds `offset` to each index so outputs are absolute positions within
 * the original buffer. Stage 1 copies its remainder block to a stack-local
 * buffer, so no readable padding past offset+len is required.
 */
int es_stage1_run(es_stage1_ctx* ctx,
              const uint8_t* buf, uint32_t offset, uint32_t len,
              int32_t* out_buf, uint32_t out_buf_capacity,
              uint32_t* out_count) {
    if (!ctx || !ctx->impl) return -1;

    if (len > ctx->impl->capacity()) {
        auto err = ctx->impl->set_capacity(len);
        if (err) return static_cast<int>(err);
    }

    auto err = ctx->impl->stage1(buf + offset, len, stage1_mode::regular);
    if (err) return static_cast<int>(err);

    uint32_t n = ctx->impl->n_structural_indexes;
    if (n > out_buf_capacity) return -2;

    const uint32_t* src = ctx->impl->structural_indexes.get();
    if (offset == 0) {
        __builtin_memcpy(out_buf, src, n * sizeof(uint32_t));
    } else {
        for (uint32_t i = 0; i < n; i++) {
            out_buf[i] = static_cast<int32_t>(src[i] + offset);
        }
    }
    *out_count = n;
    return 0;
}

/*
 * Returns a human-readable error message for the given error code returned by
 * es_stage1_run. Returns a static string — the caller must not free it.
 * Unknown codes yield "UNEXPECTED_ERROR".
 */
const char* es_stage1_error_message(int err) {
    if (err == -1) return "null or invalid context";
    if (err == -2) return "output buffer too small";
    if (err >= 0 && err < static_cast<int>(error_code::NUM_ERROR_CODES)) {
        return error_message(static_cast<error_code>(err));
    }
    return "UNEXPECTED_ERROR";
}

} /* extern "C" */
