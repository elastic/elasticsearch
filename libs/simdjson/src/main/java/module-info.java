/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Native-accelerated JSON parsing for Elasticsearch columnar source encoding (ESCF).
 *
 * <p>Stage 1 structural indexing runs in {@code libsimdjson} (SIMD-backed C++). Stage 2 is
 * fused with token walking via {@link org.elasticsearch.simdjson.SimdJsonDirectWalker} — no
 * intermediate DOM or tape — streaming field events straight to a {@link
 * org.elasticsearch.simdjson.JsonDocumentHandler}.
 *
 * <h2>Usage</h2>
 *
 * <ol>
 *   <li>Check {@link org.elasticsearch.simdjson.SimdJsonSupport#isSupported()} to
 *       confirm the native library is loaded and the vector API is available.</li>
 *   <li>Obtain a {@link org.elasticsearch.simdjson.SimdJsonParserPool} via
 *       {@link org.elasticsearch.simdjson.SimdJsonParserPool#getDefault()}.</li>
 *   <li>For each document: call {@code stage1} and {@code prepareDocumentWindow} on the
 *       thread-local {@link org.elasticsearch.simdjson.SimdJsonParser}, then
 *       {@code directWalker().walkDocument(buffer, docLen, parser, handler)}.</li>
 *   <li>At partition or batch boundaries, call
 *       {@link org.elasticsearch.simdjson.SimdJsonParserPool#releaseNames()} to merge newly
 *       discovered field names back to the shared cache.</li>
 * </ol>
 *
 * <p>Scalar and string parsing utilities are vendored from
 * <a href="https://github.com/simdjson/simdjson-java">simdjson-java</a> under
 * {@code org.elasticsearch.simdjson.internal.parsers}. Elasticsearch-specific integration
 * (native stage 1, field-name cache, direct walker) lives in the exported API and sibling
 * {@code internal} packages.
 *
 * @see org.elasticsearch.simdjson.SimdJsonParser
 * @see org.elasticsearch.simdjson.SimdJsonDirectWalker
 * @see org.elasticsearch.simdjson.JsonDocumentHandler
 */
module org.elasticsearch.simdjson {
    requires org.elasticsearch.foreign;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.base;

    exports org.elasticsearch.simdjson;
}
