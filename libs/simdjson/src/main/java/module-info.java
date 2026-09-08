/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Native-accelerated JSON parsing.
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
 *   <li>Obtain this thread's {@link org.elasticsearch.simdjson.JsonDocumentParser} from
 *       {@link org.elasticsearch.simdjson.SimdJsonParserPool#forCurrentThread()}.</li>
 *   <li>For each document no larger than
 *       {@link org.elasticsearch.simdjson.JsonDocumentParser#maxDocumentBytes()}, call
 *       {@link org.elasticsearch.simdjson.JsonDocumentParser#parseDocument}.</li>
 *   <li>At a batch or partition boundary, call
 *       {@link org.elasticsearch.simdjson.JsonDocumentParser#publishFieldNames()} so other threads
 *       can reuse the field names this parser learned.</li>
 * </ol>
 *
 * <p>Scalar and string parsing utilities are vendored from
 * <a href="https://github.com/simdjson/simdjson-java">simdjson-java</a> under
 * {@code org.elasticsearch.simdjson.internal.parsers}. Elasticsearch-specific integration
 * (native stage 1, field-name cache, direct walker) lives in the exported API and sibling
 * {@code internal} packages.
 *
 * @see org.elasticsearch.simdjson.JsonDocumentParser
 * @see org.elasticsearch.simdjson.JsonDocumentHandler
 * @see org.elasticsearch.simdjson.SimdJsonParser
 * @see org.elasticsearch.simdjson.SimdJsonDirectWalker
 */
module org.elasticsearch.simdjson {
    requires org.elasticsearch.foreign;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.base;

    exports org.elasticsearch.simdjson;
}
