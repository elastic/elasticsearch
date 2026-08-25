/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * JSON parsing support library adapted from
 * <a href="https://github.com/simdjson/simdjson-java">simdjson-java</a>.
 *
 * <p>Provides native-accelerated structural indexing (backed by the {@code libes_simdjson}
 * C++ library), a fused stage 2 + token walk via
 * {@link org.elasticsearch.simdjson.SimdJsonDirectWalker}, and field name canonicalization
 * via {@link org.elasticsearch.simdjson.fieldnames.FrozenFieldNameTable}.
 *
 * <h2>Usage</h2>
 *
 * <ol>
 *   <li>Check {@link org.elasticsearch.simdjson.SimdJsonSupport#isSupported()} to
 *       confirm the native library is loaded and the vector API is available.</li>
 *   <li>Create a {@link org.elasticsearch.simdjson.SimdJsonBatchParser} (one per thread,
 *       owns a native structural indexer, implements {@link java.lang.AutoCloseable}).</li>
 *   <li>Create a {@link org.elasticsearch.simdjson.SimdJsonDirectWalker} with a
 *       {@link org.elasticsearch.simdjson.fieldnames.FieldNameLookup} (typically a
 *       {@link org.elasticsearch.simdjson.fieldnames.FrozenFieldNameTable.Child}).</li>
 *   <li>For each batch: call {@code beginBatch}, then for each document call
 *       {@code prepareDocumentWindowChunked} followed by
 *       {@code walker.walkDocument(buffer, docLen, batchParser, handler)}.</li>
 *   <li>After each batch, call {@code walker.releaseNames()} to merge new field names
 *       back to the shared parent table.</li>
 * </ol>
 *
 * @see org.elasticsearch.simdjson.SimdJsonBatchParser
 * @see org.elasticsearch.simdjson.SimdJsonDirectWalker
 * @see org.elasticsearch.simdjson.JsonDocumentHandler
 */
module org.elasticsearch.simdjson {
    requires org.elasticsearch.foreign;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.base;

    exports org.elasticsearch.simdjson;
    exports org.elasticsearch.simdjson.fieldnames;
}
