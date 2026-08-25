/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.elasticsearch.simdjson.SimdJsonBatchParser;
import org.elasticsearch.simdjson.SimdJsonDirectWalker;
import org.elasticsearch.simdjson.SimdJsonParserPool;

/**
 * ESCF-specific facade over {@link SimdJsonParserPool}. Adds the document size threshold
 * and scratch buffer that are specific to the ESCF encoding path.
 *
 * <p>Delegates thread-local parser/walker management and field name sharing to the
 * default {@link SimdJsonParserPool} singleton.
 */
final class SimdJsonPool {

    /** Documents larger than this threshold are handled by the Jackson parser. */
    static final int MAX_DOC_BYTES = 16 * 1024;

    private static final SimdJsonParserPool POOL = SimdJsonParserPool.getDefault();

    /** Whether the native simdjson library is loaded and ready. */
    static final boolean AVAILABLE = POOL != null;

    /**
     * Scratch buffer of {@code MAX_DOC_BYTES} bytes. Used by the single-doc path to copy a
     * non-zero-offset {@link org.elasticsearch.common.bytes.BytesReference} into a
     * zero-offset array. No trailing padding is needed — all parser/walker code paths
     * have scalar tail fallbacks that stay within buffer bounds.
     */
    private static final ThreadLocal<byte[]> SCRATCH = ThreadLocal.withInitial(() -> new byte[MAX_DOC_BYTES]);

    private SimdJsonPool() {}

    /**
     * Returns the thread-local scratch buffer of length {@code MAX_DOC_BYTES}.
     */
    static byte[] scratch() {
        return SCRATCH.get();
    }

    /** Returns the thread-local batch parser. Only call when {@link #AVAILABLE} is true. */
    static SimdJsonBatchParser batchParser() {
        return POOL.batchParser();
    }

    /** Returns the thread-local direct walker. Only call when {@link #AVAILABLE} is true. */
    static SimdJsonDirectWalker directWalker() {
        return POOL.directWalker();
    }

    /** Merges field names back to the shared table. */
    static void releaseNames() {
        POOL.releaseNames();
    }
}
