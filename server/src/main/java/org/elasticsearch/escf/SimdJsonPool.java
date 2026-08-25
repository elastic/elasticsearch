/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.simdjson.SimdJsonDirectWalker;
import org.elasticsearch.simdjson.SimdJsonParser;
import org.elasticsearch.simdjson.SimdJsonParserPool;

/**
 * ESCF-specific facade over {@link SimdJsonParserPool}. Adds the document size threshold
 * and scratch buffer that are specific to the ESCF encoding path.
 *
 * <p>Delegates thread-local parser/walker management and field name sharing to the
 * default {@link SimdJsonParserPool} singleton.
 *
 * <p>Simdjson ESCF encoding is gated by {@link #SIMDJSON_ESCF_FEATURE_FLAG}. In snapshot
 * builds the flag defaults to enabled; in release builds it defaults to disabled and can
 * be turned on with {@code -Des.simdjson_escf_feature_flag_enabled=true}.
 */
final class SimdJsonPool {

    /**
     * Feature flag for the simdjson-backed ESCF JSON encode path. Disabled by default in
     * release builds.
     */
    static final FeatureFlag SIMDJSON_ESCF_FEATURE_FLAG = new FeatureFlag("simdjson_escf");

    /** Documents larger than this threshold are handled by the Jackson parser. */
    static final int MAX_DOC_BYTES = 16 * 1024;

    private static final SimdJsonParserPool POOL = SimdJsonParserPool.getDefault();

    /** Whether the native simdjson library is loaded and ready. */
    static final boolean AVAILABLE = POOL != null;

    /**
     * Returns {@code true} when simdjson ESCF encoding may be used: the native library is
     * loaded and {@link #SIMDJSON_ESCF_FEATURE_FLAG} is enabled.
     */
    static boolean isEnabled() {
        return AVAILABLE && SIMDJSON_ESCF_FEATURE_FLAG.isEnabled();
    }

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

    /** Returns the thread-local parser. Only call when {@link #AVAILABLE} is true. */
    static SimdJsonParser parser() {
        return POOL.parser();
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
