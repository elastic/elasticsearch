/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdjson.internal.fieldnames.FrozenFieldNameTable;

/**
 * Thread-local pool of {@link SimdJsonParser} and {@link SimdJsonDirectWalker} instances,
 * backed by a shared {@link FrozenFieldNameTable} for cross-thread field name canonicalization.
 *
 * <p>{@link #getDefault()} returns {@code null} when simdjson is not supported on this platform.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 *   SimdJsonParserPool pool = SimdJsonParserPool.getDefault();
 *   if (pool != null) {
 *       SimdJsonParser parser = pool.parser();
 *       SimdJsonDirectWalker walker = pool.directWalker();
 *       parser.stage1(buffer, offset, docLen);
 *       parser.prepareDocumentWindow(offset, docLen);
 *       walker.walkDocument(buffer, docLen, parser, handler);
 *       pool.releaseNames(); // after a partition or other merge boundary
 *   }
 * }</pre>
 *
 * <p><strong>Thread safety:</strong> the pool itself is thread-safe. Each thread gets its own
 * parser and walker instances via {@link ThreadLocal}. The shared {@link FrozenFieldNameTable}
 * uses lock-free CAS for cross-thread merging.
 */
public final class SimdJsonParserPool {

    private static final Logger logger = LogManager.getLogger(SimdJsonParserPool.class);

    /**
     * Maximum document size the thread-local parser is sized for. Matches the ESCF single-doc
     * limit ({@code 16 KiB}); documents larger than this must use another parser path.
     */
    static final int PARSER_CAPACITY = 16 * 1024;

    private static final SimdJsonParserPool DEFAULT = SimdJsonSupport.isSupported() ? new SimdJsonParserPool() : null;

    private final FrozenFieldNameTable nameTable = new FrozenFieldNameTable();

    private final ThreadLocal<SimdJsonParser> parsers;
    private final ThreadLocal<SimdJsonDirectWalker> walkers;

    /**
     * Returns the default pool, or {@code null} if simdjson is not supported on this platform.
     */
    @Nullable
    public static SimdJsonParserPool getDefault() {
        return DEFAULT;
    }

    private SimdJsonParserPool() {
        this.parsers = ThreadLocal.withInitial(() -> {
            logger.debug("Thread [{}] creating simdjson parser (capacity={})", Thread.currentThread().getName(), PARSER_CAPACITY);
            return new SimdJsonParser(PARSER_CAPACITY);
        });
        this.walkers = ThreadLocal.withInitial(() -> new SimdJsonDirectWalker(nameTable.makeChild()));
    }

    /**
     * Returns the thread-local batch parser.
     */
    public SimdJsonParser parser() {
        return parsers.get();
    }

    /**
     * Returns the thread-local direct walker.
     */
    public SimdJsonDirectWalker directWalker() {
        return walkers.get();
    }

    /**
     * Merges any newly discovered field names from the current thread's walker back to the
     * shared name table. Call at partition or batch boundaries so other threads can reuse
     * discovered field names.
     */
    public void releaseNames() {
        walkers.get().releaseNames();
    }
}
