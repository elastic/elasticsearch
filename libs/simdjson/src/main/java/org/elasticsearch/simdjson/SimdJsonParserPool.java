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
 * Thread-local pool of {@link SimdJsonBatchParser} and {@link SimdJsonDirectWalker} instances,
 * backed by a shared {@link FrozenFieldNameTable} for cross-thread field name canonicalization.
 *
 * <p>Use {@link #getDefault()} for the standard 256 KiB batch capacity, or
 * {@link #create(int)} for custom sizing. Both return {@code null} when simdjson is not
 * supported on this platform.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 *   SimdJsonParserPool pool = SimdJsonParserPool.getDefault();
 *   if (pool != null) {
 *       SimdJsonBatchParser parser = pool.batchParser();
 *       SimdJsonDirectWalker walker = pool.directWalker();
 *       parser.beginBatch(buffer, totalLen);
 *       for (int i = 0; i < docCount; i++) {
 *           parser.prepareDocumentWindowChunked(offsets[i], lens[i]);
 *           walker.walkDocument(buffer, lens[i], parser, handler);
 *       }
 *       pool.releaseNames();
 *   }
 * }</pre>
 *
 * <p><strong>Thread safety:</strong> the pool itself is thread-safe. Each thread gets its own
 * parser and walker instances via {@link ThreadLocal}. The shared {@link FrozenFieldNameTable}
 * uses lock-free CAS for cross-thread merging.
 */
public final class SimdJsonParserPool {

    private static final Logger logger = LogManager.getLogger(SimdJsonParserPool.class);

    /** Default batch capacity: 256 KiB. */
    private static final int DEFAULT_BATCH_CAPACITY = 256 * 1024;

    private static final SimdJsonParserPool DEFAULT = create(DEFAULT_BATCH_CAPACITY);

    private final FrozenFieldNameTable nameTable = new FrozenFieldNameTable();

    private final ThreadLocal<SimdJsonBatchParser> parsers;
    private final ThreadLocal<SimdJsonDirectWalker> walkers;

    /**
     * Returns the default pool with 256 KiB batch capacity, or {@code null} if simdjson
     * is not supported on this platform.
     */
    @Nullable
    public static SimdJsonParserPool getDefault() {
        return DEFAULT;
    }

    /**
     * Creates a pool with the specified batch capacity, or returns {@code null} if simdjson
     * is not supported on this platform.
     *
     * @param batchCapacity maximum total batch size in bytes for each thread-local parser
     */
    @Nullable
    public static SimdJsonParserPool create(int batchCapacity) {
        if (SimdJsonSupport.isSupported() == false) {
            return null;
        }
        return new SimdJsonParserPool(batchCapacity);
    }

    private SimdJsonParserPool(int batchCapacity) {
        this.batchCapacity = batchCapacity;
        this.parsers = ThreadLocal.withInitial(() -> {
            logger.debug("Thread [{}] creating simdjson batch parser (capacity={})", Thread.currentThread().getName(), batchCapacity);
            return new SimdJsonBatchParser(batchCapacity);
        });
        this.walkers = ThreadLocal.withInitial(() -> new SimdJsonDirectWalker(nameTable.makeChild()));
    }

    /**
     * Returns the thread-local batch parser.
     */
    public SimdJsonBatchParser batchParser() {
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
     * shared name table. Should be called after processing a batch of documents.
     */
    public void releaseNames() {
        walkers.get().releaseNames();
    }
}
