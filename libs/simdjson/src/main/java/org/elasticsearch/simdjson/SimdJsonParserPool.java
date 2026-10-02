/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdjson.internal.fieldnames.FrozenFieldNameTable;

/**
 * Thread-local pool of {@link JsonDocumentParser}s sharing one {@link FrozenFieldNameTable}, so
 * that field names learned on one thread can be reused by parsers on other threads.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 *   if (SimdJsonSupport.isSupported()) {
 *       JsonDocumentParser docParser = SimdJsonParserPool.getDefault().forCurrentThread();
 *       docParser.parseDocument(buffer, offset, len, handler);
 *       docParser.publishFieldNames(); // at a batch boundary
 *   }
 * }</pre>
 *
 * <p>Parsers are keyed by thread rather than leased, so the number of native stage 1 contexts a
 * pool creates is bounded by the number of threads that ever parse a document — it does not scale
 * with how many units of work are in flight. A parser is never handed back; it lives as long as
 * its thread. Nothing closes the underlying native context, so
 * {@link org.elasticsearch.simdjson.internal.StructuralIndexer} relies on its cleaner to free it
 * if the thread goes away.
 *
 * <p><strong>Thread safety:</strong> the pool is thread-safe. The parser it returns is confined to
 * the calling thread and must not be shared with another.
 */
public final class SimdJsonParserPool {

    private static final Logger logger = LogManager.getLogger(SimdJsonParserPool.class);

    private static final SimdJsonParserPool DEFAULT = new SimdJsonParserPool(SimdJsonSupport.maxDocBytes());

    private final int maxDocumentBytes;
    private final FrozenFieldNameTable nameTable = new FrozenFieldNameTable();
    private final ThreadLocal<JsonDocumentParser> parsers;

    /**
     * The pool used by production code, sized from {@link SimdJsonSupport#maxDocBytes()}.
     *
     * <p>Non-null regardless of platform support; {@link #forCurrentThread()} is what fails when
     * simdjson is unavailable. Guard with {@link SimdJsonSupport#isSupported()}.
     */
    public static SimdJsonParserPool getDefault() {
        return DEFAULT;
    }

    /**
     * Creates an independent pool with its own field name table. Mainly for tests and benchmarks
     * that need a different document size limit or an isolated name cache.
     *
     * @param maxDocumentBytes largest document parsers from this pool will accept
     */
    public SimdJsonParserPool(int maxDocumentBytes) {
        if (maxDocumentBytes <= 0) {
            throw new IllegalArgumentException("maxDocumentBytes must be positive but was [" + maxDocumentBytes + "]");
        }
        this.maxDocumentBytes = maxDocumentBytes;
        this.parsers = ThreadLocal.withInitial(() -> {
            logger.debug("Thread [{}] creating simdjson parser (capacity={})", Thread.currentThread().getName(), maxDocumentBytes);
            // A fresh child snapshots whatever the shared table holds now, so a parser created
            // after another has published its names starts out with those names already resolved.
            return new JsonDocumentParser(
                maxDocumentBytes,
                new SimdJsonParser(maxDocumentBytes),
                new SimdJsonDirectWalker(nameTable.makeChild())
            );
        });
    }

    /** The largest document, in bytes, that parsers from this pool accept. */
    public int maxDocumentBytes() {
        return maxDocumentBytes;
    }

    /**
     * Returns this thread's parser, creating it on first call.
     *
     * <p>The parser is owned by the pool and shared by everything that parses on this thread. It is
     * safe to hold a reference for the duration of a thread-confined unit of work, but it must not
     * be handed to another thread.
     *
     * @throws IllegalStateException if simdjson is not supported on this JDK/platform
     */
    public JsonDocumentParser forCurrentThread() {
        return parsers.get();
    }
}
