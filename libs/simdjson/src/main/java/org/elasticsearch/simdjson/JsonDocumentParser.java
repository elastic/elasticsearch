/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

/**
 * A single-document JSON parser obtained from {@link SimdJsonParserPool#forCurrentThread()}.
 * Bundles the native stage 1 indexer and the walker that emits events to a
 * {@link JsonDocumentHandler}, so callers never have to sequence those two themselves.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 *   JsonDocumentParser docParser = pool.forCurrentThread();
 *   for (Doc doc : docs) {
 *       if (doc.length() <= docParser.maxDocumentBytes()) {
 *           docParser.parseDocument(doc.buffer(), doc.offset(), doc.length(), handler);
 *       }
 *   }
 *   docParser.publishFieldNames();
 * }</pre>
 *
 * <p>Instances are owned by the pool, not the caller: there is nothing to close and one instance
 * is shared by everything that parses on its thread. A reference may be held for the duration of a
 * thread-confined unit of work, but must not escape to another thread.
 *
 * <p><strong>Not thread-safe.</strong>
 */
public final class JsonDocumentParser {

    private final int maxDocumentBytes;
    private final SimdJsonParser parser;
    private final SimdJsonDirectWalker walker;

    /** The thread this parser was created for, used only to assert confinement. */
    private final Thread owner;

    JsonDocumentParser(int maxDocumentBytes, SimdJsonParser parser, SimdJsonDirectWalker walker) {
        this.maxDocumentBytes = maxDocumentBytes;
        this.parser = parser;
        this.walker = walker;
        this.owner = Thread.currentThread();
    }

    /**
     * The largest document, in bytes, that {@link #parseDocument} accepts. Callers that have a
     * fallback for oversized documents should test against this rather than call and catch.
     */
    public int maxDocumentBytes() {
        return maxDocumentBytes;
    }

    /**
     * Indexes and walks one JSON document, emitting events to {@code handler}.
     *
     * <p>On {@link JsonParsingException} the handler may already have received events for a prefix
     * of the document. Callers must be able to discard whatever the handler accumulated; there is
     * no rollback.
     *
     * @param buffer  document bytes (need not start at index 0, needs no trailing padding)
     * @param offset  start of the document within {@code buffer}
     * @param len     document length in bytes, at most {@link #maxDocumentBytes()}
     * @param handler receives the parsed JSON events
     * @throws JsonParsingException     if the document is malformed
     * @throws IllegalArgumentException if {@code len} exceeds {@link #maxDocumentBytes()}
     */
    public void parseDocument(byte[] buffer, int offset, int len, JsonDocumentHandler handler) {
        assert assertThread();
        checkLength(len);
        parser.stage1(buffer, offset, len);
        parser.prepareDocumentWindow(offset, len);
        walker.walkDocument(buffer, parser, handler);
    }

    /**
     * Like {@link #parseDocument(byte[], int, int, JsonDocumentHandler)} when the document occupies
     * {@code buffer[0..len)}.
     */
    public void parseDocument(byte[] buffer, int len, JsonDocumentHandler handler) {
        parseDocument(buffer, 0, len, handler);
    }

    /**
     * Publishes the field names this parser has learned to the pool's shared table, making them
     * available to parsers on other threads, and adopts the shared table if another parser got
     * there first.
     *
     * <p>Call at a batch or partition boundary. Omitting it is safe but means names learned here
     * stay local to this thread. Calling it more than once is harmless.
     */
    public void publishFieldNames() {
        walker.releaseNames();
    }

    private void checkLength(int len) {
        if (len > maxDocumentBytes) {
            throw new IllegalArgumentException("document length [" + len + "] exceeds maximum [" + maxDocumentBytes + "]");
        }
    }

    private boolean assertThread() {
        assert Thread.currentThread() == owner
            : "parser for thread [" + owner.getName() + "] used from thread [" + Thread.currentThread().getName() + "]";
        return true;
    }
}
