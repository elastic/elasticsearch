/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.support.SubscribableListener;

import java.io.Closeable;
import java.util.Iterator;

/**
 * An {@link Iterator} with state that must be {@link #close() closed}.
 *
 * <p>Iterators may optionally expose an async-ready signal via {@link #waitForReady()}. The default
 * returns an immediately-completed listener — for synchronous iterators, {@link #hasNext()} can
 * always be called without blocking on upstream production. Iterators whose {@code hasNext()} would
 * otherwise spin or block (e.g. waiting on parser threads, network I/O) should override this so
 * the consumer can yield the calling thread back to its executor and resume when work is available.
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable {

    /**
     * Returns a listener that completes when {@link #hasNext()} can be called without blocking on
     * upstream production. The default — appropriate for synchronous iterators — completes immediately.
     */
    default SubscribableListener<Void> waitForReady() {
        return SubscribableListener.newSucceeded(null);
    }

    /**
     * Non-blocking single-step advance: returns the next element, or {@code null} if none is
     * immediately available (either because upstream is still producing or because the iterator
     * is exhausted). Callers distinguish "not yet" from "EOF" via {@link #waitForReady()}: a
     * {@code null} return paired with an immediately-done {@code waitForReady()} means EOF;
     * a {@code null} paired with a non-done listener means more data may arrive.
     *
     * <p>The default delegates to {@link #hasNext()}/{@link #next()} and is therefore blocking
     * for iterators whose {@code hasNext()} blocks. Async iterators should override this to
     * guarantee a non-blocking return so the caller (typically an executor-bound producer loop)
     * can yield its thread instead of pinning it.
     */
    default T tryAdvance() {
        return hasNext() ? next() : null;
    }
}
