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
}
