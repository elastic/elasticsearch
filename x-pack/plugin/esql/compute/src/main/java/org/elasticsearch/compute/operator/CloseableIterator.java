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
     * Returns a listener that completes when iterator state has changed such that
     * {@link #tryAdvance()} may return an element, or {@link #isExhausted()} may have become true.
     * A done listener is not EOF: it means state changed, not that the iterator is drained.
     * The default completes immediately. Drain loops must not call {@link #hasNext()}:
     * {@code hasNext()} may start and await the next GET even after this listener was done.
     */
    default SubscribableListener<Void> waitForReady() {
        return SubscribableListener.newSucceeded(null);
    }

    /**
     * Non-blocking single-step advance: returns the next element, or {@code null} if none is
     * immediately available (either because upstream is still producing or because the iterator
     * is exhausted). Callers distinguish "not yet" from "EOF" via {@link #isExhausted()} after
     * a done {@link #waitForReady()}. A done {@code waitForReady()} means state changed, not EOF;
     * a non-done listener means more data may arrive.
     *
     * <p>A page may arrive in the race between {@code tryAdvance()} returning null and the
     * recheck of {@code waitForReady()}. Drain loops retry {@code tryAdvance()} while
     * {@code waitForReady()} is done and {@link #isExhausted()} is false. They must not call
     * {@link #hasNext()}: {@code hasNext()} may start and await the next unit of I/O even after
     * {@code waitForReady()} was done.
     *
     * <p>The default delegates to {@link #hasNext()}/{@link #next()} and is therefore blocking
     * for iterators whose {@code hasNext()} blocks. Async iterators should override this to
     * guarantee a non-blocking return so the caller (typically an executor-bound producer loop)
     * can yield its thread instead of pinning it.
     */
    default T tryAdvance() {
        return hasNext() ? next() : null;
    }

    /**
     * True when a null {@link #tryAdvance()} after a done {@link #waitForReady()} is terminal EOF.
     * Call only in that window, and only from the consumer thread — same as {@link #tryAdvance()}.
     * Some implementations mutate consumer-only state (for example skipping POISON markers).
     * The default is {@code true}: synchronous iterators already used {@link #hasNext()} inside
     * {@link #tryAdvance()}, so a null return is EOF. Async iterators must override this. Do not
     * implement as {@code hasNext() == false}: wrappers would start I/O. Every {@link #waitForReady()}
     * override must override this; a forgotten override is silent EOF at the outer wrapper.
     */
    default boolean isExhausted() {
        return true;
    }

    /**
     * Called by the producer drain when it parks on downstream buffer space. The default is a
     * no-op. Drop look-ahead I/O here. Do not drop the current group's overshoot slot while its
     * bytes are still charged: that would let a second unit overshoot {@code used}. The slot
     * stays until the current group releases (M1). Waiters are tickets, not parked workers, so
     * keeping the owner is not a deadlock — the {@code waitForSpace} consumer is the driver.
     * This is not {@link #waitForReady()}: readiness parks on upstream I/O, this hook fires on
     * downstream backpressure.
     */
    default void revokeOvershootOnPark() {}
}
