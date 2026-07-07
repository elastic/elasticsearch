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
 *
 * <p>The non-blocking drain contract is three methods: {@link #waitForReady()} (when to retry),
 * {@link #pollNext()} (pull a page if one is available <em>now</em>, never blocking), and
 * {@link #isExhausted()} (the single terminal predicate). A consumer that never wants to pin its
 * executor thread polls with {@link #pollNext()} and, on a {@code null} return, concludes EOF
 * <em>only</em> when {@link #isExhausted()} is {@code true} — never on {@code pollNext()==null}
 * paired with a done {@link #waitForReady()}, since a done ready-signal can mean an internal
 * end-of-chunk marker rather than a page. For synchronous iterators the defaults reduce to the
 * ordinary {@link #hasNext()}/{@link #next()} contract.
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
     * Non-blocking pull: returns the next element if one is available <em>right now</em>, or {@code null}
     * if none is currently available. Must never block on upstream production (parser threads, network I/O).
     * A {@code null} return is <em>not</em> an EOF signal — it can mean "no page yet, more coming"; the caller
     * decides between retry (via {@link #waitForReady()}) and termination (via {@link #isExhausted()}).
     * The default — appropriate for synchronous iterators whose {@link #hasNext()} cannot block — is
     * {@code hasNext() ? next() : null}. Async iterators (parser-thread backed) must override so the
     * consumer never pins its executor thread.
     */
    default T pollNext() {
        return hasNext() ? next() : null;
    }

    /**
     * The terminal predicate: {@code true} only when the iterator is genuinely drained <em>and</em> no
     * further element will ever be produced. Must never return {@code true} while a page is still in
     * flight or a producer is still running. This is the ONLY signal on which a non-blocking drain may
     * conclude EOF; concluding EOF on {@code pollNext()==null && waitForReady().isDone()} would silently
     * drop elements at a genuine mid-stream gap. The default — appropriate for synchronous iterators — is
     * {@code hasNext() == false}.
     */
    default boolean isExhausted() {
        return hasNext() == false;
    }
}
