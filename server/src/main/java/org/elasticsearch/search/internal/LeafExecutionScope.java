/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.core.CheckedSupplier;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Owns the execution memory that the weights of a {@link ContextIndexSearcher} charge to the request circuit
 * breaker while scorers are built. A caller that scores leaves without going through
 * {@link ContextIndexSearcher#searchLeaf} builds its scorers inside {@link #capture} and calls {@link #release}
 * when it drops them. Several such callers can score the same leaf at once, each with its own scope.
 * <p>
 * The scope is bound to the calling thread only while {@link #capture} runs. {@link #release} can be called from
 * any thread.
 */
public final class LeafExecutionScope {

    private static final ThreadLocal<LeafExecutionScope> CURRENT = new ThreadLocal<>();

    private final AtomicLong bytes = new AtomicLong();
    private volatile PointRangeExecutionAccounting accounting;

    /** Runs {@code build} and takes ownership of the execution memory charged on the calling thread while it runs. */
    public <T, E extends Exception> T capture(CheckedSupplier<T, E> build) throws E {
        final LeafExecutionScope previous = CURRENT.get();
        CURRENT.set(this);
        try {
            return build.get();
        } finally {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        }
    }

    /** Releases the execution memory this scope owns. The scope can be used again afterwards. */
    public void release() {
        final PointRangeExecutionAccounting owner = accounting;
        final long held = bytes.getAndSet(0L);
        accounting = null;
        if (owner != null && held > 0L) {
            owner.releaseScoped(held);
        }
    }

    static LeafExecutionScope current() {
        return CURRENT.get();
    }

    /** Takes ownership of {@code charged} bytes, unless this scope already owns memory charged to another accounting. */
    boolean add(PointRangeExecutionAccounting from, long charged) {
        final PointRangeExecutionAccounting owner = accounting;
        if (owner == null) {
            accounting = from;
        } else if (owner != from) {
            return false;
        }
        bytes.addAndGet(charged);
        return true;
    }
}
