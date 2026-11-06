/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The request circuit breaker of a {@link ContextIndexSearcher} and the {@link LeafExecutionAccounting} charged against it,
 * created on the first charge. Searchers derived with {@link ContextIndexSearcher#withQueryCache} share the instance of the
 * searcher they were derived from, which is the only one that sets the breaker and closes it.
 */
final class ExecutionAccounting implements Releasable {

    private static final Releasable NOOP_RELEASABLE = () -> {};

    private final int leafCount;
    @Nullable
    private volatile CircuitBreaker breaker;
    private final AtomicReference<LeafExecutionAccounting> leaves = new AtomicReference<>();
    /** The bytes charged for the life of the searcher. They are released when this closes. */
    private final AtomicLong searcherBytes = new AtomicLong();

    ExecutionAccounting(int leafCount) {
        this.leafCount = leafCount;
    }

    @Nullable
    CircuitBreaker breaker() {
        return breaker;
    }

    /** Replaces the breaker, releasing everything charged to the previous one. */
    void setBreaker(@Nullable CircuitBreaker breaker) {
        close();
        this.breaker = breaker;
    }

    /** Creates the per-leaf accounting so that leaves are released from now on. No-op without a breaker. */
    void ensureCreated() {
        getOrCreate();
    }

    void charge(LeafReaderContext ctx, long bytes, String label) {
        LeafExecutionAccounting accounting = getOrCreate();
        if (accounting != null) {
            accounting.charge(ctx, bytes, label);
        }
    }

    /**
     * Charges {@code bytes} until this closes, independent of the leaf. When the breaker trips, nothing is recorded. Does
     * nothing without a breaker.
     */
    void chargeSearcher(long bytes, String label) {
        final CircuitBreaker current = breaker;
        if (current == null) {
            return;
        }
        current.addEstimateBytesAndMaybeBreak(bytes, label);
        searcherBytes.addAndGet(bytes);
    }

    /** Test-only */
    long searcherBytes() {
        return searcherBytes.get();
    }

    Releasable enterLeaf(LeafReaderContext ctx) {
        LeafExecutionAccounting accounting = leaves.get();
        return accounting == null ? NOOP_RELEASABLE : accounting.enterLeaf(ctx);
    }

    /** Test-only */
    boolean hasLeafAccounting() {
        return leaves.get() != null;
    }

    @Nullable
    private LeafExecutionAccounting getOrCreate() {
        LeafExecutionAccounting existing = leaves.get();
        if (existing != null) {
            return existing;
        }
        CircuitBreaker current = breaker;
        if (current == null) {
            return null;
        }
        LeafExecutionAccounting created = new LeafExecutionAccounting(current, leafCount);
        return leaves.compareAndSet(null, created) ? created : leaves.get();
    }

    @Override
    public void close() {
        LeafExecutionAccounting accounting = leaves.getAndSet(null);
        if (accounting != null) {
            accounting.close();
        }
        final long charged = searcherBytes.getAndSet(0);
        final CircuitBreaker current = breaker;
        if (charged > 0 && current != null) {
            current.addWithoutBreaking(-charged);
        }
    }
}
