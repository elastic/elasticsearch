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
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.LongConsumer;

/**
 * Tracks execution RAM charged to a single request's circuit breaker. One instance is owned by a single
 * {@link ContextIndexSearcher} and shared by every leaf-execution charge source registered with it (currently
 * {@link ExecutionBreakerWeight} and {@link QueryCacheBreakerWeight}, each charge under its own label).
 * A charge belongs to the {@link LeafExecutionScope} bound to the charging thread, which releases it.
 * Without one it belongs to the leaf: releasing a leaf drains everything charged to it, however it got there,
 * so an out-of-band charge on a leaf (e.g. a filter bitset materialized ahead of collection) is released
 * as soon as that leaf is next scored rather than only at {@link #close()}.
 */
final class LeafExecutionAccounting implements Releasable {

    private final CircuitBreaker breaker;
    private final AtomicLongArray perLeafBytes;
    private final AtomicLong scopedBytes = new AtomicLong();

    LeafExecutionAccounting(CircuitBreaker breaker, int leafCount) {
        this.breaker = breaker;
        this.perLeafBytes = new AtomicLongArray(leafCount);
    }

    /**
     * Reserves {@code bytes} on the request breaker under {@code label}, attributed to the {@link LeafExecutionScope}
     * bound to the calling thread, or to {@code ctx}'s leaf when there is none. No-op when {@code bytes <= 0}.
     * Propagates {@link org.elasticsearch.common.breaker.CircuitBreakingException} without recording anything if
     * the reservation trips the breaker.
     */
    void charge(LeafReaderContext ctx, long bytes, String label) {
        if (bytes > 0L) {
            attribute(ctx, bytes, label);
        }
    }

    /**
     * Like {@link #charge}, for a caller that learns later how much of the reservation it keeps.
     *
     * @return settles the reservation: given the bytes actually retained, releases what was charged beyond them from the scope
     *         or leaf the charge was attributed to. Never raises the charge, and releases nothing already released, under the
     *         conditions given at {@link #releaseFromLeaf} and {@link LeafExecutionScope#refund}. Call at most once.
     *         {@code null} when nothing was charged.
     */
    @Nullable
    LongConsumer reserve(LeafReaderContext ctx, long bytes, String label) {
        if (bytes <= 0L) {
            return null;
        }
        final LeafExecutionScope scope = attribute(ctx, bytes, label);
        if (scope != null) {
            return retainedBytes -> releaseScoped(scope.refund(this, excess(bytes, retainedBytes)));
        }
        final int ord = ctx.ord;
        return retainedBytes -> releaseFromLeaf(ord, excess(bytes, retainedBytes));
    }

    /** Charges {@code bytes} and records them, returning the scope that owns them, or {@code null} when {@code ctx}'s leaf does. */
    @Nullable
    private LeafExecutionScope attribute(LeafReaderContext ctx, long bytes, String label) {
        breaker.addEstimateBytesAndMaybeBreak(bytes, label);
        final LeafExecutionScope scope = LeafExecutionScope.current();
        if (scope != null && scope.add(this, bytes)) {
            scopedBytes.addAndGet(bytes);
            return scope;
        }
        perLeafBytes.addAndGet(ctx.ord, bytes);
        return null;
    }

    /** What was charged beyond {@code retainedBytes}, between {@code 0} and {@code charged}. */
    private static long excess(long charged, long retainedBytes) {
        assert retainedBytes >= 0L : "retained bytes must not be negative: " + retainedBytes;
        return Math.max(0L, charged - retainedBytes);
    }

    /** Releases {@code bytes} held by a {@link LeafExecutionScope}, capped at what scopes still hold. */
    void releaseScoped(long bytes) {
        if (bytes <= 0L) {
            return;
        }
        final long held = scopedBytes.getAndUpdate(current -> Math.max(0L, current - bytes));
        final long released = Math.min(held, bytes);
        if (released > 0L) {
            breaker.addWithoutBreaking(-released);
        }
    }

    /**
     * Releases everything currently charged to {@code ctx}'s leaf when the returned {@link Releasable} is
     * closed. {@link ContextIndexSearcher#searchLeaf} does so from a {@code try}-with-resources block, so
     * the release always runs, including on exceptions.
     */
    Releasable enterLeaf(LeafReaderContext ctx) {
        final int ord = ctx.ord;
        return () -> release(ord);
    }

    private void release(int ord) {
        final long charged = perLeafBytes.getAndSet(ord, 0L);
        if (charged > 0L) {
            breaker.addWithoutBreaking(-charged);
        }
    }

    /**
     * Releases {@code bytes} charged to the leaf {@code ord}, capped at what the leaf still holds.
     * <p>
     * A settle takes from what the leaf holds now, not from its own reservation. If another thread released the leaf after the
     * reservation and charged it again, the settle would release part of that new charge while it is still held. This is safe as
     * long as no two threads score the same leaf of a searcher at once: {@link ContextIndexSearcher#slices} only builds
     * whole-segment partitions, so a search scores each leaf on one thread, and the settle runs before that thread releases the
     * leaf. Splitting a segment across threads needs the settle bounded by its own reservation.
     */
    private void releaseFromLeaf(int ord, long bytes) {
        if (bytes <= 0L) {
            return;
        }
        final long held = perLeafBytes.getAndUpdate(ord, current -> Math.max(0L, current - bytes));
        final long released = Math.min(held, bytes);
        if (released > 0L) {
            breaker.addWithoutBreaking(-released);
        }
    }

    /** Releases any charge still outstanding on any leaf or held by any scope */
    @Override
    public void close() {
        for (int ord = 0; ord < perLeafBytes.length(); ord++) {
            release(ord);
        }
        final long scoped = scopedBytes.getAndSet(0L);
        if (scoped > 0L) {
            breaker.addWithoutBreaking(-scoped);
        }
    }
}
