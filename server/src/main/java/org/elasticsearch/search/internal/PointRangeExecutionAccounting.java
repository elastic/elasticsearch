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
import org.elasticsearch.core.Releasable;

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Tracks point-range execution RAM charged to a single request's circuit breaker, releasing it once each
 * leaf finishes scoring. One instance is owned by a single {@link ContextIndexSearcher}. Attribution is per
 * leaf rather than per scope: releasing a leaf drains everything currently charged to it, however it got
 * there, so an out-of-band charge on a leaf (e.g. a filter bitset materialised ahead of collection) is
 * released as soon as that leaf is next scored rather than only at {@link #close()}.
 */
final class PointRangeExecutionAccounting implements Releasable {

    private final CircuitBreaker breaker;
    private final AtomicLongArray perLeafBytes;

    PointRangeExecutionAccounting(CircuitBreaker breaker, int leafCount) {
        this.breaker = breaker;
        this.perLeafBytes = new AtomicLongArray(leafCount);
    }

    /**
     * Reserves {@code bytes} on the request breaker, attributed to {@code ctx}'s leaf. No-op when
     * {@code bytes <= 0}. Propagates {@link org.elasticsearch.common.breaker.CircuitBreakingException}
     * without recording anything if the reservation trips the breaker.
     */
    void charge(LeafReaderContext ctx, long bytes) {
        if (bytes <= 0L) {
            return;
        }
        breaker.addEstimateBytesAndMaybeBreak(bytes, "pointrange-execution");
        perLeafBytes.addAndGet(ctx.ord, bytes);
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

    /** Releases any charge still outstanding on any leaf */
    @Override
    public void close() {
        for (int ord = 0; ord < perLeafBytes.length(); ord++) {
            release(ord);
        }
    }
}
