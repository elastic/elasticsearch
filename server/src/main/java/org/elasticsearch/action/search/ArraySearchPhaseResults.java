/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.transport.LeakTracker;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * This class acts as a basic result collection that can be extended to do on-the-fly reduction or result processing
 */
class ArraySearchPhaseResults<Result extends SearchPhaseResult> extends SearchPhaseResults<Result> {
    final AtomicArray<Result> results;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Held by the search until {@link #close()}, and while each {@link #consumeResult} call records its result. The
     * last reference to go releases every collected result.
     * <p>
     * A failed phase leaves its shard requests in flight, so results keep arriving after the search closes this
     * collection. Counting the calls still recording makes the release wait for them; once the last reference is
     * gone, a late result is refused rather than referenced by a collection that can no longer release it.
     */
    private final RefCounted refs = LeakTracker.wrap(AbstractRefCounted.of(this::releaseResults));

    ArraySearchPhaseResults(int size) {
        super(size);
        this.results = new AtomicArray<>(size);
    }

    Stream<Result> getSuccessfulResults() {
        return results.asList().stream();
    }

    @Override
    void consumeResult(Result result, Runnable next) {
        assert results.get(result.getShardIndex()) == null : "shardIndex: " + result.getShardIndex() + " is already set";
        if (refs.tryIncRef()) {
            try {
                results.set(result.getShardIndex(), result);
                result.incRef();
            } finally {
                refs.decRef();
            }
        }
        next.run();
    }

    private void releaseResults() {
        // Not results.asList(), which caches its list and can hand back one built before the last result was stored.
        for (int i = 0; i < results.length(); i++) {
            Result result = results.get(i);
            if (result != null) {
                result.decRef();
            }
        }
    }

    @Override
    public final void close() {
        if (closed.compareAndSet(false, true)) {
            refs.decRef();
            doClose();
        }
    }

    /**
     * Whether {@link #close()} has been called, in which case a result being consumed must not be handed to state
     * that {@link #doClose()} releases.
     */
    protected final boolean isClosed() {
        return closed.get();
    }

    protected void doClose() {}

    boolean hasResult(int shardIndex) {
        return results.get(shardIndex) != null;
    }

    @Override
    AtomicArray<Result> getAtomicArray() {
        return results;
    }
}
