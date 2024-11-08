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
import org.elasticsearch.core.Releasable;
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

    private final Releasable releasable = LeakTracker.wrap(() -> {
        for (Result result : getAtomicArray().asList()) {
            result.decRef();
        }
    });

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
        results.set(result.getShardIndex(), result);
        result.incRef();
        next.run();
    }

    @Override
    public final void close() {
        if (closed.compareAndSet(false, true)) {
            releasable.close();
            doClose();
        }
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
