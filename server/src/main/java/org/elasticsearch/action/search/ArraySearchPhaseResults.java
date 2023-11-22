/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.transport.LeakTracker;

import java.util.stream.Stream;

/**
 * This class acts as a basic result collection that can be extended to do on-the-fly reduction or result processing
 */
class ArraySearchPhaseResults<Result extends SearchPhaseResult> extends SearchPhaseResults<Result> {
    final AtomicArray<Result> results;

    private final RefCounted refCounted = LeakTracker.wrap(AbstractRefCounted.of(this::doClose));

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

    protected void doClose() {
        for (Result result : getAtomicArray().asList()) {
            result.decRef();
        }
    }

    boolean hasResult(int shardIndex) {
        return results.get(shardIndex) != null;
    }

    @Override
    AtomicArray<Result> getAtomicArray() {
        return results;
    }

    @Override
    public void incRef() {
        refCounted.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return refCounted.tryIncRef();
    }

    @Override
    public boolean decRef() {
        return refCounted.decRef();
    }

    @Override
    public boolean hasReferences() {
        return refCounted.hasReferences();
    }
}
