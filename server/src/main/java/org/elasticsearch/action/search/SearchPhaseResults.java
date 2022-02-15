/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchPhaseResult;

import java.util.stream.Stream;

/**
 * This class acts as a basic result collection that can be extended to do on-the-fly reduction or result processing
 */
abstract class SearchPhaseResults<Result extends SearchPhaseResult> {
    private final int numShards;

    SearchPhaseResults(int numShards) {
        this.numShards = numShards;
    }

    /**
     * Returns the number of expected results this class should collect
     */
    final int getNumShards() {
        return numShards;
    }

    /**
     * A stream of all non-null (successful) shard results
     */
    abstract Stream<Result> getSuccessfulResults();

    /**
     * Consumes a single shard result
     * @param result the shards result
     * @param next a {@link Runnable} that is executed when the response has been fully consumed
     */
    abstract void consumeResult(Result result, Runnable next);

    /**
     * Returns <code>true</code> iff a result if present for the given shard ID.
     */
    abstract boolean hasResult(int shardIndex);

    void consumeShardFailure(int shardIndex) {}

    AtomicArray<Result> getAtomicArray() {
        throw new UnsupportedOperationException();
    }

    /**
     * Reduces the collected results
     */
    SearchPhaseController.ReducedQueryPhase reduce() throws Exception {
        throw new UnsupportedOperationException("reduce is not supported");
    }
}
