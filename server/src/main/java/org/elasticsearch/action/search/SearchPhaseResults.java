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
import org.elasticsearch.index.store.DirectoryMetrics;
import org.elasticsearch.search.SearchPhaseResult;

import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * This class acts as a basic result collection that can be extended to do on-the-fly reduction or result processing
 */
abstract class SearchPhaseResults<Result extends SearchPhaseResult> implements Releasable {
    private final int numShards;
    private final AtomicReference<DirectoryMetrics> directoryMetrics = new AtomicReference<>(DirectoryMetrics.EMPTY);

    SearchPhaseResults(int numShards) {
        this.numShards = numShards;
    }

    /**
     * Accumulates the {@link DirectoryMetrics} carried by each shard result as it is consumed. Shard results are
     * consumed concurrently from transport worker threads, so the merge is performed atomically.
     */
    protected void accumulateDirectoryMetrics(DirectoryMetrics m) {
        if (m.isEmpty() == false) {
            directoryMetrics.accumulateAndGet(m, (current, incoming) -> current.isEmpty() ? incoming : current.merge(incoming));
        }
    }

    DirectoryMetrics getDirectoryMetrics() {
        return directoryMetrics.get();
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
