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

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * This class acts as a basic result collection that can be extended to do on-the-fly reduction or result processing
 */
abstract class SearchPhaseResults<Result extends SearchPhaseResult> implements Releasable {
    private final int numShards;
    private volatile Consumer<DirectoryMetrics> directoryMetricsSink = m -> {};

    SearchPhaseResults(int numShards) {
        this.numShards = numShards;
    }

    /**
     * Wires the destination for {@link DirectoryMetrics} that this collection observes on each shard result.
     * Called once by the owning {@link AbstractSearchAsyncAction} (or sub-phase) before any shard responses
     * arrive. The default sink is a no-op, which is the right behavior for data-node-side consumers
     */
    void setDirectoryMetricsSink(Consumer<DirectoryMetrics> sink) {
        this.directoryMetricsSink = sink;
    }

    protected void publishDirectoryMetrics(DirectoryMetrics m) {
        if (m.isEmpty() == false) {
            directoryMetricsSink.accept(m);
        }
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
