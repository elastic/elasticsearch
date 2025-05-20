/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchPhaseResult;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

/**
 * Optimized phase result consumer that only counts the number of hits and does not
 * store any other information.
 */
class CountOnlyQueryPhaseResultConsumer extends SearchPhaseResults<SearchPhaseResult> {
    final AtomicReference<TotalHits.Relation> relationAtomicReference = new AtomicReference<>(TotalHits.Relation.EQUAL_TO);
    final LongAdder totalHits = new LongAdder();

    private final AtomicBoolean terminatedEarly = new AtomicBoolean(false);
    private final AtomicBoolean timedOut = new AtomicBoolean(false);
    private final Set<Integer> results;
    private final SearchProgressListener progressListener;

    CountOnlyQueryPhaseResultConsumer(SearchProgressListener progressListener, int numShards) {
        super(numShards);
        this.progressListener = progressListener;
        this.results = Collections.newSetFromMap(Maps.newConcurrentHashMapWithExpectedSize(numShards));
    }

    @Override
    Stream<SearchPhaseResult> getSuccessfulResults() {
        return Stream.empty();
    }

    @Override
    public void consumeResult(SearchPhaseResult result, Runnable next) {
        assert results.contains(result.getShardIndex()) == false : "shardIndex: " + result.getShardIndex() + " is already set";
        results.add(result.getShardIndex());
        progressListener.notifyQueryResult(result.getShardIndex(), result.queryResult());
        // We have an empty result, track that we saw it for this shard and continue;
        if (result.queryResult().isNull()) {
            next.run();
            return;
        }
        // set the relation to the first non-equal relation
        relationAtomicReference.compareAndSet(TotalHits.Relation.EQUAL_TO, result.queryResult().getTotalHits().relation());
        totalHits.add(result.queryResult().getTotalHits().value());
        terminatedEarly.compareAndSet(false, (result.queryResult().terminatedEarly() != null && result.queryResult().terminatedEarly()));
        timedOut.compareAndSet(false, result.queryResult().searchTimedOut());
        next.run();
    }

    @Override
    boolean hasResult(int shardIndex) {
        return results.contains(shardIndex);
    }

    @Override
    public SearchPhaseController.ReducedQueryPhase reduce() throws Exception {
        SearchPhaseController.ReducedQueryPhase reducePhase = new SearchPhaseController.ReducedQueryPhase(
            new TotalHits(totalHits.sum(), relationAtomicReference.get()),
            0,
            Float.NaN,
            timedOut.get(),
            terminatedEarly.get(),
            null,
            null,
            null,
            SearchPhaseController.SortedTopDocs.EMPTY,
            null,
            null,
            1,
            0,
            0,
            results.isEmpty()
        );
        if (progressListener != SearchProgressListener.NOOP) {
            progressListener.notifyFinalReduce(
                List.of(),
                reducePhase.totalHits(),
                reducePhase.aggregations(),
                reducePhase.numReducePhases()
            );
        }
        return reducePhase;
    }

    @Override
    AtomicArray<SearchPhaseResult> getAtomicArray() {
        return new AtomicArray<>(0);
    }

    @Override
    public void close() {}
}
