/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponse.Clusters;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;


import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.apache.lucene.search.TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
import static org.elasticsearch.search.aggregations.InternalAggregations.topLevelReduce;

/**
 * A mutable search response that allows to update and create partial response synchronously.
 * Synchronized methods ensure that updates of the content are blocked if another thread is
 * creating an async response concurrently. This limits the number of final reduction that can
 * run concurrently to 1 and ensures that we pause the search progress when an {@link AsyncSearchResponse} is built.
 */
class MutableSearchResponse {
    private final int totalShards;
    private final int skippedShards;
    private final Clusters clusters;
    private final AtomicArray<ShardSearchFailure> shardFailures;
    private final Supplier<ReduceContext> reduceContextSupplier;

    private int version;
    private boolean isPartial;
    private boolean isFinalReduce;
    private int successfulShards;
    private SearchResponseSections sections;
    private ElasticsearchException failure;

    private boolean frozen;

    /**
     * Creates a new mutable search response.
     *
     * @param totalShards The number of shards that participate in the request, or -1 to indicate a failure.
     * @param skippedShards The number of skipped shards, or -1 to indicate a failure.
     * @param clusters The remote clusters statistics.
     * @param reduceContextSupplier A supplier to run final reduce on partial aggregations.
     */
    MutableSearchResponse(int totalShards, int skippedShards, Clusters clusters, Supplier<ReduceContext> reduceContextSupplier) {
        this.totalShards = totalShards;
        this.skippedShards = skippedShards;
        this.clusters = clusters;
        this.reduceContextSupplier = reduceContextSupplier;
        this.version = 0;
        this.shardFailures = totalShards == -1 ? null : new AtomicArray<>(totalShards-skippedShards);
        this.isPartial = true;
        this.sections = totalShards == -1 ? null : new InternalSearchResponse(
            new SearchHits(SearchHits.EMPTY, new TotalHits(0, GREATER_THAN_OR_EQUAL_TO), Float.NaN),
            null, null, null, false, null, 0);
    }

    /**
     * Updates the response with the partial {@link SearchResponseSections} merged from #<code>successfulShards</code>
     * shards.
     */
    synchronized void updatePartialResponse(int successfulShards, SearchResponseSections newSections, boolean isFinalReduce) {
        failIfFrozen();
        if (newSections.getNumReducePhases() < sections.getNumReducePhases()) {
            // should never happen since partial response are updated under a lock
            // in the search phase controller
            throw new IllegalStateException("received partial response out of order: "
                + newSections.getNumReducePhases() + " < " + sections.getNumReducePhases());
        }
        failIfFrozen();
        ++ version;
        this.successfulShards = successfulShards;
        this.sections = newSections;
        this.isPartial = true;
        this.isFinalReduce = isFinalReduce;
    }

    /**
     * Updates the response with the final {@link SearchResponseSections} merged from #<code>successfulShards</code>
     * shards.
     */
    synchronized void updateFinalResponse(int successfulShards, SearchResponseSections newSections) {
        failIfFrozen();
        ++ version;
        this.successfulShards = successfulShards;
        this.sections = newSections;
        this.isPartial = false;
        this.isFinalReduce = true;
        this.frozen = true;
    }

    /**
     * Updates the response with a fatal failure. This method preserves the partial response
     * received from previous updates
     */
    synchronized void updateWithFailure(Exception exc) {
        failIfFrozen();
        ++ version;
        this.isPartial = true;
        this.failure = ElasticsearchException.guessRootCauses(exc)[0];
        this.frozen = true;
    }

    /**
     * Adds a shard failure concurrently (non-blocking).
     */
    void addShardFailure(int shardIndex, ShardSearchFailure failure) {
        synchronized (this) {
            failIfFrozen();
        }
        shardFailures.set(shardIndex, failure);
    }

    /**
     * Creates an {@link AsyncSearchResponse} based on the current state of the mutable response.
     * The final reduce of the aggregations is executed if needed (partial response).
     * This method is synchronized to ensure that we don't perform final reduces concurrently.
     */
    synchronized AsyncSearchResponse toAsyncSearchResponse(AsyncSearchTask task, long expirationTime) {
        final SearchResponse resp;
        if (totalShards != -1) {
            if (sections.aggregations() != null && isFinalReduce == false) {
                InternalAggregations oldAggs = (InternalAggregations) sections.aggregations();
                InternalAggregations newAggs = topLevelReduce(singletonList(oldAggs), reduceContextSupplier.get());
                sections = new InternalSearchResponse(sections.hits(), newAggs, sections.suggest(),
                    null, sections.timedOut(), sections.terminatedEarly(), sections.getNumReducePhases());
                isFinalReduce = true;
            }
            long tookInMillis = TimeValue.timeValueNanos(System.nanoTime() - task.getStartTimeNanos()).getMillis();
            resp = new SearchResponse(sections, null, totalShards, successfulShards,
                skippedShards, tookInMillis, buildShardFailures(), clusters);
        } else {
            resp = null;
        }
        return new AsyncSearchResponse(task.getSearchId().getEncoded(), version, resp, failure, isPartial,
            frozen == false, task.getStartTime(), expirationTime);
    }

    private void failIfFrozen() {
        if (frozen) {
            throw new IllegalStateException("invalid update received after the completion of the request");
        }
    }

    private ShardSearchFailure[] buildShardFailures() {
        if (shardFailures == null) {
            return new ShardSearchFailure[0];
        }
        List<ShardSearchFailure> failures = new ArrayList<>();
        for (int i = 0; i < shardFailures.length(); i++) {
            ShardSearchFailure failure = shardFailures.get(i);
            if (failure != null) {
                failures.add(failure);
            }
        }
        return failures.toArray(ShardSearchFailure[]::new);
    }
}
