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
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.async.AsyncTaskIndexService.restoreResponseHeadersContext;

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
    private final ThreadContext threadContext;

    private boolean isPartial;
    private int successfulShards;
    private TotalHits totalHits;
    /**
     * How we get the reduced aggs when {@link #finalResponse} isn't populated.
     * We default to returning no aggs, this {@code -> null}. We'll replace
     * this as we receive updates on the search progress listener.
     */
    private Supplier<InternalAggregations> reducedAggsSource = () -> null;
    private int reducePhase;
    /**
     * The response produced by the search API. Once we receive it we stop
     * building our own {@linkplain SearchResponse}s when get async search
     * is called, and instead return this.
     * @see #findOrBuildResponse(AsyncSearchTask)
     */
    private SearchResponse finalResponse;
    private ElasticsearchException failure;
    private Map<String, List<String>> responseHeaders;

    private boolean frozen;

    /**
     * Creates a new mutable search response.
     *
     * @param totalShards The number of shards that participate in the request, or -1 to indicate a failure.
     * @param skippedShards The number of skipped shards, or -1 to indicate a failure.
     * @param clusters The remote clusters statistics.
     * @param threadContext The thread context to retrieve the final response headers.
     */
    MutableSearchResponse(int totalShards,
                          int skippedShards,
                          Clusters clusters,
                          ThreadContext threadContext) {
        this.totalShards = totalShards;
        this.skippedShards = skippedShards;
        this.clusters = clusters;
        this.shardFailures = totalShards == -1 ? null : new AtomicArray<>(totalShards-skippedShards);
        this.isPartial = true;
        this.threadContext = threadContext;
        this.totalHits = new TotalHits(0L, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
    }

    /**
     * Updates the response with the result of a partial reduction.
     * @param reducedAggs is a strategy for producing the reduced aggs
     */
    synchronized void updatePartialResponse(int successfulShards, TotalHits totalHits,
            Supplier<InternalAggregations> reducedAggs, int reducePhase) {
        failIfFrozen();
        if (reducePhase < this.reducePhase) {
            // should never happen since partial response are updated under a lock
            // in the search phase controller
            throw new IllegalStateException("received partial response out of order: "
                + reducePhase + " < " + this.reducePhase);
        }
        //when we get partial results skipped shards are not included in the provided number of successful shards
        this.successfulShards = successfulShards + skippedShards;
        this.totalHits = totalHits;
        this.reducedAggsSource = reducedAggs;
        this.reducePhase = reducePhase;
    }

    /**
     * Updates the response with the final {@link SearchResponse} once the
     * search is complete.
     */
    synchronized void updateFinalResponse(SearchResponse response) {
        failIfFrozen();
        assert response.getTotalShards() == totalShards : "received number of total shards differs from the one " +
            "notified through onListShards";
        assert response.getSkippedShards() == skippedShards : "received number of skipped shards differs from the one " +
            "notified through onListShards";
        assert response.getFailedShards() == buildShardFailures().length : "number of tracked failures differs from failed shards";
        // copy the response headers from the current context
        this.responseHeaders = threadContext.getResponseHeaders();
        this.finalResponse = response;
        this.isPartial = false;
        this.frozen = true;
    }

    /**
     * Updates the response with a fatal failure. This method preserves the partial response
     * received from previous updates
     */
    synchronized void updateWithFailure(Exception exc) {
        failIfFrozen();
        // copy the response headers from the current context
        this.responseHeaders = threadContext.getResponseHeaders();
        //note that when search fails, we may have gotten partial results before the failure. In that case async
        // search will return an error plus the last partial results that were collected.
        this.isPartial = true;
        ElasticsearchException[] rootCauses = ElasticsearchException.guessRootCauses(exc);
        if (rootCauses == null || rootCauses.length == 0) {
            this.failure = new ElasticsearchException(exc.getMessage(), exc) {
                @Override
                protected String getExceptionName() {
                    return getExceptionName(getCause());
                }
            };
        } else {
            this.failure = rootCauses[0];
        }
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
        return new AsyncSearchResponse(task.getExecutionId().getEncoded(), findOrBuildResponse(task),
                failure, isPartial, frozen == false, task.getStartTime(), expirationTime);
    }

    private SearchResponse findOrBuildResponse(AsyncSearchTask task) {
        if (finalResponse != null) {
            // We have a final response, use it.
            return finalResponse;
        }
        if (clusters == null) {
            // An error occurred before we got the shard list
            return null;
        }
        /*
         * Build the response, reducing aggs if we haven't already and
         * storing the result of the reduction so we won't have to reduce
         * the same aggregation results a second time if nothing has changed.
         * This does cost memory because we have a reference to the finally
         * reduced aggs sitting around which can't be GCed until we get an update.
         */
        InternalAggregations reducedAggs = reducedAggsSource.get();
        reducedAggsSource = () -> reducedAggs;
        InternalSearchResponse internal = new InternalSearchResponse(
            new SearchHits(SearchHits.EMPTY, totalHits, Float.NaN), reducedAggs, null, null, false, false, reducePhase);
        long tookInMillis = TimeValue.timeValueNanos(System.nanoTime() - task.getStartTimeNanos()).getMillis();
        return new SearchResponse(internal, null, totalShards, successfulShards, skippedShards,
                tookInMillis, buildShardFailures(), clusters);
    }

    /**
     * Creates an {@link AsyncSearchResponse} based on the current state of the mutable response.
     * This method also restores the response headers in the current thread context if the final response is available.
     */
    synchronized AsyncSearchResponse toAsyncSearchResponseWithHeaders(AsyncSearchTask task, long expirationTime) {
        AsyncSearchResponse resp = toAsyncSearchResponse(task, expirationTime);
        if (responseHeaders != null) {
            restoreResponseHeadersContext(threadContext, responseHeaders);
        }
        return resp;
    }

    private void failIfFrozen() {
        if (frozen) {
            throw new IllegalStateException("invalid update received after the completion of the request");
        }
    }

    private ShardSearchFailure[] buildShardFailures() {
        if (shardFailures == null) {
            return ShardSearchFailure.EMPTY_ARRAY;
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
