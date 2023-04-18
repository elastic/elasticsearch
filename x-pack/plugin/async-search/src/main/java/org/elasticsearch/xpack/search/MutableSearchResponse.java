/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponse.Clusters;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.AsyncStatusResponse;

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
    private static final TotalHits EMPTY_TOTAL_HITS = new TotalHits(0L, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
    private final int totalShards;
    private final int skippedShards;
    private final Clusters clusters;
    private final AtomicArray<ShardSearchFailure> queryFailures;
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
    MutableSearchResponse(int totalShards, int skippedShards, Clusters clusters, ThreadContext threadContext) {
        this.totalShards = totalShards;
        this.skippedShards = skippedShards;
        this.clusters = clusters;
        this.queryFailures = totalShards == -1 ? null : new AtomicArray<>(totalShards - skippedShards);
        this.isPartial = true;
        this.threadContext = threadContext;
        this.totalHits = EMPTY_TOTAL_HITS;
    }

    /**
     * Updates the response with the result of a partial reduction.
     * @param reducedAggs is a strategy for producing the reduced aggs
     */
    @SuppressWarnings("HiddenField")
    synchronized void updatePartialResponse(
        int successfulShards,
        TotalHits totalHits,
        Supplier<InternalAggregations> reducedAggs,
        int reducePhase
    ) {
        failIfFrozen();
        if (reducePhase < this.reducePhase) {
            // should never happen since partial response are updated under a lock
            // in the search phase controller
            throw new IllegalStateException("received partial response out of order: " + reducePhase + " < " + this.reducePhase);
        }
        // when we get partial results skipped shards are not included in the provided number of successful shards
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
        assert response.getTotalShards() == totalShards
            : "received number of total shards differs from the one " + "notified through onListShards";
        assert response.getSkippedShards() == skippedShards
            : "received number of skipped shards differs from the one " + "notified through onListShards";
        this.responseHeaders = threadContext.getResponseHeaders();
        this.finalResponse = response;
        this.isPartial = false;
        this.frozen = true;
    }

    /**
     * Updates the response with a fatal failure. This method preserves the partial response
     * received from previous updates
     */
    synchronized void updateWithFailure(ElasticsearchException exc) {
        failIfFrozen();
        // copy the response headers from the current context
        this.responseHeaders = threadContext.getResponseHeaders();
        // note that when search fails, we may have gotten partial results before the failure. In that case async
        // search will return an error plus the last partial results that were collected.
        this.isPartial = true;
        this.failure = exc;
        this.frozen = true;
    }

    /**
     * Adds a shard failure concurrently (non-blocking).
     */
    void addQueryFailure(int shardIndex, ShardSearchFailure shardSearchFailure) {
        synchronized (this) {
            failIfFrozen();
        }
        queryFailures.set(shardIndex, shardSearchFailure);
    }

    private SearchResponse buildResponse(long taskStartTimeNanos, InternalAggregations reducedAggs) {
        InternalSearchResponse internal = new InternalSearchResponse(
            new SearchHits(SearchHits.EMPTY, totalHits, Float.NaN),
            reducedAggs,
            null,
            null,
            false,
            false,
            reducePhase
        );
        long tookInMillis = TimeValue.timeValueNanos(System.nanoTime() - taskStartTimeNanos).getMillis();
        return new SearchResponse(
            internal,
            null,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            buildQueryFailures(),
            clusters
        );
    }

    /**
     * Creates an {@link AsyncSearchResponse} based on the current state of the mutable response.
     * The final reduce of the aggregations is executed if needed (partial response).
     * This method is synchronized to ensure that we don't perform final reduces concurrently.
     * This method also restores the response headers in the current thread context when requested, if the final response is available.
     */
    synchronized AsyncSearchResponse toAsyncSearchResponse(AsyncSearchTask task, long expirationTime, boolean restoreResponseHeaders) {
        if (restoreResponseHeaders && responseHeaders != null) {
            restoreResponseHeadersContext(threadContext, responseHeaders);
        }
        SearchResponse searchResponse;
        if (finalResponse != null) {
            // We have a final response, use it.
            searchResponse = finalResponse;
        } else if (clusters == null) {
            // An error occurred before we got the shard list
            searchResponse = null;
        } else {
            /*
             * Build the response, reducing aggs if we haven't already and
             * storing the result of the reduction so we won't have to reduce
             * the same aggregation results a second time if nothing has changed.
             * This does cost memory because we have a reference to the finally
             * reduced aggs sitting around which can't be GCed until we get an update.
             */
            InternalAggregations reducedAggs = reducedAggsSource.get();
            reducedAggsSource = () -> reducedAggs;
            searchResponse = buildResponse(task.getStartTimeNanos(), reducedAggs);
        }
        return new AsyncSearchResponse(
            task.getExecutionId().getEncoded(),
            searchResponse,
            failure,
            isPartial,
            frozen == false,
            task.getStartTime(),
            expirationTime
        );
    }

    /**
     * Creates an {@link AsyncStatusResponse} -- status of an async response.
     * Response is created based on the current state of the mutable response or based on {@code finalResponse} if it is available.
     * @param asyncExecutionId – id of async search request
     * @param startTime – start time of task
     * @param expirationTime – expiration time of async search request
     * @return response representing the status of async search
     */
    synchronized AsyncStatusResponse toStatusResponse(String asyncExecutionId, long startTime, long expirationTime) {
        if (finalResponse != null) {
            return new AsyncStatusResponse(
                asyncExecutionId,
                false,
                false,
                startTime,
                expirationTime,
                finalResponse.getTotalShards(),
                finalResponse.getSuccessfulShards(),
                finalResponse.getSkippedShards(),
                finalResponse.getShardFailures() != null ? finalResponse.getShardFailures().length : 0,
                finalResponse.status()
            );
        }
        if (failure != null) {
            return new AsyncStatusResponse(
                asyncExecutionId,
                false,
                true,
                startTime,
                expirationTime,
                totalShards,
                successfulShards,
                skippedShards,
                queryFailures == null ? 0 : queryFailures.nonNullLength(),
                ExceptionsHelper.status(ExceptionsHelper.unwrapCause(failure))
            );
        }
        return new AsyncStatusResponse(
            asyncExecutionId,
            true,
            true,
            startTime,
            expirationTime,
            totalShards,
            successfulShards,
            skippedShards,
            queryFailures == null ? 0 : queryFailures.nonNullLength(),
            null  // for a still running search, completion status is null
        );
    }

    synchronized AsyncSearchResponse toAsyncSearchResponse(
        AsyncSearchTask task,
        long expirationTime,
        ElasticsearchException reduceException
    ) {
        if (this.failure != null) {
            reduceException.addSuppressed(this.failure);
        }
        return new AsyncSearchResponse(
            task.getExecutionId().getEncoded(),
            buildResponse(task.getStartTimeNanos(), null),
            reduceException,
            isPartial,
            frozen == false,
            task.getStartTime(),
            expirationTime
        );
    }

    private void failIfFrozen() {
        if (frozen) {
            throw new IllegalStateException("invalid update received after the completion of the request");
        }
    }

    private ShardSearchFailure[] buildQueryFailures() {
        if (queryFailures == null) {
            return ShardSearchFailure.EMPTY_ARRAY;
        }
        List<ShardSearchFailure> failures = new ArrayList<>();
        for (int i = 0; i < queryFailures.length(); i++) {
            ShardSearchFailure shardSearchFailure = queryFailures.get(i);
            if (shardSearchFailure != null) {
                failures.add(shardSearchFailure);
            }
        }
        return failures.toArray(ShardSearchFailure[]::new);
    }
}
