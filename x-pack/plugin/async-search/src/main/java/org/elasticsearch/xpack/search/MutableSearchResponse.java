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
import org.elasticsearch.action.search.SearchResponseMerger;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.transport.RemoteClusterAware;
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
class MutableSearchResponse implements Releasable {
    private int totalShards;
    private int skippedShards;
    private Clusters clusters;
    private AtomicArray<ShardSearchFailure> queryFailures;
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
    /**
     * Set to true when the local cluster has completed (its full SearchResponse
     * has been received. Only used for CCS minimize_roundtrips=true.
     */
    private boolean localClusterComplete;
    /**
     * For CCS minimize_roundtrips=true, we collect SearchResponses from each cluster in
     * order to provide partial results before all clusters have reported back results.
     */
    private List<SearchResponse> clusterResponses;
    /**
     * Set to true when the final SearchResponse has been received
     * or a fatal error has occurred.
     */
    private boolean frozen;

    /**
     * Creates a new mutable search response.
     *
     * @param threadContext The thread context to retrieve the final response headers.
     */
    MutableSearchResponse(ThreadContext threadContext) {
        this.isPartial = true;
        this.threadContext = threadContext;
        this.totalHits = Lucene.TOTAL_HITS_GREATER_OR_EQUAL_TO_ZERO;
        this.localClusterComplete = false;
    }

    /**
     * Updates the response with the number of total and skipped shards.
     *
     * @param totalShards The number of shards that participate in the request.
     * @param skippedShards The number of shards skipped.
     * <p>
     * Shards in this context depend on the value of minimize round trips (MRT):
     * They are the shards being searched by this coordinator (local only for MRT=true, local + remote otherwise).
     */
    synchronized void updateShardsAndClusters(int totalShards, int skippedShards, Clusters clusters) {
        this.totalShards = totalShards;
        this.skippedShards = skippedShards;
        this.queryFailures = new AtomicArray<>(totalShards - skippedShards);
        this.clusters = clusters;
    }

    /**
     * Updates the response with the result of a partial reduction.
     *
     * @param successfulShards
     * @param totalHits
     * @param reducedAggs is a strategy for producing the reduced aggs
     * @param reducePhase
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
    synchronized void updateFinalResponse(SearchResponse response, boolean ccsMinimizeRoundtrips) {
        failIfFrozen();

        assert shardsInResponseMatchExpected(response, ccsMinimizeRoundtrips)
            : getShardsInResponseMismatchInfo(response, ccsMinimizeRoundtrips);

        this.responseHeaders = threadContext.getResponseHeaders();
        response.mustIncRef();
        var existing = this.finalResponse;
        this.finalResponse = response;
        if (existing != null) {
            existing.decRef();
        }
        this.isPartial = isPartialResponse(response);
        this.frozen = true;
    }

    /**
     * Indicates that a cluster has finished a search operation. Used for CCS minimize_roundtrips=true only.
     *
     * @param clusterAlias alias of cluster that has finished a search operation and returned a SearchResponse.
     *                     The cluster alias for the local cluster is RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.
     * @param clusterResponse SearchResponse from cluster 'clusterAlias'
     */
    synchronized void updateResponseMinimizeRoundtrips(String clusterAlias, SearchResponse clusterResponse) {
        if (clusterResponses == null) {
            clusterResponses = new ArrayList<>();
        }
        clusterResponses.add(clusterResponse);
        clusterResponse.mustIncRef();
        if (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias)) {
            localClusterComplete = true;
        }
    }

    private boolean isPartialResponse(SearchResponse response) {
        if (response.getClusters() == null) {
            return true;
        }
        return response.getClusters().hasPartialResults();
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
        long tookInMillis = TimeValue.timeValueNanos(System.nanoTime() - taskStartTimeNanos).getMillis();
        return new SearchResponse(
            SearchHits.empty(totalHits, Float.NaN),
            reducedAggs,
            null,
            false,
            false,
            null,
            reducePhase,
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
            searchResponse.mustIncRef();
        } else if (clusters == null) {
            // An error occurred before we got the shard list
            searchResponse = null;
        } else {
            // partial results branch
            SearchResponseMerger searchResponseMerger = createSearchResponseMerger(task);
            try {
                if (searchResponseMerger == null) { // local-only search or CCS MRT=false
                    /*
                     * Build the response, reducing aggs if we haven't already and
                     * storing the result of the reduction, so we won't have to reduce
                     * the same aggregation results a second time if nothing has changed.
                     * This does cost memory because we have a reference to the finally
                     * reduced aggs sitting around which can't be GCed until we get an update.
                     */
                    InternalAggregations reducedAggs = reducedAggsSource.get();
                    reducedAggsSource = () -> reducedAggs;
                    searchResponse = buildResponse(task.getStartTimeNanos(), reducedAggs);
                } else if (localClusterComplete == false) {
                    /*
                     * For CCS MRT=true and the local cluster has reported back only partial results
                     * (subset of shards), so use SearchResponseMerger to do a merge of any full results that
                     * have come in from remote clusters and the partial results of the local cluster
                     */
                    InternalAggregations reducedAggs = reducedAggsSource.get();
                    reducedAggsSource = () -> reducedAggs;
                    SearchResponse partialAggsSearchResponse = buildResponse(task.getStartTimeNanos(), reducedAggs);
                    try {
                        searchResponse = getMergedResponse(searchResponseMerger, partialAggsSearchResponse);
                    } finally {
                        partialAggsSearchResponse.decRef();
                    }
                } else {
                    // For CCS MRT=true when the local cluster has reported back full results (via updateResponseMinimizeRoundtrips)
                    searchResponse = getMergedResponse(searchResponseMerger);
                }
            } finally {
                if (searchResponseMerger != null) {
                    searchResponseMerger.close();
                }
            }
        }
        try {
            return new AsyncSearchResponse(
                task.getExecutionId().getEncoded(),
                searchResponse,
                failure,
                isPartial,
                frozen == false,
                task.getStartTime(),
                expirationTime
            );
        } finally {
            if (searchResponse != null) {
                searchResponse.decRef();
            }
        }
    }

    /**
     * Creates a SearchResponseMerger from the Supplier of {@link SearchResponseMerger} held by the AsyncSearchTask.
     * The supplier will be null for local-only searches and CCS minimize_roundtrips=true. In those cases,
     * this method returns null.
     *
     * Otherwise, it creates a new SearchResponseMerger and populates it with all the SearchResponses
     * received so far (via the updateResponseMinimizeRoundtrips method).
     *
     * @param task holds the Supplier of SearchResponseMerger
     * @return SearchResponseMerger with all responses collected to so far or null
     *         (for local-only/CCS minimize_roundtrips=false)
     */
    private SearchResponseMerger createSearchResponseMerger(AsyncSearchTask task) {
        if (task.getSearchResponseMergerSupplier() == null) {
            return null; // local search and CCS minimize_roundtrips=false
        }
        return task.getSearchResponseMergerSupplier().get();
    }

    private SearchResponse getMergedResponse(SearchResponseMerger merger) {
        return getMergedResponse(merger, null);
    }

    private SearchResponse getMergedResponse(SearchResponseMerger merger, SearchResponse localPartialAggsOnly) {
        if (clusterResponses != null) {
            for (SearchResponse response : clusterResponses) {
                merger.add(response);
            }
        }
        if (localPartialAggsOnly != null) {
            merger.add(localPartialAggsOnly);
        }
        return merger.getMergedResponse(clusters);
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
        SearchResponse.Clusters clustersInStatus = null;
        if (clusters != null && clusters.getTotal() > 0) {
            // include clusters in the status if present and not Clusters.EMPTY (the case for local searches only)
            clustersInStatus = clusters;
        }
        if (finalResponse != null) {
            return new AsyncStatusResponse(
                asyncExecutionId,
                frozen == false,
                isPartial,
                startTime,
                expirationTime,
                startTime + finalResponse.getTook().millis(),
                finalResponse.getTotalShards(),
                finalResponse.getSuccessfulShards(),
                finalResponse.getSkippedShards(),
                finalResponse.getShardFailures() != null ? finalResponse.getShardFailures().length : 0,
                finalResponse.status(),
                clustersInStatus
            );
        }
        if (failure != null) {
            return new AsyncStatusResponse(
                asyncExecutionId,
                frozen == false,
                true,
                startTime,
                expirationTime,
                null,
                totalShards,
                successfulShards,
                skippedShards,
                queryFailures == null ? 0 : queryFailures.nonNullLength(),
                ExceptionsHelper.status(ExceptionsHelper.unwrapCause(failure)),
                clustersInStatus
            );
        }
        return new AsyncStatusResponse(
            asyncExecutionId,
            true,
            true,
            startTime,
            expirationTime,
            null,
            totalShards,
            successfulShards,
            skippedShards,
            queryFailures == null ? 0 : queryFailures.nonNullLength(),
            null,  // for a still running search, completion status is null
            clustersInStatus
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
        var response = buildResponse(task.getStartTimeNanos(), null);
        try {
            return new AsyncSearchResponse(
                task.getExecutionId().getEncoded(),
                response,
                reduceException,
                isPartial,
                frozen == false,
                task.getStartTime(),
                expirationTime
            );
        } finally {
            response.decRef();
        }
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

    private boolean shardsInResponseMatchExpected(SearchResponse response, boolean ccsMinimizeRoundtrips) {
        if (ccsMinimizeRoundtrips) {
            return response.getTotalShards() >= totalShards && response.getSkippedShards() >= skippedShards;
        } else {
            return response.getTotalShards() == totalShards && response.getSkippedShards() == skippedShards;
        }
    }

    private String getShardsInResponseMismatchInfo(SearchResponse response, boolean ccsMinimizeRoundtrips) {
        if (ccsMinimizeRoundtrips) {
            if (response.getTotalShards() < totalShards) {
                return Strings.format(
                    "received number of shards (%d) is less than the value notified via onListShards (%d)",
                    response.getTotalShards(),
                    totalShards
                );
            }
            if (response.getSkippedShards() < skippedShards) {
                return Strings.format(
                    "received number of skipped shards (%d) is less than the value notified via onListShards (%d)",
                    response.getSkippedShards(),
                    skippedShards
                );
            }
            throw new IllegalStateException("assert method hit unexpected case for ccsMinimizeRoundtrips=true");
        } else {
            if (response.getTotalShards() != totalShards) {
                return Strings.format(
                    "received number of shards (%d) differs from the one notified via onListShards (%d)",
                    response.getTotalShards(),
                    totalShards
                );
            }
            if (response.getSkippedShards() != skippedShards) {
                return Strings.format(
                    "received number of skipped shards (%d) differs from the one notified via onListShards (%d)",
                    response.getSkippedShards(),
                    skippedShards
                );
            }
            throw new IllegalStateException("assert method hit unexpected case for ccsMinimizeRoundtrips=false");
        }
    }

    @Override
    public synchronized void close() {
        if (finalResponse != null) {
            finalResponse.decRef();
        }
        if (clusterResponses != null) {
            for (SearchResponse clusterResponse : clusterResponses) {
                clusterResponse.decRef();
            }
        }
    }
}
