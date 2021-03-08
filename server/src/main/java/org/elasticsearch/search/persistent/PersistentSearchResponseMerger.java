/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.persistent;

import com.carrotsearch.hppc.IntArrayList;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.QueryPhaseResultConsumer;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.action.search.SearchProgressListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseMerger;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class PersistentSearchResponseMerger implements Releasable {
    private final String searchId;
    private final long expirationTime;
    private final QueryPhaseResultConsumer queryPhaseResultConsumer;
    private final SearchPhaseController searchPhaseController;
    private final SearchResponseMerger searchResponseMerger;
    @Nullable
    private final PersistentSearchResponse baseResponse;
    private final List<PersistentSearchShard> reducedShards;
    private final List<SearchPhaseResult> fetchResults;
    private final TransportSearchAction.SearchTimeProvider searchTimeProvider;

    private final IntArrayList reducedShardIndices = new IntArrayList();
    private final Set<PersistentSearchShard> pendingShards;
    private List<PersistentSearchShardFetchFailure> fetchErrors;

    PersistentSearchResponseMerger(String searchId,
                                   long expirationTime,
                                   SearchRequest searchRequest,
                                   TransportSearchAction.SearchTimeProvider searchTimeProvider,
                                   ThreadPool threadPool,
                                   CircuitBreaker circuitBreaker,
                                   InternalAggregation.ReduceContextBuilder aggReduceContextBuilder,
                                   PersistentSearchResponse baseResponse,
                                   SearchPhaseController searchPhaseController,
                                   List<PersistentSearchShard> shardsToReduce) {
        this.searchId = searchId;
        this.expirationTime = expirationTime;
        this.baseResponse = baseResponse;
        this.searchTimeProvider = searchTimeProvider;

        this.queryPhaseResultConsumer = searchPhaseController.newSearchPhaseResults(threadPool.executor(ThreadPool.Names.SAME),
            circuitBreaker,
            SearchProgressListener.NOOP,
            searchRequest,
            shardsToReduce.size(),
            this::onReductionError
        );
        this.searchResponseMerger =
            createSearchResponseMerger(searchRequest.source(), searchTimeProvider, aggReduceContextBuilder, searchRequest.isFinalReduce());

        this.searchPhaseController = searchPhaseController;
        this.reducedShards = new ArrayList<>(shardsToReduce.size());
        this.fetchResults = new ArrayList<>(shardsToReduce.size());
        this.pendingShards = new HashSet<>(shardsToReduce);

        if (baseResponse != null) {
            searchResponseMerger.add(baseResponse.getSearchResponse());
            addReducedShardIndices(baseResponse);
        }
    }

    static SearchResponseMerger createSearchResponseMerger(SearchSourceBuilder source,
                                                           TransportSearchAction.SearchTimeProvider timeProvider,
                                                           InternalAggregation.ReduceContextBuilder aggReduceContextBuilder,
                                                           boolean performFinalReduce) {
        final int from = source == null || source.from() == -1 ? SearchService.DEFAULT_FROM : source.from();
        final int size = source == null || source.size() == -1 ? SearchService.DEFAULT_SIZE : source.size();
        final int trackTotalHitsUpTo = source == null || source.trackTotalHitsUpTo() == null ?
            SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO : source.trackTotalHitsUpTo();
        return new SearchResponseMerger(from, size, trackTotalHitsUpTo, timeProvider, aggReduceContextBuilder, performFinalReduce);
    }

    void addResponse(PersistentSearchShard searchShardId, int shardIdx, ShardSearchResult shardSearchResult) {
        assert Thread.currentThread().getName().contains("[" + ThreadPool.Names.SEARCH + "]")
            : "Expected current thread [" + Thread.currentThread() + "] to be the a search thread.";

        if (pendingShards.remove(searchShardId) == false) {
            throw new IllegalArgumentException("Unknown shard " + searchShardId);
        }

        final QueryFetchSearchResult shardSearchResultResult = shardSearchResult.getResult();
        shardSearchResultResult.setShardIndex(shardIdx);
        // TODO: reuse this
        final SearchShardTarget shardTarget =
            new SearchShardTarget("node",
                searchShardId.getSearchShard().getShardId(),
                RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                OriginalIndices.NONE
            );
        shardSearchResultResult.setSearchShardTarget(shardTarget);
        queryPhaseResultConsumer.consumeResult(shardSearchResultResult, () -> {
            // This is empty on purpose, since queryPhaseResultConsumer executor is SAME if we trigger a reduction,
            // this reduction runs in the same thread
        });

        reducedShards.add(searchShardId);
        // TODO: clarify this
        reducedShardIndices.add(shardSearchResult.getShardIndex());
        // TODO: Keep track of fetch size as now we're storing all the results
        fetchResults.add(shardSearchResultResult.fetchResult());
    }

    void onShardResponseFetchFailure(ShardQueryResultInfo shardQueryResultInfo, Exception error) {
        if (pendingShards.remove(shardQueryResultInfo.getShardId()) == false) {
            throw new IllegalArgumentException("The shard " + shardQueryResultInfo + " is already present");
        }

        if (fetchErrors == null) {
            fetchErrors = new ArrayList<>();
        }

        fetchErrors.add(new PersistentSearchShardFetchFailure(shardQueryResultInfo, error));
    }

    @Nullable
    PartialReduceResponse getMergedResponse() throws Exception {
        if (pendingShards.isEmpty() == false) {
            throw new IllegalStateException("Pending shards to be added " + pendingShards);
        }

        if (reducedShards.size() == 0) {
            return new PartialReduceResponse(baseResponse, reducedShards, fetchErrors);
        }

        final SearchPhaseController.ReducedQueryPhase reducedQueryPhase = queryPhaseResultConsumer.reduce();

        final InternalSearchResponse internalResponse =
            searchPhaseController.merge(false, reducedQueryPhase, fetchResults, fetchResults::get);

        SearchResponse searchResponse = new SearchResponse(internalResponse,
            null, // Scroll is not supported on persistent search
            reducedShards.size(),
            reducedShards.size(),
            0, // TODO: Populate this
            searchTimeProvider.buildTookInMillis(),
            ShardSearchFailure.EMPTY_ARRAY, // TODO: We have to complete this information at the end of the execution
            SearchResponse.Clusters.EMPTY
        );

        searchResponseMerger.add(searchResponse);

        final SearchResponse mergedResponse = searchResponseMerger.getMergedResponse(SearchResponse.Clusters.EMPTY);

        return new PartialReduceResponse(
            new PersistentSearchResponse(searchId,
                mergedResponse,
                expirationTime,
                reducedShardIndices.toArray(),
                baseResponse == null ? 0 : baseResponse.getVersion() + 1
            ),
            reducedShards,
            fetchErrors
        );
    }

    private void onReductionError(Exception e) {
        throw new RuntimeException("Error reducing", e);
    }

    private void addReducedShardIndices(PersistentSearchResponse baseResponse) {
        for (Integer reducedShardIndex : baseResponse.getReducedShardIndices()) {
            reducedShardIndices.add(reducedShardIndex);
        }
    }

    @Override
    public void close() {
        Releasables.close(queryPhaseResultConsumer);
    }
}
