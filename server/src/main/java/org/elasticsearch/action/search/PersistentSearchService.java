/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.persistent.ExecutePersistentQueryFetchRequest;
import org.elasticsearch.action.search.persistent.ExecutePersistentQueryFetchResponse;
import org.elasticsearch.action.search.persistent.GetPersistentSearchRequest;
import org.elasticsearch.action.search.persistent.PersistentSearchShardId;
import org.elasticsearch.action.search.persistent.ReducePartialPersistentSearchRequest;
import org.elasticsearch.action.search.persistent.ReducePartialPersistentSearchResponse;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.persistent.PersistentSearchResponse;
import org.elasticsearch.search.persistent.PersistentSearchStorageService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.elasticsearch.action.search.SearchPhaseController.setShardIndex;

public class PersistentSearchService {
    private final SearchService searchService;
    private final SearchPhaseController searchPhaseController;
    private final PersistentSearchStorageService searchStorageService;
    private final Executor executor;
    private final LongSupplier relativeCurrentNanosProvider;
    private final Logger logger = LogManager.getLogger(PersistentSearchService.class);

    public PersistentSearchService(SearchService searchService,
                                   SearchPhaseController searchPhaseController,
                                   PersistentSearchStorageService searchStorageService,
                                   Executor executor,
                                   LongSupplier relativeCurrentNanosProvider) {
        this.searchService = searchService;
        this.searchPhaseController = searchPhaseController;
        this.searchStorageService = searchStorageService;
        this.executor = executor;
        this.relativeCurrentNanosProvider = relativeCurrentNanosProvider;
    }

    public void getPersistentSearchResponse(GetPersistentSearchRequest getPersistentSearchRequest,
                                            ActionListener<PersistentSearchResponse> listener) {
        searchStorageService.getPersistentSearchResponseAsync(getPersistentSearchRequest.getId(), listener);
    }

    public void executeAsyncQueryPhase(ExecutePersistentQueryFetchRequest request,
                                       SearchShardTask task, ActionListener<ExecutePersistentQueryFetchResponse> listener) {
        StepListener<SearchPhaseResult> queryListener = new StepListener<>();
        StepListener<String> storeListener = new StepListener<>();

        final ShardSearchRequest shardSearchRequest = request.getShardSearchRequest();
        queryListener.whenComplete(result -> {
            final SearchResponse searchResponse = convertToSearchResponse((QueryFetchSearchResult) result,
                searchService.aggReduceContextBuilder(shardSearchRequest.source()));

            String searchId = request.getSearchId();
            String docId = request.getResultDocId();
            long expireTime = request.getExpireTime();
            int shardIndex = request.getShardIndex();

            final PersistentSearchResponse persistentSearchResponse =
                new PersistentSearchResponse(docId, searchId, searchResponse, expireTime, List.of(shardIndex), 1);
            searchStorageService.storeResult(persistentSearchResponse, storeListener);
        }, listener::onFailure);

        storeListener.whenComplete(partialResultDocId ->
            listener.onResponse(new ExecutePersistentQueryFetchResponse(partialResultDocId)), listener::onFailure);

        searchService.executeQueryPhase(shardSearchRequest, false, task, queryListener);
    }

    public void executePartialReduce(ReducePartialPersistentSearchRequest request,
                                     SearchTask task,
                                     ActionListener<ReducePartialPersistentSearchResponse> listener) {
        // we need to use versioning for SearchResponse (so we avoid conflicting operations)
        final String searchId = request.getSearchId();
        StepListener<PersistentSearchResponse> getSearchResultListener = new StepListener<>();
        StepListener<Tuple<PersistentSearchResponse, List<PersistentSearchShardId>>> reduceListener = new StepListener<>();
        StepListener<List<PersistentSearchShardId>> partialResponseStoredListener = new StepListener<>();

        getSearchResultListener.whenComplete(persistentSearchResponse -> {
            try {
                PersistentSearchResponseMerger searchResponseMerger = new PersistentSearchResponseMerger(
                    request.getSearchId(),
                    request.getExpirationTime(),
                    // TODO: This doesn't work if the reduce phase is executed in a different node than the coordinator
                    new TransportSearchAction.SearchTimeProvider(request.getSearchAbsoluteStartMillis(),
                        request.getSearchRelativeStartNanos(), relativeCurrentNanosProvider),
                    searchPhaseController.getReduceContext(request.getOriginalRequest()),
                    request.performFinalReduce(),
                    persistentSearchResponse
                );

                runAsync(() -> reduce(searchResponseMerger, task, request), reduceListener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);

        reduceListener.whenComplete((reducedSearchResponseAndShards -> {
            final PersistentSearchResponse reducedSearchResponse = reducedSearchResponseAndShards.v1();
            final List<PersistentSearchShardId> reducedShards = reducedSearchResponseAndShards.v2();

            // TODO: Add timeouts
            searchStorageService.storeResult(reducedSearchResponse, new ActionListener<>() {
                @Override
                public void onResponse(String docId) {
                    partialResponseStoredListener.onResponse(reducedShards);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }), listener::onFailure);

        partialResponseStoredListener.whenComplete(reducedShards -> {
            listener.onResponse(new ReducePartialPersistentSearchResponse(reducedShards));
            deleteIntermediateResults(reducedShards);
        }, listener::onFailure);

        searchStorageService.getPersistentSearchResponseAsync(searchId, getSearchResultListener);
    }

    private void deleteIntermediateResults(List<PersistentSearchShardId> shards) {
        // It might make sense to add a TTL to the records and do a periodic cleanup
        final List<String> docsToRemove = shards.stream()
            .map(PersistentSearchShardId::getDocId)
            .collect(Collectors.toList());
        searchStorageService.deletePersistentSearchResults(docsToRemove, new ActionListener<>() {
            @Override
            public void onResponse(Collection<DeleteResponse> deleteResponses) {
                logger.info("DELETED intermediate results");
            }

            @Override
            public void onFailure(Exception e) {
                // TODO: retry?
            }
        });
    }

    private Tuple<PersistentSearchResponse, List<PersistentSearchShardId>> reduce(PersistentSearchResponseMerger searchResponseMerger,
                                                                                  SearchTask searchTask,
                                                                                  ReducePartialPersistentSearchRequest request) {
        // TODO: Use circuit breaker
        List<PersistentSearchShardId> reducedShards = new ArrayList<>(request.getShardsToReduce().size());
        for (PersistentSearchShardId searchShardId : request.getShardsToReduce()) {
            checkForCancellation(searchTask);

            try {
                final PersistentSearchResponse partialResult = searchStorageService.getPersistentSearchResponse(searchShardId.getDocId());
                searchResponseMerger.addResponse(partialResult);
            } catch (Exception e) {
                logger.info("Error getting persistent search response", e);
                // Ignore if not exists for now...
            }
            reducedShards.add(searchShardId);
        }

        checkForCancellation(searchTask);

        final PersistentSearchResponse reducedSearchResponse = searchResponseMerger.getMergedResponse();
        return Tuple.tuple(reducedSearchResponse, reducedShards);
    }

    private void checkForCancellation(SearchTask searchTask) {
        if (searchTask.isCancelled()) {
            throw new RuntimeException("Search has been cancelled");
        }
    }

    private SearchResponse convertToSearchResponse(QueryFetchSearchResult result,
                                                   InternalAggregation.ReduceContextBuilder aggReduceContextBuilder) {
        SearchPhaseController.TopDocsStats topDocsStats = new SearchPhaseController.TopDocsStats(10);
        topDocsStats.add(result.queryResult().topDocs(), false, false);
        TopDocsAndMaxScore topDocs = result.queryResult().consumeTopDocs();
        setShardIndex(topDocs.topDocs, 0);

        final SearchPhaseController.ReducedQueryPhase reducedQueryPhase =
            SearchPhaseController.reducedQueryPhase(Collections.singletonList(result),
                Collections.singletonList(result.queryResult().aggregations().expand()),
                Collections.singletonList(topDocs.topDocs),
                topDocsStats,
                0,
                false,
                aggReduceContextBuilder,
                false);
        final List<FetchSearchResult> fetchResults = Collections.singletonList(result.fetchResult());

        final InternalSearchResponse internalSearchResponse
            = searchPhaseController.merge(false, reducedQueryPhase, fetchResults, fetchResults::get);
        return new SearchResponse(internalSearchResponse,
            null,
            1,
            1,
            0,
            0,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }

    private <T> void runAsync(CheckedSupplier<T, Exception> executable, ActionListener<T> listener) {
        executor.execute(ActionRunnable.supply(listener, executable));
    }
}
