/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.ScrollQuerySearchResult;
import org.elasticsearch.transport.Transport;

import java.util.function.BiFunction;

final class SearchScrollQueryThenFetchAsyncAction extends SearchScrollAsyncAction<ScrollQuerySearchResult> {

    private final SearchTask task;
    private final AtomicArray<FetchSearchResult> fetchResults;
    private final AtomicArray<QuerySearchResult> queryResults;

    SearchScrollQueryThenFetchAsyncAction(Logger logger, ClusterService clusterService, SearchTransportService searchTransportService,
                                          SearchPhaseController searchPhaseController, SearchScrollRequest request, SearchTask task,
                                          ParsedScrollId scrollId, ActionListener<SearchResponse> listener) {
        super(scrollId, logger, clusterService.state().nodes(), listener, searchPhaseController, request,
            searchTransportService);
        this.task = task;
        this.fetchResults = new AtomicArray<>(scrollId.getContext().length);
        this.queryResults = new AtomicArray<>(scrollId.getContext().length);
    }

    protected void onFirstPhaseResult(int shardId, ScrollQuerySearchResult result) {
        queryResults.setOnce(shardId, result.queryResult());
    }

    @Override
    protected void executeInitialPhase(Transport.Connection connection, InternalScrollSearchRequest internalRequest,
                                       SearchActionListener<ScrollQuerySearchResult> searchActionListener) {
        searchTransportService.sendExecuteScrollQuery(connection, internalRequest, task, searchActionListener);
    }

    @Override
    protected SearchPhase moveToNextPhase(BiFunction<String, String, DiscoveryNode> clusterNodeLookup) {
        return new SearchPhase("fetch") {
            @Override
            public void run() {
                final SearchPhaseController.ReducedQueryPhase reducedQueryPhase = searchPhaseController.reducedScrollQueryPhase(
                    queryResults.asList());
                ScoreDoc[] scoreDocs = reducedQueryPhase.sortedTopDocs.scoreDocs;
                if (scoreDocs.length == 0) {
                    sendResponse(reducedQueryPhase, fetchResults);
                    return;
                }

                final IntArrayList[] docIdsToLoad = searchPhaseController.fillDocIdsToLoad(queryResults.length(), scoreDocs);
                final ScoreDoc[] lastEmittedDocPerShard = searchPhaseController.getLastEmittedDocPerShard(reducedQueryPhase,
                    queryResults.length());
                final CountDown counter = new CountDown(docIdsToLoad.length);
                for (int i = 0; i < docIdsToLoad.length; i++) {
                    final int index = i;
                    final IntArrayList docIds = docIdsToLoad[index];
                    if (docIds != null) {
                        final QuerySearchResult querySearchResult = queryResults.get(index);
                        ScoreDoc lastEmittedDoc = lastEmittedDocPerShard[index];
                        ShardFetchRequest shardFetchRequest = new ShardFetchRequest(querySearchResult.getContextId(), docIds,
                            lastEmittedDoc);
                        SearchShardTarget searchShardTarget = querySearchResult.getSearchShardTarget();
                        DiscoveryNode node = clusterNodeLookup.apply(searchShardTarget.getClusterAlias(), searchShardTarget.getNodeId());
                        assert node != null : "target node is null in secondary phase";
                        Transport.Connection connection = getConnection(searchShardTarget.getClusterAlias(), node);
                        searchTransportService.sendExecuteFetchScroll(connection, shardFetchRequest, task,
                            new SearchActionListener<FetchSearchResult>(querySearchResult.getSearchShardTarget(), index) {
                                @Override
                                protected void innerOnResponse(FetchSearchResult response) {
                                    fetchResults.setOnce(response.getShardIndex(), response);
                                    if (counter.countDown()) {
                                        sendResponse(reducedQueryPhase, fetchResults);
                                    }
                                }

                                @Override
                                public void onFailure(Exception t) {
                                    onShardFailure(getName(), counter, querySearchResult.getContextId(),
                                        t, querySearchResult.getSearchShardTarget(),
                                        () -> sendResponsePhase(reducedQueryPhase, fetchResults));
                                }
                            });
                    } else {
                        // the counter is set to the total size of docIdsToLoad
                        // which can have null values so we have to count them down too
                        if (counter.countDown()) {
                            sendResponse(reducedQueryPhase, fetchResults);
                        }
                    }
                }
            }
        };
    }

}
