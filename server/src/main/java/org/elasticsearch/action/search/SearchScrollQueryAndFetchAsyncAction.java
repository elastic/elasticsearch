/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.rest.action.search.SearchResponseTookMetrics;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ScrollQueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.transport.Transport;

import java.util.function.BiFunction;

final class SearchScrollQueryAndFetchAsyncAction extends SearchScrollAsyncAction<ScrollQueryFetchSearchResult> {

    private final SearchTask task;
    private final AtomicArray<QueryFetchSearchResult> queryFetchResults;
    private final SearchResponseTookMetrics searchResponseTookMetrics;


    SearchScrollQueryAndFetchAsyncAction(
        Logger logger,
        ClusterService clusterService,
        SearchTransportService searchTransportService,
        SearchScrollRequest request,
        SearchTask task,
        ParsedScrollId scrollId,
        ActionListener<SearchResponse> listener,
        SearchResponseTookMetrics searchResponseTookMetrics
    ) {
        super(scrollId, logger, clusterService.state().nodes(), listener, request, searchTransportService, searchResponseTookMetrics);
        this.task = task;
        this.queryFetchResults = new AtomicArray<>(scrollId.getContext().length);
        this.searchResponseTookMetrics = searchResponseTookMetrics;
    }

    @Override
    protected void executeInitialPhase(
        Transport.Connection connection,
        InternalScrollSearchRequest internalRequest,
        SearchActionListener<ScrollQueryFetchSearchResult> searchActionListener
    ) {
        searchTransportService.sendExecuteScrollFetch(connection, internalRequest, task, searchActionListener);
    }

    @Override
    protected SearchPhase moveToNextPhase(BiFunction<String, String, DiscoveryNode> clusterNodeLookup) {
        return sendResponsePhase(SearchPhaseController.reducedScrollQueryPhase(queryFetchResults.asList()), queryFetchResults);
    }

    @Override
    protected void onFirstPhaseResult(int shardId, ScrollQueryFetchSearchResult result) {
        queryFetchResults.setOnce(shardId, result.result());
    }
}
