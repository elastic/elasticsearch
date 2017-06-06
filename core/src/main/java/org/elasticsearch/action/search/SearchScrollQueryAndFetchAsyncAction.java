/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ScrollQueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.transport.Transport;

import java.util.function.BiFunction;

final class SearchScrollQueryAndFetchAsyncAction extends SearchScrollAsyncAction<ScrollQueryFetchSearchResult> {

    private final SearchTask task;
    private final AtomicArray<QueryFetchSearchResult> queryFetchResults;

    SearchScrollQueryAndFetchAsyncAction(Logger logger, ClusterService clusterService, SearchTransportService searchTransportService,
                                         SearchPhaseController searchPhaseController, SearchScrollRequest request, SearchTask task,
                                         ParsedScrollId scrollId, ActionListener<SearchResponse> listener) {
        super(scrollId, logger, clusterService.state().nodes(), listener, searchPhaseController, request, searchTransportService);
        this.task = task;
        this.queryFetchResults = new AtomicArray<>(scrollId.getContext().length);
    }

    @Override
    protected void executeInitialPhase(Transport.Connection connection, InternalScrollSearchRequest internalRequest,
                                       SearchActionListener<ScrollQueryFetchSearchResult> searchActionListener) {
        searchTransportService.sendExecuteScrollFetch(connection, internalRequest, task, searchActionListener);
    }

    @Override
    protected SearchPhase moveToNextPhase(BiFunction<String, String, DiscoveryNode> clusterNodeLookup) {
        return sendResponsePhase(searchPhaseController.reducedQueryPhase(queryFetchResults.asList(), true), queryFetchResults);
    }

    @Override
    protected void onFirstPhaseResult(int shardId, ScrollQueryFetchSearchResult result) {
        queryFetchResults.setOnce(shardId, result.result());
    }
}
