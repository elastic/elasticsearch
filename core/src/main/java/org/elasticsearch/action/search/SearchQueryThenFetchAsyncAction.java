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
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.transport.Transport;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;

class SearchQueryThenFetchAsyncAction extends AbstractSearchAsyncAction<QuerySearchResultProvider> {
    private final SearchPhaseController searchPhaseController;

    SearchQueryThenFetchAsyncAction(Logger logger, SearchTransportService searchTransportService,
                                    Function<String, Transport.Connection> nodeIdToConnection,
                                    Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts,
                                    SearchPhaseController searchPhaseController, Executor executor,
                                    SearchRequest request, ActionListener<SearchResponse> listener,
                                    GroupShardsIterator shardsIts, long startTime, long clusterStateVersion,
                                    SearchTask task) {
        super(logger, searchTransportService, nodeIdToConnection, aliasFilter, concreteIndexBoosts, executor, request, listener,
            shardsIts, startTime, clusterStateVersion, task);
        this.searchPhaseController = searchPhaseController;
    }

    @Override
    protected String initialPhaseName() {
        return "query";
    }

    @Override
    protected void sendExecuteFirstPhase(Transport.Connection connection, ShardSearchTransportRequest request,
                                         ActionListener<QuerySearchResultProvider> listener) {
        searchTransportService.sendExecuteQuery(connection, request, task, listener);
    }

    @Override
    protected CheckedRunnable<Exception> getNextPhase(AtomicArray<QuerySearchResultProvider> initialResults) {
        return new FetchPhase(initialResults, searchPhaseController);
    }
}
