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
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.transport.Transport;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;

final class SearchQueryAndFetchAsyncAction extends AbstractSearchAsyncAction<QueryFetchSearchResult> {

    SearchQueryAndFetchAsyncAction(Logger logger, SearchTransportService searchTransportService,
                                   Function<String, Transport.Connection> nodeIdToConnection,
                                   Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts,
                                   SearchPhaseController searchPhaseController, Executor executor,
                                   SearchRequest request, ActionListener<SearchResponse> listener,
                                   GroupShardsIterator shardsIts, long startTime, long clusterStateVersion,
                                   SearchTask task) {
        super(logger, searchTransportService, nodeIdToConnection, aliasFilter, concreteIndexBoosts, searchPhaseController, executor,
                request, listener, shardsIts, startTime, clusterStateVersion, task);
    }

    @Override
    protected String initialPhaseName() {
        return "query_fetch";
    }

    @Override
    protected void sendExecuteFirstPhase(Transport.Connection connection, ShardSearchTransportRequest request,
                                         ActionListener<QueryFetchSearchResult> listener) {
        searchTransportService.sendExecuteFetch(connection, request, task, listener);
    }

    @Override
    protected CheckedRunnable<Exception> getNextPhase(AtomicArray<QueryFetchSearchResult> initialResults) {
        return () -> sendResponseAsync("fetch", searchPhaseController, null, initialResults, initialResults);
    }
}
