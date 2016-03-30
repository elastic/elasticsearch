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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.search.action.SearchTransportService;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.action.search.SearchType.QUERY_AND_FETCH;
import static org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH;

/**
 *
 */
public class TransportSearchAction extends HandledTransportAction<SearchRequest, SearchResponse> {

    /** The maximum number of shards for a single search request. */
    public static final Setting<Long> SHARD_COUNT_LIMIT_SETTING = Setting.longSetting(
            "action.search.shard_count.limit", 1000L, 1L, Property.Dynamic, Property.NodeScope);

    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;
    private final SearchPhaseController searchPhaseController;

    @Inject
    public TransportSearchAction(Settings settings, ThreadPool threadPool, SearchPhaseController searchPhaseController,
                                 TransportService transportService, SearchTransportService searchTransportService,
                                 ClusterService clusterService, ActionFilters actionFilters, IndexNameExpressionResolver
                                             indexNameExpressionResolver) {
        super(settings, SearchAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, SearchRequest::new);
        this.searchPhaseController = searchPhaseController;
        this.searchTransportService = searchTransportService;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        // optimize search type for cases where there is only one shard group to search on
        try {
            ClusterState clusterState = clusterService.state();
            String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, searchRequest);
            Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState,
                    searchRequest.routing(), searchRequest.indices());
            int shardCount = clusterService.operationRouting().searchShardsCount(clusterState, concreteIndices, routingMap);
            if (shardCount == 1) {
                // if we only have one group, then we always want Q_A_F, no need for DFS, and no need to do THEN since we hit one shard
                searchRequest.searchType(QUERY_AND_FETCH);
            }
            if (searchRequest.isSuggestOnly()) {
                // disable request cache if we have only suggest
                searchRequest.requestCache(false);
                switch (searchRequest.searchType()) {
                    case DFS_QUERY_AND_FETCH:
                    case DFS_QUERY_THEN_FETCH:
                        // convert to Q_T_F if we have only suggest
                        searchRequest.searchType(QUERY_THEN_FETCH);
                        break;
                }
            }
        } catch (IndexNotFoundException | IndexClosedException e) {
            // ignore these failures, we will notify the search response if its really the case from the actual action
        } catch (Exception e) {
            logger.debug("failed to optimize search type, continue as normal", e);
        }

        AbstractSearchAsyncAction searchAsyncAction;
        switch(searchRequest.searchType()) {
            case DFS_QUERY_THEN_FETCH:
                searchAsyncAction = new SearchDfsQueryThenFetchAsyncAction(logger, searchTransportService, clusterService,
                        indexNameExpressionResolver, searchPhaseController, threadPool, searchRequest, listener);
                break;
            case QUERY_THEN_FETCH:
                searchAsyncAction = new SearchQueryThenFetchAsyncAction(logger, searchTransportService, clusterService,
                        indexNameExpressionResolver, searchPhaseController, threadPool, searchRequest, listener);
                break;
            case DFS_QUERY_AND_FETCH:
                searchAsyncAction = new SearchDfsQueryAndFetchAsyncAction(logger, searchTransportService, clusterService,
                        indexNameExpressionResolver, searchPhaseController, threadPool, searchRequest, listener);
                break;
            case QUERY_AND_FETCH:
                searchAsyncAction = new SearchQueryAndFetchAsyncAction(logger, searchTransportService, clusterService,
                        indexNameExpressionResolver, searchPhaseController, threadPool, searchRequest, listener);
                break;
            default:
                throw new IllegalStateException("Unknown search type: [" + searchRequest.searchType() + "]");
        }
        searchAsyncAction.start();
    }
}
