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

package org.elasticsearch.action.suggest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.*;

/**
 * Defines the transport of a suggestion request across the cluster.
 * Delegates to {@link TransportSuggestThenFetchAction} in case of multiple shards
 * and {@link TransportSuggestAndFetchAction} for single shard
 */
public class TransportSuggestAction extends HandledTransportAction<SuggestRequest, SuggestResponse> {

    private final TransportSuggestThenFetchAction suggestThenFetchAction;
    private final TransportSuggestAndFetchAction suggestAndFetchAction;
    private final ClusterService clusterService;

    @Inject
    public TransportSuggestAction(Settings settings, ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters,
                                  IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService,
                                  TransportSuggestThenFetchAction suggestThenFetchAction, TransportSuggestAndFetchAction suggestAndFetchAction) {
        super(settings, SuggestAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, SuggestRequest.class);
        this.clusterService = clusterService;
        this.suggestThenFetchAction = suggestThenFetchAction;
        this.suggestAndFetchAction = suggestAndFetchAction;
    }

    @Override
    protected void doExecute(SuggestRequest request, final ActionListener<SuggestResponse> listener) {
        final SearchRequest searchRequest = new SearchRequest(request.indices());
        if (request.suggest() != null) {
            searchRequest.source(request.suggest());
        }
        searchRequest.preference(request.preference());
        searchRequest.routing(request.routing());
        searchRequest.indicesOptions(request.indicesOptions());
        final ActionListener<SearchResponse> searchResponseActionListener = new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                Suggest suggest = searchResponse.getSuggest();
                if (suggest != null) {
                    listener.onResponse(new SuggestResponse(suggest));
                } else {
                    listener.onResponse(new SuggestResponse(new Suggest(Collections.<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>>emptyList())));
                }
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        };
        boolean singleShard = false;
        try {
            ClusterState clusterState = clusterService.state();
            String[] concreteIndices = indexNameExpressionResolver.concreteIndices(clusterState, searchRequest);
            Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, searchRequest.routing(), searchRequest.indices());
            int shardCount = clusterService.operationRouting().searchShardsCount(clusterState, concreteIndices, routingMap);
            if (shardCount == 1) {
                singleShard = true;
            }
        } catch (IndexNotFoundException | IndexClosedException e) {
            // ignore these failures, we will notify the search response if its really the case from the actual action
        } catch (Exception e) {
            logger.debug("failed to optimize search type, continue as normal", e);
        }
        if (singleShard) {
            suggestAndFetchAction.doExecute(searchRequest, searchResponseActionListener);
        } else {
            suggestThenFetchAction.doExecute(searchRequest, searchResponseActionListener);
        }
    }

}
