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
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class TransportMultiSearchAction extends HandledTransportAction<MultiSearchRequest, MultiSearchResponse> {

    private final int availableProcessors;
    private final ClusterService clusterService;
    private final TransportAction<SearchRequest, SearchResponse> searchAction;

    @Inject
    public TransportMultiSearchAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                ClusterService clusterService, TransportSearchAction searchAction,
                                ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, MultiSearchAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, MultiSearchRequest::new);
        this.clusterService = clusterService;
        this.searchAction = searchAction;
        this.availableProcessors = EsExecutors.boundedNumberOfProcessors(settings);
    }

    // For testing only:
    TransportMultiSearchAction(ThreadPool threadPool, ActionFilters actionFilters, TransportService transportService,
                               ClusterService clusterService, TransportAction<SearchRequest, SearchResponse> searchAction,
                               IndexNameExpressionResolver indexNameExpressionResolver, int availableProcessors) {
        super(Settings.EMPTY, MultiSearchAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, MultiSearchRequest::new);
        this.clusterService = clusterService;
        this.searchAction = searchAction;
        this.availableProcessors = availableProcessors;
    }

    @Override
    protected void doExecute(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
        ClusterState clusterState = clusterService.state();
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

        int maxConcurrentSearches = request.maxConcurrentSearchRequests();
        if (maxConcurrentSearches == 0) {
            maxConcurrentSearches = defaultMaxConcurrentSearches(availableProcessors, clusterState);
        }

        Queue<SearchRequestSlot> searchRequestSlots = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < request.requests().size(); i++) {
            SearchRequest searchRequest = request.requests().get(i);
            searchRequestSlots.add(new SearchRequestSlot(searchRequest, i));
        }

        int numRequests = request.requests().size();
        final AtomicArray<MultiSearchResponse.Item> responses = new AtomicArray<>(numRequests);
        final AtomicInteger responseCounter = new AtomicInteger(numRequests);
        int numConcurrentSearches = Math.min(numRequests, maxConcurrentSearches);
        for (int i = 0; i < numConcurrentSearches; i++) {
            executeSearch(searchRequestSlots, responses, responseCounter, listener);
        }
    }

    /*
     * This is not perfect and makes a big assumption, that all nodes have the same thread pool size / have the number
     * of processors and that shard of the indices the search requests go to are more or less evenly distributed across
     * all nodes in the cluster. But I think it is a good enough default for most cases, if not then the default should be
     * overwritten in the request itself.
     */
    static int defaultMaxConcurrentSearches(int availableProcessors, ClusterState state) {
        int numDateNodes = state.getNodes().getDataNodes().size();
        // availableProcessors will never be larger than 32, so max defaultMaxConcurrentSearches will never be larger than 49,
        // but we don't know about about other search requests that are being executed so lets cap at 10 per node
        int defaultSearchThreadPoolSize = Math.min(ThreadPool.searchThreadPoolSize(availableProcessors), 10);
        return Math.max(1, numDateNodes * defaultSearchThreadPoolSize);
    }

    void executeSearch(Queue<SearchRequestSlot> requests, AtomicArray<MultiSearchResponse.Item> responses,
                       AtomicInteger responseCounter, ActionListener<MultiSearchResponse> listener) {
        SearchRequestSlot request = requests.poll();
        if (request == null) {
            // Ok... so there're no more requests then this is ok, we're then waiting for running requests to complete
            return;
        }
        searchAction.execute(request.request, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                responses.set(request.responseSlot, new MultiSearchResponse.Item(searchResponse, null));
                handleResponse();
            }

            @Override
            public void onFailure(Throwable e) {
                responses.set(request.responseSlot, new MultiSearchResponse.Item(null, e));
                handleResponse();
            }

            private void handleResponse() {
                if (responseCounter.decrementAndGet() == 0) {
                    listener.onResponse(new MultiSearchResponse(responses.toArray(new MultiSearchResponse.Item[responses.length()])));
                } else {
                    executeSearch(requests, responses, responseCounter, listener);
                }
            }
        });
    }

    final static class SearchRequestSlot {

        final SearchRequest request;
        final int responseSlot;

        SearchRequestSlot(SearchRequest request, int responseSlot) {
            this.request = request;
            this.responseSlot = responseSlot;
        }
    }
}
