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
        this.availableProcessors = EsExecutors.numberOfProcessors(settings);
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
            /*
             * The number of times that we poll an item from the queue here is the minimum of the number of requests and the maximum number
             * of concurrent requests. At first glance, it appears that we should never poll from the queue and not obtain a request given
             * that we only poll here no more times than the number of requests. However, this is not the only consumer of this queue as
             * earlier requests that have already completed will poll from the queue too and they could complete before later polls are
             * invoked here. Thus, it can be the case that we poll here and and the queue was empty.
             */
            return;
        }

        /*
         * With a request in hand, we are going to asynchronously execute the search request. When the search request returns, either with
         * a success or with a failure, we set the response corresponding to the request. Then, we enter a loop that repeatedly pulls
         * requests off the request queue, this time only setting the response corresponding to the request.
         */
        searchAction.execute(request.request, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(final SearchResponse searchResponse) {
                handleResponse(request.responseSlot, new MultiSearchResponse.Item(searchResponse, null));
                executeSearchLoop();
            }

            @Override
            public void onFailure(final Exception e) {
                handleResponse(request.responseSlot, new MultiSearchResponse.Item(null, e));
                executeSearchLoop();
            }

            private void handleResponse(final int responseSlot, final MultiSearchResponse.Item item) {
                responses.set(responseSlot, item);
                if (responseCounter.decrementAndGet() == 0) {
                    assert requests.isEmpty();
                    finish();
                }
            }

            private void finish() {
                listener.onResponse(new MultiSearchResponse(responses.toArray(new MultiSearchResponse.Item[responses.length()])));
            }

            private void executeSearchLoop() {
                SearchRequestSlot next;
                while ((next = requests.poll()) != null) {
                    final int nextResponseSlot = next.responseSlot;
                    searchAction.execute(next.request, new ActionListener<SearchResponse>() {
                        @Override
                        public void onResponse(SearchResponse searchResponse) {
                            handleResponse(nextResponseSlot, new MultiSearchResponse.Item(searchResponse, null));
                        }

                        @Override
                        public void onFailure(Exception e) {
                            handleResponse(nextResponseSlot, new MultiSearchResponse.Item(null, e));
                        }
                    });
                }
            }
        });
    }

    static final class SearchRequestSlot {

        final SearchRequest request;
        final int responseSlot;

        SearchRequestSlot(SearchRequest request, int responseSlot) {
            this.request = request;
            this.responseSlot = responseSlot;
        }
    }
}
