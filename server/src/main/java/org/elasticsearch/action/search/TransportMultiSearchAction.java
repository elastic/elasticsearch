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
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

public class TransportMultiSearchAction extends HandledTransportAction<MultiSearchRequest, MultiSearchResponse> {

    private final int allocatedProcessors;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final LongSupplier relativeTimeProvider;
    private final NodeClient client;

    @Inject
    public TransportMultiSearchAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                      ClusterService clusterService, ActionFilters actionFilters, NodeClient client) {
        super(MultiSearchAction.NAME, transportService, actionFilters, (Writeable.Reader<MultiSearchRequest>) MultiSearchRequest::new);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.allocatedProcessors = EsExecutors.allocatedProcessors(settings);
        this.relativeTimeProvider = System::nanoTime;
        this.client = client;
    }

    TransportMultiSearchAction(ThreadPool threadPool, ActionFilters actionFilters, TransportService transportService,
                               ClusterService clusterService, int allocatedProcessors,
                               LongSupplier relativeTimeProvider, NodeClient client) {
        super(MultiSearchAction.NAME, transportService, actionFilters, (Writeable.Reader<MultiSearchRequest>) MultiSearchRequest::new);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.allocatedProcessors = allocatedProcessors;
        this.relativeTimeProvider = relativeTimeProvider;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
        final long relativeStartTime = relativeTimeProvider.getAsLong();

        ClusterState clusterState = clusterService.state();
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

        int maxConcurrentSearches = request.maxConcurrentSearchRequests();
        if (maxConcurrentSearches == MultiSearchRequest.MAX_CONCURRENT_SEARCH_REQUESTS_DEFAULT) {
            maxConcurrentSearches = defaultMaxConcurrentSearches(allocatedProcessors, clusterState);
        }

        Queue<SearchRequestSlot> searchRequestSlots = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < request.requests().size(); i++) {
            SearchRequest searchRequest = request.requests().get(i);
            searchRequest.setParentTask(client.getLocalNodeId(), task.getId());
            searchRequestSlots.add(new SearchRequestSlot(searchRequest, i));
        }

        int numRequests = request.requests().size();
        final AtomicArray<MultiSearchResponse.Item> responses = new AtomicArray<>(numRequests);
        final AtomicInteger responseCounter = new AtomicInteger(numRequests);
        int numConcurrentSearches = Math.min(numRequests, maxConcurrentSearches);
        for (int i = 0; i < numConcurrentSearches; i++) {
            executeSearch(searchRequestSlots, responses, responseCounter, listener, relativeStartTime);
        }
    }

    /*
     * This is not perfect and makes a big assumption, that all nodes have the same thread pool size / have the number of processors and
     * that shard of the indices the search requests go to are more or less evenly distributed across all nodes in the cluster. But I think
     * it is a good enough default for most cases, if not then the default should be overwritten in the request itself.
     */
    static int defaultMaxConcurrentSearches(final int allocatedProcessors, final ClusterState state) {
        int numDateNodes = state.getNodes().getDataNodes().size();
        // we bound the default concurrency to preserve some search thread pool capacity for other searches
        final int defaultSearchThreadPoolSize = Math.min(ThreadPool.searchThreadPoolSize(allocatedProcessors), 10);
        return Math.max(1, numDateNodes * defaultSearchThreadPoolSize);
    }

    /**
     * Executes a single request from the queue of requests. When a request finishes, another request is taken from the queue. When a
     * request is executed, a permit is taken on the specified semaphore, and released as each request completes.
     *
     * @param requests the queue of multi-search requests to execute
     * @param responses atomic array to hold the responses corresponding to each search request slot
     * @param responseCounter incremented on each response
     * @param listener the listener attached to the multi-search request
     */
    void executeSearch(
            final Queue<SearchRequestSlot> requests,
            final AtomicArray<MultiSearchResponse.Item> responses,
            final AtomicInteger responseCounter,
            final ActionListener<MultiSearchResponse> listener,
            final long relativeStartTime) {
        SearchRequestSlot request = requests.poll();
        if (request == null) {
            /*
             * The number of times that we poll an item from the queue here is the minimum of the number of requests and the maximum number
             * of concurrent requests. At first glance, it appears that we should never poll from the queue and not obtain a request given
             * that we only poll here no more times than the number of requests. However, this is not the only consumer of this queue as
             * earlier requests that have already completed will poll from the queue too and they could complete before later polls are
             * invoked here. Thus, it can be the case that we poll here and the queue was empty.
             */
            return;
        }

        /*
         * With a request in hand, we are now prepared to execute the search request. There are two possibilities, either we go asynchronous
         * or we do not (this can happen if the request does not resolve to any shards). If we do not go asynchronous, we are going to come
         * back on the same thread that attempted to execute the search request. At this point, or any other point where we come back on the
         * same thread as when the request was submitted, we should not recurse lest we might descend into a stack overflow. To avoid this,
         * when we handle the response rather than going recursive, we fork to another thread, otherwise we recurse.
         */
        final Thread thread = Thread.currentThread();
        client.search(request.request, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(final SearchResponse searchResponse) {
                handleResponse(request.responseSlot, new MultiSearchResponse.Item(searchResponse, null));
            }

            @Override
            public void onFailure(final Exception e) {
                handleResponse(request.responseSlot, new MultiSearchResponse.Item(null, e));
            }

            private void handleResponse(final int responseSlot, final MultiSearchResponse.Item item) {
                responses.set(responseSlot, item);
                if (responseCounter.decrementAndGet() == 0) {
                    assert requests.isEmpty();
                    finish();
                } else {
                    if (thread == Thread.currentThread()) {
                        // we are on the same thread, we need to fork to another thread to avoid recursive stack overflow on a single thread
                        threadPool.generic()
                                .execute(() -> executeSearch(requests, responses, responseCounter, listener, relativeStartTime));
                    } else {
                        // we are on a different thread (we went asynchronous), it's safe to recurse
                        executeSearch(requests, responses, responseCounter, listener, relativeStartTime);
                    }
                }
            }

            private void finish() {
                listener.onResponse(new MultiSearchResponse(responses.toArray(new MultiSearchResponse.Item[responses.length()]),
                        buildTookInMillis()));
            }

            /**
             * Builds how long it took to execute the msearch.
             */
            private long buildTookInMillis() {
                return TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - relativeStartTime);
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
