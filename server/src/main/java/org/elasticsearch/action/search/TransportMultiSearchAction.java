/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

public class TransportMultiSearchAction extends HandledTransportAction<MultiSearchRequest, MultiSearchResponse> {

    public static final String NAME = "indices:data/read/msearch";
    public static final ActionType<MultiSearchResponse> TYPE = new ActionType<>(NAME);
    private static final Logger logger = LogManager.getLogger(TransportMultiSearchAction.class);
    private final int allocatedProcessors;
    private final ClusterService clusterService;
    private final LongSupplier relativeTimeProvider;
    private final NodeClient client;

    @Inject
    public TransportMultiSearchAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        NodeClient client
    ) {
        super(TYPE.name(), transportService, actionFilters, MultiSearchRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.allocatedProcessors = EsExecutors.allocatedProcessors(settings);
        this.relativeTimeProvider = System::nanoTime;
        this.client = client;
    }

    TransportMultiSearchAction(
        ActionFilters actionFilters,
        TransportService transportService,
        ClusterService clusterService,
        int allocatedProcessors,
        LongSupplier relativeTimeProvider,
        NodeClient client
    ) {
        super(TYPE.name(), transportService, actionFilters, MultiSearchRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
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
        final int defaultSearchThreadPoolSize = Math.min(ThreadPool.searchOrGetThreadPoolSize(allocatedProcessors), 10);
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
        final long relativeStartTime
    ) {
        /*
         * The number of times that we poll an item from the queue here is the minimum of the number of requests and the maximum number
         * of concurrent requests. At first glance, it appears that we should never poll from the queue and not obtain a request given
         * that we only poll here no more times than the number of requests. However, this is not the only consumer of this queue as
         * earlier requests that have already completed will poll from the queue too, and they could complete before later polls are
         * invoked here. Thus, it can be the case that we poll here and the queue was empty.
         */
        SearchRequestSlot request = requests.poll();
        // If we have another request to execute, we execute it. If the execution forked #doExecuteSearch will return false and will
        // recursively call this method again eventually. If it did not fork and was able to execute the search right away #doExecuteSearch
        // will return true, in which case we continue and run the next search request here.
        while (request != null && doExecuteSearch(requests, responses, responseCounter, relativeStartTime, request, listener)) {
            request = requests.poll();
        }
    }

    private boolean doExecuteSearch(
        Queue<SearchRequestSlot> requests,
        AtomicArray<MultiSearchResponse.Item> responses,
        AtomicInteger responseCounter,
        long relativeStartTime,
        SearchRequestSlot request,
        ActionListener<MultiSearchResponse> listener
    ) {
        final SubscribableListener<MultiSearchResponse.Item> subscribeListener = new SubscribableListener<>();
        client.search(request.request, subscribeListener.safeMap(searchResponse -> {
            searchResponse.mustIncRef(); // acquire reference on behalf of MultiSearchResponse.Item below
            return new MultiSearchResponse.Item(searchResponse, null);
        }));
        final ActionListener<MultiSearchResponse.Item> responseListener = new ActionListener<>() {
            @Override
            public void onResponse(final MultiSearchResponse.Item searchResponse) {
                handleResponse(request.responseSlot, searchResponse);
            }

            @Override
            public void onFailure(final Exception e) {
                if (ExceptionsHelper.status(e).getStatus() >= 500 && ExceptionsHelper.isNodeOrShardUnavailableTypeException(e) == false) {
                    logger.warn("TransportMultiSearchAction failure", e);
                }
                handleResponse(request.responseSlot, new MultiSearchResponse.Item(null, e));
            }

            private void handleResponse(final int responseSlot, final MultiSearchResponse.Item item) {
                responses.set(responseSlot, item);
                if (responseCounter.decrementAndGet() == 0) {
                    assert requests.isEmpty();
                    finish();
                }
            }

            private void finish() {
                ActionListener.respondAndRelease(
                    listener,
                    new MultiSearchResponse(responses.toArray(new MultiSearchResponse.Item[responses.length()]), buildTookInMillis())
                );
            }

            /**
             * Builds how long it took to execute the msearch.
             */
            private long buildTookInMillis() {
                return TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - relativeStartTime);
            }
        };
        if (subscribeListener.isDone()) {
            subscribeListener.addListener(responseListener);
            return true;
        }
        // we went forked and have to check if there's more searches to execute after we're done with this search
        subscribeListener.addListener(
            ActionListener.runAfter(
                responseListener,
                () -> executeSearch(requests, responses, responseCounter, listener, relativeStartTime)
            )
        );
        return false;
    }

    record SearchRequestSlot(SearchRequest request, int responseSlot) {

    }
}
