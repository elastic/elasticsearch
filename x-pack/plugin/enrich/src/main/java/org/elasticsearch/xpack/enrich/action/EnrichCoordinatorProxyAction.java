/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import org.apache.logging.log4j.util.BiConsumer;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.CoordinatorStats;
import org.elasticsearch.xpack.enrich.EnrichPlugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * An internal action to locally manage the load of the search requests that originate from the enrich processor.
 * This is because the enrich processor executes asynchronously and a bulk request could easily overload
 * the search tp.
 */
public class EnrichCoordinatorProxyAction extends ActionType<SearchResponse> {

    public static final EnrichCoordinatorProxyAction INSTANCE = new EnrichCoordinatorProxyAction();
    public static final String NAME = "indices:data/read/xpack/enrich/coordinate_lookups";

    private EnrichCoordinatorProxyAction() {
        super(NAME, SearchResponse::new);
    }

    public static class TransportAction extends HandledTransportAction<SearchRequest, SearchResponse> {

        private final Coordinator coordinator;

        @Inject
        public TransportAction(TransportService transportService, ActionFilters actionFilters, Coordinator coordinator) {
            super(NAME, transportService, actionFilters, SearchRequest::new);
            this.coordinator = coordinator;
        }

        @Override
        protected void doExecute(Task task, SearchRequest request, ActionListener<SearchResponse> listener) {
            // Write tp is expected when executing enrich processor from index / bulk api
            // System_write is expected when executing enrich against system indices
            // Management tp is expected when executing enrich processor from ingest simulate api
            // Search tp is allowed for now - After enriching, the remaining parts of the pipeline are processed on the
            // search thread, which could end up here again if there is more than one enrich processor in a pipeline.
            assert Thread.currentThread().getName().contains(ThreadPool.Names.WRITE)
                || Thread.currentThread().getName().contains(ThreadPool.Names.SYSTEM_WRITE)
                || Thread.currentThread().getName().contains(ThreadPool.Names.SEARCH)
                || Thread.currentThread().getName().contains(ThreadPool.Names.MANAGEMENT);
            coordinator.schedule(request, listener);
        }
    }

    public static class Coordinator {

        final BiConsumer<MultiSearchRequest, BiConsumer<MultiSearchResponse, Exception>> lookupFunction;
        final int maxLookupsPerRequest;
        final int maxNumberOfConcurrentRequests;
        final int queueCapacity;
        final BlockingQueue<Slot> queue;
        final Semaphore remoteRequestPermits;
        final LongAdder remoteRequestsTotal = new LongAdder();
        final LongAdder executedSearchesTotal = new LongAdder();

        public Coordinator(Client client, Settings settings) {
            this(
                lookupFunction(client),
                EnrichPlugin.COORDINATOR_PROXY_MAX_LOOKUPS_PER_REQUEST.get(settings),
                EnrichPlugin.COORDINATOR_PROXY_MAX_CONCURRENT_REQUESTS.get(settings),
                EnrichPlugin.COORDINATOR_PROXY_QUEUE_CAPACITY.get(settings)
            );
        }

        Coordinator(
            BiConsumer<MultiSearchRequest, BiConsumer<MultiSearchResponse, Exception>> lookupFunction,
            int maxLookupsPerRequest,
            int maxNumberOfConcurrentRequests,
            int queueCapacity
        ) {
            this.lookupFunction = lookupFunction;
            this.maxLookupsPerRequest = maxLookupsPerRequest;
            this.maxNumberOfConcurrentRequests = maxNumberOfConcurrentRequests;
            this.queueCapacity = queueCapacity;
            this.queue = new ArrayBlockingQueue<>(queueCapacity);
            this.remoteRequestPermits = new Semaphore(maxNumberOfConcurrentRequests);
        }

        void schedule(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
            // Use offer(...) instead of put(...). We are on a write thread and blocking here can be dangerous,
            // especially since the logic to kick off draining the queue is located right after this section. If we
            // cannot insert a request to the queue, we should reject the document with a 429 error code.
            boolean accepted = queue.offer(new Slot(searchRequest, listener));
            int queueSize = queue.size();

            // Coordinate lookups no matter what, even if queues were full. Search threads should be draining the queue,
            // but they may be busy with processing the remaining work for enrich results. If there is more than one
            // enrich processor in a pipeline, those search threads may find themselves here again before they can
            // coordinate the next set of lookups.
            coordinateLookups();

            if (accepted == false) {
                listener.onFailure(
                    new EsRejectedExecutionException(
                        "Could not perform enrichment, enrich coordination queue at capacity [" + queueSize + "/" + queueCapacity + "]"
                    )
                );
            }
        }

        CoordinatorStats getStats(String nodeId) {
            return new CoordinatorStats(
                nodeId,
                queue.size(),
                getRemoteRequestsCurrent(),
                remoteRequestsTotal.longValue(),
                executedSearchesTotal.longValue()
            );
        }

        int getRemoteRequestsCurrent() {
            return maxNumberOfConcurrentRequests - remoteRequestPermits.availablePermits();
        }

        void coordinateLookups() {
            while (true) {
                if (remoteRequestPermits.tryAcquire() == false) {
                    return;
                }

                final List<Slot> slots = new ArrayList<>(Math.min(queue.size(), maxLookupsPerRequest));
                if (queue.drainTo(slots, maxLookupsPerRequest) == 0) {
                    remoteRequestPermits.release();
                    return;
                }
                assert slots.isEmpty() == false;
                remoteRequestsTotal.increment();
                final MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
                slots.forEach(slot -> multiSearchRequest.add(slot.searchRequest));
                lookupFunction.accept(multiSearchRequest, (response, e) -> handleResponse(slots, response, e));
            }
        }

        void handleResponse(List<Slot> slots, MultiSearchResponse response, Exception e) {
            remoteRequestPermits.release();
            executedSearchesTotal.add(slots.size());

            if (response != null) {
                assert slots.size() == response.getResponses().length;
                for (int i = 0; i < response.getResponses().length; i++) {
                    MultiSearchResponse.Item responseItem = response.getResponses()[i];
                    Slot slot = slots.get(i);

                    if (responseItem.isFailure()) {
                        slot.actionListener.onFailure(responseItem.getFailure());
                    } else {
                        slot.actionListener.onResponse(responseItem.getResponse());
                    }
                }
            } else if (e != null) {
                slots.forEach(slot -> slot.actionListener.onFailure(e));
            } else {
                throw new AssertionError("no response and no error");
            }

            // There may be room to for a new request now that numberOfOutstandingRequests has been decreased:
            coordinateLookups();
        }

        static class Slot {

            final SearchRequest searchRequest;
            final ActionListener<SearchResponse> actionListener;

            Slot(SearchRequest searchRequest, ActionListener<SearchResponse> actionListener) {
                this.searchRequest = Objects.requireNonNull(searchRequest);
                this.actionListener = Objects.requireNonNull(actionListener);
            }
        }

        static BiConsumer<MultiSearchRequest, BiConsumer<MultiSearchResponse, Exception>> lookupFunction(ElasticsearchClient client) {
            return (request, consumer) -> {
                int slot = 0;
                final Map<String, List<Tuple<Integer, SearchRequest>>> itemsPerIndex = new HashMap<>();
                for (SearchRequest searchRequest : request.requests()) {
                    List<Tuple<Integer, SearchRequest>> items = itemsPerIndex.computeIfAbsent(
                        searchRequest.indices()[0],
                        k -> new ArrayList<>()
                    );
                    items.add(new Tuple<>(slot, searchRequest));
                    slot++;
                }

                final AtomicInteger counter = new AtomicInteger(0);
                final ConcurrentMap<String, Tuple<MultiSearchResponse, Exception>> shardResponses = new ConcurrentHashMap<>();
                for (Map.Entry<String, List<Tuple<Integer, SearchRequest>>> entry : itemsPerIndex.entrySet()) {
                    final String enrichIndexName = entry.getKey();
                    final List<Tuple<Integer, SearchRequest>> enrichIndexRequestsAndSlots = entry.getValue();
                    ActionListener<MultiSearchResponse> listener = ActionListener.wrap(response -> {
                        shardResponses.put(enrichIndexName, new Tuple<>(response, null));
                        if (counter.incrementAndGet() == itemsPerIndex.size()) {
                            consumer.accept(reduce(request.requests().size(), itemsPerIndex, shardResponses), null);
                        }
                    }, e -> {
                        shardResponses.put(enrichIndexName, new Tuple<>(null, e));
                        if (counter.incrementAndGet() == itemsPerIndex.size()) {
                            consumer.accept(reduce(request.requests().size(), itemsPerIndex, shardResponses), null);
                        }
                    });

                    MultiSearchRequest mrequest = new MultiSearchRequest();
                    enrichIndexRequestsAndSlots.stream().map(Tuple::v2).forEach(mrequest::add);
                    client.execute(EnrichShardMultiSearchAction.INSTANCE, new EnrichShardMultiSearchAction.Request(mrequest), listener);
                }
            };
        }

        static MultiSearchResponse reduce(
            int numRequest,
            Map<String, List<Tuple<Integer, SearchRequest>>> itemsPerIndex,
            Map<String, Tuple<MultiSearchResponse, Exception>> shardResponses
        ) {
            MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[numRequest];
            for (Map.Entry<String, Tuple<MultiSearchResponse, Exception>> rspEntry : shardResponses.entrySet()) {
                List<Tuple<Integer, SearchRequest>> reqSlots = itemsPerIndex.get(rspEntry.getKey());
                if (rspEntry.getValue().v1() != null) {
                    MultiSearchResponse shardResponse = rspEntry.getValue().v1();
                    for (int i = 0; i < shardResponse.getResponses().length; i++) {
                        int slot = reqSlots.get(i).v1();
                        items[slot] = shardResponse.getResponses()[i];
                    }
                } else if (rspEntry.getValue().v2() != null) {
                    Exception e = rspEntry.getValue().v2();
                    for (Tuple<Integer, SearchRequest> originSlot : reqSlots) {
                        items[originSlot.v1()] = new MultiSearchResponse.Item(null, e);
                    }
                } else {
                    throw new AssertionError();
                }
            }
            return new MultiSearchResponse(items, 1L);
        }

    }

}
