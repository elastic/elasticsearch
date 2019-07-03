/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.apache.logging.log4j.util.BiConsumer;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.enrich.EnrichPlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An internal action to locally manage the load of the search requests that originate from the enrich processor.
 * This is because the enrich processor executes asynchronously and a bulk request could easily overload
 * the search tp.
 */
public class CoordinatorProxyAction extends Action<SearchResponse> {

    public static final CoordinatorProxyAction INSTANCE = new CoordinatorProxyAction();
    public static final String NAME = "indices:data/read/xpack/enrich/coordinate_lookups";

    private CoordinatorProxyAction() {
        super(NAME, SearchResponse::new);
    }

    public static class TransportAction extends HandledTransportAction<SearchRequest, SearchResponse> {

        private final Coordinator coordinator;

        @Inject
        public TransportAction(TransportService transportService, ActionFilters actionFilters, Coordinator coordinator) {
            super(NAME, transportService, actionFilters, (Writeable.Reader<SearchRequest>) SearchRequest::new);
            this.coordinator = coordinator;
        }

        @Override
        protected void doExecute(Task task, SearchRequest request, ActionListener<SearchResponse> listener) {
            assert Thread.currentThread().getName().contains(ThreadPool.Names.WRITE);
            coordinator.schedule(request, listener);
        }
    }

    public static class Coordinator {

        final BiConsumer<MultiSearchRequest, BiConsumer<MultiSearchResponse, Exception>> lookupFunction;
        final int maxLookupsPerRequest;
        final int maxNumberOfConcurrentRequests;
        final BlockingQueue<Slot> queue;
        final AtomicInteger numberOfOutstandingRequests = new AtomicInteger(0);

        public Coordinator(Client client, Settings settings) {
            this(
                (request, consumer) -> client.multiSearch(request,
                    ActionListener.wrap(response -> consumer.accept(response, null), e -> consumer.accept(null, e))),
                EnrichPlugin.COORDINATOR_PROXY_MAX_LOOKUPS_PER_REQUEST.get(settings),
                EnrichPlugin.COORDINATOR_PROXY_MAX_CONCURRENT_REQUESTS.get(settings),
                EnrichPlugin.COORDINATOR_PROXY_QUEUE_CAPACITY.get(settings)
            );
        }

        Coordinator(BiConsumer<MultiSearchRequest, BiConsumer<MultiSearchResponse, Exception>> lookupFunction,
                    int maxLookupsPerRequest, int maxNumberOfConcurrentRequests, int queueCapacity) {
            this.lookupFunction = lookupFunction;
            this.maxLookupsPerRequest = maxLookupsPerRequest;
            this.maxNumberOfConcurrentRequests = maxNumberOfConcurrentRequests;
            this.queue = new ArrayBlockingQueue<>(queueCapacity);
        }

        void schedule(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
            // Use put(...), because if queue is full then this method will wait until a free slot becomes available
            // The calling thread here is a write thread (write tp is used by ingest) and
            // this will create natural back pressure from the enrich processor.
            // If there are no write threads available then write requests with ingestion will fail with 429 error code.
            try {
                queue.put(new Slot(searchRequest, listener));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("unable to add item to queue", e);
            }
            coordinateLookups();
        }

        synchronized void coordinateLookups() {
            while (queue.isEmpty() == false &&
                numberOfOutstandingRequests.get() < maxNumberOfConcurrentRequests) {

                final List<Slot> slots = new ArrayList<>();
                queue.drainTo(slots, maxLookupsPerRequest);
                final MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
                slots.forEach(slot -> multiSearchRequest.add(slot.searchRequest));

                numberOfOutstandingRequests.incrementAndGet();
                lookupFunction.accept(multiSearchRequest, (response, e) -> {
                    handleResponse(slots, response, e);
                });
            }
        }

        void handleResponse(List<Slot> slots, MultiSearchResponse response, Exception e) {
            numberOfOutstandingRequests.decrementAndGet();

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

            // There may be room to for a new request now the numberOfOutstandingRequests has been decreased:
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

    }

}
