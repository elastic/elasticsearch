/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import org.apache.logging.log4j.util.BiConsumer;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.xpack.enrich.action.EnrichCoordinatorProxyAction.Coordinator;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class CoordinatorTests extends ESTestCase {

    public void testCoordinateLookups() {
        MockLookupFunction lookupFunction = new MockLookupFunction();
        Coordinator coordinator = new Coordinator(lookupFunction, 5, 1, 100);

        List<ActionListener<SearchResponse>> searchActionListeners = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            SearchRequest searchRequest = new SearchRequest("my-index");
            searchRequest.source().query(new MatchQueryBuilder("my_field", String.valueOf(i)));
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> actionListener = Mockito.mock(ActionListener.class);
            searchActionListeners.add(actionListener);
            coordinator.queue.add(new Coordinator.Slot(searchRequest, actionListener));
        }

        SearchRequest searchRequest = new SearchRequest("my-index");
        searchRequest.source().query(new MatchQueryBuilder("my_field", String.valueOf(10)));
        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> actionListener = Mockito.mock(ActionListener.class);
        searchActionListeners.add(actionListener);
        coordinator.schedule(searchRequest, actionListener);

        // First batch of search requests have been sent off:
        // (However still 5 should remain in the queue)
        assertThat(coordinator.queue.size(), equalTo(5));
        assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(1));
        assertThat(lookupFunction.capturedRequests.size(), equalTo(1));
        assertThat(lookupFunction.capturedRequests.getFirst().requests().size(), equalTo(5));

        // Nothing should happen now, because there is an outstanding request and max number of requests has been set to 1:
        coordinator.coordinateLookups();
        assertThat(coordinator.queue.size(), equalTo(5));
        assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(1));
        assertThat(lookupFunction.capturedRequests.size(), equalTo(1));

        SearchResponse emptyResponse = emptySearchResponse();
        // Replying a response and that should trigger another coordination round
        MultiSearchResponse.Item[] responseItems = new MultiSearchResponse.Item[5];
        for (int i = 0; i < 5; i++) {
            emptyResponse.incRef();
            responseItems[i] = new MultiSearchResponse.Item(emptyResponse, null);
        }
        emptyResponse.decRef();
        final MultiSearchResponse res1 = new MultiSearchResponse(responseItems, 1L);
        try {
            lookupFunction.capturedConsumers.getFirst().accept(res1, null);
            assertThat(coordinator.queue.size(), equalTo(0));
            assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(1));
            assertThat(lookupFunction.capturedRequests.size(), equalTo(2));

            // Replying last response, resulting in an empty queue and no outstanding requests.
            responseItems = new MultiSearchResponse.Item[5];
            for (int i = 0; i < 5; i++) {
                emptyResponse.incRef();
                responseItems[i] = new MultiSearchResponse.Item(emptyResponse, null);
            }
            var res2 = new MultiSearchResponse(responseItems, 1L);
            try {
                lookupFunction.capturedConsumers.get(1).accept(res2, null);
                assertThat(coordinator.queue.size(), equalTo(0));
                assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(0));
                assertThat(lookupFunction.capturedRequests.size(), equalTo(2));

                // All individual action listeners for the search requests should have been invoked:
                for (ActionListener<SearchResponse> searchActionListener : searchActionListeners) {
                    Mockito.verify(searchActionListener).onResponse(Mockito.eq(emptyResponse));
                }
            } finally {
                res2.decRef();
            }
        } finally {
            res1.decRef();
        }
    }

    public void testCoordinateLookupsMultiSearchError() {
        MockLookupFunction lookupFunction = new MockLookupFunction();
        Coordinator coordinator = new Coordinator(lookupFunction, 5, 1, 100);

        List<ActionListener<SearchResponse>> searchActionListeners = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            SearchRequest searchRequest = new SearchRequest("my-index");
            searchRequest.source().query(new MatchQueryBuilder("my_field", String.valueOf(i)));
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> actionListener = Mockito.mock(ActionListener.class);
            searchActionListeners.add(actionListener);
            coordinator.queue.add(new Coordinator.Slot(searchRequest, actionListener));
        }

        SearchRequest searchRequest = new SearchRequest("my-index");
        searchRequest.source().query(new MatchQueryBuilder("my_field", String.valueOf(5)));
        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> actionListener = Mockito.mock(ActionListener.class);
        searchActionListeners.add(actionListener);
        coordinator.schedule(searchRequest, actionListener);

        // First batch of search requests have been sent off:
        // (However still 5 should remain in the queue)
        assertThat(coordinator.queue.size(), equalTo(0));
        assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(1));
        assertThat(lookupFunction.capturedRequests.size(), equalTo(1));
        assertThat(lookupFunction.capturedRequests.getFirst().requests().size(), equalTo(5));

        RuntimeException e = new RuntimeException();
        lookupFunction.capturedConsumers.getFirst().accept(null, e);
        assertThat(coordinator.queue.size(), equalTo(0));
        assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(0));
        assertThat(lookupFunction.capturedRequests.size(), equalTo(1));

        // All individual action listeners for the search requests should have been invoked:
        for (ActionListener<SearchResponse> searchActionListener : searchActionListeners) {
            Mockito.verify(searchActionListener).onFailure(Mockito.eq(e));
        }
    }

    public void testCoordinateLookupsMultiSearchItemError() {
        MockLookupFunction lookupFunction = new MockLookupFunction();
        Coordinator coordinator = new Coordinator(lookupFunction, 5, 1, 100);

        List<ActionListener<SearchResponse>> searchActionListeners = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            SearchRequest searchRequest = new SearchRequest("my-index");
            searchRequest.source().query(new MatchQueryBuilder("my_field", String.valueOf(i)));
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> actionListener = Mockito.mock(ActionListener.class);
            searchActionListeners.add(actionListener);
            coordinator.queue.add(new Coordinator.Slot(searchRequest, actionListener));
        }

        SearchRequest searchRequest = new SearchRequest("my-index");
        searchRequest.source().query(new MatchQueryBuilder("my_field", String.valueOf(5)));
        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> actionListener = Mockito.mock(ActionListener.class);
        searchActionListeners.add(actionListener);
        coordinator.schedule(searchRequest, actionListener);

        // First batch of search requests have been sent off:
        // (However still 5 should remain in the queue)
        assertThat(coordinator.queue.size(), equalTo(0));
        assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(1));
        assertThat(lookupFunction.capturedRequests.size(), equalTo(1));
        assertThat(lookupFunction.capturedRequests.getFirst().requests().size(), equalTo(5));

        RuntimeException e = new RuntimeException();
        // Replying a response and that should trigger another coordination round
        MultiSearchResponse.Item[] responseItems = new MultiSearchResponse.Item[5];
        for (int i = 0; i < 5; i++) {
            responseItems[i] = new MultiSearchResponse.Item(null, e);
        }
        var res = new MultiSearchResponse(responseItems, 1L);
        try {
            lookupFunction.capturedConsumers.getFirst().accept(res, null);
            assertThat(coordinator.queue.size(), equalTo(0));
            assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(0));
            assertThat(lookupFunction.capturedRequests.size(), equalTo(1));

            // All individual action listeners for the search requests should have been invoked:
            for (ActionListener<SearchResponse> searchActionListener : searchActionListeners) {
                Mockito.verify(searchActionListener).onFailure(Mockito.eq(e));
            }
        } finally {
            res.decRef();
        }
    }

    public void testNoBlockingWhenQueueing() {
        MockLookupFunction lookupFunction = new MockLookupFunction();
        // Only one request allowed in flight. Queue size maxed at 1.
        Coordinator coordinator = new Coordinator(lookupFunction, 1, 1, 1);

        // Pre-load the queue to be at capacity and spoof the coordinator state to seem like max requests in flight.
        coordinator.queue.add(new Coordinator.Slot(new SearchRequest(), ActionListener.noop()));
        assertTrue(coordinator.remoteRequestPermits.tryAcquire());

        // Try to schedule an item into the coordinator, should emit an exception
        SearchRequest searchRequest = new SearchRequest();
        final AtomicReference<Exception> capturedException = new AtomicReference<>();
        coordinator.schedule(searchRequest, ActionListener.wrap(response -> {}, capturedException::set));

        // Ensure rejection since queue is full
        Exception rejectionException = capturedException.get();
        assertThat(rejectionException.getMessage(), containsString("Could not perform enrichment, enrich coordination queue at capacity"));

        // Ensure that nothing was scheduled because max requests is already in flight
        assertThat(lookupFunction.capturedConsumers, is(empty()));

        // Try to schedule again while max requests is not full. Ensure that despite the rejection, the queued request is sent.
        coordinator.remoteRequestPermits.release();
        capturedException.set(null);
        coordinator.schedule(searchRequest, ActionListener.wrap(response -> {}, capturedException::set));
        rejectionException = capturedException.get();
        assertThat(rejectionException.getMessage(), containsString("Could not perform enrichment, enrich coordination queue at capacity"));
        assertThat(lookupFunction.capturedRequests.size(), is(1));
        assertThat(lookupFunction.capturedConsumers.size(), is(1));

        // Schedule once more now, the queue should be able to accept the item, but will not schedule it yet
        capturedException.set(null);
        coordinator.schedule(searchRequest, ActionListener.wrap(response -> {}, capturedException::set));
        rejectionException = capturedException.get();
        assertThat(rejectionException, is(nullValue()));
        assertThat(coordinator.queue.size(), is(1));
        assertThat(coordinator.getRemoteRequestsCurrent(), is(1));
        assertThat(lookupFunction.capturedRequests.size(), is(1));
        assertThat(lookupFunction.capturedConsumers.size(), is(1));

        // Fulfill the captured consumer which will schedule the next item in the queue.
        var res = new MultiSearchResponse(new MultiSearchResponse.Item[] { new MultiSearchResponse.Item(emptySearchResponse(), null) }, 1L);
        try {
            lookupFunction.capturedConsumers.getFirst().accept(res, null);
            // Ensure queue was drained and that the item in it was scheduled
            assertThat(coordinator.queue.size(), equalTo(0));
            assertThat(lookupFunction.capturedRequests.size(), equalTo(2));
            assertThat(lookupFunction.capturedRequests.get(1).requests().getFirst(), sameInstance(searchRequest));
        } finally {
            res.decRef();
        }
    }

    public void testLookupFunction() {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        List<String> indices = List.of("index1", "index2", "index3");
        for (String index : indices) {
            multiSearchRequest.add(new SearchRequest(index));
            multiSearchRequest.add(new SearchRequest(index));
        }

        List<EnrichShardMultiSearchAction.Request> requests = new ArrayList<>();
        ElasticsearchClient client = new ElasticsearchClient() {

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> ActionFuture<Response> execute(
                ActionType<Response> action,
                Request request
            ) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void execute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                requests.add((EnrichShardMultiSearchAction.Request) request);
            }

            @Override
            public ThreadPool threadPool() {
                throw new UnsupportedOperationException();
            }
        };
        BiConsumer<MultiSearchRequest, BiConsumer<MultiSearchResponse, Exception>> consumer = Coordinator.lookupFunction(
            client,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST)
        );
        consumer.accept(multiSearchRequest, null);

        assertThat(requests.size(), equalTo(indices.size()));
        requests.sort(Comparator.comparing(SingleShardRequest::index));
        for (int i = 0; i < indices.size(); i++) {
            String index = indices.get(i);
            assertThat(requests.get(i).index(), equalTo(index));
            assertThat(requests.get(i).getMultiSearchRequest().requests().size(), equalTo(2));
            assertThat(requests.get(i).getMultiSearchRequest().requests().getFirst().indices().length, equalTo(1));
            assertThat(requests.get(i).getMultiSearchRequest().requests().getFirst().indices()[0], equalTo(index));
        }
    }

    public void testReduce() {
        Map<String, List<Tuple<Integer, SearchRequest>>> itemsPerIndex = new HashMap<>();
        Map<String, Tuple<MultiSearchResponse, Exception>> shardResponses = new HashMap<>();

        try {
            var empty = emptySearchResponse();
            // use empty response 3 times below and we start out with ref-count 1
            empty.incRef();
            empty.incRef();
            MultiSearchResponse.Item item1 = new MultiSearchResponse.Item(empty, null);
            itemsPerIndex.put("index1", List.of(new Tuple<>(0, null), new Tuple<>(1, null), new Tuple<>(2, null)));
            shardResponses.put(
                "index1",
                new Tuple<>(new MultiSearchResponse(new MultiSearchResponse.Item[] { item1, item1, item1 }, 1), null)
            );

            Exception failure = new RuntimeException();
            itemsPerIndex.put("index2", List.of(new Tuple<>(3, null), new Tuple<>(4, null), new Tuple<>(5, null)));
            shardResponses.put("index2", new Tuple<>(null, failure));

            // use empty response 3 times below
            empty.incRef();
            empty.incRef();
            empty.incRef();
            MultiSearchResponse.Item item2 = new MultiSearchResponse.Item(empty, null);
            itemsPerIndex.put("index3", List.of(new Tuple<>(6, null), new Tuple<>(7, null), new Tuple<>(8, null)));
            shardResponses.put(
                "index3",
                new Tuple<>(new MultiSearchResponse(new MultiSearchResponse.Item[] { item2, item2, item2 }, 1), null)
            );

            MultiSearchResponse result = Coordinator.reduce(9, itemsPerIndex, shardResponses);
            try {
                assertThat(result.getResponses().length, equalTo(9));
                assertThat(result.getResponses()[0], sameInstance(item1));
                assertThat(result.getResponses()[1], sameInstance(item1));
                assertThat(result.getResponses()[2], sameInstance(item1));
                assertThat(result.getResponses()[3].getFailure(), sameInstance(failure));
                assertThat(result.getResponses()[4].getFailure(), sameInstance(failure));
                assertThat(result.getResponses()[5].getFailure(), sameInstance(failure));
                assertThat(result.getResponses()[6], sameInstance(item2));
                assertThat(result.getResponses()[7], sameInstance(item2));
                assertThat(result.getResponses()[8], sameInstance(item2));
            } finally {
                result.decRef();
            }
        } finally {
            for (Tuple<MultiSearchResponse, Exception> value : shardResponses.values()) {
                var res = value.v1();
                if (res != null) {
                    res.decRef();
                }
            }
        }
    }

    /** Responses with hits are charged to the request breaker until the slot listeners have run, and released afterwards. */
    public void testResponseIsChargedWhileSlotListenersRun() {
        CapturingClient client = new CapturingClient();
        CircuitBreaker breaker = requestBreaker(null, CircuitBreakerMetrics.NOOP);
        Coordinator coordinator = new Coordinator(Coordinator.lookupFunction(client, breaker), 3, 1, 100);
        List<Long> usedWhileHandled = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            coordinator.queue.add(slot("my-index", ActionListener.running(() -> usedWhileHandled.add(breaker.getUsed()))));
        }
        coordinator.coordinateLookups();

        client.respond("my-index", multiSearchResponse(new int[][] { { 1000 }, { 2000, 3000 }, {} }));

        long expected = 3 * Coordinator.PER_HIT_OBJECT_OVERHEAD + 1000 + 2000 + 3000;
        assertThat(usedWhileHandled, equalTo(List.of(expected, expected, expected)));
        assertThat(breaker.getUsed(), equalTo(0L));
        assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(0));
    }

    /** In a batch spanning several enrich indices, the response that arrives first is charged while it waits for the others. */
    public void testFirstIndexResponseIsChargedWhileWaitingForOthers() {
        CapturingClient client = new CapturingClient();
        CircuitBreaker breaker = requestBreaker(null, CircuitBreakerMetrics.NOOP);
        Coordinator coordinator = new Coordinator(Coordinator.lookupFunction(client, breaker), 4, 1, 100);
        List<Long> usedWhileHandled = new ArrayList<>();
        for (String index : List.of("index1", "index2", "index1", "index2")) {
            coordinator.queue.add(slot(index, ActionListener.running(() -> usedWhileHandled.add(breaker.getUsed()))));
        }
        coordinator.coordinateLookups();
        assertThat(client.pendingIndices(), equalTo(Set.of("index1", "index2")));

        MultiSearchResponse first = multiSearchResponse(new int[][] { { 1000 }, { 1000 } });
        client.respond("index1", first);
        long firstBytes = 2 * Coordinator.PER_HIT_OBJECT_OVERHEAD + 2 * 1000;
        assertThat(breaker.getUsed(), equalTo(firstBytes));
        assertTrue("retained until the batch is reduced", first.hasReferences());
        assertThat(usedWhileHandled, empty());

        client.respond("index2", multiSearchResponse(new int[][] { { 2000 }, { 2000 } }));

        long expected = firstBytes + 2 * Coordinator.PER_HIT_OBJECT_OVERHEAD + 2 * 2000;
        assertThat(usedWhileHandled, equalTo(List.of(expected, expected, expected, expected)));
        assertThat(breaker.getUsed(), equalTo(0L));
        assertFalse(first.hasReferences());
        assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(0));
    }

    /**
     * A trip fails the slots of the tripping index only and frees its response right away. Whatever was charged for the other indices
     * is released once the batch is done, and the next batch goes out.
     */
    public void testBreakerTripFailsOnlySlotsOfTrippingIndex() {
        CapturingClient client = new CapturingClient();
        CircuitBreaker breaker = requestBreaker(10_000L, CircuitBreakerMetrics.NOOP);
        Coordinator coordinator = new Coordinator(Coordinator.lookupFunction(client, breaker), 2, 1, 100);
        List<Long> usedWhileHandled = new ArrayList<>();
        List<Exception> failures = new ArrayList<>();
        coordinator.queue.add(slot("index1", ActionListener.running(() -> usedWhileHandled.add(breaker.getUsed()))));
        coordinator.queue.add(slot("index2", ActionListener.wrap(r -> fail("unexpected response"), failures::add)));
        coordinator.queue.add(slot("index3", ActionListener.noop()));
        coordinator.coordinateLookups();
        assertThat(client.pendingIndices(), equalTo(Set.of("index1", "index2")));
        assertThat(coordinator.queue.size(), equalTo(1));

        client.respond("index1", multiSearchResponse(new int[][] { { 100 } }));
        long firstBytes = Coordinator.PER_HIT_OBJECT_OVERHEAD + 100;
        assertThat(breaker.getUsed(), equalTo(firstBytes));

        MultiSearchResponse tripping = multiSearchResponse(new int[][] { { 20_000 } });
        client.respond("index2", tripping);
        assertFalse("not retained after a trip", tripping.hasReferences());

        assertThat(usedWhileHandled, equalTo(List.of(firstBytes)));
        assertThat(failures.size(), equalTo(1));
        assertThat(failures.getFirst(), instanceOf(CircuitBreakingException.class));
        assertThat(((CircuitBreakingException) failures.getFirst()).status().getStatus(), equalTo(429));
        assertThat(failures.getFirst().getMessage(), containsString(Coordinator.BREAKER_LABEL));
        assertThat(breaker.getUsed(), equalTo(0L));
        // the permit was released and the queued slot went out in the next batch
        assertThat(coordinator.queue.size(), equalTo(0));
        assertThat(client.pendingIndices(), equalTo(Set.of("index3")));
        assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(1));
    }

    /** Failed lookups hold no response, so nothing is charged for them. */
    public void testFailuresChargeNothing() {
        // a limit of zero trips on any charge, so a charge would fail the slots with a breaker exception instead
        CapturingClient client = new CapturingClient();
        CircuitBreaker breaker = requestBreaker(0L, CircuitBreakerMetrics.NOOP);
        Coordinator coordinator = new Coordinator(Coordinator.lookupFunction(client, breaker), 2, 1, 100);
        List<Exception> failures = new ArrayList<>();
        for (String index : List.of("index1", "index2")) {
            coordinator.queue.add(slot(index, ActionListener.wrap(r -> fail("unexpected response"), failures::add)));
        }
        coordinator.coordinateLookups();

        RuntimeException shardFailure = new RuntimeException("shard");
        client.respondWithFailure("index1", shardFailure);
        RuntimeException itemFailure = new RuntimeException("item");
        client.respond(
            "index2",
            new MultiSearchResponse(new MultiSearchResponse.Item[] { new MultiSearchResponse.Item(null, itemFailure) }, 1L)
        );

        assertThat(failures, equalTo(List.of(shardFailure, itemFailure)));
        assertThat(breaker.getUsed(), equalTo(0L));
        assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(0));
    }

    /** Items that are still charged on the shard side are not charged again by the coordinator. */
    public void testShardSideChargedItemsAreNotChargedAgain() {
        CapturingClient client = new CapturingClient();
        CircuitBreaker breaker = requestBreaker(null, CircuitBreakerMetrics.NOOP);
        Coordinator coordinator = new Coordinator(Coordinator.lookupFunction(client, breaker), 2, 1, 100);
        List<Long> usedWhileHandled = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            coordinator.queue.add(slot("my-index", ActionListener.running(() -> usedWhileHandled.add(breaker.getUsed()))));
        }
        coordinator.coordinateLookups();

        // the shard-side reservation as the local transport path hands it to the coordinator, next to a plain (remote) item
        // large enough to be charged right away, smaller reservations are only charged once they cross the threshold
        long shardSideBytes = EnrichShardMultiSearchAction.BREAKER_CHARGE_THRESHOLD_BYTES;
        var reservation = new EnrichShardMultiSearchAction.SourceBytesReservation(breaker);
        reservation.add(shardSideBytes);
        SearchHit chargedHit = new SearchHit(0).sourceRef(new BytesArray(new byte[(int) shardSideBytes]));
        SearchHits chargedHits = new SearchHits(new SearchHit[] { chargedHit }, Lucene.TOTAL_HITS_EQUAL_TO_ZERO, Float.NaN);
        SearchResponse charged = new EnrichShardMultiSearchAction.ChargedSearchResponse(chargedHits, reservation);
        chargedHits.decRef();
        reservation.decRef();
        MultiSearchResponse plainResponse = multiSearchResponse(new int[][] { { 700 } });
        SearchResponse plain = plainResponse.getResponses()[0].getResponse();
        plain.incRef();
        plainResponse.decRef();
        MultiSearchResponse response = new MultiSearchResponse(
            new MultiSearchResponse.Item[] { new MultiSearchResponse.Item(charged, null), new MultiSearchResponse.Item(plain, null) },
            1L
        );
        assertThat(breaker.getUsed(), equalTo(shardSideBytes));

        client.respond("my-index", response);

        long expected = shardSideBytes + Coordinator.PER_HIT_OBJECT_OVERHEAD + 700;
        assertThat(usedWhileHandled, equalTo(List.of(expected, expected)));
        // the coordinator's charge is released once the batch is done, the shard-side one with the response
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** The release goes to the category of the charge, so that the per-category memory gauge returns to zero. */
    public void testChargeIsReleasedUnderItsCategory() {
        RecordingMeterRegistry meter = new RecordingMeterRegistry();
        // exposes a recording meter registry, so that the held-usage gauge can be read per category
        CircuitBreakerMetrics metrics = new CircuitBreakerMetrics(new TelemetryProvider.NoopTelemetryProvider() {
            @Override
            public MeterRegistry getMeterRegistry() {
                return meter;
            }
        });
        CapturingClient client = new CapturingClient();
        CircuitBreaker breaker = requestBreaker(null, metrics);
        Coordinator coordinator = new Coordinator(Coordinator.lookupFunction(client, breaker), 1, 1, 100);
        coordinator.queue.add(slot("my-index", ActionListener.noop()));
        coordinator.coordinateLookups();

        client.respond("my-index", multiSearchResponse(new int[][] { { 1000 } }));

        assertThat(breaker.getUsed(), equalTo(0L));
        Map<Object, Long> heldByCategory = meter.getRecorder()
            .getMeasurements(InstrumentType.LONG_UP_DOWN_COUNTER, CircuitBreakerMetrics.ES_BREAKER_MEMORY_HELD)
            .stream()
            .filter(m -> CircuitBreaker.REQUEST.equals(m.attributes().get(ChildMemoryCircuitBreaker.BREAKER_METRIC_TYPE_ATTRIBUTE)))
            .collect(
                Collectors.groupingBy(
                    m -> m.attributes().get(ChildMemoryCircuitBreaker.CIRCUIT_BREAKER_CATEGORY_ATTRIBUTE),
                    Collectors.summingLong(Measurement::getLong)
                )
            );
        assertThat(heldByCategory.get(ChildMemoryCircuitBreaker.CATEGORY_MSEARCH), equalTo(0L));
        assertThat(heldByCategory.getOrDefault(ChildMemoryCircuitBreaker.CATEGORY_UNCATEGORIZED, 0L), equalTo(0L));
    }

    private static Coordinator.Slot slot(String index, ActionListener<SearchResponse> listener) {
        return new Coordinator.Slot(new SearchRequest(index), listener);
    }

    private static MultiSearchResponse multiSearchResponse(int[][] sourceSizesPerItem) {
        MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[sourceSizesPerItem.length];
        for (int i = 0; i < items.length; i++) {
            SearchHit[] hits = new SearchHit[sourceSizesPerItem[i].length];
            for (int j = 0; j < hits.length; j++) {
                hits[j] = new SearchHit(j).sourceRef(new BytesArray(new byte[sourceSizesPerItem[i][j]]));
            }
            SearchHits searchHits = new SearchHits(hits, Lucene.TOTAL_HITS_EQUAL_TO_ZERO, Float.NaN);
            try {
                items[i] = new MultiSearchResponse.Item(SearchResponseUtils.successfulResponse(searchHits), null);
            } finally {
                searchHits.decRef();
            }
        }
        return new MultiSearchResponse(items, 1L);
    }

    /**
     * A real request breaker from a real hierarchy service. The real-memory parent check is off so that the tests do not depend on the
     * heap usage of the test JVM.
     */
    private static CircuitBreaker requestBreaker(Long limitBytes, CircuitBreakerMetrics metrics) {
        Settings.Builder settings = Settings.builder().put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false);
        if (limitBytes != null) {
            settings.put(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limitBytes + "b");
        }
        Settings built = settings.build();
        return new HierarchyCircuitBreakerService(
            metrics,
            built,
            Collections.emptyList(),
            new ClusterSettings(built, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ).getBreaker(CircuitBreaker.REQUEST);
    }

    /**
     * Captures the per-index shard lookups of {@link Coordinator#lookupFunction}, so that a test can answer them one at a time and in
     * any order. Like the transport layer, it releases a response once the listener has returned.
     */
    private static class CapturingClient implements ElasticsearchClient {
        private final Map<String, ActionListener<MultiSearchResponse>> pending = new HashMap<>();

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> ActionFuture<Response> execute(
            ActionType<Response> action,
            Request request
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        @SuppressWarnings("unchecked") // the only action executed here is the enrich shard multi-search, which responds with this type
        public <Request extends ActionRequest, Response extends ActionResponse> void execute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertSame(EnrichShardMultiSearchAction.INSTANCE, action);
            String index = ((EnrichShardMultiSearchAction.Request) request).index();
            assertNull(pending.put(index, (ActionListener<MultiSearchResponse>) listener));
        }

        @Override
        public ThreadPool threadPool() {
            throw new UnsupportedOperationException();
        }

        Set<String> pendingIndices() {
            return Set.copyOf(pending.keySet());
        }

        void respond(String index, MultiSearchResponse response) {
            try {
                pending.remove(index).onResponse(response);
            } finally {
                response.decRef();
            }
        }

        void respondWithFailure(String index, Exception e) {
            pending.remove(index).onFailure(e);
        }
    }

    private static SearchResponse emptySearchResponse() {
        return SearchResponseUtils.successfulResponse(SearchHits.empty(Lucene.TOTAL_HITS_EQUAL_TO_ZERO, Float.NaN));
    }

    private class MockLookupFunction implements BiConsumer<MultiSearchRequest, BiConsumer<MultiSearchResponse, Exception>> {

        private final List<MultiSearchRequest> capturedRequests = new ArrayList<>();
        private final List<BiConsumer<MultiSearchResponse, Exception>> capturedConsumers = new ArrayList<>();

        @Override
        public void accept(MultiSearchRequest multiSearchRequest, BiConsumer<MultiSearchResponse, Exception> consumer) {
            capturedRequests.add(multiSearchRequest);
            capturedConsumers.add(consumer);
        }
    }

    public void testAllSearchesExecuted() throws Exception {

        final ThreadPool threadPool = new TestThreadPool("test");
        final Coordinator coordinator = new Coordinator((request, responseConsumer) -> threadPool.generic().execute(() -> {
            final MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[request.requests().size()];
            for (int i = 0; i < items.length; i++) {
                items[i] = new MultiSearchResponse.Item(emptySearchResponse(), null);
            }
            var res = new MultiSearchResponse(items, 0L);
            try {
                responseConsumer.accept(res, null);
            } finally {
                res.decRef();
            }
        }), 5, 2, 20);
        try {

            final Semaphore schedulePermits = new Semaphore(between(100, 10000));
            final CountDownLatch completionCountdown = new CountDownLatch(schedulePermits.availablePermits());
            for (int i = 0; i < 5; i++) {
                threadPool.generic().execute(() -> {
                    while (schedulePermits.tryAcquire()) {
                        final AtomicBoolean completed = new AtomicBoolean();
                        coordinator.schedule(new SearchRequest("index"), ActionListener.running(() -> {
                            assertTrue(completed.compareAndSet(false, true)); // no double-completion
                            completionCountdown.countDown();
                        }));
                    }
                });
            }

            assertTrue(completionCountdown.await(20L, TimeUnit.SECONDS));
            assertThat(coordinator.queue, empty());

            assertBusy(() -> assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(0)));
            // ^ assertBusy here because the final check of the queue briefly counts as another remote request, after everything is complete
        } finally {
            ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS);
        }

    }

}
