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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.xpack.enrich.action.EnrichCoordinatorProxyAction.Coordinator;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

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
        BatchCircuitBreakerReading reading = new BatchCircuitBreakerReading(breaker);
        for (int i = 0; i < 3; i++) {
            coordinator.queue.add(slot("my-index", reading.listener()));
        }
        coordinator.coordinateLookups();

        client.respond("my-index", multiSearchResponse(1000, 2000, 3000));

        // at least the sources, the estimate adds the overhead of the responses and hits on top
        assertThat(reading.used(3), greaterThanOrEqualTo(1000L + 2000 + 3000));
        // breaker is reset after listeners complete
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** In a batch spanning several enrich indices, the response that arrives first is charged while it waits for the others. */
    public void testFirstIndexResponseIsChargedWhileWaitingForOthers() {
        CapturingClient client = new CapturingClient();
        CircuitBreaker breaker = requestBreaker(null, CircuitBreakerMetrics.NOOP);
        Coordinator coordinator = new Coordinator(Coordinator.lookupFunction(client, breaker), 4, 1, 100);
        BatchCircuitBreakerReading reading = new BatchCircuitBreakerReading(breaker);
        for (String index : List.of("index1", "index2", "index1", "index2")) {
            coordinator.queue.add(slot(index, reading.listener()));
        }
        coordinator.coordinateLookups();
        assertThat(client.pendingIndices(), equalTo(Set.of("index1", "index2")));

        MultiSearchResponse first = multiSearchResponse(1000, 1000);
        client.respond("index1", first);

        assertThat(breaker.getUsed(), greaterThanOrEqualTo(2 * 1000L)); // breaker is charged for first response
        assertTrue("retained until the batch is reduced", first.hasReferences());
        assertFalse("listeners run once batch is reduced", reading.hasRun());

        client.respond("index2", multiSearchResponse(2000, 2000));

        assertThat(reading.used(4), greaterThanOrEqualTo(2 * 1000L + 2 * 2000L));
        assertThat(breaker.getUsed(), equalTo(0L));
        assertFalse(first.hasReferences());
    }

    /**
     * A trip fails the slots of the tripping index only and frees its response right away. Whatever was charged for the other indices
     * is released once the batch is done, and the next batch goes out.
     */
    public void testBreakerTripFailsOnlySlotsOfTrippingIndex() {
        CapturingClient client = new CapturingClient();
        CircuitBreaker breaker = requestBreaker(10_000L, CircuitBreakerMetrics.NOOP);
        Coordinator coordinator = new Coordinator(Coordinator.lookupFunction(client, breaker), 2, 1, 100);
        BatchCircuitBreakerReading reading = new BatchCircuitBreakerReading(breaker);
        List<Exception> failures = new ArrayList<>();
        coordinator.queue.add(slot("index1", reading.listener()));
        coordinator.queue.add(slot("index2", ActionListener.wrap(r -> fail("unexpected response"), failures::add)));
        coordinator.queue.add(slot("index3", ActionListener.noop()));
        coordinator.coordinateLookups();
        assertThat(client.pendingIndices(), equalTo(Set.of("index1", "index2")));
        assertThat(coordinator.queue.size(), equalTo(1));

        client.respond("index1", multiSearchResponse(100));
        assertThat(breaker.getUsed(), greaterThanOrEqualTo(100L));

        MultiSearchResponse tripping = multiSearchResponse(20_000);
        client.respond("index2", tripping);
        assertFalse("not retained after a trip", tripping.hasReferences());

        assertThat(reading.used(1), greaterThanOrEqualTo(100L));
        assertThat(failures, hasSize(1));
        assertThat(
            asInstanceOf(CircuitBreakingException.class, failures.getFirst()),
            hasMessage(containsString(Coordinator.BREAKER_LABEL))
        );

        // breaker is reset after listeners of the first batch complete
        assertThat(breaker.getUsed(), equalTo(0L));
        // the permit was released and the queued slot went out in the next batch
        assertThat(coordinator.queue.size(), equalTo(0));
        assertThat(client.pendingIndices(), equalTo(Set.of("index3")));
    }

    /**
     * A failed lookup of an index holds no response, so nothing is charged for it at any point, and it reaches its slots as it is.
     */
    public void testFailedLookupChargesNothing() {
        // a limit of zero trips on any checked charge, so such a charge would fail the slots with a breaker exception instead
        CapturingClient client = new CapturingClient();
        CircuitBreaker breaker = requestBreaker(0L, CircuitBreakerMetrics.NOOP);
        Coordinator coordinator = new Coordinator(Coordinator.lookupFunction(client, breaker), 2, 1, 100);
        BatchCircuitBreakerReading reading = new BatchCircuitBreakerReading(breaker);
        AtomicInteger failures = new AtomicInteger();
        for (String index : List.of("index1", "index2")) {
            coordinator.queue.add(slot(index, reading.failureListener(e -> failures.incrementAndGet())));
        }
        coordinator.coordinateLookups();

        client.respondWithFailure("index1", new RuntimeException("index1"));
        client.respondWithFailure("index2", new RuntimeException("index2"));

        assertThat(failures.get(), equalTo(2));
        // the slot listeners ran with nothing charged
        assertThat(reading.used(2), equalTo(0L));
    }

    /** The response of a local lookup is still charged on the shard side, and not charged again by the coordinator. */
    public void testShardSideChargedItemsAreNotChargedAgain() {
        CapturingClient client = new CapturingClient();
        CircuitBreaker breaker = requestBreaker(null, CircuitBreakerMetrics.NOOP);
        Coordinator coordinator = new Coordinator(Coordinator.lookupFunction(client, breaker), 1, 1, 100);
        BatchCircuitBreakerReading reading = new BatchCircuitBreakerReading(breaker);
        coordinator.queue.add(slot("my-index", reading.listener()));
        coordinator.coordinateLookups();

        long shardSideBytes = EnrichShardMultiSearchAction.BREAKER_CHARGE_THRESHOLD_BYTES;
        MultiSearchResponse response = shardSideChargedMultiSearchResponse(breaker, (int) shardSideBytes);
        assertThat(breaker.getUsed(), equalTo(shardSideBytes));

        // prevent response from being released on respond, so we can observe later
        response.incRef();
        client.respond("my-index", response);

        assertThat(reading.used(1), equalTo(shardSideBytes)); // no additional charge
        assertThat(breaker.getUsed(), equalTo(shardSideBytes));
        response.decRef();
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

        client.respond("my-index", multiSearchResponse(1000));

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
        // the charge was recorded, and every category is back at zero, so the release did not go to a different one than the charge
        assertThat(heldByCategory, hasKey(ChildMemoryCircuitBreaker.CATEGORY_MSEARCH));
        assertThat(heldByCategory.values(), everyItem(equalTo(0L)));
    }

    /**
     * A batch whose response is large keeps its permit while its slot listeners run, so that no further batch can be dispatched or
     * received behind it, and releases it once they are done.
     */
    public void testLargeResponseHoldsPermitWhileSlotListenersRun() {
        MockLookupFunction lookupFunction = new MockLookupFunction();
        Coordinator coordinator = new Coordinator(lookupFunction, 2, 1, 100);
        PermitReading reading = new PermitReading(coordinator);
        coordinator.queue.add(slot("my-index", reading.listener()));
        coordinator.queue.add(slot("my-index", reading.listener()));
        coordinator.coordinateLookups();
        assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(1));

        respond(lookupFunction, 0, multiSearchResponse(largeSourceBytes(), 100));

        assertThat("permit still held while the listeners ran", reading.remoteRequestsCurrent(2), equalTo(1));
        assertThat("permit released once the batch is done", coordinator.getRemoteRequestsCurrent(), equalTo(0));
        assertThat(lookupFunction.capturedRequests, hasSize(1));
    }

    /**
     * A batch whose response is small releases its permit before its first slot listener runs, as before; the next batch still goes out
     * only after the loop.
     */
    public void testSmallResponseReleasesPermitBeforeSlotListenersRun() {
        MockLookupFunction lookupFunction = new MockLookupFunction();
        Coordinator coordinator = new Coordinator(lookupFunction, 2, 1, 100);
        PermitReading reading = new PermitReading(coordinator);
        coordinator.queue.add(slot("my-index", reading.listener()));
        coordinator.queue.add(slot("my-index", reading.listener()));
        coordinator.queue.add(slot("my-index", ActionListener.noop())); // stays queued for the next batch
        coordinator.coordinateLookups();
        assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(1));
        assertThat(coordinator.queue.size(), equalTo(1));

        respond(lookupFunction, 0, multiSearchResponse(100, 100));

        assertThat("permit released before the listeners ran", reading.remoteRequestsCurrent(2), equalTo(0));
        assertThat("next batch dispatched after the loop only", reading.queued(), equalTo(1));
        assertThat(lookupFunction.capturedRequests, hasSize(2));
        assertThat(coordinator.queue.size(), equalTo(0));
        assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(1));
    }

    /**
     * A slot listener that throws in a large batch must not leak the permit the batch holds, so that the queued slots still go out with
     * the next coordination round.
     */
    public void testThrowingSlotListenerInLargeBatchReleasesPermit() {
        MockLookupFunction lookupFunction = new MockLookupFunction();
        Coordinator coordinator = new Coordinator(lookupFunction, 1, 1, 100);
        RuntimeException thrown = new RuntimeException("simulated");
        coordinator.queue.add(slot("my-index", new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                throw thrown;
            }

            @Override
            public void onFailure(Exception e) {
                fail(e, "unexpected failure");
            }
        }));
        coordinator.queue.add(slot("my-index", ActionListener.noop())); // stays queued for the next batch
        coordinator.coordinateLookups();
        assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(1));

        MultiSearchResponse response = multiSearchResponse(largeSourceBytes());
        try {
            assertSame(
                thrown,
                expectThrows(RuntimeException.class, () -> lookupFunction.capturedConsumers.getFirst().accept(response, null))
            );
        } finally {
            response.decRef();
        }
        assertThat("permit released although the listener threw", coordinator.getRemoteRequestsCurrent(), equalTo(0));

        // the exception skipped the coordination round of the batch, the next one picks the queued slot up
        assertThat(lookupFunction.capturedRequests, hasSize(1));
        coordinator.coordinateLookups();
        assertThat(lookupFunction.capturedRequests, hasSize(2));
        assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(1));
    }

    /** A failed batch holds no response, so its permit is released before the slot listeners are failed. */
    public void testFailedBatchReleasesPermitBeforeSlotListenersRun() {
        MockLookupFunction lookupFunction = new MockLookupFunction();
        Coordinator coordinator = new Coordinator(lookupFunction, 2, 1, 100);
        PermitReading reading = new PermitReading(coordinator);
        AtomicInteger failures = new AtomicInteger();
        coordinator.queue.add(slot("my-index", reading.failureListener(e -> failures.incrementAndGet())));
        coordinator.queue.add(slot("my-index", reading.failureListener(e -> failures.incrementAndGet())));
        coordinator.coordinateLookups();
        assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(1));

        lookupFunction.capturedConsumers.getFirst().accept(null, new RuntimeException("simulated"));

        assertThat(failures.get(), equalTo(2));
        assertThat("permit released before the listeners ran", reading.remoteRequestsCurrent(2), equalTo(0));
        assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(0));
    }

    /**
     * When one index of a batch trips the breaker and another returns a large response, the batch holds its permit for the large
     * response and releases it exactly once together with the charge, so that the next batch goes out and the permits balance.
     */
    public void testBreakerTripInLargeBatchReleasesPermitOnce() {
        CapturingClient client = new CapturingClient();
        CircuitBreaker breaker = requestBreaker(2L * largeSourceBytes(), CircuitBreakerMetrics.NOOP);
        Coordinator coordinator = new Coordinator(Coordinator.lookupFunction(client, breaker), 2, 1, 100);
        PermitReading reading = new PermitReading(coordinator);
        AtomicInteger failures = new AtomicInteger();
        coordinator.queue.add(slot("index1", reading.listener()));
        coordinator.queue.add(slot("index2", reading.failureListener(e -> {
            assertThat(asInstanceOf(CircuitBreakingException.class, e), hasMessage(containsString(Coordinator.BREAKER_LABEL)));
            failures.incrementAndGet();
        })));
        coordinator.queue.add(slot("index3", ActionListener.noop())); // stays queued for the next batch
        coordinator.coordinateLookups();
        assertThat(client.pendingIndices(), equalTo(Set.of("index1", "index2")));
        assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(1));

        client.respond("index1", multiSearchResponse(largeSourceBytes()));
        assertThat(breaker.getUsed(), greaterThanOrEqualTo((long) largeSourceBytes()));
        client.respond("index2", multiSearchResponse(2 * largeSourceBytes()));

        assertThat(failures.get(), equalTo(1));
        assertThat("permit held for the large response while the listeners ran", reading.remoteRequestsCurrent(2), equalTo(1));
        assertThat("next batch dispatched after the loop only", reading.queued(), equalTo(1));
        assertThat(breaker.getUsed(), equalTo(0L));

        // released once: the next batch took the single permit, and no permit is left over after it
        assertThat(client.pendingIndices(), equalTo(Set.of("index3")));
        assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(1));
        client.respond("index3", multiSearchResponse(100));
        assertThat(coordinator.getRemoteRequestsCurrent(), equalTo(0));
        assertThat(coordinator.remoteRequestPermits.availablePermits(), equalTo(1));
    }

    /** Records what the slot listeners of one batch see of the coordinator while they run. */
    private static final class PermitReading {
        private final Coordinator coordinator;
        private int remoteRequestsCurrent;
        private int queued;
        private int listeners;

        PermitReading(Coordinator coordinator) {
            this.coordinator = coordinator;
        }

        ActionListener<SearchResponse> listener() {
            return ActionListener.running(this::takeReading);
        }

        ActionListener<SearchResponse> failureListener(Consumer<Exception> consumer) {
            return ActionListener.wrap(r -> fail("unexpected response"), e -> {
                consumer.accept(e);
                takeReading();
            });
        }

        private void takeReading() {
            int current = coordinator.getRemoteRequestsCurrent();
            int size = coordinator.queue.size();
            assert listeners == 0 || (current == remoteRequestsCurrent && size == queued)
                : "inconsistent reading in batch: " + current + "/" + size + " vs " + remoteRequestsCurrent + "/" + queued;
            remoteRequestsCurrent = current;
            queued = size;
            listeners++;
        }

        int remoteRequestsCurrent(int expectedListeners) {
            assertThat("slot listeners that ran", listeners, equalTo(expectedListeners));
            return remoteRequestsCurrent;
        }

        int queued() {
            assertThat("slot listeners that ran", listeners, greaterThanOrEqualTo(1));
            return queued;
        }
    }

    /** Source bytes of a single hit above {@link Coordinator#PERMIT_HOLD_THRESHOLD_BYTES}, so that a response with it holds the permit. */
    private static int largeSourceBytes() {
        return Math.toIntExact(Coordinator.PERMIT_HOLD_THRESHOLD_BYTES) + 1;
    }

    /** Answers a captured batch of a {@link MockLookupFunction} and, like the transport layer, releases the response afterwards. */
    private static void respond(MockLookupFunction lookupFunction, int batch, MultiSearchResponse response) {
        try {
            lookupFunction.capturedConsumers.get(batch).accept(response, null);
        } finally {
            response.decRef();
        }
    }

    private static final class BatchCircuitBreakerReading {
        private final CircuitBreaker breaker;
        private long used;
        private int listeners;

        BatchCircuitBreakerReading(CircuitBreaker breaker) {
            this.breaker = breaker;
        }

        ActionListener<SearchResponse> listener() {
            return ActionListener.running(this::takeReading);
        }

        ActionListener<SearchResponse> failureListener(Consumer<Exception> consumer) {
            return ActionListener.wrap(r -> fail("unexpected response"), e -> {
                consumer.accept(e);
                takeReading();
            });
        }

        private void takeReading() {
            long current = breaker.getUsed();
            assert hasRun() == false || current == used : "inconsistent breaker reading in batch: " + current + " vs " + used;
            used = current;
            listeners++;
        }

        boolean hasRun() {
            return listeners > 0;
        }

        long used(int expectedListeners) {
            assertThat("slot listeners that ran", listeners, equalTo(expectedListeners));
            return used;
        }
    }

    private static Coordinator.Slot slot(String index, ActionListener<SearchResponse> listener) {
        return new Coordinator.Slot(new SearchRequest(index), listener);
    }

    private static MultiSearchResponse multiSearchResponse(int... sourceBytesPerItem) {
        MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[sourceBytesPerItem.length];
        for (int i = 0; i < items.length; i++) {
            items[i] = new MultiSearchResponse.Item(searchResponse(sourceBytesPerItem[i]), null);
        }
        return new MultiSearchResponse(items, 1L);
    }

    private static SearchResponse searchResponse(int sourceBytes) {
        SearchHit hit = new SearchHit(0).sourceRef(new BytesArray(new byte[sourceBytes]));
        SearchHits searchHits = new SearchHits(new SearchHit[] { hit }, Lucene.TOTAL_HITS_EQUAL_TO_ZERO, Float.NaN);
        try {
            return SearchResponseUtils.successfulResponse(searchHits);
        } finally {
            searchHits.decRef();
        }
    }

    private static MultiSearchResponse shardSideChargedMultiSearchResponse(CircuitBreaker breaker, int sourceBytes) {
        return new MultiSearchResponse(
            new MultiSearchResponse.Item[] { new MultiSearchResponse.Item(shardSideChargedResponse(breaker, sourceBytes), null) },
            1L
        );
    }

    private static SearchResponse shardSideChargedResponse(CircuitBreaker breaker, int sourceBytes) {
        var reservation = new EnrichShardMultiSearchAction.SourceBytesReservation(breaker);
        reservation.add(sourceBytes);
        SearchHit hit = new SearchHit(0).sourceRef(new BytesArray(new byte[sourceBytes]));
        SearchHits hits = new SearchHits(new SearchHit[] { hit }, Lucene.TOTAL_HITS_EQUAL_TO_ZERO, Float.NaN);
        try {
            return new EnrichShardMultiSearchAction.ChargedSearchResponse(hits, reservation);
        } finally {
            // the response holds references of its own now
            hits.decRef();
            reservation.decRef();
        }
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
