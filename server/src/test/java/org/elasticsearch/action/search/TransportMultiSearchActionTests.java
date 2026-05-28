/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportMultiSearchActionTests extends ESTestCase {

    public void testParentTaskId() throws Exception {
        // Initialize dependencies of TransportMultiSearchAction
        Settings settings = Settings.builder().put("node.name", TransportMultiSearchActionTests.class.getSimpleName()).build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        ThreadPool threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
        try {
            TransportService transportService = new TransportService(
                Settings.EMPTY,
                mock(Transport.class),
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                boundAddress -> DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID())
                    .applySettings(settings)
                    .address(boundAddress.publishAddress())
                    .build(),
                null,
                Collections.emptySet()
            );
            ClusterService clusterService = mock(ClusterService.class);
            when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test")).build());

            String localNodeId = randomAlphaOfLengthBetween(3, 10);
            int numSearchRequests = randomIntBetween(1, 100);
            MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
            for (int i = 0; i < numSearchRequests; i++) {
                multiSearchRequest.add(new SearchRequest());
            }
            AtomicInteger counter = new AtomicInteger(0);
            Task task = multiSearchRequest.createTask(randomLong(), "type", "action", null, Collections.emptyMap());
            NodeClient client = new NodeClient(settings, threadPool, TestProjectResolvers.alwaysThrow()) {
                @Override
                public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {
                    assertEquals(task.getId(), request.getParentTask().getId());
                    assertEquals(localNodeId, request.getParentTask().getNodeId());
                    counter.incrementAndGet();
                    var response = SearchResponse.emptyResponseBuilder().tookInMillis(1L).build();
                    try {
                        listener.onResponse(response);
                    } finally {
                        response.decRef();
                    }
                }

                @Override
                public String getLocalNodeId() {
                    return localNodeId;
                }
            };
            TransportMultiSearchAction action = new TransportMultiSearchAction(
                actionFilters,
                transportService,
                clusterService,
                10,
                System::nanoTime,
                client,
                DefaultProjectResolver.INSTANCE,
                new NoopCircuitBreaker("test")
            );

            PlainActionFuture<MultiSearchResponse> future = new PlainActionFuture<>();
            action.execute(task, multiSearchRequest, future);
            future.get();
            assertEquals(numSearchRequests, counter.get());
        } finally {
            assertTrue(ESTestCase.terminate(threadPool));
        }
    }

    public void testBatchExecute() throws ExecutionException, InterruptedException {
        // Initialize dependencies of TransportMultiSearchAction
        Settings settings = Settings.builder().put("node.name", TransportMultiSearchActionTests.class.getSimpleName()).build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        ThreadPool threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID())
                .applySettings(settings)
                .address(boundAddress.publishAddress())
                .build(),
            null,
            Collections.emptySet()
        );
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test")).build());

        // Keep track of the number of concurrent searches started by multi search api,
        // and if there are more searches than is allowed create an error and remember that.
        int maxAllowedConcurrentSearches = scaledRandomIntBetween(1, 16);
        AtomicInteger counter = new AtomicInteger();
        AtomicReference<AssertionError> errorHolder = new AtomicReference<>();
        // randomize whether or not requests are executed asynchronously
        final List<Executor> executorServices = Arrays.asList(threadPool.generic(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        Randomness.shuffle(executorServices);
        final Executor commonExecutor = executorServices.get(0);
        final Executor rarelyExecutor = executorServices.get(1);
        final Set<SearchRequest> requests = Collections.newSetFromMap(Collections.synchronizedMap(new IdentityHashMap<>()));
        NodeClient client = new NodeClient(settings, threadPool, TestProjectResolvers.alwaysThrow()) {
            @Override
            public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {
                requests.add(request);
                int currentConcurrentSearches = counter.incrementAndGet();
                if (currentConcurrentSearches > maxAllowedConcurrentSearches) {
                    errorHolder.set(
                        new AssertionError(
                            "Current concurrent search ["
                                + currentConcurrentSearches
                                + "] is higher than is allowed ["
                                + maxAllowedConcurrentSearches
                                + "]"
                        )
                    );
                }
                final Executor executorService = rarely() ? rarelyExecutor : commonExecutor;
                executorService.execute(() -> {
                    counter.decrementAndGet();
                    var response = SearchResponseUtils.emptyWithTotalHits(
                        null,
                        0,
                        0,
                        0,
                        0L,
                        ShardSearchFailure.EMPTY_ARRAY,
                        SearchResponse.Clusters.EMPTY
                    );
                    try {
                        listener.onResponse(response);
                    } finally {
                        response.decRef();
                    }
                });
            }

            @Override
            public String getLocalNodeId() {
                return "local_node_id";
            }
        };

        TransportMultiSearchAction action = new TransportMultiSearchAction(
            actionFilters,
            transportService,
            clusterService,
            10,
            System::nanoTime,
            client,
            DefaultProjectResolver.INSTANCE,
            new NoopCircuitBreaker("test")
        );

        // Execute the multi search api and fail if we find an error after executing:
        try {
            /*
             * Allow for a large number of search requests in a single batch as previous implementations could stack overflow if the number
             * of requests in a single batch was large
             */
            int numSearchRequests = scaledRandomIntBetween(1, 8192);
            MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
            multiSearchRequest.maxConcurrentSearchRequests(maxAllowedConcurrentSearches);
            for (int i = 0; i < numSearchRequests; i++) {
                multiSearchRequest.add(new SearchRequest());
            }

            final PlainActionFuture<Void> future = new PlainActionFuture<>();
            ActionTestUtils.execute(action, multiSearchRequest, future.delegateFailure((l, response) -> {
                assertThat(response.getResponses().length, equalTo(numSearchRequests));
                assertThat(requests.size(), equalTo(numSearchRequests));
                assertThat(errorHolder.get(), nullValue());
                l.onResponse(null);
            }));
            future.get();
        } finally {
            assertTrue(ESTestCase.terminate(threadPool));
        }
    }

    public void testDefaultMaxConcurrentSearches() {
        int numDataNodes = randomIntBetween(1, 10);
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (int i = 0; i < numDataNodes; i++) {
            builder.add(DiscoveryNodeUtils.builder("_id" + i).roles(Collections.singleton(DiscoveryNodeRole.DATA_ROLE)).build());
        }
        builder.add(DiscoveryNodeUtils.builder("master").roles(Collections.singleton(DiscoveryNodeRole.MASTER_ROLE)).build());
        builder.add(DiscoveryNodeUtils.builder("ingest").roles(Collections.singleton(DiscoveryNodeRole.INGEST_ROLE)).build());

        ClusterState state = ClusterState.builder(new ClusterName("_name")).nodes(builder).build();
        int result = TransportMultiSearchAction.defaultMaxConcurrentSearches(10, state);
        assertThat(result, equalTo(10 * numDataNodes));

        state = ClusterState.builder(new ClusterName("_name")).build();
        result = TransportMultiSearchAction.defaultMaxConcurrentSearches(10, state);
        assertThat(result, equalTo(1));
    }

    public void testEstimateActualBytes() throws Exception {
        assertEstimateBytes(
            new SearchHit(0, "id"),
            TransportMultiSearchAction.BASE_RESPONSE_OVERHEAD + TransportMultiSearchAction.PER_HIT_OBJECT_OVERHEAD
        );

        byte[] sourceBytes = new byte[5120];
        SearchHit withSource = new SearchHit(0, "id");
        withSource.sourceRef(new BytesArray(sourceBytes));
        assertEstimateBytes(
            withSource,
            TransportMultiSearchAction.BASE_RESPONSE_OVERHEAD + TransportMultiSearchAction.PER_HIT_OBJECT_OVERHEAD + sourceBytes.length
        );

        BytesReference compressed = CompressorFactory.COMPRESSOR.compress(new BytesArray("{\"k\":\"v\"}"));
        SearchHit compressedHit = new SearchHit(0, "id");
        compressedHit.sourceRef(compressed);
        // Verify rawSourceLength() returns the compressed (stored) byte count without decompressing.
        assertThat(compressedHit.rawSourceLength(), equalTo(compressed.length()));
        assertEstimateBytes(
            compressedHit,
            TransportMultiSearchAction.BASE_RESPONSE_OVERHEAD + TransportMultiSearchAction.PER_HIT_OBJECT_OVERHEAD + compressed.length()
        );
        // Note: do not use compressedHit after assertEstimateBytes — responseWithHits transfers
        // ownership into SearchHits, and response.decRef() releases the hit transitively.

        // SearchHit is final so Mockito spy is not usable here. The correct code path
        // (getDocumentFields/getMetadataFields instead of the allocating getFields()) is
        // verified indirectly: the estimate uses only the non-allocating accessors and
        // still produces the correct count.
        SearchHit oneDocField = new SearchHit(0, "id");
        oneDocField.addDocumentFields(Map.of("foo", new DocumentField("foo", List.of("v"))), Map.of());
        assertEstimateBytes(
            oneDocField,
            TransportMultiSearchAction.BASE_RESPONSE_OVERHEAD + TransportMultiSearchAction.PER_HIT_OBJECT_OVERHEAD
                + TransportMultiSearchAction.PER_FIELD_OVERHEAD
        );

        SearchHit withFields = new SearchHit(0, "id");
        withFields.addDocumentFields(
            Map.of("foo", new DocumentField("foo", List.of("v"))),
            Map.of("meta", new DocumentField("meta", List.of(1)))
        );
        assertEstimateBytes(
            withFields,
            TransportMultiSearchAction.BASE_RESPONSE_OVERHEAD + TransportMultiSearchAction.PER_HIT_OBJECT_OVERHEAD + 2L
                * TransportMultiSearchAction.PER_FIELD_OVERHEAD
        );

        SearchResponse emptyHits = SearchResponse.emptyResponseBuilder().build();
        try {
            assertThat(
                TransportMultiSearchAction.estimateActualBytes(emptyHits),
                equalTo(TransportMultiSearchAction.BASE_RESPONSE_OVERHEAD)
            );
        } finally {
            emptyHits.decRef();
        }

        SearchHit inner = new SearchHit(0, "inner");
        SearchHit outer = new SearchHit(0, "outer");
        inner.sourceRef(new BytesArray("inner"));
        outer.sourceRef(new BytesArray("outer"));
        outer.setInnerHits(Map.of("nested", new SearchHits(new SearchHit[] { inner }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1f)));
        assertEstimateBytes(
            outer,
            TransportMultiSearchAction.BASE_RESPONSE_OVERHEAD + 2 * TransportMultiSearchAction.PER_HIT_OBJECT_OVERHEAD + "outer".length()
                + "inner".length()
        );

        SearchHit deepest = new SearchHit(0, "deepest");
        SearchHit middle = new SearchHit(0, "middle");
        SearchHit top = new SearchHit(0, "top");
        deepest.sourceRef(new BytesArray("d"));
        middle.sourceRef(new BytesArray("m"));
        top.sourceRef(new BytesArray("t"));
        middle.setInnerHits(
            Map.of("level2", new SearchHits(new SearchHit[] { deepest }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1f))
        );
        top.setInnerHits(Map.of("level1", new SearchHits(new SearchHit[] { middle }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1f)));
        assertEstimateBytes(
            top,
            TransportMultiSearchAction.BASE_RESPONSE_OVERHEAD + 3 * TransportMultiSearchAction.PER_HIT_OBJECT_OVERHEAD + "t".length() + "m"
                .length() + "d".length()
        );

        InternalAggregations aggs = InternalAggregations.from(List.of(new Max("max", 42.0, DocValueFormat.RAW, Map.of())));
        SearchResponse withAggs = SearchResponseUtils.response(SearchHits.EMPTY_WITH_TOTAL_HITS).aggregations(aggs).build();
        try {
            long aggBytes = DelayableWriteable.getUncompressedSerializedSize(aggs);
            assertThat(
                TransportMultiSearchAction.estimateActualBytes(withAggs),
                equalTo(TransportMultiSearchAction.BASE_RESPONSE_OVERHEAD + aggBytes)
            );
        } finally {
            withAggs.decRef();
        }

        SearchHit hit = new SearchHit(0, "id");
        hit.sourceRef(new BytesArray("x"));
        SearchHits combinedHits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1f);
        SearchResponse combined = SearchResponseUtils.response(combinedHits).aggregations(aggs).build();
        combinedHits.decRef();
        try {
            long expected = TransportMultiSearchAction.BASE_RESPONSE_OVERHEAD + TransportMultiSearchAction.PER_HIT_OBJECT_OVERHEAD + 1
                + DelayableWriteable.getUncompressedSerializedSize(aggs);
            assertThat(TransportMultiSearchAction.estimateActualBytes(combined), equalTo(expected));
        } finally {
            combined.decRef();
        }
    }

    private static void assertEstimateBytes(SearchHit hit, long expectedBytes) throws Exception {
        SearchResponse response = responseWithHits(hit);
        try {
            assertThat(TransportMultiSearchAction.estimateActualBytes(response), equalTo(expectedBytes));
        } finally {
            response.decRef();
        }
    }

    public void testBreakerReservesOnEachResponse() throws Exception {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1);
        int numRequests = 3;
        SearchResponse emptyResponse = SearchResponse.emptyResponseBuilder().tookInMillis(1L).build();
        try {
            long perResponseBytes = TransportMultiSearchAction.estimateActualBytes(emptyResponse);
            runMsearchWithBreaker(
                breaker,
                numRequests,
                () -> SearchResponse.emptyResponseBuilder().tookInMillis(1L).build(),
                null,
                items -> assertThat(breaker.getUsed(), equalTo(perResponseBytes * numRequests))
            );
            assertThat(breaker.getUsed(), equalTo(0L));
            assertThat(breaker.totalReserved(), equalTo(perResponseBytes * numRequests));
        } finally {
            emptyResponse.decRef();
        }
    }

    public void testBreakerReleasedOnSuccess() throws Exception {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1);
        runMsearchWithBreaker(breaker, 2, () -> SearchResponse.emptyResponseBuilder().tookInMillis(1L).build());
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testBreakerReleasedOnSubSearchFailure() throws Exception {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1);
        AtomicInteger call = new AtomicInteger();
        SearchResponse emptyResponse = SearchResponse.emptyResponseBuilder().tookInMillis(1L).build();
        try {
            long perResponseBytes = TransportMultiSearchAction.estimateActualBytes(emptyResponse);
            runMsearchWithBreaker(breaker, 2, () -> SearchResponse.emptyResponseBuilder().tookInMillis(1L).build(), call);
            assertThat(breaker.getUsed(), equalTo(0L));
            assertThat(breaker.totalReserved(), equalTo(perResponseBytes));
        } finally {
            emptyResponse.decRef();
        }
    }

    public void testBreakerTripDuringExecution() throws Exception {
        int tripOn = 2;
        int numRequests = 3;
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(tripOn);
        boolean[] failures = new boolean[numRequests];
        Exception[] failureExceptions = new Exception[numRequests];
        MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[numRequests];
        runMsearchWithBreaker(
            breaker,
            numRequests,
            () -> SearchResponse.emptyResponseBuilder().tookInMillis(1L).build(),
            null,
            (captured) -> {
                for (int i = 0; i < captured.length; i++) {
                    items[i] = captured[i];
                    failures[i] = captured[i].isFailure();
                    failureExceptions[i] = captured[i].getFailure();
                }
            }
        );
        assertFalse(failures[0]);
        assertThat(items[0].getResponse(), not(nullValue()));
        assertTrue(failures[1]);
        assertThat(items[1].getResponse(), nullValue());
        assertThat(failureExceptions[1], instanceOf(CircuitBreakingException.class));
        assertFalse(failures[2]);
        assertThat(items[2].getResponse(), not(nullValue()));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    private static SearchResponse responseWithHits(SearchHit... hits) {
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1f);
        SearchResponse response = SearchResponseUtils.response(searchHits).build();
        searchHits.decRef();
        return response;
    }

    private void runMsearchWithBreaker(
        TrackingCircuitBreaker breaker,
        int numRequests,
        java.util.function.Supplier<SearchResponse> responseSupplier
    ) throws Exception {
        runMsearchWithBreaker(breaker, numRequests, responseSupplier, null, null);
    }

    private void runMsearchWithBreaker(
        TrackingCircuitBreaker breaker,
        int numRequests,
        java.util.function.Supplier<SearchResponse> responseSupplier,
        AtomicInteger failFirstSearch
    ) throws Exception {
        runMsearchWithBreaker(breaker, numRequests, responseSupplier, failFirstSearch, null);
    }

    private void runMsearchWithBreaker(
        TrackingCircuitBreaker breaker,
        int numRequests,
        java.util.function.Supplier<SearchResponse> responseSupplier,
        AtomicInteger failFirstSearch,
        java.util.function.Consumer<MultiSearchResponse.Item[]> responseItemsConsumer
    ) throws Exception {
        Settings settings = Settings.builder().put("node.name", "msearch-breaker-test").build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        ThreadPool threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
        try {
            TransportService transportService = new TransportService(
                Settings.EMPTY,
                mock(Transport.class),
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                boundAddress -> DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID())
                    .applySettings(settings)
                    .address(boundAddress.publishAddress())
                    .build(),
                null,
                Collections.emptySet()
            );
            ClusterService clusterService = mock(ClusterService.class);
            when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test")).build());
            MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
            for (int i = 0; i < numRequests; i++) {
                multiSearchRequest.add(new SearchRequest());
            }
            NodeClient client = new NodeClient(settings, threadPool, TestProjectResolvers.alwaysThrow()) {
                @Override
                public void search(SearchRequest request, ActionListener<SearchResponse> listener) {
                    if (failFirstSearch != null && failFirstSearch.getAndIncrement() == 0) {
                        listener.onFailure(new RuntimeException("simulated failure"));
                        return;
                    }
                    SearchResponse response = responseSupplier.get();
                    try {
                        listener.onResponse(response);
                    } finally {
                        response.decRef();
                    }
                }

                @Override
                public String getLocalNodeId() {
                    return "local";
                }
            };
            TransportMultiSearchAction action = new TransportMultiSearchAction(
                actionFilters,
                transportService,
                clusterService,
                10,
                System::nanoTime,
                client,
                DefaultProjectResolver.INSTANCE,
                breaker
            );
            PlainActionFuture<MultiSearchResponse> future = new PlainActionFuture<>();
            action.execute(
                multiSearchRequest.createTask(0, "type", "action", null, Map.of()),
                multiSearchRequest,
                responseItemsConsumer == null ? future : future.delegateFailureAndWrap((l, response) -> {
                    responseItemsConsumer.accept(response.getResponses());
                    l.onResponse(response);
                })
            );
            future.get();
        } finally {
            assertTrue(ESTestCase.terminate(threadPool));
        }
    }

    private static final class TrackingCircuitBreaker extends NoopCircuitBreaker {
        private final AtomicLong used = new AtomicLong();
        private final AtomicLong totalReserved = new AtomicLong();
        private final AtomicInteger reservationCalls = new AtomicInteger();
        private final int tripOnCall;

        TrackingCircuitBreaker(int tripOnCall) {
            super("msearch-test");
            this.tripOnCall = tripOnCall;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            if (tripOnCall >= 0 && reservationCalls.incrementAndGet() == tripOnCall) {
                throw new CircuitBreakingException("tripped", getDurability());
            }
            used.addAndGet(bytes);
            totalReserved.addAndGet(bytes);
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used.addAndGet(bytes);
        }

        @Override
        public long getUsed() {
            return used.get();
        }

        long totalReserved() {
            return totalReserved.get();
        }
    }

}
