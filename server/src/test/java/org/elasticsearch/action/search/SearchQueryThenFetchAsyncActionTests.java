/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.lucene.grouping.TopFieldGroups;
import org.elasticsearch.rest.action.search.SearchResponseMetrics;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchQueryThenFetchAsyncActionTests extends ESTestCase {
    public void testBottomFieldSort() throws Exception {
        testCase(false, false);
    }

    public void testScrollDisableBottomFieldSort() throws Exception {
        testCase(true, false);
    }

    public void testCollapseDisableBottomFieldSort() throws Exception {
        testCase(false, true);
    }

    private void testCase(boolean withScroll, boolean withCollapse) throws Exception {
        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            0,
            System.nanoTime(),
            System::nanoTime
        );

        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = DiscoveryNodeUtils.create("node1");
        DiscoveryNode replicaNode = DiscoveryNodeUtils.create("node2");
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));

        int numShards = randomIntBetween(10, 20);
        int numConcurrent = randomIntBetween(1, 4);
        AtomicInteger numWithTopDocs = new AtomicInteger();
        AtomicInteger successfulOps = new AtomicInteger();
        AtomicBoolean canReturnNullResponse = new AtomicBoolean(false);
        // Collect birth refs to release after latch.await(), mirroring InboundHandler's
        // response.decRef() that runs after handleResponse returns.
        List<QuerySearchResult> resultsToRelease = Collections.synchronizedList(new ArrayList<>());
        var transportService = mock(TransportService.class);
        when(transportService.getLocalNode()).thenReturn(primaryNode);
        SearchTransportService searchTransportService = new SearchTransportService(transportService, null, null) {
            @Override
            public void sendExecuteQuery(
                Transport.Connection connection,
                ShardSearchRequest request,
                SearchTask task,
                ActionListener<SearchPhaseResult> listener,
                LongConsumer bytesConsumer,
                LongConsumer requestBytesConsumer
            ) {
                int shardId = request.shardId().id();
                if (request.canReturnNullResponseIfMatchNoDocs()) {
                    canReturnNullResponse.set(true);
                }
                if (request.getBottomSortValues() != null) {
                    assertNotEquals(shardId, (int) request.getBottomSortValues().getFormattedSortValues()[0]);
                    numWithTopDocs.incrementAndGet();
                }
                QuerySearchResult queryResult = new QuerySearchResult(
                    new ShardSearchContextId("N/A", 123),
                    new SearchShardTarget("node1", new ShardId("idx", "na", shardId), null),
                    null
                );
                SortField sortField = new SortField("timestamp", SortField.Type.LONG);
                if (withCollapse) {
                    queryResult.topDocs(
                        new TopDocsAndMaxScore(
                            new TopFieldGroups(
                                "collapse_field",
                                new TotalHits(1, withScroll ? TotalHits.Relation.EQUAL_TO : TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                                new FieldDoc[] { new FieldDoc(randomInt(1000), Float.NaN, new Object[] { request.shardId().id() }) },
                                new SortField[] { sortField },
                                new Object[] { 0L }
                            ),
                            Float.NaN
                        ),
                        new DocValueFormat[] { DocValueFormat.RAW }
                    );
                } else {
                    queryResult.topDocs(
                        new TopDocsAndMaxScore(
                            new TopFieldDocs(
                                new TotalHits(1, withScroll ? TotalHits.Relation.EQUAL_TO : TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                                new FieldDoc[] { new FieldDoc(randomInt(1000), Float.NaN, new Object[] { request.shardId().id() }) },
                                new SortField[] { sortField }
                            ),
                            Float.NaN
                        ),
                        new DocValueFormat[] { DocValueFormat.RAW }
                    );
                }
                queryResult.from(0);
                queryResult.size(1);
                successfulOps.incrementAndGet();
                resultsToRelease.add(queryResult);
                new Thread(() -> listener.onResponse(queryResult)).start();
            }
        };
        CountDownLatch latch = new CountDownLatch(1);
        List<SearchShardIterator> shardsIter = SearchAsyncActionTests.getShardsIter(
            "idx",
            new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS),
            numShards,
            randomBoolean(),
            primaryNode,
            replicaNode
        );
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.setMaxConcurrentShardRequests(numConcurrent);
        searchRequest.setBatchedReduceSize(2);
        searchRequest.source(new SearchSourceBuilder().size(1).sort(SortBuilders.fieldSort("timestamp")));
        if (withScroll) {
            searchRequest.scroll(TimeValue.timeValueMillis(100));
        } else {
            searchRequest.source().trackTotalHitsUpTo(2);
        }
        if (withCollapse) {
            searchRequest.source().collapse(new CollapseBuilder("collapse_field"));
        }
        searchRequest.allowPartialSearchResults(false);
        SearchPhaseController controller = new SearchPhaseController((t, r) -> InternalAggregationTestCase.emptyReduceContextBuilder());
        SearchTask task = new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap());
        try (
            QueryPhaseResultConsumer resultConsumer = new QueryPhaseResultConsumer(
                searchRequest,
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                controller,
                task::isCancelled,
                task.getProgressListener(),
                shardsIter.size(),
                exc -> {}
            )
        ) {
            SearchQueryThenFetchAsyncAction action = new SearchQueryThenFetchAsyncAction(
                logger,
                null,
                searchTransportService,
                new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofBytes(Long.MAX_VALUE)),
                (clusterAlias, node) -> lookup.get(node),
                Collections.singletonMap("_na_", AliasFilter.EMPTY),
                Collections.emptyMap(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                resultConsumer,
                searchRequest,
                null,
                shardsIter,
                Collections.emptyMap(),
                timeProvider,
                new ClusterState.Builder(new ClusterName("test")).build(),
                task,
                SearchResponse.Clusters.EMPTY,
                null,
                false,
                false,
                new SearchResponseMetrics(TelemetryProvider.NOOP.getMeterRegistry()),
                Map.of()
            ) {
                @Override
                protected SearchPhase getNextPhase() {
                    return new SearchPhase("test") {
                        @Override
                        protected void run() {
                            latch.countDown();
                        }
                    };
                }
            };
            action.start();
            latch.await();
            // All onResponse calls are done; release birth refs to mirror the transport's post-handleResponse decRef.
            resultsToRelease.forEach(QuerySearchResult::decRef);
            assertThat(successfulOps.get(), equalTo(numShards));
            if (withScroll) {
                assertFalse(canReturnNullResponse.get());
                assertThat(numWithTopDocs.get(), equalTo(0));
            } else if (withCollapse) {
                assertThat(numWithTopDocs.get(), equalTo(0));
            }
            SearchPhaseController.ReducedQueryPhase phase = action.results.reduce();
            assertThat(phase.numReducePhases(), greaterThanOrEqualTo(1));
            if (withScroll) {
                assertThat(phase.totalHits().value(), equalTo((long) numShards));
                assertThat(phase.totalHits().relation(), equalTo(TotalHits.Relation.EQUAL_TO));
            } else {
                assertThat(phase.totalHits().value(), equalTo(2L));
                assertThat(phase.totalHits().relation(), equalTo(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));
            }
            assertThat(phase.sortedTopDocs().scoreDocs().length, equalTo(1));
            assertThat(phase.sortedTopDocs().scoreDocs()[0], instanceOf(FieldDoc.class));
            assertThat(((FieldDoc) phase.sortedTopDocs().scoreDocs()[0]).fields.length, equalTo(1));
            assertThat(((FieldDoc) phase.sortedTopDocs().scoreDocs()[0]).fields[0], equalTo(0));
        }
    }

    static class BadRawDocValueFormat implements DocValueFormat {
        @Override
        public String getWriteableName() {
            return "bad";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}

        @Override
        public Object format(long value) {
            if (value == Long.MAX_VALUE) {
                // Simulate a bad value that cannot be formatted correctly
                throw new IllegalArgumentException("Cannot format Long.MAX_VALUE");
            }
            return RawDocValueFormat.INSTANCE.format(value);
        }

        @Override
        public Object format(double value) {
            return RawDocValueFormat.INSTANCE.format(value);
        }

        @Override
        public Object format(BytesRef value) {
            return RawDocValueFormat.INSTANCE.format(value);
        }

        @Override
        public long parseLong(String value, boolean roundUp, LongSupplier now) {
            return RawDocValueFormat.INSTANCE.parseLong(value, roundUp, now);
        }

        @Override
        public double parseDouble(String value, boolean roundUp, LongSupplier now) {
            return RawDocValueFormat.INSTANCE.parseLong(value, roundUp, now);
        }

        @Override
        public BytesRef parseBytesRef(Object value) {
            return RawDocValueFormat.INSTANCE.parseBytesRef(value);
        }

        @Override
        public Object formatSortValue(Object value) {
            return RawDocValueFormat.INSTANCE.formatSortValue(value);
        }
    }

    // Test what happens if doc formatter fails to format the bottom sort values
    public void testBadFormatting() throws Exception {
        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            0,
            System.nanoTime(),
            System::nanoTime
        );

        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = DiscoveryNodeUtils.create("node1");
        DiscoveryNode replicaNode = DiscoveryNodeUtils.create("node2");
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));

        int numShards = randomIntBetween(10, 20);
        int numConcurrent = randomIntBetween(1, 4);
        AtomicInteger numWithTopDocs = new AtomicInteger();
        AtomicInteger successfulOps = new AtomicInteger();
        AtomicBoolean canReturnNullResponse = new AtomicBoolean(false);
        // Collect birth refs to release after latch.await(), mirroring InboundHandler's
        // response.decRef() that runs after handleResponse returns.
        List<QuerySearchResult> resultsToRelease = Collections.synchronizedList(new ArrayList<>());
        var transportService = mock(TransportService.class);
        when(transportService.getLocalNode()).thenReturn(primaryNode);
        SearchTransportService searchTransportService = new SearchTransportService(transportService, null, null) {
            @Override
            public void sendExecuteQuery(
                Transport.Connection connection,
                ShardSearchRequest request,
                SearchTask task,
                ActionListener<SearchPhaseResult> listener,
                LongConsumer bytesConsumer,
                LongConsumer requestBytesConsumer
            ) {
                int shardId = request.shardId().id();
                if (request.canReturnNullResponseIfMatchNoDocs()) {
                    canReturnNullResponse.set(true);
                }
                if (request.getBottomSortValues() != null) {
                    numWithTopDocs.incrementAndGet();
                }
                QuerySearchResult queryResult = new QuerySearchResult(
                    new ShardSearchContextId("N/A", 123),
                    new SearchShardTarget("node1", new ShardId("idx", "na", shardId), null),
                    null
                );
                SortField sortField = new SortField("RegistrationDate", SortField.Type.LONG);
                queryResult.topDocs(
                    new TopDocsAndMaxScore(
                        new TopFieldDocs(
                            new TotalHits(1, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                            new FieldDoc[] { new FieldDoc(0, Float.NaN, new Object[] { Long.MAX_VALUE }) },
                            new SortField[] { sortField }
                        ),
                        Float.NaN
                    ),
                    new DocValueFormat[] { new BadRawDocValueFormat() }
                );
                queryResult.from(0);
                queryResult.size(1);
                successfulOps.incrementAndGet();
                resultsToRelease.add(queryResult);
                new Thread(() -> listener.onResponse(queryResult)).start();
            }
        };
        CountDownLatch latch = new CountDownLatch(1);
        List<SearchShardIterator> shardsIter = SearchAsyncActionTests.getShardsIter(
            "idx",
            new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS),
            numShards,
            randomBoolean(),
            primaryNode,
            replicaNode
        );
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.setMaxConcurrentShardRequests(numConcurrent);
        searchRequest.setBatchedReduceSize(2);
        searchRequest.source(new SearchSourceBuilder().size(1).sort(SortBuilders.fieldSort("timestamp")));
        searchRequest.source().trackTotalHitsUpTo(2);
        searchRequest.allowPartialSearchResults(false);
        SearchPhaseController controller = new SearchPhaseController((t, r) -> InternalAggregationTestCase.emptyReduceContextBuilder());
        SearchTask task = new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap());
        try (
            QueryPhaseResultConsumer resultConsumer = new QueryPhaseResultConsumer(
                searchRequest,
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                controller,
                task::isCancelled,
                task.getProgressListener(),
                shardsIter.size(),
                exc -> {}
            )
        ) {
            SearchQueryThenFetchAsyncAction action = new SearchQueryThenFetchAsyncAction(
                logger,
                null,
                searchTransportService,
                new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofBytes(Long.MAX_VALUE)),
                (clusterAlias, node) -> lookup.get(node),
                Collections.singletonMap("_na_", AliasFilter.EMPTY),
                Collections.emptyMap(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                resultConsumer,
                searchRequest,
                null,
                shardsIter,
                Collections.emptyMap(),
                timeProvider,
                new ClusterState.Builder(new ClusterName("test")).build(),
                task,
                SearchResponse.Clusters.EMPTY,
                null,
                false,
                false,
                new SearchResponseMetrics(TelemetryProvider.NOOP.getMeterRegistry()),
                Map.of()
            ) {
                @Override
                protected SearchPhase getNextPhase() {
                    return new SearchPhase("test") {
                        @Override
                        protected void run() {
                            latch.countDown();
                        }
                    };
                }

                @Override
                void onShardFailure(int shardIndex, SearchShardTarget shardTarget, Exception e) {
                    latch.countDown();
                    fail(e, "Unexpected shard failure");
                }
            };
            action.start();
            latch.await();
            // All onResponse calls are done; release birth refs to mirror the transport's post-handleResponse decRef.
            resultsToRelease.forEach(QuerySearchResult::decRef);
            assertThat(successfulOps.get(), equalTo(numShards));
            SearchPhaseController.ReducedQueryPhase phase = action.results.reduce();
            assertThat(phase.numReducePhases(), greaterThanOrEqualTo(1));
            assertThat(phase.totalHits().value(), equalTo(2L));
        }
    }

    /**
     * Ensure the batched query handler always responds to the coordinator channel even when an
     * exception is thrown during response serialization.
     */
    public void testBatchedNodeResponseExceptionReachesCoordinator() throws Exception {
        assertSerializationExceptionReachesCoordinator(TransportVersion.current());
    }

    /**
     * Ensure the batched query handler always responds to the coordinator channel even when an
     * exception is thrown during response serialization on the backwards-compatible
     * {@code bwcBuildResponse} path.
     */
    public void testBwcBuildResponseExceptionReachesCoordinator() throws Exception {
        // batched_query_phase_version predates batched_response_might_include_reduction_failure,
        // so using it as the channel version steers buildResponse() into bwcBuildResponse().
        assertSerializationExceptionReachesCoordinator(TransportVersion.fromName("batched_query_phase_version"));
    }

    private void assertSerializationExceptionReachesCoordinator(TransportVersion channelVersion) throws Exception {
        TestThreadPool threadPool = new TestThreadPool(getTestName());
        var innerTransport = MockTransportService.newMockTransport(Settings.EMPTY, TransportVersion.current(), threadPool);
        String nodeId = UUIDs.randomBase64UUID();
        MockTransportService transport = new MockTransportService(
            Settings.EMPTY,
            innerTransport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNodeUtils.builder(nodeId)
                .address(boundAddress.publishAddress())
                .version(VersionInformation.CURRENT)
                .build(),
            null,
            Collections.emptySet(),
            nodeId
        ) {
            // This stream will throw on first write
            @Override
            public RecyclerBytesStreamOutput newNetworkBytesStream(CircuitBreaker circuitBreaker) {
                return new RecyclerBytesStreamOutput(
                    new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofBytes(Long.MAX_VALUE)).bytesRefRecycler()
                ) {
                    @Override
                    public void writeVInt(int i) {
                        throw new RuntimeException("simulated write failure");
                    }
                };
            }
        };
        ClusterService clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool,
            null
        );
        List<QuerySearchResult> resultsToRelease = Collections.synchronizedList(new ArrayList<>());
        SearchService searchService = new SearchService(
            clusterService,
            null,
            threadPool,
            null,
            null,
            new FetchPhase(Collections.emptyList()),
            newLimitedBreakerService(ByteSizeValue.ofMb(10)),
            EmptySystemIndices.INSTANCE.getExecutorSelector(),
            Tracer.NOOP,
            OnlinePrewarmingService.NOOP
        ) {
            @Override
            public void executeQueryPhase(ShardSearchRequest req, CancellableTask task, ActionListener<SearchPhaseResult> listener) {
                QuerySearchResult result = new QuerySearchResult(
                    new ShardSearchContextId(UUIDs.randomBase64UUID(), 1),
                    new SearchShardTarget(transport.getLocalNode().getId(), req.shardId(), null),
                    null
                );
                result.topDocs(
                    new TopDocsAndMaxScore(new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]), Float.NaN),
                    new DocValueFormat[0]
                );
                result.from(0);
                result.size(0);
                resultsToRelease.add(result);
                listener.onResponse(result);
            }
        };
        try {
            transport.start();

            SearchQueryThenFetchAsyncAction.registerNodeSearchAction(
                new SearchTransportService(transport, null, null),
                searchService,
                new SearchPhaseController((t, r) -> InternalAggregationTestCase.emptyReduceContextBuilder()),
                new NamedWriteableRegistry(List.of())
            );

            SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(new SearchSourceBuilder().size(0));
            var nodeQueryRequest = new SearchQueryThenFetchAsyncAction.NodeQueryRequest(searchRequest, 2, 0L, null, false);
            nodeQueryRequest.shards.add(
                new SearchQueryThenFetchAsyncAction.ShardToQuery(
                    1.0f,
                    new String[] { "idx" },
                    0,
                    new ShardId("idx", "uuid", 0),
                    null,
                    SplitShardCountSummary.UNSET
                )
            );
            nodeQueryRequest.shards.add(
                new SearchQueryThenFetchAsyncAction.ShardToQuery(
                    1.0f,
                    new String[] { "idx" },
                    1,
                    new ShardId("idx", "uuid", 1),
                    null,
                    SplitShardCountSummary.UNSET
                )
            );

            CountDownLatch latch = new CountDownLatch(1);
            TransportChannel channel = new TransportChannel() {
                @Override
                public String getProfileName() {
                    return "";
                }

                @Override
                public TransportVersion getVersion() {
                    return channelVersion;
                }

                @Override
                public void sendResponse(TransportResponse response) {}

                @Override
                public void sendResponse(Exception exception) {
                    latch.countDown();
                }
            };

            @SuppressWarnings("unchecked")
            RequestHandlerRegistry<SearchQueryThenFetchAsyncAction.NodeQueryRequest> handler = (RequestHandlerRegistry<
                SearchQueryThenFetchAsyncAction.NodeQueryRequest>) transport.getRequestHandler(
                    SearchQueryThenFetchAsyncAction.NODE_SEARCH_ACTION_NAME
                );
            handler.processMessageReceived(nodeQueryRequest, channel);

            boolean completed = latch.await(10, TimeUnit.SECONDS);
            resultsToRelease.forEach(QuerySearchResult::decRef);
            assertTrue("channel was not responded to within 10 seconds", completed);
        } finally {
            IOUtils.closeWhileHandlingException(searchService, clusterService, transport, threadPool);
        }
    }

}
