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
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.lucene.grouping.TopFieldGroups;
import org.elasticsearch.rest.action.search.SearchResponseMetrics;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.Transport;
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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchQueryThenFetchAsyncActionTests extends ESTestCase {

    /**
     * Regression test for a bug in the batched (multi-shard-per-node) query phase: if a data node throws anything
     * other than an {@link IOException} while building the combined response for a {@code NodeQueryRequest}
     * (inside {@code QueryPerNodeState#onShardDone}/{@code #bwcRespond}), the exception used to propagate up to
     * {@code executeShardTasks}'s generic catch block, which "recovers" by calling {@code onShardDone()} again.
     * But the internal {@link org.elasticsearch.common.util.concurrent.CountDown} had already reached zero on the
     * first (failing) call, so the retry silently no-ops and the transport channel is never responded to -- the
     * coordinating node then waits forever for a response that will never arrive.
     * <p>
     * This test drives the real, unmodified {@link SearchQueryThenFetchAsyncAction} end-to-end over a real (mock)
     * transport connection between two nodes, and registers the real, unmodified server-side handler
     * ({@link SearchQueryThenFetchAsyncAction#registerNodeSearchAction}) on the data node. The only test double is
     * the data node's {@link NamedWriteableRegistry}, made to fail exactly the way a genuine PIT/alias-filter
     * decode failure would -- this is what triggers the real (unguarded, prior to the fix) exception inside
     * {@code maybeFreeContext -> isPartOfPIT} while the data node writes its response.
     */
    public void testUnguardedExceptionBuildingBatchedNodeResponseDoesNotHangCoordinator() throws Exception {
        TestThreadPool threadPool = new TestThreadPool(getTestName());
        MockTransportService coordinatorTransport = MockTransportService.createNewService(
            Settings.EMPTY,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool
        );
        MockTransportService dataNodeTransport = MockTransportService.createNewService(
            Settings.EMPTY,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool
        );
        try {
            coordinatorTransport.start();
            coordinatorTransport.acceptIncomingRequests();
            dataNodeTransport.start();
            dataNodeTransport.acceptIncomingRequests();
            DiscoveryNode coordinatorNode = coordinatorTransport.getLocalNode();
            DiscoveryNode dataNode = dataNodeTransport.getLocalNode();
            AbstractSimpleTransportTestCase.connectToNode(coordinatorTransport, dataNode);
            AbstractSimpleTransportTestCase.connectToNode(dataNodeTransport, coordinatorNode);

            // Data node: register the real batched-query handler. The registry fails to resolve the alias
            // filter's query builder while decoding the PIT -- a realistic decode failure -- which is what makes
            // QueryPerNodeState#maybeFreeContext (via #isPartOfPIT) throw while the response is being built.
            NamedWriteableRegistry failingRegistry = mock(NamedWriteableRegistry.class);
            when(failingRegistry.getReader(eq(QueryBuilder.class), any())).thenThrow(
                new IllegalStateException("simulated failure resolving PIT alias filter query")
            );
            SearchService mockSearchService = mock(SearchService.class);
            when(mockSearchService.getCircuitBreaker()).thenReturn(new NoopCircuitBreaker(CircuitBreaker.REQUEST));
            // Collect the "birth ref" of each result created below, to release once the test is done -- mirroring
            // the real InboundHandler's response.decRef() that normally runs once a result has been consumed.
            List<QuerySearchResult> resultsToRelease = Collections.synchronizedList(new ArrayList<>());
            doAnswer(invocation -> {
                ShardSearchRequest request = invocation.getArgument(0);
                @SuppressWarnings("unchecked")
                ActionListener<SearchPhaseResult> listener = invocation.getArgument(2);
                QuerySearchResult result = new QuerySearchResult(
                    new ShardSearchContextId(UUIDs.randomBase64UUID(), request.shardId().id()),
                    new SearchShardTarget(dataNode.getId(), request.shardId(), null),
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
                return null;
            }).when(mockSearchService).executeQueryPhase(any(), any(), any());
            SearchTransportService dataSearchTransportService = new SearchTransportService(dataNodeTransport, null, null);
            SearchQueryThenFetchAsyncAction.registerNodeSearchAction(
                dataSearchTransportService,
                mockSearchService,
                new SearchPhaseController((t, r) -> InternalAggregationTestCase.emptyReduceContextBuilder()),
                failingRegistry
            );

            // Coordinator: a real search request with a PIT referencing a non-empty alias filter, and 2 shards
            // that both live on the (only) data node -- so the coordinator batches them into a single
            // NodeQueryRequest, exactly the scenario that exercises the vulnerable response-building code.
            String indexUuid = "test-index-uuid";
            ShardId shard0 = new ShardId("idx", indexUuid, 0);
            ShardId shard1 = new ShardId("idx", indexUuid, 1);
            Map<ShardId, SearchContextIdForNode> pitShards = Map.of(
                shard0,
                new SearchContextIdForNode(null, dataNode.getId(), new ShardSearchContextId(UUIDs.randomBase64UUID(), 0)),
                shard1,
                new SearchContextIdForNode(null, dataNode.getId(), new ShardSearchContextId(UUIDs.randomBase64UUID(), 1))
            );
            Map<String, AliasFilter> pitAliasFilters = Map.of(indexUuid, AliasFilter.of(new MatchAllQueryBuilder(), "alias1"));
            BytesReference pitId = SearchContextId.encode(
                pitShards,
                pitAliasFilters,
                TransportVersion.current(),
                ShardSearchFailure.EMPTY_ARRAY
            );

            SearchRequest searchRequest = new SearchRequest();
            searchRequest.allowPartialSearchResults(true);
            searchRequest.source(new SearchSourceBuilder().size(0).pointInTimeBuilder(new PointInTimeBuilder(pitId)));

            List<SearchShardIterator> shardsIter = SearchAsyncActionTests.getShardsIter(
                "idx",
                new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS),
                2,
                false,
                dataNode,
                null
            );
            TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
                0,
                System.nanoTime(),
                System::nanoTime
            );
            SearchTask task = new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap());
            SearchTransportService coordinatorSearchTransportService = new SearchTransportService(coordinatorTransport, null, null);
            CountDownLatch latch = new CountDownLatch(1);
            try (
                QueryPhaseResultConsumer resultConsumer = new QueryPhaseResultConsumer(
                    searchRequest,
                    EsExecutors.DIRECT_EXECUTOR_SERVICE,
                    new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                    new SearchPhaseController((t, r) -> InternalAggregationTestCase.emptyReduceContextBuilder()),
                    task::isCancelled,
                    task.getProgressListener(),
                    shardsIter.size(),
                    exc -> {}
                )
            ) {
                SearchQueryThenFetchAsyncAction action = new SearchQueryThenFetchAsyncAction(
                    logger,
                    null,
                    coordinatorSearchTransportService,
                    new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofBytes(Long.MAX_VALUE)),
                    (clusterAlias, nodeId) -> coordinatorTransport.getConnection(dataNode),
                    Collections.singletonMap("_na_", AliasFilter.EMPTY),
                    Collections.emptyMap(),
                    EsExecutors.DIRECT_EXECUTOR_SERVICE,
                    resultConsumer,
                    searchRequest,
                    ActionListener.running(latch::countDown),
                    shardsIter,
                    Collections.emptyMap(),
                    timeProvider,
                    new ClusterState.Builder(new ClusterName("test")).build(),
                    task,
                    SearchResponse.Clusters.EMPTY,
                    null,
                    true,  // batchQueryPhase
                    false, // pitRelocationEnabled
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
                boolean completed = latch.await(10, TimeUnit.SECONDS);
                resultsToRelease.forEach(QuerySearchResult::decRef);
                assertTrue(
                    "search never completed: the data node's response to the batched query never reached the "
                        + "coordinator. This is the bug: an exception in QueryPerNodeState#onShardDone/#bwcRespond "
                        + "left the transport channel with no response at all, so the coordinator waits forever.",
                    completed
                );
            }
        } finally {
            IOUtils.closeWhileHandlingException(coordinatorTransport, dataNodeTransport, threadPool);
        }
    }

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

}
