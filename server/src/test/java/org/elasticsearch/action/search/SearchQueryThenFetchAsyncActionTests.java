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
import org.elasticsearch.action.NoShardAvailableActionException;
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
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
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
import org.elasticsearch.common.util.concurrent.TaskLoadAttribution;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.lucene.grouping.TopFieldGroups;
import org.elasticsearch.rest.action.search.SearchResponseMetrics;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.PointInTimeBuilder;
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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.BytesTransportResponse;
import org.elasticsearch.transport.CloseableConnection;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.TestProxyConnections;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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
                ShardResponseReservation reservation,
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
                NoopCircuitBreaker.INSTANCE,
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
                NoopCircuitBreaker.INSTANCE,
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

    /**
     * A shard response that trips the breaker while it arrives fails the search once, without retrying shards or finishing the phase.
     */
    public void testPerShardResponseTripFailsSearch() throws Exception {
        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = DiscoveryNodeUtils.create("node1");
        DiscoveryNode replicaNode = DiscoveryNodeUtils.create("node2");
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));

        Map<Integer, Integer> requestsPerShard = new ConcurrentHashMap<>();
        List<ShardSearchContextId> freedContexts = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<CircuitBreakingException> trip = new AtomicReference<>();
        List<QuerySearchResult> resultsToRelease = Collections.synchronizedList(new ArrayList<>());
        Map<Integer, Runnable> responses = new ConcurrentHashMap<>();
        var transportService = mock(TransportService.class);
        when(transportService.getLocalNode()).thenReturn(primaryNode);
        SearchTransportService searchTransportService = new SearchTransportService(transportService, null, null) {
            @Override
            public void sendExecuteQuery(
                Transport.Connection connection,
                ShardSearchRequest request,
                SearchTask task,
                ShardResponseReservation reservation,
                ActionListener<SearchPhaseResult> listener,
                LongConsumer bytesConsumer,
                LongConsumer requestBytesConsumer
            ) {
                int shardId = request.shardId().id();
                requestsPerShard.merge(shardId, 1, Integer::sum);
                // Answered once all three are in flight, as a trip doesn't stop requests already sent
                responses.put(shardId, () -> answer(shardId, connection, request, reservation, listener));
            }

            private void answer(
                int shardId,
                Transport.Connection connection,
                ShardSearchRequest request,
                ShardResponseReservation reservation,
                ActionListener<SearchPhaseResult> listener
            ) {
                switch (shardId) {
                    case 0 -> trip.set(ShardResponseReservationTests.tripWhileArriving(reservation, listener));
                    case 1 -> {
                        QuerySearchResult result = new QuerySearchResult(
                            new ShardSearchContextId("late", shardId),
                            new SearchShardTarget(connection.getNode().getId(), request.shardId(), null),
                            null
                        );
                        result.topDocs(
                            new TopDocsAndMaxScore(new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]), Float.NaN),
                            new DocValueFormat[0]
                        );
                        resultsToRelease.add(result);
                        listener.onResponse(result);
                    }
                    case 2 -> listener.onFailure(new NoShardAvailableActionException(request.shardId()));
                    default -> throw new AssertionError("unexpected shard [" + shardId + "]");
                }
            }

            @Override
            public void sendFreeContext(
                Transport.Connection connection,
                ShardSearchContextId contextId,
                ActionListener<SearchFreeContextResponse> listener
            ) {
                freedContexts.add(contextId);
            }
        };
        List<SearchShardIterator> shardsIter = SearchAsyncActionTests.getShardsIter(
            "idx",
            new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS),
            3,
            true,
            primaryNode,
            replicaNode
        );
        List<Exception> failures = Collections.synchronizedList(new ArrayList<>());
        try (QueryPhaseResultConsumer resultConsumer = newResultConsumer(shardsIter.size())) {
            newSearchThatMustNotFinish(
                searchTransportService,
                lookup,
                shardsIter,
                resultConsumer,
                false,
                SearchRequest.DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS,
                failures
            ).start();
            assertThat(responses.keySet(), containsInAnyOrder(0, 1, 2));
            for (int shardId = 0; shardId < 3; shardId++) {
                responses.get(shardId).run();
            }
            resultsToRelease.forEach(QuerySearchResult::decRef);

            assertThat(failures, hasSize(1));
            assertThat(failures.getFirst(), instanceOf(SearchPhaseExecutionException.class));
            SearchPhaseExecutionException e = (SearchPhaseExecutionException) failures.getFirst();
            assertThat(e.getPhaseName(), equalTo("query"));
            assertThat(e.getCause(), sameInstance(trip.get()));
            assertThat(e.shardFailures(), emptyArray());
            // Shard 2 has a replica, but a retry would only add work to a search that has failed
            assertThat(requestsPerShard, equalTo(Map.of(0, 1, 1, 1, 2, 1)));
            assertThat(freedContexts, contains(new ShardSearchContextId("late", 1)));
        }
    }

    /**
     * A batched response that trips the breaker while it arrives fails the search once, without retrying shards or finishing the phase.
     */
    public void testBatchedResponseTripFailsSearch() throws Exception {
        // An older data node takes the bwcHandleException route, which would fail the search a second time
        assertBatchedResponseTripFailsSearch(TransportVersion.current());
        assertBatchedResponseTripFailsSearch(TransportVersion.fromName("batched_query_phase_version"));
    }

    private void assertBatchedResponseTripFailsSearch(TransportVersion dataNodeVersion) throws Exception {
        DiscoveryNode coordinator = DiscoveryNodeUtils.create("coordinator");
        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        for (String nodeId : List.of("node1", "node2", "replicas")) {
            DiscoveryNode node = DiscoveryNodeUtils.create(nodeId);
            lookup.put(nodeId, new CloseableConnection() {
                @Override
                public DiscoveryNode getNode() {
                    return node;
                }

                @Override
                public TransportVersion getTransportVersion() {
                    return dataNodeVersion;
                }

                @Override
                public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) {
                    throw new UnsupportedOperationException();
                }
            });
        }
        // Two shards on each data node, so that each node gets a batch, and a second copy of every shard to retry on
        OriginalIndices originalIndices = new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS);
        List<SearchShardIterator> shardsIter = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            shardsIter.add(
                new SearchShardIterator(
                    null,
                    new ShardId("idx", "_na_", i),
                    List.of(i < 2 ? "node1" : "node2", "replicas"),
                    originalIndices,
                    null,
                    null,
                    false,
                    false,
                    SplitShardCountSummary.UNSET
                )
            );
        }

        Map<String, TransportResponseHandler<?>> batches = new ConcurrentHashMap<>();
        var transportService = mock(TransportService.class);
        when(transportService.getLocalNode()).thenReturn(coordinator);
        doAnswer(invocation -> {
            Transport.Connection connection = invocation.getArgument(0);
            batches.put(connection.getNode().getId(), invocation.getArgument(4));
            return null;
        }).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                eq(SearchQueryThenFetchAsyncAction.NODE_SEARCH_ACTION_NAME),
                any(),
                any(),
                any()
            );
        AtomicInteger retries = new AtomicInteger();
        SearchTransportService searchTransportService = new SearchTransportService(transportService, null, null) {
            @Override
            public void sendExecuteQuery(
                Transport.Connection connection,
                ShardSearchRequest request,
                SearchTask task,
                ShardResponseReservation reservation,
                ActionListener<SearchPhaseResult> listener,
                LongConsumer bytesConsumer,
                LongConsumer requestBytesConsumer
            ) {
                retries.incrementAndGet();
            }
        };

        List<Exception> failures = Collections.synchronizedList(new ArrayList<>());
        try (QueryPhaseResultConsumer resultConsumer = newResultConsumer(shardsIter.size())) {
            newSearchThatMustNotFinish(
                searchTransportService,
                lookup,
                shardsIter,
                resultConsumer,
                true,
                SearchRequest.DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS,
                failures
            ).start();
            assertThat(batches.keySet(), containsInAnyOrder("node1", "node2"));

            CircuitBreakingException trip = ShardResponseReservationTests.tripWhileArriving(batches.get("node1"));
            batches.get("node2")
                .handleException(new SendRequestTransportException(lookup.get("node2").getNode(), "action", new IOException("boom")));

            assertThat(failures, hasSize(1));
            SearchPhaseExecutionException e = (SearchPhaseExecutionException) failures.getFirst();
            assertThat(e.getPhaseName(), equalTo("query"));
            assertThat(e.getCause(), sameInstance(trip));
            assertThat(e.shardFailures(), emptyArray());
            assertThat("a shard was retried on another copy", retries.get(), equalTo(0));
        }
    }

    /**
     * A result that arrives while another shard trips is released, also when the trip releases the results it has after the late result
     * passed its check but before it was added.
     */
    public void testResultArrivingDuringTripIsReleased() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node1");
        Map<String, Transport.Connection> lookup = Map.of("node1", new SearchAsyncActionTests.MockConnection(node));
        OriginalIndices originalIndices = new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS);
        List<SearchShardIterator> shardsIter = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            shardsIter.add(
                new SearchShardIterator(
                    null,
                    new ShardId("idx", "_na_", i),
                    List.of("node1"),
                    originalIndices,
                    null,
                    null,
                    false,
                    false,
                    SplitShardCountSummary.UNSET
                )
            );
        }

        Map<Integer, Runnable> responses = new ConcurrentHashMap<>();
        List<ShardSearchContextId> freedContexts = Collections.synchronizedList(new ArrayList<>());
        List<QuerySearchResult> resultsToRelease = Collections.synchronizedList(new ArrayList<>());
        var transportService = mock(TransportService.class);
        when(transportService.getLocalNode()).thenReturn(DiscoveryNodeUtils.create("coordinator"));
        SearchTransportService searchTransportService = new SearchTransportService(transportService, null, null) {
            @Override
            public void sendExecuteQuery(
                Transport.Connection connection,
                ShardSearchRequest request,
                SearchTask task,
                ShardResponseReservation reservation,
                ActionListener<SearchPhaseResult> listener,
                LongConsumer bytesConsumer,
                LongConsumer requestBytesConsumer
            ) {
                int shardId = request.shardId().id();
                if (shardId == 0) {
                    responses.put(shardId, () -> ShardResponseReservationTests.tripWhileArriving(reservation, listener));
                    return;
                }
                responses.put(shardId, () -> {
                    QuerySearchResult result = new QuerySearchResult(
                        new ShardSearchContextId("late", shardId),
                        new SearchShardTarget(connection.getNode().getId(), request.shardId(), null),
                        null
                    );
                    result.topDocs(
                        new TopDocsAndMaxScore(new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]), Float.NaN),
                        new DocValueFormat[0]
                    );
                    resultsToRelease.add(result);
                    listener.onResponse(result);
                });
            }

            @Override
            public void sendFreeContext(
                Transport.Connection connection,
                ShardSearchContextId contextId,
                ActionListener<SearchFreeContextResponse> listener
            ) {
                freedContexts.add(contextId);
            }
        };

        List<Exception> failures = Collections.synchronizedList(new ArrayList<>());
        // Shard 0 trips while shard 1's result is being added, the order two threads can run them in
        try (
            QueryPhaseResultConsumer resultConsumer = new QueryPhaseResultConsumer(
                new SearchRequest(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                NoopCircuitBreaker.INSTANCE,
                new SearchPhaseController((t, r) -> InternalAggregationTestCase.emptyReduceContextBuilder()),
                () -> false,
                SearchProgressListener.NOOP,
                shardsIter.size(),
                exc -> {}
            ) {
                @Override
                public void consumeResult(SearchPhaseResult result, Runnable next) {
                    responses.get(0).run();
                    super.consumeResult(result, next);
                }
            }
        ) {
            newSearchThatMustNotFinish(
                searchTransportService,
                lookup,
                shardsIter,
                resultConsumer,
                false,
                SearchRequest.DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS,
                failures
            ).start();
            assertThat(responses.keySet(), containsInAnyOrder(0, 1));

            responses.get(1).run();
            resultsToRelease.forEach(QuerySearchResult::decRef);

            assertThat(failures, hasSize(1));
            assertThat(freedContexts, contains(new ShardSearchContextId("late", 1)));
        }
    }

    /**
     * A shard that fails after the trip on its last copy isn't counted, so the phase doesn't finish with results the trip released.
     */
    public void testShardFailureAfterTripDoesNotFinishPhase() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node1");
        Map<String, Transport.Connection> lookup = Map.of("node1", new SearchAsyncActionTests.MockConnection(node));
        // One copy of each shard, so that a failure isn't retried
        OriginalIndices originalIndices = new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS);
        List<SearchShardIterator> shardsIter = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            shardsIter.add(
                new SearchShardIterator(
                    null,
                    new ShardId("idx", "_na_", i),
                    List.of("node1"),
                    originalIndices,
                    null,
                    null,
                    false,
                    false,
                    SplitShardCountSummary.UNSET
                )
            );
        }

        Map<Integer, Runnable> responses = new ConcurrentHashMap<>();
        AtomicReference<CircuitBreakingException> trip = new AtomicReference<>();
        var transportService = mock(TransportService.class);
        when(transportService.getLocalNode()).thenReturn(DiscoveryNodeUtils.create("coordinator"));
        SearchTransportService searchTransportService = new SearchTransportService(transportService, null, null) {
            @Override
            public void sendExecuteQuery(
                Transport.Connection connection,
                ShardSearchRequest request,
                SearchTask task,
                ShardResponseReservation reservation,
                ActionListener<SearchPhaseResult> listener,
                LongConsumer bytesConsumer,
                LongConsumer requestBytesConsumer
            ) {
                int shardId = request.shardId().id();
                responses.put(
                    shardId,
                    shardId == 0
                        ? () -> trip.set(ShardResponseReservationTests.tripWhileArriving(reservation, listener))
                        : () -> listener.onFailure(new NoShardAvailableActionException(request.shardId()))
                );
            }
        };

        List<Exception> failures = Collections.synchronizedList(new ArrayList<>());
        try (QueryPhaseResultConsumer resultConsumer = newResultConsumer(shardsIter.size())) {
            newSearchThatMustNotFinish(
                searchTransportService,
                lookup,
                shardsIter,
                resultConsumer,
                false,
                SearchRequest.DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS,
                failures
            ).start();
            assertThat(responses.keySet(), containsInAnyOrder(0, 1));

            responses.get(0).run();
            responses.get(1).run();

            assertThat(failures, hasSize(1));
            assertThat(failures.getFirst().getCause(), sameInstance(trip.get()));
        }
    }

    /**
     * Shards queued behind a request that trips are not sent. The search task is gone by then, so a failed response would send no
     * cancel_child, and the reader context the request opens would wait for the reaper.
     */
    public void testPerShardResponseTripSkipsQueuedShards() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node1");
        Map<String, Transport.Connection> lookup = Map.of("node1", new SearchAsyncActionTests.MockConnection(node));
        OriginalIndices originalIndices = new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS);
        List<SearchShardIterator> shardsIter = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            shardsIter.add(
                new SearchShardIterator(
                    null,
                    new ShardId("idx", "_na_", i),
                    List.of("node1"),
                    originalIndices,
                    null,
                    null,
                    false,
                    false,
                    SplitShardCountSummary.UNSET
                )
            );
        }

        List<Integer> sentShards = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Runnable> tripFirst = new AtomicReference<>();
        AtomicReference<CircuitBreakingException> trip = new AtomicReference<>();
        var transportService = mock(TransportService.class);
        when(transportService.getLocalNode()).thenReturn(DiscoveryNodeUtils.create("coordinator"));
        SearchTransportService searchTransportService = new SearchTransportService(transportService, null, null) {
            @Override
            public void sendExecuteQuery(
                Transport.Connection connection,
                ShardSearchRequest request,
                SearchTask task,
                ShardResponseReservation reservation,
                ActionListener<SearchPhaseResult> listener,
                LongConsumer bytesConsumer,
                LongConsumer requestBytesConsumer
            ) {
                sentShards.add(request.shardId().id());
                tripFirst.compareAndSet(null, () -> trip.set(ShardResponseReservationTests.tripWhileArriving(reservation, listener)));
            }
        };

        List<Exception> failures = Collections.synchronizedList(new ArrayList<>());
        try (QueryPhaseResultConsumer resultConsumer = newResultConsumer(shardsIter.size())) {
            // One request at a time per node, so that the other two shards wait for the first
            newSearchThatMustNotFinish(searchTransportService, lookup, shardsIter, resultConsumer, false, 1, failures).start();
            assertThat(sentShards, contains(0));

            tripFirst.get().run();

            assertThat(failures, hasSize(1));
            assertThat(failures.getFirst().getCause(), sameInstance(trip.get()));
            assertThat("a queued shard was sent after the trip", sentShards, contains(0));
        }
    }

    /**
     * Node batches not yet sent when a sent one trips are not sent, for the reason
     * {@link #testPerShardResponseTripSkipsQueuedShards} gives.
     */
    public void testBatchedResponseTripSkipsUnsentNodes() throws Exception {
        DiscoveryNode coordinator = DiscoveryNodeUtils.create("coordinator");
        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        for (String nodeId : List.of("node1", "node2")) {
            lookup.put(nodeId, new SearchAsyncActionTests.MockConnection(DiscoveryNodeUtils.create(nodeId)));
        }
        // Two shards on each data node, so that each node gets a batch
        OriginalIndices originalIndices = new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS);
        List<SearchShardIterator> shardsIter = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            shardsIter.add(
                new SearchShardIterator(
                    null,
                    new ShardId("idx", "_na_", i),
                    List.of(i < 2 ? "node1" : "node2"),
                    originalIndices,
                    null,
                    null,
                    false,
                    false,
                    SplitShardCountSummary.UNSET
                )
            );
        }

        List<String> sentNodes = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<CircuitBreakingException> trip = new AtomicReference<>();
        var transportService = mock(TransportService.class);
        when(transportService.getLocalNode()).thenReturn(coordinator);
        doAnswer(invocation -> {
            Transport.Connection connection = invocation.getArgument(0);
            sentNodes.add(connection.getNode().getId());
            // The first node answers while the coordinator is still sending, and trips
            if (sentNodes.size() == 1) {
                trip.set(ShardResponseReservationTests.tripWhileArriving((TransportResponseHandler<?>) invocation.getArgument(4)));
            }
            return null;
        }).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                eq(SearchQueryThenFetchAsyncAction.NODE_SEARCH_ACTION_NAME),
                any(),
                any(),
                any()
            );
        SearchTransportService searchTransportService = new SearchTransportService(transportService, null, null);

        List<Exception> failures = Collections.synchronizedList(new ArrayList<>());
        try (QueryPhaseResultConsumer resultConsumer = newResultConsumer(shardsIter.size())) {
            newSearchThatMustNotFinish(
                searchTransportService,
                lookup,
                shardsIter,
                resultConsumer,
                true,
                SearchRequest.DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS,
                failures
            ).start();

            assertThat(failures, hasSize(1));
            assertThat(failures.getFirst().getCause(), sameInstance(trip.get()));
            assertThat("a node batch was sent after the trip", sentNodes, hasSize(1));
        }
    }

    /**
     * The breaker trips after the coordinator decided to send a shard request, but before it was sent. The search must fail only
     * once that request is sent. For the reasoning, see {@code AbstractSearchAsyncAction#deferCoordinatorTripFailure}.
     */
    public void testTripWhileSendingShardRequestFailsSearchAfterItIsSent() {
        OriginalIndices originalIndices = new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS);
        List<SearchShardIterator> shardsIter = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            shardsIter.add(
                new SearchShardIterator(
                    null,
                    new ShardId("idx", "_na_", i),
                    List.of("node" + (i + 1)),
                    originalIndices,
                    null,
                    null,
                    false,
                    false,
                    SplitShardCountSummary.UNSET
                )
            );
        }

        List<Exception> failures = Collections.synchronizedList(new ArrayList<>());
        Map<Integer, Integer> failuresWhenSent = new ConcurrentHashMap<>();
        AtomicReference<Runnable> tripShard0 = new AtomicReference<>();
        AtomicReference<CircuitBreakingException> trip = new AtomicReference<>();
        // Unused, as sendExecuteQuery is overridden below, but SearchTransportService needs one; a real one needs a
        // transport and thread pool
        var transportService = mock(TransportService.class);
        SearchTransportService searchTransportService = new SearchTransportService(transportService, null, null) {
            @Override
            public void sendExecuteQuery(
                Transport.Connection connection,
                ShardSearchRequest request,
                SearchTask task,
                ShardResponseReservation reservation,
                ActionListener<SearchPhaseResult> listener,
                LongConsumer bytesConsumer,
                LongConsumer requestBytesConsumer
            ) {
                int shardId = request.shardId().id();
                failuresWhenSent.put(shardId, failures.size());
                if (shardId == 0) {
                    tripShard0.set(() -> trip.set(ShardResponseReservationTests.tripWhileArriving(reservation, listener)));
                }
            }
        };
        // Shard 0 trips while the coordinator gets the connection for shard 1, after shard 1 checked for a trip
        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>() {
            @Override
            public Transport.Connection get(Object nodeId) {
                Runnable tripNow = nodeId.equals("node2") ? tripShard0.getAndSet(null) : null;
                if (tripNow != null) {
                    tripNow.run();
                }
                return super.get(nodeId);
            }
        };
        for (String nodeId : List.of("node1", "node2")) {
            lookup.put(nodeId, new SearchAsyncActionTests.MockConnection(DiscoveryNodeUtils.create(nodeId)));
        }

        try (QueryPhaseResultConsumer resultConsumer = newResultConsumer(shardsIter.size())) {
            newSearchThatMustNotFinish(
                searchTransportService,
                lookup,
                shardsIter,
                resultConsumer,
                false,
                SearchRequest.DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS,
                failures
            ).start();

            assertThat("the search failed before shard 1 was sent", failuresWhenSent, equalTo(Map.of(0, 0, 1, 0)));
            assertThat(failures, hasSize(1));
            assertThat(failures.getFirst().getCause(), sameInstance(trip.get()));
        }
    }

    /**
     * Same as {@link #testTripWhileSendingShardRequestFailsSearchAfterItIsSent}, for a node batch.
     */
    public void testTripWhileSendingNodeBatchFailsSearchAfterItIsSent() {
        // Two shards on each data node, so that each node gets a batch
        OriginalIndices originalIndices = new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS);
        List<SearchShardIterator> shardsIter = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            shardsIter.add(
                new SearchShardIterator(
                    null,
                    new ShardId("idx", "_na_", i),
                    List.of(i < 2 ? "node1" : "node2"),
                    originalIndices,
                    null,
                    null,
                    false,
                    false,
                    SplitShardCountSummary.UNSET
                )
            );
        }

        List<Exception> failures = Collections.synchronizedList(new ArrayList<>());
        List<Integer> failuresWhenSent = Collections.synchronizedList(new ArrayList<>());
        List<TransportResponseHandler<?>> sentBatches = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<CircuitBreakingException> trip = new AtomicReference<>();
        // Only getLocalNode and sendChildRequest are used, to capture each batch's handler; a real TransportService needs a transport
        // and thread pool
        var transportService = mock(TransportService.class);
        when(transportService.getLocalNode()).thenReturn(DiscoveryNodeUtils.create("coordinator"));
        doAnswer(invocation -> {
            failuresWhenSent.add(failures.size());
            sentBatches.add(invocation.getArgument(4));
            return null;
        }).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                eq(SearchQueryThenFetchAsyncAction.NODE_SEARCH_ACTION_NAME),
                any(),
                any(),
                any()
            );
        SearchTransportService searchTransportService = new SearchTransportService(transportService, null, null);
        // The batch sent first trips while the coordinator gets the connection for the other node, which checked for a trip already
        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>() {
            @Override
            public Transport.Connection get(Object nodeId) {
                if (sentBatches.size() == 1 && trip.get() == null) {
                    trip.set(ShardResponseReservationTests.tripWhileArriving(sentBatches.getFirst()));
                }
                return super.get(nodeId);
            }
        };
        for (String nodeId : List.of("node1", "node2")) {
            lookup.put(nodeId, new SearchAsyncActionTests.MockConnection(DiscoveryNodeUtils.create(nodeId)));
        }

        try (QueryPhaseResultConsumer resultConsumer = newResultConsumer(shardsIter.size())) {
            newSearchThatMustNotFinish(
                searchTransportService,
                lookup,
                shardsIter,
                resultConsumer,
                true,
                SearchRequest.DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS,
                failures
            ).start();

            assertThat("the search failed before the second batch was sent", failuresWhenSent, contains(0, 0));
            assertThat(failures, hasSize(1));
            assertThat(failures.getFirst().getCause(), sameInstance(trip.get()));
        }
    }

    /**
     * A batch through a gateway node charges nothing while its response arrives, for the reason
     * {@link SearchTransportService#unlessProxied} gives. {@link #testBatchedResponseTripFailsSearch} is the control, over direct
     * connections.
     */
    public void testBatchedResponseThroughGatewayIsNotCharged() throws Exception {
        Transport.Connection gateway = new SearchAsyncActionTests.MockConnection(DiscoveryNodeUtils.create("gateway"));
        Map<String, Transport.Connection> lookup = Map.of(
            "node1",
            TestProxyConnections.newProxyConnection(gateway, DiscoveryNodeUtils.create("node1"))
        );
        // Two shards, so that the node gets a batch
        OriginalIndices originalIndices = new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS);
        List<SearchShardIterator> shardsIter = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            shardsIter.add(
                new SearchShardIterator(
                    null,
                    new ShardId("idx", "_na_", i),
                    List.of("node1"),
                    originalIndices,
                    null,
                    null,
                    false,
                    false,
                    SplitShardCountSummary.UNSET
                )
            );
        }

        AtomicReference<TransportResponseHandler<?>> batch = new AtomicReference<>();
        var transportService = mock(TransportService.class);
        when(transportService.getLocalNode()).thenReturn(DiscoveryNodeUtils.create("coordinator"));
        doAnswer(invocation -> {
            batch.set(invocation.getArgument(4));
            return null;
        }).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                eq(SearchQueryThenFetchAsyncAction.NODE_SEARCH_ACTION_NAME),
                any(),
                any(),
                any()
            );
        SearchTransportService searchTransportService = new SearchTransportService(transportService, null, null);

        List<Exception> failures = Collections.synchronizedList(new ArrayList<>());
        try (QueryPhaseResultConsumer resultConsumer = newResultConsumer(shardsIter.size())) {
            newSearchThatMustNotFinish(
                searchTransportService,
                lookup,
                shardsIter,
                resultConsumer,
                true,
                SearchRequest.DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS,
                failures
            ).start();
            assertNotNull(batch.get());

            // Over the limit of the search's breaker, so a reservation would trip
            assertThat(batch.get().reserveResponseBytes(1000), sameInstance(TransportResponseHandler.NO_RESERVATION));
            assertThat(failures, empty());
        }
    }

    private static QueryPhaseResultConsumer newResultConsumer(int numShards) {
        return new QueryPhaseResultConsumer(
            new SearchRequest(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            NoopCircuitBreaker.INSTANCE,
            new SearchPhaseController((t, r) -> InternalAggregationTestCase.emptyReduceContextBuilder()),
            () -> false,
            SearchProgressListener.NOOP,
            numShards,
            exc -> {}
        );
    }

    /**
     * Builds a search that charges shard responses to a breaker of {@link ShardResponseReservationTests#TRIPPING_LIMIT} and collects
     * its failures. Its query phase must not finish.
     */
    private SearchQueryThenFetchAsyncAction newSearchThatMustNotFinish(
        SearchTransportService searchTransportService,
        Map<String, Transport.Connection> lookup,
        List<SearchShardIterator> shardsIter,
        QueryPhaseResultConsumer resultConsumer,
        boolean batched,
        int maxConcurrentShardRequests,
        List<Exception> failures
    ) {
        SearchRequest request = new SearchRequest().allowPartialSearchResults(true);
        request.setMaxConcurrentShardRequests(maxConcurrentShardRequests);
        return new SearchQueryThenFetchAsyncAction(
            logger,
            null,
            searchTransportService,
            new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofBytes(Long.MAX_VALUE)),
            (clusterAlias, node) -> lookup.get(node),
            Collections.singletonMap("_na_", AliasFilter.EMPTY),
            Collections.emptyMap(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            resultConsumer,
            newLimitedBreaker(ShardResponseReservationTests.TRIPPING_LIMIT),
            request,
            ActionListener.wrap(r -> fail("the search must fail"), failures::add),
            shardsIter,
            Collections.emptyMap(),
            new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(), System::nanoTime),
            new ClusterState.Builder(new ClusterName("test")).build(),
            new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap()),
            SearchResponse.Clusters.EMPTY,
            null,
            batched,
            false,
            new SearchResponseMetrics(TelemetryProvider.NOOP.getMeterRegistry()),
            Map.of()
        ) {
            @Override
            protected SearchPhase getNextPhase() {
                throw new AssertionError("the query phase must not finish after a trip");
            }
        };
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
                ShardResponseReservation reservation,
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
                NoopCircuitBreaker.INSTANCE,
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
                NoopCircuitBreaker.INSTANCE,
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
     * Like {@link #testBadFormatting}, but for the batched query handler on the data node. A bottom sort value that cannot be
     * formatted must not drop the shard result, since that leaves its reader context open until the keep-alive expires.
     */
    public void testBatchedBadFormattingKeepsShardResults() throws Exception {
        int numShards = 2;
        List<ShardSearchContextId> openedContexts = Collections.synchronizedList(new ArrayList<>());
        List<ShardSearchContextId> freedContexts = Collections.synchronizedList(new ArrayList<>());
        RecordingChannel channel = runBatchedNodeQuery(
            TransportVersion.current(),
            newLimitedBreakerService(ByteSizeValue.ofMb(10)),
            numShards,
            false,
            (shardIds, weight) -> (nanos, cpuNanos, finishNanos) -> {},
            new SearchSourceBuilder().size(1).sort(SortBuilders.fieldSort("timestamp")),
            result -> {
                openedContexts.add(result.getContextId());
                result.topDocs(
                    new TopDocsAndMaxScore(
                        new TopFieldDocs(
                            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                            new FieldDoc[] { new FieldDoc(0, Float.NaN, new Object[] { Long.MAX_VALUE }) },
                            new SortField[] { new SortField("timestamp", SortField.Type.LONG) }
                        ),
                        Float.NaN
                    ),
                    new DocValueFormat[] { new BadRawDocValueFormat() }
                );
                result.from(0);
                result.size(1);
            },
            freedContexts::add
        );
        try {
            assertNull("expected success response but got: " + channel.failure, channel.failure);
            assertThat(openedContexts, hasSize(numShards));
            // Both bottoms fail to format and are skipped. With size=1 only one shard is in the top hits, so the node asks to free
            // the other shard's context and keeps the top one for the fetch phase.
            assertThat(freedContexts, hasSize(numShards - 1));
            assertTrue(openedContexts.containsAll(freedContexts));
        } finally {
            if (channel.response != null) {
                channel.response.decRef();
            }
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
        RecordingChannel channel = runBatchedNodeQuery(channelVersion, newLimitedBreakerService(ByteSizeValue.ofMb(10)), 2, true);
        assertNotNull(channel.failure);
    }

    /**
     * The batched query response holds the reduced results of every shard on the node, so its bytes stay charged to the request
     * breaker until the transport has written them.
     */
    public void testBatchedNodeResponseChargedUntilSent() throws Exception {
        assertResponseChargedUntilSent(TransportVersion.current());
        assertResponseChargedUntilSent(TransportVersion.fromName("batched_query_phase_version"));
    }

    private void assertResponseChargedUntilSent(TransportVersion channelVersion) throws Exception {
        CircuitBreakerService breakerService = newLimitedBreakerService(ByteSizeValue.ofMb(10));
        CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
        RecordingChannel channel = runBatchedNodeQuery(channelVersion, breakerService, 2, false);
        assertNull(channel.failure);
        BytesTransportResponse response = (BytesTransportResponse) channel.response;
        try {
            // The consumer has released its own charge, so what stays charged is the response, in whole pages.
            assertThat(breaker.getUsed(), greaterThanOrEqualTo((long) response.bytes().length()));
            assertThat(breaker.getUsed(), greaterThan(0L));
        } finally {
            assertTrue(response.decRef());
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * A breaker trip on the first page of the response fails the node's request with the trip, and leaves nothing charged.
     */
    public void testBatchedNodeResponseTripReachesCoordinator() throws Exception {
        // Smaller than the first page.
        assertResponseTripReachesCoordinator(TransportVersion.current(), 2, 1);
        assertResponseTripReachesCoordinator(TransportVersion.fromName("batched_query_phase_version"), 2, 1);
    }

    /**
     * A breaker trip while the response grows releases the pages already charged.
     */
    public void testBatchedNodeResponseTripWhileWritingReleasesPages() throws Exception {
        // An empty shard result takes under 100 bytes, so 1,000 of them span several pages. The limit leaves room for the first page only.
        int limit = PageCacheRecycler.BYTE_PAGE_SIZE * 3 / 2;
        assertResponseTripReachesCoordinator(TransportVersion.current(), 1_000, limit);
        assertResponseTripReachesCoordinator(TransportVersion.fromName("batched_query_phase_version"), 1_000, limit);
    }

    private void assertResponseTripReachesCoordinator(TransportVersion channelVersion, int shards, long limitBytes) throws Exception {
        CircuitBreakerService breakerService = newLimitedBreakerService(ByteSizeValue.ofBytes(limitBytes));
        CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
        RecordingChannel channel = runBatchedNodeQuery(channelVersion, breakerService, shards, false);
        assertThat(channel.failure, instanceOf(CircuitBreakingException.class));
        assertThat(breaker.getTrippedCount(), equalTo(1L));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * Without enough results to fork a merge, the node merges them when it builds its response, and charges it to its shards
     * by the docs each matched, here its shard number.
     */
    public void testBatchedNodeResponseChargesMergeToShards() throws Exception {
        int shards = between(2, 5);
        AtomicLong merged = new AtomicLong();
        Map<ShardId, Long> weights = new ConcurrentHashMap<>();
        RecordingChannel channel = runBatchedNodeQuery(
            TransportVersion.current(),
            newLimitedBreakerService(ByteSizeValue.ofMb(10)),
            shards,
            false,
            (shardIds, weight) -> (nanos, cpuNanos, finishNanos) -> {
                merged.addAndGet(nanos);
                shardIds.forEach(shardId -> weights.put(shardId, weight.applyAsLong(shardId)));
            }
        );
        assertNull(channel.failure);
        assertTrue(channel.response.decRef());
        assertThat(merged.get(), greaterThan(0L));
        for (int i = 0; i < shards; i++) {
            assertThat(weights.get(new ShardId("idx", "uuid", i)), equalTo((long) i));
        }
    }

    private RecordingChannel runBatchedNodeQuery(
        TransportVersion channelVersion,
        CircuitBreakerService breakerService,
        int shards,
        boolean failWrites
    ) throws Exception {
        return runBatchedNodeQuery(
            channelVersion,
            breakerService,
            shards,
            failWrites,
            (shardIds, weight) -> (nanos, cpuNanos, finishNanos) -> {}
        );
    }

    /**
     * Runs a request for {@code shards} shards through the batched query handler of a node whose shards return no hits, each
     * matching as many docs as its shard number, and returns the channel once the handler has responded on it.
     *
     * @param failWrites whether the response stream throws on its first write
     * @param searchLoad builds the target the node charges merging the results of its shards to, from the shards and their weights
     */
    private RecordingChannel runBatchedNodeQuery(
        TransportVersion channelVersion,
        CircuitBreakerService breakerService,
        int shards,
        boolean failWrites,
        BiFunction<List<ShardId>, ToLongFunction<ShardId>, TaskLoadAttribution.Target> searchLoad
    ) throws Exception {
        return runBatchedNodeQuery(
            channelVersion,
            breakerService,
            shards,
            failWrites,
            searchLoad,
            new SearchSourceBuilder().size(0),
            result -> {
                result.topDocs(
                    new TopDocsAndMaxScore(
                        new TopDocs(
                            new TotalHits(result.getSearchShardTarget().getShardId().id(), TotalHits.Relation.EQUAL_TO),
                            new ScoreDoc[0]
                        ),
                        Float.NaN
                    ),
                    new DocValueFormat[0]
                );
                result.from(0);
                result.size(0);
            },
            contextId -> {}
        );
    }

    /**
     * Runs a request for {@code shards} shards through the batched query handler of a node, and returns the channel once the
     * handler has responded on it. The caller must release the response.
     *
     * @param failWrites whether the response stream throws on its first write
     * @param searchLoad builds the target the node charges merging the results of its shards to, from the shards and their weights
     * @param source the search source of the request
     * @param shardResult fills in the query result each shard returns
     * @param onFreeContext called with the id of each reader context the node asks to free
     */
    private RecordingChannel runBatchedNodeQuery(
        TransportVersion channelVersion,
        CircuitBreakerService breakerService,
        int shards,
        boolean failWrites,
        BiFunction<List<ShardId>, ToLongFunction<ShardId>, TaskLoadAttribution.Target> searchLoad,
        SearchSourceBuilder source,
        Consumer<QuerySearchResult> shardResult,
        Consumer<ShardSearchContextId> onFreeContext
    ) throws Exception {
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
            @Override
            public RecyclerBytesStreamOutput newNetworkBytesStream(CircuitBreaker circuitBreaker) {
                if (failWrites == false) {
                    return super.newNetworkBytesStream(circuitBreaker);
                }
                // This stream will throw on first write
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
            breakerService,
            EmptySystemIndices.INSTANCE.getExecutorSelector(),
            OnlinePrewarmingService.NOOP,
            transport.getTaskManager()
        ) {
            @Override
            public TaskLoadAttribution.Target searchLoad(List<ShardId> shardIds, ToLongFunction<ShardId> weight) {
                return searchLoad.apply(shardIds, weight);
            }

            @Override
            public void executeQueryPhase(ShardSearchRequest req, CancellableTask task, ActionListener<SearchPhaseResult> listener) {
                QuerySearchResult result = new QuerySearchResult(
                    new ShardSearchContextId(UUIDs.randomBase64UUID(), 1),
                    new SearchShardTarget(transport.getLocalNode().getId(), req.shardId(), null),
                    null
                );
                shardResult.accept(result);
                resultsToRelease.add(result);
                listener.onResponse(result);
            }

            @Override
            public boolean freeReaderContext(ShardSearchContextId contextId) {
                onFreeContext.accept(contextId);
                return super.freeReaderContext(contextId);
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
            searchRequest.source(source);
            // The coordinator always sets it before sending. Unset, building each shard request fails.
            searchRequest.allowPartialSearchResults(true);
            var nodeQueryRequest = new SearchQueryThenFetchAsyncAction.NodeQueryRequest(searchRequest, shards, 0L, null);
            for (int i = 0; i < shards; i++) {
                nodeQueryRequest.shards.add(
                    new SearchQueryThenFetchAsyncAction.ShardToQuery(
                        1.0f,
                        new String[] { "idx" },
                        i,
                        new ShardId("idx", "uuid", i),
                        null,
                        SplitShardCountSummary.UNSET
                    )
                );
            }

            RecordingChannel channel = new RecordingChannel(channelVersion);

            @SuppressWarnings("unchecked")
            RequestHandlerRegistry<SearchQueryThenFetchAsyncAction.NodeQueryRequest> handler = (RequestHandlerRegistry<
                SearchQueryThenFetchAsyncAction.NodeQueryRequest>) transport.getRequestHandler(
                    SearchQueryThenFetchAsyncAction.NODE_SEARCH_ACTION_NAME
                );
            handler.processMessageReceived(nodeQueryRequest, channel);

            boolean completed = channel.responded.await(10, TimeUnit.SECONDS);
            resultsToRelease.forEach(QuerySearchResult::decRef);
            assertTrue("channel was not responded to within 10 seconds", completed);
            return channel;
        } finally {
            IOUtils.closeWhileHandlingException(searchService, clusterService, transport, threadPool);
        }
    }

    /**
     * Records what the handler sends. It keeps a reference to the response, as the transport does until its write completes, so the
     * caller must release it.
     */
    private static final class RecordingChannel implements TransportChannel {
        private final TransportVersion version;
        private final CountDownLatch responded = new CountDownLatch(1);
        private volatile TransportResponse response;
        private volatile Exception failure;

        RecordingChannel(TransportVersion version) {
            this.version = version;
        }

        @Override
        public String getProfileName() {
            return "";
        }

        @Override
        public TransportVersion getVersion() {
            return version;
        }

        @Override
        public void sendResponse(TransportResponse response) {
            response.mustIncRef();
            this.response = response;
            responded.countDown();
        }

        @Override
        public void sendResponse(Exception exception) {
            this.failure = exception;
            responded.countDown();
        }
    }

    /**
     * Reproduces https://github.com/elastic/elasticsearch/issues/158991.
     * When a coordinator running a newer transport version encodes a PIT and a data node running an older version
     * processes the batched query phase, the decoding error must be caught and handled.
     */
    public void testBatchedQueryPhaseSucceedsWhenPITEncodedByNewerVersion() throws Exception {
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
        );
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
            OnlinePrewarmingService.NOOP,
            transport.getTaskManager()
        ) {
            @Override
            public TaskLoadAttribution.Target searchLoad(List<ShardId> shardIds, ToLongFunction<ShardId> weight) {
                return (nanos, cpuNanos, finishNanos) -> {};
            }

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
                result.size(1);
                resultsToRelease.add(result);
                listener.onResponse(result);
            }
        };
        // Encode a PIT using a transport version unknown to this node, simulating a newer coordinator.
        // SearchContextId.decode throws IllegalArgumentException for unknown versions.
        TransportVersion futureVersion = TransportVersionUtils.getNextVersion(TransportVersion.current(), true);
        assertFalse("futureVersion must be unknown to this node", futureVersion.isKnown());
        BytesReference encodedPit = SearchContextId.encode(Collections.emptyMap(), Collections.emptyMap(), futureVersion, List.of());
        expectThrows(IllegalArgumentException.class, () -> SearchContextId.decode(new NamedWriteableRegistry(List.of()), encodedPit));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> channelFailure = new AtomicReference<>();
        try {
            transport.start();
            SearchQueryThenFetchAsyncAction.registerNodeSearchAction(
                new SearchTransportService(transport, null, null),
                searchService,
                new SearchPhaseController((t, r) -> InternalAggregationTestCase.emptyReduceContextBuilder()),
                new NamedWriteableRegistry(List.of())
            );
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.allowPartialSearchResults(false);
            searchRequest.source(new SearchSourceBuilder().size(1).pointInTimeBuilder(new PointInTimeBuilder(encodedPit)));
            // Two shards: with size=1 and no matching docs, both shards are outside the top results,
            // which triggers maybeFreeContext -> isPartOfPIT for each
            var nodeQueryRequest = new SearchQueryThenFetchAsyncAction.NodeQueryRequest(searchRequest, 2, 0L, null);
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
            TransportChannel channel = new TransportChannel() {
                @Override
                public String getProfileName() {
                    return "";
                }

                @Override
                public TransportVersion getVersion() {
                    return TransportVersion.current();
                }

                @Override
                public void sendResponse(TransportResponse response) {
                    latch.countDown();
                }

                @Override
                public void sendResponse(Exception exception) {
                    channelFailure.set(exception);
                    latch.countDown();
                }
            };
            @SuppressWarnings("unchecked")
            RequestHandlerRegistry<SearchQueryThenFetchAsyncAction.NodeQueryRequest> handler = (RequestHandlerRegistry<
                SearchQueryThenFetchAsyncAction.NodeQueryRequest>) transport.getRequestHandler(
                    SearchQueryThenFetchAsyncAction.NODE_SEARCH_ACTION_NAME
                );
            handler.processMessageReceived(nodeQueryRequest, channel);
            assertTrue("channel was not responded to within 10 seconds", latch.await(10, TimeUnit.SECONDS));
            resultsToRelease.forEach(QuerySearchResult::decRef);
            assertNull("expected success response but got: " + channelFailure.get(), channelFailure.get());
        } finally {
            IOUtils.closeWhileHandlingException(searchService, clusterService, transport, threadPool);
        }
    }

}
