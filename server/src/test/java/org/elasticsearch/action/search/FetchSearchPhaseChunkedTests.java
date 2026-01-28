/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.fetch.chunk.TransportFetchPhaseCoordinationAction;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportStats;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import static org.elasticsearch.action.search.FetchSearchPhaseTests.addProfiling;
import static org.elasticsearch.action.search.FetchSearchPhaseTests.fetchProfile;
import static org.elasticsearch.action.search.FetchSearchPhaseTests.searchPhaseFactory;

public class FetchSearchPhaseChunkedTests extends ESTestCase {

    /**
     * Test that chunked fetch is used when all conditions are met:
     * - fetchPhaseChunked is true
     * - data node supports CHUNKED_FETCH_PHASE
     * - not a CCS query (no cluster alias)
     * - not a scroll or reindex query
     */
    /* public void testChunkedFetchUsedWhenConditionsMet() throws Exception {
        // Install 2 shards to avoid single-shard query-and-fetch optimization
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        ThreadPool threadPool = new TestThreadPool("test");
        try {
            TransportService mockTransportService = createMockTransportService(threadPool);

            try (SearchPhaseResults<SearchPhaseResult> results = createSearchPhaseResults(mockSearchPhaseContext)) {
                boolean profiled = randomBoolean();

                // Add first shard result
                final ShardSearchContextId ctx1 = new ShardSearchContextId(UUIDs.base64UUID(), 123);
                SearchShardTarget shardTarget1 = new SearchShardTarget("node1", new ShardId("test", "na", 0), null);
                addQuerySearchResult(ctx1, shardTarget1, profiled, 0, results);

                // Add second shard result
                final ShardSearchContextId ctx2 = new ShardSearchContextId(UUIDs.base64UUID(), 124);
                SearchShardTarget shardTarget2 = new SearchShardTarget("node2", new ShardId("test", "na", 1), null);
                addQuerySearchResult(ctx2, shardTarget2, profiled, 1, results);

                AtomicBoolean chunkedFetchUsed = new AtomicBoolean(false);
                AtomicBoolean traditionalFetchUsed = new AtomicBoolean(false);

                // Create the coordination action that will be called for chunked fetch
                TransportFetchPhaseCoordinationAction fetchCoordinationAction = new TransportFetchPhaseCoordinationAction(
                    mockTransportService,
                    new ActionFilters(Collections.emptySet()),
                    new ActiveFetchPhaseTasks(),
                    null
                ) {
                    @Override
                    public void doExecute(Task task, Request request, ActionListener<Response> listener) {
                        chunkedFetchUsed.set(true);
                        FetchSearchResult fetchResult = new FetchSearchResult();
                        try {
                            // Return result based on context ID
                            SearchShardTarget target = request.getShardFetchRequest().contextId().equals(ctx1)
                                ? shardTarget1
                                : shardTarget2;
                            int docId = request.getShardFetchRequest().contextId().equals(ctx1) ? 42 : 43;

                            fetchResult.setSearchShardTarget(target);
                            SearchHits hits = SearchHits.unpooled(
                                new SearchHit[] { SearchHit.unpooled(docId) },
                                new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                                1.0F
                            );
                            fetchResult.shardResult(hits, fetchProfile(profiled));
                            listener.onResponse(new Response(fetchResult));
                        } finally {
                            fetchResult.decRef();
                        }
                    }
                };
                provideSearchTransportWithChunkedFetch(mockSearchPhaseContext, mockTransportService, threadPool, fetchCoordinationAction);

                SearchPhaseController.ReducedQueryPhase reducedQueryPhase = results.reduce();
                FetchSearchPhase phase = new FetchSearchPhase(results, null, mockSearchPhaseContext, reducedQueryPhase) {
                    @Override
                    protected SearchPhase nextPhase(
                        SearchResponseSections searchResponseSections,
                        AtomicArray<SearchPhaseResult> queryPhaseResults
                    ) {
                        return searchPhaseFactory(mockSearchPhaseContext).apply(searchResponseSections, queryPhaseResults);
                    }
                };

                phase.run();
                mockSearchPhaseContext.assertNoFailure();

                assertTrue("Chunked fetch should be used", chunkedFetchUsed.get());
                assertFalse("Traditional fetch should not be used", traditionalFetchUsed.get());

                SearchResponse searchResponse = mockSearchPhaseContext.searchResponse.get();
                assertNotNull(searchResponse);
                assertEquals(2, searchResponse.getHits().getTotalHits().value());
                // Results are sorted by score, so higher score (43) comes first
                assertTrue(searchResponse.getHits().getAt(0).docId() == 42 || searchResponse.getHits().getAt(0).docId() == 43);
            } finally {
                mockSearchPhaseContext.results.close();
                var resp = mockSearchPhaseContext.searchResponse.get();
                if (resp != null) {
                    resp.decRef();
                }
            }
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeValue.timeValueSeconds(5).timeUnit());
        }
    }*/

    /**
     * Test that traditional fetch is used when fetchPhaseChunked is disabled
     */
    public void testTraditionalFetchUsedWhenChunkedDisabled() throws Exception {
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        ThreadPool threadPool = new TestThreadPool("test");
        try {
            TransportService mockTransportService = createMockTransportService(threadPool);

            try (SearchPhaseResults<SearchPhaseResult> results = createSearchPhaseResults(mockSearchPhaseContext)) {
                boolean profiled = randomBoolean();

                // Add first shard result
                final ShardSearchContextId ctx1 = new ShardSearchContextId(UUIDs.base64UUID(), 123);
                SearchShardTarget shardTarget1 = new SearchShardTarget("node1", new ShardId("test", "na", 0), null);
                addQuerySearchResult(ctx1, shardTarget1, profiled, 0, results);

                // Add second shard result
                final ShardSearchContextId ctx2 = new ShardSearchContextId(UUIDs.base64UUID(), 124);
                SearchShardTarget shardTarget2 = new SearchShardTarget("node2", new ShardId("test", "na", 1), null);
                addQuerySearchResult(ctx2, shardTarget2, profiled, 1, results);

                AtomicBoolean traditionalFetchUsed = new AtomicBoolean(false);

                provideSearchTransport(
                    mockSearchPhaseContext,
                    mockTransportService,
                    traditionalFetchUsed,
                    ctx1,
                    shardTarget1,
                    shardTarget2,
                    profiled
                );

                SearchPhaseController.ReducedQueryPhase reducedQueryPhase = results.reduce();
                FetchSearchPhase phase = new FetchSearchPhase(results, null, mockSearchPhaseContext, reducedQueryPhase) {
                    @Override
                    protected SearchPhase nextPhase(
                        SearchResponseSections searchResponseSections,
                        AtomicArray<SearchPhaseResult> queryPhaseResults
                    ) {
                        return searchPhaseFactory(mockSearchPhaseContext).apply(searchResponseSections, queryPhaseResults);
                    }
                };

                phase.run();
                mockSearchPhaseContext.assertNoFailure();

                assertTrue("Traditional fetch should be used when chunked fetch is disabled", traditionalFetchUsed.get());

                SearchResponse searchResponse = mockSearchPhaseContext.searchResponse.get();
                assertNotNull(searchResponse);
                assertEquals(2, searchResponse.getHits().getTotalHits().value());
            } finally {
                mockSearchPhaseContext.results.close();
                var resp = mockSearchPhaseContext.searchResponse.get();
                if (resp != null) {
                    resp.decRef();
                }
            }
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeValue.timeValueSeconds(5).timeUnit());
        }
    }

    /**
     * Test that traditional fetch is used for scroll queries
     */
    public void testTraditionalFetchUsedForScrollQuery() throws Exception {
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        mockSearchPhaseContext.getRequest().scroll(TimeValue.timeValueMinutes(1));

        ThreadPool threadPool = new TestThreadPool("test");
        try {
            TransportService mockTransportService = createMockTransportService(threadPool);

            try (SearchPhaseResults<SearchPhaseResult> results = createSearchPhaseResults(mockSearchPhaseContext)) {
                boolean profiled = randomBoolean();

                // Add first shard result
                final ShardSearchContextId ctx1 = new ShardSearchContextId(UUIDs.base64UUID(), 123);
                SearchShardTarget shardTarget1 = new SearchShardTarget("node1", new ShardId("test", "na", 0), null);
                addQuerySearchResult(ctx1, shardTarget1, profiled, 0, results);

                // Add second shard result
                final ShardSearchContextId ctx2 = new ShardSearchContextId(UUIDs.base64UUID(), 124);
                SearchShardTarget shardTarget2 = new SearchShardTarget("node2", new ShardId("test", "na", 1), null);
                addQuerySearchResult(ctx2, shardTarget2, profiled, 1, results);

                AtomicBoolean traditionalFetchUsed = new AtomicBoolean(false);

                provideSearchTransport(
                    mockSearchPhaseContext,
                    mockTransportService,
                    traditionalFetchUsed,
                    ctx1,
                    shardTarget1,
                    shardTarget2,
                    profiled
                );

                SearchPhaseController.ReducedQueryPhase reducedQueryPhase = results.reduce();

                // Store query results in an AtomicArray for scroll ID generation
                AtomicArray<SearchPhaseResult> queryResults = new AtomicArray<>(2);
                queryResults.set(0, results.getAtomicArray().get(0));
                queryResults.set(1, results.getAtomicArray().get(1));

                FetchSearchPhase phase = new FetchSearchPhase(results, null, mockSearchPhaseContext, reducedQueryPhase) {
                    @Override
                    protected SearchPhase nextPhase(
                        SearchResponseSections searchResponseSections,
                        AtomicArray<SearchPhaseResult> fetchResults
                    ) {
                        // Pass the query results for scroll ID generation
                        return searchPhaseFactoryBi(mockSearchPhaseContext, queryResults).apply(searchResponseSections, fetchResults);
                    }
                };

                phase.run();
                mockSearchPhaseContext.assertNoFailure();

                assertTrue("Traditional fetch should be used for scroll queries", traditionalFetchUsed.get());

                SearchResponse searchResponse = mockSearchPhaseContext.searchResponse.get();
                assertNotNull(searchResponse);
                assertEquals(2, searchResponse.getHits().getTotalHits().value());
                assertNotNull("Scroll ID should be present for scroll queries", searchResponse.getScrollId());
            } finally {
                mockSearchPhaseContext.results.close();
                var resp = mockSearchPhaseContext.searchResponse.get();
                if (resp != null) {
                    resp.decRef();
                }
            }
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeValue.timeValueSeconds(5).timeUnit());
        }
    }

    private static BiFunction<SearchResponseSections, AtomicArray<SearchPhaseResult>, SearchPhase> searchPhaseFactoryBi(
        MockSearchPhaseContext mockSearchPhaseContext,
        AtomicArray<SearchPhaseResult> queryResults
    ) {
        return (searchResponseSections, fetchResults) -> new SearchPhase("test") {
            @Override
            public void run() {
                mockSearchPhaseContext.sendSearchResponse(searchResponseSections, queryResults);
            }
        };
    }

    /**
     * Test that traditional fetch is used for CCS queries
     */
    public void testTraditionalFetchUsedForCCSQuery() throws Exception {
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        ThreadPool threadPool = new TestThreadPool("test");
        try {
            TransportService mockTransportService = createMockTransportService(threadPool);

            try (SearchPhaseResults<SearchPhaseResult> results = createSearchPhaseResults(mockSearchPhaseContext)) {
                boolean profiled = randomBoolean();

                // Add first shard result - CCS query with cluster alias
                final ShardSearchContextId ctx1 = new ShardSearchContextId(UUIDs.base64UUID(), 123);
                SearchShardTarget shardTarget1 = new SearchShardTarget("node1", new ShardId("test", "na", 0), "remote_cluster");
                addQuerySearchResult(ctx1, shardTarget1, profiled, 0, results);

                // Add second shard result - CCS query with cluster alias
                final ShardSearchContextId ctx2 = new ShardSearchContextId(UUIDs.base64UUID(), 124);
                SearchShardTarget shardTarget2 = new SearchShardTarget("node2", new ShardId("test", "na", 1), "remote_cluster");
                addQuerySearchResult(ctx2, shardTarget2, profiled, 1, results);

                AtomicBoolean traditionalFetchUsed = new AtomicBoolean(false);

                provideSearchTransport(
                    mockSearchPhaseContext,
                    mockTransportService,
                    traditionalFetchUsed,
                    ctx1,
                    shardTarget1,
                    shardTarget2,
                    profiled
                );

                SearchPhaseController.ReducedQueryPhase reducedQueryPhase = results.reduce();
                FetchSearchPhase phase = new FetchSearchPhase(results, null, mockSearchPhaseContext, reducedQueryPhase) {
                    @Override
                    protected SearchPhase nextPhase(
                        SearchResponseSections searchResponseSections,
                        AtomicArray<SearchPhaseResult> queryPhaseResults
                    ) {
                        return searchPhaseFactory(mockSearchPhaseContext).apply(searchResponseSections, queryPhaseResults);
                    }
                };

                phase.run();
                mockSearchPhaseContext.assertNoFailure();

                assertTrue("Traditional fetch should be used for CCS queries", traditionalFetchUsed.get());

                SearchResponse searchResponse = mockSearchPhaseContext.searchResponse.get();
                assertNotNull(searchResponse);
                assertEquals(2, searchResponse.getHits().getTotalHits().value());
            } finally {
                mockSearchPhaseContext.results.close();
                var resp = mockSearchPhaseContext.searchResponse.get();
                if (resp != null) {
                    resp.decRef();
                }
            }
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeValue.timeValueSeconds(5).timeUnit());
        }
    }

    private SearchPhaseResults<SearchPhaseResult> createSearchPhaseResults(MockSearchPhaseContext mockSearchPhaseContext) {
        SearchPhaseController controller = new SearchPhaseController((t, s) -> InternalAggregationTestCase.emptyReduceContextBuilder());

        return controller.newSearchPhaseResults(
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            () -> false,
            SearchProgressListener.NOOP,
            mockSearchPhaseContext.getRequest(),
            2,
            exc -> {}
        );
    }

    private void provideSearchTransportWithChunkedFetch(
        MockSearchPhaseContext mockSearchPhaseContext,
        TransportService transportService,
        ThreadPool threadPool,
        TransportFetchPhaseCoordinationAction fetchCoordinationAction
    ) {
        ClusterService clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool,
            null
        );

        Transport.Connection mockConnection = new Transport.Connection() {
            @Override
            public void incRef() {}

            @Override
            public boolean tryIncRef() {
                return false;
            }

            @Override
            public boolean decRef() {
                return false;
            }

            @Override
            public boolean hasReferences() {
                return false;
            }

            @Override
            public DiscoveryNode getNode() {
                return transportService.getLocalNode();
            }

            @Override
            public TransportVersion getTransportVersion() {
                return TransportVersion.current();
            }

            @Override
            public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) {
                throw new UnsupportedOperationException("mock connection");
            }

            @Override
            public void addCloseListener(ActionListener<Void> listener) {}

            @Override
            public boolean isClosed() {
                return false;
            }

            @Override
            public void close() {}

            @Override
            public void onRemoved() {}

            @Override
            public void addRemovedListener(ActionListener<Void> listener) {}
        };

        NodeClient nodeClient = new NodeClient(Settings.EMPTY, null, null);
        Map<ActionType<?>, TransportAction<?, ?>> actions = new java.util.HashMap<>();
        actions.put(TransportFetchPhaseCoordinationAction.TYPE, fetchCoordinationAction);
        nodeClient.initialize(actions, transportService.getTaskManager(), () -> "local", mockConnection, null);

        SearchTransportService searchTransport = new SearchTransportService(transportService, nodeClient, null);
        searchTransport.setSearchService(new StubSearchService(true, clusterService, threadPool));

        mockSearchPhaseContext.searchTransport = searchTransport;
        mockSearchPhaseContext.addReleasable(() -> {
            clusterService.close();
            ThreadPool.terminate(threadPool, 10, java.util.concurrent.TimeUnit.SECONDS);
        });
    }

    /**
     * Minimal stub SearchService that only implements fetchPhaseChunked()
     */
    private static class StubSearchService extends SearchService {
        private final boolean chunkedEnabled;

        StubSearchService(boolean chunkedEnabled, ClusterService clusterService, ThreadPool threadPool) {
            super(
                clusterService,
                null, // indicesService
                threadPool,
                null, // scriptService
                null, // bigArrays
                new FetchPhase(Collections.emptyList()),
                new NoneCircuitBreakerService(),
                EmptySystemIndices.INSTANCE.getExecutorSelector(),
                Tracer.NOOP,
                OnlinePrewarmingService.NOOP
            );
            this.chunkedEnabled = chunkedEnabled;
        }

        @Override
        public boolean fetchPhaseChunked() {
            return chunkedEnabled;
        }

        @Override
        protected void doStart() {}

        @Override
        protected void doStop() {}

        @Override
        protected void doClose() {}
    }

    private void provideSearchTransport(
        MockSearchPhaseContext mockSearchPhaseContext,
        TransportService mockTransportService,
        AtomicBoolean traditionalFetchUsed,
        ShardSearchContextId ctx1,
        SearchShardTarget shardTarget1,
        SearchShardTarget shardTarget2,
        boolean profiled
    ) {
        mockSearchPhaseContext.searchTransport = new SearchTransportService(mockTransportService, null, null) {
            @Override
            public void sendExecuteFetch(
                Transport.Connection connection,
                ShardFetchSearchRequest request,
                AbstractSearchAsyncAction<?> context,
                SearchShardTarget shardTarget,
                ActionListener<FetchSearchResult> listener
            ) {
                traditionalFetchUsed.set(true);
                FetchSearchResult fetchResult = new FetchSearchResult();
                try {
                    SearchShardTarget target = request.contextId().equals(ctx1) ? shardTarget1 : shardTarget2;
                    int docId = request.contextId().equals(ctx1) ? 42 : 43;

                    fetchResult.setSearchShardTarget(target);
                    SearchHits hits = SearchHits.unpooled(
                        new SearchHit[] { SearchHit.unpooled(docId) },
                        new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                        1.0F
                    );
                    fetchResult.shardResult(hits, fetchProfile(profiled));
                    listener.onResponse(fetchResult);
                } finally {
                    fetchResult.decRef();
                }
            }
        };
    }

    private void addQuerySearchResult(
        ShardSearchContextId ctx,
        SearchShardTarget shardTarget,
        boolean profiled,
        int shardIndex,
        SearchPhaseResults<SearchPhaseResult> results
    ) {
        QuerySearchResult queryResult = new QuerySearchResult(ctx, shardTarget, null);
        try {
            queryResult.topDocs(
                new TopDocsAndMaxScore(
                    new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(42 + shardIndex, 1.0F) }),
                    1.0F
                ),
                new DocValueFormat[0]
            );
            queryResult.size(10);
            queryResult.setShardIndex(shardIndex);
            addProfiling(profiled, queryResult);
            results.consumeResult(queryResult, () -> {});
        } finally {
            queryResult.decRef();
        }
    }

    private TransportService createMockTransportService(ThreadPool threadPool) {
        DiscoveryNode localNode = new DiscoveryNode(
            "local",
            "local",
            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
            Collections.emptyMap(),
            Collections.emptySet(),
            null
        );

        return new TransportService(
            Settings.EMPTY,
            new MockTransport(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> localNode,
            null,
            Collections.emptySet()
        );
    }

    // Simple mock transport implementation
    private static class MockTransport implements Transport {
        @Override
        public Lifecycle.State lifecycleState() {
            return Lifecycle.State.STARTED;
        }

        @Override
        public void addLifecycleListener(LifecycleListener listener) {}

        @Override
        public void start() {}

        @Override
        public void stop() {}

        @Override
        public void close() {}

        @Override
        public BoundTransportAddress boundAddress() {
            return new BoundTransportAddress(
                new TransportAddress[] { new TransportAddress(TransportAddress.META_ADDRESS, 9300) },
                new TransportAddress(TransportAddress.META_ADDRESS, 9300)
            );
        }

        @Override
        public BoundTransportAddress boundRemoteIngressAddress() {
            return null;
        }

        @Override
        public Map<String, BoundTransportAddress> profileBoundAddresses() {
            return Collections.emptyMap();
        }

        @Override
        public TransportAddress[] addressesFromString(String address) {
            return new TransportAddress[0];
        }

        @Override
        public void openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Connection> listener) {
            listener.onFailure(new UnsupportedOperationException("mock transport"));
        }

        @Override
        public TransportStats getStats() {
            return null;
        }

        @Override
        public List<String> getDefaultSeedAddresses() {
            return Collections.emptyList();
        }

        @Override
        public void setMessageListener(TransportMessageListener listener) {}

        @Override
        public ResponseHandlers getResponseHandlers() {
            return new ResponseHandlers();
        }

        @Override
        public RequestHandlers getRequestHandlers() {
            return new RequestHandlers();
        }
    }
}
