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
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.RescoreDocIds;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.fetch.chunk.ActiveFetchPhaseTasks;
import org.elasticsearch.search.fetch.chunk.TransportFetchPhaseCoordinationAction;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.CloseableConnection;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
    public void testChunkedFetchUsedWhenConditionsMet() throws Exception {
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

                // Add first shard result
                final ShardSearchContextId ctx2 = new ShardSearchContextId(UUIDs.base64UUID(), 124);
                SearchShardTarget shardTarget2 = new SearchShardTarget("node2", new ShardId("test", "na", 1), null);
                addQuerySearchResult(ctx2, shardTarget2, profiled, 1, results);

                AtomicBoolean chunkedFetchUsed = new AtomicBoolean(false);

                // Create the coordination action that will be called for chunked fetch
                TransportFetchPhaseCoordinationAction fetchCoordinationAction = new TransportFetchPhaseCoordinationAction(
                    mockTransportService,
                    new ActionFilters(Collections.emptySet()),
                    new ActiveFetchPhaseTasks(),
                    newLimitedBreakerService(ByteSizeValue.ofMb(10)),
                    new NamedWriteableRegistry(Collections.emptyList())
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

                SearchResponse searchResponse = mockSearchPhaseContext.searchResponse.get();
                assertNotNull(searchResponse);
                assertEquals(2, searchResponse.getHits().getTotalHits().value());
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
    }

    public void testChunkedFetchUsedForPointInTimeQuery() throws Exception {
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        mockSearchPhaseContext.getRequest()
            .source(new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(new BytesArray("test-pit-id"))));
        ThreadPool threadPool = new TestThreadPool("test");
        try {
            TransportService mockTransportService = createMockTransportService(threadPool);

            try (SearchPhaseResults<SearchPhaseResult> results = createSearchPhaseResults(mockSearchPhaseContext)) {
                boolean profiled = randomBoolean();

                final ShardSearchContextId ctx1 = new ShardSearchContextId(UUIDs.base64UUID(), 123);
                SearchShardTarget shardTarget1 = new SearchShardTarget("node1", new ShardId("test", "na", 0), null);
                addQuerySearchResult(ctx1, shardTarget1, profiled, 0, results);

                final ShardSearchContextId ctx2 = new ShardSearchContextId(UUIDs.base64UUID(), 124);
                SearchShardTarget shardTarget2 = new SearchShardTarget("node2", new ShardId("test", "na", 1), null);
                addQuerySearchResult(ctx2, shardTarget2, profiled, 1, results);

                AtomicBoolean chunkedFetchUsed = new AtomicBoolean(false);
                TransportFetchPhaseCoordinationAction fetchCoordinationAction = new TransportFetchPhaseCoordinationAction(
                    mockTransportService,
                    new ActionFilters(Collections.emptySet()),
                    new ActiveFetchPhaseTasks(),
                    newLimitedBreakerService(ByteSizeValue.ofMb(10)),
                    new NamedWriteableRegistry(Collections.emptyList())
                ) {
                    @Override
                    public void doExecute(Task task, Request request, ActionListener<Response> listener) {
                        chunkedFetchUsed.set(true);
                        FetchSearchResult fetchResult = new FetchSearchResult();
                        try {
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

                // PIT response generation requires query phase results for building the PIT id.
                AtomicArray<SearchPhaseResult> queryResults = new AtomicArray<>(2);
                queryResults.set(0, results.getAtomicArray().get(0));
                queryResults.set(1, results.getAtomicArray().get(1));

                FetchSearchPhase phase = new FetchSearchPhase(results, null, mockSearchPhaseContext, reducedQueryPhase) {
                    @Override
                    protected SearchPhase nextPhase(
                        SearchResponseSections searchResponseSections,
                        AtomicArray<SearchPhaseResult> fetchResults
                    ) {
                        return searchPhaseFactoryBi(mockSearchPhaseContext, queryResults).apply(searchResponseSections, fetchResults);
                    }
                };

                phase.run();
                mockSearchPhaseContext.assertNoFailure();
                assertTrue("Chunked fetch should be used for PIT queries", chunkedFetchUsed.get());

                SearchResponse searchResponse = mockSearchPhaseContext.searchResponse.get();
                assertNotNull(searchResponse);
                assertNotNull("PIT id should be present in response", searchResponse.pointInTimeId());
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

    public void testChunkedFetchHandlesPartialShardFailure() throws Exception {
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        ThreadPool threadPool = new TestThreadPool("test");
        try {
            TransportService mockTransportService = createMockTransportService(threadPool);

            try (SearchPhaseResults<SearchPhaseResult> results = createSearchPhaseResults(mockSearchPhaseContext)) {
                boolean profiled = randomBoolean();

                final ShardSearchContextId ctx1 = new ShardSearchContextId(UUIDs.base64UUID(), 123);
                SearchShardTarget shardTarget1 = new SearchShardTarget("node1", new ShardId("test", "na", 0), null);
                addQuerySearchResult(ctx1, shardTarget1, profiled, 0, results);

                final ShardSearchContextId ctx2 = new ShardSearchContextId(UUIDs.base64UUID(), 124);
                SearchShardTarget shardTarget2 = new SearchShardTarget("node2", new ShardId("test", "na", 1), null);
                addQuerySearchResult(ctx2, shardTarget2, profiled, 1, results);

                AtomicBoolean chunkedFetchUsed = new AtomicBoolean(false);
                TransportFetchPhaseCoordinationAction fetchCoordinationAction = new TransportFetchPhaseCoordinationAction(
                    mockTransportService,
                    new ActionFilters(Collections.emptySet()),
                    new ActiveFetchPhaseTasks(),
                    newLimitedBreakerService(ByteSizeValue.ofMb(10)),
                    new NamedWriteableRegistry(Collections.emptyList())
                ) {
                    @Override
                    public void doExecute(Task task, Request request, ActionListener<Response> listener) {
                        chunkedFetchUsed.set(true);
                        if (request.getShardFetchRequest().contextId().equals(ctx2)) {
                            listener.onFailure(new RuntimeException("simulated chunked fetch failure"));
                            return;
                        }

                        FetchSearchResult fetchResult = new FetchSearchResult();
                        try {
                            fetchResult.setSearchShardTarget(shardTarget1);
                            SearchHits hits = SearchHits.unpooled(
                                new SearchHit[] { SearchHit.unpooled(42) },
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

                SearchResponse searchResponse = mockSearchPhaseContext.searchResponse.get();
                assertNotNull(searchResponse);
                assertEquals(1, searchResponse.getFailedShards());
                assertEquals(1, searchResponse.getSuccessfulShards());
                assertEquals(1, searchResponse.getShardFailures().length);
                assertEquals("simulated chunked fetch failure", searchResponse.getShardFailures()[0].getCause().getMessage());
                assertEquals(1, searchResponse.getHits().getHits().length);
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

    public void testChunkedFetchTreatsTaskCancellationAsShardFailure() throws Exception {
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        ThreadPool threadPool = new TestThreadPool("test");
        try {
            TransportService mockTransportService = createMockTransportService(threadPool);

            try (SearchPhaseResults<SearchPhaseResult> results = createSearchPhaseResults(mockSearchPhaseContext)) {
                boolean profiled = randomBoolean();

                final ShardSearchContextId ctx1 = new ShardSearchContextId(UUIDs.base64UUID(), 123);
                SearchShardTarget shardTarget1 = new SearchShardTarget("node1", new ShardId("test", "na", 0), null);
                addQuerySearchResult(ctx1, shardTarget1, profiled, 0, results);

                final ShardSearchContextId ctx2 = new ShardSearchContextId(UUIDs.base64UUID(), 124);
                SearchShardTarget shardTarget2 = new SearchShardTarget("node2", new ShardId("test", "na", 1), null);
                addQuerySearchResult(ctx2, shardTarget2, profiled, 1, results);

                TransportFetchPhaseCoordinationAction fetchCoordinationAction = new TransportFetchPhaseCoordinationAction(
                    mockTransportService,
                    new ActionFilters(Collections.emptySet()),
                    new ActiveFetchPhaseTasks(),
                    newLimitedBreakerService(ByteSizeValue.ofMb(10)),
                    new NamedWriteableRegistry(Collections.emptyList())
                ) {
                    @Override
                    public void doExecute(Task task, Request request, ActionListener<Response> listener) {
                        if (request.getShardFetchRequest().contextId().equals(ctx2)) {
                            listener.onFailure(new TaskCancelledException("simulated cancellation"));
                            return;
                        }

                        FetchSearchResult fetchResult = new FetchSearchResult();
                        try {
                            fetchResult.setSearchShardTarget(shardTarget1);
                            SearchHits hits = SearchHits.unpooled(
                                new SearchHit[] { SearchHit.unpooled(42) },
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

                SearchResponse searchResponse = mockSearchPhaseContext.searchResponse.get();
                assertNotNull(searchResponse);
                assertEquals(1, searchResponse.getFailedShards());
                assertEquals(1, searchResponse.getShardFailures().length);
                assertTrue(searchResponse.getShardFailures()[0].getCause() instanceof TaskCancelledException);
                assertEquals("simulated cancellation", searchResponse.getShardFailures()[0].getCause().getMessage());
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

    public void testTraditionalFetchUsedWhenDataNodeDoesNotSupportChunkedTransportVersion() throws Exception {
        ThreadPool threadPool = new TestThreadPool("test");
        ClusterService clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool,
            null
        );
        MockSearchPhaseContext mockSearchPhaseContext = null;

        MockTransportService transportService = MockTransportService.createNewService(
            Settings.EMPTY,
            VersionInformation.CURRENT,
            TransportFetchPhaseCoordinationAction.CHUNKED_FETCH_PHASE,
            threadPool
        );

        try {
            transportService.start();
            transportService.acceptIncomingRequests();

            AtomicBoolean traditionalFetchUsed = new AtomicBoolean(false);
            AtomicBoolean chunkedPathUsed = new AtomicBoolean(false);

            transportService.registerRequestHandler(
                SearchTransportService.FETCH_ID_ACTION_NAME,
                threadPool.executor(ThreadPool.Names.GENERIC),
                ShardFetchSearchRequest::new,
                (req, channel, task) -> {
                    traditionalFetchUsed.set(true);
                    FetchSearchResult result = createFetchSearchResult();
                    channel.sendResponse(result);
                }
            );

            transportService.registerRequestHandler(
                TransportFetchPhaseCoordinationAction.TYPE.name(),
                threadPool.executor(ThreadPool.Names.GENERIC),
                TransportFetchPhaseCoordinationAction.Request::new,
                (req, channel, task) -> {
                    chunkedPathUsed.set(true);
                    channel.sendResponse(new IllegalStateException("chunked coordination path should not be used"));
                }
            );

            SearchTransportService searchTransportService = new SearchTransportService(transportService, null, null);
            searchTransportService.setSearchService(new StubSearchService(true, clusterService, threadPool));

            mockSearchPhaseContext = new MockSearchPhaseContext(1);
            mockSearchPhaseContext.searchTransport = searchTransportService;

            ShardId shardId = new ShardId("test", "na", 0);
            SearchShardTarget shardTarget = new SearchShardTarget("node1", shardId, null);
            ShardFetchSearchRequest shardFetchRequest = createShardFetchSearchRequest(shardId);

            Transport.Connection delegateConnection = transportService.getConnection(transportService.getLocalNode());
            TransportVersion unsupportedVersion = TransportVersion.fromId(
                TransportFetchPhaseCoordinationAction.CHUNKED_FETCH_PHASE.id() - 1
            );
            Transport.Connection oldVersionConnection = withTransportVersion(delegateConnection, unsupportedVersion);

            PlainActionFuture<FetchSearchResult> future = new PlainActionFuture<>();
            searchTransportService.sendExecuteFetch(oldVersionConnection, shardFetchRequest, mockSearchPhaseContext, shardTarget, future);

            FetchSearchResult result = future.actionGet(10, TimeUnit.SECONDS);
            result.decRef();

            assertTrue("Traditional fetch should be used for unsupported data node version", traditionalFetchUsed.get());
            assertFalse("Chunked coordination path should not be used", chunkedPathUsed.get());
        } finally {
            if (mockSearchPhaseContext != null) {
                mockSearchPhaseContext.results.close();
                var resp = mockSearchPhaseContext.searchResponse.get();
                if (resp != null) {
                    resp.decRef();
                }
            }
            transportService.close();
            clusterService.close();
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

        transportService.start();
        transportService.acceptIncomingRequests();

        SearchTransportService searchTransport = new SearchTransportService(transportService, null, null);
        searchTransport.setSearchService(new StubSearchService(true, clusterService, threadPool));

        mockSearchPhaseContext.searchTransport = searchTransport;
        mockSearchPhaseContext.addReleasable(clusterService::close);
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
                newLimitedBreakerService(ByteSizeValue.ofMb(10)),
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

    private ShardFetchSearchRequest createShardFetchSearchRequest(ShardId shardId) {
        ShardSearchContextId contextId = new ShardSearchContextId("test", randomLong());
        OriginalIndices originalIndices = new OriginalIndices(
            new String[] { "test-index" },
            IndicesOptions.strictExpandOpenAndForbidClosed()
        );
        ShardSearchRequest shardSearchRequest = new ShardSearchRequest(shardId, System.currentTimeMillis(), AliasFilter.EMPTY);
        List<Integer> docIds = List.of(0, 1, 2, 3, 4);
        return new ShardFetchSearchRequest(originalIndices, contextId, shardSearchRequest, docIds, null, null, RescoreDocIds.EMPTY, null);
    }

    private Transport.Connection withTransportVersion(Transport.Connection delegate, TransportVersion version) {
        return new CloseableConnection() {
            @Override
            public DiscoveryNode getNode() {
                return delegate.getNode();
            }

            @Override
            public TransportVersion getTransportVersion() {
                return version;
            }

            @Override
            public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                throws TransportException {
                try {
                    delegate.sendRequest(requestId, action, request, options);
                } catch (Exception e) {
                    throw new TransportException("failed to send request", e);
                }
            }

            @Override
            public void close() {
                delegate.close();
                super.close();
            }

            @Override
            public void onRemoved() {
                delegate.onRemoved();
                super.onRemoved();
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

    private FetchSearchResult createFetchSearchResult() {
        ShardSearchContextId contextId = new ShardSearchContextId("test", randomLong());
        FetchSearchResult result = new FetchSearchResult(contextId, new SearchShardTarget("node", new ShardId("test", "na", 0), null));
        result.shardResult(SearchHits.unpooled(new SearchHit[0], null, Float.NaN), null);
        return result;
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

        return new MockTransport().createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> localNode,
            null,
            Collections.emptySet()
        );
    }
}
