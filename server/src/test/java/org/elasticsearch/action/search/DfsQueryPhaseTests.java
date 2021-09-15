/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.transport.Transport;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicReference;

public class DfsQueryPhaseTests extends ESTestCase {

    private static DfsSearchResult newSearchResult(int shardIndex, ShardSearchContextId contextId, SearchShardTarget target) {
        DfsSearchResult result = new DfsSearchResult(contextId, target, null);
        result.setShardIndex(shardIndex);
        return result;
    }

    public void testDfsWith2Shards() throws IOException {
        AtomicArray<DfsSearchResult> results = new AtomicArray<>(2);
        AtomicReference<AtomicArray<SearchPhaseResult>> responseRef = new AtomicReference<>();
        results.set(0, newSearchResult(0, new ShardSearchContextId("", 1),
            new SearchShardTarget("node1", new ShardId("test", "na", 0), null, OriginalIndices.NONE)));
        results.set(1, newSearchResult(1, new ShardSearchContextId("", 2),
            new SearchShardTarget("node2", new ShardId("test", "na", 0), null, OriginalIndices.NONE)));
        results.get(0).termsStatistics(new Term[0], new TermStatistics[0]);
        results.get(1).termsStatistics(new Term[0], new TermStatistics[0]);

        SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
            @Override
            public void sendExecuteQuery(Transport.Connection connection, QuerySearchRequest request, SearchTask task,
                                         SearchActionListener<QuerySearchResult> listener) {
                if (request.contextId().getId() == 1) {
                    QuerySearchResult queryResult = new QuerySearchResult(new ShardSearchContextId("", 123),
                        new SearchShardTarget("node1", new ShardId("test", "na", 0), null, OriginalIndices.NONE), null);
                    queryResult.topDocs(new TopDocsAndMaxScore(
                            new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                                    new ScoreDoc[] {new ScoreDoc(42, 1.0F)}), 2.0F), new DocValueFormat[0]);
                    queryResult.size(2); // the size of the result set
                    listener.onResponse(queryResult);
                } else if (request.contextId().getId() == 2) {
                    QuerySearchResult queryResult = new QuerySearchResult(new ShardSearchContextId("", 123),
                        new SearchShardTarget("node2", new ShardId("test", "na", 0), null, OriginalIndices.NONE), null);
                    queryResult.topDocs(new TopDocsAndMaxScore(
                            new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] {new ScoreDoc(84, 2.0F)}), 2.0F),
                            new DocValueFormat[0]);
                    queryResult.size(2); // the size of the result set
                    listener.onResponse(queryResult);
                } else {
                    fail("no such request ID: " + request.contextId());
                }
            }
        };
        SearchPhaseController searchPhaseController = searchPhaseController();
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        mockSearchPhaseContext.searchTransport = searchTransportService;
        QueryPhaseResultConsumer consumer = searchPhaseController.newSearchPhaseResults(EsExecutors.DIRECT_EXECUTOR_SERVICE,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST), SearchProgressListener.NOOP, mockSearchPhaseContext.searchRequest,
            results.length(), exc -> {});
        DfsQueryPhase phase = new DfsQueryPhase(results.asList(), null, consumer,
            (response) -> new SearchPhase("test") {
            @Override
            public void run() throws IOException {
                responseRef.set(response.results);
            }
        }, mockSearchPhaseContext);
        assertEquals("dfs_query", phase.getName());
        phase.run();
        mockSearchPhaseContext.assertNoFailure();
        assertNotNull(responseRef.get());
        assertNotNull(responseRef.get().get(0));
        assertNull(responseRef.get().get(0).fetchResult());
        assertEquals(1, responseRef.get().get(0).queryResult().topDocs().topDocs.totalHits.value);
        assertEquals(42, responseRef.get().get(0).queryResult().topDocs().topDocs.scoreDocs[0].doc);
        assertNotNull(responseRef.get().get(1));
        assertNull(responseRef.get().get(1).fetchResult());
        assertEquals(1, responseRef.get().get(1).queryResult().topDocs().topDocs.totalHits.value);
        assertEquals(84, responseRef.get().get(1).queryResult().topDocs().topDocs.scoreDocs[0].doc);
        assertTrue(mockSearchPhaseContext.releasedSearchContexts.isEmpty());
        assertEquals(2, mockSearchPhaseContext.numSuccess.get());
    }

    public void testDfsWith1ShardFailed() throws IOException {
        AtomicArray<DfsSearchResult> results = new AtomicArray<>(2);
        AtomicReference<AtomicArray<SearchPhaseResult>> responseRef = new AtomicReference<>();
        results.set(0, newSearchResult(0, new ShardSearchContextId("", 1),
            new SearchShardTarget("node1", new ShardId("test", "na", 0), null, OriginalIndices.NONE)));
        results.set(1, newSearchResult(1, new ShardSearchContextId("", 2),
            new SearchShardTarget("node2", new ShardId("test", "na", 0), null, OriginalIndices.NONE)));
        results.get(0).termsStatistics(new Term[0], new TermStatistics[0]);
        results.get(1).termsStatistics(new Term[0], new TermStatistics[0]);

        SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
            @Override
            public void sendExecuteQuery(Transport.Connection connection, QuerySearchRequest request, SearchTask task,
                                         SearchActionListener<QuerySearchResult> listener) {
                if (request.contextId().getId() == 1) {
                    QuerySearchResult queryResult = new QuerySearchResult(new ShardSearchContextId("", 123),
                        new SearchShardTarget("node1", new ShardId("test", "na", 0),
                        null, OriginalIndices.NONE), null);
                    queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(
                            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                            new ScoreDoc[] {new ScoreDoc(42, 1.0F)}), 2.0F), new DocValueFormat[0]);
                    queryResult.size(2); // the size of the result set
                    listener.onResponse(queryResult);
                } else if (request.contextId().getId() == 2) {
                    listener.onFailure(new MockDirectoryWrapper.FakeIOException());
                } else {
                    fail("no such request ID: " + request.contextId());
                }
            }
        };
        SearchPhaseController searchPhaseController = searchPhaseController();
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        mockSearchPhaseContext.searchTransport = searchTransportService;
        QueryPhaseResultConsumer consumer = searchPhaseController.newSearchPhaseResults(EsExecutors.DIRECT_EXECUTOR_SERVICE,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST), SearchProgressListener.NOOP, mockSearchPhaseContext.searchRequest,
            results.length(), exc -> {});
        DfsQueryPhase phase = new DfsQueryPhase(results.asList(), null, consumer,
            (response) -> new SearchPhase("test") {
                @Override
                public void run() throws IOException {
                    responseRef.set(response.results);
                }
            }, mockSearchPhaseContext);
        assertEquals("dfs_query", phase.getName());
        phase.run();
        mockSearchPhaseContext.assertNoFailure();
        assertNotNull(responseRef.get());
        assertNotNull(responseRef.get().get(0));
        assertNull(responseRef.get().get(0).fetchResult());
        assertEquals(1, responseRef.get().get(0).queryResult().topDocs().topDocs.totalHits.value);
        assertEquals(42, responseRef.get().get(0).queryResult().topDocs().topDocs.scoreDocs[0].doc);
        assertNull(responseRef.get().get(1));

        assertEquals(1, mockSearchPhaseContext.numSuccess.get());
        assertEquals(1, mockSearchPhaseContext.failures.size());
        assertTrue(mockSearchPhaseContext.failures.get(0).getCause() instanceof MockDirectoryWrapper.FakeIOException);
        assertEquals(1, mockSearchPhaseContext.releasedSearchContexts.size());
        assertTrue(mockSearchPhaseContext.releasedSearchContexts.contains(new ShardSearchContextId("", 2L)));
        assertNull(responseRef.get().get(1));
    }


    public void testFailPhaseOnException() throws IOException {
        AtomicArray<DfsSearchResult> results = new AtomicArray<>(2);
        AtomicReference<AtomicArray<SearchPhaseResult>> responseRef = new AtomicReference<>();
        results.set(0, newSearchResult(0, new ShardSearchContextId("", 1),
            new SearchShardTarget("node1", new ShardId("test", "na", 0), null, OriginalIndices.NONE)));
        results.set(1, newSearchResult(1, new ShardSearchContextId("", 2),
            new SearchShardTarget("node2", new ShardId("test", "na", 0), null, OriginalIndices.NONE)));
        results.get(0).termsStatistics(new Term[0], new TermStatistics[0]);
        results.get(1).termsStatistics(new Term[0], new TermStatistics[0]);

        SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
            @Override
            public void sendExecuteQuery(Transport.Connection connection, QuerySearchRequest request, SearchTask task,
                                         SearchActionListener<QuerySearchResult> listener) {
                if (request.contextId().getId() == 1) {
                    QuerySearchResult queryResult = new QuerySearchResult(new ShardSearchContextId("", 123),
                        new SearchShardTarget("node1", new ShardId("test", "na", 0), null, OriginalIndices.NONE), null);
                    queryResult.topDocs(new TopDocsAndMaxScore(
                            new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                                    new ScoreDoc[] {new ScoreDoc(42, 1.0F)}), 2.0F), new DocValueFormat[0]);
                    queryResult.size(2); // the size of the result set
                    listener.onResponse(queryResult);
                } else if (request.contextId().getId() == 2) {
                   throw new UncheckedIOException(new MockDirectoryWrapper.FakeIOException());
                } else {
                    fail("no such request ID: " + request.contextId());
                }
            }
        };
        SearchPhaseController searchPhaseController = searchPhaseController();
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        mockSearchPhaseContext.searchTransport = searchTransportService;
        QueryPhaseResultConsumer consumer = searchPhaseController.newSearchPhaseResults(EsExecutors.DIRECT_EXECUTOR_SERVICE,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST), SearchProgressListener.NOOP, mockSearchPhaseContext.searchRequest,
            results.length(), exc -> {});
        DfsQueryPhase phase = new DfsQueryPhase(results.asList(), null, consumer,
            (response) -> new SearchPhase("test") {
                @Override
                public void run() throws IOException {
                    responseRef.set(response.results);
                }
            }, mockSearchPhaseContext);
        assertEquals("dfs_query", phase.getName());
        expectThrows(UncheckedIOException.class, phase::run);
        assertTrue(mockSearchPhaseContext.releasedSearchContexts.isEmpty()); // phase execution will clean up on the contexts
    }

    private SearchPhaseController searchPhaseController() {
        return new SearchPhaseController(request -> InternalAggregationTestCase.emptyReduceContextBuilder());
    }
 }
