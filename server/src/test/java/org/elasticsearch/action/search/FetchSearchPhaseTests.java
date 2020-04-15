/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.search;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.SearchContextId;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.transport.Transport;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.search.SearchProgressListener.NOOP;

public class FetchSearchPhaseTests extends ESTestCase {

    public void testShortcutQueryAndFetchOptimization() {
        SearchPhaseController controller = new SearchPhaseController(
            writableRegistry(), s -> InternalAggregationTestCase.emptyReduceContextBuilder());
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        ArraySearchPhaseResults<SearchPhaseResult> results = controller.newSearchPhaseResults(NOOP, mockSearchPhaseContext.getRequest(), 1);
        boolean hasHits = randomBoolean();
        final int numHits;
        if (hasHits) {
            QuerySearchResult queryResult = new QuerySearchResult();
            queryResult.setSearchShardTarget(new SearchShardTarget("node0",
                new ShardId("index", "index", 0), null, OriginalIndices.NONE));
            queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {new ScoreDoc(42, 1.0F)}), 1.0F), new DocValueFormat[0]);
            queryResult.size(1);
            FetchSearchResult fetchResult = new FetchSearchResult();
            fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit(42)}, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0F));
            QueryFetchSearchResult fetchSearchResult = new QueryFetchSearchResult(queryResult, fetchResult);
            fetchSearchResult.setShardIndex(0);
            results.consumeResult(fetchSearchResult);
            numHits = 1;
        } else {
            numHits = 0;
        }

        FetchSearchPhase phase = new FetchSearchPhase(results, controller, mockSearchPhaseContext, ClusterState.EMPTY_STATE,
            (searchResponse, scrollId) -> new SearchPhase("test") {
            @Override
            public void run() {
                mockSearchPhaseContext.sendSearchResponse(searchResponse, null);
            }
        });
        assertEquals("fetch", phase.getName());
        phase.run();
        mockSearchPhaseContext.assertNoFailure();
        SearchResponse searchResponse = mockSearchPhaseContext.searchResponse.get();
        assertNotNull(searchResponse);
        assertEquals(numHits, searchResponse.getHits().getTotalHits().value);
        if (numHits != 0) {
            assertEquals(42, searchResponse.getHits().getAt(0).docId());
        }
        assertTrue(mockSearchPhaseContext.releasedSearchContexts.isEmpty());
    }

    public void testFetchTwoDocument() {
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        SearchPhaseController controller = new SearchPhaseController(
            writableRegistry(), s -> InternalAggregationTestCase.emptyReduceContextBuilder());
        ArraySearchPhaseResults<SearchPhaseResult> results = controller.newSearchPhaseResults(NOOP, mockSearchPhaseContext.getRequest(), 2);
        int resultSetSize = randomIntBetween(2, 10);
        final SearchContextId ctx1 = new SearchContextId(UUIDs.randomBase64UUID(), 123);
        QuerySearchResult queryResult = new QuerySearchResult(ctx1,
            new SearchShardTarget("node1", new ShardId("test", "na", 0), null, OriginalIndices.NONE));
        queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] {new ScoreDoc(42, 1.0F)}), 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize); // the size of the result set
        queryResult.setShardIndex(0);
        results.consumeResult(queryResult);

        final SearchContextId ctx2 = new SearchContextId(UUIDs.randomBase64UUID(), 312);
        queryResult = new QuerySearchResult(ctx2,
            new SearchShardTarget("node2", new ShardId("test", "na", 1), null, OriginalIndices.NONE));
        queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] {new ScoreDoc(84, 2.0F)}), 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize);
        queryResult.setShardIndex(1);
        results.consumeResult(queryResult);

        mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null) {
            @Override
            public void sendExecuteFetch(Transport.Connection connection, ShardFetchSearchRequest request, SearchTask task,
                                         SearchActionListener<FetchSearchResult> listener) {
                FetchSearchResult fetchResult = new FetchSearchResult();
                if (request.contextId().equals(ctx2)) {
                    fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit(84)},
                        new TotalHits(1, TotalHits.Relation.EQUAL_TO), 2.0F));
                } else {
                    assertEquals(ctx1, request.contextId());
                    fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit(42)},
                        new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0F));
                }
                listener.onResponse(fetchResult);
            }
        };
        FetchSearchPhase phase = new FetchSearchPhase(results, controller, mockSearchPhaseContext, ClusterState.EMPTY_STATE,
            (searchResponse, scrollId) -> new SearchPhase("test") {
                @Override
                public void run() {
                    mockSearchPhaseContext.sendSearchResponse(searchResponse, null);
                }
            });
        assertEquals("fetch", phase.getName());
        phase.run();
        mockSearchPhaseContext.assertNoFailure();
        SearchResponse searchResponse = mockSearchPhaseContext.searchResponse.get();
        assertNotNull(searchResponse);
        assertEquals(2, searchResponse.getHits().getTotalHits().value);
        assertEquals(84, searchResponse.getHits().getAt(0).docId());
        assertEquals(42, searchResponse.getHits().getAt(1).docId());
        assertEquals(0, searchResponse.getFailedShards());
        assertEquals(2, searchResponse.getSuccessfulShards());
        assertTrue(mockSearchPhaseContext.releasedSearchContexts.isEmpty());
    }

    public void testFailFetchOneDoc() {
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        SearchPhaseController controller = new SearchPhaseController(
            writableRegistry(), s -> InternalAggregationTestCase.emptyReduceContextBuilder());
        ArraySearchPhaseResults<SearchPhaseResult> results =
            controller.newSearchPhaseResults(NOOP, mockSearchPhaseContext.getRequest(), 2);
        int resultSetSize = randomIntBetween(2, 10);
        SearchContextId ctx1 = new SearchContextId(UUIDs.randomBase64UUID(), 123);
        QuerySearchResult queryResult = new QuerySearchResult(ctx1,
            new SearchShardTarget("node1", new ShardId("test", "na", 0), null, OriginalIndices.NONE));
        queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] {new ScoreDoc(42, 1.0F)}), 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize); // the size of the result set
        queryResult.setShardIndex(0);
        results.consumeResult(queryResult);

        SearchContextId ctx2 = new SearchContextId(UUIDs.randomBase64UUID(), 321);
        queryResult = new QuerySearchResult(ctx2,
            new SearchShardTarget("node2", new ShardId("test", "na", 1), null, OriginalIndices.NONE));
        queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] {new ScoreDoc(84, 2.0F)}), 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize);
        queryResult.setShardIndex(1);
        results.consumeResult(queryResult);

        mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null) {
            @Override
            public void sendExecuteFetch(Transport.Connection connection, ShardFetchSearchRequest request, SearchTask task,
                                         SearchActionListener<FetchSearchResult> listener) {
                if (request.contextId().getId() == 321) {
                    FetchSearchResult fetchResult = new FetchSearchResult();
                    fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit(84)},
                        new TotalHits(1, TotalHits.Relation.EQUAL_TO), 2.0F));
                    listener.onResponse(fetchResult);
                } else {
                    listener.onFailure(new MockDirectoryWrapper.FakeIOException());
                }

            }
        };
        FetchSearchPhase phase = new FetchSearchPhase(results, controller, mockSearchPhaseContext, ClusterState.EMPTY_STATE,
            (searchResponse, scrollId) -> new SearchPhase("test") {
                @Override
                public void run() {
                    mockSearchPhaseContext.sendSearchResponse(searchResponse, null);
                }
            });
        assertEquals("fetch", phase.getName());
        phase.run();
        mockSearchPhaseContext.assertNoFailure();
        SearchResponse searchResponse = mockSearchPhaseContext.searchResponse.get();
        assertNotNull(searchResponse);
        assertEquals(2, searchResponse.getHits().getTotalHits().value);
        assertEquals(84, searchResponse.getHits().getAt(0).docId());
        assertEquals(1, searchResponse.getFailedShards());
        assertEquals(1, searchResponse.getSuccessfulShards());
        assertEquals(1, searchResponse.getShardFailures().length);
        assertTrue(searchResponse.getShardFailures()[0].getCause() instanceof MockDirectoryWrapper.FakeIOException);
        assertEquals(1, mockSearchPhaseContext.releasedSearchContexts.size());
        assertTrue(mockSearchPhaseContext.releasedSearchContexts.contains(ctx1));
    }

    public void testFetchDocsConcurrently() throws InterruptedException {
        int resultSetSize = randomIntBetween(0, 100);
        // we use at least 2 hits otherwise this is subject to single shard optimization and we trip an assert...
        int numHits = randomIntBetween(2, 100); // also numshards --> 1 hit per shard
        SearchPhaseController controller = new SearchPhaseController(
            writableRegistry(), s -> InternalAggregationTestCase.emptyReduceContextBuilder());
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(numHits);
        ArraySearchPhaseResults<SearchPhaseResult> results = controller.newSearchPhaseResults(NOOP,
            mockSearchPhaseContext.getRequest(), numHits);
        for (int i = 0; i < numHits; i++) {
            QuerySearchResult queryResult = new QuerySearchResult(new SearchContextId("", i),
                new SearchShardTarget("node1", new ShardId("test", "na", 0), null, OriginalIndices.NONE));
            queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {new ScoreDoc(i+1, i)}), i), new DocValueFormat[0]);
            queryResult.size(resultSetSize); // the size of the result set
            queryResult.setShardIndex(i);
            results.consumeResult(queryResult);
        }
        mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null) {
            @Override
            public void sendExecuteFetch(Transport.Connection connection, ShardFetchSearchRequest request, SearchTask task,
                                         SearchActionListener<FetchSearchResult> listener) {
                new Thread(() -> {
                    FetchSearchResult fetchResult = new FetchSearchResult();
                    fetchResult.hits(new SearchHits(new SearchHit[]{new SearchHit((int) (request.contextId().getId() + 1))},
                        new TotalHits(1, TotalHits.Relation.EQUAL_TO), 100F));
                    listener.onResponse(fetchResult);
                }).start();
            }
        };
        CountDownLatch latch = new CountDownLatch(1);
        FetchSearchPhase phase = new FetchSearchPhase(results, controller, mockSearchPhaseContext, ClusterState.EMPTY_STATE,
            (searchResponse, scrollId) -> new SearchPhase("test") {
                @Override
                public void run() {
                    mockSearchPhaseContext.sendSearchResponse(searchResponse, null);
                    latch.countDown();
                }
            });
        assertEquals("fetch", phase.getName());
        phase.run();
        latch.await();
        mockSearchPhaseContext.assertNoFailure();
        SearchResponse searchResponse = mockSearchPhaseContext.searchResponse.get();
        assertNotNull(searchResponse);
        assertEquals(numHits, searchResponse.getHits().getTotalHits().value);
        assertEquals(Math.min(numHits, resultSetSize), searchResponse.getHits().getHits().length);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (int i = 0; i < hits.length; i++) {
            assertNotNull(hits[i]);
            assertEquals("index: " + i, numHits-i, hits[i].docId());
            assertEquals("index: " + i, numHits-1-i, (int)hits[i].getScore());
        }
        assertEquals(0, searchResponse.getFailedShards());
        assertEquals(numHits, searchResponse.getSuccessfulShards());
        int sizeReleasedContexts = Math.max(0, numHits - resultSetSize); // all non fetched results will be freed
        assertEquals(mockSearchPhaseContext.releasedSearchContexts.toString(),
            sizeReleasedContexts, mockSearchPhaseContext.releasedSearchContexts.size());
    }

    public void testExceptionFailsPhase() {
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        SearchPhaseController controller = new SearchPhaseController(
            writableRegistry(), s -> InternalAggregationTestCase.emptyReduceContextBuilder());
        ArraySearchPhaseResults<SearchPhaseResult> results =
            controller.newSearchPhaseResults(NOOP, mockSearchPhaseContext.getRequest(), 2);
        int resultSetSize = randomIntBetween(2, 10);
        QuerySearchResult queryResult = new QuerySearchResult(new SearchContextId("", 123),
            new SearchShardTarget("node1", new ShardId("test", "na", 0), null, OriginalIndices.NONE));
        queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] {new ScoreDoc(42, 1.0F)}), 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize); // the size of the result set
        queryResult.setShardIndex(0);
        results.consumeResult(queryResult);

        queryResult = new QuerySearchResult(new SearchContextId("", 321),
            new SearchShardTarget("node2", new ShardId("test", "na", 1), null, OriginalIndices.NONE));
        queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] {new ScoreDoc(84, 2.0F)}), 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize);
        queryResult.setShardIndex(1);
        results.consumeResult(queryResult);
        AtomicInteger numFetches = new AtomicInteger(0);
        mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null) {
            @Override
            public void sendExecuteFetch(Transport.Connection connection, ShardFetchSearchRequest request, SearchTask task,
                                         SearchActionListener<FetchSearchResult> listener) {
                FetchSearchResult fetchResult = new FetchSearchResult();
                if (numFetches.incrementAndGet() == 1) {
                    throw new RuntimeException("BOOM");
                }
                if (request.contextId().getId() == 321) {
                    fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit(84)},
                        new TotalHits(1, TotalHits.Relation.EQUAL_TO), 2.0F));
                } else {
                    assertEquals(request.contextId().getId(), 123);
                    fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit(42)},
                        new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0F));
                }
                listener.onResponse(fetchResult);
            }
        };
        FetchSearchPhase phase = new FetchSearchPhase(results, controller, mockSearchPhaseContext, ClusterState.EMPTY_STATE,
            (searchResponse, scrollId) -> new SearchPhase("test") {
                @Override
                public void run() {
                    mockSearchPhaseContext.sendSearchResponse(searchResponse, null);
                }
            });
        assertEquals("fetch", phase.getName());
        phase.run();
        assertNotNull(mockSearchPhaseContext.phaseFailure.get());
        assertEquals(mockSearchPhaseContext.phaseFailure.get().getMessage(), "BOOM");
        assertNull(mockSearchPhaseContext.searchResponse.get());
        assertTrue(mockSearchPhaseContext.releasedSearchContexts.isEmpty());
    }

    public void testCleanupIrrelevantContexts() { // contexts that are not fetched should be cleaned up
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        SearchPhaseController controller = new SearchPhaseController(
            writableRegistry(), s -> InternalAggregationTestCase.emptyReduceContextBuilder());
        ArraySearchPhaseResults<SearchPhaseResult> results =
            controller.newSearchPhaseResults(NOOP, mockSearchPhaseContext.getRequest(), 2);
        int resultSetSize = 1;
        SearchContextId ctx1 = new SearchContextId(UUIDs.randomBase64UUID(), 123);
        QuerySearchResult queryResult = new QuerySearchResult(ctx1,
            new SearchShardTarget("node1", new ShardId("test", "na", 0), null, OriginalIndices.NONE));
        queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] {new ScoreDoc(42, 1.0F)}), 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize); // the size of the result set
        queryResult.setShardIndex(0);
        results.consumeResult(queryResult);

        SearchContextId ctx2 = new SearchContextId(UUIDs.randomBase64UUID(), 321);
        queryResult = new QuerySearchResult(ctx2,
            new SearchShardTarget("node2", new ShardId("test", "na", 1), null, OriginalIndices.NONE));
        queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] {new ScoreDoc(84, 2.0F)}), 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize);
        queryResult.setShardIndex(1);
        results.consumeResult(queryResult);

        mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null) {
            @Override
            public void sendExecuteFetch(Transport.Connection connection, ShardFetchSearchRequest request, SearchTask task,
                                         SearchActionListener<FetchSearchResult> listener) {
                FetchSearchResult fetchResult = new FetchSearchResult();
                if (request.contextId().equals(ctx2)) {
                    fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit(84)},
                        new TotalHits(1, TotalHits.Relation.EQUAL_TO), 2.0F));
                } else {
                    fail("requestID 123 should not be fetched but was");
                }
                listener.onResponse(fetchResult);
            }
        };
        FetchSearchPhase phase = new FetchSearchPhase(results, controller, mockSearchPhaseContext, ClusterState.EMPTY_STATE,
            (searchResponse, scrollId) -> new SearchPhase("test") {
                @Override
                public void run() {
                    mockSearchPhaseContext.sendSearchResponse(searchResponse, null);
                }
            });
        assertEquals("fetch", phase.getName());
        phase.run();
        mockSearchPhaseContext.assertNoFailure();
        SearchResponse searchResponse = mockSearchPhaseContext.searchResponse.get();
        assertNotNull(searchResponse);
        assertEquals(2, searchResponse.getHits().getTotalHits().value);
        assertEquals(1, searchResponse.getHits().getHits().length);
        assertEquals(84, searchResponse.getHits().getAt(0).docId());
        assertEquals(0, searchResponse.getFailedShards());
        assertEquals(2, searchResponse.getSuccessfulShards());
        assertEquals(1, mockSearchPhaseContext.releasedSearchContexts.size());
        assertTrue(mockSearchPhaseContext.releasedSearchContexts.contains(ctx1));
    }
}
