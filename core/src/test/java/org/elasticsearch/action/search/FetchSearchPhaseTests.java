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
import org.apache.lucene.store.MockDirectoryWrapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.Index;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Transport;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class FetchSearchPhaseTests extends ESTestCase {

    public void testShortcutQueryAndFetchOptimization() throws IOException {
        SearchPhaseController controller = new SearchPhaseController(Settings.EMPTY, BigArrays.NON_RECYCLING_INSTANCE, null);
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        InitialSearchPhase.ArraySearchPhaseResults<SearchPhaseResult> results =
            controller.newSearchPhaseResults(mockSearchPhaseContext.getRequest(), 1);
        AtomicReference<SearchResponse> responseRef = new AtomicReference<>();
        boolean hasHits = randomBoolean();
        final int numHits;
        if (hasHits) {
            QuerySearchResult queryResult = new QuerySearchResult();
            queryResult.topDocs(new TopDocs(1, new ScoreDoc[] {new ScoreDoc(42, 1.0F)}, 1.0F), new DocValueFormat[0]);
            queryResult.size(1);
            FetchSearchResult fetchResult = new FetchSearchResult();
            fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit(42)}, 1, 1.0F));
            QueryFetchSearchResult fetchSearchResult = new QueryFetchSearchResult(queryResult, fetchResult);
            fetchSearchResult.setShardIndex(0);
            results.consumeResult(fetchSearchResult);
            numHits = 1;
        } else {
            numHits = 0;
        }

        FetchSearchPhase phase = new FetchSearchPhase(results, controller, mockSearchPhaseContext,
            (searchResponse, scrollId) -> new SearchPhase("test") {
            @Override
            public void run() throws IOException {
                responseRef.set(mockSearchPhaseContext.buildSearchResponse(searchResponse, null));
            }
        });
        assertEquals("fetch", phase.getName());
        phase.run();
        mockSearchPhaseContext.assertNoFailure();
        assertNotNull(responseRef.get());
        assertEquals(numHits, responseRef.get().getHits().totalHits);
        if (numHits != 0) {
            assertEquals(42, responseRef.get().getHits().getAt(0).docId());
        }
        assertTrue(mockSearchPhaseContext.releasedSearchContexts.isEmpty());
    }

    public void testFetchTwoDocument() throws IOException {
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        SearchPhaseController controller = new SearchPhaseController(Settings.EMPTY, BigArrays.NON_RECYCLING_INSTANCE, null);
        InitialSearchPhase.ArraySearchPhaseResults<SearchPhaseResult> results =
            controller.newSearchPhaseResults(mockSearchPhaseContext.getRequest(), 2);
        AtomicReference<SearchResponse> responseRef = new AtomicReference<>();
        int resultSetSize = randomIntBetween(2, 10);
        QuerySearchResult queryResult = new QuerySearchResult(123, new SearchShardTarget("node1", new Index("test", "na"), 0, null));
        queryResult.topDocs(new TopDocs(1, new ScoreDoc[] {new ScoreDoc(42, 1.0F)}, 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize); // the size of the result set
        queryResult.setShardIndex(0);
        results.consumeResult(queryResult);

        queryResult = new QuerySearchResult(321, new SearchShardTarget("node2", new Index("test", "na"), 1, null));
        queryResult.topDocs(new TopDocs(1, new ScoreDoc[] {new ScoreDoc(84, 2.0F)}, 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize);
        queryResult.setShardIndex(1);
        results.consumeResult(queryResult);

        SearchTransportService searchTransportService = new SearchTransportService(
            Settings.builder().put("search.remote.connect", false).build(), null, null) {
            @Override
            public void sendExecuteFetch(Transport.Connection connection, ShardFetchSearchRequest request, SearchTask task,
                                         SearchActionListener<FetchSearchResult> listener) {
                FetchSearchResult fetchResult = new FetchSearchResult();
                if (request.id() == 321) {
                    fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit(84)}, 1, 2.0F));
                } else {
                    assertEquals(123, request.id());
                    fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit(42)}, 1, 1.0F));
                }
                listener.onResponse(fetchResult);
            }
        };
        mockSearchPhaseContext.searchTransport = searchTransportService;
        FetchSearchPhase phase = new FetchSearchPhase(results, controller, mockSearchPhaseContext,
            (searchResponse, scrollId) -> new SearchPhase("test") {
                @Override
                public void run() throws IOException {
                    responseRef.set(mockSearchPhaseContext.buildSearchResponse(searchResponse, null));
                }
            });
        assertEquals("fetch", phase.getName());
        phase.run();
        mockSearchPhaseContext.assertNoFailure();
        assertNotNull(responseRef.get());
        assertEquals(2, responseRef.get().getHits().totalHits);
        assertEquals(84, responseRef.get().getHits().getAt(0).docId());
        assertEquals(42, responseRef.get().getHits().getAt(1).docId());
        assertEquals(0, responseRef.get().getFailedShards());
        assertEquals(2, responseRef.get().getSuccessfulShards());
        assertTrue(mockSearchPhaseContext.releasedSearchContexts.isEmpty());
    }

    public void testFailFetchOneDoc() throws IOException {
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        SearchPhaseController controller = new SearchPhaseController(Settings.EMPTY, BigArrays.NON_RECYCLING_INSTANCE, null);
        InitialSearchPhase.ArraySearchPhaseResults<SearchPhaseResult> results =
            controller.newSearchPhaseResults(mockSearchPhaseContext.getRequest(), 2);
        AtomicReference<SearchResponse> responseRef = new AtomicReference<>();
        int resultSetSize = randomIntBetween(2, 10);
        QuerySearchResult queryResult = new QuerySearchResult(123, new SearchShardTarget("node1", new Index("test", "na"), 0, null));
        queryResult.topDocs(new TopDocs(1, new ScoreDoc[] {new ScoreDoc(42, 1.0F)}, 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize); // the size of the result set
        queryResult.setShardIndex(0);
        results.consumeResult(queryResult);

        queryResult = new QuerySearchResult(321, new SearchShardTarget("node2", new Index("test", "na"), 1, null));
        queryResult.topDocs(new TopDocs(1, new ScoreDoc[] {new ScoreDoc(84, 2.0F)}, 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize);
        queryResult.setShardIndex(1);
        results.consumeResult(queryResult);

        SearchTransportService searchTransportService = new SearchTransportService(
            Settings.builder().put("search.remote.connect", false).build(), null, null) {
            @Override
            public void sendExecuteFetch(Transport.Connection connection, ShardFetchSearchRequest request, SearchTask task,
                                         SearchActionListener<FetchSearchResult> listener) {
                if (request.id() == 321) {
                    FetchSearchResult fetchResult = new FetchSearchResult();
                    fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit(84)}, 1, 2.0F));
                    listener.onResponse(fetchResult);
                } else {
                    listener.onFailure(new MockDirectoryWrapper.FakeIOException());
                }

            }
        };
        mockSearchPhaseContext.searchTransport = searchTransportService;
        FetchSearchPhase phase = new FetchSearchPhase(results, controller, mockSearchPhaseContext,
            (searchResponse, scrollId) -> new SearchPhase("test") {
                @Override
                public void run() throws IOException {
                    responseRef.set(mockSearchPhaseContext.buildSearchResponse(searchResponse, null));
                }
            });
        assertEquals("fetch", phase.getName());
        phase.run();
        mockSearchPhaseContext.assertNoFailure();
        assertNotNull(responseRef.get());
        assertEquals(2, responseRef.get().getHits().totalHits);
        assertEquals(84, responseRef.get().getHits().getAt(0).docId());
        assertEquals(1, responseRef.get().getFailedShards());
        assertEquals(1, responseRef.get().getSuccessfulShards());
        assertEquals(1, responseRef.get().getShardFailures().length);
        assertTrue(responseRef.get().getShardFailures()[0].getCause() instanceof MockDirectoryWrapper.FakeIOException);
        assertEquals(1, mockSearchPhaseContext.releasedSearchContexts.size());
        assertTrue(mockSearchPhaseContext.releasedSearchContexts.contains(123L));
    }

    public void testFetchDocsConcurrently() throws IOException, InterruptedException {
        int resultSetSize = randomIntBetween(0, 100);
        // we use at least 2 hits otherwise this is subject to single shard optimization and we trip an assert...
        int numHits = randomIntBetween(2, 100); // also numshards --> 1 hit per shard
        SearchPhaseController controller = new SearchPhaseController(Settings.EMPTY, BigArrays.NON_RECYCLING_INSTANCE, null);
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(numHits);
        InitialSearchPhase.ArraySearchPhaseResults<SearchPhaseResult> results =
            controller.newSearchPhaseResults(mockSearchPhaseContext.getRequest(), numHits);
        AtomicReference<SearchResponse> responseRef = new AtomicReference<>();
        for (int i = 0; i < numHits; i++) {
            QuerySearchResult queryResult = new QuerySearchResult(i, new SearchShardTarget("node1", new Index("test", "na"), 0, null));
            queryResult.topDocs(new TopDocs(1, new ScoreDoc[] {new ScoreDoc(i+1, i)}, i), new DocValueFormat[0]);
            queryResult.size(resultSetSize); // the size of the result set
            queryResult.setShardIndex(i);
            results.consumeResult(queryResult);
        }
        SearchTransportService searchTransportService = new SearchTransportService(
            Settings.builder().put("search.remote.connect", false).build(), null, null) {
            @Override
            public void sendExecuteFetch(Transport.Connection connection, ShardFetchSearchRequest request, SearchTask task,
                                         SearchActionListener<FetchSearchResult> listener) {
                new Thread(() -> {
                    FetchSearchResult fetchResult = new FetchSearchResult();
                    fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit((int) (request.id()+1))}, 1, 100F));
                    listener.onResponse(fetchResult);
                }).start();
            }
        };
        mockSearchPhaseContext.searchTransport = searchTransportService;
        CountDownLatch latch = new CountDownLatch(1);
        FetchSearchPhase phase = new FetchSearchPhase(results, controller, mockSearchPhaseContext,
            (searchResponse, scrollId) -> new SearchPhase("test") {
                @Override
                public void run() throws IOException {
                    responseRef.set(mockSearchPhaseContext.buildSearchResponse(searchResponse, null));
                    latch.countDown();
                }
            });
        assertEquals("fetch", phase.getName());
        phase.run();
        latch.await();
        mockSearchPhaseContext.assertNoFailure();
        assertNotNull(responseRef.get());
        assertEquals(numHits, responseRef.get().getHits().totalHits);
        assertEquals(Math.min(numHits, resultSetSize), responseRef.get().getHits().getHits().length);
        SearchHit[] hits = responseRef.get().getHits().getHits();
        for (int i = 0; i < hits.length; i++) {
            assertNotNull(hits[i]);
            assertEquals("index: " + i, numHits-i, hits[i].docId());
            assertEquals("index: " + i, numHits-1-i, (int)hits[i].getScore());
        }
        assertEquals(0, responseRef.get().getFailedShards());
        assertEquals(numHits, responseRef.get().getSuccessfulShards());
        int sizeReleasedContexts = Math.max(0, numHits - resultSetSize); // all non fetched results will be freed
        assertEquals(mockSearchPhaseContext.releasedSearchContexts.toString(),
            sizeReleasedContexts, mockSearchPhaseContext.releasedSearchContexts.size());
    }

    public void testExceptionFailsPhase() throws IOException {
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        SearchPhaseController controller = new SearchPhaseController(Settings.EMPTY, BigArrays.NON_RECYCLING_INSTANCE, null);
        InitialSearchPhase.ArraySearchPhaseResults<SearchPhaseResult> results =
            controller.newSearchPhaseResults(mockSearchPhaseContext.getRequest(), 2);
        AtomicReference<SearchResponse> responseRef = new AtomicReference<>();
        int resultSetSize = randomIntBetween(2, 10);
        QuerySearchResult queryResult = new QuerySearchResult(123, new SearchShardTarget("node1", new Index("test", "na"), 0, null));
        queryResult.topDocs(new TopDocs(1, new ScoreDoc[] {new ScoreDoc(42, 1.0F)}, 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize); // the size of the result set
        queryResult.setShardIndex(0);
        results.consumeResult(queryResult);

        queryResult = new QuerySearchResult(321, new SearchShardTarget("node2", new Index("test", "na"), 1, null));
        queryResult.topDocs(new TopDocs(1, new ScoreDoc[] {new ScoreDoc(84, 2.0F)}, 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize);
        queryResult.setShardIndex(1);
        results.consumeResult(queryResult);
        AtomicInteger numFetches = new AtomicInteger(0);
        SearchTransportService searchTransportService = new SearchTransportService(
            Settings.builder().put("search.remote.connect", false).build(), null, null) {
            @Override
            public void sendExecuteFetch(Transport.Connection connection, ShardFetchSearchRequest request, SearchTask task,
                                         SearchActionListener<FetchSearchResult> listener) {
                FetchSearchResult fetchResult = new FetchSearchResult();
                if (numFetches.incrementAndGet() == 1) {
                    throw new RuntimeException("BOOM");
                }
                if (request.id() == 321) {
                    fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit(84)}, 1, 2.0F));
                } else {
                    assertEquals(request, 123);
                    fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit(42)}, 1, 1.0F));
                }
                listener.onResponse(fetchResult);
            }
        };
        mockSearchPhaseContext.searchTransport = searchTransportService;
        FetchSearchPhase phase = new FetchSearchPhase(results, controller, mockSearchPhaseContext,
            (searchResponse, scrollId) -> new SearchPhase("test") {
                @Override
                public void run() throws IOException {
                    responseRef.set(mockSearchPhaseContext.buildSearchResponse(searchResponse, null));
                }
            });
        assertEquals("fetch", phase.getName());
        phase.run();
        assertNotNull(mockSearchPhaseContext.phaseFailure.get());
        assertEquals(mockSearchPhaseContext.phaseFailure.get().getMessage(), "BOOM");
        assertNull(responseRef.get());
        assertTrue(mockSearchPhaseContext.releasedSearchContexts.isEmpty());
    }

    public void testCleanupIrrelevantContexts() throws IOException { // contexts that are not fetched should be cleaned up
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        SearchPhaseController controller = new SearchPhaseController(Settings.EMPTY, BigArrays.NON_RECYCLING_INSTANCE, null);
        InitialSearchPhase.ArraySearchPhaseResults<SearchPhaseResult> results =
            controller.newSearchPhaseResults(mockSearchPhaseContext.getRequest(), 2);
        AtomicReference<SearchResponse> responseRef = new AtomicReference<>();
        int resultSetSize = 1;
        QuerySearchResult queryResult = new QuerySearchResult(123, new SearchShardTarget("node1", new Index("test", "na"), 0, null));
        queryResult.topDocs(new TopDocs(1, new ScoreDoc[] {new ScoreDoc(42, 1.0F)}, 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize); // the size of the result set
        queryResult.setShardIndex(0);
        results.consumeResult(queryResult);

        queryResult = new QuerySearchResult(321, new SearchShardTarget("node2", new Index("test", "na"), 1, null));
        queryResult.topDocs(new TopDocs(1, new ScoreDoc[] {new ScoreDoc(84, 2.0F)}, 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize);
        queryResult.setShardIndex(1);
        results.consumeResult(queryResult);

        SearchTransportService searchTransportService = new SearchTransportService(
            Settings.builder().put("search.remote.connect", false).build(), null, null) {
            @Override
            public void sendExecuteFetch(Transport.Connection connection, ShardFetchSearchRequest request, SearchTask task,
                                         SearchActionListener<FetchSearchResult> listener) {
                FetchSearchResult fetchResult = new FetchSearchResult();
                if (request.id() == 321) {
                    fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit(84)}, 1, 2.0F));
                } else {
                    fail("requestID 123 should not be fetched but was");
                }
                listener.onResponse(fetchResult);
            }
        };
        mockSearchPhaseContext.searchTransport = searchTransportService;
        FetchSearchPhase phase = new FetchSearchPhase(results, controller, mockSearchPhaseContext,
            (searchResponse, scrollId) -> new SearchPhase("test") {
                @Override
                public void run() throws IOException {
                    responseRef.set(mockSearchPhaseContext.buildSearchResponse(searchResponse, null));
                }
            });
        assertEquals("fetch", phase.getName());
        phase.run();
        mockSearchPhaseContext.assertNoFailure();
        assertNotNull(responseRef.get());
        assertEquals(2, responseRef.get().getHits().totalHits);
        assertEquals(1, responseRef.get().getHits().getHits().length);
        assertEquals(84, responseRef.get().getHits().getAt(0).docId());
        assertEquals(0, responseRef.get().getFailedShards());
        assertEquals(2, responseRef.get().getSuccessfulShards());
        assertEquals(1, mockSearchPhaseContext.releasedSearchContexts.size());
        assertTrue(mockSearchPhaseContext.releasedSearchContexts.contains(123L));
    }
}
