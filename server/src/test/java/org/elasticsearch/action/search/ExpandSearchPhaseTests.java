/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.AbstractSearchTestCase;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ExpandSearchPhaseTests extends ESTestCase {

    public void testCollapseSingleHit() throws IOException {
        final int iters = randomIntBetween(5, 10);
        for (int i = 0; i < iters; i++) {
            final int numInnerHits = randomIntBetween(1, 5);
            List<SearchHits> collapsedHits = new ArrayList<>(numInnerHits);
            for (int innerHitNum = 0; innerHitNum < numInnerHits; innerHitNum++) {
                SearchHits hits = new SearchHits(
                    new SearchHit[] {
                        new SearchHit(innerHitNum, "ID", Collections.emptyMap(), Collections.emptyMap()),
                        new SearchHit(innerHitNum + 1, "ID", Collections.emptyMap(), Collections.emptyMap()) },
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    1.0F
                );
                collapsedHits.add(hits);
            }

            AtomicBoolean executedMultiSearch = new AtomicBoolean(false);
            QueryBuilder originalQuery = randomBoolean() ? null : QueryBuilders.termQuery("foo", "bar");
            Map<String, Object> runtimeMappings = randomBoolean() ? emptyMap() : AbstractSearchTestCase.randomRuntimeMappings();

            final MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
            String collapseValue = randomBoolean() ? null : "boom";

            mockSearchPhaseContext.getRequest()
                .source(
                    new SearchSourceBuilder().collapse(
                        new CollapseBuilder("someField").setInnerHits(
                            IntStream.range(0, numInnerHits).mapToObj(hitNum -> new InnerHitBuilder().setName("innerHit" + hitNum)).toList()
                        )
                    )
                );
            mockSearchPhaseContext.getRequest().source().query(originalQuery).runtimeMappings(runtimeMappings);
            mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
                @Override
                void sendExecuteMultiSearch(MultiSearchRequest request, SearchTask task, ActionListener<MultiSearchResponse> listener) {
                    assertTrue(executedMultiSearch.compareAndSet(false, true));
                    assertEquals(numInnerHits, request.requests().size());
                    SearchRequest searchRequest = request.requests().get(0);
                    assertTrue(searchRequest.source().query() instanceof BoolQueryBuilder);

                    BoolQueryBuilder groupBuilder = (BoolQueryBuilder) searchRequest.source().query();
                    if (collapseValue == null) {
                        assertThat(groupBuilder.mustNot(), Matchers.contains(QueryBuilders.existsQuery("someField")));
                    } else {
                        assertThat(groupBuilder.filter(), Matchers.contains(QueryBuilders.matchQuery("someField", "boom")));
                    }
                    if (originalQuery != null) {
                        assertThat(groupBuilder.must(), Matchers.contains(QueryBuilders.termQuery("foo", "bar")));
                    }
                    assertArrayEquals(mockSearchPhaseContext.getRequest().indices(), searchRequest.indices());
                    assertThat(searchRequest.source().runtimeMappings(), equalTo(runtimeMappings));

                    List<MultiSearchResponse.Item> mSearchResponses = new ArrayList<>(numInnerHits);
                    for (int innerHitNum = 0; innerHitNum < numInnerHits; innerHitNum++) {
                        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(
                            collapsedHits.get(innerHitNum),
                            null,
                            null,
                            null,
                            false,
                            null,
                            1
                        );
                        mockSearchPhaseContext.sendSearchResponse(internalSearchResponse, null);
                        mSearchResponses.add(new MultiSearchResponse.Item(mockSearchPhaseContext.searchResponse.get(), null));
                    }

                    listener.onResponse(
                        new MultiSearchResponse(mSearchResponses.toArray(new MultiSearchResponse.Item[0]), randomIntBetween(1, 10000))
                    );
                }
            };

            SearchHits hits = new SearchHits(
                new SearchHit[] {
                    new SearchHit(
                        1,
                        "ID",
                        Collections.singletonMap("someField", new DocumentField("someField", Collections.singletonList(collapseValue))),
                        Collections.emptyMap()
                    ) },
                new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                1.0F
            );
            InternalSearchResponse internalSearchResponse = new InternalSearchResponse(hits, null, null, null, false, null, 1);
            ExpandSearchPhase phase = new ExpandSearchPhase(
                mockSearchPhaseContext,
                internalSearchResponse,
                null,
                () -> new SearchPhase("test") {
                    @Override
                    public void run() {
                        mockSearchPhaseContext.sendSearchResponse(internalSearchResponse, null);
                    }
                }
            );

            phase.run();
            mockSearchPhaseContext.assertNoFailure();
            SearchResponse theResponse = mockSearchPhaseContext.searchResponse.get();
            assertNotNull(theResponse);
            assertEquals(numInnerHits, theResponse.getHits().getHits()[0].getInnerHits().size());

            for (int innerHitNum = 0; innerHitNum < numInnerHits; innerHitNum++) {
                assertSame(theResponse.getHits().getHits()[0].getInnerHits().get("innerHit" + innerHitNum), collapsedHits.get(innerHitNum));
            }

            assertTrue(executedMultiSearch.get());
        }
    }

    public void testFailOneItemFailsEntirePhase() throws IOException {
        AtomicBoolean executedMultiSearch = new AtomicBoolean(false);

        SearchHits collapsedHits = new SearchHits(
            new SearchHit[] {
                new SearchHit(2, "ID", Collections.emptyMap(), Collections.emptyMap()),
                new SearchHit(3, "ID", Collections.emptyMap(), Collections.emptyMap()) },
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            1.0F
        );
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        String collapseValue = randomBoolean() ? null : "boom";
        mockSearchPhaseContext.getRequest()
            .source(
                new SearchSourceBuilder().collapse(
                    new CollapseBuilder("someField").setInnerHits(new InnerHitBuilder().setName("foobarbaz"))
                )
            );
        mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
            @Override
            void sendExecuteMultiSearch(MultiSearchRequest request, SearchTask task, ActionListener<MultiSearchResponse> listener) {
                assertTrue(executedMultiSearch.compareAndSet(false, true));
                InternalSearchResponse internalSearchResponse = new InternalSearchResponse(collapsedHits, null, null, null, false, null, 1);
                SearchResponse searchResponse = new SearchResponse(
                    internalSearchResponse,
                    null,
                    1,
                    1,
                    0,
                    0,
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponse.Clusters.EMPTY
                );
                listener.onResponse(
                    new MultiSearchResponse(
                        new MultiSearchResponse.Item[] {
                            new MultiSearchResponse.Item(null, new RuntimeException("boom")),
                            new MultiSearchResponse.Item(searchResponse, null) },
                        randomIntBetween(1, 10000)
                    )
                );
            }
        };

        SearchHits hits = new SearchHits(
            new SearchHit[] {
                new SearchHit(
                    1,
                    "ID",
                    Collections.singletonMap("someField", new DocumentField("someField", Collections.singletonList(collapseValue))),
                    Collections.emptyMap()
                ),
                new SearchHit(
                    2,
                    "ID2",
                    Collections.singletonMap("someField", new DocumentField("someField", Collections.singletonList(collapseValue))),
                    Collections.emptyMap()
                ) },
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            1.0F
        );
        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(hits, null, null, null, false, null, 1);
        ExpandSearchPhase phase = new ExpandSearchPhase(
            mockSearchPhaseContext,
            internalSearchResponse,
            null,
            () -> new SearchPhase("test") {
                @Override
                public void run() {
                    mockSearchPhaseContext.sendSearchResponse(internalSearchResponse, null);
                }
            }
        );
        phase.run();
        assertThat(mockSearchPhaseContext.phaseFailure.get(), Matchers.instanceOf(RuntimeException.class));
        assertEquals("boom", mockSearchPhaseContext.phaseFailure.get().getMessage());
        assertNotNull(mockSearchPhaseContext.phaseFailure.get());
        assertNull(mockSearchPhaseContext.searchResponse.get());
    }

    public void testSkipPhase() throws IOException {
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
            @Override
            void sendExecuteMultiSearch(MultiSearchRequest request, SearchTask task, ActionListener<MultiSearchResponse> listener) {
                fail("no collapsing here");
            }
        };

        SearchHits hits = new SearchHits(
            new SearchHit[] {
                new SearchHit(
                    1,
                    "ID",
                    Collections.singletonMap("someField", new DocumentField("someField", Collections.singletonList(null))),
                    Collections.emptyMap()
                ),
                new SearchHit(
                    2,
                    "ID2",
                    Collections.singletonMap("someField", new DocumentField("someField", Collections.singletonList(null))),
                    Collections.emptyMap()
                ) },
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            1.0F
        );
        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(hits, null, null, null, false, null, 1);
        ExpandSearchPhase phase = new ExpandSearchPhase(
            mockSearchPhaseContext,
            internalSearchResponse,
            null,
            () -> new SearchPhase("test") {
                @Override
                public void run() {
                    mockSearchPhaseContext.sendSearchResponse(internalSearchResponse, null);
                }
            }
        );
        phase.run();
        mockSearchPhaseContext.assertNoFailure();
        assertNotNull(mockSearchPhaseContext.searchResponse.get());
    }

    public void testSkipExpandCollapseNoHits() throws IOException {
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
            @Override
            void sendExecuteMultiSearch(MultiSearchRequest request, SearchTask task, ActionListener<MultiSearchResponse> listener) {
                fail("expand should not try to send empty multi search request");
            }
        };
        mockSearchPhaseContext.getRequest()
            .source(
                new SearchSourceBuilder().collapse(
                    new CollapseBuilder("someField").setInnerHits(new InnerHitBuilder().setName("foobarbaz"))
                )
            );

        SearchHits hits = new SearchHits(new SearchHit[0], new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(hits, null, null, null, false, null, 1);
        ExpandSearchPhase phase = new ExpandSearchPhase(
            mockSearchPhaseContext,
            internalSearchResponse,
            null,
            () -> new SearchPhase("test") {
                @Override
                public void run() {
                    mockSearchPhaseContext.sendSearchResponse(internalSearchResponse, null);
                }
            }
        );
        phase.run();
        mockSearchPhaseContext.assertNoFailure();
        assertNotNull(mockSearchPhaseContext.searchResponse.get());
    }

    public void testExpandRequestOptions() throws IOException {
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        boolean version = randomBoolean();
        final boolean seqNoAndTerm = randomBoolean();
        SearchHit[] hits = new SearchHit[randomIntBetween(1, 10)];
        for (int i = 0; i < hits.length; i++) {
            hits[i] = new SearchHit(i, Integer.toString(i), Map.of("someField", new DocumentField("someField", List.of())), Map.of());
        }
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
        AtomicBoolean expanded = new AtomicBoolean();
        mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
            @Override
            void sendExecuteMultiSearch(MultiSearchRequest msearchRequest, SearchTask task, ActionListener<MultiSearchResponse> listener) {
                assertTrue(expanded.compareAndSet(false, true));
                assertThat(msearchRequest.requests(), hasSize(hits.length));
                assertThat(msearchRequest.maxConcurrentSearchRequests(), equalTo(3));
                for (SearchRequest r : msearchRequest.requests()) {
                    assertThat(r.preference(), equalTo("foobar"));
                    assertThat(r.routing(), equalTo("baz"));
                    assertThat(r.source().version(), equalTo(version));
                    assertThat(r.source().seqNoAndPrimaryTerm(), equalTo(seqNoAndTerm));
                    assertNull(r.source().fetchSource());
                    assertThat(r.source().postFilter(), equalTo(QueryBuilders.existsQuery("foo")));
                }
                MultiSearchResponse.Item[] responses = new MultiSearchResponse.Item[msearchRequest.requests().size()];
                for (int i = 0; i < responses.length; i++) {
                    InternalSearchResponse internalSearchResponse = new InternalSearchResponse(null, null, null, null, false, null, 1);
                    mockSearchPhaseContext.sendSearchResponse(internalSearchResponse, null);
                    responses[i] = new MultiSearchResponse.Item(mockSearchPhaseContext.searchResponse.get(), null);
                }
                listener.onResponse(new MultiSearchResponse(responses, randomIntBetween(1, 10000)));
            }
        };
        mockSearchPhaseContext.getRequest()
            .source(
                new SearchSourceBuilder().collapse(
                    new CollapseBuilder("someField").setInnerHits(
                        new InnerHitBuilder().setName("foobarbaz").setVersion(version).setSeqNoAndPrimaryTerm(seqNoAndTerm)
                    ).setMaxConcurrentGroupRequests(3)
                ).fetchSource(false).postFilter(QueryBuilders.existsQuery("foo"))
            )
            .preference("foobar")
            .routing("baz");

        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(searchHits, null, null, null, false, null, 1);
        ExpandSearchPhase phase = new ExpandSearchPhase(
            mockSearchPhaseContext,
            internalSearchResponse,
            null,
            () -> new SearchPhase("test") {
                @Override
                public void run() throws IOException {
                    mockSearchPhaseContext.sendSearchResponse(internalSearchResponse, null);
                }
            }
        );
        phase.run();
        assertTrue(expanded.get());
        mockSearchPhaseContext.assertNoFailure();
        assertNotNull(mockSearchPhaseContext.searchResponse.get());
    }

    public void testMaxConcurrentExpandRequests() {
        BiFunction<Integer, AtomicArray<SearchPhaseResult>, MultiSearchRequest> runExpandPhase = (searchThreadPoolSize, queryResults) -> {
            MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
            boolean version = randomBoolean();
            final boolean seqNoAndTerm = randomBoolean();
            SearchHit[] hits = new SearchHit[randomIntBetween(1, 10)];
            for (int i = 0; i < hits.length; i++) {
                hits[i] = new SearchHit(i, Integer.toString(i), Map.of("someField", new DocumentField("someField", List.of())), Map.of());
            }
            SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
            TestThreadPool testThreadPool = new TestThreadPool("test", Settings.builder().put("node.name", "node").build()) {
                @Override
                public Info info(String name) {
                    if (Names.SEARCH.equals(name)) {
                        return new Info(name, ThreadPoolType.FIXED, searchThreadPoolSize);
                    }
                    return super.info(name);
                }
            };
            AtomicReference<MultiSearchRequest> expandRequest = new AtomicReference<>();
            mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
                @Override
                void sendExecuteMultiSearch(
                    MultiSearchRequest msearchRequest,
                    SearchTask task,
                    ActionListener<MultiSearchResponse> listener
                ) {
                    assertTrue(expandRequest.compareAndSet(null, msearchRequest));
                    MultiSearchResponse.Item[] responses = new MultiSearchResponse.Item[msearchRequest.requests().size()];
                    for (int i = 0; i < responses.length; i++) {
                        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(null, null, null, null, false, null, 1);
                        mockSearchPhaseContext.sendSearchResponse(internalSearchResponse, null);
                        responses[i] = new MultiSearchResponse.Item(mockSearchPhaseContext.searchResponse.get(), null);
                    }
                    listener.onResponse(new MultiSearchResponse(responses, randomIntBetween(1, 10000)));
                }

                @Override
                public ThreadPool getThreadPool() {
                    return testThreadPool;
                }
            };
            CollapseBuilder collapseBuilder = new CollapseBuilder("someField").setInnerHits(
                new InnerHitBuilder().setName("foobarbaz").setVersion(version).setSeqNoAndPrimaryTerm(seqNoAndTerm)
            );
            mockSearchPhaseContext.getRequest()
                .source(new SearchSourceBuilder().collapse(collapseBuilder))
                .preference(randomAlphaOfLength(10));
            InternalSearchResponse internalSearchResponse = new InternalSearchResponse(searchHits, null, null, null, false, null, 1);
            ExpandSearchPhase phase = new ExpandSearchPhase(
                mockSearchPhaseContext,
                internalSearchResponse,
                queryResults,
                () -> new SearchPhase("test") {
                    @Override
                    public void run() throws IOException {
                        mockSearchPhaseContext.sendSearchResponse(internalSearchResponse, null);
                    }
                }
            );
            phase.run();
            assertNotNull(expandRequest.get());
            assertThat(expandRequest.get().requests(), hasSize(hits.length));
            mockSearchPhaseContext.assertNoFailure();
            assertNotNull(mockSearchPhaseContext.searchResponse.get());
            ThreadPool.terminate(testThreadPool, 1, TimeUnit.MINUTES);
            return expandRequest.get();
        };
        {
            Index index = new Index("test", "n/a");
            AtomicArray<SearchPhaseResult> queryResults = new AtomicArray<>(3);
            queryResults.set(0, new QuerySearchResult(null, new SearchShardTarget("node_1", new ShardId(index, 0), null), null));
            queryResults.set(1, new QuerySearchResult(null, new SearchShardTarget("node_2", new ShardId(index, 1), null), null));
            queryResults.set(2, new QuerySearchResult(null, new SearchShardTarget("node_3", new ShardId(index, 2), null), null));
            int searchThreadPoolSize = between(1, 9);
            MultiSearchRequest multiSearch = runExpandPhase.apply(searchThreadPoolSize, queryResults);
            assertThat(multiSearch.maxConcurrentSearchRequests(), equalTo(searchThreadPoolSize));

            searchThreadPoolSize = between(10, 1024);
            multiSearch = runExpandPhase.apply(searchThreadPoolSize, queryResults);
            assertThat("capped 10 shard requests per node", multiSearch.maxConcurrentSearchRequests(), equalTo(10));
        }
        {
            Index index = new Index("test", "n/a");
            AtomicArray<SearchPhaseResult> queryResults = new AtomicArray<>(5);
            queryResults.set(0, new QuerySearchResult(null, new SearchShardTarget("node_1", new ShardId(index, 0), null), null));
            queryResults.set(1, new QuerySearchResult(null, new SearchShardTarget("node_2", new ShardId(index, 1), null), null));
            queryResults.set(2, new QuerySearchResult(null, new SearchShardTarget("node_3", new ShardId(index, 2), null), null));
            queryResults.set(3, new QuerySearchResult(null, new SearchShardTarget("node_1", new ShardId(index, 3), null), null));
            queryResults.set(4, new QuerySearchResult(null, new SearchShardTarget("node_1", new ShardId(index, 4), null), null));
            int searchThreadPoolSize = between(1, 3);
            MultiSearchRequest multiSearch = runExpandPhase.apply(searchThreadPoolSize, queryResults);
            assertThat(multiSearch.maxConcurrentSearchRequests(), equalTo(1));

            searchThreadPoolSize = between(3, 9);
            multiSearch = runExpandPhase.apply(searchThreadPoolSize, queryResults);
            assertThat(multiSearch.maxConcurrentSearchRequests(), equalTo(searchThreadPoolSize / 3));

            searchThreadPoolSize = between(10, 1024);
            multiSearch = runExpandPhase.apply(searchThreadPoolSize, queryResults);
            assertThat("capped 10 shard requests per node", multiSearch.maxConcurrentSearchRequests(), equalTo(3));
        }
    }
}
