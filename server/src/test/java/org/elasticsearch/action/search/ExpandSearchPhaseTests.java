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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.AbstractSearchTestCase;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;

public class ExpandSearchPhaseTests extends ESTestCase {

    public void testCollapseSingleHit() throws IOException {
        final int iters = randomIntBetween(5, 10);
        for (int i = 0; i < iters; i++) {
            final int numInnerHits = randomIntBetween(1, 5);
            List<SearchHits> collapsedHits = new ArrayList<>(numInnerHits);
            for (int innerHitNum = 0; innerHitNum < numInnerHits; innerHitNum++) {
                SearchHits hits = SearchHits.unpooled(
                    new SearchHit[] { SearchHit.unpooled(innerHitNum, "ID"), SearchHit.unpooled(innerHitNum + 1, "ID") },
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    1.0F
                );
                collapsedHits.add(hits);
            }

            AtomicBoolean executedMultiSearch = new AtomicBoolean(false);
            QueryBuilder originalQuery = randomBoolean() ? null : QueryBuilders.termQuery("foo", "bar");
            Map<String, Object> runtimeMappings = randomBoolean() ? emptyMap() : AbstractSearchTestCase.randomRuntimeMappings();

            final MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
            try {
                String collapseValue = randomBoolean() ? null : "boom";

                mockSearchPhaseContext.getRequest()
                    .source(
                        new SearchSourceBuilder().collapse(
                            new CollapseBuilder("someField").setInnerHits(
                                IntStream.range(0, numInnerHits)
                                    .mapToObj(hitNum -> new InnerHitBuilder().setName("innerHit" + hitNum))
                                    .toList()
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
                            var sections = new SearchResponseSections(collapsedHits.get(innerHitNum), null, null, false, null, null, 1);
                            try {
                                mockSearchPhaseContext.sendSearchResponse(sections, null);
                            } finally {
                                sections.decRef();
                            }
                            mSearchResponses.add(new MultiSearchResponse.Item(mockSearchPhaseContext.searchResponse.get(), null));
                            // transferring ownership to the multi-search response so no need to release here
                            mockSearchPhaseContext.searchResponse.set(null);
                        }

                        ActionListener.respondAndRelease(
                            listener,
                            new MultiSearchResponse(mSearchResponses.toArray(new MultiSearchResponse.Item[0]), randomIntBetween(1, 10000))
                        );
                    }
                };

                SearchHit hit = new SearchHit(1, "ID");
                hit.setDocumentField("someField", new DocumentField("someField", Collections.singletonList(collapseValue)));
                SearchHits hits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0F);
                try {
                    ExpandSearchPhase phase = new ExpandSearchPhase(mockSearchPhaseContext, hits, () -> new SearchPhase("test") {
                        @Override
                        public void run() {
                            var sections = new SearchResponseSections(hits, null, null, false, null, null, 1);
                            try {
                                mockSearchPhaseContext.sendSearchResponse(sections, null);
                            } finally {
                                sections.decRef();
                            }
                        }
                    });

                    phase.run();
                    mockSearchPhaseContext.assertNoFailure();
                    SearchResponse theResponse = mockSearchPhaseContext.searchResponse.get();
                    assertNotNull(theResponse);
                    assertEquals(numInnerHits, theResponse.getHits().getHits()[0].getInnerHits().size());

                    for (int innerHitNum = 0; innerHitNum < numInnerHits; innerHitNum++) {
                        assertSame(
                            theResponse.getHits().getHits()[0].getInnerHits().get("innerHit" + innerHitNum),
                            collapsedHits.get(innerHitNum)
                        );
                    }

                    assertTrue(executedMultiSearch.get());
                } finally {
                    hits.decRef();
                }
            } finally {
                mockSearchPhaseContext.execute(() -> {});
                var resp = mockSearchPhaseContext.searchResponse.get();
                if (resp != null) {
                    resp.decRef();
                }

            }
        }
    }

    public void testFailOneItemFailsEntirePhase() throws IOException {
        AtomicBoolean executedMultiSearch = new AtomicBoolean(false);

        SearchHits collapsedHits = new SearchHits(
            new SearchHit[] { new SearchHit(2, "ID"), new SearchHit(3, "ID") },
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
                SearchResponse searchResponse = new SearchResponse(
                    collapsedHits,
                    null,
                    null,
                    false,
                    null,
                    null,
                    1,
                    null,
                    1,
                    1,
                    0,
                    0,
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponse.Clusters.EMPTY
                );
                ActionListener.respondAndRelease(
                    listener,
                    new MultiSearchResponse(
                        new MultiSearchResponse.Item[] {
                            new MultiSearchResponse.Item(null, new RuntimeException("boom")),
                            new MultiSearchResponse.Item(searchResponse, null) },
                        randomIntBetween(1, 10000)
                    )
                );
            }
        };

        SearchHit hit1 = new SearchHit(1, "ID");
        hit1.setDocumentField("someField", new DocumentField("someField", Collections.singletonList(collapseValue)));
        SearchHit hit2 = new SearchHit(2, "ID2");
        hit2.setDocumentField("someField", new DocumentField("someField", Collections.singletonList(collapseValue)));
        SearchHits hits = new SearchHits(new SearchHit[] { hit1, hit2 }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0F);
        try {
            ExpandSearchPhase phase = new ExpandSearchPhase(mockSearchPhaseContext, hits, () -> new SearchPhase("test") {
                @Override
                public void run() {
                    var sections = new SearchResponseSections(hits, null, null, false, null, null, 1);
                    try {
                        mockSearchPhaseContext.sendSearchResponse(sections, null);
                    } finally {
                        sections.decRef();
                    }
                }
            });
            phase.run();
            assertThat(mockSearchPhaseContext.phaseFailure.get(), Matchers.instanceOf(RuntimeException.class));
            assertEquals("boom", mockSearchPhaseContext.phaseFailure.get().getMessage());
            assertNotNull(mockSearchPhaseContext.phaseFailure.get());
            assertNull(mockSearchPhaseContext.searchResponse.get());
        } finally {
            mockSearchPhaseContext.execute(() -> {});
            hits.decRef();
            collapsedHits.decRef();
        }
    }

    public void testSkipPhase() throws IOException {
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        try {
            mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
                @Override
                void sendExecuteMultiSearch(MultiSearchRequest request, SearchTask task, ActionListener<MultiSearchResponse> listener) {
                    fail("no collapsing here");
                }
            };

            SearchHit hit1 = new SearchHit(1, "ID");
            hit1.setDocumentField("someField", new DocumentField("someField", Collections.singletonList(null)));
            SearchHit hit2 = new SearchHit(2, "ID2");
            hit2.setDocumentField("someField", new DocumentField("someField", Collections.singletonList(null)));
            SearchHits hits = new SearchHits(new SearchHit[] { hit1, hit2 }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0F);
            try {
                ExpandSearchPhase phase = new ExpandSearchPhase(mockSearchPhaseContext, hits, () -> new SearchPhase("test") {
                    @Override
                    public void run() {
                        var sections = new SearchResponseSections(hits, null, null, false, null, null, 1);
                        try {
                            mockSearchPhaseContext.sendSearchResponse(sections, null);
                        } finally {
                            sections.decRef();
                        }
                    }
                });
                phase.run();
                mockSearchPhaseContext.assertNoFailure();
                assertNotNull(mockSearchPhaseContext.searchResponse.get());
            } finally {
                hits.decRef();
            }
        } finally {
            mockSearchPhaseContext.execute(() -> {});
            var resp = mockSearchPhaseContext.searchResponse.get();
            if (resp != null) {
                resp.decRef();
            }
        }
    }

    public void testSkipExpandCollapseNoHits() throws IOException {
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        try {
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

            SearchHits hits = SearchHits.empty(new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
            ExpandSearchPhase phase = new ExpandSearchPhase(mockSearchPhaseContext, hits, () -> new SearchPhase("test") {
                @Override
                public void run() {
                    mockSearchPhaseContext.sendSearchResponse(new SearchResponseSections(hits, null, null, false, null, null, 1), null);
                }
            });
            phase.run();
            mockSearchPhaseContext.assertNoFailure();
            assertNotNull(mockSearchPhaseContext.searchResponse.get());
        } finally {
            var resp = mockSearchPhaseContext.searchResponse.get();
            if (resp != null) {
                resp.decRef();
            }
        }
    }

    public void testExpandRequestOptions() throws IOException {
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        try {
            boolean version = randomBoolean();
            final boolean seqNoAndTerm = randomBoolean();

            mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
                @Override
                void sendExecuteMultiSearch(MultiSearchRequest request, SearchTask task, ActionListener<MultiSearchResponse> listener) {
                    final QueryBuilder postFilter = QueryBuilders.existsQuery("foo");
                    assertTrue(request.requests().stream().allMatch((r) -> "foobar".equals(r.preference())));
                    assertTrue(request.requests().stream().allMatch((r) -> "baz".equals(r.routing())));
                    assertTrue(request.requests().stream().allMatch((r) -> version == r.source().version()));
                    assertTrue(request.requests().stream().allMatch((r) -> seqNoAndTerm == r.source().seqNoAndPrimaryTerm()));
                    assertTrue(request.requests().stream().allMatch((r) -> postFilter.equals(r.source().postFilter())));
                    assertTrue(request.requests().stream().allMatch((r) -> r.source().fetchSource() == null));
                }
            };
            mockSearchPhaseContext.getRequest()
                .source(
                    new SearchSourceBuilder().collapse(
                        new CollapseBuilder("someField").setInnerHits(
                            new InnerHitBuilder().setName("foobarbaz").setVersion(version).setSeqNoAndPrimaryTerm(seqNoAndTerm)
                        )
                    ).fetchSource(false).postFilter(QueryBuilders.existsQuery("foo"))
                )
                .preference("foobar")
                .routing("baz");

            SearchHit hit = new SearchHit(1, "ID");
            hit.setDocumentField("someField", new DocumentField("someField", Collections.singletonList("foo")));
            SearchHits hits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0F);
            try {
                ExpandSearchPhase phase = new ExpandSearchPhase(mockSearchPhaseContext, hits, () -> new SearchPhase("test") {
                    @Override
                    public void run() {
                        mockSearchPhaseContext.sendSearchResponse(new SearchResponseSections(hits, null, null, false, null, null, 1), null);
                    }
                });
                phase.run();
                mockSearchPhaseContext.assertNoFailure();
            } finally {
                hits.decRef();
            }
        } finally {
            var resp = mockSearchPhaseContext.searchResponse.get();
            if (resp != null) {
                resp.decRef();
            }
        }
    }

    public void testExpandSearchRespectsOriginalPIT() {
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        final PointInTimeBuilder pit = new PointInTimeBuilder(new BytesArray("foo"));
        try {
            boolean version = randomBoolean();
            final boolean seqNoAndTerm = randomBoolean();

            mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
                @Override
                void sendExecuteMultiSearch(MultiSearchRequest request, SearchTask task, ActionListener<MultiSearchResponse> listener) {
                    assertTrue(request.requests().stream().allMatch((r) -> r.preference() == null));
                    assertTrue(request.requests().stream().allMatch((r) -> r.indices() == Strings.EMPTY_ARRAY));
                    assertTrue(request.requests().stream().allMatch((r) -> r.source().pointInTimeBuilder().equals(pit)));
                }
            };
            mockSearchPhaseContext.getRequest()
                .source(
                    new SearchSourceBuilder().collapse(
                        new CollapseBuilder("someField").setInnerHits(
                            new InnerHitBuilder().setName("foobarbaz").setVersion(version).setSeqNoAndPrimaryTerm(seqNoAndTerm)
                        )
                    ).fetchSource(false).postFilter(QueryBuilders.existsQuery("foo")).pointInTimeBuilder(pit)
                )
                .routing("baz");

            SearchHit hit = new SearchHit(1, "ID");
            hit.setDocumentField("someField", new DocumentField("someField", Collections.singletonList("foo")));
            SearchHits hits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0F);
            try {
                ExpandSearchPhase phase = new ExpandSearchPhase(mockSearchPhaseContext, hits, () -> new SearchPhase("test") {
                    @Override
                    public void run() {
                        mockSearchPhaseContext.sendSearchResponse(
                            new SearchResponseSections(hits, null, null, false, null, null, 1),
                            new AtomicArray<>(0)
                        );
                    }
                });
                phase.run();
                mockSearchPhaseContext.assertNoFailure();
            } finally {
                hits.decRef();
            }
        } finally {
            var resp = mockSearchPhaseContext.searchResponse.get();
            if (resp != null) {
                resp.decRef();
            }
        }
    }
}
