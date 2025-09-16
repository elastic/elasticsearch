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
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitTests;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.LookupField;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class FetchLookupFieldsPhaseTests extends ESTestCase {

    public void testNoLookupField() {
        MockSearchPhaseContext searchPhaseContext = new MockSearchPhaseContext(1);
        try {
            searchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
                @Override
                void sendExecuteMultiSearch(MultiSearchRequest request, SearchTask task, ActionListener<MultiSearchResponse> listener) {
                    throw new AssertionError("No lookup field");
                }
            };
            int numHits = randomIntBetween(0, 10);
            SearchHit[] searchHits = new SearchHit[randomIntBetween(0, 10)];
            for (int i = 0; i < searchHits.length; i++) {
                searchHits[i] = SearchHitTests.createTestItem(randomBoolean(), randomBoolean());
            }
            try (
                var sections = new SearchResponseSections(
                    new SearchHits(searchHits, new TotalHits(numHits, TotalHits.Relation.EQUAL_TO), 1.0f),
                    null,
                    null,
                    false,
                    null,
                    null,
                    1
                )
            ) {
                FetchLookupFieldsPhase phase = new FetchLookupFieldsPhase(searchPhaseContext, sections, null);
                phase.run();
            }
            searchPhaseContext.assertNoFailure();
            assertNotNull(searchPhaseContext.searchResponse.get());
        } finally {
            var resp = searchPhaseContext.searchResponse.get();
            if (resp != null) {
                resp.decRef();
            }
        }
    }

    public void testBasic() {
        MockSearchPhaseContext searchPhaseContext = new MockSearchPhaseContext(1);
        try {
            final AtomicBoolean requestSent = new AtomicBoolean();
            searchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
                @Override
                void sendExecuteMultiSearch(
                    MultiSearchRequest multiSearchRequest,
                    SearchTask task,
                    ActionListener<MultiSearchResponse> listener
                ) {
                    assertTrue(requestSent.compareAndSet(false, true));
                    // send 4 requests for term_1, term_2, term_3, and unknown
                    assertThat(multiSearchRequest.requests(), hasSize(4));
                    for (SearchRequest r : multiSearchRequest.requests()) {
                        assertNotNull(r.source());
                        assertThat(r.source().query(), instanceOf(TermQueryBuilder.class));
                        assertThat(r.source().size(), equalTo(1));
                    }
                    final List<String> queryTerms = multiSearchRequest.requests().stream().map(r -> {
                        final TermQueryBuilder query = (TermQueryBuilder) r.source().query();
                        return query.value().toString();
                    }).sorted().toList();
                    assertThat(queryTerms, equalTo(List.of("term_1", "term_2", "term_3", "xyz")));
                    final MultiSearchResponse.Item[] responses = new MultiSearchResponse.Item[multiSearchRequest.requests().size()];
                    for (int i = 0; i < responses.length; i++) {
                        final SearchRequest r = multiSearchRequest.requests().get(i);
                        final TermQueryBuilder query = (TermQueryBuilder) r.source().query();
                        final Map<String, List<Object>> fields = switch (query.value().toString()) {
                            case "term_1" -> Map.of("field_a", List.of("a1", "a2"), "field_b", List.of("b2"));
                            case "term_2" -> Map.of("field_a", List.of("a2", "a3"), "field_b", List.of("b1"));
                            case "term_3" -> Map.of("field_a", List.of("a2"), "field_b", List.of("b1", "b2"));
                            case "xyz" -> null;
                            default -> throw new AssertionError("unknown term value");
                        };
                        final SearchHits searchHits;
                        if (fields != null) {
                            final SearchHit hit = new SearchHit(randomInt(1000));
                            fields.forEach((f, values) -> hit.setDocumentField(new DocumentField(f, values, List.of())));
                            searchHits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
                        } else {
                            searchHits = SearchHits.empty(new TotalHits(0, TotalHits.Relation.EQUAL_TO), 1.0f);
                        }
                        responses[i] = new MultiSearchResponse.Item(
                            new SearchResponse(
                                searchHits,
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
                                randomNonNegativeLong(),
                                ShardSearchFailure.EMPTY_ARRAY,
                                SearchResponseTests.randomClusters(),
                                null
                            ),
                            null
                        );
                        searchHits.decRef();
                    }
                    ActionListener.respondAndRelease(listener, new MultiSearchResponse(responses, randomNonNegativeLong()));
                }
            };

            SearchHit leftHit0 = new SearchHit(randomInt(100));
            final List<FieldAndFormat> fetchFields = List.of(new FieldAndFormat(randomAlphaOfLength(10), null));
            {
                leftHit0.setDocumentField(
                    new DocumentField(
                        "lookup_field_1",
                        List.of(),
                        List.of(),
                        List.of(
                            new LookupField("test_index", new TermQueryBuilder("test_field", "term_1"), fetchFields, 1),
                            new LookupField("test_index", new TermQueryBuilder("test_field", "term_2"), fetchFields, 1)
                        )
                    )
                );
                leftHit0.setDocumentField(
                    new DocumentField(
                        "lookup_field_2",
                        List.of(),
                        List.of(),
                        List.of(new LookupField("test_index", new TermQueryBuilder("test_field", "term_2"), fetchFields, 1))
                    )
                );
            }

            SearchHit leftHit1 = new SearchHit(randomInt(100));
            {
                leftHit1.setDocumentField(
                    new DocumentField(
                        "lookup_field_2",
                        List.of(),
                        List.of(),
                        List.of(
                            new LookupField("test_index", new TermQueryBuilder("test_field", "term_2"), fetchFields, 1),
                            new LookupField("test_index", new TermQueryBuilder("test_field", "xyz"), fetchFields, 1)
                        )
                    )
                );
                leftHit1.setDocumentField(
                    new DocumentField(
                        "lookup_field_3",
                        List.of(),
                        List.of(),
                        List.of(new LookupField("test_index", new TermQueryBuilder("test_field", "term_3"), fetchFields, 1))
                    )
                );
            }
            try (
                var sections = new SearchResponseSections(
                    new SearchHits(new SearchHit[] { leftHit0, leftHit1 }, new TotalHits(2, TotalHits.Relation.EQUAL_TO), 1.0f),
                    null,
                    null,
                    false,
                    null,
                    null,
                    1
                )
            ) {
                FetchLookupFieldsPhase phase = new FetchLookupFieldsPhase(searchPhaseContext, sections, null);
                phase.run();
            }
            assertTrue(requestSent.get());
            searchPhaseContext.assertNoFailure();
            assertNotNull(searchPhaseContext.searchResponse.get());
            assertSame(searchPhaseContext.searchResponse.get().getHits().getHits()[0], leftHit0);
            assertSame(searchPhaseContext.searchResponse.get().getHits().getHits()[1], leftHit1);
            assertFalse(leftHit0.hasLookupFields());
            assertThat(
                leftHit0.field("lookup_field_1").getValues(),
                containsInAnyOrder(
                    Map.of("field_a", List.of("a1", "a2"), "field_b", List.of("b2")),
                    Map.of("field_a", List.of("a2", "a3"), "field_b", List.of("b1"))
                )
            );
            assertThat(
                leftHit0.field("lookup_field_2").getValues(),
                contains(Map.of("field_a", List.of("a2", "a3"), "field_b", List.of("b1")))
            );

            assertFalse(leftHit1.hasLookupFields());
            assertThat(
                leftHit1.field("lookup_field_2").getValues(),
                contains(Map.of("field_a", List.of("a2", "a3"), "field_b", List.of("b1")))
            );
            assertThat(
                leftHit1.field("lookup_field_3").getValues(),
                contains(Map.of("field_a", List.of("a2"), "field_b", List.of("b1", "b2")))
            );
        } finally {
            searchPhaseContext.results.close();
            var resp = searchPhaseContext.searchResponse.get();
            if (resp != null) {
                resp.decRef();
            }
        }
    }

}
