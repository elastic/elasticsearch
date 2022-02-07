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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitTests;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.LookupField;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;

public class FetchLookupFieldsPhaseTests extends ESTestCase {

    public void testNoLookupField() {
        MockSearchPhaseContext searchPhaseContext = new MockSearchPhaseContext(1);
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
        SearchHits hits = new SearchHits(searchHits, new TotalHits(numHits, TotalHits.Relation.EQUAL_TO), 1.0f);
        InternalSearchResponse searchResponse = new InternalSearchResponse(hits, null, null, null, false, null, 1);
        FetchLookupFieldsPhase phase = new FetchLookupFieldsPhase(searchPhaseContext, searchResponse, null);
        phase.run();
        searchPhaseContext.assertNoFailure();
        assertNotNull(searchPhaseContext.searchResponse.get());
    }

    public void testRandom() {
        final Map<String, Map<String, List<Object>>> lookupValues = new HashMap<>();
        final int numKeys = randomIntBetween(1, 100);
        for (int i = 0; i < numKeys; i++) {
            final Map<String, List<Object>> values = new HashMap<>();
            final int numValues = randomIntBetween(1, 5);
            for (int j = 0; j < numValues; j++) {
                values.put("external_field_" + randomInt(100), randomList(1, 5, () -> randomAlphaOfLength(5)));
            }
            lookupValues.put("key_" + i, values);
        }

        final MockSearchPhaseContext searchPhaseContext = new MockSearchPhaseContext(1);
        final AtomicBoolean sentRequest = new AtomicBoolean();
        final Map<String, List<String>> lookupKeys = new HashMap<>();
        searchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
            @Override
            void sendExecuteMultiSearch(MultiSearchRequest multiSearch, SearchTask task, ActionListener<MultiSearchResponse> listener) {
                assertTrue(sentRequest.compareAndSet(false, true));
                assertFalse(lookupKeys.isEmpty());
                MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[multiSearch.requests().size()];
                for (int i = 0; i < multiSearch.requests().size(); i++) {
                    long tookInMillis = randomNonNegativeLong();
                    int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
                    int successfulShards = randomIntBetween(0, totalShards);
                    int skippedShards = totalShards - successfulShards;
                    SearchResponse.Clusters clusters = SearchResponseTests.randomClusters();
                    TermQueryBuilder termQuery = (TermQueryBuilder) multiSearch.requests().get(i).source().query();
                    Map<String, List<Object>> values = lookupValues.get((String) termQuery.value());
                    SearchHit hit = new SearchHit(randomIntBetween(0, 1000));
                    for (Map.Entry<String, List<Object>> e : values.entrySet()) {
                        hit.setDocumentField(e.getKey(), new DocumentField(e.getKey(), e.getValue(), List.of()));
                    }
                    SearchHits hits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
                    InternalSearchResponse internalSearchResponse = new InternalSearchResponse(hits, null, null, null, false, null, 1);
                    SearchResponse searchResponse = new SearchResponse(
                        internalSearchResponse,
                        null,
                        totalShards,
                        successfulShards,
                        skippedShards,
                        tookInMillis,
                        ShardSearchFailure.EMPTY_ARRAY,
                        clusters,
                        null
                    );
                    items[i] = new MultiSearchResponse.Item(searchResponse, null);
                }
                listener.onResponse(new MultiSearchResponse(items, randomNonNegativeLong()));
            }
        };
        int numHits = randomIntBetween(0, 10);
        SearchHit[] searchHits = new SearchHit[randomIntBetween(1, 10)];
        for (int i = 0; i < searchHits.length; i++) {
            SearchHit hit = SearchHitTests.createTestItem(randomBoolean(), randomBoolean());
            int numFields = randomIntBetween(0, 5);
            for (int j = 0; j < numFields; j++) {
                String fieldName = "lookup_field_" + i + "_" + j;
                List<String> keys = randomSubsetOf(lookupKeys.keySet());
                if (keys.isEmpty() == false) {
                    lookupKeys.put(fieldName, keys);
                    List<LookupField> fields = keys.stream().map(k -> {
                        String lookupIndex = "test-lookup";
                        QueryBuilder query = new TermQueryBuilder("target_field", randomFrom(lookupValues.keySet()));
                        List<FieldAndFormat> fetchFields = randomList(1, 2, () -> new FieldAndFormat("field-*", null));
                        return new LookupField(lookupIndex, query, fetchFields);
                    }).toList();
                    hit.setDocumentField(fieldName, new DocumentField(fieldName, List.of(), List.of(), fields));
                }
            }
            searchHits[i] = hit;
        }
        SearchHits hits = new SearchHits(searchHits, new TotalHits(numHits, TotalHits.Relation.EQUAL_TO), 1.0f);
        InternalSearchResponse searchResponse = new InternalSearchResponse(hits, null, null, null, false, null, 1);
        FetchLookupFieldsPhase phase = new FetchLookupFieldsPhase(searchPhaseContext, searchResponse, null);
        phase.run();
        searchPhaseContext.assertNoFailure();
        assertNotNull(searchPhaseContext.searchResponse.get());
        if (lookupKeys.isEmpty()) {
            assertFalse(sentRequest.get());
        } else {
            assertTrue(sentRequest.get());
        }
        for (SearchHit hit : searchHits) {
            assertFalse(hit.hasLookupFields());
        }
        for (SearchHit hit : searchHits) {
            for (DocumentField doc : hit.getDocumentFields().values()) {
                if (doc.getName().startsWith("lookup_field_")) {
                    final List<Map<String, List<Object>>> expectedValues = lookupKeys.get(doc.getName())
                        .stream()
                        .map(lookupValues::get)
                        .toList();
                    assertThat(doc.getValues(), equalTo(expectedValues));
                }
            }
        }
    }
}
