/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;

public class CollapseSearchResultsIT extends ESIntegTestCase {

    public void testCollapse() {
        final String indexName = "test_collapse";
        createIndex(indexName);
        final String collapseField = "collapse_field";
        assertAcked(indicesAdmin().preparePutMapping(indexName).setSource(collapseField, "type=keyword"));
        index(indexName, "id_1", Map.of(collapseField, "value1"));
        index(indexName, "id_2", Map.of(collapseField, "value2"));
        refresh(indexName);
        assertNoFailuresAndResponse(
            prepareSearch(indexName).setQuery(new MatchAllQueryBuilder())
                .setCollapse(new CollapseBuilder(collapseField).setInnerHits(new InnerHitBuilder("ih").setSize(2))),
            searchResponse -> {
                assertEquals(collapseField, searchResponse.getHits().getCollapseField());
                assertEquals(Set.of(new BytesRef("value1"), new BytesRef("value2")), Set.of(searchResponse.getHits().getCollapseValues()));
            }
        );
    }

    public void testCollapseWithDocValueFields() {
        final String indexName = "test_collapse";
        createIndex(indexName);
        final String collapseField = "collapse_field";
        final String otherField = "other_field";
        assertAcked(indicesAdmin().preparePutMapping(indexName).setSource(collapseField, "type=keyword", otherField, "type=keyword"));
        index(indexName, "id_1_0", Map.of(collapseField, "value1", otherField, "other_value1"));
        index(indexName, "id_1_1", Map.of(collapseField, "value1", otherField, "other_value2"));
        index(indexName, "id_2_0", Map.of(collapseField, "value2", otherField, "other_value3"));
        refresh(indexName);

        assertNoFailuresAndResponse(
            prepareSearch(indexName).setQuery(new MatchAllQueryBuilder())
                .addDocValueField(otherField)
                .setCollapse(new CollapseBuilder(collapseField).setInnerHits(new InnerHitBuilder("ih").setSize(2))),
            searchResponse -> {
                assertEquals(collapseField, searchResponse.getHits().getCollapseField());
                assertEquals(Set.of(new BytesRef("value1"), new BytesRef("value2")), Set.of(searchResponse.getHits().getCollapseValues()));
            }
        );
    }

    public void testCollapseWithFields() {
        final String indexName = "test_collapse";
        createIndex(indexName);
        final String collapseField = "collapse_field";
        final String otherField = "other_field";
        assertAcked(indicesAdmin().preparePutMapping(indexName).setSource(collapseField, "type=keyword", otherField, "type=keyword"));
        index(indexName, "id_1_0", Map.of(collapseField, "value1", otherField, "other_value1"));
        index(indexName, "id_1_1", Map.of(collapseField, "value1", otherField, "other_value2"));
        index(indexName, "id_2_0", Map.of(collapseField, "value2", otherField, "other_value3"));
        refresh(indexName);

        assertNoFailuresAndResponse(
            prepareSearch(indexName).setQuery(new MatchAllQueryBuilder())
                .setFetchSource(false)
                .addFetchField(otherField)
                .setCollapse(new CollapseBuilder(collapseField).setInnerHits(new InnerHitBuilder("ih").setSize(2))),
            searchResponse -> {
                assertEquals(collapseField, searchResponse.getHits().getCollapseField());
                assertEquals(Set.of(new BytesRef("value1"), new BytesRef("value2")), Set.of(searchResponse.getHits().getCollapseValues()));
            }
        );
    }

    public void testCollapseWithStoredFields() {
        final String indexName = "test_collapse";
        createIndex(indexName);
        final String collapseField = "collapse_field";
        assertAcked(indicesAdmin().preparePutMapping(indexName).setSource("""
                     {
                       "dynamic": "strict",
                       "properties": {
                         "collapse_field":  { "type": "keyword", "store": true },
                         "ts":  { "type": "date", "store": true }
                       }
                     }
            """, XContentType.JSON));
        index(indexName, "id_1_0", Map.of(collapseField, "value1", "ts", 0));
        index(indexName, "id_1_1", Map.of(collapseField, "value1", "ts", 1));
        index(indexName, "id_2_0", Map.of(collapseField, "value2", "ts", 2));
        refresh(indexName);

        assertNoFailuresAndResponse(
            prepareSearch(indexName).setQuery(new MatchAllQueryBuilder())
                .setFetchSource(false)
                .storedFields("*")
                .setCollapse(new CollapseBuilder(collapseField)),
            searchResponse -> {
                assertEquals(collapseField, searchResponse.getHits().getCollapseField());
            }
        );
    }

    public void testCollapseOnMixedIntAndLongSortTypes() {
        assertAcked(
            prepareCreate("shop_short").setMapping("brand_id", "type=short", "price", "type=integer"),
            prepareCreate("shop_long").setMapping("brand_id", "type=long", "price", "type=integer"),
            prepareCreate("shop_int").setMapping("brand_id", "type=integer", "price", "type=integer")
        );

        BulkRequestBuilder bulkRequest = client().prepareBulk();
        bulkRequest.add(client().prepareIndex("shop_short").setId("short01").setSource("brand_id", 1, "price", 100));
        bulkRequest.add(client().prepareIndex("shop_short").setId("short02").setSource("brand_id", 1, "price", 101));
        bulkRequest.add(client().prepareIndex("shop_short").setId("short03").setSource("brand_id", 1, "price", 102));
        bulkRequest.add(client().prepareIndex("shop_short").setId("short04").setSource("brand_id", 3, "price", 301));
        bulkRequest.get();

        BulkRequestBuilder bulkRequest1 = client().prepareBulk();
        bulkRequest1.add(client().prepareIndex("shop_long").setId("long01").setSource("brand_id", 1, "price", 100));
        bulkRequest1.add(client().prepareIndex("shop_long").setId("long02").setSource("brand_id", 1, "price", 103));
        bulkRequest1.add(client().prepareIndex("shop_long").setId("long03").setSource("brand_id", 1, "price", 105));
        bulkRequest1.add(client().prepareIndex("shop_long").setId("long04").setSource("brand_id", 2, "price", 200));
        bulkRequest1.add(client().prepareIndex("shop_long").setId("long05").setSource("brand_id", 2, "price", 201));
        bulkRequest1.get();

        BulkRequestBuilder bulkRequest2 = client().prepareBulk();
        bulkRequest2.add(client().prepareIndex("shop_int").setId("int01").setSource("brand_id", 1, "price", 101));
        bulkRequest2.add(client().prepareIndex("shop_int").setId("int02").setSource("brand_id", 1, "price", 102));
        bulkRequest2.add(client().prepareIndex("shop_int").setId("int03").setSource("brand_id", 1, "price", 104));
        bulkRequest2.add(client().prepareIndex("shop_int").setId("int04").setSource("brand_id", 2, "price", 202));
        bulkRequest2.add(client().prepareIndex("shop_int").setId("int05").setSource("brand_id", 2, "price", 203));
        bulkRequest2.add(client().prepareIndex("shop_int").setId("int06").setSource("brand_id", 3, "price", 300));
        bulkRequest2.get();
        refresh();

        assertNoFailuresAndResponse(
            prepareSearch("shop_long", "shop_int", "shop_short").setQuery(new MatchAllQueryBuilder())
                .setCollapse(
                    new CollapseBuilder("brand_id").setInnerHits(
                        new InnerHitBuilder("ih").setSize(3).addSort(SortBuilders.fieldSort("price").order(SortOrder.DESC))
                    )
                )
                .addSort("brand_id", SortOrder.ASC)
                .addSort("price", SortOrder.DESC),
            response -> {
                SearchHits hits = response.getHits();
                assertEquals(3, hits.getHits().length);

                // First hit should be brand_id=1 with highest price
                Map<String, Object> firstHitSource = hits.getAt(0).getSourceAsMap();
                assertEquals(1, firstHitSource.get("brand_id"));
                assertEquals(105, firstHitSource.get("price"));
                assertEquals("long03", hits.getAt(0).getId());

                // Check inner hits for brand_id=1
                SearchHits innerHits1 = hits.getAt(0).getInnerHits().get("ih");
                assertEquals(3, innerHits1.getHits().length);
                assertEquals("long03", innerHits1.getAt(0).getId());
                assertEquals("int03", innerHits1.getAt(1).getId());
                assertEquals("long02", innerHits1.getAt(2).getId());

                // Second hit should be brand_id=2 with highest price
                Map<String, Object> secondHitSource = hits.getAt(1).getSourceAsMap();
                assertEquals(2, secondHitSource.get("brand_id"));
                assertEquals(203, secondHitSource.get("price"));
                assertEquals("int05", hits.getAt(1).getId());

                // Check inner hits for brand_id=2
                SearchHits innerHits2 = hits.getAt(1).getInnerHits().get("ih");
                assertEquals(3, innerHits2.getHits().length);
                assertEquals("int05", innerHits2.getAt(0).getId());
                assertEquals("int04", innerHits2.getAt(1).getId());
                assertEquals("long05", innerHits2.getAt(2).getId());

                // third hit should be brand_id=3 with highest price
                Map<String, Object> thirdHitSource = hits.getAt(2).getSourceAsMap();
                assertEquals(3, thirdHitSource.get("brand_id"));
                assertEquals(301, thirdHitSource.get("price"));
                assertEquals("short04", hits.getAt(2).getId());

                // Check inner hits for brand_id=3
                SearchHits innerHits3 = hits.getAt(2).getInnerHits().get("ih");
                assertEquals(2, innerHits3.getHits().length);
                assertEquals("short04", innerHits3.getAt(0).getId());
                assertEquals("int06", innerHits3.getAt(1).getId());
            }
        );
    }
}
