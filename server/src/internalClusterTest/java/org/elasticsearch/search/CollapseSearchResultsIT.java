/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.test.ESIntegTestCase;

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
}
