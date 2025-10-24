/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class UpdateByQueryBasicTests extends ReindexTestCase {
    public void testBasics() throws Exception {
        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("foo", "a"),
            prepareIndex("test").setId("2").setSource("foo", "a"),
            prepareIndex("test").setId("3").setSource("foo", "b"),
            prepareIndex("test").setId("4").setSource("foo", "c")
        );
        assertHitCount(prepareSearch("test").setSize(0), 4);
        assertEquals(1, client().prepareGet("test", "1").get().getVersion());
        assertEquals(1, client().prepareGet("test", "4").get().getVersion());

        // Reindex all the docs
        assertThat(updateByQuery().source("test").refresh(true).get(), matcher().updated(4));
        assertEquals(2, client().prepareGet("test", "1").get().getVersion());
        assertEquals(2, client().prepareGet("test", "4").get().getVersion());

        // Now none of them
        assertThat(updateByQuery().source("test").filter(termQuery("foo", "no_match")).refresh(true).get(), matcher().updated(0));
        assertEquals(2, client().prepareGet("test", "1").get().getVersion());
        assertEquals(2, client().prepareGet("test", "4").get().getVersion());

        // Now half of them
        assertThat(updateByQuery().source("test").filter(termQuery("foo", "a")).refresh(true).get(), matcher().updated(2));
        assertEquals(3, client().prepareGet("test", "1").get().getVersion());
        assertEquals(3, client().prepareGet("test", "2").get().getVersion());
        assertEquals(2, client().prepareGet("test", "3").get().getVersion());
        assertEquals(2, client().prepareGet("test", "4").get().getVersion());

        // Limit with size
        UpdateByQueryRequestBuilder request = updateByQuery().source("test").size(3).refresh(true);
        request.source().addSort("foo.keyword", SortOrder.ASC);
        assertThat(request.get(), matcher().updated(3));
        // Only the first three documents are updated because of sort
        assertEquals(4, client().prepareGet("test", "1").get().getVersion());
        assertEquals(4, client().prepareGet("test", "2").get().getVersion());
        assertEquals(3, client().prepareGet("test", "3").get().getVersion());
        assertEquals(2, client().prepareGet("test", "4").get().getVersion());
    }

    public void testSlices() throws Exception {
        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("foo", "a"),
            prepareIndex("test").setId("2").setSource("foo", "a"),
            prepareIndex("test").setId("3").setSource("foo", "b"),
            prepareIndex("test").setId("4").setSource("foo", "c")
        );
        assertHitCount(prepareSearch("test").setSize(0), 4);
        assertEquals(1, client().prepareGet("test", "1").get().getVersion());
        assertEquals(1, client().prepareGet("test", "4").get().getVersion());

        int slices = randomSlices(2, 10);
        int expectedSlices = expectedSliceStatuses(slices, "test");

        // Reindex all the docs
        assertThat(
            updateByQuery().source("test").refresh(true).setSlices(slices).get(),
            matcher().updated(4).slices(hasSize(expectedSlices))
        );
        assertEquals(2, client().prepareGet("test", "1").get().getVersion());
        assertEquals(2, client().prepareGet("test", "4").get().getVersion());

        // Now none of them
        assertThat(
            updateByQuery().source("test").filter(termQuery("foo", "no_match")).setSlices(slices).refresh(true).get(),
            matcher().updated(0).slices(hasSize(expectedSlices))
        );
        assertEquals(2, client().prepareGet("test", "1").get().getVersion());
        assertEquals(2, client().prepareGet("test", "4").get().getVersion());

        // Now half of them
        assertThat(
            updateByQuery().source("test").filter(termQuery("foo", "a")).refresh(true).setSlices(slices).get(),
            matcher().updated(2).slices(hasSize(expectedSlices))
        );
        assertEquals(3, client().prepareGet("test", "1").get().getVersion());
        assertEquals(3, client().prepareGet("test", "2").get().getVersion());
        assertEquals(2, client().prepareGet("test", "3").get().getVersion());
        assertEquals(2, client().prepareGet("test", "4").get().getVersion());
    }

    public void testMultipleSources() throws Exception {
        int sourceIndices = between(2, 5);

        Map<String, List<IndexRequestBuilder>> docs = new HashMap<>();
        for (int sourceIndex = 0; sourceIndex < sourceIndices; sourceIndex++) {
            String indexName = "test" + sourceIndex;
            docs.put(indexName, new ArrayList<>());
            int numDocs = between(5, 15);
            for (int i = 0; i < numDocs; i++) {
                docs.get(indexName).add(prepareIndex(indexName).setId(Integer.toString(i)).setSource("foo", "a"));
            }
        }

        List<IndexRequestBuilder> allDocs = docs.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        indexRandom(true, allDocs);
        for (Map.Entry<String, List<IndexRequestBuilder>> entry : docs.entrySet()) {
            assertHitCount(prepareSearch(entry.getKey()).setSize(0), entry.getValue().size());
        }

        int slices = randomSlices(1, 10);
        int expectedSlices = expectedSliceStatuses(slices, docs.keySet());

        String[] sourceIndexNames = docs.keySet().toArray(new String[docs.size()]);
        BulkByScrollResponse response = updateByQuery().source(sourceIndexNames).refresh(true).setSlices(slices).get();
        assertThat(response, matcher().updated(allDocs.size()).slices(hasSize(expectedSlices)));

        for (Map.Entry<String, List<IndexRequestBuilder>> entry : docs.entrySet()) {
            String index = entry.getKey();
            List<IndexRequestBuilder> indexDocs = entry.getValue();
            int randomDoc = between(0, indexDocs.size() - 1);
            assertEquals(2, client().prepareGet(index, Integer.toString(randomDoc)).get().getVersion());
        }
    }

    public void testMissingSources() {
        BulkByScrollResponse response = updateByQuery().source("missing-index-*")
            .refresh(true)
            .setSlices(AbstractBulkByScrollRequest.AUTO_SLICES)
            .get();
        assertThat(response, matcher().updated(0).slices(hasSize(0)));
    }

    public void testUpdateByQueryIncludeVectors() throws Exception {
        var resp1 = prepareCreate("test").setSettings(
            Settings.builder().put(IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey(), true).build()
        ).setMapping("foo", "type=dense_vector,similarity=l2_norm", "bar", "type=sparse_vector").get();
        assertAcked(resp1);

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("foo", List.of(3.0f, 2.0f, 1.5f), "bar", Map.of("token_1", 4f, "token_2", 7f))
        );

        var searchResponse = prepareSearch("test").get();
        try {
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(1));
            var sourceMap = searchResponse.getHits().getAt(0).getSourceAsMap();
            assertThat(sourceMap.size(), equalTo(0));
        } finally {
            searchResponse.decRef();
        }

        // Copy all the docs
        var updateByQueryResponse = updateByQuery().source("test").refresh(true).get();
        assertThat(updateByQueryResponse, matcher().updated(1L));

        searchResponse = prepareSearch("test").get();
        try {
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(1));
            var sourceMap = searchResponse.getHits().getAt(0).getSourceAsMap();
            assertThat(sourceMap.size(), equalTo(0));
        } finally {
            searchResponse.decRef();
        }

        searchResponse = prepareSearch("test").setExcludeVectors(false).get();
        try {
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(1));
            var sourceMap = searchResponse.getHits().getAt(0).getSourceAsMap();
            assertThat(sourceMap.get("foo"), anyOf(equalTo(List.of(3f, 2f, 1.5f)), equalTo(List.of(3d, 2d, 1.5d))));
            assertThat(
                sourceMap.get("bar"),
                anyOf(equalTo(Map.of("token_1", 4f, "token_2", 7f)), equalTo(Map.of("token_1", 4d, "token_2", 7d)))
            );
        } finally {
            searchResponse.decRef();
        }
    }
}
