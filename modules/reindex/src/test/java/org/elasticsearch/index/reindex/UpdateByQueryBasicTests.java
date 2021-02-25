/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.hasSize;

public class UpdateByQueryBasicTests extends ReindexTestCase {
    public void testBasics() throws Exception {
        indexRandom(true, client().prepareIndex("test").setId("1").setSource("foo", "a"),
                client().prepareIndex("test").setId("2").setSource("foo", "a"),
                client().prepareIndex("test").setId("3").setSource("foo", "b"),
                client().prepareIndex("test").setId("4").setSource("foo", "c"));
        assertHitCount(client().prepareSearch("test").setSize(0).get(), 4);
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
        indexRandom(true,
            client().prepareIndex("test").setId("1").setSource("foo", "a"),
            client().prepareIndex("test").setId("2").setSource("foo", "a"),
            client().prepareIndex("test").setId("3").setSource("foo", "b"),
            client().prepareIndex("test").setId("4").setSource("foo", "c"));
        assertHitCount(client().prepareSearch("test").setSize(0).get(), 4);
        assertEquals(1, client().prepareGet("test", "1").get().getVersion());
        assertEquals(1, client().prepareGet("test", "4").get().getVersion());

        int slices = randomSlices(2, 10);
        int expectedSlices = expectedSliceStatuses(slices, "test");

        // Reindex all the docs
        assertThat(
            updateByQuery()
                .source("test")
                .refresh(true)
                .setSlices(slices).get(),
            matcher()
                .updated(4)
                .slices(hasSize(expectedSlices)));
        assertEquals(2, client().prepareGet("test", "1").get().getVersion());
        assertEquals(2, client().prepareGet("test", "4").get().getVersion());

        // Now none of them
        assertThat(
            updateByQuery()
                .source("test")
                .filter(termQuery("foo", "no_match"))
                .setSlices(slices)
                .refresh(true).get(),
            matcher()
                .updated(0)
                .slices(hasSize(expectedSlices)));
        assertEquals(2, client().prepareGet("test", "1").get().getVersion());
        assertEquals(2, client().prepareGet("test", "4").get().getVersion());

        // Now half of them
        assertThat(
            updateByQuery()
                .source("test")
                .filter(termQuery("foo", "a"))
                .refresh(true)
                .setSlices(slices).get(),
            matcher()
                .updated(2)
                .slices(hasSize(expectedSlices)));
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
                docs.get(indexName).add(client().prepareIndex(indexName).setId(Integer.toString(i)).setSource("foo", "a"));
            }
        }

        List<IndexRequestBuilder> allDocs = docs.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        indexRandom(true, allDocs);
        for (Map.Entry<String, List<IndexRequestBuilder>> entry : docs.entrySet()) {
            assertHitCount(client().prepareSearch(entry.getKey()).setSize(0).get(), entry.getValue().size());
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
        BulkByScrollResponse response = updateByQuery()
            .source("missing-index-*")
            .refresh(true)
            .setSlices(AbstractBulkByScrollRequest.AUTO_SLICES)
            .get();
        assertThat(response, matcher().updated(0).slices(hasSize(0)));
    }
}
