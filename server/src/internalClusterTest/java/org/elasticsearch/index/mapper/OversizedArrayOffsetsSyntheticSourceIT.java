/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end check of the {@code _ignored_source} fallback for arrays whose encoded offsets are too large for a sorted doc
 * value, on a standard index with synthetic source and {@code synthetic_source_keep: arrays}. Unlike the mapper-level tests,
 * documents go through flush, optionally an index sort and a force merge, so the {@code _ignored_source} entry must stay with
 * its document when segments are sorted and merged.
 */
public class OversizedArrayOffsetsSyntheticSourceIT extends ESSingleNodeTestCase {

    public void testOversizedArraysSurviveSortAndMerge() throws Exception {
        final String index = "test-oversized-offsets";
        final boolean indexSort = randomBoolean();
        Settings.Builder settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "synthetic")
            .put(Mapper.SYNTHETIC_SOURCE_KEEP_INDEX_SETTING.getKey(), "arrays");
        if (indexSort) {
            settings.put("index.sort.field", "sort_key").put("index.sort.order", "desc");
        }
        assertAcked(indicesAdmin().prepareCreate(index).setSettings(settings).setMapping("""
            {
              "properties": {
                "sort_key": { "type": "long" },
                "kw": { "type": "keyword" },
                "n": { "type": "long" }
              }
            }"""));

        // Regular documents with small arrays, which get native offsets, and one or two documents with an oversized array,
        // which fall back to _ignored_source. All arrays have out-of-order values, duplicates and nulls.
        final Map<String, Map<String, Object>> documents = new LinkedHashMap<>();
        final int numDocs = between(3, 10);
        for (int i = 0; i < numDocs; i++) {
            Map<String, Object> document = new LinkedHashMap<>();
            document.put("sort_key", i);
            document.put("kw", Arrays.asList("b", "a", null, "b", "doc-" + i));
            document.put("n", Arrays.asList(3, 1, null, 3, 2, i));
            documents.put("doc-" + i, document);
        }
        Map<String, Object> oversized = new LinkedHashMap<>();
        oversized.put("sort_key", numDocs);
        oversized.put("kw", Arrays.asList("b", "a", null, "b"));
        oversized.put("n", oversizedValues(List.of(0, 1, 2, 3)));
        documents.put("oversized-n", oversized);
        if (randomBoolean()) {
            Map<String, Object> bothOversized = new LinkedHashMap<>();
            bothOversized.put("sort_key", numDocs + 1);
            bothOversized.put("kw", oversizedValues(List.of("a", "b", "c", "d")));
            bothOversized.put("n", oversizedValues(List.of(0, 1, 2, 3)));
            documents.put("oversized-kw-n", bothOversized);
        }

        // Index in random order, with refreshes in between so that the index has several segments to sort and merge.
        List<String> ids = new ArrayList<>(documents.keySet());
        for (String id : shuffledList(ids)) {
            prepareIndex(index).setId(id).setSource(documents.get(id)).get();
            if (randomBoolean()) {
                indicesAdmin().prepareRefresh(index).get();
            }
        }
        indicesAdmin().prepareRefresh(index).get();
        if (randomBoolean()) {
            assertNoFailures(indicesAdmin().prepareForceMerge(index).setMaxNumSegments(1).get());
            indicesAdmin().prepareRefresh(index).get();
        }

        for (String id : ids) {
            // Read from the index rather than from the translog, so the synthesized source is what is compared.
            GetResponse response = client().prepareGet(index, id).setRealtime(false).get();
            assertTrue(id, response.isExists());
            assertThat(id, response.getSourceAsMap(), equalTo(documents.get(id)));
        }
        SearchRequestBuilder search = client().prepareSearch(index)
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(documents.size())
            .setFetchSource(true);
        assertResponse(search, response -> {
            assertNoFailures(response);
            assertThat(response.getHits().getTotalHits().value(), equalTo((long) documents.size()));
            for (SearchHit hit : response.getHits().getHits()) {
                assertThat(hit.getId(), hit.getSourceAsMap(), equalTo(documents.get(hit.getId())));
            }
        });
    }

    /**
     * An array with more elements than a sorted doc value can hold offsets for: one more slot than
     * {@link IndexWriter#MAX_TERM_LENGTH} guarantees that, since every slot takes at least one byte on top of the length prefix.
     */
    private static <T> List<T> oversizedValues(List<T> pool) {
        int numSlots = IndexWriter.MAX_TERM_LENGTH + between(1, 500);
        List<T> values = new ArrayList<>(numSlots);
        for (int i = 0; i < numSlots; i++) {
            values.add(randomInt(10) == 1 ? null : randomFrom(pool));
        }
        return values;
    }
}
