/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

public class IndexSortRangeQueryNestedSingleNodeTests extends ESSingleNodeTestCase {

    private static final int NUM_DOCS = 20;

    private static final int[][] RANGES = { { 0, 0 }, { 3, 3 }, { 5, 9 }, { 0, 3 }, { 10, 19 }, { 0, 19 } };

    /**
     * The sort field has doc values and no points, as a LogsDB {@code @timestamp} has by default since 9.4.
     * A range on it returned wrong hits.
     */
    public void testRangeOnDocValuesOnlySortFieldWithNestedDocuments() throws IOException {
        assertRangesOnSortedIndexWithNestedDocuments("date", false, "asc");
    }

    /**
     * The sort field has points. A range on it returned the right hits and a wrong count.
     */
    public void testRangeOnIndexedSortFieldWithNestedDocuments() throws IOException {
        assertRangesOnSortedIndexWithNestedDocuments("long", true, "desc");
    }

    private void assertRangesOnSortedIndexWithNestedDocuments(String type, boolean points, String order) throws IOException {
        final String index = "sorted-" + type;
        createSortedIndexWithNestedDocuments(index, type, points, order);
        for (final int[] range : RANGES) {
            final Set<String> expected = new TreeSet<>();
            for (int id = range[0]; id <= range[1]; id++) {
                expected.add("doc" + id);
            }
            final String description = String.format(Locale.ROOT, "%s sorted %s, range [%d, %d]", type, order, range[0], range[1]);
            final Set<String> actual = new TreeSet<>();
            final var rangeQuery = QueryBuilders.rangeQuery("sort_key").gte(range[0]).lte(range[1]);
            assertResponse(client().prepareSearch(index).setQuery(rangeQuery).setSize(NUM_DOCS), response -> {
                for (final var hit : response.getHits().getHits()) {
                    actual.add(hit.getId());
                }
            });
            final Set<String> missing = new TreeSet<>(expected);
            missing.removeAll(actual);
            final Set<String> unexpected = new TreeSet<>(actual);
            unexpected.removeAll(expected);
            assertEquals(description + ": missing " + missing + ", unexpected " + unexpected, expected, actual);
            assertHitCount(client().prepareSearch(index).setQuery(rangeQuery).setSize(0).setTrackTotalHits(true), expected.size());
        }
    }

    private void createSortedIndexWithNestedDocuments(String index, String type, boolean points, String order) throws IOException {
        createIndex(
            index,
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.sort.field", "sort_key")
                .put("index.sort.order", order)
                .put(IndexSettings.USE_DOC_VALUES_SKIPPER.getKey(), randomBoolean())
                .build(),
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("sort_key")
                .field("type", type)
                .field("index", points)
                .endObject()
                .startObject("tags")
                .field("type", "nested")
                .startObject("properties")
                .startObject("k")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        final BulkRequestBuilder bulk = client().prepareBulk();
        for (int id = 0; id < NUM_DOCS; id++) {
            bulk.add(
                prepareIndex(index).setId("doc" + id)
                    .setSource(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .field("sort_key", id)
                            .startArray("tags")
                            .startObject()
                            .field("k", "a")
                            .endObject()
                            .startObject()
                            .field("k", "b")
                            .endObject()
                            .endArray()
                            .endObject()
                    )
            );
        }
        assertFalse(bulk.get().hasFailures());
        indicesAdmin().prepareRefresh(index).get();
    }
}
