/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersions;

import java.util.List;
import java.util.Map;

/**
 * Reproduces issue #155243: nested sort on an integer field returns the LONG missing
 * sentinel instead of the actual nested value when the index was created before the
 * integer sort optimization was introduced (pre-8.19 / 9.0.x).
 *
 * <p>Root cause: {@code IndexNumericFieldData.sortField(boolean, IndexVersion, ...)} applies
 * a BWC rewrite for old indices, replacing the nested-aware {@code SortField(IntValuesComparatorSource)}
 * with a plain {@code SortedNumericSortField(field, LONG, reverse)}. Parent documents do not carry
 * the nested sub-field directly in their own doc values, so the sort falls back to the LONG missing
 * sentinel ({@code Long.MAX_VALUE} for ascending, {@code Long.MIN_VALUE} for descending) instead of
 * the value aggregated from the matching nested children.
 */
public class NestedSortIntegerBwcIT extends AbstractRollingUpgradeTestCase {

    private static final String INDEX_NAME = "test_nested_int_sort_bwc";

    public NestedSortIntegerBwcIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @SuppressWarnings("unchecked")
    public void testNestedSortOnIntegerField() throws Exception {
        // The BWC rewrite that drops nested support applies only to indices created before
        // the integer sort type was introduced: pre-8.19 for the 8.x line, and 9.0.x for
        // the 9.x line (between the Lucene 10.0 upgrade and the INT sort introduction in 9.1).
        assumeTrue(
            "Old cluster not subject to integer sort BWC rewrite; skipping",
            getOldClusterIndexVersion().before(IndexVersions.INDEX_INT_SORT_INT_TYPE_8_19)
                || (getOldClusterIndexVersion().onOrAfter(IndexVersions.UPGRADE_TO_LUCENE_10_0_0)
                    && getOldClusterIndexVersion().before(IndexVersions.INDEX_INT_SORT_INT_TYPE))
        );

        if (isOldCluster()) {
            createIndex(
                INDEX_NAME,
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .build(),
                """
                    {
                        "properties": {
                            "obj": {
                                "type": "nested",
                                "properties": {
                                    "value": { "type": "integer" }
                                }
                            }
                        }
                    }
                    """
            );

            // Index three parent docs with nested children in non-sorted order so that
            // a correct ascending sort must reorder them as 10 -> 20 -> 30.
            var bulkRequest = new Request("POST", "/" + INDEX_NAME + "/_bulk");
            bulkRequest.setJsonEntity("""
                {"index": {}}
                {"obj": {"value": 30}}
                {"index": {}}
                {"obj": {"value": 10}}
                {"index": {}}
                {"obj": {"value": 20}}
                """);
            bulkRequest.addParameter("refresh", "true");
            assertOK(client().performRequest(bulkRequest));
        }

        // Sort ascending by the nested integer field and collect the returned sort values.
        // On a correctly functioning cluster the values must be [10, 20, 30].
        // With the bug the BWC rewrite creates a bare SortedNumericSortField(LONG) that
        // ignores the nested structure; parent docs appear to have no value and sort is
        // assigned Long.MAX_VALUE (ascending LONG missing sentinel) for every document.
        var searchRequest = new Request("GET", "/" + INDEX_NAME + "/_search");
        searchRequest.setJsonEntity("""
            {
                "size": 10,
                "sort": [
                    {
                        "obj.value": {
                            "order": "asc",
                            "nested": { "path": "obj" }
                        }
                    }
                ]
            }
            """);
        var searchResponse = client().performRequest(searchRequest);
        assertOK(searchResponse);
        var body = entityAsMap(searchResponse);
        var hits = (List<Map<String, Object>>) ((Map<String, Object>) body.get("hits")).get("hits");
        assertEquals("Expected 3 hits", 3, hits.size());

        List<Long> sortValues = hits.stream()
            .map(h -> ((Number) ((List<Object>) h.get("sort")).get(0)).longValue())
            .toList();

        assertEquals(
            "Nested integer sort returned " + sortValues + " but expected [10, 20, 30]. "
                + "The integer sort BWC rewrite may have replaced the nested-aware comparator "
                + "with a plain LONG SortedNumericSortField that ignores nested children.",
            List.of(10L, 20L, 30L),
            sortValues
        );
    }
}
