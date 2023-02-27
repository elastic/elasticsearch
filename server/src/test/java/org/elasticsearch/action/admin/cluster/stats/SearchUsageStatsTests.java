/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SearchUsageStatsTests extends AbstractWireSerializingTestCase<SearchUsageStats> {

    private static final List<String> QUERY_TYPES = List.of(
        "match",
        "bool",
        "term",
        "terms",
        "multi_match",
        "function_score",
        "range",
        "script_score"
    );

    private static final List<String> SECTIONS = List.of(
        "highlight",
        "query",
        "post_filter",
        "aggs",
        "terminate_after",
        "indices_boost",
        "range",
        "script_score"
    );

    @Override
    protected Reader<SearchUsageStats> instanceReader() {
        return SearchUsageStats::new;
    }

    private static Map<String, Long> randomSectionsUsage(int size) {
        Map<String, Long> sectionsUsage = new HashMap<>();
        while (sectionsUsage.size() < size) {
            sectionsUsage.put(randomFrom(SECTIONS), randomLongBetween(1, Long.MAX_VALUE));
        }
        return sectionsUsage;
    }

    private static Map<String, Long> randomQueryUsage(int size) {
        Map<String, Long> queryUsage = new HashMap<>();
        while (queryUsage.size() < size) {
            queryUsage.put(randomFrom(QUERY_TYPES), randomLongBetween(1, Long.MAX_VALUE));
        }
        return queryUsage;
    }

    @Override
    protected SearchUsageStats createTestInstance() {
        if (randomBoolean()) {
            return new SearchUsageStats();
        }
        return new SearchUsageStats(
            randomQueryUsage(randomIntBetween(0, 4)),
            randomSectionsUsage(randomIntBetween(0, 4)),
            randomLongBetween(10, Long.MAX_VALUE)
        );
    }

    @Override
    protected SearchUsageStats mutateInstance(SearchUsageStats instance) {
        if (randomBoolean()) {
            return new SearchUsageStats(
                randomQueryUsage(instance.getQueryUsage().size() + 1),
                instance.getSectionsUsage(),
                instance.getTotalSearchCount()
            );
        }
        if (randomBoolean()) {
            return new SearchUsageStats(
                instance.getQueryUsage(),
                randomSectionsUsage(instance.getSectionsUsage().size() + 1),
                instance.getTotalSearchCount()
            );
        }
        return new SearchUsageStats(instance.getQueryUsage(), instance.getSectionsUsage(), randomLongBetween(10, Long.MAX_VALUE));
    }

    public void testAdd() {
        SearchUsageStats searchUsageStats = new SearchUsageStats();
        assertEquals(Map.of(), searchUsageStats.getQueryUsage());
        assertEquals(Map.of(), searchUsageStats.getSectionsUsage());
        assertEquals(0, searchUsageStats.getTotalSearchCount());

        searchUsageStats.add(new SearchUsageStats(Map.of("match", 10L), Map.of("query", 10L), 10L));
        assertEquals(Map.of("match", 10L), searchUsageStats.getQueryUsage());
        assertEquals(Map.of("query", 10L), searchUsageStats.getSectionsUsage());
        assertEquals(10L, searchUsageStats.getTotalSearchCount());

        searchUsageStats.add(new SearchUsageStats(Map.of("term", 1L, "match", 1L), Map.of("query", 10L, "knn", 1L), 10L));
        assertEquals(Map.of("match", 11L, "term", 1L), searchUsageStats.getQueryUsage());
        assertEquals(Map.of("query", 20L, "knn", 1L), searchUsageStats.getSectionsUsage());
        assertEquals(20L, searchUsageStats.getTotalSearchCount());
    }

    public void testToXContent() throws IOException {
        SearchUsageStats searchUsageStats = new SearchUsageStats(Map.of("term", 1L), Map.of("query", 10L), 10L);
        assertEquals(
            "{\"search\":{\"total\":10,\"queries\":{\"term\":1},\"sections\":{\"query\":10}}}",
            Strings.toString(searchUsageStats)
        );
    }
}
