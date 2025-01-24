/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;

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

    private static final List<String> RESCORER_TYPES = List.of("query", "learning_to_rank");

    private static final List<String> SECTIONS = List.of(
        "highlight",
        "query",
        "post_filter",
        "aggs",
        "terminate_after",
        "indices_boost",
        "range",
        "script_score",
        "retrievers"
    );

    private static final List<String> RETRIEVERS = List.of("standard", "knn", "rrf", "random", "text_similarity_reranker");

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

    private static Map<String, Long> randomRescorerUsage(int size) {
        Map<String, Long> rescorerUsage = new HashMap<>();
        while (rescorerUsage.size() < size) {
            rescorerUsage.put(randomFrom(RESCORER_TYPES), randomLongBetween(1, Long.MAX_VALUE));
        }
        return rescorerUsage;
    }

    private static Map<String, Long> randomRetrieversUsage(int size) {
        Map<String, Long> retrieversUsage = new HashMap<>();
        while (retrieversUsage.size() < size) {
            retrieversUsage.put(randomFrom(RETRIEVERS), randomLongBetween(1, Long.MAX_VALUE));
        }
        return retrieversUsage;
    }

    @Override
    protected SearchUsageStats createTestInstance() {
        if (randomBoolean()) {
            return new SearchUsageStats();
        }
        return new SearchUsageStats(
            randomQueryUsage(randomIntBetween(0, QUERY_TYPES.size())),
            randomRescorerUsage(randomIntBetween(0, RESCORER_TYPES.size())),
            randomSectionsUsage(randomIntBetween(0, SECTIONS.size())),
            randomRetrieversUsage(randomIntBetween(0, RETRIEVERS.size())),
            randomLongBetween(10, Long.MAX_VALUE)
        );
    }

    @Override
    protected SearchUsageStats mutateInstance(SearchUsageStats instance) {
        int i = randomInt(4);
        return switch (i) {
            case 0 -> new SearchUsageStats(
                randomValueOtherThan(instance.getQueryUsage(), () -> randomQueryUsage(randomIntBetween(0, QUERY_TYPES.size()))),
                instance.getRescorerUsage(),
                instance.getSectionsUsage(),
                instance.getRetrieversUsage(),
                instance.getTotalSearchCount()
            );
            case 1 -> new SearchUsageStats(
                instance.getQueryUsage(),
                randomValueOtherThan(instance.getRescorerUsage(), () -> randomRescorerUsage(randomIntBetween(0, RESCORER_TYPES.size()))),
                instance.getSectionsUsage(),
                instance.getRetrieversUsage(),
                instance.getTotalSearchCount()
            );
            case 2 -> new SearchUsageStats(
                instance.getQueryUsage(),
                instance.getRescorerUsage(),
                randomValueOtherThan(instance.getSectionsUsage(), () -> randomSectionsUsage(randomIntBetween(0, SECTIONS.size()))),
                instance.getRetrieversUsage(),
                instance.getTotalSearchCount()
            );
            case 3 -> new SearchUsageStats(
                instance.getQueryUsage(),
                instance.getRescorerUsage(),
                instance.getSectionsUsage(),
                randomValueOtherThan(instance.getRetrieversUsage(), () -> randomSectionsUsage(randomIntBetween(0, SECTIONS.size()))),
                instance.getTotalSearchCount()
            );
            case 4 -> new SearchUsageStats(
                instance.getQueryUsage(),
                instance.getRescorerUsage(),
                instance.getSectionsUsage(),
                instance.getRetrieversUsage(),
                randomValueOtherThan(instance.getTotalSearchCount(), () -> randomLongBetween(10, Long.MAX_VALUE))
            );
            default -> throw new IllegalStateException("Unexpected value: " + i);
        };
    }

    public void testAdd() {
        SearchUsageStats searchUsageStats = new SearchUsageStats();
        assertEquals(Map.of(), searchUsageStats.getQueryUsage());
        assertEquals(Map.of(), searchUsageStats.getRescorerUsage());
        assertEquals(Map.of(), searchUsageStats.getSectionsUsage());
        assertEquals(0, searchUsageStats.getTotalSearchCount());

        searchUsageStats.add(
            new SearchUsageStats(Map.of("match", 10L), Map.of("query", 5L), Map.of("query", 10L), Map.of("knn", 10L), 10L)
        );
        assertEquals(Map.of("match", 10L), searchUsageStats.getQueryUsage());
        assertEquals(Map.of("query", 10L), searchUsageStats.getSectionsUsage());
        assertEquals(Map.of("query", 5L), searchUsageStats.getRescorerUsage());
        assertEquals(10L, searchUsageStats.getTotalSearchCount());

        searchUsageStats.add(
            new SearchUsageStats(
                Map.of("term", 1L, "match", 1L),
                Map.of("query", 5L, "learning_to_rank", 2L),
                Map.of("query", 10L, "knn", 1L),
                Map.of("knn", 10L, "rrf", 2L),
                10L
            )
        );
        assertEquals(Map.of("match", 11L, "term", 1L), searchUsageStats.getQueryUsage());
        assertEquals(Map.of("query", 20L, "knn", 1L), searchUsageStats.getSectionsUsage());
        assertEquals(Map.of("query", 10L, "learning_to_rank", 2L), searchUsageStats.getRescorerUsage());
        assertEquals(Map.of("knn", 20L, "rrf", 2L), searchUsageStats.getRetrieversUsage());
        assertEquals(20L, searchUsageStats.getTotalSearchCount());
    }

    public void testToXContent() throws IOException {
        SearchUsageStats searchUsageStats = new SearchUsageStats(
            Map.of("term", 1L),
            Map.of("query", 2L),
            Map.of("query", 10L),
            Map.of("knn", 10L),
            10L
        );
        assertEquals(
            "{\"search\":{\"total\":10,\"queries\":{\"term\":1},\"rescorers\":{\"query\":2},"
                + "\"sections\":{\"query\":10},\"retrievers\":{\"knn\":10}}}",
            Strings.toString(searchUsageStats)
        );
    }

    /**
     * Test (de)serialization on all previous released versions
     */
    public void testSerializationBWC() throws IOException {
        for (TransportVersion version : TransportVersionUtils.allReleasedVersions()) {
            SearchUsageStats testInstance = new SearchUsageStats(
                randomQueryUsage(QUERY_TYPES.size()),
                version.onOrAfter(TransportVersions.V_8_12_0) ? randomRescorerUsage(RESCORER_TYPES.size()) : Map.of(),
                randomSectionsUsage(SECTIONS.size()),
                version.onOrAfter(TransportVersions.V_8_16_0) ? randomRetrieversUsage(RETRIEVERS.size()) : Map.of(),
                randomLongBetween(0, Long.MAX_VALUE)
            );
            assertSerialization(testInstance, version);
        }
    }
}
