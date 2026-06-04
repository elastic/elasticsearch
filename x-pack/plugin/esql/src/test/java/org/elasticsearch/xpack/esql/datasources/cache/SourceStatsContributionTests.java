/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;

public class SourceStatsContributionTests extends ESTestCase {

    public void testClassifyPoisonWinsOverEverything() {
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put(ExternalStats.CHUNK_HAD_ERRORS_KEY, Boolean.TRUE);
        raw.put(ExternalStats.PARTIAL_CHUNK_KEY, Boolean.TRUE);
        raw.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 5L);
        assertThat(SourceStatsContribution.classify(raw), instanceOf(SourceStatsContribution.Poison.class));
    }

    public void testClassifyPartialChunkStripsMarkerAndCoverageKeys() {
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put(ExternalStats.PARTIAL_CHUNK_KEY, Boolean.TRUE);
        raw.put(ExternalStats.COVERAGE_START_KEY, 40L);
        raw.put(ExternalStats.COVERAGE_END_KEY, 100L);
        raw.put(ExternalStats.COVERAGE_IS_LAST_KEY, Boolean.TRUE);
        raw.put(ExternalStats.MTIME_MILLIS_KEY, 1000L);
        raw.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 12L);
        SourceStatsContribution classified = SourceStatsContribution.classify(raw);
        assertThat(classified, instanceOf(SourceStatsContribution.PartialChunk.class));
        SourceStatsContribution.PartialChunk pc = (SourceStatsContribution.PartialChunk) classified;
        assertEquals("coverage start is parsed onto the record", 40L, pc.start());
        assertEquals("coverage end is parsed onto the record", 100L, pc.end());
        assertTrue("coverage last flag is parsed onto the record", pc.last());
        assertTrue("a partial with a valid range has coverage", pc.hasCoverage());
        Map<String, Object> stats = pc.stats();
        assertFalse("PARTIAL_CHUNK_KEY must be stripped", stats.containsKey(ExternalStats.PARTIAL_CHUNK_KEY));
        assertFalse("coverage keys must be stripped from stats", stats.containsKey(ExternalStats.COVERAGE_START_KEY));
        assertEquals(1000L, stats.get(ExternalStats.MTIME_MILLIS_KEY));
        assertEquals(12L, stats.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
    }

    public void testClassifyPartialChunkWithoutCoverageIsUnaddressable() {
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put(ExternalStats.PARTIAL_CHUNK_KEY, Boolean.TRUE);
        raw.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 12L);
        SourceStatsContribution classified = SourceStatsContribution.classify(raw);
        assertThat(classified, instanceOf(SourceStatsContribution.PartialChunk.class));
        assertFalse("a partial with no coverage cannot be addressed", ((SourceStatsContribution.PartialChunk) classified).hasCoverage());
    }

    public void testClassifyWholeFileIsTheDefault() {
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put(ExternalStats.MTIME_MILLIS_KEY, 1000L);
        raw.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 42L);
        SourceStatsContribution classified = SourceStatsContribution.classify(raw);
        assertThat(classified, instanceOf(SourceStatsContribution.WholeFile.class));
        assertEquals(42L, ((SourceStatsContribution.WholeFile) classified).stats().get(SourceStatisticsSerializer.STATS_ROW_COUNT));
    }
}
