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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class SourceStatsContributionTests extends ESTestCase {

    public void testClassifyPoisonWinsOverEverything() {
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put(ExternalStats.CHUNK_HAD_ERRORS_KEY, Boolean.TRUE);
        raw.put(ExternalStats.FINALIZE_CHUNKS_KEY, Boolean.TRUE);
        raw.put(ExternalStats.PARTIAL_CHUNK_KEY, Boolean.TRUE);
        raw.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 5L);
        assertThat(SourceStatsContribution.classify(raw), instanceOf(SourceStatsContribution.Poison.class));
    }

    public void testClassifyFinalizeMarker() {
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put(ExternalStats.FINALIZE_CHUNKS_KEY, Boolean.TRUE);
        assertThat(SourceStatsContribution.classify(raw), instanceOf(SourceStatsContribution.Finalize.class));
    }

    public void testClassifyFinalizeWithStatsTripsAssertion() {
        // Defends the "Finalize carries no stats" contract: a stats-bearing finalize used to
        // silently fall through to WholeFile/PartialChunk and corrupt the merge.
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put(ExternalStats.FINALIZE_CHUNKS_KEY, Boolean.TRUE);
        raw.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 7L);
        AssertionError e = expectThrows(AssertionError.class, () -> SourceStatsContribution.classify(raw));
        assertThat(e.getMessage(), containsString("Finalize marker must not carry stats"));
    }

    public void testClassifyPartialChunkStripsMarkerKeys() {
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put(ExternalStats.PARTIAL_CHUNK_KEY, Boolean.TRUE);
        raw.put(ExternalStats.MTIME_MILLIS_KEY, 1000L);
        raw.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 12L);
        SourceStatsContribution classified = SourceStatsContribution.classify(raw);
        assertThat(classified, instanceOf(SourceStatsContribution.PartialChunk.class));
        Map<String, Object> stats = ((SourceStatsContribution.PartialChunk) classified).stats();
        assertFalse("PARTIAL_CHUNK_KEY must be stripped", stats.containsKey(ExternalStats.PARTIAL_CHUNK_KEY));
        assertEquals(1000L, stats.get(ExternalStats.MTIME_MILLIS_KEY));
        assertEquals(12L, stats.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
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
