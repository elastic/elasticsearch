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

    public void testClassifyStripeFragmentParsesOrdinalAnchorsAndTypedStats() {
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put(ExternalStats.PARTIAL_CHUNK_KEY, Boolean.TRUE);
        raw.put(ExternalStats.STRIPE_SIZE_KEY, 1024L);
        raw.put(ExternalStats.STRIPE_ORDINAL_KEY, 3L);
        raw.put(ExternalStats.COVERAGE_START_KEY, 40L);
        raw.put(ExternalStats.COVERAGE_END_KEY, 100L);
        raw.put(ExternalStats.STRIPE_AT_START_KEY, Boolean.TRUE);
        raw.put(ExternalStats.STRIPE_AT_END_KEY, Boolean.TRUE);
        raw.put(ExternalStats.COVERAGE_IS_LAST_KEY, Boolean.TRUE);
        raw.put(ExternalStats.MTIME_MILLIS_KEY, 1000L);
        raw.put(ExternalStats.CONFIG_FINGERPRINT_KEY, "fp");
        raw.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 12L);
        SourceStatsContribution classified = SourceStatsContribution.classify(raw);
        assertThat(classified, instanceOf(SourceStatsContribution.StripeFragment.class));
        SourceStatsContribution.StripeFragment f = (SourceStatsContribution.StripeFragment) classified;
        assertEquals("ordinal is parsed", 3L, f.ordinal());
        assertEquals("sub-range start is parsed", 40L, f.start());
        assertEquals("sub-range end is parsed", 100L, f.end());
        assertTrue("atStripeStart anchor is parsed", f.atStripeStart());
        assertTrue("atStripeEnd anchor is parsed", f.atStripeEnd());
        assertTrue("eof flag is parsed", f.eof());
        assertTrue("a fragment with a grid + ordinal + range is stripe-addressed", f.stripeAddressed());
        assertEquals("mtime is parsed as a typed field", 1000L, f.mtimeMillis());
        assertEquals("fingerprint is parsed as a typed field", "fp", f.configFingerprint());
        assertEquals("statistics are parsed into a typed SourceStatistics", 12L, f.stats().rowCount().getAsLong());
    }

    public void testClassifyChunkWithoutStripeAddressingIsUnaddressable() {
        // A chunk marker with no stripe keys (older node, reader not yet emitting stripes): classified
        // as a StripeFragment but NOT stripe-addressed, so the reconciler safe-misses rather than
        // mis-reading it as a whole-file read.
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put(ExternalStats.PARTIAL_CHUNK_KEY, Boolean.TRUE);
        raw.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 12L);
        SourceStatsContribution classified = SourceStatsContribution.classify(raw);
        assertThat(classified, instanceOf(SourceStatsContribution.StripeFragment.class));
        assertFalse(
            "a chunk with no stripe addressing is not cacheable",
            ((SourceStatsContribution.StripeFragment) classified).stripeAddressed()
        );
    }

    public void testClassifyWholeFileIsTheDefault() {
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put(ExternalStats.MTIME_MILLIS_KEY, 1000L);
        raw.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 42L);
        SourceStatsContribution classified = SourceStatsContribution.classify(raw);
        assertThat(classified, instanceOf(SourceStatsContribution.WholeFile.class));
        SourceStatsContribution.WholeFile wf = (SourceStatsContribution.WholeFile) classified;
        assertEquals(42L, wf.stats().rowCount().getAsLong());
        assertEquals(1000L, wf.mtimeMillis());
    }
}
