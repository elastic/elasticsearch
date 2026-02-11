/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.bench;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

public class MultiSeedBenchSupportTests extends ESTestCase {

    public void testBootstrapIdenticalArrays() {
        // NOTE: non-zero values so refMedian != 0 and delta percentage is meaningful.
        final int[] ref = { 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000 };
        final int[] pipeline = ref.clone();
        final MultiSeedBenchSupport.BootstrapResult result = MultiSeedBenchSupport.bootstrap(ref, pipeline, 0xABCDL);
        assertTrue("ciLow should be <= 0 for identical arrays, got " + result.ciLow(), result.ciLow() <= 0.0);
        assertTrue("ciHigh should be >= 0 for identical arrays, got " + result.ciHigh(), result.ciHigh() >= 0.0);
    }

    public void testBootstrapIdenticalZeroArrays() {
        // NOTE: exercises the refMedian == 0 path; all deltas should be 0.
        final int[] ref = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        final int[] pipeline = ref.clone();
        final MultiSeedBenchSupport.BootstrapResult result = MultiSeedBenchSupport.bootstrap(ref, pipeline, 0x1234L);
        assertEquals(0.0, result.ciLow(), 0.0);
        assertEquals(0.0, result.ciHigh(), 0.0);
    }

    public void testBootstrapPipelineAlwaysSmaller() {
        final int[] ref = { 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900 };
        final int[] pipeline = { 500, 550, 600, 650, 700, 750, 800, 850, 900, 950 };
        final MultiSeedBenchSupport.BootstrapResult result = MultiSeedBenchSupport.bootstrap(ref, pipeline, 0xBEEFL);
        assertTrue("ciHigh should be < 0 when pipeline always smaller, got " + result.ciHigh(), result.ciHigh() < 0.0);
    }

    public void testBootstrapDeterministic() {
        final int[] ref = { 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000 };
        final int[] pipeline = { 110, 190, 310, 390, 510, 590, 710, 790, 910, 990 };
        final MultiSeedBenchSupport.BootstrapResult r1 = MultiSeedBenchSupport.bootstrap(ref, pipeline, 0xCAFEL);
        final MultiSeedBenchSupport.BootstrapResult r2 = MultiSeedBenchSupport.bootstrap(ref, pipeline, 0xCAFEL);
        assertEquals(r1.ciLow(), r2.ciLow(), 0.0);
        assertEquals(r1.ciHigh(), r2.ciHigh(), 0.0);
    }

    public void testBootstrapCIOrdering() {
        final int[] ref = { 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000 };
        final int[] pipeline = { 110, 190, 310, 390, 510, 590, 710, 790, 910, 990 };
        final MultiSeedBenchSupport.BootstrapResult result = MultiSeedBenchSupport.bootstrap(ref, pipeline, 0xFACEL);
        assertTrue("ciLow should be <= ciHigh, got " + result.ciLow() + " > " + result.ciHigh(), result.ciLow() <= result.ciHigh());
    }

    public void testWinnerPerSeedBreaksTieByStageCount() {
        // NOTE: two pipelines with equal sizes across 4 seeds; pipeline 0 has 4 stages, pipeline 1 has 2 stages.
        // Without tie-break, pipeline 0 wins (index order). With tie-break, pipeline 1 wins (fewer stages).
        final int[][] perSeed = {
            { 500, 500 },  // seed 0: tied
            { 500, 500 },  // seed 1: tied
            { 500, 500 },  // seed 2: tied
            { 500, 500 },  // seed 3: tied
        };
        final int[] stageCounts = { 4, 2 };
        final String[] names = { "delta+offset+gcd+bitPack", "offset+bitPack" };

        final int[] winners = MultiSeedBenchSupport.winnerPerSeed(perSeed, stageCounts, names);

        for (int s = 0; s < winners.length; s++) {
            assertEquals("seed " + s + " should pick pipeline 1 (fewer stages)", 1, winners[s]);
        }

        // NOTE: verify win counts reflect the tie-break.
        final int[] counts = MultiSeedBenchSupport.winCounts(winners, 2);
        assertEquals(0, counts[0]);
        assertEquals(4, counts[1]);
    }

    public void testWinnerPerSeedBreaksTieByNameWhenStageCountEqual() {
        // NOTE: three pipelines with equal sizes and equal stageCount; name ordering decides.
        final int[][] perSeed = { { 800, 800, 800 }, { 800, 800, 800 }, };
        final int[] stageCounts = { 3, 3, 3 };
        final String[] names = { "delta+offset+gcd+simpleBitPack", "delta+offset+simpleBitPack", "offset+simpleBitPack" };

        final int[] winners = MultiSeedBenchSupport.winnerPerSeed(perSeed, stageCounts, names);

        // NOTE: "delta+offset+gcd+simpleBitPack" < "delta+offset+simpleBitPack" < "offset+simpleBitPack" lexicographically.
        for (int s = 0; s < winners.length; s++) {
            assertEquals("seed " + s + " should pick pipeline 0 (earliest name)", 0, winners[s]);
        }
    }

    public void testWinnerPerSeedSizeTrumpsStageCount() {
        // NOTE: pipeline 0 has fewer stages but larger size -- pipeline 1 should still win on size.
        final int[][] perSeed = { { 600, 500 }, { 700, 400 }, };
        final int[] stageCounts = { 2, 5 };
        final String[] names = { "aaa", "zzz" };

        final int[] winners = MultiSeedBenchSupport.winnerPerSeed(perSeed, stageCounts, names);

        assertEquals("seed 0: pipeline 1 wins on size", 1, winners[0]);
        assertEquals("seed 1: pipeline 1 wins on size", 1, winners[1]);
    }

    public void testBootstrapResamplesMinimum() {
        final String prev = getSystemProperty(MultiSeedBenchSupport.BOOTSTRAP_RESAMPLES_PROPERTY);
        try {
            setSystemProperty(MultiSeedBenchSupport.BOOTSTRAP_RESAMPLES_PROPERTY, "5");
            final int[] ref = { 100, 200, 300 };
            final int[] pipeline = { 100, 200, 300 };
            expectThrows(IllegalArgumentException.class, () -> MultiSeedBenchSupport.bootstrap(ref, pipeline, 0L));
        } finally {
            if (prev == null) {
                clearSystemProperty(MultiSeedBenchSupport.BOOTSTRAP_RESAMPLES_PROPERTY);
            } else {
                setSystemProperty(MultiSeedBenchSupport.BOOTSTRAP_RESAMPLES_PROPERTY, prev);
            }
        }
    }

    @SuppressForbidden(reason = "test needs to manipulate system properties to verify property-driven behavior")
    private static String getSystemProperty(String key) {
        return System.getProperty(key);
    }

    @SuppressForbidden(reason = "test needs to manipulate system properties to verify property-driven behavior")
    private static void setSystemProperty(String key, String value) {
        System.setProperty(key, value);
    }

    @SuppressForbidden(reason = "test needs to manipulate system properties to verify property-driven behavior")
    private static void clearSystemProperty(String key) {
        System.clearProperty(key);
    }
}
