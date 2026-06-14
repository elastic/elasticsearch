/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Behavioural tests for {@link StructuralChangeDetector}: it runs on a single channel of values and returns the
 * verified step/trend boundaries. (Distribution changes, which are this same detector run on a dispersion channel,
 * and the merging of the channels, are exercised in {@link EventDetectorTests}.)
 */
public class StructuralChangeDetectorTests extends ESTestCase {

    private static final int MAX_DEGREE = 3;
    private static final double P_VALUE = 0.05;

    private static StructuralChangeDetector detector(int minSegmentLength) {
        return new StructuralChangeDetector(minSegmentLength, MAX_DEGREE, P_VALUE);
    }

    public void testDetectsSingleStepChange() {
        double[] values = step(180, 90, 2.0, 11.0);
        List<Integer> breaks = breakIndexes(detector(20).detect(values));
        assertTrue("expected a structural break near the step, got " + breaks, hasNear(breaks, 90, 8));
    }

    public void testDetectsMultipleStepChanges() {
        double[] values = new double[230];
        for (int i = 0; i < values.length; i++) {
            values[i] = i < 70 ? 1.0 : (i < 150 ? 8.0 : 3.0);
        }
        List<Integer> breaks = breakIndexes(detector(24).detect(values));
        assertTrue("expected breaks near 70 and 150, got " + breaks, hasNear(breaks, 70, 10) && hasNear(breaks, 150, 10));
    }

    public void testDetectsThreeStepChanges() {
        double[] values = new double[360];
        for (int i = 0; i < values.length; i++) {
            values[i] = i < 90 ? 1.0 : (i < 180 ? 6.0 : (i < 270 ? 2.0 : 9.0));
        }
        List<Integer> breaks = breakIndexes(detector(24).detect(values));
        assertTrue(
            "expected breaks near 90, 180, 270, got " + breaks,
            hasNear(breaks, 90, 12) && hasNear(breaks, 180, 12) && hasNear(breaks, 270, 12)
        );
    }

    public void testDetectsTrendChange() {
        // Slope changes 5x at the midpoint (a continuous derivative change, no level jump).
        int cp = 100;
        double[] values = new double[220];
        for (int i = 0; i < values.length; i++) {
            values[i] = i < cp ? 0.05 * i : 0.05 * cp + 0.25 * (i - cp);
        }
        List<Integer> breaks = breakIndexes(new StructuralChangeDetector(20, 1.0, MAX_DEGREE, P_VALUE).detect(values));
        assertTrue("expected a break near the slope change, got " + breaks, hasNear(breaks, cp, 14));
    }

    public void testSmoothDriftIsNotFragmented() {
        // A smooth quadratic drift is no change; the cubic no-change model should absorb it.
        double[] values = new double[240];
        for (int i = 0; i < values.length; i++) {
            values[i] = 0.0008 * i * i + 0.03 * i;
        }
        assertTrue("smooth drift should not fragment into many breaks", breakIndexes(detector(24).detect(values)).size() <= 2);
    }

    public void testStationaryNoiseHasNoBreaks() {
        Random random = new Random(17);
        double[] values = new double[200];
        for (int i = 0; i < values.length; i++) {
            values[i] = 50.0 + random.nextGaussian() * 3.0;
        }
        assertTrue("stationary noise should not yield structural breaks", breakIndexes(detector(20).detect(values)).isEmpty());
    }

    public void testConstantSeriesHasNoBreaks() {
        double[] values = new double[160];
        java.util.Arrays.fill(values, 5.0);
        assertTrue(breakIndexes(detector(20).detect(values)).isEmpty());
    }

    public void testHighMagnitudeConstantSeriesHasNoBreaks() {
        // Regression: a constant series at a large level used to manufacture spurious breaks through catastrophic
        // cancellation in the running-variance; the offset-centring and precision floor prevent that.
        double[] values = new double[160];
        java.util.Arrays.fill(values, 100000.0);
        assertTrue("a high-magnitude constant series must be flat", breakIndexes(detector(20).detect(values)).isEmpty());
    }

    public void testTooShortSeriesYieldsNoBreaks() {
        double[] values = new double[30];
        java.util.Arrays.fill(values, 1.0);
        assertTrue(breakIndexes(detector(20).detect(values)).isEmpty());
    }

    public void testIsolatedSpikeDoesNotCreateAStructuralBreak() {
        // The local down-weighting must keep a lone spike from being fit as a step.
        Random random = new Random(42);
        double[] values = new double[160];
        for (int i = 0; i < values.length; i++) {
            values[i] = 100.0 + random.nextGaussian() * 3.0;
        }
        values[80] += 200.0;
        assertTrue(
            "an isolated spike must not be reported as a structural break",
            hasNear(breakIndexes(detector(16).detect(values)), 80, 4) == false
        );
    }

    public void testKeepsStructuralBreakWhenSpikeIsNearTheTrueChange() {
        int cp = 110;
        double[] values = step(220, cp, 0.0, 6.0);
        values[cp - 25] = 25.0; // spike on the low regime, well before the step
        List<Integer> breaks = breakIndexes(detector(20).detect(values));
        assertTrue("the genuine step must survive a nearby spike, got " + breaks, hasNear(breaks, cp, 10));
    }

    // ---- with noise -------------------------------------------------------------------------------------------

    public void testDetectsAStepInNoise() {
        int cp = 90;
        double[] values = withNoise(step(180, cp, 0.0, 10.0), 1.0, new Random(1)); // SNR ~10
        assertTrue("expected a step in noise near " + cp, hasNear(breakIndexes(detector(20).detect(values)), cp, 10));
    }

    public void testDetectsADownwardStepInNoise() {
        int cp = 100;
        double[] values = withNoise(step(200, cp, 10.0, 0.0), 1.0, new Random(2));
        assertTrue("expected a downward step in noise near " + cp, hasNear(breakIndexes(detector(20).detect(values)), cp, 10));
    }

    public void testDetectsATrendChangeInNoise() {
        int cp = 110;
        double[] values = new double[230];
        Random random = new Random(3);
        for (int i = 0; i < values.length; i++) {
            double mean = i < cp ? 0.05 * i : 0.05 * cp + 0.30 * (i - cp);
            values[i] = mean + random.nextGaussian() * 0.5;
        }
        List<Integer> breaks = breakIndexes(new StructuralChangeDetector(20, 1.0, MAX_DEGREE, P_VALUE).detect(values));
        assertTrue("expected a trend change in noise near " + cp + ", got " + breaks, hasNear(breaks, cp, 16));
    }

    public void testDetectsAStepAtSeveralPositions() {
        for (int cp : new int[] { 40, 90, 140 }) {
            double[] values = withNoise(step(200, cp, 0.0, 8.0), 0.8, new Random(cp));
            assertTrue("missed a step at position " + cp, hasNear(breakIndexes(detector(20).detect(values)), cp, 10));
        }
    }

    public void testHandlesAStepTrendStepSequence() {
        // Flat low, then a rising ramp, then flat high: two boundaries (onset and end of the ramp).
        int cp1 = 80;
        int cp2 = 180;
        double[] values = new double[280];
        Random random = new Random(4);
        for (int i = 0; i < values.length; i++) {
            double mean;
            if (i < cp1) {
                mean = 5.0;
            } else if (i < cp2) {
                mean = 5.0 + 0.20 * (i - cp1);
            } else {
                mean = 5.0 + 0.20 * (cp2 - cp1);
            }
            values[i] = mean + random.nextGaussian() * 0.5;
        }
        List<Integer> breaks = breakIndexes(detector(24).detect(values));
        assertTrue(
            "expected boundaries near " + cp1 + " and " + cp2 + ", got " + breaks,
            hasNear(breaks, cp1, 16) && hasNear(breaks, cp2, 16)
        );
    }

    public void testHeteroscedasticSeriesWithoutLevelShiftHasNoStructuralBreak() {
        // The mean is constant; only the noise level changes. That is a distribution change (the EventDetector's
        // dispersion channel), NOT a step/trend on the value channel — so the value channel must stay quiet.
        Random random = new Random(5);
        double[] values = new double[260];
        for (int i = 0; i < values.length; i++) {
            values[i] = 50.0 + random.nextGaussian() * (i < 130 ? 0.5 : 6.0);
        }
        assertTrue("a variance-only change must not be a structural break", breakIndexes(detector(24).detect(values)).isEmpty());
    }

    public void testIsBoundedOnAPeriodicSignal() {
        double[] values = new double[300];
        for (int i = 0; i < values.length; i++) {
            values[i] = Math.sin(2.0 * Math.PI * i / 30.0);
        }
        assertTrue("out-of-domain periodic data must not explode into breaks", breakIndexes(detector(20).detect(values)).size() <= 15);
    }

    public void testFuzzedSeriesPreserveCoreInvariants() {
        int n = 240;
        int minSeg = 20;
        for (int seed = 0; seed < 12; seed++) {
            Random random = new Random(seed);
            double[] values = new double[n];
            double level = 0.0;
            for (int i = 0; i < n; i++) {
                if (random.nextDouble() < 0.02) {
                    level += random.nextGaussian() * 5.0; // occasional level shifts
                }
                values[i] = level + random.nextGaussian() * 2.0;
            }
            List<Integer> breaks = breakIndexes(detector(minSeg).detect(values)); // must not throw
            assertTrue("break count must stay bounded, seed=" + seed + " got " + breaks, breaks.size() <= n / minSeg + 2);
            for (int i = 1; i < breaks.size(); i++) {
                assertTrue("break indexes must be strictly increasing, seed=" + seed, breaks.get(i) > breaks.get(i - 1));
            }
        }
    }

    // ---- stationary / non-stationary classification (reported only when there is no change) --------------------

    public void testFlatNoisySeriesIsClassifiedStationary() {
        Random random = new Random(6);
        double[] values = new double[200];
        for (int i = 0; i < values.length; i++) {
            values[i] = 30.0 + random.nextGaussian() * 2.0;
        }
        List<ChangeType> events = detector(20).detect(values);
        assertTrue("a flat noisy series has no structural break", breakIndexes(events).isEmpty());
        assertTrue("it should be classified Stationary: " + events, events.stream().anyMatch(e -> e instanceof ChangeType.Stationary));
    }

    public void testSmoothRampIsClassifiedNonStationary() {
        double[] values = new double[200];
        for (int i = 0; i < values.length; i++) {
            values[i] = 0.1 * i; // a clean monotone ramp, no break
        }
        List<ChangeType> events = detector(20).detect(values);
        assertTrue("a smooth ramp has no structural break", breakIndexes(events).isEmpty());
        assertTrue(
            "it should be classified NonStationary: " + events,
            events.stream().anyMatch(e -> e instanceof ChangeType.NonStationary)
        );
        assertFalse("a ramp is not stationary: " + events, events.stream().anyMatch(e -> e instanceof ChangeType.Stationary));
    }

    private static double[] withNoise(double[] base, double sigma, Random random) {
        double[] values = new double[base.length];
        for (int i = 0; i < base.length; i++) {
            values[i] = base[i] + random.nextGaussian() * sigma;
        }
        return values;
    }

    private static double[] step(int n, int cp, double low, double high) {
        double[] values = new double[n];
        for (int i = 0; i < n; i++) {
            values[i] = i < cp ? low : high;
        }
        return values;
    }

    private static List<Integer> breakIndexes(List<ChangeType> events) {
        List<Integer> indexes = new ArrayList<>();
        for (ChangeType e : events) {
            if (e instanceof ChangeType.StepChange || e instanceof ChangeType.TrendChange) {
                indexes.add(e.changePoint());
            }
        }
        return indexes;
    }

    private static boolean hasNear(List<Integer> values, int target, int tolerance) {
        for (int v : values) {
            if (Math.abs(v - target) <= tolerance) {
                return true;
            }
        }
        return false;
    }
}
