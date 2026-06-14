/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Random;

/** Behavioural tests for {@link PulseDetector}: point spike/dip excursions from a local baseline. */
public class PulseDetectorTests extends ESTestCase {

    private static final double P_VALUE = 0.01;

    private static PulseDetector detector(int minSegmentLength) {
        return new PulseDetector(minSegmentLength, P_VALUE);
    }

    public void testDetectsAnIsolatedSpike() {
        Random random = new Random(42);
        double[] values = noise(160, 100.0, 3.0, random);
        values[80] += 200.0;
        List<ChangeType> events = detector(16).detect(values);
        assertTrue("an isolated spike should be reported as a Spike, got " + events, hasSpikeNear(events, 80, 2));
    }

    public void testDetectsAnIsolatedDip() {
        Random random = new Random(43);
        double[] values = noise(160, 100.0, 3.0, random);
        values[80] -= 80.0;
        List<ChangeType> events = detector(16).detect(values);
        assertTrue(
            "an isolated dip should be reported as a Dip, got " + events,
            events.stream().anyMatch(e -> e instanceof ChangeType.Dip && Math.abs(e.changePoint() - 80) <= 2)
        );
    }

    public void testDetectsALargeSpikeOnAHighMagnitudeSeries() {
        Random random = new Random(7);
        double[] values = new double[150];
        for (int i = 0; i < values.length; i++) {
            values[i] = 1.0e9 + random.nextGaussian() * 1.0e8;
        }
        values[112] = 1.44e11;
        assertTrue("a huge spike should be reported regardless of scale", hasSpikeNear(detector(16).detect(values), 112, 2));
    }

    public void testReportsSeveralDistinctSpikesWithoutMasking() {
        // The single shared null (with all tested excursions removed) must let multiple genuinely distinct spikes
        // all survive — the leave-one-out null used to let the largest mask the rest.
        Random random = new Random(2);
        double[] values = noise(150, 100.0, 5.0, random);
        for (int p : new int[] { 30, 70, 110 }) {
            values[p] += 400.0;
        }
        List<ChangeType> events = detector(16).detect(values);
        assertTrue(
            "all three distinct spikes should be reported, got " + events,
            hasSpikeNear(events, 30, 2) && hasSpikeNear(events, 70, 2) && hasSpikeNear(events, 110, 2)
        );
    }

    public void testDoesNotFlagARecurringPopulation() {
        // A periodic train of equal-magnitude peaks is a frequent population, not a set of point anomalies: the
        // robust scale inflates so they never reach the candidate list.
        double[] values = new double[150];
        for (int i = 0; i < values.length; i++) {
            values[i] = (i % 3 == 0) ? 300.0 : 0.0;
        }
        assertTrue("a recurring peak population must not be reported as spikes", detector(16).detect(values).isEmpty());
    }

    public void testDoesNotReportAWideExcursionAsAPointPulse() {
        // A wide (sustained) excursion is tracked by the rolling-median baseline and absorbed; it belongs to the
        // structural/dispersion channels, not the point-pulse stream.
        double[] values = new double[120];
        java.util.Arrays.fill(values, 5.0);
        for (int i = 50; i < 56; i++) {
            values[i] = 500.0;
        }
        List<ChangeType> events = detector(16).detect(values);
        assertTrue(
            "a wide excursion must not be reported as a point pulse, got " + events,
            events.stream().noneMatch(e -> e.changePoint() >= 48 && e.changePoint() <= 58)
        );
    }

    public void testLimitsTheNumberOfReportedPulses() {
        // Many isolated spikes: the output is capped at max(5, 2% of n).
        double[] values = noise(400, 100.0, 4.0, new Random(9));
        for (int p = 20; p < 400; p += 20) {
            values[p] += 300.0 + p; // 19 distinct spikes
        }
        int cap = Math.max(5, (int) Math.ceil(0.02 * 400)); // = 8
        assertTrue("the pulse count must be capped at " + cap, detector(16).detect(values).size() <= cap);
    }

    public void testReturnsNothingOnConstantOrShortSeries() {
        double[] constant = new double[120];
        java.util.Arrays.fill(constant, 7.0);
        assertTrue("a constant series has no excursions", detector(16).detect(constant).isEmpty());
        assertTrue("a too-short series yields nothing", detector(16).detect(new double[] { 1, 2, 1 }).isEmpty());
    }

    private static double[] noise(int n, double level, double sigma, Random random) {
        double[] values = new double[n];
        for (int i = 0; i < n; i++) {
            values[i] = level + random.nextGaussian() * sigma;
        }
        return values;
    }

    private static boolean hasSpikeNear(List<ChangeType> events, int target, int tolerance) {
        return events.stream().anyMatch(e -> e instanceof ChangeType.Spike && Math.abs(e.changePoint() - target) <= tolerance);
    }
}
