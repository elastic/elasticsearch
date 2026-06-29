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

    public void testDetectsSpikesOnAFlatIntegerBaseline() {
        // Almost all zeros with a few isolated integer counts. The background (excursions removed) is a constant
        // zero, so the KDE bandwidth collapses and the empirical-tail fallback, Bonferroni-corrected over the
        // series, cannot call any point significant. The quantization-step bandwidth floor rescues this: each
        // isolated count is a rise-and-fall pair, so the smallest count (1) recurs and sets a granularity of 1,
        // giving a minimal bandwidth against which the clear multi-count outliers stand out.
        double[] values = new double[359];
        values[14] = 1.0;
        values[222] = 15.0;
        values[282] = 2.0;
        values[330] = 7.0;
        List<ChangeType> events = detector(20).detect(values);
        assertTrue(
            "the largest outlier (15) on a flat zero baseline must be a spike, got " + events,
            events.stream().anyMatch(e -> e instanceof ChangeType.Spike && Math.abs(e.changePoint() - 222) <= 1)
        );
        assertTrue(
            "the clear outlier (7) on a flat zero baseline must be a spike, got " + events,
            events.stream().anyMatch(e -> e instanceof ChangeType.Spike && Math.abs(e.changePoint() - 330) <= 1)
        );
    }

    public void testDetectsAWithinRegimeSpikeOnASparseStructuredSeries() {
        // A mostly-zero baseline with an active regime that has two sub-levels (~1400 then ~3300). The sharp
        // sub-level transition produces a large rolling-median residual, so a plain std of the background
        // residuals is inflated by it and the KDE bandwidth is wide enough to smear a within-regime spike.
        // The Winsorized std clips the transition residual and recovers the within-regime noise, so a spike
        // clearly above the regime is detected.
        Random random = new Random(7);
        double[] values = new double[360];
        for (int i = 200; i < 240; i++) {
            values[i] = 1400.0 + 80.0 * random.nextGaussian();
        }
        for (int i = 240; i < 300; i++) {
            values[i] = 3300.0 + 150.0 * random.nextGaussian();
        }
        values[270] = 5600.0; // within-regime spike, well above the ~3300 sub-level
        List<ChangeType> events = detector(20).detect(values);
        assertTrue(
            "a within-regime spike on a sparse, internally-structured series should be detected, got " + events,
            events.stream().anyMatch(e -> e instanceof ChangeType.Spike && Math.abs(e.changePoint() - 270) <= 2)
        );
    }

    public void testAsinhGateKeepsAFarAnomalyAboveAHeavyTailedPopulation() {
        // Multiplicative, heavy-tailed background (large values span orders of magnitude and are part of the
        // typical distribution). The asinh-stabilised value gate must still flag a value sitting far above
        // the entire tail. (The complementary property — that magnitudes typical of the tail are NOT flagged
        // — is exercised against real telemetry; here we pin that a genuine, order-of-magnitude-above anomaly
        // is still recalled.)
        Random random = new Random(3);
        int n = 400;
        double[] values = new double[n];
        for (int i = 0; i < n; i++) {
            values[i] = Math.exp(Math.log(5e8) + random.nextGaussian() * 1.4); // ~1e7 .. ~1e11
        }
        values[50] = 0.0;
        values[177] = 0.0;
        values[310] = 0.0;
        int anomaly = 300;
        values[anomaly] = 2e13; // far above the whole tail
        List<ChangeType> events = detector(16).detect(values);
        assertTrue(
            "the far anomaly above a heavy-tailed population must still be reported, got " + events,
            events.stream().anyMatch(e -> e instanceof ChangeType.Spike && Math.abs(e.changePoint() - anomaly) <= 2)
        );
    }

    public void testKeepsAWithinRegimeSpikeAfterAStep() {
        // A large level step makes the value distribution bimodal. The KDE bandwidth must come from the within-
        // regime residual, not the (step-inflated) value spread: otherwise the kernel is so wide that a genuine
        // spike that is extreme within its own regime, but small next to the size of the step, is smeared away.
        Random random = new Random(11);
        int n = 300;
        double[] values = new double[n];
        for (int i = 0; i < 150; i++) {
            values[i] = 1.0e8 + random.nextGaussian() * 2.0e6;  // regime 1
        }
        for (int i = 150; i < n; i++) {
            values[i] = 1.0e10 + random.nextGaussian() * 2.0e8; // regime 2 (a 100x step up)
        }
        values[225] = 1.0e10 + 3.0e9; // ~15 within-regime sigma, tiny next to the step
        List<ChangeType> events = detector(16).detect(values);
        assertTrue(
            "a within-regime spike after a step must still be flagged (bandwidth from residuals, not values): " + events,
            events.stream().anyMatch(e -> e instanceof ChangeType.Spike && Math.abs(e.changePoint() - 225) <= 2)
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

    public void testDetectsASpikeNearTheStartOfTheSeries() {
        // A spike at index 2 sits inside the boundary region where the centred rolling-median residual collapses;
        // the Theil-Sen boundary line restores its residual so it is proposed and gated like an interior spike.
        Random random = new Random(44);
        double[] values = noise(160, 100.0, 3.0, random);
        values[2] += 200.0;
        List<ChangeType> events = detector(16).detect(values);
        assertTrue("a spike near the start should be reported as a Spike, got " + events, hasSpikeNear(events, 2, 2));
    }

    public void testDetectsADipAtTheEndOfTheSeries() {
        Random random = new Random(45);
        double[] values = noise(160, 100.0, 3.0, random);
        values[158] -= 80.0;
        List<ChangeType> events = detector(16).detect(values);
        assertTrue("a dip near the end should be reported as a Dip, got " + events, hasDipNear(events, 158, 2));
    }

    public void testDoesNotReportTrendEndpointsAsSpikes() {
        // A clean linear ramp: the boundary line fits the endpoints exactly, so their restored residual is ~0
        // and no boundary excursion is manufactured. Regression guard for the boundary-residual treatment.
        Random random = new Random(46);
        double[] values = new double[160];
        for (int i = 0; i < values.length; i++) {
            values[i] = 10.0 + 0.5 * i + random.nextGaussian() * 1.0;
        }
        List<ChangeType> events = detector(16).detect(values);
        assertTrue(
            "a clean ramp must not produce boundary spikes, got " + events,
            events.stream().noneMatch(e -> e.changePoint() <= 4 || e.changePoint() >= values.length - 5)
        );
    }

    public void testDoesNotReportABoundaryAnomalyOnAQuantizedMonotoneCounter() {
        // A rising counter that repeats values (so >50% of first differences are zero) collapses a median
        // of |diff| noise scale to ~0. Without the IQR movement-scale floor, the trend's extreme endpoint
        // - the global minimum - is mis-proposed and the value gate (which trivially calls the series minimum
        // a lower-tail event) reports it as a spurious boundary dip. Regression guard for that interaction.
        Random random = new Random(7);
        double[] values = new double[320];
        double level = 2.14e12;
        for (int i = 0; i < values.length; i++) {
            if (random.nextDouble() < 0.45) {
                level += 5.0e7 + random.nextDouble() * 1.5e8; // irregular upward steps, with long repeated runs
            }
            values[i] = level;
        }
        List<ChangeType> events = detector(16).detect(values);
        assertTrue(
            "a rising quantized counter must not produce boundary spikes/dips, got " + events,
            events.stream()
                .noneMatch(
                    e -> (e instanceof ChangeType.Dip || e instanceof ChangeType.Spike)
                        && (e.changePoint() <= 4 || e.changePoint() >= values.length - 5)
                )
        );
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

    private static boolean hasDipNear(List<ChangeType> events, int target, int tolerance) {
        return events.stream().anyMatch(e -> e instanceof ChangeType.Dip && Math.abs(e.changePoint() - target) <= tolerance);
    }
}
