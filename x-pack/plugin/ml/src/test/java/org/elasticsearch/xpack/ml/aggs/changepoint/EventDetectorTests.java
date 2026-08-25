/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;

import java.util.List;
import java.util.Random;

/**
 * Tests for {@link EventDetector}, the top-level entry point. Focus areas: de-duplication of coincident
 * structural/distribution events, the independence of the spike/dip stream, the dispersion (distribution-change)
 * path end to end, and remapping of value-array indices back to source bucket indices (including empty buckets).
 */
public class EventDetectorTests extends ESTestCase {

    public void testDetectsAStepAndReportsItOnce() {
        double[] values = step(200, 100, 2.0, 11.0);
        List<ChangeType> events = new EventDetector(20).detect(buckets(values));
        assertEquals("a single step should be reported exactly once near 100: " + events, 1, structuralOrDistributionNear(events, 100, 12));
    }

    public void testDeduplicatesCoincidentStructuralAndDistributionChange() {
        // A boundary where BOTH the level and the noise change: the value channel sees a step and the dispersion
        // channel sees a variance change at the same place. They must collapse to a single reported event.
        Random random = new Random(31);
        int n = 240;
        int cp = 120;
        double[] values = new double[n];
        for (int i = 0; i < n; i++) {
            values[i] = i < cp ? 5.0 + random.nextGaussian() * 0.5 : 50.0 + random.nextGaussian() * 5.0;
        }
        List<ChangeType> events = new EventDetector(24).detect(buckets(values));
        assertEquals(
            "the coincident level+variance change must be reported once: " + events,
            1,
            structuralOrDistributionNear(events, cp, 30)
        );
    }

    public void testReportsAStructuralBreakAndACoincidentSpikeSeparately() {
        // Pulses are added after de-duplication, so a spike that lands on a structural boundary is not suppressed:
        // both the step and the spike should be reported.
        Random random = new Random(5);
        int n = 220;
        int cp = 110;
        double[] values = new double[n];
        for (int i = 0; i < n; i++) {
            values[i] = (i < cp ? 0.0 : 100.0) + random.nextGaussian() * 3.0;
        }
        values[cp + 3] += 250.0; // a spike just inside the new regime
        List<ChangeType> events = new EventDetector(20).detect(buckets(values));
        assertTrue("the step should still be reported: " + events, structuralOrDistributionNear(events, cp, 12) >= 1);
        assertTrue(
            "the coincident spike should also be reported: " + events,
            events.stream().anyMatch(e -> e instanceof ChangeType.Spike && Math.abs(e.changePoint() - (cp + 3)) <= 4)
        );
    }

    public void testDetectsAVarianceChangeAsADistributionChange() {
        // Mean constant, only the noise level changes; invisible to the value channel, surfaced by the dispersion
        // channel as a DistributionChange.
        Random random = new Random(99);
        int n = 280;
        int cp = 130;
        double[] values = new double[n];
        for (int i = 0; i < n; i++) {
            values[i] = 100.0 + random.nextGaussian() * (i < cp ? 0.5 : 3.0);
        }
        List<ChangeType> events = new EventDetector(24).detect(buckets(values));
        assertTrue(
            "a variance change should be a DistributionChange near the boundary: " + events,
            events.stream()
                .anyMatch(e -> e instanceof ChangeType.DistributionChange && Math.abs(e.changePoint() - cp) <= Math.max(8, n / 12))
        );
    }

    public void testPlacesADistributionChangeAtTheVarianceBoundary() {
        // A clean variance step with a constant mean, at a boundary aligned to the dispersion window. The dispersion
        // channel detects the step; the channel->value remap must land on the boundary (window * k), not the centre
        // of the first new-regime window, which lagged the true boundary by half a window. Localisation is limited to
        // +/- one window (the channel has one sample per window), but the estimate must be centred on the boundary
        // rather than offset to one side.
        Random random = new Random(7);
        int n = 256;
        int cp = 128; // a multiple of the (8-bucket) dispersion window, so the boundary is unambiguous
        double[] values = new double[n];
        for (int i = 0; i < n; i++) {
            values[i] = 100.0 + random.nextGaussian() * (i < cp ? 0.3 : 4.0);
        }
        List<ChangeType> events = new EventDetector(24).detect(buckets(values));
        assertTrue(
            "the distribution change should sit at the variance boundary near " + cp + ": " + events,
            events.stream().anyMatch(e -> e instanceof ChangeType.DistributionChange && Math.abs(e.changePoint() - cp) <= 8)
        );
    }

    public void testDoesNotReportAMeanStepAsADistributionChange() {
        double[] values = step(200, 100, 2.0, 9.0);
        List<ChangeType> events = new EventDetector(20).detect(buckets(values));
        assertTrue(
            "a pure step is not a distribution change: " + events,
            events.stream().noneMatch(e -> e instanceof ChangeType.DistributionChange)
        );
        assertTrue("the step itself should be detected: " + events, structuralOrDistributionNear(events, 100, 8) >= 1);
    }

    public void testDetectsALowHighLowVarianceBumpAsADistributionChange() {
        Random random = new Random(3);
        int n = 180;
        double[] values = new double[n];
        for (int i = 0; i < n; i++) {
            values[i] = 50.0 + random.nextGaussian() * ((i >= 45 && i < 120) ? 3.0 : 0.3);
        }
        List<ChangeType> events = new EventDetector(24).detect(buckets(values));
        assertTrue(
            "a low-high-low variance bump should yield a DistributionChange at a boundary: " + events,
            events.stream().anyMatch(e -> e instanceof ChangeType.DistributionChange && e.changePoint() >= 35 && e.changePoint() <= 135)
        );
    }

    public void testRemapsEventIndicesThroughEmptyBuckets() {
        // The detector works in value-array space; the events must be reported in source-bucket space. Here every
        // array position maps to bucket 3*position (two empty buckets between each), so a step at array index 90
        // must be reported near source bucket 270, not 90.
        int n = 180;
        int arrayCp = 90;
        double[] values = step(n, arrayCp, 1.0, 9.0);
        long[] docCounts = new long[n];
        int[] sourceBuckets = new int[n];
        for (int i = 0; i < n; i++) {
            docCounts[i] = 1L;
            sourceBuckets[i] = 3 * i;
        }
        List<ChangeType> events = new EventDetector(20).detect(new MlAggsHelper.DoubleBucketValues(docCounts, values, sourceBuckets));

        assertTrue(
            "the step should be reported near source bucket " + (3 * arrayCp) + ", got " + events,
            events.stream().anyMatch(e -> isStructural(e) && Math.abs(e.changePoint() - 3 * arrayCp) <= 3 * 8)
        );
        assertTrue(
            "events must be in source-bucket space, not value-array space: " + events,
            events.stream().filter(EventDetectorTests::isStructural).noneMatch(e -> Math.abs(e.changePoint() - arrayCp) <= 8)
        );
    }

    public void testReturnsEmptyForAShortOrEmptySeries() {
        assertTrue(new EventDetector(20).detect(buckets(new double[] { 1.0, 2.0, 3.0 })).stream().noneMatch(ChangeType::isChange));
        assertTrue(new EventDetector(20).detect(buckets(new double[0])).stream().noneMatch(ChangeType::isChange));
    }

    public void testDetectsANoisyStepEndToEnd() {
        Random random = new Random(11);
        double[] values = new double[220];
        for (int i = 0; i < values.length; i++) {
            values[i] = (i < 110 ? 0.0 : 12.0) + random.nextGaussian() * 1.0;
        }
        List<ChangeType> events = new EventDetector(20).detect(buckets(values));
        assertTrue("expected a noisy step end to end: " + events, structuralOrDistributionNear(events, 110, 12) >= 1);
    }

    public void testClassifiesAFlatNoisySeriesAsStationary() {
        Random random = new Random(12);
        double[] values = new double[200];
        for (int i = 0; i < values.length; i++) {
            values[i] = 40.0 + random.nextGaussian() * 2.0;
        }
        List<ChangeType> events = new EventDetector(20).detect(buckets(values));
        assertTrue(
            "a flat noisy series should be Stationary: " + events,
            events.stream().anyMatch(e -> e instanceof ChangeType.Stationary)
        );
        assertTrue("no structural change should be reported: " + events, events.stream().noneMatch(EventDetectorTests::isStructural));
    }

    public void testClassificationWithExplicitBucketsIsNotRemappedAndDoesNotThrow() {
        // Regression: a stationary/non-stationary result carries NO_CHANGE_POINT (-1); remapping it would index
        // an explicit bucket array at -1. With sparse buckets the classification must pass through unremapped.
        Random random = new Random(13);
        int n = 200;
        double[] values = new double[n];
        long[] docCounts = new long[n];
        int[] sourceBuckets = new int[n];
        for (int i = 0; i < n; i++) {
            values[i] = 40.0 + random.nextGaussian() * 2.0;
            docCounts[i] = 1L;
            sourceBuckets[i] = 3 * i;
        }
        // Pre-fix this threw ArrayIndexOutOfBoundsException (getBucketIndex(-1) on the explicit bucket array);
        // returning at all is the regression check.
        List<ChangeType> events = new EventDetector(20).detect(new MlAggsHelper.DoubleBucketValues(docCounts, values, sourceBuckets));
        assertTrue(
            "expected a stationary/non-stationary classification: " + events,
            events.stream().anyMatch(e -> e instanceof ChangeType.Stationary || e instanceof ChangeType.NonStationary)
        );
    }

    public void testLongStationarySeriesIsDownsampledAndClassifiedWithoutThrowing() {
        // Above the downsample cap the series carries an explicit bucket array, so the classification path must
        // still not attempt to remap NO_CHANGE_POINT.
        Random random = new Random(14);
        double[] values = new double[2500];
        for (int i = 0; i < values.length; i++) {
            values[i] = 7.0 + random.nextGaussian() * 1.0;
        }
        List<ChangeType> events = new EventDetector(20).detect(buckets(values));
        assertTrue(
            "a long flat series should be classified without throwing: " + events,
            events.stream().anyMatch(e -> e instanceof ChangeType.Stationary || e instanceof ChangeType.NonStationary)
        );
    }

    private static boolean isStructural(ChangeType e) {
        return e instanceof ChangeType.StepChange || e instanceof ChangeType.TrendChange || e instanceof ChangeType.DistributionChange;
    }

    private static int structuralOrDistributionNear(List<ChangeType> events, int target, int tolerance) {
        return (int) events.stream()
            .filter(EventDetectorTests::isStructural)
            .filter(e -> Math.abs(e.changePoint() - target) <= tolerance)
            .count();
    }

    private static double[] step(int n, int cp, double low, double high) {
        double[] values = new double[n];
        for (int i = 0; i < n; i++) {
            values[i] = i < cp ? low : high;
        }
        return values;
    }

    private static MlAggsHelper.DoubleBucketValues buckets(double[] values) {
        long[] docCounts = new long[values.length];
        java.util.Arrays.fill(docCounts, 1L);
        return new MlAggsHelper.DoubleBucketValues(docCounts, values);
    }
}
