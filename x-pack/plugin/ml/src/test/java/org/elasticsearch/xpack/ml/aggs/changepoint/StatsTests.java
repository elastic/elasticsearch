/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.test.ESTestCase;

import java.util.Random;

/** Smoke tests for the statistical utilities in {@link Stats}. */
public class StatsTests extends ESTestCase {

    public void testMeanAndMedian() {
        assertEquals(2.0, Stats.mean(new double[] { 1.0, 2.0, 3.0 }), 1e-12);
        assertEquals(2.0, Stats.median(new double[] { 3.0, 1.0, 2.0 }), 1e-12);   // odd
        assertEquals(2.5, Stats.median(new double[] { 4.0, 1.0, 3.0, 2.0 }), 1e-12); // even -> average of middle two
        assertEquals(0.0, Stats.median(new double[0]), 1e-12);
    }

    public void testWeightedMeanAndRss() {
        double[] v = { 1.0, 3.0, 5.0 };
        double[] w = { 1.0, 1.0, 1.0 };
        assertEquals(3.0, Stats.weightedMean(v, w, 0, 3), 1e-12);
        // RSS about the (weighted) mean of {1,3,5} is 4 + 0 + 4 = 8.
        assertEquals(8.0, Stats.weightedRss(v, w, 0, 3), 1e-9);
        // A zero-weighted extreme point does not move the mean.
        assertEquals(3.0, Stats.weightedMean(new double[] { 1.0, 3.0, 5.0, 1000.0 }, new double[] { 1, 1, 1, 0 }, 0, 4), 1e-9);
    }

    public void testRollingMedianResidualsAreZeroOnConstantAndPeakOnASpike() {
        double[] constant = new double[40];
        java.util.Arrays.fill(constant, 7.0);
        for (double r : Stats.rollingMedianResiduals(constant, 4)) {
            assertEquals(0.0, r, 1e-12);
        }

        double[] spiky = new double[40];
        java.util.Arrays.fill(spiky, 7.0);
        spiky[20] = 100.0;
        double[] residuals = Stats.rollingMedianResiduals(spiky, 4);
        assertEquals("the spike sits well above its local median", 93.0, residuals[20], 1e-9);
        assertEquals("a point far from the spike has no residual", 0.0, residuals[5], 1e-12);
    }

    public void testNoiseVarianceRecoversTheScaleAndIgnoresLevel() {
        Random random = new Random(13);
        double[] v = new double[400];
        for (int i = 0; i < v.length; i++) {
            v[i] = 1000.0 + random.nextGaussian() * 4.0; // sigma = 4, large constant level
        }
        double sigma = Math.sqrt(Stats.globalNoiseVariance(v));
        assertTrue("recovered sigma should be near 4, was " + sigma, sigma > 2.0 && sigma < 8.0);
        // globalNoiseVariance is just localNoiseVariance over the whole series.
        assertEquals(Stats.localNoiseVariance(v, 0, v.length), Stats.globalNoiseVariance(v), 1e-9);

        double[] constant = new double[40];
        java.util.Arrays.fill(constant, 5.0);
        assertTrue("a constant series has ~zero noise variance", Stats.globalNoiseVariance(constant) < 1e-6);
    }

    public void testInterquartileNoiseVarianceTracksSpread() {
        Random random = new Random(5);
        double[] quiet = new double[120];
        double[] noisy = new double[120];
        for (int i = 0; i < 120; i++) {
            quiet[i] = random.nextGaussian() * 0.5;
            noisy[i] = random.nextGaussian() * 5.0;
        }
        assertTrue(
            "the noisier window must have the larger inter-quartile noise variance",
            Stats.interquartileNoiseVariance(noisy, 0, 120) > Stats.interquartileNoiseVariance(quiet, 0, 120)
        );
    }

    public void testCompositeScaleIsPositiveAndCollapseResistant() {
        // Pure MAD would be zero here (>half the residuals identical); the composite stays positive via the floor.
        double[] mostlyZero = new double[20];
        mostlyZero[3] = 10.0;
        mostlyZero[9] = 12.0;
        assertTrue(Stats.compositeScale(mostlyZero, 12.0) > 0.0);

        double[] spread = { -2, -1, 0, 1, 2, -1.5, 1.5, 0.5, -0.5, 0 };
        assertTrue("scale should reflect the spread of the residuals", Stats.compositeScale(spread, 2.0) > 0.5);
    }

    public void testLocalRobustScaleIsWindowedAndFloored() {
        double[] residuals = new double[40];
        // Quiet first half, noisy second half.
        Random random = new Random(1);
        for (int i = 0; i < 20; i++) {
            residuals[i] = random.nextGaussian() * 0.1;
        }
        for (int i = 20; i < 40; i++) {
            residuals[i] = random.nextGaussian() * 5.0;
        }
        double quiet = Stats.localRobustScale(residuals, 0, 12, 5.0);
        double noisy = Stats.localRobustScale(residuals, 28, 40, 5.0);
        assertTrue("the local scale must adapt to the local noise level", noisy > quiet);
        assertTrue("a flat window is floored, never zero", Stats.localRobustScale(new double[10], 0, 10, 1.0) > 0.0);
    }

    public void testKdeBandwidthAndTailProbability() {
        assertEquals("constant background has zero bandwidth", 0.0, Stats.kdeBandwidth(new double[] { 3.0, 3.0, 3.0 }), 1e-12);

        Random random = new Random(7);
        double[] background = new double[200];
        for (int i = 0; i < background.length; i++) {
            background[i] = random.nextGaussian();
        }
        double bandwidth = Stats.kdeBandwidth(background);
        assertTrue(bandwidth > 0.0);

        double farTail = Stats.kdeTailProbability(8.0, background, bandwidth, 1);
        double atCentre = Stats.kdeTailProbability(0.0, background, bandwidth, 1);
        assertTrue("a value far above the background has a tiny upper-tail probability", farTail < 1e-3);
        assertTrue("a central value sits near the median of the upper tail", atCentre > 0.3 && atCentre < 0.7);

        // With a degenerate bandwidth the tail is the empirical fraction beyond the value.
        double empirical = Stats.kdeTailProbability(1.0, new double[] { 0, 0, 2, 2 }, 0.0, 1);
        assertEquals(0.5, empirical, 1e-12);
    }

    public void testWindowedDispersionIsNullWhenTooShortAndRisesWithVariance() {
        assertNull("too few points for two dispersion regimes", Stats.windowedDispersion(new double[10], 3, 8));

        Random random = new Random(3);
        double[] v = new double[240];
        for (int i = 0; i < v.length; i++) {
            double sigma = i < 120 ? 0.5 : 6.0;
            v[i] = 100.0 + random.nextGaussian() * sigma;
        }
        double[] channel = Stats.windowedDispersion(v, 3, 8);
        assertEquals("240 points / window 8 = 30 channel samples", 30, channel.length);
        double firstHalf = Stats.mean(java.util.Arrays.copyOfRange(channel, 0, 15));
        double secondHalf = Stats.mean(java.util.Arrays.copyOfRange(channel, 15, 30));
        assertTrue("the higher-variance half has a higher dispersion-channel level", secondHalf > firstHalf + 1.0);
    }

    public void testStabilizeRssIsAPositiveRegularizer() {
        assertTrue("stabilized RSS is strictly positive", Stats.stabilizeRss(0.0, 100, 4.0) > 0.0);
        assertTrue("stabilization only ever adds to the RSS", Stats.stabilizeRss(10.0, 100, 4.0) > 10.0);
    }
}
