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

    public void testQuantizationStepRecoversGranularityAndIgnoresRareSmallGaps() {
        double[] integers = new double[144];
        for (int i = 0; i < integers.length; i++) {
            integers[i] = (i % 4 == 3) ? 4.0 : 5.0;
        }
        assertTrue("an integer oscillation resolves to a step of 1", Stats.quantizationStep(integers, 0, integers.length) > 0.0);

        // One rare sub-integer value introduces a 0.1 gap; it occurs once and so does not recur, while the integer
        // (size-1) differences recur many times - so the estimate stays at the bulk granularity, not the lone gap.
        integers[70] = 4.9;
        assertTrue(
            "a single sub-granularity gap must not collapse the quantization step",
            Stats.quantizationStep(integers, 0, integers.length) > 0.5
        );

        double[] constant = new double[50];
        assertEquals("an exactly constant series has no step", 0.0, Stats.quantizationStep(constant, 0, constant.length), 0.0);

        // A clean step series' only nonzero differences ARE the step jumps - distinct and isolated, so none recurs and
        // the step is 0 (no floor). This is the crucial property: a noiseless step must not be read as "quantized" at
        // the step size, which would floor away the very change we want to detect.
        double[] step = new double[180];
        for (int i = 90; i < step.length; i++) {
            step[i] = 9.0;
        }
        assertEquals("an isolated step jump does not recur, so the step is zero", 0.0, Stats.quantizationStep(step, 0, step.length), 0.0);

        // Continuous (Gaussian) data: differences are all distinct, so nothing recurs and there is no quantization.
        Random random = new Random(17);
        double[] continuous = new double[200];
        for (int i = 0; i < continuous.length; i++) {
            continuous[i] = 100.0 + 3.0 * random.nextGaussian();
        }
        assertEquals("continuous data has no recurring difference", 0.0, Stats.quantizationStep(continuous, 0, continuous.length), 0.0);
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

    public void testKdeBandwidthFloorRescuesADegenerateBackground() {
        // A constant background collapses the estimated spread to zero, so the unfloored bandwidth is zero (the tail
        // then falls back to an empirical step that cannot resolve an outlier). A positive bandwidth floor keeps a
        // minimal kernel width, so a clear outlier gets a tiny upper-tail probability instead of a coin-flip.
        double[] flat = new double[200];
        assertEquals("a constant background has zero bandwidth unfloored", 0.0, Stats.kdeBandwidth(flat), 1e-12);
        double floored = Stats.kdeBandwidth(flat, 0.3);
        assertEquals("the floor sets the bandwidth directly on a degenerate background", 0.3, floored, 1e-12);
        assertTrue(
            "with a floored bandwidth a clear outlier above a flat background is highly significant",
            Stats.kdeTailProbability(4.0, flat, floored, 1) < 1e-3
        );
        // The floor only raises the bandwidth; a background with real spread above the floor is unchanged.
        Random random = new Random(11);
        double[] noisy = new double[200];
        for (int i = 0; i < noisy.length; i++) {
            noisy[i] = random.nextGaussian() * 5.0;
        }
        assertEquals(
            "a small floor leaves a well-spread background unchanged",
            Stats.kdeBandwidth(noisy),
            Stats.kdeBandwidth(noisy, 0.01),
            1e-12
        );
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

    public void testRange() {
        assertEquals(6.0, Stats.range(new double[] { 3.0, 7.0, 1.0, 5.0 }), 1e-12);
        assertEquals("a constant array has zero range", 0.0, Stats.range(new double[] { 4.0, 4.0, 4.0 }), 1e-12);
        assertEquals("an empty array has zero range", 0.0, Stats.range(new double[0]), 1e-12);
    }

    public void testTheilSenLineRecoversALineAndIsRobustToASpike() {
        // y = 2 + 3 x on [0, 6): every pairwise slope is 3 and every intercept is 2.
        double[] clean = { 2.0, 5.0, 8.0, 11.0, 14.0, 17.0 };
        double[] line = Stats.theilSenLine(clean, 0, clean.length);
        assertEquals("intercept at start", 2.0, line[0], 1e-9);
        assertEquals("slope", 3.0, line[1], 1e-9);

        // A single gross outlier (index 3) must not move the median slope or intercept.
        double[] spiky = { 2.0, 5.0, 8.0, 100.0, 14.0, 17.0, 20.0 };
        double[] robust = Stats.theilSenLine(spiky, 0, spiky.length);
        assertEquals("Theil-Sen ignores the lone spike's slope", 3.0, robust[1], 1e-9);
        assertEquals("Theil-Sen ignores the lone spike's intercept", 2.0, robust[0], 1e-9);

        // The intercept is reported at the window start, so a sub-range is fit in its own local coordinates.
        double[] offsetLine = { -100.0, -100.0, 2.0, 5.0, 8.0, 11.0 };
        double[] sub = Stats.theilSenLine(offsetLine, 2, 4);
        assertEquals("intercept at the sub-range start", 2.0, sub[0], 1e-9);
        assertEquals(3.0, sub[1], 1e-9);
    }

    public void testQuantizationStepRecoversGranularityAndIgnoresStepsAndNoise() {
        // Many unit first-differences -> the granularity is 1, and a lone fractional gap (0.5) and a larger
        // gap (1.5) that do not recur are skipped.
        double[] integerish = { 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 5.5, 7.0, 8.0, 9.0 };
        assertEquals(1.0, Stats.quantizationStep(integerish, 0, integerish.length), 1e-12);

        // A clean step (one non-zero difference) is structure, not granularity: no floor.
        double[] step = { 5.0, 5.0, 5.0, 9.0, 9.0, 9.0, 9.0 };
        assertEquals("a lone step is not a quantization", 0.0, Stats.quantizationStep(step, 0, step.length), 1e-12);

        // Continuous data: the smallest difference does not recur within tolerance, so there is no floor.
        Random random = new Random(11);
        double[] continuous = new double[200];
        for (int i = 0; i < continuous.length; i++) {
            continuous[i] = random.nextGaussian();
        }
        assertEquals("continuous data has no quantization step", 0.0, Stats.quantizationStep(continuous, 0, continuous.length), 1e-12);
    }

    public void testKdeBandwidthFloorRescuesADegenerateBackgroundButIsInertOtherwise() {
        // A constant background has zero spread; the floor is returned so the gate can still call an outlier.
        double[] constant = { 3.0, 3.0, 3.0, 3.0 };
        assertEquals(0.05, Stats.kdeBandwidth(constant, 0.05), 1e-12);

        // On a well-spread background Silverman's bandwidth dominates and a tiny floor is inert.
        Random random = new Random(17);
        double[] spread = new double[200];
        for (int i = 0; i < spread.length; i++) {
            spread[i] = random.nextGaussian();
        }
        assertEquals(
            "a negligible floor leaves Silverman's bandwidth unchanged",
            Stats.kdeBandwidth(spread),
            Stats.kdeBandwidth(spread, 1e-9),
            1e-12
        );
    }

    public void testLogErfcMatchesLogOfErfcAndStaysFiniteInTheFarTail() {
        assertEquals("erfc(0) = 1", 0.0, Stats.logErfc(0.0), 1e-12);
        assertEquals("log(erfc(1))", -1.849, Stats.logErfc(1.0), 1e-2);
        assertEquals("log(erfc(2))", -5.365, Stats.logErfc(2.0), 1e-2);
        // Far in the tail erfc underflows to 0, but the asymptotic branch keeps the log finite and ~ -x^2.
        double far = Stats.logErfc(30.0);
        assertTrue("far-tail log-erfc is finite", Double.isFinite(far));
        assertTrue("and very negative, near -x^2", far < -800.0);
        assertTrue("and monotonically decreasing", Stats.logErfc(40.0) < far);
    }

    public void testLogTailProbabilityDoesNotUnderflowWhereTheLinearTailWould() {
        Random random = new Random(23);
        double[] background = new double[200];
        for (int i = 0; i < background.length; i++) {
            background[i] = random.nextGaussian();
        }
        double bandwidth = Stats.kdeBandwidth(background);

        // For a moderate value the log-tail is just the log of the linear tail.
        double moderate = 1.5;
        assertEquals(
            Math.log(Stats.kdeTailProbability(moderate, background, bandwidth, 1)),
            Stats.kdeLogTailProbability(moderate, background, bandwidth, 1),
            1e-9
        );

        // Far in the tail the linear probability underflows to exactly 0, but the log-tail stays finite and
        // very negative -- this is the whole point of carrying log probabilities through the pulse gate.
        double far = 20.0;
        assertEquals("the linear tail underflows", 0.0, Stats.kdeTailProbability(far, background, bandwidth, 1), 0.0);
        double logFar = Stats.kdeLogTailProbability(far, background, bandwidth, 1);
        assertTrue("the log tail is finite", Double.isFinite(logFar));
        assertTrue("and very negative", logFar < -100.0);
    }

    public void testAsinhStabilizationFamily() {
        assertEquals(0.0, Stats.asinh(0.0), 1e-12);
        assertEquals("asinh(1) = ln(1 + sqrt(2))", 0.881373587, Stats.asinh(1.0), 1e-9);
        assertEquals("asinh is odd", -Stats.asinh(1.0), Stats.asinh(-1.0), 1e-12);

        // asinhStabilize(x, s) = asinh(x / s), elementwise.
        double[] stabilized = Stats.asinhStabilize(new double[] { 3.0, -3.0 }, 3.0);
        assertEquals(Stats.asinh(1.0), stabilized[0], 1e-12);
        assertEquals(Stats.asinh(-1.0), stabilized[1], 1e-12);

        // asinhScale is a robust spread (IQR / 1.349). For 1..9 the IQR is 7 - 3 = 4.
        assertEquals(4.0 / 1.349, Stats.asinhScale(new double[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 }), 1e-9);
        // A constant series has no spread; the scale falls back to 1.0 rather than 0.
        assertEquals("degenerate spread falls back to 1", 1.0, Stats.asinhScale(new double[] { 5.0, 5.0, 5.0, 5.0 }), 1e-12);
    }
}
