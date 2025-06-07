/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.apache.commons.math3.special.Erf;
import org.apache.commons.math3.util.FastMath;

import java.util.Arrays;

import static org.apache.commons.math3.stat.StatUtils.variance;

/**
 * Kernel Density Estimator
 */
final class KDE {
    private static final double SQRT2 = FastMath.sqrt(2.0);
    private static final double ESTIMATOR_EPS = 1e-10;

    private static int lowerBound(double[] xs, double x) {
        int retVal = Arrays.binarySearch(xs, x);
        if (retVal < 0) {
            retVal = -1 - retVal;
        }
        return retVal;
    }

    private interface IntervalComplementFunction {
        double value(int a, int b);
    }

    private interface KernelFunction {
        double value(double centre, double x);
    }

    private ValueAndMagnitude evaluate(IntervalComplementFunction complement, KernelFunction kernel, double x) {
        if (orderedValues.length == 0) {
            return new ValueAndMagnitude(1.0, 0.0);
        }
        int a = Math.min(lowerBound(orderedValues, x - 3.0 * bandwidth), orderedValues.length - 1);
        int b = Math.max(lowerBound(orderedValues, x + 3.0 * bandwidth), a + 1);
        // Account for all the values outside the interval [a, b) using the kernel complement.
        double kdfx = complement.value(a, b);
        double diff = Double.MAX_VALUE;
        for (int i = a; i < b; i++) {
            double centre = orderedValues[i];
            kdfx += kernel.value(centre, x);
            diff = Math.min(Math.abs(centre - x), diff);
        }
        return new ValueAndMagnitude(kdfx / orderedValues.length, diff);
    }

    private final double[] orderedValues;
    private final double bandwidth;

    // This uses Silverman's rule of thumb for the bandwidth and chooses it to be proportional
    // to the standard deviation divided by the 5th root of the sample count. The constant of
    // proportionality is supplied as the smoothing parameter.
    //
    // A value of 1.06 is recommended by Silverman, which is optimal for Gaussian data with
    // Gaussian Kernel. This tends to oversmooth from an minimum (M)ISE perspective on many
    // distributions. However, we actually prefer oversmoothing for our use case.
    //
    // Note that orderedValues must be ordered ascending and are shallow copied.
    KDE(double[] orderedValues, double smoothing) {

        for (int i = 1; i < orderedValues.length; i++) {
            if (orderedValues[i - 1] > orderedValues[i]) {
                throw new IllegalArgumentException("Values must be ordered ascending, got [" + Arrays.toString(orderedValues) + "].");
            }
        }

        this.orderedValues = orderedValues;
        bandwidth = smoothing * Math.pow(orderedValues.length, -0.2) * Math.sqrt(variance(orderedValues));
    }

    ValueAndMagnitude cdf(double x) {
        return evaluate((a, b) -> a, (centre, x_) -> normCdf(centre, x_), x);
    }

    ValueAndMagnitude sf(double x) {
        return evaluate((a, b) -> orderedValues.length - b, (centre, x_) -> normSf(centre, x_), x);
    }

    double normCdf(double mean, double x) {
        final double dev = x - mean;
        if (Math.abs(dev) > 40.0 * bandwidth) {
            return dev > 0 ? 1.0d : 0.0d;
        }
        // We use the fact that erf(-x) = -erf(x) and substitute for erfc(x) = 1 - erf(x)
        return 0.5 * Erf.erfc(-dev / (bandwidth * SQRT2));
    }

    double normSf(double mean, double x) {
        final double dev = x - mean;
        if (Math.abs(dev) > 40.0 * bandwidth) {
            return dev > 0 ? 0.0d : 1.0d;
        }
        return 0.5 * Erf.erfc(dev / (bandwidth * SQRT2));
    }

    int size() {
        return orderedValues.length;
    }

    double[] data() {
        return orderedValues;
    }

    record ValueAndMagnitude(double value, double magnitude) {
        boolean isMoreSignificant(ValueAndMagnitude o) {
            int c = Double.compare(value, o.value());
            return c != 0 ? (c < 0) : (magnitude > o.magnitude);
        }

        double pValue(int numberOfTestedValues) {
            return value > ESTIMATOR_EPS ? 1 - Math.pow(1 - value, numberOfTestedValues) : numberOfTestedValues * value;
        }
    }
}
