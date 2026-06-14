/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.apache.commons.math3.special.Erf;

import java.util.Arrays;

public class Stats {

    public static double mean(double[] values) {
        double sum = 0.0;
        for (double v : values) {
            sum += v;
        }
        return sum / Math.max(values.length, 1);
    }

    public static double median(double[] values) {
        if (values.length == 0) {
            return 0.0;
        }
        int n = values.length;
        double[] tmp = Arrays.copyOf(values, n);
        Arrays.sort(tmp);
        return n % 2 == 0 ? (tmp[n / 2 - 1] + tmp[n / 2]) / 2.0 : tmp[n / 2];
    }

    /**
     * Linear-interpolation quantile of an already-sorted array.
     */
    private static double quantile(double[] sorted, double q) {
        int n = sorted.length;
        if (n == 1) {
            return sorted[0];
        }
        double pos = q * (n - 1);
        int lo = (int) Math.floor(pos);
        int hi = (int) Math.ceil(pos);
        return sorted[lo] + (pos - lo) * (sorted[hi] - sorted[lo]);
    }

    /**
     * Returns weighted mean over a range with non-negative weights.
     */
    public static double weightedMean(double[] values, double[] weights, int start, int end) {
        double weightedSum = 0.0;
        double weightTotal = 0.0;
        for (int i = start; i < end; i++) {
            double w = Math.max(weights[i], 0.0);
            weightedSum += w * values[i];
            weightTotal += w;
        }
        if (weightTotal <= 0.0) {
            return 0.0;
        }
        return weightedSum / weightTotal;
    }

    /**
     * Returns weighted RSS around a single weighted mean for a half-window.
     */
    public static double weightedRss(double[] values, double[] weights, int start, int end) {
        double mean = weightedMean(values, weights, start, end);
        double rss = 0.0;
        for (int i = start; i < end; i++) {
            double w = Math.max(weights[i], 0.0);
            double diff = values[i] - mean;
            rss += w * diff * diff;
        }
        return rss;
    }

    /**
     * Smoothly regularizes RSS for BIC, preventing tiny numerical gains from dominating.
     */
    public static double stabilizeRss(double rss, int n, double localNoiseVariance) {
        double variance = Math.max(localNoiseVariance, MIN_VARIANCE);
        return rss + (VARIANCE_FLOOR_SCALE * n * variance);
    }

    /**
     * Collapse-resistant robust scale of {@code residuals}: the larger of the MAD and the inter-decile (Q90-Q10)
     * scale, floored by numerical noise at the given magnitude.
     *
     * Two reasons for the composite rather than a plain MAD:
     *   - The MAD drops to zero once more than half the residuals are identical (common on sparse series with
     *     runs of zeros).
     *   - The inter-decile <em>inflates</em> once a sizable population (more than ~10%) of large residuals
     *     appear, so a frequent excursion population — e.g. a high-variance regime — raises the scale rather
     *     than each of its points reading as an outlier while still being resistant to heavy tails.
     *
     * The numerical floor keeps the scale strictly positive.
     */
    public static double compositeScale(double[] residuals, double magnitude) {
        return Math.max(Math.max(madScale(residuals), interdecileScale(residuals)), residualNumericalScaleFloor(magnitude));
    }

    /** Robust scale of residuals: 1.4826 * median(|r - median(r)|). */
    private static double madScale(double[] residuals) {
        int n = residuals.length;
        double[] tmp = Arrays.copyOf(residuals, n);
        Arrays.sort(tmp);
        double med = tmp[n / 2];
        for (int i = 0; i < n; i++) {
            tmp[i] = Math.abs(residuals[i] - med);
        }
        Arrays.sort(tmp);
        return 1.4826 * tmp[n / 2];
    }

    /** Inter-decile (Q90 - Q10) scale, converted to sigma units; holds up until more than ~90% are identical. */
    private static double interdecileScale(double[] residuals) {
        int n = residuals.length;
        if (n < 2) {
            return 0.0;
        }
        double[] tmp = Arrays.copyOf(residuals, n);
        Arrays.sort(tmp);
        double q10 = tmp[(int) Math.floor(0.10 * (n - 1))];
        double q90 = tmp[(int) Math.ceil(0.90 * (n - 1))];
        return Math.max(0.0, (q90 - q10) / 2.5631);
    }

    /**
     * Robust local scale of {@code residuals} over the window {@code [start, end)}: 1.4826 * MAD, floored by
     * numerical noise. Unlike {@link #compositeScale}, this uses the plain MAD (no inter-decile term): its 50%
     * breakdown ignores the minority of points from an adjacent regime when the window straddles a structural
     * change, so the estimate reflects the local regime's noise rather than being inflated by the other side.
     */
    public static double localRobustScale(double[] residuals, int start, int end, double magnitude) {
        int n = end - start;
        double[] tmp = new double[n];
        System.arraycopy(residuals, start, tmp, 0, n);
        Arrays.sort(tmp);
        double med = tmp[n / 2];
        for (int i = 0; i < n; i++) {
            tmp[i] = Math.abs(residuals[start + i] - med);
        }
        Arrays.sort(tmp);
        return Math.max(1.4826 * tmp[n / 2], residualNumericalScaleFloor(magnitude));
    }

    /**
     * Absolute noise floor for a std-scale estimate: a residual formed from values of this magnitude carries
     * rounding error ~ulp(magnitude), so any scale below this is numerical dust.
     */
    private static double residualNumericalScaleFloor(double magnitude) {
        return SCALE_PRECISION_ULP_FACTOR * Math.ulp(Math.max(Math.abs(magnitude), 1.0));
    }

    /**
     * Residuals of each value from the median of a centred window, used as a local-deviation signal.
     */
    public static double[] rollingMedianResiduals(double[] values, int halfWindow) {
        int n = values.length;
        double[] residuals = new double[n];
        double[] window = new double[2 * halfWindow + 1];
        for (int i = 0; i < n; i++) {
            // Shrink the window symmetrically near the ends. A one-sided (independently clamped) window biases the
            // median toward the interior on a trending series, so the first/last points read as large residuals and
            // are mis-flagged as a dip/spike. A symmetric window has zero trend bias: for a locally linear trend its
            // median is the centre value, so the boundary residual is ~0.
            int h = Math.min(halfWindow, Math.min(i, n - 1 - i));
            int len = 2 * h + 1;
            System.arraycopy(values, i - h, window, 0, len);
            Arrays.sort(window, 0, len);
            residuals[i] = values[i] - window[len / 2];
        }
        return residuals;
    }

    /**
     * The dispersion channel for {@code values}: one sample per non-overlapping window of {@code window} points,
     * each being {@code log1p} of a robust local noise scale, so a change in noise level shows up as a level
     * change on this series. Non-overlapping windows keep the samples roughly independent (overlap would
     * autocorrelate them and make the downstream segmenter over-detect). Returns {@code null} if the series is
     * too short to hold two {@code minSegmentLength} windows. Each sample {@code k} is centred on original index
     * {@code window/2 + window * k}.
     */
    static double[] windowedDispersion(double[] values, int minSegmentLength, int window) {
        int n = values.length;
        if (n < 2 * minSegmentLength * window) {
            return null;
        }

        double[] dispersion = new double[(n + window - 1) / window];
        int m = 0;
        for (int a = 0; a < n; a += window) {
            int b = Math.min(n, a + window);
            if (b - a < 2) {
                break; // Drop a length-1 trailing window (no meaningful spread).
            }
            dispersion[m] = Math.log1p(Math.sqrt(interquartileNoiseVariance(values, a, b)));
            m++;
        }
        if (m < 2 * minSegmentLength) {
            return null;
        }
        return Arrays.copyOf(dispersion, m);
    }

    /**
     * Local noise scale (returned as a variance) over {@code [start, end)} from the inter-quartile range of the
     * first differences: {@code sigma = (Q75 - Q25) / 1.349}, the IQR-to-sigma conversion for a normal. The
     * differences' spread is a constant factor larger than the per-sample noise, but that factor is constant
     * across the series so it cancels in the dispersion channel's level-change detection; what matters here is
     * robustness, not the absolute scale. The IQR's 25%-per-tail breakdown is the right middle ground for the
     * dispersion channel: the median of |differences| is too robust (a window that is, say, 40% excursions then
     * flat reads as zero, missing the variance change), while the raw standard deviation is not robust enough (a
     * single spike contributes one large positive and one large negative difference and inflates the window).
     */
    public static double interquartileNoiseVariance(double[] values, int start, int end) {
        int n = end - start;
        if (n < 3) {
            return MIN_VARIANCE;
        }
        int count = n - 1;
        double[] diffs = new double[count];
        for (int i = 0; i < count; i++) {
            diffs[i] = values[start + i + 1] - values[start + i];
        }
        Arrays.sort(diffs);
        double sigma = Math.max(0.0, (quantile(diffs, 0.75) - quantile(diffs, 0.25)) / 1.349);
        return Math.max(sigma * sigma, MIN_VARIANCE);
    }

    /**
     * Estimates baseline noise from the MAD of first differences to suppress step/trend effects.
     */
    public static double globalNoiseVariance(double[] values) {
        return localNoiseVariance(values, 0, values.length);
    }

    /**
     * Estimates local noise variance from the median of absolute first differences (a lag-1 difference estimator),
     * which is robust to a slowly-varying mean because differencing cancels it. For iid Gaussian noise the first
     * differences have variance 2*sigma^2, and a half-normal has median(|d|) = 0.6745 * sqrt(2) * sigma, so the
     * scale is recovered as sigma = median(|d|) * 1.4826 / sqrt(2). NB: this is the median of |d|, not the MAD of
     * the differences (which would subtract median(d) first); the two coincide under local stationarity, but on a
     * steady ramp this picks up the slope rather than returning ~0.
     */
    public static double localNoiseVariance(double[] values, int start, int end) {
        int n = end - start;
        if (n < 3) {
            return MIN_VARIANCE;
        }

        double[] absoluteDifferences = new double[n - 1];
        for (int i = 1; i < n; i++) {
            absoluteDifferences[i - 1] = Math.abs(values[start + i] - values[start + i - 1]);
        }
        Arrays.sort(absoluteDifferences);
        double medianAbsoluteDifference = absoluteDifferences[absoluteDifferences.length / 2];
        if (medianAbsoluteDifference <= 0.0) {
            return MIN_VARIANCE;
        }
        double sigma = (1.4826 * medianAbsoluteDifference) / Math.sqrt(2.0);
        return sigma * sigma;
    }

    /**
     * Silverman's rule-of-thumb bandwidth on the background, using the robust {@code min(std, IQR/1.349)}
     * spread so a residual heavy tail in the background cannot inflate it. Returns 0 for a degenerate
     * constant or too-small) background, which {@link #kdeTailProbability} handles as an empirical step.
     */
    public static double kdeBandwidth(double[] background) {
        int m = background.length;
        if (m < 2) {
            return 0.0;
        }
        double backgroundMean = mean(background);
        double variance = 0.0;
        for (double x : background) {
            double d = x - backgroundMean;
            variance += d * d;
        }
        variance /= (m - 1);
        double std = Math.sqrt(Math.max(variance, 0.0));
        double[] sorted = Arrays.copyOf(background, m);
        Arrays.sort(sorted);
        double iqr = quantile(sorted, 0.75) - quantile(sorted, 0.25);
        double spread = iqr > 0.0 ? Math.min(std, iqr / 1.349) : std;
        return SILVERMAN_FACTOR * spread * Math.pow(m, -0.2);
    }

    /**
     * Upper- (sign greater than 0) or lower-tail (sign less than 0) probability of {@code value} under
     * a Gaussian KDE fitted to {@code background}. With a degenerate bandwidth the KDE collapses to the
     * empirical distribution, so we return the empirical tail fraction.
     */
    public static double kdeTailProbability(double value, double[] background, double bandwidth, int sign) {
        int m = background.length;
        if (m == 0) {
            return 1.0;
        }
        if (bandwidth <= 0.0) {
            int beyond = 0;
            for (double x : background) {
                if (sign > 0 ? x >= value : x <= value) {
                    beyond++;
                }
            }
            return (double) beyond / m;
        }
        double scale = bandwidth * Math.sqrt(2.0);
        double sum = 0.0;
        for (double x : background) {
            double arg = sign > 0 ? (value - x) / scale : (x - value) / scale;
            sum += 0.5 * Erf.erfc(arg);
        }
        return sum / m;
    }

    /**
     * asinh(x) = sign(x) * log(|x| + sqrt(x^2 + 1)); Java has no {@code Math.asinh}.
     *
     * Odd and stable for x less than 0.
     */
    public static double asinh(double x) {
        double a = Math.abs(x);
        double r = Math.log(a + Math.sqrt(a * a + 1.0));
        return x < 0.0 ? -r : r;
    }

    /**
     * A robust spread of {@code values} used as the soft linear/log crossover scale for {@link #asinhStabilize}:
     * the inter-quartile range rescaled to a standard-deviation equivalent, falling back to the MAD and then to
     * 1.0, so the returned scale is always strictly positive.
     */
    public static double asinhScale(double[] values) {
        int m = values.length;
        if (m < 2) {
            return 1.0;
        }
        double[] sorted = Arrays.copyOf(values, m);
        Arrays.sort(sorted);
        double iqr = quantile(sorted, 0.75) - quantile(sorted, 0.25);
        if (iqr > 0.0) {
            return iqr / 1.349;
        }
        double median = quantile(sorted, 0.5);
        double[] deviations = new double[m];
        for (int i = 0; i < m; i++) {
            deviations[i] = Math.abs(values[i] - median);
        }
        Arrays.sort(deviations);
        double mad = quantile(deviations, 0.5);
        return mad > 0.0 ? mad / 0.6745 : 1.0;
    }

    /**
     * The variance-stabilising transform {@code asinh(x / scale)} applied elementwise. It is monotonic, so a
     * value-based tail gate run in this space still asks "is this magnitude one we see at other times"; it is
     * odd and finite at zero, so exact zeros and sign changes are handled; and it is linear for |x| &lt;&lt; scale and
     * logarithmic for |x| &gt;&gt; scale, which turns a multiplicative (magnitude-dependent) spread into a roughly
     * constant one so that a single KDE bandwidth is valid across orders of magnitude.
     */
    public static double[] asinhStabilize(double[] values, double scale) {
        double s = scale > 0.0 ? scale : 1.0;
        double[] out = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = asinh(values[i] / s);
        }
        return out;
    }

    // The minimum variance returned by the local noise estimator, to prevent numerical collapse on flat or
    // near-flat segments close to zero.
    private static final double MIN_VARIANCE = 1e-12;
    // Fraction of the local noise variance added back into a segment's RSS before taking its BIC (see
    // stabilizeRss): a small regularizer so a near-perfect fit cannot earn an unbounded log-likelihood and
    // dominate the model comparison on numerical noise.
    private static final double VARIANCE_FLOOR_SCALE = 0.01;
    // Silverman's rule-of-thumb bandwidth on the background, using the robust {@code min(std, IQR/1.349)}
    // spread so a residual heavy tail in the background cannot inflate it.
    private static final double SILVERMAN_FACTOR = 0.9;
    // The numerical floor used by the composite robust scale below.
    private static final double SCALE_PRECISION_ULP_FACTOR = 32.0;
}
