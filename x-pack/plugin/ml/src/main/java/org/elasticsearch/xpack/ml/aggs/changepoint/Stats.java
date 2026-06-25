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

    /** Mean of {@code a} over {@code [start, end)}, clamped to the array bounds; 0 if the clamped range is empty. */
    public static double meanRange(double[] a, int start, int end) {
        start = Math.max(0, start);
        end = Math.min(a.length, end);
        if (end <= start) {
            return 0.0;
        }
        double sum = 0.0;
        for (int i = start; i < end; i++) {
            sum += a[i];
        }
        return sum / (end - start);
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

    /** Linear-interpolation quantile of an already-sorted array. */
    public static double quantile(double[] sorted, double q) {
        int n = sorted.length;
        if (n == 1) {
            return sorted[0];
        }
        double pos = q * (n - 1);
        int lo = (int) Math.floor(pos);
        int hi = (int) Math.ceil(pos);
        return sorted[lo] + (pos - lo) * (sorted[hi] - sorted[lo]);
    }

    /** Range (max - min) of {@code values}; 0 for an empty or constant array. */
    public static double range(double[] values) {
        if (values.length == 0) {
            return 0.0;
        }
        double min = values[0];
        double max = values[0];
        for (double v : values) {
            min = Math.min(min, v);
            max = Math.max(max, v);
        }
        return max - min;
    }

    /**
     * Robust line over {@code values[start, start+length)} as {@code {intercept_at_start, slope}},
     * via Theil-Sen (median pairwise slope, then median intercept) so a boundary spike does not pull
     * the fit.
     */
    public static double[] theilSenLine(double[] values, int start, int length) {
        double[] slopes = new double[length * (length - 1) / 2];
        int s = 0;
        for (int j = 0; j < length; j++) {
            for (int k = j + 1; k < length; k++) {
                slopes[s++] = (values[start + k] - values[start + j]) / (k - j);
            }
        }
        double slope = median(slopes);
        double[] intercepts = new double[length];
        for (int j = 0; j < length; j++) {
            intercepts[j] = values[start + j] - slope * j;
        }
        return new double[] { median(intercepts), slope };
    }

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
     * Weighted RSS around a single weighted mean for a window.
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
        double mad = madScale(residuals, 0, residuals.length);
        double idr = interdecileScale(residuals, 0, residuals.length);
        return Math.max(Math.max(mad, idr), residualNumericalScaleFloor(magnitude));
    }

    /**
     * Robust local scale of {@code residuals} over the window {@code [start, end)}: 1.4826 * MAD, floored by
     * numerical noise. Unlike {@link #compositeScale}, this uses the plain MAD (no inter-decile term): its 50%
     * breakdown ignores the minority of points from an adjacent regime when the window straddles a structural
     * change, so the estimate reflects the local regime's noise rather than being inflated by the other side.
     */
    public static double localRobustScale(double[] residuals, int start, int end, double magnitude) {
        double mad = madScale(residuals, start, end);
        return Math.max(mad, residualNumericalScaleFloor(magnitude));
    }

    /** Robust scale of residuals: 1.4826 * median(|r - median(r)|). */
    private static double madScale(double[] residuals, int start, int end) {
        int n = end - start;
        double[] tmp = new double[n];
        System.arraycopy(residuals, start, tmp, 0, n);
        Arrays.sort(tmp);
        double med = tmp[n / 2];
        for (int i = 0; i < n; i++) {
            tmp[i] = Math.abs(residuals[start + i] - med);
        }
        Arrays.sort(tmp);
        return 1.4826 * tmp[n / 2];
    }

    /** Inter-decile scale: (Q90 - Q10) / 2.5631. */
    private static double interdecileScale(double[] residuals, int start, int end) {
        int n = end - start;
        if (n < 2) {
            return 0.0;
        }
        double[] tmp = new double[n];
        System.arraycopy(residuals, start, tmp, 0, n);
        Arrays.sort(tmp);
        double q10 = tmp[(int) Math.floor(0.10 * (n - 1))];
        double q90 = tmp[(int) Math.ceil(0.90 * (n - 1))];
        return Math.max(0.0, (q90 - q10) / 2.5631);
    }

    /**
     * Absolute noise floor for a std-scale estimate: a residual formed from values of this magnitude carries
     * rounding error ~ulp(magnitude), so any scale below this is numerical dust.
     */
    private static double residualNumericalScaleFloor(double magnitude) {
        return SCALE_PRECISION_ULP_FACTOR * Math.ulp(Math.max(Math.abs(magnitude), 1.0));
    }

    /**
     * The data's quantization step over {@code [start, end)}: the smallest first-difference magnitude that recurs,
     * or 0 if no magnitude recurs. This is the finest level difference the series resolves — 1 for integer counts,
     * the tick size for a rounded metric, ~0 for genuinely continuous data. It lets a noise-scale test refuse to
     * claim a variance change below the measurement granularity, where the IQR of a discrete first-difference
     * distribution is unstable (a 4/5 integer oscillation can read as a spurious variance step purely from how its
     * {0, +/-1} differences fall across the quartiles).
     *
     * <p>Recurrence is the key. Genuine quantization produces many differences of size ~q; an isolated step jump
     * on an otherwise-clean series shows up as a handful of large, distinct differences whose smallest is a step,
     * not a granularity. Returning that step would floor away the very signal we want to detect. So we require the
     * smallest magnitude to appear at least {@link #QUANTIZATION_MIN_RECURRENCE} times (within a small relative
     * tolerance) before treating it as the quantization step; otherwise the nonzero differences are structure,
     * not noise, and we return 0 (no floor). This also makes the estimate robust to the odd lone small difference
     * (a rare fractional value among integers), which does not recur and is skipped.
     */
    public static double quantizationStep(double[] values, int start, int end) {
        int count = 0;
        for (int i = start + 1; i < end; i++) {
            if (values[i] != values[i - 1]) {
                count++;
            }
        }
        if (count < QUANTIZATION_MIN_RECURRENCE) {
            return 0.0;
        }
        double[] positiveDiffs = new double[count];
        int k = 0;
        for (int i = start + 1; i < end; i++) {
            double d = Math.abs(values[i] - values[i - 1]);
            if (d > 0.0) {
                positiveDiffs[k++] = d;
            }
        }
        Arrays.sort(positiveDiffs);
        for (int i = 0; i + QUANTIZATION_MIN_RECURRENCE - 1 < positiveDiffs.length; i++) {
            if (positiveDiffs[i + QUANTIZATION_MIN_RECURRENCE - 1] <= positiveDiffs[i] * (1.0 + QUANTIZATION_TOLERANCE)) {
                return positiveDiffs[i];
            }
        }
        return 0.0;
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
        return windowedDispersion(values, minSegmentLength, window, window);
    }

    /**
     * The dispersion channel with a sliding window of {@code window} points stepped by {@code stride}. With
     * {@code stride == window} this is the non-overlapping channel above; with {@code stride < window} it keeps
     * the robust per-window IQR estimate but produces ~{@code window/stride}x more samples, restoring resolution.
     * The price is autocorrelation between overlapping samples, which the caller must offset with a larger channel
     * {@code minSegmentLength} so the segmenter does not over-detect. Sample {@code k} covers original indices
     * {@code [k*stride, k*stride + window)}; a level change at sample {@code k} corresponds to roughly original
     * index {@code k*stride + window/2}.
     */
    static double[] windowedDispersion(double[] values, int minSegmentLength, int window, int stride) {
        int n = values.length;
        if (window < 2 || stride < 1 || n < window) {
            return null;
        }
        int maxSamples = (n - window) / stride + 1;
        if (maxSamples < 2 * minSegmentLength) {
            return null;
        }
        double[] dispersion = new double[maxSamples];
        int m = 0;
        for (int a = 0; a + window <= n; a += stride) {
            dispersion[m++] = Math.log1p(Math.sqrt(interquartileNoiseVariance(values, a, a + window)));
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
     * Silverman's rule-of-thumb bandwidth on the background, using a Winsorized standard deviation as the spread
     * so a residual heavy tail cannot inflate it. Returns 0 for a degenerate (constant or too-small) background,
     * which {@link #kdeTailProbability} handles as an empirical step.
     */
    public static double kdeBandwidth(double[] background) {
        return kdeBandwidth(background, 0.0);
    }

    /**
     * As {@link #kdeBandwidth(double[])}, but {@code minBandwidth} rescues a <em>degenerate</em> background. The
     * floor exists solely to avoid a zero bandwidth: a constant background collapses the spread to 0, the KDE tail
     * falls back to an empirical step that, Bonferroni-corrected over the series, cannot call any point significant,
     * and a clear outlier is missed. So the floor is applied <em>only</em> when the bandwidth is otherwise zero.
     *
     * <p>The spread is a <em>Winsorized</em> standard deviation: the residuals are clipped to the
     * [{@link #WINSORIZE_ALPHA}, 1-{@link #WINSORIZE_ALPHA}] percentiles and the std of the clipped sample is taken.
     * This is the robust scale that fits this job. Unlike a plain std it is not inflated by a heavy tail or by the
     * sharp transition residuals of a structured regime (those are clipped).
     */
    public static double kdeBandwidth(double[] background, double minBandwidth) {
        int m = background.length;
        if (m < 2) {
            return Math.max(minBandwidth, 0.0);
        }
        double[] sorted = Arrays.copyOf(background, m);
        Arrays.sort(sorted);
        double lo = quantile(sorted, WINSORIZE_ALPHA);
        double hi = quantile(sorted, 1.0 - WINSORIZE_ALPHA);
        double clippedSum = 0.0;
        for (double x : background) {
            clippedSum += Math.min(Math.max(x, lo), hi);
        }
        double clippedMean = clippedSum / m;
        double clippedVariance = 0.0;
        for (double x : background) {
            double d = Math.min(Math.max(x, lo), hi) - clippedMean;
            clippedVariance += d * d;
        }
        clippedVariance /= (m - 1);
        double spread = Math.sqrt(Math.max(clippedVariance, 0.0));
        double bandwidth = SILVERMAN_FACTOR * spread * Math.pow(m, -0.2);
        return bandwidth > 0.0 ? bandwidth : Math.max(minBandwidth, 0.0);
    }

    /**
     * Upper- (sign greater than 0) or lower-tail (sign less than 0) probability of {@code value} under
     * a Gaussian KDE fitted to {@code background}. With a degenerate bandwidth the KDE collapses to the
     * empirical distribution, so we return the empirical tail fraction.
     */
    public static double kdeTailProbability(double value, double[] background, double bandwidth, int sign) {
        return Math.exp(kdeLogTailProbability(value, background, bandwidth, sign));
    }

    /**
     * Log of {@link #kdeTailProbability}: the same Gaussian-KDE tail, but computed by log-sum-exp over
     * the kernels so it does not underflow to {@code log(0) = -inf} for a value far in the tail, where
     * every {@code erfc} term is sub-normal. This is the spike/dip analogue of carrying a structural
     * change's log p-value.
     */
    public static double kdeLogTailProbability(double value, double[] background, double bandwidth, int sign) {
        int m = background.length;
        if (m == 0) {
            return 0.0; // tail probability 1
        }
        if (bandwidth <= 0.0) {
            int beyond = 0;
            for (double x : background) {
                if (sign > 0 ? x >= value : x <= value) {
                    beyond++;
                }
            }
            // Floor at half a count so a value beyond all of the background still has a finite log-tail.
            return Math.log(Math.max(beyond, 0.5) / m);
        }
        double scale = bandwidth * Math.sqrt(2.0);
        double logHalf = Math.log(0.5);
        double[] terms = new double[m];
        double maxTerm = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < m; i++) {
            double arg = sign > 0 ? (value - background[i]) / scale : (background[i] - value) / scale;
            terms[i] = logHalf + logErfc(arg);
            if (terms[i] > maxTerm) {
                maxTerm = terms[i];
            }
        }
        double sum = 0.0;
        for (double term : terms) {
            sum += Math.exp(term - maxTerm);
        }
        return maxTerm + Math.log(sum) - Math.log(m);
    }

    /**
     * Numerically stable natural log of {@code erfc(x)}.
     *
     * For moderate {@code x} this is {@code log(erfc(x))}.
     *
     * For large {@code x}, where {@code erfc} underflows to zero, it uses the asymptotic expansion
     * {@code erfc(x) ~ exp(-x^2)/(x*sqrt(pi)) * (1 - 1/(2x^2) + 3/(4x^4))}.
     */
    public static double logErfc(double x) {
        if (x <= 0.0) {
            return Math.log(Erf.erfc(x)); // erfc(x) in [1, 2] here, so the direct log is safe
        }
        double erfc = Erf.erfc(x);
        if (erfc > 1e-300) {
            return Math.log(erfc);
        }
        double x2 = x * x;
        double series = -1.0 / (2.0 * x2) + 3.0 / (4.0 * x2 * x2);
        return -x2 - Math.log(x) - 0.5 * Math.log(Math.PI) + Math.log1p(series);
    }

    /**
     * The variance-stabilising transform {@code asinh(x / scale)} applied elementwise. It is monotonic, so a
     * value-based tail gate run in this space still asks "is this magnitude one we see at other times"; it is
     * odd and finite at zero, so exact zeros and sign changes are handled; and it is linear for |x| much less
     * than scale and logarithmic for |x| much greater than scale, which turns a multiplicative (magnitude-
     * dependent) spread into a roughly constant one so that a single KDE bandwidth is valid across orders of
     * magnitude.
     */
    public static double[] asinhStabilize(double[] values, double scale) {
        double s = scale > 0.0 ? scale : 1.0;
        double[] out = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = asinh(values[i] / s);
        }
        return out;
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

    // The minimum variance returned by the local noise estimator, to prevent numerical collapse on flat
    // or near-flat segments close to zero.
    private static final double MIN_VARIANCE = 1e-12;
    // Fraction of the local noise variance added back into a segment's RSS before taking its BIC (see
    // {@code stabilizeRss}): a small regularizer so a near-perfect fit cannot earn an unbounded log-
    // likelihood and dominate the model comparison on numerical noise.
    private static final double VARIANCE_FLOOR_SCALE = 0.01;
    // The quantization step is the smallest first-difference magnitude that recurs at least this many times
    // (within QUANTIZATION_TOLERANCE, relative). Requiring recurrence stops a clean step series being read
    // as quantized at the step size (which would floor away the step).
    private static final int QUANTIZATION_MIN_RECURRENCE = 3;
    // Near-exact: catches float-rounding on genuinely equal quantized differences (relative error ~1e-15) while
    // rejecting continuous data, whose smallest differences cluster but are never this close in relative terms
    // (a looser tolerance would mistake that clustering near the noise mode for quantization and floor at the
    // noise).
    private static final double QUANTIZATION_TOLERANCE = 1e-9;
    // Per-tail fraction clipped when forming the Winsorized standard deviation for the KDE bandwidth: enough
    // to clip a heavy tail or the sharp transition residuals of a structured regime, small enough to leave
    // the bulk (and hence the within-regime noise) intact. Tuned against the synthetic benchmark.
    private static final double WINSORIZE_ALPHA = 0.025;
    // Silverman's rule-of-thumb bandwidth on the background, using the robust {@code min(std, IQR/1.349)}
    // spread so a residual heavy tail in the background cannot inflate it.
    private static final double SILVERMAN_FACTOR = 0.9;
    // The numerical floor used by the composite robust scale below.
    private static final double SCALE_PRECISION_ULP_FACTOR = 32.0;
}
