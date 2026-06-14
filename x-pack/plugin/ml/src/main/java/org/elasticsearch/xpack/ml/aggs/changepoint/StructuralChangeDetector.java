/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Detects structural (step/trend) changes: down-weight local excursions, find candidate boundaries
 * with PELT, then verify and label each with the BIC classifier.
 */
public class StructuralChangeDetector {

    private static final Logger logger = LogManager.getLogger(StructuralChangeDetector.class);

    public StructuralChangeDetector(int minSegmentLength, int classifierMaxDegree, double pValueThreshold) {
        this(minSegmentLength, BETA_MULTIPLIER, classifierMaxDegree, pValueThreshold);
    }

    public StructuralChangeDetector(int minSegmentLength, double betaMultiplier, int classifierMaxDegree, double pValueThreshold) {
        this.minSegmentLength = minSegmentLength;
        this.betaMultiplier = betaMultiplier;
        this.classifier = new StructuralChangeClassifier(minSegmentLength, classifierMaxDegree, pValueThreshold);
    }

    /**
     * Detect structural (step/trend) changes: down-weight local excursions, find candidate boundaries
     * with PELT, then verify and label each with the BIC classifier.
     */
    List<ChangeType> detect(double[] values) {
        if (values.length < 2 * minSegmentLength) {
            return Collections.emptyList();
        }

        // Work on values shifted by a constant offset. Polynomial-with-intercept RSS is invariant to a
        // constant shift in y, so this leaves every cost identical in exact arithmetic while collapsing
        // the working magnitudes from O(level^2) to O(spread^2). That reduces the catastrophic cancellation
        // in E[y^2] - E[y]^2 that can otherwise manufacture spurious change points on (near-)constant
        // high-magnitude series. The offset is purely numerical and never surfaces: events carry indices,
        // not values.
        double offset = Stats.mean(values);
        double[] shiftedValues = shift(values, -offset);
        double[] weights = localDeviationWeights(shiftedValues, minSegmentLength);

        double sigma2 = Stats.globalNoiseVariance(shiftedValues);
        int[] candidates = runPelt(shiftedValues, weights, sigma2, minSegmentLength);

        return classifier.selectAndClassify(shiftedValues, weights, candidates);
    }

    private static double[] shift(double[] values, double delta) {
        double[] shifted = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            shifted[i] = values[i] + delta;
        }
        return shifted;
    }

    /**
     * PELT with a profiled-variance Gaussian segment cost, which is scale-invariant and so does not over-
     * segment smooth signals. The cost is the BIC-like form n/2 * (log(RSS/n) + 1) plus a per-segment
     * penalty beta that grows with log(n) and the segment model complexity (degree + 1). The penalty is
     * scaled by a bias factor to suppress borderline splits of smooth oscillations while still detecting
     * genuine steps and trends, which carry much stronger evidence. The local-deviation weights are
     * applied to the segment fits but not the cost calculation, so a spike is not rewarded as a segment
     * but still suppresses nearby candidates.
     */
    private int[] runPelt(double[] values, double[] weights, double sigma2, int minSeg) {
        int n = values.length;
        double mean = 0.0;
        double minValue = Double.POSITIVE_INFINITY;
        double maxValue = Double.NEGATIVE_INFINITY;
        for (double v : values) {
            mean += v;
            minValue = Math.min(minValue, v);
            maxValue = Math.max(maxValue, v);
        }
        mean /= Math.max(n, 1);
        double range = Math.max(0.0, maxValue - minValue);
        double rangeFloorSigma2 = Math.pow(0.01 * range, 2);
        double precisionFloorSigma2 = LeastSquaresOnlineRegression.variancePrecisionFloor(mean);
        sigma2 = Math.max(sigma2, Math.max(Math.max(1e-6, rangeFloorSigma2), precisionFloorSigma2));

        double beta = betaMultiplier * (SEGMENT_DEGREE + 1) * Math.log(n);
        // Per-break penalty. The bias (>1) biases toward fewer segments, suppressing the borderline
        // splits that a smooth oscillation would otherwise accrue under the scale-invariant cost;
        // genuine steps/trends carry far more evidence and are unaffected.
        double breakPenalty = SEGMENT_PENALTY_BIAS * beta;

        // Prefix sums of the weighted moments. The profiled-variance cost of any candidate segment
        // [tau, t) is then an O(1) combination of prefix[t] - prefix[tau] (see segmentResidualVariance).
        double[] pW = new double[n + 1];
        double[] pX = new double[n + 1];
        double[] pXX = new double[n + 1];
        double[] pY = new double[n + 1];
        double[] pXY = new double[n + 1];
        double[] pYY = new double[n + 1];
        for (int i = 0; i < n; i++) {
            double w = weights[i];
            double x = i;
            double y = values[i];
            double wx = w * x;
            double wy = w * y;
            pW[i + 1] = pW[i] + w;
            pX[i + 1] = pX[i] + wx;
            pXX[i + 1] = pXX[i] + wx * x;
            pY[i + 1] = pY[i] + wy;
            pXY[i + 1] = pXY[i] + wx * y;
            pYY[i + 1] = pYY[i] + wy * y;
        }

        double[] opt = new double[n + 1];
        int[] bestPrev = new int[n + 1];
        opt[0] = -breakPenalty;

        List<Integer> R = new ArrayList<>();
        R.add(0);

        for (int t = 1; t <= n; t++) {
            double minCost = Double.MAX_VALUE;
            int optTau = 0;

            for (int i = 0; i < R.size(); i++) {
                int tau = R.get(i);

                int segmentLength = t - tau;
                if (segmentLength < minSeg) {
                    continue;
                }

                // Single scale-invariant (profiled-variance) Gaussian segment cost. We use the profiled
                // form rather than a fixed global variance because the global noise estimate is unreliable
                // on smooth signals (the MAD of first differences collapses on a slowly-varying series),
                // which would make a fixed-variance cost over-segment them; the profiled cost depends
                // only on the ratio rss/L and is immune to that. The rss floor keeps the log finite,
                // which is all that is needed to keep a pristine segment from being rewarded - no
                // separate short-segment penalty.
                double segmentCost = segmentCost(tau, t, segmentLength, sigma2, pW, pX, pXX, pY, pXY, pYY);
                double totalCost = opt[tau] + segmentCost + breakPenalty;

                if (totalCost < minCost) {
                    minCost = totalCost;
                    optTau = tau;
                    logger.trace("PELT new optimal split at [{}] with total cost [{}] and segment cost [{}]", tau, totalCost, segmentCost);
                }
            }

            opt[t] = minCost;
            bestPrev[t] = optTau;

            for (int i = R.size() - 1; i >= 0; i--) {
                int tau = R.get(i);
                int segmentLength = t - tau;
                if (segmentLength >= minSeg) {
                    double segmentCost = segmentCost(tau, t, segmentLength, sigma2, pW, pX, pXX, pY, pXY, pYY);
                    if (opt[tau] + segmentCost >= opt[t]) {
                        R.remove(i);
                        logger.trace(
                            "PELT pruning candidate with split at [{}] due to cost [{}] >= opt[{}] [{}]",
                            tau,
                            opt[tau] + segmentCost,
                            t,
                            opt[t]
                        );
                    }
                }
            }

            R.add(t);
        }

        List<Integer> changePoints = new ArrayList<>();
        int current = n;
        while (current > 0) {
            current = bestPrev[current];
            if (current > 0) {
                changePoints.add(current);
            }
        }
        Collections.reverse(changePoints);
        return changePoints.stream().mapToInt(i -> i).toArray();
    }

    /** Profiled-variance Gaussian segment cost for {@code [tau, t)}, from the prefix-summed weighted moments. */
    private static double segmentCost(
        int tau,
        int t,
        int segmentLength,
        double sigma2,
        double[] pW,
        double[] pX,
        double[] pXX,
        double[] pY,
        double[] pXY,
        double[] pYY
    ) {
        double residualVariance = segmentResidualVariance(tau, t, pW, pX, pXX, pY, pXY, pYY);
        double rss = Stats.stabilizeRss(residualVariance * segmentLength, segmentLength, sigma2);
        return (segmentLength / 2.0) * (Math.log(rss / segmentLength) + 1.0);
    }

    /**
     * Residual variance of the weighted degree-{@link #SEGMENT_DEGREE} (linear) fit over {@code [tau, t)},
     * computed in O(1) from the prefix-summed weighted moments.
     *
     * This reproduces {@link LeastSquaresOnlineRegression#residualVariance()} for a degree-1 model: it forms
     * the same normalised moment system, falls back to the mean-only total variance when the 2x2 system is
     * ill-conditioned (the analytic analogue of that method's SVD condition guard, using the eigenvalues of
     * the symmetric PSD moment matrix as its singular values), and clamps to {@code [0, total variance]}.
     * Specialised to degree 1 because PELT only proposes constant/linear segments ({@code SEGMENT_DEGREE}).
     */
    private static double segmentResidualVariance(
        int tau,
        int t,
        double[] pW,
        double[] pX,
        double[] pXX,
        double[] pY,
        double[] pXY,
        double[] pYY
    ) {
        double w = pW[t] - pW[tau];
        if (w <= 0.0) {
            return 0.0;
        }
        double ex = (pX[t] - pX[tau]) / w;
        double exx = (pXX[t] - pXX[tau]) / w;
        double ey = (pY[t] - pY[tau]) / w;
        double exy = (pXY[t] - pXY[tau]) / w;
        double eyy = (pYY[t] - pYY[tau]) / w;

        double varRaw = eyy - ey * ey;
        double var = Math.max(varRaw, LeastSquaresOnlineRegression.variancePrecisionFloor(ey));

        // Normal-equation matrix [[1, ex], [ex, exx]] (symmetric PSD), so its singular values are
        // its eigenvalues.
        double trace = 1.0 + exx;
        double diff = 1.0 - exx;
        double disc = Math.sqrt(Math.max(0.0, diff * diff + 4.0 * ex * ex));
        double lambdaMax = 0.5 * (trace + disc);
        double lambdaMin = 0.5 * (trace - disc);
        double det = exx - ex * ex; // Var(x); equals the product of the eigenvalues
        if (det <= 0.0 || lambdaMin <= 0.0 || lambdaMax > SVD_MAX_COND * lambdaMin) {
            return var; // ill-conditioned: fall back to the mean-only fit, as residualVariance() does.
        }

        double b = (exy - ex * ey) / det; // slope
        double a = ey - ex * b;           // intercept
        double tMean = ey - (a + b * ex);
        double residual = (eyy - (a * ey + b * exy)) - tMean * tMean;
        if (Double.isFinite(residual) == false) {
            return var;
        }
        return Math.max(0.0, Math.min(residual, var));
    }

    /**
     * Down-weights local excursions for the structural fit using a redescending Tukey biweight on residuals
     * from a rolling-median baseline. Each residual is judged against a <em>local</em> robust scale - the
     * MAD of the residuals in a window of about {@code minSegmentLength} around it, capped by the global
     * composite scale - so the scale adapts to the noise level of the surrounding regime. This is essential
     * for heteroscedastic series: a spike sitting on a low-variance regime is many local sigma and is
     * suppressed, even though it would be only a fraction of a sigma against the global scale (which is
     * dominated by any high-variance regime elsewhere). Leaving such a spike at full weight pulls a nearby
     * PELT boundary off the true change. The global cap keeps a short or noisy window from over-estimating
     * the local scale and so under-suppressing. A tiny weight floor avoids degenerate all-zero-weight
     * segments.
     */
    public static double[] localDeviationWeights(double[] values, int minSegmentLength) {
        int n = values.length;
        double[] weights = new double[n];
        if (n < 3) {
            Arrays.fill(weights, 1.0);
            return weights;
        }
        double[] residuals = Stats.rollingMedianResiduals(values, WEIGHT_HALF_WINDOW);
        // The symmetric rolling-median residual collapses to ~0 at the very ends (a point is its own median
        // when the window shrinks to it), so an endpoint spike would keep full weight and create a spurious
        // trends. Re-residual the boundary points against a robust (Theil-Sen) line over a local boundary
        // window: on a trend the endpoint lies on the line (residual ~0, full weight, trends preserved),
        // while a spike is an outlier the robust line ignores (large residual, down-weighted).
        applyBoundaryLineResiduals(values, residuals);
        double maxAbs = 0.0;
        for (double v : values) {
            maxAbs = Math.max(maxAbs, Math.abs(v));
        }
        double globalScale = Stats.compositeScale(residuals, maxAbs);
        if (globalScale <= 0.0) {
            Arrays.fill(weights, 1.0);
            return weights;
        }
        // Window for the local scale: about half a minimum segment each side, so the MAD has enough points
        // to be stable while still adapting between regimes (and a regime of the minimum length is not
        // dominated by its neighbours). Floored at the rolling-median half-window so it is never degenerately
        // small.
        int scaleHalfWindow = Math.max(WEIGHT_HALF_WINDOW, minSegmentLength / 2);
        for (int i = 0; i < n; i++) {
            int lo = Math.max(0, i - scaleHalfWindow);
            int hi = Math.min(n, i + scaleHalfWindow + 1);
            double scale = Math.min(globalScale, Stats.localRobustScale(residuals, lo, hi, maxAbs));
            double u = Math.abs(residuals[i]) / scale;
            if (u >= TUKEY_C) {
                weights[i] = MIN_WEIGHT;
            } else {
                double t = 1.0 - (u / TUKEY_C) * (u / TUKEY_C);
                weights[i] = Math.max(MIN_WEIGHT, t * t);
            }
        }
        return weights;
    }

    /**
     * Replaces the residuals of the first and last {@link #WEIGHT_HALF_WINDOW} points (where the symmetric
     * rolling-median window has collapsed and the residual is ~0) with the residual from a robust Theil-Sen
     * line fitted over a local boundary window. This keeps trend endpoints at full weight while exposing
     * endpoint spikes.
     */
    private static void applyBoundaryLineResiduals(double[] values, double[] residuals) {
        int n = values.length;
        int window = Math.min(n, BOUNDARY_LINE_WINDOW);
        if (window < 3) {
            return;
        }
        double[] head = theilSenLine(values, 0, window);
        for (int i = 0; i < WEIGHT_HALF_WINDOW && i < n; i++) {
            residuals[i] = values[i] - (head[0] + head[1] * i);
        }
        int tailStart = n - window;
        double[] tail = theilSenLine(values, tailStart, window);
        for (int i = Math.max(0, n - WEIGHT_HALF_WINDOW); i < n; i++) {
            residuals[i] = values[i] - (tail[0] + tail[1] * (i - tailStart));
        }
    }

    /**
     * Robust line over {@code values[start, start+length)} as {@code {intercept_at_start, slope}},
     * via Theil-Sen (median pairwise slope, then median intercept) so a boundary spike does not pull
     * the fit.
     */
    private static double[] theilSenLine(double[] values, int start, int length) {
        double[] slopes = new double[length * (length - 1) / 2];
        int s = 0;
        for (int j = 0; j < length; j++) {
            for (int k = j + 1; k < length; k++) {
                slopes[s++] = (values[start + k] - values[start + j]) / (k - j);
            }
        }
        double slope = Stats.median(slopes);
        double[] intercepts = new double[length];
        for (int j = 0; j < length; j++) {
            intercepts[j] = values[start + j] - slope * j;
        }
        return new double[] { Stats.median(intercepts), slope };
    }

    // PELT generates candidates with constant/linear segments only: higher-order segments are unstable
    // on the short windows PELT explores and largely degenerate with extra linear pieces. The verifier
    // then re-tests each candidate with its own (independent) alternative-model degree. This can't be
    // changed without changing {@code segmentResidualVariance}.
    private static final int SEGMENT_DEGREE = 1;
    // Maximum singular-value condition number for the linear segment fit before we fall back to the
    // mean-only fit, matching LeastSquaresOnlineRegression's SVD guard so segmentResidualVariance
    // reproduces residualVariance().
    private static final double SVD_MAX_COND = 1e12;
    // Per-break penalty bias (multiple of the BIC penalty beta). >1 biases the scale-invariant cost
    // toward fewer segments, so a smooth oscillation does not accrue borderline piecewise-linear splits
    // while genuine steps/trends (far stronger evidence) are unaffected.
    private static final double SEGMENT_PENALTY_BIAS = 1.5;
    // Default scaling of the BIC complexity penalty beta used by PELT. >1 makes candidate generation
    // more conservative (fewer proposed boundaries).
    private static final double BETA_MULTIPLIER = 1.0;
    // Local-deviation weighting: down-weight short excursions relative to a rolling-median baseline
    // with a redescending Tukey biweight, so genuine regimes (small local residuals) keep full weight
    // while spikes and dips lose influence on the structural fit. Robustness is local, not relative
    // to a global median.
    private static final double TUKEY_C = 4.685;
    private static final int WEIGHT_HALF_WINDOW = 4;
    private static final double MIN_WEIGHT = 1e-3;
    // Boundary window for the robust line used to residual the first/last WEIGHT_HALF_WINDOW points
    // (see applyBoundaryLineResiduals): wide enough for a stable Theil-Sen slope, local enough to
    // track the boundary.
    private static final int BOUNDARY_LINE_WINDOW = 2 * WEIGHT_HALF_WINDOW + 1;

    private final int minSegmentLength;
    private final double betaMultiplier;
    private final StructuralChangeClassifier classifier;
}
