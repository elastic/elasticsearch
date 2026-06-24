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
import java.util.List;
import java.util.TreeSet;

/**
 * Verifies and labels structural (step/trend) changes using a BIC classifier.
 */
public class StructuralChangeClassifier {

    private static final Logger logger = LogManager.getLogger(StructuralChangeClassifier.class);

    /**
     * @param effectiveSampleFactor the fraction of the samples that are statistically independent (1.0 for a
     * raw value channel; {@code stride/window} for an overlapping dispersion channel, whose adjacent samples
     * are correlated). It scales the BIC's data term and effective {@code n}, so the verifier does not over-
     * count the evidence from correlated samples and emit spurious changes.
     * @param detectVarianceShifts when true, a boundary whose mean (pooled) split is not significant but whose
     * per-segment (profiled) split is — i.e. a change driven by a variance shift rather than a mean shift — is
     * reported as a {@link ChangeType.DistributionChange}. Used on the value channel to catch a short, strong
     * variance change the (coarser, conservatively-discounted) dispersion channel misses. Off for the dispersion
     * channel itself, where a "variance of the variance" change is meaningless.
     */
    public StructuralChangeClassifier(
        int minSegmentLength,
        int maxDegree,
        double pValueThreshold,
        double effectiveSampleFactor,
        boolean detectVarianceShifts
    ) {
        this.minSegmentLength = minSegmentLength;
        this.maxDegree = maxDegree;
        this.pValueThreshold = pValueThreshold;
        this.deltaBicThreshold = toBic(pValueThreshold);
        this.effectiveSampleFactor = effectiveSampleFactor;
        this.detectVarianceShifts = detectVarianceShifts;
    }

    /**
     * Selects which candidates to keep with a dynamic program over the candidate grid, rather than testing
     * each candidate against a window bounded by its (provisional) neighbours. The pipeline is:
     *
     * <ol>
     *   <li>Build the boundary grid {@code [0, candidates..., n]} and cache, for every candidate-bounded
     *       interval and every degree, its weighted polynomial RSS (full weights). This is the only state
     *       computed up front and it is shared by every step below.</li>
     *   <li>Persistence pre-filter: drop spike-driven candidates (a proposer artefact — PELT boundaries
     *       next to extreme points) by testing each on its own neighbour window. This is candidate cleaning,
     *       separate from selection, so the DP then runs on a strictly cleaner set.</li>
     *   <li>Run an optimal-partition DP over the surviving candidates: the kept set minimises the summed
     *       best-degree segment BIC plus a per-break penalty (the BIC equivalent of the p-value threshold).
     *       This is the same recurrence as PELT but on the reduced candidate grid; the penalty is the
     *       significance authority.</li>
     *   <li>In a single pass, label each selected boundary as a step or trend (on the segment bounded by
     *       its neighbours in the partition) and drop any that does not clear the significance threshold.
     *       No persistence here (done up front) and no re-running of the DP, so a dropped boundary never
     *       widens a neighbour's window and a genuine localized change cannot be lost.</li>
     * </ol>
     */
    List<ChangeType> selectAndClassify(double[] values, double[] weights, int[] structuralCandidates, double offset) {
        int n = values.length;
        // Quantization noise floor for every BIC on this series: the variance of rounding to the data's
        // granularity, q^2/12. Caps log(RSS/n) from below so a quantized/near-constant series cannot
        // manufacture significant steps or trends from sub-quantum (numerically near-zero) residual
        // variance. Negligible for continuous data.
        double quantum = Stats.quantizationStep(values, 0, n);
        this.quantizationVariance = quantum * quantum / 12.0;
        int[] interior = interiorCandidates(structuralCandidates, n);
        int m = interior.length + 2;
        int[] grid = new int[m];
        grid[0] = 0;
        for (int i = 0; i < interior.length; i++) {
            grid[i + 1] = interior[i];
        }
        grid[m - 1] = n;

        // Per-interval, per-degree weighted RSS under full weights, plus the interval's local noise variance.
        // Only intervals at least minSegmentLength long are ever used by the DP or the labelling, so shorter
        // ones are left null. (A prefix-moment formulation would make each entry O(1); kept as direct fits
        // for clarity, which is adequate while the candidate count is small.)
        double[][][] intervalRss = new double[m][m][];
        double[][] intervalNoise = new double[m][m];
        for (int a = 0; a < m; a++) {
            for (int b = a + 1; b < m; b++) {
                if (grid[b] - grid[a] < minSegmentLength) {
                    continue;
                }
                intervalRss[a][b] = rssByDegree(values, weights, grid[a], grid[b]);
                intervalNoise[a][b] = Stats.localNoiseVariance(values, grid[a], grid[b]);
            }
        }

        // Persistence pre-filter (candidate cleaning). PELT puts a boundary next to an extreme spike because
        // its cost sees the spike — a known proposer weakness, not a regime change. We test each candidate
        // for spike-drivenness on its PELT-neighbour window (a significant-looking change whose evidence
        // collapses when the weights around it are muted) and disallow those, so the selection runs on a
        // strictly cleaner candidate set. Each test is on the candidate's own neighbour window, so it is
        // order-free; doing it up front (rather than as a post-veto) also means a vetoed boundary can never
        // widen a survivor's window and drop a genuine localized change.
        boolean[] allowed = new boolean[m];
        Arrays.fill(allowed, true);
        for (int ci = 1; ci < m - 1; ci++) {
            if (intervalRss[ci - 1][ci + 1] == null) {
                continue; // window too short to assess; leave it to the DP's minSegmentLength constraint
            }
            if (isSpikeDriven(values, weights, intervalRss, intervalNoise, ci - 1, ci, ci + 1, grid)) {
                logger.trace("vetoing candidate boundary at index [{}] because it is spike-driven", grid[ci]);
                allowed[ci] = false;
            }
        }

        // Select the optimal partition over the cleaned candidates, then label each boundary in a single pass.
        int[] kept = selectKeptBoundaries(grid, allowed, intervalRss, intervalNoise);
        List<ChangeType> verified = new ArrayList<>();
        for (int k = 0; k < kept.length; k++) {
            int leftIdx = k == 0 ? 0 : kept[k - 1];
            int cpIdx = kept[k];
            int rightIdx = k == kept.length - 1 ? m - 1 : kept[k + 1];
            ChangeType labelled = labelChange(values, weights, offset, intervalRss, intervalNoise, leftIdx, cpIdx, rightIdx, grid);
            if (labelled != null) {
                logger.trace(
                    "candidate boundary at index [{}] between [{}] and [{}] is classified as [{}]",
                    grid[cpIdx],
                    grid[leftIdx],
                    grid[rightIdx],
                    labelled
                );
                verified.add(labelled);
            }
        }

        // No surviving boundary: report the whole series as stationary or non-stationary, as verifyAndClassify does.
        if (verified.isEmpty()) {
            Bic constant = constantBic(intervalRss[0][m - 1][0], grid[m - 1] - grid[0], intervalNoise[0][m - 1]);
            Bic noChange = bestNoChangeBic(intervalRss[0][m - 1], grid[m - 1] - grid[0], intervalNoise[0][m - 1]);
            if (noChange.degree() == 0) {
                verified.add(new ChangeType.Stationary());
            } else {
                double trendGain = constant.bic() - noChange.bic();
                double logPValue = -0.5 * Math.max(trendGain, 0.0);
                verified.add(
                    new ChangeType.NonStationary(
                        logPValue,
                        noChange.r2(),
                        slopeSign(values, weights, 0, n) < 0.0 ? "decreasing" : "increasing"
                    )
                );
            }
        }

        return verified;
    }

    /**
     * The optimal-partition DP over the boundary grid. {@code dp[b]} is the minimum total cost to partition
     * {@code [0, grid[b])} with {@code grid[b]} as a segment end, where each segment costs its best-degree
     * no-change BIC and each interior break costs an additional {@link #deltaBicThreshold} penalty. Only
     * grid points flagged in {@code allowed} (the spike pre-filter's survivors) plus the sentinels 0 and
     * n may be segment ends, and no segment shorter than minSegmentLength is permitted. Returns the kept
     * interior grid indices, sorted.
     */
    private int[] selectKeptBoundaries(int[] grid, boolean[] allowed, double[][][] intervalRss, double[][] intervalNoise) {
        int m = grid.length;
        double breakPenalty = deltaBicThreshold;
        double[] dp = new double[m];
        int[] from = new int[m];
        Arrays.fill(dp, Double.POSITIVE_INFINITY);
        Arrays.fill(from, -1);
        dp[0] = 0.0;
        for (int b = 1; b < m; b++) {
            if (b < m - 1 && allowed[b] == false) {
                continue; // an interior end must be an allowed (non-spike) candidate
            }
            for (int a = 0; a < b; a++) {
                if (a > 0 && allowed[a] == false) {
                    continue;
                }
                if (dp[a] == Double.POSITIVE_INFINITY || intervalRss[a][b] == null) {
                    continue; // unreachable predecessor or a sub-minSegmentLength segment
                }
                double segmentBic = bestNoChangeBic(intervalRss[a][b], grid[b] - grid[a], intervalNoise[a][b]).bic();
                double cost = dp[a] + segmentBic + (a == 0 ? 0.0 : breakPenalty);
                if (cost < dp[b]) {
                    dp[b] = cost;
                    from[b] = a;
                }
            }
        }

        List<Integer> keptReversed = new ArrayList<>();
        for (int b = from[m - 1]; b > 0; b = from[b]) {
            keptReversed.add(b);
        }
        int[] kept = new int[keptReversed.size()];
        for (int i = 0; i < kept.length; i++) {
            kept[i] = keptReversed.get(kept.length - 1 - i);
        }
        return kept;
    }

    /**
     * True if a candidate boundary is spike-driven: a significant-looking change on its window whose
     * evidence collapses once the weights immediately around it are muted. PELT proposes such boundaries
     * next to extreme points; they are a proposer artefact, not a regime change, and are pruned before
     * selection. The full no-change and split fits read the cached interval RSS; only the muted fit
     * (specific to cp) is recomputed. is not significant on its window is left to the DP/labelling
     * and is not our concern here.
     */
    private boolean isSpikeDriven(
        double[] values,
        double[] weights,
        double[][][] intervalRss,
        double[][] intervalNoise,
        int leftIdx,
        int cpIdx,
        int rightIdx,
        int[] grid
    ) {
        int start = grid[leftIdx];
        int cp = grid[cpIdx];
        int end = grid[rightIdx];
        int windowLength = end - start;
        double localNoiseVariance = intervalNoise[leftIdx][rightIdx];

        Bic noChange = bestNoChangeBic(intervalRss[leftIdx][rightIdx], windowLength, localNoiseVariance);
        Bic change = symmetricSplit(intervalRss, leftIdx, cpIdx, rightIdx, windowLength, localNoiseVariance);
        double bicGain = noChange.bic() - change.bic();
        if (bicGain < deltaBicThreshold) {
            return false;
        }

        // Re-fit the same per-segment model with the weights around the boundary muted; if the gain mostly survives,
        // the change is a sustained regime rather than a few extreme points. The muted split keeps each side's degree
        // from the full fit, so this tests the same alternative under muting.
        double[] mutedWeights = mutedWeights(weights, start, cp, end);
        double mutedNoChangeBic = calculateNoChangeBic(values, mutedWeights, start, end, localNoiseVariance).bic();
        double mutedChangeRss = Stats.stabilizeRss(
            splitPolynomialRss(values, mutedWeights, start, cp, end, change.degree()),
            windowLength,
            localNoiseVariance
        );
        double mutedChangeBic = bicFromRss(mutedChangeRss, windowLength, 2 * (change.degree() + 1));
        return changePersists(bicGain, mutedNoChangeBic - mutedChangeBic) == false;
    }

    /**
     * Labels a DP-selected boundary as a step or trend on the segment bounded by its neighbours in
     * the partition, or returns null if its (symmetric-degree) gain does not clear the significance
     * threshold. Spike-drivenness has already been pruned up front (see {@link #isSpikeDriven}), so
     * this pass does not re-test it. All fits read the cached interval RSS, and it is a single pass
     * — removing a sub-threshold boundary never widens a neighbour's window.
     */
    private ChangeType labelChange(
        double[] values,
        double[] weights,
        double offset,
        double[][][] intervalRss,
        double[][] intervalNoise,
        int leftIdx,
        int cpIdx,
        int rightIdx,
        int[] grid
    ) {
        int cp = grid[cpIdx];
        int windowLength = grid[rightIdx] - grid[leftIdx];
        double localNoiseVariance = intervalNoise[leftIdx][rightIdx];

        Bic noChange = bestNoChangeBic(intervalRss[leftIdx][rightIdx], windowLength, localNoiseVariance);
        Bic change = symmetricSplit(intervalRss, leftIdx, cpIdx, rightIdx, windowLength, localNoiseVariance);

        if (toPValue(meanGain) < pValueThreshold) {
            // A mean change (level or trend).
            double logPValue = -0.5 * Math.max(meanGain, 0.0);
            // Local level and slope on a short shoulder window each side of the boundary. The series is
            // mean-shifted, so add the offset back for a level-relative percentage; the step size and
            // slope are themselves shift-invariant. The denominator is floored by the local noise scale
            // so a near-zero baseline (a zero error count, a flat->ramp leak) does not blow up.
            double floor = Math.max(Math.sqrt(Math.max(localNoiseVariance, 0.0)), SCALE_FLOOR);
            int shoulder = Math.min(minSegmentLength, windowLength);
            double[] left = localLine(values, weights, Math.max(grid[leftIdx], cp - shoulder), cp, cp);
            double[] right = localLine(values, weights, cp, Math.min(grid[rightIdx], cp + shoulder), cp);
            double levelBefore = left[0] + offset;
            double levelAfter = right[0] + offset;
            if (change.degree() == 0) {
                double stepPercent = 100.0 * (levelAfter - levelBefore) / Math.max(Math.abs(levelBefore), floor);
                return new ChangeType.StepChange(logPValue, cp, stepPercent);
            }
            double noChangeRss = Math.max(noChange.rss(), VAR_FLOOR * windowLength);
            double r2 = Math.max(0.0, Math.min(1.0, 1.0 - (change.rss() / noChangeRss)));
            double levelAtChange = 0.5 * (levelBefore + levelAfter);
            double gradientPercent = 100.0 * (right[1] - left[1]) / Math.max(Math.abs(levelAtChange), floor);
            return new ChangeType.TrendChange(logPValue, r2, cp, gradientPercent);
        }

        if (detectVarianceShifts) {
            // Not a significant mean change, but the two sides may have different noise levels. Estimate
            // each side's noise with a robust scale: the IQR of its first differences. First-differencing
            // removes level/trend, and the IQR's 25%-per-tail breakdown ignores a small fraction of outliers,
            // so a lone spike (which would inflate an RSS-based variance) cannot pose as a variance regime.
            // This makes the value channel a robust, full-resolution backstop for a short, strong variance
            // change the (coarser, discounted) dispersion channel misses. Scored as a one-variance null vs
            // a two-variance split, in the same BIC currency.
            int start = grid[leftIdx];
            int end = grid[rightIdx];
            int nLeft = cp - start;
            int nRight = end - cp;
            double varLeft = Stats.interquartileNoiseVariance(values, start, cp);
            double varRight = Stats.interquartileNoiseVariance(values, cp, end);
            // Floor both scales at one quantization step squared. Below the measurement granularity the
            // IQR of a discrete first-difference distribution is unstable: a near-constant integer series
            // (e.g. a 4/5/6 oscillation) has differences on {0, +/-1}, and two halves with slightly different
            // transition densities read as a large (sub-unit) variance ratio that, over a long window, the
            // BIC calls overwhelmingly significant. Flooring at q^2 makes both sides equal when the noise
            // is unresolved (so no change is claimed), while leaving continuous data (q ~ 0) and any genuine
            // above-granularity variance change (scales >> q) untouched.
            double quantum = Stats.quantizationStep(values, start, end);
            double varFloor = quantum * quantum;
            varLeft = Math.max(varLeft, varFloor);
            varRight = Math.max(varRight, varFloor);
            double varWindow = (nLeft * varLeft + nRight * varRight) / windowLength;
            // Discount the effective sample size for the variance test. A robust IQR-of-first-differences
            // scale carries far less information per sample than a least-squares residual: the IQR has
            // ~0.35 asymptotic efficiency for a Gaussian scale and first-differencing correlates adjacent
            // terms, so the estimate is much noisier than its n suggests. Discounting to this efficiency
            // calibrates the test so it fires only on a genuinely strong variance change.
            double varianceEffectiveFactor = effectiveSampleFactor * 0.35;
            double noChangeBic = bicFromRss(varWindow * windowLength, windowLength, 1, varianceEffectiveFactor);
            double bicLeft = bicFromRss(varLeft * nLeft, nLeft, 1, varianceEffectiveFactor);
            double bicRight = bicFromRss(varRight * nRight, nRight, 1, varianceEffectiveFactor);
            double splitBic = bicLeft + bicRight;
            double varianceGain = noChangeBic - splitBic;
            if (toPValue(varianceGain) < pValueThreshold) {
                double logPValue = -0.5 * Math.max(varianceGain, 0.0);
                double scaleBefore = Math.sqrt(varLeft);
                double scaleAfter = Math.sqrt(varRight);
                double scaleFloor = Math.max(Math.sqrt(Math.max(localNoiseVariance, 0.0)), SCALE_FLOOR);
                double magnitudePercent = 100.0 * (scaleAfter - scaleBefore) / Math.max(scaleBefore, scaleFloor);
                return new ChangeType.DistributionChange(logPValue, cp, magnitudePercent);
            }
        }
    }

    /**
     * Reads the fitted level (at {@code atIndex}) and slope (per bucket) of a weighted linear fit over
     * {@code [start, end)}, returned as {@code {value at atIndex, slope per bucket}}, used for a reported
     * change magnitude.
     */
    private double[] localLine(double[] values, double[] weights, int start, int end, int atIndex) {
        int length = end - start;
        double centre = 0.5 * (length - 1);
        double scale = centre > 0.0 ? centre : 1.0;
        double[] coefficients = fitPolynomial(values, weights, start, end, 1).parameters();
        double mappedX = ((atIndex - start) - centre) / scale;
        double value = coefficients[0] + coefficients[1] * mappedX;
        double slopePerBucket = coefficients[1] / scale;
        return new double[] { value, slopePerBucket };
    }

    /**
     * The best symmetric-degree split alternative for a boundary: the same polynomial degree on both
     * sides, combining the cached half RSS. Returns the chosen degree's BIC, summed RSS and degree
     * (the r2 field is unused).
     */
    private Bic symmetricSplit(
        double[][][] intervalRss,
        int leftIdx,
        int cpIdx,
        int rightIdx,
        int windowLength,
        double localNoiseVariance
    ) {
        double bestBic = Double.POSITIVE_INFINITY;
        double bestRss = 0.0;
        int bestDegree = 0;
        for (int degree = 0; degree <= maxDegree; degree++) {
            double leftRss = intervalRss[leftIdx][cpIdx][degree];
            double rightRss = intervalRss[cpIdx][rightIdx][degree];
            double rss = Math.max(leftRss + rightRss, VAR_FLOOR * windowLength);
            double bic = bicFromRss(Stats.stabilizeRss(rss, windowLength, localNoiseVariance), windowLength, 2 * (degree + 1));
            if (bic < bestBic) {
                bestBic = bic;
                bestRss = rss;
                bestDegree = degree;
            }
        }
        return new Bic(bestBic, bestRss, 0.0, bestDegree);
    }

    /**
     * Calculates BIC for a constant model by regularizing its RSS with local noise variance.
     */
    private Bic calculateConstantBic(double rss, int windowLength, double localNoiseVariance) {
        double stabilizedRss = Stats.stabilizeRss(rss, windowLength, localNoiseVariance);
        double bic = bicFromRss(stabilizedRss, windowLength, 1);
        return new Bic(bic, stabilizedRss, 0.0, 0);
    }

    /**
     * Calculates BIC for the no-change model by fitting a single polynomial and regularizing its RSS with
     * local noise variance.
     */
    private Bic calculateNoChangeBic(double[] values, double[] weights, int start, int end, double localNoiseVariance) {
        return bestNoChangeBic(rssByDegree(values, weights, start, end), end - start, localNoiseVariance);
    }

    /** The best (minimum-BIC) no-change polynomial degree for an interval, read from its cached per-degree RSS. */
    private Bic bestNoChangeBic(double[] rssByDegree, int windowLength, double localNoiseVariance) {
        double rss0 = rssByDegree[0];
        double bestBic = bicFromRss(Stats.stabilizeRss(rss0, windowLength, localNoiseVariance), windowLength, 1);
        double bestRss = rss0;
        double bestR2 = 0.0;
        int bestDegree = 0;
        for (int degree = 1; degree <= maxDegree; degree++) {
            double rss = rssByDegree[degree];
            double bic = bicFromRss(Stats.stabilizeRss(rss, windowLength, localNoiseVariance), windowLength, degree + 1);
            if (bic < bestBic) {
                bestBic = bic;
                bestRss = rss;
                bestR2 = Math.max(0.0, Math.min(1.0, 1.0 - (rss / Math.max(rss0, VAR_FLOOR * windowLength))));
                bestDegree = degree;
            }
        }
        return new Bic(bestBic, bestRss, bestR2, bestDegree);
    }

    /**
     * The interior PELT candidates (those strictly inside the series), de-duplicated and sorted, used
     * directly as the structural boundaries to verify.
     */
    private int[] interiorCandidates(int[] structuralCandidates, int n) {
        TreeSet<Integer> interior = new TreeSet<>();
        for (int c : structuralCandidates) {
            if (c > 0 && c < n) {
                interior.add(c);
            }
        }
        return interior.stream().mapToInt(Integer::intValue).toArray();
    }

    /**
     * Reduces weights in the candidate's immediate vicinity to check if the evidence for change is
     * predominantly driven by a small number of extreme values, which would suggest a less reliable
     * variance change rather than a structural step/trend change.
     */
    private double[] mutedWeights(double[] weights, int start, int cp, int end) {
        double[] muted = Arrays.copyOf(weights, weights.length);
        int shoulder = Math.max(1, Math.min(3, minSegmentLength / 4));
        int muteStart = Math.max(start, cp - shoulder);
        int muteEnd = Math.min(end, cp + shoulder);
        for (int i = muteStart; i < muteEnd; i++) {
            muted[i] *= MUTED_WEIGHT_FACTOR;
        }
        return muted;
    }

    /**
     * The weighted RSS at every degree {@code 0..maxDegree} over {@code [start, end)}. Degree 0 is the
     * weighted RSS about the mean; higher degrees are read off a <em>single</em> {@code maxDegree}
     * accumulation via {@link LeastSquaresOnlineRegression#residualVarianceForDegree} (each lower-degree
     * normal-equation system is a leading principal submatrix of the top one), so this is one pass over
     * the window rather than one per degree.
     */
    private double[] rssByDegree(double[] values, double[] weights, int start, int end) {
        double[] rss = new double[maxDegree + 1];
        rss[0] = polynomialRss(values, weights, start, end, 0);
        if (maxDegree >= 1) {
            LeastSquaresOnlineRegression regression = fitPolynomial(values, weights, start, end, maxDegree);
            int length = end - start;
            for (int degree = 1; degree <= maxDegree; degree++) {
                rss[degree] = Math.max(regression.residualVarianceForDegree(degree), SCALE_FLOOR * SCALE_FLOOR) * length;
            }
        }
        return rss;
    }

    /**
     * Checks if the BIC gain of the full data is above threshold and not predominantly driven
     * by a small number of extreme values, as approximated by the gain with muted weights in
     * the candidate change point's vicinity.
     */
    private boolean changePersists(double fullGain, double mutedGain) {
        if (fullGain < deltaBicThreshold || mutedGain <= 0.0) {
            return false;
        }
        if (mutedGain >= deltaBicThreshold) {
            return true;
        }
        if (mutedGain < 0.5 * deltaBicThreshold) {
            return false;
        }
        return (mutedGain / fullGain) >= MIN_MUTED_GAIN_RATIO;
    }

    /**
     * BIC for an RSS-based Gaussian residual model, with the sample count discounted to the
     * number of statistically independent observations ({@code effectiveSampleFactor * n}).
     * The per-sample variance estimate {@code rss/n} still uses the actual {@code n}, but the
     * log-likelihood weight and the complexity penalty use the effective count, so a channel
     * of correlated (overlapping) samples is not credited with more evidence than it carries.
     * For {@code effectiveSampleFactor == 1} this is the standard BIC unchanged.
     */
    private double bicFromRss(double rss, int n, int k) {
        return bicFromRss(rss, n, k, effectiveSampleFactor);
    }

    private double bicFromRss(double rss, int n, int k, double effectiveFactor) {
        double effectiveN = Math.max(effectiveFactor * n, 2.0);
        double flooredRss = Math.max(rss, n * Math.max(quantizationVariance, VAR_FLOOR));
        return effectiveFactor * n * Math.log(flooredRss / n) + k * Math.log(effectiveN);
    }

    /** Fits left/right polynomial models split at the candidate boundary and sums their RSS. */
    private double splitPolynomialRss(double[] values, double[] weights, int start, int cp, int end, int degree) {
        double left = polynomialRss(values, weights, start, cp, degree);
        double right = polynomialRss(values, weights, cp, end, degree);
        return Math.max(left + right, 1e-10);
    }

    /** Fits a single polynomial model over a window and returns its weighted RSS. */
    private double polynomialRss(double[] values, double[] weights, int start, int end, int degree) {
        if (degree <= 0) {
            return Stats.weightedRss(values, weights, start, end);
        }
        LeastSquaresOnlineRegression reg = fitPolynomial(values, weights, start, end, degree);
        return Math.max(reg.residualVariance(), VAR_FLOOR) * (end - start);
    }

    /** Computes the sign of the slope for a linear fit over the specified window. */
    private double slopeSign(double[] values, double[] weights, int start, int end) {
        LeastSquaresOnlineRegression reg = fitPolynomial(values, weights, start, end, 1);
        return reg.slopeSign();
    }

    private LeastSquaresOnlineRegression fitPolynomial(double[] values, double[] weights, int start, int end, int degree) {
        LeastSquaresOnlineRegression reg = new LeastSquaresOnlineRegression(degree);
        // Map x affinely onto [-1, 1] (centred on the window midpoint, scaled by the half-span) rather
        // than using raw indices. RSS is invariant under an affine reparametrization of a degree-d
        // polynomial fit (the function space is unchanged, so the residuals are identical), but the
        // conditioning is not: raw indices push the moment matrix to ~x^(2*degree), e.g. ~1e19 for a
        // cubic over a 2000-point window, which trips the SVD singularity guard and silently degrades
        // the fit to mean-only. On [-1, 1] every moment term is O(1), so the conditioning is the
        // intrinsic monomial-basis bound (~1e3 for a cubic), independent of window length.
        int length = end - start;
        double centre = 0.5 * (length - 1);
        double scale = centre > 0.0 ? centre : 1.0;
        for (int i = start; i < end && i < values.length; i++) {
            double x = ((i - start) - centre) / scale;
            reg.add(x, values[i], weights[i]);
        }
        return reg;
    }

    private double toPValue(double bicImprovement) {
        return Math.max(Double.MIN_VALUE, Math.min(1.0, Math.exp(-0.5 * Math.max(bicImprovement, 0.0))));
    }

    private double toBic(double pValue) {
        return -2.0 * Math.log(Math.max(pValue, Double.MIN_VALUE));
    }

    // Muting drops the weights immediately around a candidate; if the BIC gain mostly survives the
    // muting the change is supported by a sustained regime rather than a few extreme points (which
    // belong to the pulse stream).
    private static final double MUTED_WEIGHT_FACTOR = 0.1;
    private static final double MIN_MUTED_GAIN_RATIO = 0.2;

    // Absolute floor for the denominator of a percent-change magnitude, to avoid division by zero
    // when both the baseline level and the local noise scale are ~0 (a perfectly constant series).
    private static final double SCALE_FLOOR = 1e-10;
    private static final double VAR_FLOOR = SCALE_FLOOR * SCALE_FLOOR;

    private final int minSegmentLength;
    // Highest polynomial order the validator may fit, applied symmetrically to the single no-change
    // (null) model and to each segment of the change (alternative) model. The alternative is the same
    // model class split at the candidate, so it is strictly more flexible than the null at the same
    // degree (it can fit a step or kink); the null is never allowed a higher degree than the alternative.
    // The degree is set per channel by the caller: the value channel uses a high order so smooth drift
    // is absorbed by the null and not split, while the dispersion channel (much shorter segments) uses
    // linear so a low-high-low variance bump is not absorbed and is detected.
    private final int maxDegree;
    private final double pValueThreshold;
    private final double deltaBicThreshold;
    // Quantization (rounding) noise variance q^2/12 of the current series, the floor on the per-point
    // variance in the BIC (see bicFromRss). Set per call from the series' first-difference granularity;
    // ~0 for continuous data.
    private double quantizationVariance;
    // Fraction of the channel's samples that are statistically independent (1.0 for a raw value channel;
    // stride/window for an overlapping dispersion channel). Discounts the BIC evidence so correlated
    // samples are not over-counted.
    private final double effectiveSampleFactor;
    // When true, a variance-driven boundary (per-segment profiled split significant, mean split not) is
    // reported as a DistributionChange. On for the value channel (a backstop for short, strong variance
    // changes); off for the dispersion channel.
    private final boolean detectVarianceShifts;

    private record Bic(double bic, double rss, double r2, int degree) {}
}
