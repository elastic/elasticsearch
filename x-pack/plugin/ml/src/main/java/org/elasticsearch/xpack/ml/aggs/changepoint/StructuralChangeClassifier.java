/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

/**
 * Verifies and labels structural (step/trend) changes using a BIC classifier.
 */
public class StructuralChangeClassifier {

    private static final Logger logger = LogManager.getLogger(StructuralChangeDetector.class);

    public StructuralChangeClassifier(int minSegmentLength, int maxDegree, double pValueThreshold) {
        this.minSegmentLength = minSegmentLength;
        this.maxDegree = maxDegree;
        this.pValueThreshold = pValueThreshold;
        this.deltaBicThreshold = toBic(pValueThreshold);
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
    List<ChangeType> selectAndClassify(double[] values, double[] weights, int[] structuralCandidates) {
        int n = values.length;
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
            ChangeType labelled = labelChange(intervalRss, intervalNoise, leftIdx, cpIdx, rightIdx, grid);
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
            Bic noChange = bestNoChangeBic(intervalRss[0][m - 1], grid[m - 1] - grid[0], intervalNoise[0][m - 1]);
            if (noChange.degree() == 0) {
                verified.add(new ChangeType.Stationary());
            } else {
                verified.add(
                    new ChangeType.NonStationary(
                        pValueThreshold,
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

        Bic bicNoChange = bestNoChangeBic(intervalRss[leftIdx][rightIdx], windowLength, localNoiseVariance);
        Bic change = symmetricSplit(intervalRss, leftIdx, cpIdx, rightIdx, windowLength, localNoiseVariance);
        double bicGain = bicNoChange.bic() - change.bic();
        if (bicGain < deltaBicThreshold) {
            return false;
        }

        double[] mutedWeights = mutedWeights(weights, start, cp, end);
        double mutedNoChangeBic = calculateNoChangeBic(values, mutedWeights, start, end, localNoiseVariance).bic();
        double mutedChangeRss = splitPolynomialRss(values, mutedWeights, start, cp, end, change.degree());
        double mutedChangeBic = bicFromRss(
            Stats.stabilizeRss(mutedChangeRss, windowLength, localNoiseVariance),
            windowLength,
            2 * (change.degree() + 1)
        );
        return changePersists(bicGain, mutedNoChangeBic - mutedChangeBic) == false;
    }

    /**
     * Labels a DP-selected boundary as a step or trend on the segment bounded by its neighbours in
     * the partition, or returns null if its (symmetric-degree) gain does not clear the significance
     * threshold. Spike-drivenness has already been pruned up front (see {@link #isSpikeDriven}), so
     * this pass does not re-test it. All fits read the cached interval RSS, and it is a single pass
     * — removing a sub-threshold boundary never widens a neighbour's window.
     */
    private ChangeType labelChange(double[][][] intervalRss, double[][] intervalNoise, int leftIdx, int cpIdx, int rightIdx, int[] grid) {
        int cp = grid[cpIdx];
        int windowLength = grid[rightIdx] - grid[leftIdx];
        double localNoiseVariance = intervalNoise[leftIdx][rightIdx];

        Bic bicNoChange = bestNoChangeBic(intervalRss[leftIdx][rightIdx], windowLength, localNoiseVariance);
        Bic change = symmetricSplit(intervalRss, leftIdx, cpIdx, rightIdx, windowLength, localNoiseVariance);
        double pValue = toPValue(bicNoChange.bic() - change.bic());
        if (pValue >= pValueThreshold) {
            return null;
        }
        if (change.degree() == 0) {
            return new ChangeType.StepChange(pValue, cp);
        }
        double r2 = Math.max(0.0, Math.min(1.0, 1.0 - (change.rss() / Math.max(bicNoChange.rss(), 1e-10))));
        return new ChangeType.TrendChange(pValue, r2, cp);
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
            double rss = Math.max(intervalRss[leftIdx][cpIdx][degree] + intervalRss[cpIdx][rightIdx][degree], 1e-10);
            double bic = bicFromRss(Stats.stabilizeRss(rss, windowLength, localNoiseVariance), windowLength, 2 * (degree + 1));
            if (bic < bestBic) {
                bestBic = bic;
                bestRss = rss;
                bestDegree = degree;
            }
        }
        return new Bic(bestBic, bestRss, 0.0, bestDegree);
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
                bestR2 = Math.max(0.0, Math.min(1.0, 1.0 - (rss / Math.max(rss0, 1e-10))));
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
     * Calculates BIC for the no-change model by fitting a single polynomial and regularizing its
     * RSS with local noise variance.
     */
    private Bic calculateNoChangeBic(double[] values, double[] weights, int start, int end, double localNoiseVariance) {
        return bestNoChangeBic(rssByDegree(values, weights, start, end), end - start, localNoiseVariance);
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
                rss[degree] = Math.max(regression.residualVarianceForDegree(degree) * length, 1e-10);
            }
        }
        return rss;
    }

    /**
     * Checks if the BIC gain of the full data is above threshold and not predominantly driven by
     * a small number of extreme values, as approximated by the gain with muted weights in the
     * candidate change point's vicinity.
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

    /** Computes standard BIC for RSS-based Gaussian residual models. */
    private double bicFromRss(double rss, int n, int k) {
        return n * Math.log(Math.max(rss, 1e-10) / n) + k * Math.log(n);
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
        return Math.max(reg.residualVariance() * (end - start), 1e-10);
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

    private record Bic(double bic, double rss, double r2, int degree) {}
}
