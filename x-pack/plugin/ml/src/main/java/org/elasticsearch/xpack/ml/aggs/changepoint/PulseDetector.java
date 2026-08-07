/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.apache.commons.math3.util.FastMath;
import org.elasticsearch.common.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class PulseDetector {

    // Half-width of the centred window used for the rolling-median baseline that residuals are taken from.
    private static final int WEIGHT_HALF_WINDOW = 5;
    // Width of the local window used to fit the robust Theil-Sen boundary line that restores the first/last
    // WEIGHT_HALF_WINDOW residuals (where the centred rolling median has collapsed). Wide enough for a stable
    // slope, local enough to track the boundary.
    private static final int BOUNDARY_LINE_WINDOW = 2 * WEIGHT_HALF_WINDOW + 1;
    // Candidate ("long list") threshold: a point is a candidate excursion when its residual from the local
    // rolling-median baseline exceeds this many robust sigmas of the residual scale. Generous pre-filter; the
    // significance decision is the KDE gate.
    private static final double PULSE_Z_THRESHOLD = 3.0;
    // We report at most this many pulses — the highest-z excursions. A small floor plus a slowly-growing fraction
    // of the series length, so a pathological or very noisy series cannot drown the output in spikes.
    private static final int MAX_REPORTED_PULSES_FLOOR = 5;
    private static final double MAX_REPORTED_PULSES_FRACTION = 0.02;
    // Fraction of the series removed from the significance-gate null (top excursions by peak z), so a genuine
    // anomaly is judged against a quiet background rather than one that still contains its neighbours. This is the
    // anomaly/population boundary: if more than this fraction of the series looks like the tested excursion, its
    // siblings remain in the null and it correctly reads as an ordinary member of a population, not a point pulse.
    // A cluster counts as one excursion here and is removed as a whole span, so a wide spike is one anomaly, not
    // its width.
    private static final double BACKGROUND_ANOMALY_FRACTION = 0.05;
    // Minimum KDE bandwidth as a fraction of the stabilized residual range, so the kernel width never collapses
    // to zero on a flat/degenerate background (which would make the empirical-tail gate unable to flag any outlier).
    // Small enough to be inert on a well-spread background, where Silverman's bandwidth dominates.
    private static final double BANDWIDTH_RANGE_FLOOR = 0.02;

    private final int minSegmentLength;
    private final double pValueThreshold;

    private static final Logger logger = LogManager.getLogger(PulseDetector.class);

    PulseDetector(int minSegmentLength, double pValueThreshold) {
        this.pValueThreshold = pValueThreshold;
        this.minSegmentLength = minSegmentLength;
    }

    /**
     * Detects spike/dip events as point excursions from a local rolling-median baseline. Working off the local
     * residual (not raw values versus a global centre) means level structure is removed — a multi-level series
     * cannot mask an excursion on a low-level regime — and smooth curvature is tracked, so a gradually bending
     * trend does not leave the large residuals a piecewise-linear fit would.
     *
     * The pipeline is:
     * 1. propose a long list of candidates whose residual exceeds {@link #PULSE_Z_THRESHOLD} robust sigmas of
     *    the residual scale,
     * 2. merge adjacent candidates of the same sign into single excursions, dropping any that span a full
     *    {@code minSegmentLength},
     * 3. sort the excursions by their peak z,
     * 4. build ONE Gaussian-KDE null from the residuals with the top {@code ceil(BACKGROUND_ANOMALY_FRACTION * n)}
     *    excursions removed, and keep an excursion only if its peak's Bonferroni-corrected tail probability under
     *    that null clears the threshold; surface at most {@code max(MAX_REPORTED_PULSES_FLOOR, MAX_REPORTED_PULSES_FRACTION * n)}.
     *
     * Removing the tested excursions from the single null at once is deliberate: scoring each against a null that
     * contains the others (leave-one-out) means the largest spike and dip mask all the rest. Removing them together
     * means several genuinely distinct excursions are each judged against the quiet remainder and all survive,
     * while a recurring population is still rejected. How many we remove — {@code BACKGROUND_ANOMALY_FRACTION} of
     * the series — is the anomaly/population boundary, kept separate from the output cap so that decision is
     * statistical, not a side effect of how many pulses we happen to report.
     */
    public List<ChangeType> detect(double[] values) {
        int n = values.length;
        if (n < 4) {
            return new ArrayList<>();
        }

        // Residual signal and its robust scale. This scale is only the generous pre-filter for the candidate
        // long list; the significance decision is the KDE gate below. It is the largest of three estimators:
        // - the MAD of the residuals. We deliberately use the plain MAD, not the inter-decile composite scale:
        // the MAD's 50%-breakdown ignores a localized cluster of same-sign excursions, whereas the inter-decile
        // term (10%-per-tail) is pulled up the moment such a cluster exceeds ~10% of the points, so the cluster
        // inflates the very scale used to detect it and slips under the candidate threshold. When the MAD
        // collapses on a flat/quantized background it simply falls through to the tiny difference-based floor
        // below, which is what we want -- the excursions then all become candidates and the KDE gate decides.
        // - the global first-difference noise from the median of |first differences| (meaningful on smooth data),
        // - a tie-robust movement scale from the IQR of first differences. The median estimator collapses to
        // zero on a quantized or stepped series with many repeated values (>50% tied differences) -- and then
        // even a tiny residual reads as many sigma, so a trend's extreme endpoint (the series min/max, which
        // the value gate trivially calls a tail event) is mis-proposed as a spurious spike/dip. The IQR
        // tolerates 25%-per-tail ties, recovering the characteristic step size; it is taken as a per-sample
        // equivalent (differences have sqrt(2) the per-sample spread) so it matches the median estimator on
        // clean data and only ever raises the scale where the median has collapsed. These two difference-based
        // terms also carry the recurring-population guard: a frequent/periodic excursion population makes its
        // rises and falls frequent large first differences, so they raise the scale and keep those points off
        // the candidate list -- a localized cluster, whose few edges barely move the difference quantiles, does
        // not, which is exactly the distinction the inter-decile term could not draw.
        double[] residuals = Stats.rollingMedianResiduals(values, WEIGHT_HALF_WINDOW);
        // The centred rolling-median window collapses at the ends (a point becomes its own median), leaving the
        // first/last WEIGHT_HALF_WINDOW residuals ~0 -- so a spike or dip sitting in those boundary regions was
        // invisible to the proposer. Re-residual the boundary points against a robust Theil-Sen line over a local
        // window: a point on a local trend keeps a ~0 residual (no false boundary spike), while a genuine boundary
        // excursion the robust line ignores gets a large residual and is proposed like any interior point.
        Stats.applyBoundaryLineResiduals(values, residuals, WEIGHT_HALF_WINDOW, BOUNDARY_LINE_WINDOW);
        double maxAbs = 0.0;
        for (double v : values) {
            maxAbs = Math.max(maxAbs, Math.abs(v));
        }
        double movementScale = Math.sqrt(Stats.interquartileNoiseVariance(values, 0, n) / 2.0);
        double scale = Math.max(
            Stats.localRobustScale(residuals, 0, n, maxAbs),
            Math.max(Math.sqrt(Stats.globalNoiseVariance(values)), movementScale)
        );
        logger.trace("Pulse detection on series of length [{}] has residual scale [{}]", n, scale);
        if (scale <= 0.0) {
            return new ArrayList<>();
        }

        // Long list, then merge adjacent same-sign candidates into excursions (dropping regime-length runs).
        List<Excursion> excursions = mergeCandidates(residuals, scale, n);
        logger.trace("Pulse detection found [{}] initial excursions", excursions.size());
        if (excursions.isEmpty()) {
            return new ArrayList<>();
        }

        // Order by peak z (most extreme first), then take two decoupled prefixes of that order:
        // - excludedFromNull: the top ceil(BACKGROUND_ANOMALY_FRACTION * n) excursions, cut from the gate's null
        // so a genuine anomaly is judged against a quiet background. Any excursion beyond this stays in the
        // null, so once the anomalies exceed this fraction they see their own neighbours there and are rejected
        // as a population -- this prefix, not the output cap, is the anomaly/population boundary.
        // - tested: the top max(MAX_REPORTED_PULSES_FLOOR, MAX_REPORTED_PULSES_FRACTION * n) excursions, the only
        // ones we gate and can surface. Both are views of the same peak-z order; a cluster is one excursion in
        // either, and it is removed from the null as a whole span (see backgroundExcluding), so a wide spike
        // counts once.
        excursions.sort(Comparator.comparingDouble((Excursion e) -> e.peakZ()).reversed());
        int reportLimit = Math.max(MAX_REPORTED_PULSES_FLOOR, (int) Math.ceil(MAX_REPORTED_PULSES_FRACTION * n));
        int backgroundExclusion = (int) Math.ceil(BACKGROUND_ANOMALY_FRACTION * n);
        List<Excursion> excludedFromNull = excursions.subList(0, Math.min(backgroundExclusion, excursions.size()));
        List<Excursion> tested = excursions.subList(0, Math.min(reportLimit, excursions.size()));

        // Gate each excursion against a KDE null, all in an asinh-stabilised value space. Telemetry noise is
        // typically multiplicative (its spread grows with magnitude), so a single bandwidth on raw values is
        // far too narrow up in the high-magnitude tail and flags ordinary large values. asinh(x / scale) is
        // monotonic (so the gate still asks the value-based question "is this magnitude one we see at other
        // times?") but removes the magnitude dependence so one bandwidth is valid across orders of magnitude.
        //
        // Two distinct scales are used deliberately. The KDE null is the stabilised background values: so a
        // magnitude that recurs elsewhere has neighbours and is not surprising. The bandwidth (the kernel's
        // smoothing width) is taken from the stabilised residuals, not the stabilised values: a level change
        // makes the value distribution bimodal and would inflate a value-derived width, making the gate
        // suppress genuine within-regime spike after a step.
        double[] backgroundValues = backgroundExcluding(values, excludedFromNull, n);
        double stabilizingScale = Stats.asinhScale(backgroundValues);
        double[] stabilizedBackground = Stats.asinhStabilize(backgroundValues, stabilizingScale);
        double[] stabilizedValues = Stats.asinhStabilize(values, stabilizingScale);
        double[] stabilizedResiduals = Stats.rollingMedianResiduals(stabilizedValues, WEIGHT_HALF_WINDOW);
        // Floor the KDE bandwidth at a small multiple of the stabilized residual range. On a flat (e.g. all-
        // zero or constant) background the estimated spread collapses to ~0, and the empirical-step fallback
        // (Bonferroni-corrected over n) cannot call any single point significant so a clear outlier is missed.
        // A floor tied to the residual range is always well-defined and needs no granularity estimation; we
        // use the range of the residuals (not the values) so it strips level/step structure (a large step
        // must not widen the kernel and smear a within-regime spike) while still reflecting the scale of the
        // excursions being tested. The fraction is small enough that on a well-spread background Silverman's
        // value dominates and the floor is inert; it only rescues the degenerate case. Working in asinh space
        // keeps it scale-robust, so a huge spike does not mask a moderate one.
        double residualRange = Stats.range(stabilizedResiduals);
        double minBandwidth = BANDWIDTH_RANGE_FLOOR * residualRange;
        double bandwidth = Stats.kdeBandwidth(backgroundExcluding(stabilizedResiduals, excludedFromNull, n), minBandwidth);
        double logN = Math.log(n);
        List<ChangeType> pulses = new ArrayList<>();
        for (Excursion e : tested) {
            double stabilizedValue = FastMath.asinh(values[e.peak()] / stabilizingScale);
            double logPValue = Math.min(
                0.0,
                Stats.kdeLogTailProbability(stabilizedValue, stabilizedBackground, bandwidth, e.sign()) + logN
            );
            double pValue = Math.exp(logPValue);
            if (pValue < pValueThreshold) {
                // Percent deviation of the peak from the local rolling-median baseline (signed: + for a spike,
                // - for a dip). Floored by the robust residual scale so a baseline near zero does not blow the
                // percentage up.
                double deviation = residuals[e.peak()];
                double baseline = values[e.peak()] - deviation;
                double magnitudePercent = 100.0 * deviation / Math.max(Math.abs(baseline), scale);
                // Short description of the size of the peak.
                String description = Strings.format("%+.2f%% change from rolling median", magnitudePercent);
                pulses.add(
                    e.sign() > 0
                        ? new ChangeType.Spike(logPValue, e.peak(), description)
                        : new ChangeType.Dip(logPValue, e.peak(), description)
                );
            }
        }
        pulses.sort(Comparator.comparingInt(ChangeType::changePoint));
        logger.trace("Pulse detection found [{}] significant pulses", pulses.size());
        return pulses;
    }

    /**
     * Builds the long list (residual z above threshold) and merges strictly adjacent same-sign candidates into
     * excursions. We do NOT chain across gaps: repeated/recurring excursions are a structural/dispersion matter,
     * not something the pulse stream should fuse. A run spanning a full minimum segment length is dropped — that
     * is a regime, owned by the structural/dispersion channels, not a point pulse.
     */
    private List<Excursion> mergeCandidates(double[] residuals, double scale, int n) {
        List<Excursion> excursions = new ArrayList<>();
        int i = 0;
        while (i < n) {
            double z = Math.abs(residuals[i]) / scale;
            if (z <= PULSE_Z_THRESHOLD) {
                i++;
                continue;
            }
            int sign = residuals[i] >= 0.0 ? 1 : -1;
            int start = i;
            int peak = i;
            double peakZ = z;
            int j = i + 1;
            while (j < n && Math.abs(residuals[j]) / scale > PULSE_Z_THRESHOLD && (residuals[j] >= 0.0 ? 1 : -1) == sign) {
                double zj = Math.abs(residuals[j]) / scale;
                if (zj > peakZ) {
                    peakZ = zj;
                    peak = j;
                }
                j++;
            }
            if (j - start < minSegmentLength) {
                excursions.add(new Excursion(start, j, sign, peak, peakZ));
            }
            i = j;
        }
        return excursions;
    }

    /** Values of all points not covered by any of the given excursions, forming the KDE null. */
    private static double[] backgroundExcluding(double[] values, List<Excursion> excursions, int n) {
        boolean[] excluded = new boolean[n];
        for (Excursion e : excursions) {
            for (int i = e.start(); i < e.end(); i++) {
                excluded[i] = true;
            }
        }
        // We exclude the first and last point since we also exclude them as candidates, so the count must be
        // taken over the same [1, n - 1) range as the fill below -- otherwise the array is oversized and the
        // trailing slots stay 0.0, injecting phantom zero-valued samples into the KDE null (which, for a series
        // away from zero, masks dips by making low values look unremarkable).
        int count = 0;
        for (int i = 0; i < n; i++) {
            if (excluded[i] == false) {
                count++;
            }
        }
        double[] background = new double[count];
        int b = 0;
        for (int i = 0; i < n; i++) {
            if (excluded[i] == false) {
                background[b++] = values[i];
            }
        }
        return background;
    }

    private record Excursion(int start, int end, int sign, int peak, double peakZ) {}
}
