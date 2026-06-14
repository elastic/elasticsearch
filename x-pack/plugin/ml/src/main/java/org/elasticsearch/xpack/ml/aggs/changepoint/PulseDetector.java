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
import java.util.Comparator;
import java.util.List;

public class PulseDetector {

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
     * 2. merge candidates within {@link #PULSE_MERGE_MAX_GAP} of one another and have the same sign into single
     *    excursions, dropping any that span a full {@code minSegmentLength},
     * 3. sort the excursions by their peak z and keep the top {@code max(MAX_PULSES_FLOOR, MAX_PULSES_FRACTION * n)}.
     * 4. build ONE Gaussian-KDE null from the residuals with <em>all</em> of those top excursions removed,
     *    and keep an excursion only if its peak's Bonferroni-corrected tail probability under that null clears
     *    the threshold.
     *
     * Removing all the tested excursions from the single null at once is deliberate: scoring each against a null
     * that contains the others (leave-one-out) means the largest spike and dip mask all the rest. Removing them
     * together means several genuinely distinct excursions are each judged against the quiet remainder and all
     * survive, while a recurring population is still rejected.
     */
    public List<ChangeType> detect(double[] values) {
        int n = values.length;
        if (n < 4) {
            return new ArrayList<>();
        }

        // Residual signal and its robust scale. The scale is the larger of the global first-difference noise
        // (which stays meaningful on smooth/monotone data where residuals are mostly exactly zero) and the
        // composite scale of the residuals (which inflates once a heteroscedastic high-variance regime appears,
        // so that regime's ordinary fluctuations are not each treated as candidates).
        double[] residuals = Stats.rollingMedianResiduals(values, WEIGHT_HALF_WINDOW);
        double maxAbs = 0.0;
        for (double v : values) {
            maxAbs = Math.max(maxAbs, Math.abs(v));
        }
        double scale = Math.max(Stats.compositeScale(residuals, maxAbs), Math.sqrt(Stats.globalNoiseVariance(values)));
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

        // Keep the top excursions by peak z (the most extreme); the gate then decides which are significant.
        int limit = Math.max(MAX_PULSES_FLOOR, (int) Math.ceil(MAX_PULSES_FRACTION * n));
        if (excursions.size() > limit) {
            excursions.sort(Comparator.comparingDouble((Excursion e) -> e.peakZ()).reversed());
            excursions = new ArrayList<>(excursions.subList(0, limit));
        }

        // Gate each excursion against a KDE null, all in an asinh-stabilised value space. Telemetry noise is
        // typically multiplicative (its spread grows with magnitude), so a single bandwidth on raw values is
        // far too narrow up in the high-magnitude tail and flags ordinary large values. asinh(x / scale) is
        // monotonic (so the gate still asks the value-based question "is this magnitude one we see at other
        // times?") but removes the magnitude dependence so one bandwidth is valid across orders of magnitude.
        //
        // Two distinct scales are used deliberately. The KDE NULL is the stabilised background values: it
        // carries the heavy-tail mass, so a magnitude that recurs elsewhere has neighbours and is not surprising. The
        // BANDWIDTH (the kernel's smoothing width) is taken from the stabilised RESIDUALS, not the stabilised
        // values: a level change makes the value distribution bimodal and would inflate a value-derived width,
        // making the gate suppress genuine within-regime spike after a step is missed. The residual removes
        // the step, so the width reflects within-regime noise and stays sensitive.
        double[] backgroundValues = backgroundExcluding(values, excursions, n);
        double stabilizingScale = Stats.asinhScale(backgroundValues);
        double[] stabilizedBackground = Stats.asinhStabilize(backgroundValues, stabilizingScale);
        double[] stabilizedResiduals = Stats.rollingMedianResiduals(Stats.asinhStabilize(values, stabilizingScale), WEIGHT_HALF_WINDOW);
        double bandwidth = Stats.kdeBandwidth(backgroundExcluding(stabilizedResiduals, excursions, n));
        List<ChangeType> pulses = new ArrayList<>();
        for (Excursion e : excursions) {
            double stabilizedValue = Stats.asinh(values[e.peak()] / stabilizingScale);
            double tail = Stats.kdeTailProbability(stabilizedValue, stabilizedBackground, bandwidth, e.sign());
            // Bonferroni over the n points scanned to pick the extremes.
            double pValue = Math.max(Double.MIN_VALUE, Math.min(1.0, tail * n));
            if (pValue < pValueThreshold) {
                pulses.add(e.sign() > 0 ? new ChangeType.Spike(pValue, e.peak()) : new ChangeType.Dip(pValue, e.peak()));
            }
        }
        pulses.sort(Comparator.comparingInt(ChangeType::changePoint));
        logger.trace("Pulse detection found [{}] significant pulses", pulses.size());
        return pulses;
    }

    /**
     * Builds the long list (residual z above threshold) and merges adjacent same-sign candidates within
     * {@link #PULSE_MERGE_MAX_GAP} into excursions. A run spanning a full minimum segment length is dropped
     * — that is a regime, owned by the structural/dispersion channels, not a point pulse.
     */
    private List<Excursion> mergeCandidates(double[] residuals, double scale, int n) {
        List<Excursion> excursions = new ArrayList<>();
        int i = 0;
        while (i < n) {
            if (Math.abs(residuals[i]) / scale <= PULSE_Z_THRESHOLD) {
                i++;
                continue;
            }
            int sign = residuals[i] >= 0.0 ? 1 : -1;
            int start = i;
            int last = i;
            int j = i + 1;
            while (j < n
                && j - last <= PULSE_MERGE_MAX_GAP
                && Math.abs(residuals[j]) / scale > PULSE_Z_THRESHOLD
                && (residuals[j] >= 0.0 ? 1 : -1) == sign) {
                last = j;
                j++;
            }
            if (last + 1 - start < minSegmentLength) {
                int peak = peakIndex(residuals, start, last + 1, sign);
                excursions.add(new Excursion(start, last + 1, sign, peak, Math.abs(residuals[peak]) / scale));
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
        int count = 0;
        for (boolean b : excluded) {
            if (b == false) {
                count++;
            }
        }
        double[] background = new double[count];
        int b = 0;
        // We exclude the first and last point since we also exclude them as candidates.
        for (int i = 1; i < n - 1; i++) {
            if (excluded[i] == false) {
                background[b++] = values[i];
            }
        }
        return background;
    }

    /** Index of the most extreme residual (in the excursion's direction) over {@code [start, end)}. */
    private static int peakIndex(double[] residuals, int start, int end, int sign) {
        int peak = start;
        double best = Double.NEGATIVE_INFINITY;
        for (int i = start; i < end; i++) {
            double oriented = sign > 0 ? residuals[i] : -residuals[i];
            if (oriented > best) {
                best = oriented;
                peak = i;
            }
        }
        return peak;
    }

    // Half-width of the centred window used for the rolling-median baseline that residuals are taken from.
    private static final int WEIGHT_HALF_WINDOW = 5;
    // Candidate ("long list") threshold: a point is a candidate excursion when its residual from the local
    // rolling-median baseline exceeds this many robust sigmas of the residual scale. Generous pre-filter; the
    // significance decision is the KDE gate.
    private static final double PULSE_Z_THRESHOLD = 5.0;
    // Candidates separated by at most this many buckets (and of the same sign) are one physical excursion and are
    // merged. We do NOT chain across larger gaps: repeated/recurring excursions are a structural/dispersion matter,
    // not something the pulse stream should fuse. Set to 1 (strictly adjacent).
    private static final int PULSE_MERGE_MAX_GAP = 1;
    // We report at most this many pulses — the highest-z excursions. A small floor plus a slowly-growing fraction
    // of the series length, so a pathological or very noisy series cannot drown the output in spikes.
    private static final int MAX_PULSES_FLOOR = 5;
    private static final double MAX_PULSES_FRACTION = 0.02;

    private final int minSegmentLength;
    private final double pValueThreshold;

    private record Excursion(int start, int end, int sign, int peak, double peakZ) {}
}
