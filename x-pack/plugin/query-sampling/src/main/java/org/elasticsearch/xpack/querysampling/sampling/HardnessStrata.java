/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.Hardness;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tilts picks towards hard queries so the sample is not mostly easy ones. Hardness is the mean hit score over the best
 * score (not local intrinsic dimensionality: scores depend on the field similarity), so queries are put in terciles
 * by z-score against earlier queries of the same field and dimensions. The tilt factor is recorded in the draw
 * probability, so estimates stay unbiased whatever the measure.
 */
public final class HardnessStrata {

    /** Queries to see before terciles are assigned, as the mean and deviation mean little earlier. */
    static final int MIN_QUERIES = 30;

    static final int MIN_HITS = 3;

    /** Z-score of the limits between the thirds of a normal distribution. */
    static final double TERCILE = 0.4307;

    static final int MAX_SPACES = 100;

    private final Map<String, Moments> spaces = new HashMap<>();
    private volatile double tilt;

    public void watch(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(QuerySamplingSettings.HARDNESS_TILT, value -> this.tilt = value);
    }

    /**
     * @return the mean score over the best, in (0, 1], or NaN if there are too few hits or a score is not positive and finite
     */
    static double contrast(List<CapturedSearch.Hit> hits) {
        if (hits.size() < MIN_HITS) {
            return Double.NaN;
        }
        double best = 0;
        double sum = 0;
        for (CapturedSearch.Hit hit : hits) {
            double score = hit.score();
            if (Double.isFinite(score) == false || score <= 0) {
                return Double.NaN;
            }
            best = Math.max(best, score);
            sum += score;
        }
        return sum / (hits.size() * best);
    }

    /**
     * @return the hardness, medium until enough queries were seen, or null if it cannot be told
     */
    @Nullable
    public Hardness assign(String field, int dims, List<CapturedSearch.Hit> hits) {
        double contrast = contrast(hits);
        if (Double.isNaN(contrast)) {
            return null;
        }
        String name = field + "/" + dims;
        synchronized (spaces) {
            Moments moments = spaces.get(name);
            if (moments == null) {
                if (spaces.size() >= MAX_SPACES) {
                    return null;
                }
                moments = new Moments();
                spaces.put(name, moments);
            }
            Hardness hardness = moments.classify(contrast);
            moments.add(contrast);
            return hardness;
        }
    }

    /**
     * Medium queries get {@code 3 / (e^-η + 1 + e^η)}, easy and hard ones that times {@code e^-η} and {@code e^η}.
     */
    public double factor(@Nullable Hardness hardness) {
        double tilt = this.tilt;
        if (hardness == null || tilt == 0.0) {
            return 1.0;
        }
        return 3.0 * Math.exp(tilt * hardness.rank()) / (Math.exp(-tilt) + 1.0 + Math.exp(tilt));
    }

    private static final class Moments {
        private long count;
        private double mean;
        private double sumOfSquares;

        Hardness classify(double value) {
            if (count < MIN_QUERIES) {
                return Hardness.MEDIUM;
            }
            double deviation = Math.sqrt(sumOfSquares / (count - 1));
            if (deviation == 0.0) {
                return Hardness.MEDIUM;
            }
            double z = (value - mean) / deviation;
            if (z > TERCILE) {
                return Hardness.HARD;
            }
            if (z < -TERCILE) {
                return Hardness.EASY;
            }
            return Hardness.MEDIUM;
        }

        void add(double value) {
            count++;
            double delta = value - mean;
            mean += delta / count;
            sumOfSquares += delta * (value - mean);
        }
    }
}
