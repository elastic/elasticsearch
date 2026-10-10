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
 * Keeps the sample from being made of the easy queries only: queries that the index answers well say little about
 * where it goes wrong, and are the most of the traffic.
 * <p>
 * The hardness of a query is taken from the scores of the hits that were returned for it, which are what is at hand
 * without another search. It is the average of the scores over the best one, which is 1 when the hits are all as good
 * as the first and gets smaller as the first is better than the rest. It is a measure of how well the nearest
 * neighbours stand out from the others, like local intrinsic dimensionality, which is worked out from distances, but
 * <b>it is not that</b>: a score is a function of a distance that depends on the similarity of the field, and
 * the scale of this measure depends on it too. For that the queries are compared with those of the same field and
 * dimensions only: they are put in the easiest, the middle or the hardest third by how many standard deviations the
 * measure is from the average of the queries seen before.
 * <p>
 * Then the probability of picking a query is multiplied with a factor, that is above one for the hard queries and
 * below one for the easy ones by an amount that the tilt sets, and such that a third of each gives an average of one.
 * Which measure of hardness is used does not matter for the estimates to be right: the factor is recorded in the
 * probability of the draw, like the others. It matters for how useful the sample is.
 */
public final class HardnessStrata {

    /**
     * The queries that have to be seen before they are told apart, as the average and the deviation mean little before.
     */
    static final int MIN_QUERIES = 30;

    /**
     * The hits needed for a measure, fewer say nothing of how well the first stand out.
     */
    static final int MIN_HITS = 3;

    /**
     * The distance from the average, in standard deviations, of the limits between the thirds of a normal distribution.
     */
    static final double TERCILE = 0.4307;

    /**
     * The most spaces that are kept track of.
     */
    static final int MAX_SPACES = 100;

    private final Map<String, Moments> spaces = new HashMap<>();
    private volatile double tilt;

    /**
     * Follows the tilt setting, now and when it changes.
     */
    public void watch(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(QuerySamplingSettings.HARDNESS_TILT, value -> this.tilt = value);
    }

    /**
     * How well the first of the hits stand out from the rest, which is higher for the harder queries.
     *
     * @return the average score over the best, between 0 and 1, or NaN if there are not enough hits or the scores
     *         are not positive and finite numbers
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
     * Tells which third a query that has not been seen before is in, and takes it in.
     *
     * @return the hardness, medium if there are not enough queries to tell yet, or null if there is nothing to tell it by
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
     * What the probability of picking a query of the hardness is multiplied with. Medium queries have
     * {@code 3 / (e^-η + 1 + e^η)}, and the easy and the hard ones that, times {@code e^-η} and {@code e^η}.
     */
    public double factor(@Nullable Hardness hardness) {
        double tilt = this.tilt;
        if (hardness == null || tilt == 0.0) {
            return 1.0;
        }
        return 3.0 * Math.exp(tilt * hardness.rank()) / (Math.exp(-tilt) + 1.0 + Math.exp(tilt));
    }

    /**
     * Average and deviation of the measure, as the queries come in.
     */
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
