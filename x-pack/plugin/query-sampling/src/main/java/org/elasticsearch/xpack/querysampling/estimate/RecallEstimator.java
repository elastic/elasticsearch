/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.estimate;

import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;
import org.elasticsearch.xpack.querysampling.storage.StoredSample;

import java.util.HashSet;
import java.util.List;
import java.util.OptionalDouble;
import java.util.Set;

/**
 * Estimates the recall of the search from the sampled queries whose ground truth is known.
 * <p>
 * The sample is not uniform: a query was more likely to be picked the less it was searched, and it may not even
 * have been captured. A plain average of the recalls would therefore describe the sample and not the traffic, so
 * every query is weighted by the inverse of its chance to be in the sample, as in a Horvitz–Thompson estimate.
 * Two things can be estimated:
 * <ul>
 * <li>the recall over the traffic. A query counts as often as it was searched, which is the estimated number of
 * its arrivals, and the chance to have been picked is {@code π}. A query that was not captured at all has no
 * arrivals to count, so there is nothing more to correct for;</li>
 * <li>the recall over distinct queries, each counting once. A distinct query also has to have been captured at all
 * to be known, which has the chance {@code s}, so its weight is {@code 1 / (π · s)}.</li>
 * </ul>
 * Both are ratios of weighted sums, which makes them insensitive to how many queries there happen to be in the
 * sample. For each the effective sample size of Kish is given, {@code (Σw)² / Σw²}: how many equally weighted
 * queries the estimate is worth.
 */
public final class RecallEstimator {

    private RecallEstimator() {}

    /**
     * Recall of one query: the share of the true nearest neighbours that the live search returned. A document is
     * identified by its index and its id. Only the first {@code k} hits count, which is what was asked for.
     *
     * @return the recall, or nothing if the ground truth is not known or empty
     */
    public static OptionalDouble recall(StoredSample sample) {
        GroundTruth groundTruth = sample.groundTruth();
        if (groundTruth == null || groundTruth.neighbors().isEmpty()) {
            return OptionalDouble.empty();
        }
        Set<DocumentKey> truth = new HashSet<>();
        for (CapturedSearch.Hit hit : groundTruth.neighbors()) {
            truth.add(new DocumentKey(hit.index(), hit.id()));
        }
        List<CapturedSearch.Hit> live = sample.search().hits();
        int considered = Math.min(live.size(), sample.search().query().k());
        int found = 0;
        for (int i = 0; i < considered; i++) {
            found += truth.contains(new DocumentKey(live.get(i).index(), live.get(i).id())) ? 1 : 0;
        }
        return OptionalDouble.of((double) found / truth.size());
    }

    public static RecallEstimate estimate(List<StoredSample> samples) {
        Accumulator traffic = new Accumulator();
        Accumulator unique = new Accumulator();
        int used = 0;
        for (StoredSample sample : samples) {
            OptionalDouble recall = recall(sample);
            TrackedQuery.Weights weights = sample.weights();
            if (recall.isEmpty() || weights.inclusionProbability() <= 0 || weights.seenProbability() <= 0) {
                continue;
            }
            used++;
            traffic.add(weights.weightedMultiplicity() / weights.inclusionProbability(), recall.getAsDouble());
            unique.add(1.0 / (weights.inclusionProbability() * weights.seenProbability()), recall.getAsDouble());
        }
        return new RecallEstimate(samples.size(), used, traffic.mean(), traffic.effectiveSize(), unique.mean(), unique.effectiveSize());
    }

    private record DocumentKey(String index, String id) {}

    /**
     * Weighted sums, from which both the mean and the effective sample size follow.
     */
    private static final class Accumulator {
        private double weights;
        private double squaredWeights;
        private double weightedRecalls;

        void add(double weight, double recall) {
            weights += weight;
            squaredWeights += weight * weight;
            weightedRecalls += weight * recall;
        }

        Double mean() {
            return weights > 0 ? weightedRecalls / weights : null;
        }

        double effectiveSize() {
            return squaredWeights > 0 ? weights * weights / squaredWeights : 0;
        }
    }
}
