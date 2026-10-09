/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.estimate;

import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.Hardness;
import org.elasticsearch.xpack.querysampling.dedup.Stratum;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;
import org.elasticsearch.xpack.querysampling.storage.StoredSample;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.TreeMap;

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
 * <p>
 * The same is done for the queries of each hardness and of each cluster of the vector space. Their weights are the
 * ones they have among all the queries, so each is an estimate for a part of the population, and the parts show what
 * the average of all of them hides.
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

    /**
     * @param samples the sampled queries: those that have no ground truth, or a probability of zero to be there, are
     *                left out
     */
    public static RecallEstimate estimate(List<StoredSample> samples) {
        Estimates all = new Estimates();
        Map<Hardness, Estimates> byHardness = new EnumMap<>(Hardness.class);
        Map<Stratum, Estimates> byCluster = new TreeMap<>(Comparator.comparing(Stratum::space).thenComparingInt(Stratum::cluster));
        int used = 0;
        for (StoredSample sample : samples) {
            OptionalDouble recall = recall(sample);
            TrackedQuery.Weights weights = sample.weights();
            if (recall.isEmpty() || weights.inclusionProbability() <= 0 || weights.seenProbability() <= 0) {
                continue;
            }
            used++;
            double trafficWeight = weights.weightedMultiplicity() / weights.inclusionProbability();
            double uniqueWeight = 1.0 / (weights.inclusionProbability() * weights.seenProbability());
            all.add(trafficWeight, uniqueWeight, recall.getAsDouble());
            // each stratum is a part of the population, and the weights of its queries are what they are for all the queries:
            // the same ratio of weighted sums, over those of the stratum only
            if (sample.hardness() != null) {
                byHardness.computeIfAbsent(sample.hardness(), key -> new Estimates())
                    .add(trafficWeight, uniqueWeight, recall.getAsDouble());
            }
            if (sample.stratum() != null) {
                byCluster.computeIfAbsent(sample.stratum(), key -> new Estimates()).add(trafficWeight, uniqueWeight, recall.getAsDouble());
            }
        }
        List<RecallEstimate.GroupEstimate> hardnesses = new ArrayList<>();
        byHardness.forEach((hardness, estimates) -> hardnesses.add(estimates.group(hardness.name().toLowerCase(Locale.ROOT))));
        List<RecallEstimate.GroupEstimate> clusters = new ArrayList<>();
        byCluster.forEach((stratum, estimates) -> clusters.add(estimates.group(stratum.space() + "#" + stratum.cluster())));
        return new RecallEstimate(
            samples.size(),
            used,
            all.traffic.mean(),
            all.traffic.effectiveSize(),
            all.unique.mean(),
            all.unique.effectiveSize(),
            hardnesses,
            clusters
        );
    }

    /**
     * Both estimates, of the traffic and of the distinct queries, over a set of queries.
     */
    private static final class Estimates {
        private final Accumulator traffic = new Accumulator();
        private final Accumulator unique = new Accumulator();
        private int records;

        void add(double trafficWeight, double uniqueWeight, double recall) {
            records++;
            traffic.add(trafficWeight, recall);
            unique.add(uniqueWeight, recall);
        }

        RecallEstimate.GroupEstimate group(String key) {
            return new RecallEstimate.GroupEstimate(
                key,
                records,
                traffic.mean(),
                traffic.effectiveSize(),
                unique.mean(),
                unique.effectiveSize()
            );
        }
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
