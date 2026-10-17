/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.estimate;

import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.Hardness;
import org.elasticsearch.xpack.querysampling.dedup.Selectivity;
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
import java.util.function.Function;

/**
 * Estimates recall from sampled queries with known ground truth. The sample is not uniform, so each query is weighted by the
 * inverse of its chance to be in it (Horvitz-Thompson): for the traffic, searches times {@code 1 / π}; for distinct queries,
 * {@code 1 / (π · s)}, where {@code s} is the chance of being captured at all. Kish's effective sample size
 * {@code (Σw)² / Σw²} says how much to trust each. Events are a sample of searches and make their own estimate with
 * weights of {@code 1 / π}. Strata use the weights they have among all queries.
 */
public final class RecallEstimator {

    private RecallEstimator() {}

    /**
     * Only the first {@code k} live hits count. Empty if the ground truth is not known or empty.
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
     * Samples without ground truth, or with a zero probability of being in the sample, are left out.
     */
    public static RecallEstimate estimate(List<StoredSample> samples) {
        Estimates all = new Estimates();
        Accumulator events = new Accumulator();
        int eventRecords = 0;
        int eventsUsed = 0;
        int queryRecords = 0;
        Map<Hardness, Estimates> byHardness = new EnumMap<>(Hardness.class);
        Map<Selectivity, Estimates> bySelectivity = new EnumMap<>(Selectivity.class);
        Map<Stratum, Estimates> byCluster = new TreeMap<>(Comparator.comparing(Stratum::space).thenComparingInt(Stratum::cluster));
        int used = 0;
        for (StoredSample sample : samples) {
            OptionalDouble recall = recall(sample);
            TrackedQuery.Weights weights = sample.weights();
            if (sample.isEvent()) {
                eventRecords++;
                if (recall.isPresent() && weights.inclusionProbability() > 0) {
                    eventsUsed++;
                    events.add(1.0 / weights.inclusionProbability(), recall.getAsDouble());
                }
                continue;
            }
            queryRecords++;
            if (recall.isEmpty() || weights.inclusionProbability() <= 0 || weights.seenProbability() <= 0) {
                continue;
            }
            used++;
            double trafficWeight = weights.weightedMultiplicity() / weights.inclusionProbability();
            double uniqueWeight = 1.0 / (weights.inclusionProbability() * weights.seenProbability());
            all.add(trafficWeight, uniqueWeight, recall.getAsDouble());
            // strata keep the weights the queries have among all queries
            addTo(byHardness, sample.hardness(), trafficWeight, uniqueWeight, recall.getAsDouble());
            addTo(bySelectivity, sample.selectivity(), trafficWeight, uniqueWeight, recall.getAsDouble());
            addTo(byCluster, sample.stratum(), trafficWeight, uniqueWeight, recall.getAsDouble());
        }
        List<RecallEstimate.GroupEstimate> hardnesses = groups(byHardness, hardness -> hardness.name().toLowerCase(Locale.ROOT));
        List<RecallEstimate.GroupEstimate> selectivities = groups(bySelectivity, s -> s.name().toLowerCase(Locale.ROOT));
        List<RecallEstimate.GroupEstimate> clusters = groups(byCluster, stratum -> stratum.space() + "#" + stratum.cluster());
        return new RecallEstimate(
            queryRecords,
            used,
            all.traffic.mean(),
            all.traffic.effectiveSize(),
            all.unique.mean(),
            all.unique.effectiveSize(),
            hardnesses,
            clusters,
            selectivities,
            new RecallEstimate.EventEstimate(eventRecords, eventsUsed, events.mean(), events.effectiveSize())
        );
    }

    /** Queries not assigned to a group of this kind (null key) are left out of it. */
    private static <K> void addTo(Map<K, Estimates> groups, K key, double trafficWeight, double uniqueWeight, double recall) {
        if (key != null) {
            groups.computeIfAbsent(key, k -> new Estimates()).add(trafficWeight, uniqueWeight, recall);
        }
    }

    private static <K> List<RecallEstimate.GroupEstimate> groups(Map<K, Estimates> groups, Function<K, String> label) {
        List<RecallEstimate.GroupEstimate> estimates = new ArrayList<>();
        groups.forEach((key, group) -> estimates.add(group.group(label.apply(key))));
        return estimates;
    }

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
