/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.estimate;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;
import org.elasticsearch.xpack.querysampling.storage.StoredSample;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class RecallEstimatorTests extends ESTestCase {

    public void testPopularQueriesCountMoreInTheTrafficEstimateButNotInTheUniqueOne() {
        List<StoredSample> samples = List.of(
            sample(2, List.of("a", "b"), List.of("a", "b"), 90, 1, 1),
            sample(2, List.of("x", "y"), List.of("a", "b"), 10, 1, 1)
        );

        RecallEstimate estimate = RecallEstimator.estimate(samples);

        assertThat(estimate.trafficWeightedRecall(), closeTo(0.9, 1e-12));
        assertThat(estimate.uniqueQueryRecall(), closeTo(0.5, 1e-12));
    }

    public void testQueriesThatWereLikelyToBePickedCountLess() {
        List<StoredSample> samples = List.of(
            sample(2, List.of("a", "b"), List.of("a", "b"), 1, 1.0, 1),
            sample(2, List.of("x", "y"), List.of("a", "b"), 1, 0.25, 1)
        );

        RecallEstimate estimate = RecallEstimator.estimate(samples);

        assertThat(estimate.trafficWeightedRecall(), closeTo(1.0 / 5, 1e-12));
        assertThat(estimate.uniqueQueryRecall(), closeTo(1.0 / 5, 1e-12));
    }

    public void testOnlyDistinctQueriesAreCorrectedForNotHavingBeenSeen() {
        List<StoredSample> samples = List.of(
            sample(2, List.of("a", "b"), List.of("a", "b"), 1, 1, 1),
            sample(2, List.of("x", "y"), List.of("a", "b"), 1, 1, 0.5)
        );

        RecallEstimate estimate = RecallEstimator.estimate(samples);

        assertThat("the traffic of a query is its arrivals, nothing is missing", estimate.trafficWeightedRecall(), closeTo(0.5, 1e-12));
        assertThat(estimate.uniqueQueryRecall(), closeTo(1.0 / 3, 1e-12));
    }

    public void testEventsMakeAnEstimateOfTheirOwnAndAreNotPartOfTheOthers() {
        List<StoredSample> samples = List.of(
            sample(2, List.of("a", "b"), List.of("a", "b"), 1, 1, 1),
            asEvent(sample(2, List.of("a", "b"), List.of("a", "b"), 1, 1, 1), "e1", 0.5),
            asEvent(sample(2, List.of("x", "y"), List.of("a", "b"), 1, 1, 1), "e2", 0.5),
            asEvent(sample(2, List.of("a", "y"), List.of("a", "b"), 1, 1, 1), "e3", 0.5),
            asEvent(sample(2, List.of("x", "y"), List.of("a", "b"), 1, 1, 1), "e4", 0.1)
        );

        RecallEstimate estimate = RecallEstimator.estimate(samples);

        assertThat("only the picked query", estimate.records(), equalTo(1));
        assertThat(estimate.trafficWeightedRecall(), closeTo(1.0, 1e-12));
        assertThat(estimate.events().records(), equalTo(4));
        assertThat(estimate.events().recordsWithGroundTruth(), equalTo(4));
        assertThat(estimate.events().recall(), closeTo(3.0 / 16, 1e-12));
        assertThat(estimate.events().effectiveSize(), closeTo(16.0 * 16.0 / (4 + 4 + 4 + 100), 1e-12));
    }

    public void testEstimatesAreCloseToTheTruthWhenQueriesAreSampledUnevenly() {
        Random random = new Random(randomLong());
        int queries = 3000;
        int[] traffic = new int[queries];
        double[] recall = new double[queries];
        double[] picked = new double[queries];
        double[] seen = new double[queries];
        double trafficTotal = 0;
        double trafficRecall = 0;
        double uniqueRecall = 0;
        for (int j = 0; j < queries; j++) {
            traffic[j] = 1 + (int) (1000.0 / (1 + j));
            recall[j] = Math.min(1.0, (0.5 + random.nextDouble() * 0.5) * (1 - 0.3 / (1 + 0.01 * traffic[j])));
            recall[j] = Math.round(recall[j] * 10) / 10.0;
            picked[j] = Math.min(1.0, 0.8 * Math.log1p(traffic[j]) / Math.log1p(1000.0) + 0.05);
            seen[j] = 1 - Math.pow(0.9, traffic[j]);
            trafficTotal += traffic[j];
            trafficRecall += traffic[j] * recall[j];
            uniqueRecall += recall[j];
        }
        double expectedTraffic = trafficRecall / trafficTotal;
        double expectedUnique = uniqueRecall / queries;

        int trials = 100;
        double meanTraffic = 0;
        double meanUnique = 0;
        for (int trial = 0; trial < trials; trial++) {
            List<StoredSample> sampled = new ArrayList<>();
            for (int j = 0; j < queries; j++) {
                if (random.nextDouble() < picked[j] * seen[j]) {
                    int found = (int) Math.round(recall[j] * 10);
                    sampled.add(sampleWithRecall(found, traffic[j] / seen[j], picked[j], seen[j]));
                }
            }
            RecallEstimate estimate = RecallEstimator.estimate(sampled);
            meanTraffic += estimate.trafficWeightedRecall() / trials;
            meanUnique += estimate.uniqueQueryRecall() / trials;
        }

        assertThat(meanTraffic, closeTo(expectedTraffic, 0.01));
        assertThat(meanUnique, closeTo(expectedUnique, 0.01));
    }

    private static StoredSample sampleWithRecall(int found, double multiplicity, double picked, double seen) {
        List<String> truth = new ArrayList<>();
        List<String> live = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            truth.add("t" + i);
            live.add(i < found ? "t" + i : "other" + i);
        }
        return sample(10, live, truth, multiplicity, picked, seen);
    }

    private static StoredSample sample(
        int k,
        List<String> live,
        List<String> truth,
        double multiplicity,
        double inclusionProbability,
        double seenProbability
    ) {
        CapturedSearch search = new CapturedSearch(query(k), hits(live), 1, 1.0);
        return stored(search, new GroundTruth(hits(truth)), multiplicity, inclusionProbability, seenProbability);
    }

    private static StoredSample stored(
        CapturedSearch search,
        GroundTruth groundTruth,
        double multiplicity,
        double inclusionProbability,
        double seenProbability
    ) {
        TrackedQuery.Weights weights = new TrackedQuery.Weights(1, multiplicity, inclusionProbability, seenProbability, 1.0);
        return new StoredSample("sampler", "fingerprint", search, weights, 0, 0, groundTruth, null, null, null, null);
    }

    private static StoredSample asEvent(StoredSample sample, String eventId, double inclusionProbability) {
        TrackedQuery.Weights weights = new TrackedQuery.Weights(1, 1.0 / inclusionProbability, inclusionProbability, 1.0, 1.0);
        return new StoredSample(
            sample.samplerId(),
            sample.fingerprint(),
            sample.search(),
            weights,
            sample.pickedAt(),
            sample.updatedAt(),
            sample.groundTruth(),
            null,
            null,
            eventId,
            null
        );
    }

    private static List<CapturedSearch.Hit> hits(List<String> ids) {
        return ids.stream().map(id -> new CapturedSearch.Hit("idx", id, 1f)).toList();
    }

    private static CapturedQuery query(int k) {
        return new CapturedQuery(new String[] { "idx" }, "vec", new float[] { 1f }, k, 10, null, null, List.of(), null);
    }
}
