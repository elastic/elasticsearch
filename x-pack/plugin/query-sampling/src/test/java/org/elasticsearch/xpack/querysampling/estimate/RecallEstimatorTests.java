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
import org.elasticsearch.xpack.querysampling.dedup.Hardness;
import org.elasticsearch.xpack.querysampling.dedup.Selectivity;
import org.elasticsearch.xpack.querysampling.dedup.Stratum;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;
import org.elasticsearch.xpack.querysampling.storage.StoredSample;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;
import java.util.Random;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RecallEstimatorTests extends ESTestCase {

    public void testRecallIsTheShareOfTheTrueNeighboursThatWereReturned() {
        // the true neighbours are d0..d3, the live search returned d0, d2, d9 and d7
        StoredSample sample = sample(4, List.of("d0", "d2", "d9", "d7"), List.of("d0", "d1", "d2", "d3"), 1, 1, 1);

        assertThat(RecallEstimator.recall(sample), equalTo(OptionalDouble.of(0.5)));
    }

    public void testOrderDoesNotMatterButTheIndexDoes() {
        CapturedQuery query = query(2);
        CapturedSearch search = new CapturedSearch(
            query,
            List.of(new CapturedSearch.Hit("b", "x", 1f), new CapturedSearch.Hit("a", "y", 1f)),
            1,
            1.0
        );
        GroundTruth truth = new GroundTruth(List.of(new CapturedSearch.Hit("a", "y", 1f), new CapturedSearch.Hit("a", "x", 1f)));

        // y is found, x exists in index a but the live search returned the x of index b
        assertThat(RecallEstimator.recall(stored(search, truth, 1, 1, 1)), equalTo(OptionalDouble.of(0.5)));
    }

    public void testOnlyTheFirstKHitsCount() {
        // k is 2, so d5 and d6 were not asked for even if they happen to be true neighbours
        StoredSample sample = sample(2, List.of("d0", "d9", "d5", "d6"), List.of("d0", "d5"), 1, 1, 1);

        assertThat(RecallEstimator.recall(sample), equalTo(OptionalDouble.of(0.5)));
    }

    public void testNoRecallWithoutGroundTruth() {
        CapturedSearch search = new CapturedSearch(query(2), List.of(), 1, 1.0);

        assertThat(RecallEstimator.recall(stored(search, null, 1, 1, 1)), equalTo(OptionalDouble.empty()));
        assertThat(RecallEstimator.recall(stored(search, new GroundTruth(List.of()), 1, 1, 1)), equalTo(OptionalDouble.empty()));
    }

    public void testEqualWeightsGiveThePlainAverage() {
        List<StoredSample> samples = List.of(
            sample(2, List.of("a", "b"), List.of("a", "b"), 1, 1, 1),
            sample(2, List.of("a", "x"), List.of("a", "b"), 1, 1, 1),
            sample(2, List.of("x", "y"), List.of("a", "b"), 1, 1, 1)
        );

        RecallEstimate estimate = RecallEstimator.estimate(samples);

        assertThat(estimate.trafficWeightedRecall(), closeTo(0.5, 1e-12));
        assertThat(estimate.uniqueQueryRecall(), closeTo(0.5, 1e-12));
        assertThat(estimate.trafficEffectiveSize(), closeTo(3, 1e-12));
        assertThat(estimate.uniqueQueryEffectiveSize(), closeTo(3, 1e-12));
        assertThat(estimate.recordsWithGroundTruth(), equalTo(3));
    }

    public void testPopularQueriesCountMoreInTheTrafficEstimateButNotInTheUniqueOne() {
        List<StoredSample> samples = List.of(
            sample(2, List.of("a", "b"), List.of("a", "b"), 90, 1, 1), // recall 1, searched 90 times
            sample(2, List.of("x", "y"), List.of("a", "b"), 10, 1, 1) // recall 0, searched 10 times
        );

        RecallEstimate estimate = RecallEstimator.estimate(samples);

        assertThat(estimate.trafficWeightedRecall(), closeTo(0.9, 1e-12));
        assertThat(estimate.uniqueQueryRecall(), closeTo(0.5, 1e-12));
    }

    public void testQueriesThatWereLikelyToBePickedCountLess() {
        List<StoredSample> samples = List.of(
            sample(2, List.of("a", "b"), List.of("a", "b"), 1, 1.0, 1), // picked for sure, recall 1
            sample(2, List.of("x", "y"), List.of("a", "b"), 1, 0.25, 1) // picked once in four, recall 0, so it stands for four
        );

        RecallEstimate estimate = RecallEstimator.estimate(samples);

        assertThat(estimate.trafficWeightedRecall(), closeTo(1.0 / 5, 1e-12));
        assertThat(estimate.uniqueQueryRecall(), closeTo(1.0 / 5, 1e-12));
    }

    public void testOnlyDistinctQueriesAreCorrectedForNotHavingBeenSeen() {
        List<StoredSample> samples = List.of(
            sample(2, List.of("a", "b"), List.of("a", "b"), 1, 1, 1), // seen for sure, recall 1
            sample(2, List.of("x", "y"), List.of("a", "b"), 1, 1, 0.5) // seen half of the time, recall 0
        );

        RecallEstimate estimate = RecallEstimator.estimate(samples);

        assertThat("the traffic of a query is its arrivals, nothing is missing", estimate.trafficWeightedRecall(), closeTo(0.5, 1e-12));
        assertThat(estimate.uniqueQueryRecall(), closeTo(1.0 / 3, 1e-12));
    }

    public void testEffectiveSizeFallsWhenOneQueryCarriesTheWeight() {
        List<StoredSample> samples = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            samples.add(sample(2, List.of("a", "b"), List.of("a", "b"), 1, 1, 1));
        }
        samples.add(sample(2, List.of("a", "b"), List.of("a", "b"), 1000, 1, 1));

        RecallEstimate estimate = RecallEstimator.estimate(samples);

        assertThat(estimate.uniqueQueryEffectiveSize(), closeTo(10, 1e-9));
        // (1000 + 9)^2 / (1000^2 + 9) is barely more than one
        assertThat(estimate.trafficEffectiveSize(), closeTo(1009.0 * 1009.0 / (1000.0 * 1000.0 + 9), 1e-9));
    }

    public void testQueriesThatCannotBeUsedAreLeftOutAndCounted() {
        List<StoredSample> samples = List.of(
            sample(2, List.of("a", "b"), List.of("a", "b"), 1, 1, 1),
            stored(new CapturedSearch(query(2), List.of(), 1, 1.0), null, 1, 1, 1), // no ground truth yet
            sample(2, List.of("a", "b"), List.of("a", "b"), 1, 0, 1) // cannot be weighted
        );

        RecallEstimate estimate = RecallEstimator.estimate(samples);

        assertThat(estimate.records(), equalTo(3));
        assertThat(estimate.recordsWithGroundTruth(), equalTo(1));
        assertThat(estimate.trafficWeightedRecall(), closeTo(1.0, 1e-12));
    }

    public void testEachStratumHasItsOwnEstimateAndItsOwnWeights() {
        Stratum near = new Stratum("vec/1", 0);
        Stratum far = new Stratum("vec/1", 1);
        List<StoredSample> samples = List.of(
            inStratum(sample(2, List.of("a", "b"), List.of("a", "b"), 90, 1, 1), near, Hardness.EASY), // recall 1, searched 90 times
            inStratum(sample(2, List.of("a", "b"), List.of("a", "b"), 10, 1, 1), near, Hardness.EASY), // recall 1, searched 10 times
            inStratum(sample(2, List.of("x", "y"), List.of("a", "b"), 30, 1, 1), far, Hardness.HARD), // recall 0, searched 30 times
            inStratum(sample(2, List.of("a", "y"), List.of("a", "b"), 10, 1, 1), far, Hardness.HARD) // recall 0.5, searched 10 times
        );

        RecallEstimate estimate = RecallEstimator.estimate(samples);

        assertThat(estimate.trafficWeightedRecall(), closeTo(105.0 / 140, 1e-12));
        assertThat(estimate.byHardness().stream().map(RecallEstimate.GroupEstimate::key).toList(), equalTo(List.of("easy", "hard")));
        RecallEstimate.GroupEstimate easy = estimate.byHardness().get(0);
        RecallEstimate.GroupEstimate hard = estimate.byHardness().get(1);
        assertThat(easy.trafficWeightedRecall(), closeTo(1.0, 1e-12));
        assertThat(easy.uniqueQueryRecall(), closeTo(1.0, 1e-12));
        assertThat(easy.recordsWithGroundTruth(), equalTo(2));
        assertThat(hard.trafficWeightedRecall(), closeTo(5.0 / 40, 1e-12));
        assertThat(hard.uniqueQueryRecall(), closeTo(0.25, 1e-12));
        assertThat(hard.trafficEffectiveSize(), closeTo(40.0 * 40.0 / (30.0 * 30.0 + 10.0 * 10.0), 1e-12));
        assertThat(estimate.byCluster().stream().map(RecallEstimate.GroupEstimate::key).toList(), equalTo(List.of("vec/1#0", "vec/1#1")));
        assertThat(estimate.byCluster().get(0).trafficWeightedRecall(), closeTo(1.0, 1e-12));
        assertThat(estimate.byCluster().get(1).trafficWeightedRecall(), closeTo(5.0 / 40, 1e-12));
    }

    public void testClustersAreInTheOrderOfTheirNumbersAndQueriesWithoutStrataAreInNoGroup() {
        List<StoredSample> samples = new ArrayList<>();
        for (int cluster : new int[] { 10, 2, 33, 1 }) {
            samples.add(inStratum(sample(2, List.of("a", "b"), List.of("a", "b"), 1, 1, 1), new Stratum("vec/1", cluster), null));
        }
        samples.add(sample(2, List.of("a", "b"), List.of("a", "b"), 1, 1, 1));

        RecallEstimate estimate = RecallEstimator.estimate(samples);

        assertThat(
            estimate.byCluster().stream().map(RecallEstimate.GroupEstimate::key).toList(),
            equalTo(List.of("vec/1#1", "vec/1#2", "vec/1#10", "vec/1#33"))
        );
        assertThat(estimate.byHardness(), equalTo(List.of()));
        assertThat(estimate.recordsWithGroundTruth(), equalTo(5));
    }

    public void testQueriesThatCannotBeUsedAreNotInAStratumEither() {
        Stratum stratum = new Stratum("vec/1", 0);
        List<StoredSample> samples = List.of(
            inStratum(sample(2, List.of("a", "b"), List.of("a", "b"), 1, 1, 1), stratum, Hardness.MEDIUM),
            inStratum(sample(2, List.of("a", "b"), List.of("a", "b"), 1, 0, 1), stratum, Hardness.MEDIUM) // cannot be weighted
        );

        RecallEstimate estimate = RecallEstimator.estimate(samples);

        assertThat(estimate.byCluster().get(0).recordsWithGroundTruth(), equalTo(1));
        assertThat(estimate.byHardness().get(0).recordsWithGroundTruth(), equalTo(1));
    }

    public void testEventsMakeAnEstimateOfTheirOwnAndAreNotPartOfTheOthers() {
        List<StoredSample> samples = List.of(
            sample(2, List.of("a", "b"), List.of("a", "b"), 1, 1, 1), // a picked query, recall 1
            asEvent(sample(2, List.of("a", "b"), List.of("a", "b"), 1, 1, 1), "e1", 0.5), // recall 1, kept with a chance of one half
            asEvent(sample(2, List.of("x", "y"), List.of("a", "b"), 1, 1, 1), "e2", 0.5), // recall 0
            asEvent(sample(2, List.of("a", "y"), List.of("a", "b"), 1, 1, 1), "e3", 0.5), // recall 0.5
            asEvent(sample(2, List.of("x", "y"), List.of("a", "b"), 1, 1, 1), "e4", 0.1) // recall 0, kept with a chance of a tenth
        );

        RecallEstimate estimate = RecallEstimator.estimate(samples);

        assertThat("only the picked query", estimate.records(), equalTo(1));
        assertThat(estimate.trafficWeightedRecall(), closeTo(1.0, 1e-12));
        assertThat(estimate.events().records(), equalTo(4));
        assertThat(estimate.events().recordsWithGroundTruth(), equalTo(4));
        // weights of 2, 2, 2 and 10: (1 * 2 + 0 * 2 + 0.5 * 2 + 0 * 10) / 16
        assertThat(estimate.events().recall(), closeTo(3.0 / 16, 1e-12));
        assertThat(estimate.events().effectiveSize(), closeTo(16.0 * 16.0 / (4 + 4 + 4 + 100), 1e-12));
    }

    public void testEventsThatCannotBeUsedAreLeftOutAndCounted() {
        List<StoredSample> samples = List.of(
            asEvent(sample(2, List.of("a", "b"), List.of("a", "b"), 1, 1, 1), "e1", 0.5),
            asEvent(stored(new CapturedSearch(query(2), List.of(), 1, 1.0), null, 1, 1, 1), "e2", 0.5) // no ground truth yet
        );

        RecallEstimate estimate = RecallEstimator.estimate(samples);

        assertThat(estimate.events().records(), equalTo(2));
        assertThat(estimate.events().recordsWithGroundTruth(), equalTo(1));
        assertThat(estimate.events().recall(), closeTo(1.0, 1e-12));
    }

    public void testEachSelectivityHasItsOwnEstimate() {
        List<StoredSample> samples = List.of(
            withSelectivity(sample(2, List.of("a", "b"), List.of("a", "b"), 90, 1, 1), Selectivity.UNFILTERED), // recall 1
            withSelectivity(sample(2, List.of("a", "b"), List.of("a", "b"), 10, 1, 1), Selectivity.UNFILTERED), // recall 1
            withSelectivity(sample(2, List.of("x", "y"), List.of("a", "b"), 30, 1, 1), Selectivity.LOW), // recall 0
            withSelectivity(sample(2, List.of("a", "y"), List.of("a", "b"), 10, 1, 1), Selectivity.LOW), // recall 0.5
            sample(2, List.of("a", "b"), List.of("a", "b"), 10, 1, 1) // not counted, in no group
        );

        RecallEstimate estimate = RecallEstimator.estimate(samples);

        assertThat(
            "in the order of the enum, and only those there are",
            estimate.bySelectivity().stream().map(RecallEstimate.GroupEstimate::key).toList(),
            equalTo(List.of("unfiltered", "low"))
        );
        assertThat(estimate.bySelectivity().get(0).trafficWeightedRecall(), closeTo(1.0, 1e-12));
        assertThat(estimate.bySelectivity().get(1).trafficWeightedRecall(), closeTo(5.0 / 40, 1e-12));
        assertThat(estimate.bySelectivity().get(1).uniqueQueryRecall(), closeTo(0.25, 1e-12));
        assertThat(estimate.bySelectivity().get(1).recordsWithGroundTruth(), equalTo(2));
        assertThat(estimate.recordsWithGroundTruth(), equalTo(5));
    }

    public void testNothingToEstimateFrom() {
        RecallEstimate estimate = RecallEstimator.estimate(List.of());

        assertThat(estimate.trafficWeightedRecall(), nullValue());
        assertThat(estimate.uniqueQueryRecall(), nullValue());
        assertThat(estimate.trafficEffectiveSize(), equalTo(0.0));
        assertThat(estimate.byHardness(), equalTo(List.of()));
        assertThat(estimate.byCluster(), equalTo(List.of()));
        assertThat(estimate.bySelectivity(), equalTo(List.of()));
        assertThat(estimate.events().recall(), nullValue());
        assertThat(estimate.events().records(), equalTo(0));
    }

    /**
     * The estimates have to be right for the traffic and not for the sample, so draw samples from a population whose
     * recall is known the way the sampler would: popular queries are picked less often than rare ones, and some are
     * not seen at all.
     */
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
            traffic[j] = 1 + (int) (1000.0 / (1 + j)); // a long tail: a few popular queries and many rare ones
            // popular queries are harder, so recall depends on popularity and a plain average would be off
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
                    // a query that was seen has a weighted multiplicity that is on average its traffic divided by the
                    // chance of having been seen: that is what makes the traffic estimate need no other correction
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

    /**
     * A sample of a query with k = 10 whose live search found {@code found} of the true neighbours.
     */
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

    private static StoredSample inStratum(StoredSample sample, Stratum stratum, Hardness hardness) {
        return new StoredSample(
            sample.samplerId(),
            sample.fingerprint(),
            sample.search(),
            sample.weights(),
            sample.pickedAt(),
            sample.updatedAt(),
            sample.groundTruth(),
            stratum,
            hardness,
            sample.eventId(),
            sample.selectivity()
        );
    }

    private static StoredSample withSelectivity(StoredSample sample, Selectivity selectivity) {
        return new StoredSample(
            sample.samplerId(),
            sample.fingerprint(),
            sample.search(),
            sample.weights(),
            sample.pickedAt(),
            sample.updatedAt(),
            sample.groundTruth(),
            sample.stratum(),
            sample.hardness(),
            sample.eventId(),
            selectivity
        );
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
