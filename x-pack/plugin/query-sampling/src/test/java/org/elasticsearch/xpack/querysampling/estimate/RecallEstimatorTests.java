/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.estimate;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.querysampling.action.QuerySamplingRecallResponse;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.Hardness;
import org.elasticsearch.xpack.querysampling.dedup.Selectivity;
import org.elasticsearch.xpack.querysampling.dedup.Stratum;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;
import org.elasticsearch.xpack.querysampling.storage.StoredSample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;
import java.util.Random;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class RecallEstimatorTests extends ESTestCase {

    public void testRecallIsTheShareOfTheTrueNeighboursThatWereReturned() {
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

        assertThat(RecallEstimator.recall(stored(search, truth, 1, 1, 1)), equalTo(OptionalDouble.of(0.5)));
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

    public void testEffectiveSizeFallsWhenOneQueryCarriesTheWeight() {
        List<StoredSample> samples = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            samples.add(sample(2, List.of("a", "b"), List.of("a", "b"), 1, 1, 1));
        }
        samples.add(sample(2, List.of("a", "b"), List.of("a", "b"), 1000, 1, 1));

        RecallEstimate estimate = RecallEstimator.estimate(samples);

        assertThat(estimate.uniqueQueryEffectiveSize(), closeTo(10, 1e-9));
        assertThat(estimate.trafficEffectiveSize(), closeTo(1009.0 * 1009.0 / (1000.0 * 1000.0 + 9), 1e-9));
    }

    public void testQueriesThatCannotBeUsedAreLeftOutAndCounted() {
        List<StoredSample> samples = List.of(
            sample(2, List.of("a", "b"), List.of("a", "b"), 1, 1, 1),
            stored(new CapturedSearch(query(2), List.of(), 1, 1.0), null, 1, 1, 1),
            sample(2, List.of("a", "b"), List.of("a", "b"), 1, 0, 1)
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
            inStratum(sample(2, List.of("a", "b"), List.of("a", "b"), 90, 1, 1), near, Hardness.EASY),
            inStratum(sample(2, List.of("a", "b"), List.of("a", "b"), 10, 1, 1), near, Hardness.EASY),
            inStratum(sample(2, List.of("x", "y"), List.of("a", "b"), 30, 1, 1), far, Hardness.HARD),
            inStratum(sample(2, List.of("a", "y"), List.of("a", "b"), 10, 1, 1), far, Hardness.HARD)
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

    private static final String NO_EVENTS_JSON =
        "\"event_slice\":{\"records\":0,\"records_with_ground_truth\":0,\"recall\":null,\"effective_size\":0.0}";

    private static final RecallEstimate.EventEstimate NO_EVENTS = new RecallEstimate.EventEstimate(0, 0, null, 0);

    public void testResponseRendersTheEstimate() throws IOException {
        QuerySamplingRecallResponse response = new QuerySamplingRecallResponse(
            new RecallEstimate(5, 4, 0.9, 3.5, 0.8, 4.0, List.of(), List.of(), List.of(), NO_EVENTS),
            null
        );

        assertThat(
            render(response),
            equalTo(
                "{\"records\":5,\"records_with_ground_truth\":4,\"traffic_weighted_recall\":0.9,\"traffic_effective_size\":3.5,"
                    + "\"unique_query_recall\":0.8,\"unique_query_effective_size\":4.0,\"by_hardness\":[],\"by_cluster\":[],"
                    + "\"by_selectivity\":[],"
                    + NO_EVENTS_JSON
                    + "}"
            )
        );
    }

    private static String render(QuerySamplingRecallResponse response) throws IOException {
        XContentBuilder builder = response.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
        return Strings.toString(builder);
    }
}
