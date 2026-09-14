/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class KnnEvalResponseTests extends ESTestCase {

    private static KnnEvalResponse createRandomResponse() {
        // the value based metrics travel as one group: either everything about them is present or none of it is
        boolean valueMetrics = randomBoolean();
        // the bin width is present only when the histogram is binned, which depends on k
        boolean binned = randomBoolean();
        boolean histogram = randomBoolean();
        int numberOfCandidates = randomIntBetween(1, 4);
        List<KnnEvalResponse.KnnSettingsResult> results = new ArrayList<>(numberOfCandidates);
        for (int c = 0; c < numberOfCandidates; c++) {
            int numberOfQueries = randomIntBetween(0, 3);
            Map<String, KnnEvalResponse.QueryDetail> details = Maps.newMapWithExpectedSize(numberOfQueries);
            for (int q = 0; q < numberOfQueries; q++) {
                details.put("query_" + q, randomQueryDetail());
            }
            List<Double> recalls = new ArrayList<>();
            for (int q = 0; q < numberOfQueries; q++) {
                recalls.add(randomDoubleBetween(0.0, 1.0, true));
            }
            KnnEvalResponse.DoubleStats recallStats = KnnEvalResponse.DoubleStats.of(recalls);
            results.add(
                new KnnEvalResponse.KnnSettingsResult(
                    randomEffectiveKnobs(),
                    recallStats.mean(),
                    recallStats,
                    // the histogram is opt in, so both the present and absent shapes have to round trip
                    histogram ? KnnEvalResponse.RecallBucket.histogram(recalls, binned ? 23 : 5) : null,
                    histogram && binned ? KnnEvalResponse.RecallBucket.BIN_WIDTH : null,
                    valueMetrics ? recallStats.mean() : null,
                    valueMetrics ? recallStats : null,
                    null,
                    valueMetrics ? randomFidelity() : null,
                    // took_ms.count has to agree with recall_stats.count: both are one entry per successful query
                    KnnEvalResponse.LongStats.of(randomLongs(numberOfQueries)),
                    randomStats(),
                    details
                )
            );
        }
        Map<String, Exception> failures = Maps.newMapWithExpectedSize(1);
        if (randomBoolean()) {
            failures.put("failed_query", new ParsingException(1, 2, "no such field", null));
        }
        Map<String, KnnEvalResponse.BaselineDetail> baselineDetails = Maps.newMapWithExpectedSize(2);
        if (randomBoolean()) {
            baselineDetails.put("query_0", randomBaselineDetail());
        }
        return new KnnEvalResponse(
            randomEffectiveKnobs(),
            randomStats(),
            randomStats(),
            randomBoolean() ? KnnEvalResponse.FULL_PRECISION_SCAN : KnnEvalResponse.QUANTIZED_VISIT_PLUS_RESCORE,
            baselineDetails,
            randomBoolean() ? null : randomEnvironment(),
            valueMetrics ? randomDoubleBetween(0.0, 0.5, true) : null,
            results,
            failures
        );
    }

    private static KnnEvalResponse.LongStats randomStats() {
        return KnnEvalResponse.LongStats.of(randomLongs(randomIntBetween(0, 10)));
    }

    private static List<Long> randomLongs(int size) {
        List<Long> values = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            values.add((long) randomIntBetween(0, 500));
        }
        return values;
    }

    public void testLongStatsAreExactPercentiles() {
        // shuffled 1..20, so nearest rank puts p50 at the 10th value and p95 at the 19th
        List<Long> values = new ArrayList<>();
        for (long value = 1; value <= 20; value++) {
            values.add(value);
        }
        Collections.shuffle(values, random());
        KnnEvalResponse.LongStats stats = KnnEvalResponse.LongStats.of(values);
        assertEquals(10.5, stats.mean(), 1e-9);
        assertEquals(10L, stats.p50());
        assertEquals(19L, stats.p95());
        assertEquals(20L, stats.max());
        assertEquals(210L, stats.sum());
        assertEquals(20L, stats.count());
    }

    public void testLongStatsOfASingleValue() {
        KnnEvalResponse.LongStats stats = KnnEvalResponse.LongStats.of(List.of(7L));
        assertEquals(7.0, stats.mean(), 0.0);
        assertEquals(7L, stats.p50());
        assertEquals(7L, stats.p95());
        assertEquals(7L, stats.max());
        assertEquals(7L, stats.sum());
        assertEquals(1L, stats.count());
    }

    public void testLongStatsOfNothing() {
        assertEquals(KnnEvalResponse.LongStats.EMPTY, KnnEvalResponse.LongStats.of(List.of()));
        assertEquals(0L, KnnEvalResponse.LongStats.EMPTY.count());
    }

    /** Keeps the documented invariants: hits with a rank are the retrieved relevant ones, and {@code missed} accounts for the rest. */
    private static KnnEvalResponse.QueryDetail randomQueryDetail() {
        int relevant = randomIntBetween(0, 10);
        int relevantRetrieved = randomIntBetween(0, relevant);
        List<KnnEvalResponse.RankedHit> hits = new ArrayList<>();
        for (int i = 0; i < relevantRetrieved; i++) {
            hits.add(new KnnEvalResponse.RankedHit("baseline" + i, randomFloatBetween(0.0f, 10.0f, true), i));
        }
        // false positives: returned by the candidate but absent from the baseline, so their rank is null
        for (int i = 0; i < relevant - relevantRetrieved; i++) {
            hits.add(new KnnEvalResponse.RankedHit("extra" + i, randomFloatBetween(0.0f, 10.0f, true), null));
        }
        List<KnnEvalResponse.RankedHit> missed = new ArrayList<>();
        for (int i = relevantRetrieved; i < relevant; i++) {
            missed.add(new KnnEvalResponse.RankedHit("baseline" + i, randomFloatBetween(0.0f, 10.0f, true), i));
        }
        return new KnnEvalResponse.QueryDetail(
            relevant == 0 ? 0.0 : (double) relevantRetrieved / relevant,
            relevantRetrieved,
            relevant,
            hits,
            missed,
            // one entry per rank, null where the loss was unbounded
            Arrays.asList(0.0, randomBoolean() ? null : 0.25, null),
            randomBoolean(),
            relevant == 0 ? 0.0 : (double) relevantRetrieved / relevant,
            relevantRetrieved
        );
    }

    /** The derived fields are present only when the field mapping was readable, so both shapes have to round trip. */
    private static KnnEvalResponse.EffectiveKnobs randomEffectiveKnobs() {
        KnnEvalKnobs knobs = KnnEvalSpecTests.createTestKnobs();
        return randomBoolean()
            ? KnnEvalResponse.EffectiveKnobs.of(knobs)
            : new KnnEvalResponse.EffectiveKnobs(knobs, randomIntBetween(1, 10000), randomIntBetween(0, 10000));
    }

    private static KnnEvalResponse.Fidelity randomFidelity() {
        if (randomBoolean()) {
            return KnnEvalResponse.Fidelity.skipped(KnnEvalFidelity.RESCORING_DISABLED);
        }
        List<Double> maxEpsilons = new ArrayList<>();
        for (int i = randomIntBetween(0, 5); i > 0; i--) {
            maxEpsilons.add(randomDoubleBetween(0.0, 2.0, true));
        }
        List<Double> meanByRank = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            // a rank no query reached is null, so nulls have to survive the round trip
            meanByRank.add(randomBoolean() ? null : randomDoubleBetween(0.0, 2.0, true));
        }
        return KnnEvalResponse.Fidelity.of(KnnEvalResponse.DoubleStats.of(maxEpsilons), randomIntBetween(0, 3), meanByRank);
    }

    /** Every block is independently omittable, so all four combinations have to round trip. */
    private static KnnEvalEnvironment randomEnvironment() {
        KnnEvalEnvironment.IndexSummary index = randomBoolean()
            ? null
            : new KnnEvalEnvironment.IndexSummary(1, 1, 30, 0, 2, 10, 15, 20, 4096);
        KnnEvalEnvironment.FieldSummary field = randomBoolean()
            ? null
            : new KnnEvalEnvironment.FieldSummary(
                "dense_vector",
                randomBoolean() ? null : 64,
                "float",
                "l2_norm",
                Map.of("type", "bbq_disk", "rescore_vector", Map.of("oversample", 3.0))
            );
        List<String> versions = randomFrom(List.<List<String>>of(List.of(), List.of("9060000"), List.of("9000000", "9060000")));
        return new KnnEvalEnvironment(index, field, versions, randomBoolean());
    }

    private static KnnEvalResponse.BaselineDetail randomBaselineDetail() {
        List<KnnEvalResponse.Hit> hits = new ArrayList<>();
        int numberOfHits = randomIntBetween(0, 5);
        for (int i = 0; i < numberOfHits; i++) {
            hits.add(new KnnEvalResponse.Hit(randomAlphaOfLengthBetween(1, 8), randomFloatBetween(0.0f, 10.0f, true)));
        }
        return new KnnEvalResponse.BaselineDetail(hits);
    }

    public void testRecallStatsAgreeWithTheHeadlineRecall() {
        KnnEvalResponse response = createRandomResponse();
        for (KnnEvalResponse.KnnSettingsResult result : response.getResults()) {
            KnnEvalResponse.DoubleStats stats = result.recallStats();
            assertEquals("the headline recall is the mean of the distribution", stats.mean(), result.recall(), 0.0);
            assertEquals("one recall value per timed search", result.tookMs().count(), stats.count());
            assertTrue(stats.min() <= stats.p10());
            assertTrue(stats.p10() <= stats.p50());
            assertTrue(stats.p50() <= stats.p90());
            assertTrue(stats.p90() <= stats.p95());
            assertTrue(stats.p95() <= stats.max());
            if (result.recallHistogram() != null) {
                assertHistogramMatchesStats(result);
            }
            // recall_value only ever appears together with fidelity
            assertEquals(result.fidelity() == null, result.recallValue() == null);
        }
    }

    /** Every query lands in exactly one entry, and the entries are ascending with no empty ones. */
    private static void assertHistogramMatchesStats(KnnEvalResponse.KnnSettingsResult result) {
        long total = 0;
        double previous = Double.NEGATIVE_INFINITY;
        for (KnnEvalResponse.RecallBucket bucket : result.recallHistogram()) {
            assertTrue("entries ascend by recall", bucket.recall() > previous);
            assertTrue("no empty entries", bucket.count() > 0);
            previous = bucket.recall();
            total += bucket.count();
        }
        assertEquals("the histogram accounts for every query", result.recallStats().count(), total);
    }

    public void testHistogramMergesValuesThatRoundTogether() {
        List<KnnEvalResponse.RecallBucket> buckets = KnnEvalResponse.RecallBucket.histogram(
            List.of(0.1 + 0.2, 0.3, 1.0, 1.0, 0.3, 0.30000001),
            5
        );
        assertEquals(List.of(new KnnEvalResponse.RecallBucket(0.3, 4), new KnnEvalResponse.RecallBucket(1.0, 2)), buckets);
        assertEquals(List.of(), KnnEvalResponse.RecallBucket.histogram(List.of(), 5));
        assertFalse(KnnEvalResponse.RecallBucket.isBinned(KnnEvalResponse.RecallBucket.MAX_EXACT_K));
    }

    /** At a k this large the exact values are dense, so they are grouped into fixed ranges instead. */
    public void testHistogramIsBinnedAtLargeK() {
        for (int k : new int[] { 23, 1000 }) {
            List<Double> values = new ArrayList<>();
            for (int i = 0; i <= k; i++) {
                values.add((double) i / k);
            }
            assertTrue(KnnEvalResponse.RecallBucket.isBinned(k));
            List<KnnEvalResponse.RecallBucket> buckets = KnnEvalResponse.RecallBucket.histogram(values, k);

            assertThat("twenty ranges plus an exact bin for 1.0", buckets.size(), lessThanOrEqualTo(21));
            long total = 0;
            double previous = Double.NEGATIVE_INFINITY;
            for (KnnEvalResponse.RecallBucket bucket : buckets) {
                assertTrue(bucket.recall() > previous);
                previous = bucket.recall();
                // every entry is a lower edge, so a multiple of the bin width; 1.0 is one too
                double bins = bucket.recall() / KnnEvalResponse.RecallBucket.BIN_WIDTH;
                assertEquals(Math.round(bins), bins, 1e-9);
                total += bucket.count();
            }
            assertEquals(values.size(), total);
            // a perfect recall is never merged into the [0.95, 1.0) range below it
            KnnEvalResponse.RecallBucket last = buckets.get(buckets.size() - 1);
            assertEquals(1.0, last.recall(), 0.0);
            assertEquals(1L, last.count());
        }
    }

    public void testBinEdgesAreHalfOpenRanges() {
        // 0.05 is the lower edge of [0.05, 0.10) and does not fall into [0.00, 0.05), even arriving as a value a hair below it
        List<KnnEvalResponse.RecallBucket> buckets = KnnEvalResponse.RecallBucket.histogram(
            List.of(0.0, 0.049, 0.05, 0.099, 0.95, 0.999, 1.0),
            23
        );
        assertEquals(
            List.of(
                new KnnEvalResponse.RecallBucket(0.0, 2),
                new KnnEvalResponse.RecallBucket(0.05, 2),
                new KnnEvalResponse.RecallBucket(0.95, 2),
                new KnnEvalResponse.RecallBucket(1.0, 1)
            ),
            buckets
        );
    }

    public void testDoubleStatsAreExactPercentiles() {
        // 0.0, 0.1 ... 0.9: nearest rank puts p10 at the 1st value, p50 at the 5th and p90 at the 9th
        List<Double> values = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            values.add(i / 10.0);
        }
        Collections.shuffle(values, random());
        KnnEvalResponse.DoubleStats stats = KnnEvalResponse.DoubleStats.of(values);
        assertEquals(0.45, stats.mean(), 1e-9);
        assertEquals(0.0, stats.min(), 0.0);
        assertEquals(0.0, stats.p10(), 0.0);
        assertEquals(0.4, stats.p50(), 1e-9);
        assertEquals(0.8, stats.p90(), 1e-9);
        assertEquals(0.9, stats.p95(), 1e-9);
        assertEquals(0.9, stats.max(), 1e-9);
        assertEquals(10L, stats.count());
        assertEquals(KnnEvalResponse.DoubleStats.EMPTY, KnnEvalResponse.DoubleStats.of(List.of()));
    }

    public void testSerialization() throws IOException {
        KnnEvalResponse original = createRandomResponse();
        KnnEvalResponse deserialized = copyWriteable(original, new NamedWriteableRegistry(List.of()), KnnEvalResponse::new);
        assertNotSame(original, deserialized);
        assertEquals(original.getBaseline(), deserialized.getBaseline());
        assertEquals(original.getBaselineTookMs(), deserialized.getBaselineTookMs());
        assertEquals(original.getBaselineVectorOps(), deserialized.getBaselineVectorOps());
        assertEquals(original.getBaselineVectorOpsKind(), deserialized.getBaselineVectorOpsKind());
        assertEquals(original.getBaselineDetails(), deserialized.getBaselineDetails());
        assertEquals(original.getEnvironment(), deserialized.getEnvironment());
        assertEquals(original.getResults(), deserialized.getResults());
        assertEquals(original.getFailures().keySet(), deserialized.getFailures().keySet());
        for (Map.Entry<String, Exception> failure : original.getFailures().entrySet()) {
            assertThat(deserialized.getFailures().get(failure.getKey()).getMessage(), containsString(failure.getValue().getMessage()));
        }
    }

    public void testToXContent() throws IOException {
        KnnEvalResponse response = new KnnEvalResponse(
            KnnEvalResponse.EffectiveKnobs.of(new KnnEvalKnobs(100.0f, null, null, false)),
            KnnEvalResponse.LongStats.of(List.of(4L, 6L)),
            KnnEvalResponse.LongStats.of(List.of(500L, 700L)),
            KnnEvalResponse.QUANTIZED_VISIT_PLUS_RESCORE,
            Map.of(
                "q1",
                new KnnEvalResponse.BaselineDetail(List.of(new KnnEvalResponse.Hit("a", 1.5f), new KnnEvalResponse.Hit("b", 1.25f)))
            ),
            new KnnEvalEnvironment(
                new KnnEvalEnvironment.IndexSummary(1, 1, 30, 2, 2, 10, 15, 20, 4096),
                new KnnEvalEnvironment.FieldSummary("dense_vector", 64, "float", "l2_norm", Map.of("type", "hnsw")),
                List.of("9060000"),
                true
            ),
            0.05,
            List.of(
                new KnnEvalResponse.KnnSettingsResult(
                    KnnEvalResponse.EffectiveKnobs.of(new KnnEvalKnobs(5.0f, null, null, false)),
                    0.5,
                    KnnEvalResponse.DoubleStats.of(List.of(0.25, 0.75)),
                    KnnEvalResponse.RecallBucket.histogram(List.of(0.25, 0.75), 5),
                    null,
                    null,
                    null,
                    KnnEvalFidelity.RESCORING_DISABLED,
                    KnnEvalResponse.Fidelity.skipped(KnnEvalFidelity.RESCORING_DISABLED),
                    KnnEvalResponse.LongStats.of(List.of(1L, 3L)),
                    KnnEvalResponse.LongStats.of(List.of(10L, 30L)),
                    Map.of()
                ),
                new KnnEvalResponse.KnnSettingsResult(
                    KnnEvalResponse.EffectiveKnobs.of(new KnnEvalKnobs(20.0f, 200, null, false)),
                    0.5,
                    KnnEvalResponse.DoubleStats.of(List.of(0.5)),
                    KnnEvalResponse.RecallBucket.histogram(List.of(0.5), 5),
                    null,
                    0.75,
                    KnnEvalResponse.DoubleStats.of(List.of(0.75)),
                    null,
                    KnnEvalResponse.Fidelity.of(KnnEvalResponse.DoubleStats.of(List.of(0.5)), 1, Arrays.asList(0.5, null)),
                    KnnEvalResponse.LongStats.of(List.of(2L)),
                    KnnEvalResponse.LongStats.of(List.of(40L)),
                    Map.of(
                        "q1",
                        new KnnEvalResponse.QueryDetail(
                            0.5,
                            1,
                            2,
                            List.of(new KnnEvalResponse.RankedHit("a", 1.5f, 0), new KnnEvalResponse.RankedHit("x", 1.25f, null)),
                            List.of(new KnnEvalResponse.RankedHit("b", 1.25f, 1)),
                            Arrays.asList(0.0, null),
                            true,
                            0.5,
                            1
                        )
                    )
                )
            ),
            Map.of()
        );

        String expected = """
            {
              "baseline": { "visit_percentage": 100.0 },
              "baseline_took_ms": { "mean": 5.0, "p50": 4, "p95": 6, "max": 6, "sum": 10, "count": 2 },
              "baseline_vector_ops": { "mean": 600.0, "p50": 500, "p95": 700, "max": 700, "sum": 1200, "count": 2 },
              "baseline_vector_ops_kind": "quantized_visit_plus_rescore",
              "baseline_details": { "q1": { "hits": [ { "_id": "a", "_score": 1.5 }, { "_id": "b", "_score": 1.25 } ] } },
              "environment": {
                "index": {
                  "indices": 1, "shards": 1,
                  "docs": { "live": 30, "deleted": 2 },
                  "segments": { "count": 2, "min_docs": 10, "median_docs": 15, "max_docs": 20 },
                  "store_size_in_bytes": 4096
                },
                "field": {
                  "type": "dense_vector", "dims": 64, "element_type": "float", "similarity": "l2_norm",
                  "index_options": { "type": "hnsw" }
                },
                "index_version_created": "9060000",
                "allow_expensive_queries": true
              },
              "value_tolerance": 0.05,
              "results": [
                {
                  "knn_settings": { "visit_percentage": 5.0 },
                  "recall": 0.5,
                  "recall_stats": {
                    "mean": 0.5, "min": 0.25, "p10": 0.25, "p50": 0.25, "p90": 0.75, "p95": 0.75, "max": 0.75, "count": 2
                  },
                  "recall_histogram": [ { "recall": 0.25, "count": 1 }, { "recall": 0.75, "count": 1 } ],
                  "recall_value": null,
                  "recall_value_skipped": "rescoring disabled; scores are quantized estimates",
                  "fidelity": { "skipped": "rescoring disabled; scores are quantized estimates" },
                  "took_ms": { "mean": 2.0, "p50": 1, "p95": 3, "max": 3, "sum": 4, "count": 2 },
                  "vector_ops": { "mean": 20.0, "p50": 10, "p95": 30, "max": 30, "sum": 40, "count": 2 }
                },
                {
                  "knn_settings": { "visit_percentage": 20.0, "num_candidates": 200 },
                  "recall": 0.5,
                  "recall_stats": {
                    "mean": 0.5, "min": 0.5, "p10": 0.5, "p50": 0.5, "p90": 0.5, "p95": 0.5, "max": 0.5, "count": 1
                  },
                  "recall_histogram": [ { "recall": 0.5, "count": 1 } ],
                  "recall_value": 0.75,
                  "recall_value_stats": {
                    "mean": 0.75, "min": 0.75, "p10": 0.75, "p50": 0.75, "p90": 0.75, "p95": 0.75, "max": 0.75, "count": 1
                  },
                  "fidelity": {
                    "max_epsilon": { "mean": 0.5, "min": 0.5, "p10": 0.5, "p50": 0.5, "p90": 0.5, "p95": 0.5, "max": 0.5, "count": 1 },
                    "infinite_count": 1,
                    "mean_epsilon_by_rank": [ 0.5, null ]
                  },
                  "took_ms": { "mean": 2.0, "p50": 2, "p95": 2, "max": 2, "sum": 2, "count": 1 },
                  "vector_ops": { "mean": 40.0, "p50": 40, "p95": 40, "max": 40, "sum": 40, "count": 1 },
                  "details": {
                    "q1": {
                      "recall": 0.5,
                      "relevant_retrieved": 1,
                      "relevant": 2,
                      "hits": [
                        { "_id": "a", "_score": 1.5, "baseline_rank": 0 },
                        { "_id": "x", "_score": 1.25, "baseline_rank": null }
                      ],
                      "missed": [ { "_id": "b", "_score": 1.25, "baseline_rank": 1 } ],
                      "recall_value": 0.5,
                      "value_matches": 1,
                      "epsilon_profile": [ 0.0, null ],
                      "incomplete": true
                    }
                  }
                }
              ],
              "failures": {}
            }""";
        assertToXContentEquivalent(
            new BytesArray(expected),
            toXContent(response, XContentType.JSON, ToXContent.EMPTY_PARAMS, false),
            XContentType.JSON
        );
    }

    public void testFailuresAreRendered() throws IOException {
        KnnEvalResponse response = new KnnEvalResponse(
            KnnEvalResponse.EffectiveKnobs.of(new KnnEvalKnobs(100.0f, null, null, false)),
            KnnEvalResponse.LongStats.EMPTY,
            KnnEvalResponse.LongStats.EMPTY,
            KnnEvalResponse.FULL_PRECISION_SCAN,
            Map.of(),
            null,
            0.0,
            List.of(
                new KnnEvalResponse.KnnSettingsResult(
                    KnnEvalResponse.EffectiveKnobs.of(new KnnEvalKnobs(5.0f, null, null, false)),
                    0.0,
                    KnnEvalResponse.DoubleStats.EMPTY,
                    List.of(),
                    null,
                    null,
                    null,
                    KnnEvalFidelity.RESCORING_DISABLED,
                    KnnEvalResponse.Fidelity.skipped(KnnEvalFidelity.RESCORING_DISABLED),
                    KnnEvalResponse.LongStats.EMPTY,
                    KnnEvalResponse.LongStats.EMPTY,
                    Map.of()
                )
            ),
            Map.of("q1", new ParsingException(1, 2, "no such field [emb]", null))
        );
        assertThat(response.toString(), containsString("no such field [emb]"));
        assertThat(response.toString(), containsString("\"type\":\"parsing_exception\""));
    }
}
