/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.search.vectors.KnnSearchProfileData.Algorithm;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Unit tests for the {@code knn_profile} accumulator, covering the shape of {@link KnnSearchProfileData#toMap()}
 * and the aggregation it derives from the per-leaf records. The per-segment detail, which needs real
 * {@code LeafReaderContext}s, is covered by {@link ESKnnVectorQueryProfileTests} and
 * {@link IVFKnnFloatVectorQueryTests} against real indices.
 */
public class KnnSearchProfileDataTests extends ESTestCase {

    public void testIvfToMapContainsAllFields() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithm(Algorithm.IVF);
        data.setTotalSearchTimeNs(10_000_000);
        data.setFilterTimeNs(500_000);
        data.setMergeTimeNs(100_000);
        data.setEarlyTerminated(false);

        data.addIvfLeafSearchForTest(5_000_000, 0.15f);
        data.addIvfLeafSearchForTest(4_000_000, 0.15f);

        data.addCentroidsEvaluated(12);
        data.addCentroidIteratorCreateTimeNs(200_000);
        data.addPostingVisitTimeNs(6_000_000);
        data.addResetPostingsScorerTimeNs(400_000);
        data.addPostingsScored(5000);
        data.addExpectedDocs(8000);
        data.addDocIdReadTimeNs(800_000);
        data.addScoringTimeNs(4_500_000);
        data.addQueryQuantizationTimeNs(50_000);
        data.addCentroidReadTimeNs(150_000);

        Map<String, Object> map = data.toMap();

        assertThat(map.get("algorithm"), equalTo("ivf"));
        assertThat(map.get("total_time_ns"), equalTo(10_000_000L));
        assertThat(map.get("segments_searched"), equalTo(2));
        assertThat(map.get("early_terminated"), equalTo(false));
        assertThat(map.get("filter_time_ns"), equalTo(500_000L));
        // Derived from the per-leaf records rather than tracked separately, so it cannot drift from them.
        assertThat(map.get("approximate_search_time_ns"), equalTo(9_000_000L));
        assertThat(map.get("merge_time_ns"), equalTo(100_000L));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> segments = (List<Map<String, Object>>) map.get("segments");
        assertThat(segments, notNullValue());
        assertThat(segments.size(), equalTo(2));
        assertThat(segments.get(0).get("search_time_ns"), equalTo(5_000_000L));
        assertThat(segments.get(0).get("visit_ratio_used"), equalTo(0.15f));

        @SuppressWarnings("unchecked")
        Map<String, Object> ivf = (Map<String, Object>) map.get("ivf");
        assertThat(ivf, notNullValue());
        assertThat(ivf.get("visit_ratio_used"), equalTo(0.15f));
        assertThat(ivf.get("centroids_evaluated"), equalTo(12));
        assertThat(ivf.get("postings_scored"), equalTo(5000L));
        assertThat(ivf.get("expected_docs_visited"), equalTo(8000L));

        @SuppressWarnings("unchecked")
        Map<String, Object> timings = (Map<String, Object>) ivf.get("timings");
        assertThat(timings, notNullValue());
        assertThat(timings.get("centroid_iterator_create_ns"), equalTo(200_000L));
        assertThat(timings.get("centroid_read_ns"), equalTo(150_000L));
        assertThat(timings.get("reset_postings_scorer_ns"), equalTo(400_000L));
        assertThat(timings.get("posting_visit_ns"), equalTo(6_000_000L));
        assertThat(timings.get("doc_id_read_ns"), equalTo(800_000L));
        assertThat(timings.get("query_quantization_ns"), equalTo(50_000L));
        assertThat(timings.get("scoring_ns"), equalTo(4_500_000L));
        // What is left of total_time_ns once the filter, the per-segment searches and the merge are removed.
        assertThat(timings.get("overhead_ns"), equalTo(400_000L));

        assertThat(map, not(hasKey("hnsw")));
        assertThat(map, not(hasKey("rescore")));
    }

    public void testHnswToMapContainsAllFields() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithm(Algorithm.HNSW);
        data.setTotalSearchTimeNs(5_000_000);
        data.setMergeTimeNs(200_000);
        data.setEarlyTerminated(false);
        data.setHnswQueryParams(10, 100, false);

        data.addHnswLeafSearchForTest(1_000_000, 300, 10);
        data.addHnswLeafSearchForTest(2_000_000, 500, 10);
        data.addHnswLeafSearchForTest(500_000, 200, 10);

        Map<String, Object> map = data.toMap();

        assertThat(map.get("algorithm"), equalTo("hnsw"));
        assertThat(map.get("total_time_ns"), equalTo(5_000_000L));
        assertThat(map.get("segments_searched"), equalTo(3));
        assertThat(map.get("early_terminated"), equalTo(false));
        assertThat(map.get("approximate_search_time_ns"), equalTo(3_500_000L));
        assertThat(map.get("merge_time_ns"), equalTo(200_000L));

        @SuppressWarnings("unchecked")
        Map<String, Object> hnsw = (Map<String, Object>) map.get("hnsw");
        assertThat(hnsw, notNullValue());
        assertThat(hnsw.get("k"), equalTo(10));
        assertThat(hnsw.get("num_candidates"), equalTo(100));
        assertThat(hnsw.get("has_filter"), equalTo(false));
        assertThat(hnsw.get("nodes_visited"), equalTo(1000L));
        assertThat(hnsw.get("results_found_before_merge"), equalTo(30L));

        @SuppressWarnings("unchecked")
        Map<String, Object> timings = (Map<String, Object>) hnsw.get("timings");
        assertThat(timings, notNullValue());
        assertThat(timings.get("avg_leaf_search_ns"), equalTo(3_500_000L / 3));
        assertThat(timings.get("max_leaf_search_ns"), equalTo(2_000_000L));
        assertThat(timings.get("min_leaf_search_ns"), equalTo(500_000L));
        assertThat((long) timings.get("overhead_ns"), greaterThan(0L));

        assertThat(map, not(hasKey("ivf")));
        assertThat(map, not(hasKey("rescore")));
    }

    public void testHnswWithFilter() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithm(Algorithm.HNSW);
        data.setTotalSearchTimeNs(5_000_000);
        data.setMergeTimeNs(100_000);
        data.setHnswQueryParams(10, 100, true);

        data.addHnswLeafSearchForTest(2_000_000, 500, 10);

        Map<String, Object> map = data.toMap();
        @SuppressWarnings("unchecked")
        Map<String, Object> hnsw = (Map<String, Object>) map.get("hnsw");
        assertThat(hnsw.get("has_filter"), equalTo(true));
    }

    public void testNoRescoreWhenNotSet() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithm(Algorithm.HNSW);
        data.setTotalSearchTimeNs(1_000_000);
        Map<String, Object> map = data.toMap();
        assertThat(map, not(hasKey("rescore")));
    }

    public void testNoFilterTimeWhenZero() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithm(Algorithm.HNSW);
        data.setTotalSearchTimeNs(1_000_000);
        Map<String, Object> map = data.toMap();
        assertThat(map, not(hasKey("filter_time_ns")));
    }

    /**
     * The accumulator is shared by the parallel per-leaf search tasks, so all threads are released together
     * on a barrier to make them contend rather than run one after another.
     */
    public void testThreadSafeConcurrentAccumulation() throws Exception {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithm(Algorithm.IVF);

        int threadCount = 8;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        Thread[] threads = new Thread[threadCount];
        for (int t = 0; t < threadCount; t++) {
            threads[t] = new Thread(() -> {
                safeAwait(barrier);
                data.addIvfLeafSearchForTest(1_000_000, 0.1f);
                data.addCentroidsEvaluated(5);
                data.addPostingVisitTimeNs(500_000);
                data.addPostingsScored(100);
                data.addDocIdReadTimeNs(50_000);
                data.addScoringTimeNs(400_000);
            });
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }

        Map<String, Object> map = data.toMap();
        assertThat(map.get("segments_searched"), equalTo(threadCount));
        assertThat(map.get("approximate_search_time_ns"), equalTo((long) threadCount * 1_000_000));

        @SuppressWarnings("unchecked")
        Map<String, Object> ivf = (Map<String, Object>) map.get("ivf");
        assertThat(ivf.get("centroids_evaluated"), equalTo(threadCount * 5));
        assertThat(ivf.get("postings_scored"), equalTo((long) threadCount * 100));

        @SuppressWarnings("unchecked")
        Map<String, Object> timings = (Map<String, Object>) ivf.get("timings");
        assertThat(timings.get("posting_visit_ns"), equalTo((long) threadCount * 500_000));
        assertThat(timings.get("doc_id_read_ns"), equalTo((long) threadCount * 50_000));
        assertThat(timings.get("scoring_ns"), equalTo((long) threadCount * 400_000));
    }

    public void testHnswMinMaxLeafTiming() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithm(Algorithm.HNSW);
        data.setTotalSearchTimeNs(10_000_000);
        data.setMergeTimeNs(100_000);
        data.setHnswQueryParams(10, 100, false);

        data.addHnswLeafSearchForTest(100_000, 10, 5);
        data.addHnswLeafSearchForTest(5_000_000, 500, 10);
        data.addHnswLeafSearchForTest(200_000, 20, 5);

        Map<String, Object> map = data.toMap();
        @SuppressWarnings("unchecked")
        Map<String, Object> hnsw = (Map<String, Object>) map.get("hnsw");
        @SuppressWarnings("unchecked")
        Map<String, Object> timings = (Map<String, Object>) hnsw.get("timings");

        assertThat(timings.get("min_leaf_search_ns"), equalTo(100_000L));
        assertThat(timings.get("max_leaf_search_ns"), equalTo(5_000_000L));
    }

    public void testHnswNoSegmentsSearched() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithm(Algorithm.HNSW);
        data.setTotalSearchTimeNs(0);
        data.setHnswQueryParams(10, 100, false);

        Map<String, Object> map = data.toMap();
        assertThat(map.get("segments_searched"), equalTo(0));
        assertThat(map.get("approximate_search_time_ns"), equalTo(0L));
        assertThat(map, not(hasKey("segments")));

        @SuppressWarnings("unchecked")
        Map<String, Object> hnsw = (Map<String, Object>) map.get("hnsw");
        assertThat(hnsw.get("nodes_visited"), equalTo(0L));
        @SuppressWarnings("unchecked")
        Map<String, Object> timings = (Map<String, Object>) hnsw.get("timings");
        assertThat(timings, not(hasKey("avg_leaf_search_ns")));
        assertThat(timings, not(hasKey("min_leaf_search_ns")));
    }

    public void testQuantizationAndScorerSurfaced() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithm(Algorithm.IVF);
        data.setQuantization("bbq_disk");
        data.setScorer("panama");

        Map<String, Object> map = data.toMap();
        assertThat(map.get("quantization"), equalTo("bbq_disk"));
        assertThat(map.get("scorer"), equalTo("panama"));
    }

    /**
     * IVF times only the codec's per-segment search, so the collector drain and the doc ID deduplication
     * that follow it land in {@code overhead_ns}. It is clamped at zero rather than going negative, because
     * the per-segment times are a sum over segments that may have been searched in parallel.
     */
    public void testIvfOverheadClampedWhenSegmentsSearchedInParallel() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithm(Algorithm.IVF);
        data.setTotalSearchTimeNs(3_000_000);
        data.addIvfLeafSearchForTest(2_500_000, 0.1f);
        data.addIvfLeafSearchForTest(2_500_000, 0.1f);

        Map<String, Object> map = data.toMap();
        @SuppressWarnings("unchecked")
        Map<String, Object> timings = (Map<String, Object>) ((Map<String, Object>) map.get("ivf")).get("timings");
        assertThat(timings.get("overhead_ns"), equalTo(0L));
    }

    public void testIvfOmitsZeroInnerTimings() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithm(Algorithm.IVF);
        data.addIvfLeafSearchForTest(1_000_000, 0.1f);
        data.addPostingVisitTimeNs(1_000_000);
        // Inner visitor timings left at 0 - must not appear as if scoring was free.
        Map<String, Object> map = data.toMap();
        @SuppressWarnings("unchecked")
        Map<String, Object> timings = (Map<String, Object>) ((Map<String, Object>) map.get("ivf")).get("timings");
        assertThat(timings.get("posting_visit_ns"), equalTo(1_000_000L));
        assertThat(timings, not(hasKey("scoring_ns")));
        assertThat(timings, not(hasKey("doc_id_read_ns")));
        assertThat(timings, not(hasKey("query_quantization_ns")));
        assertThat(timings, not(hasKey("centroid_read_ns")));
    }

    public void testVisitRatioMinWhenLeavesDisagree() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithm(Algorithm.IVF);
        data.addIvfLeafSearchForTest(1_000_000, 0.04f);
        data.addIvfLeafSearchForTest(1_000_000, 0.10f);
        data.addIvfLeafSearchForTest(1_000_000, 0.04f);
        @SuppressWarnings("unchecked")
        Map<String, Object> ivf = (Map<String, Object>) data.toMap().get("ivf");
        assertThat(ivf.get("visit_ratio_used"), equalTo(0.10f));
        assertThat(ivf.get("visit_ratio_min"), equalTo(0.04f));
    }

    public void testNoVisitRatioMinWhenLeavesAgree() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithm(Algorithm.IVF);
        data.addIvfLeafSearchForTest(1_000_000, 0.04f);
        data.addIvfLeafSearchForTest(1_000_000, 0.04f);
        @SuppressWarnings("unchecked")
        Map<String, Object> ivf = (Map<String, Object>) data.toMap().get("ivf");
        assertThat(ivf.get("visit_ratio_used"), equalTo(0.04f));
        assertThat(ivf, not(hasKey("visit_ratio_min")));
    }

    /** The codec does not report a ratio when the search never reaches it (e.g. an empty segment). */
    public void testNoVisitRatioWhenCodecReportedNone() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithm(Algorithm.IVF);
        data.addIvfLeafSearchForTest(1_000_000, Float.NaN);
        @SuppressWarnings("unchecked")
        Map<String, Object> ivf = (Map<String, Object>) data.toMap().get("ivf");
        assertThat(ivf, not(hasKey("visit_ratio_used")));
        assertThat(ivf, not(hasKey("visit_ratio_min")));
    }

    /**
     * The parallel leaf tasks append in completion order; the emitted per-segment list must still follow
     * leaf ordinal so repeated runs of the same search are comparable.
     */
    public void testSegmentsOrderedByLeafOrdinal() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithm(Algorithm.HNSW);
        data.addHnswLeafSearchForTest(300, 3, 3);
        data.addHnswLeafSearchForTest(100, 1, 1);
        data.addHnswLeafSearchForTest(200, 2, 2);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> segments = (List<Map<String, Object>>) data.toMap().get("segments");
        assertThat(segments.size(), equalTo(3));
        assertThat(segments.get(0).get("search_time_ns"), equalTo(300L));
        assertThat(segments.get(1).get("search_time_ns"), equalTo(100L));
        assertThat(segments.get(2).get("search_time_ns"), equalTo(200L));
    }

    public void testFieldSurfaced() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithm(Algorithm.HNSW);
        data.setField("vector");
        assertThat(data.toMap().get("field"), equalTo("vector"));
    }

    public void testScorerFirstNonNullWins() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithm(Algorithm.IVF);
        data.setScorer("panama");
        data.setScorer("scalar");
        assertThat(data.toMap().get("scorer"), equalTo("panama"));
    }

    public void testNoQuantizationOrScorerWhenNotSet() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithm(Algorithm.HNSW);

        Map<String, Object> map = data.toMap();
        assertThat(map, not(hasKey("quantization")));
        assertThat(map, not(hasKey("scorer")));
    }

    /** No algorithm set (e.g. a breakdown that only carries a rescore section) emits neither section. */
    public void testNoAlgorithmSection() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        Map<String, Object> map = data.toMap();
        assertThat(map, not(hasKey("algorithm")));
        assertThat(map, not(hasKey("hnsw")));
        assertThat(map, not(hasKey("ivf")));
    }

    public void testScorerImplementationClassification() {
        assertThat(KnnSearchProfileData.scorerImplementation(null), equalTo(null));
        // Plain classes (no "Native" marker, not in a ...vectorization package) are the scalar fallback.
        assertThat(KnnSearchProfileData.scorerImplementation(new Object()), equalTo("scalar"));
        // A class whose fully-qualified name carries the "Native" marker is classified as native.
        class NativeSampleScorer {}
        assertThat(KnnSearchProfileData.scorerImplementation(new NativeSampleScorer()), equalTo("native"));
    }
}
