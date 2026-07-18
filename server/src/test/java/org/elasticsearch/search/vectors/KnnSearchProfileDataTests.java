/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class KnnSearchProfileDataTests extends ESTestCase {

    public void testIvfToMapContainsAllFields() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithmType("ivf");
        data.setTotalSearchTimeNs(10_000_000);
        data.setFilterTimeNs(500_000);
        data.setMergeTimeNs(100_000);
        data.setVisitRatioUsed(0.15f);
        data.setEarlyTerminated(false);

        data.addSegmentSearched();
        data.addSegmentSearched();
        data.addApproximateSearchTimeNs(9_000_000);

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
        assertThat(map.get("approximate_search_time_ns"), equalTo(9_000_000L));
        assertThat(map.get("merge_time_ns"), equalTo(100_000L));

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

        assertThat(map, not(hasKey("hnsw")));
        assertThat(map, not(hasKey("rescore")));
    }

    public void testHnswToMapContainsAllFields() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithmType("hnsw");
        data.setTotalSearchTimeNs(5_000_000);
        data.setMergeTimeNs(200_000);
        data.setEarlyTerminated(false);
        data.setHnswQueryParams(10, 100, false);

        data.addHnswLeafSearch(1_000_000, 300, 10);
        data.addHnswLeafSearch(2_000_000, 500, 10);
        data.addHnswLeafSearch(500_000, 200, 10);

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
        assertThat(hnsw.get("leaf_searches"), equalTo(3));
        assertThat(hnsw.get("nodes_visited"), equalTo(1000L));
        assertThat(hnsw.get("results_found_before_merge"), equalTo(30));

        @SuppressWarnings("unchecked")
        Map<String, Object> timings = (Map<String, Object>) hnsw.get("timings");
        assertThat(timings, notNullValue());
        assertThat(timings.get("avg_leaf_search_ns"), equalTo(3_500_000L / 3));
        assertThat(timings.get("max_leaf_search_ns"), equalTo(2_000_000L));
        assertThat(timings.get("min_leaf_search_ns"), equalTo(500_000L));
        assertThat(timings, not(hasKey("merge_ns")));
        assertThat((long) timings.get("filter_and_overhead_ns"), greaterThan(0L));

        assertThat(map, not(hasKey("ivf")));
        assertThat(map, not(hasKey("rescore")));
    }

    public void testHnswWithFilter() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithmType("hnsw");
        data.setTotalSearchTimeNs(5_000_000);
        data.setMergeTimeNs(100_000);
        data.setHnswQueryParams(10, 100, true);

        data.addHnswLeafSearch(2_000_000, 500, 10);

        Map<String, Object> map = data.toMap();
        @SuppressWarnings("unchecked")
        Map<String, Object> hnsw = (Map<String, Object>) map.get("hnsw");
        assertThat(hnsw.get("has_filter"), equalTo(true));
    }

    public void testNoRescoreWhenNotSet() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithmType("hnsw");
        data.setTotalSearchTimeNs(1_000_000);
        Map<String, Object> map = data.toMap();
        assertThat(map, not(hasKey("rescore")));
    }

    public void testNoFilterTimeWhenZero() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithmType("hnsw");
        data.setTotalSearchTimeNs(1_000_000);
        Map<String, Object> map = data.toMap();
        assertThat(map, not(hasKey("filter_time_ns")));
    }

    public void testThreadSafeConcurrentAccumulation() throws Exception {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithmType("ivf");

        int threadCount = 8;
        Thread[] threads = new Thread[threadCount];
        for (int t = 0; t < threadCount; t++) {
            threads[t] = new Thread(() -> {
                data.addSegmentSearched();
                data.addApproximateSearchTimeNs(1_000_000);
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
        data.setAlgorithmType("hnsw");
        data.setTotalSearchTimeNs(10_000_000);
        data.setMergeTimeNs(100_000);
        data.setHnswQueryParams(10, 100, false);

        data.addHnswLeafSearch(100_000, 10, 5);
        data.addHnswLeafSearch(5_000_000, 500, 10);
        data.addHnswLeafSearch(200_000, 20, 5);

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
        data.setAlgorithmType("hnsw");
        data.setTotalSearchTimeNs(0);
        data.setHnswQueryParams(10, 100, false);

        Map<String, Object> map = data.toMap();
        assertThat(map.get("segments_searched"), equalTo(0));
        assertThat(map.get("approximate_search_time_ns"), equalTo(0L));

        @SuppressWarnings("unchecked")
        Map<String, Object> hnsw = (Map<String, Object>) map.get("hnsw");
        assertThat(hnsw.get("leaf_searches"), equalTo(0));
        @SuppressWarnings("unchecked")
        Map<String, Object> timings = (Map<String, Object>) hnsw.get("timings");
        assertThat(timings, not(hasKey("avg_leaf_search_ns")));
        assertThat(timings, not(hasKey("min_leaf_search_ns")));
    }

    public void testQuantizationAndScorerSurfaced() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithmType("ivf");
        data.setQuantization("bbq_disk");
        data.setScorer("panama");

        Map<String, Object> map = data.toMap();
        assertThat(map.get("quantization"), equalTo("bbq_disk"));
        assertThat(map.get("scorer"), equalTo("panama"));
    }

    public void testNoQuantizationOrScorerWhenNotSet() {
        KnnSearchProfileData data = new KnnSearchProfileData();
        data.setAlgorithmType("hnsw");

        Map<String, Object> map = data.toMap();
        assertThat(map, not(hasKey("quantization")));
        assertThat(map, not(hasKey("scorer")));
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
