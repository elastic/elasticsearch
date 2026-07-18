/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe accumulator for KNN search profiling data.
 * One instance per query rewrite, shared across parallel per-leaf search tasks.
 * Leaf-level metrics use atomics for safe concurrent accumulation.
 */
public final class KnnSearchProfileData {

    // --- query-level (set once, after all tasks) ---
    private volatile long filterTimeNs;
    private volatile long totalSearchTimeNs;
    private volatile long mergeTimeNs;
    private volatile float visitRatioUsed;
    private volatile boolean earlyTerminated;
    private volatile String algorithmType;
    private volatile String quantization;
    private volatile String scorer;

    // --- per-leaf, accumulated across parallel tasks ---
    private final AtomicInteger segmentsSearched = new AtomicInteger();
    private final AtomicLong approximateSearchTimeNs = new AtomicLong();

    // --- IVF-specific, accumulated from IVFVectorsReader.search() via strategy ---
    private final AtomicInteger centroidsEvaluated = new AtomicInteger();
    private final AtomicLong centroidIteratorCreateTimeNs = new AtomicLong();
    private final AtomicLong postingVisitTimeNs = new AtomicLong();
    private final AtomicLong quantizationTimeNs = new AtomicLong();
    private final AtomicLong postingsScored = new AtomicLong();
    private final AtomicLong expectedDocsTotal = new AtomicLong();

    // --- IVF granular: from PostingVisitor ---
    private final AtomicLong docIdReadTimeNs = new AtomicLong();
    private final AtomicLong scoringTimeNs = new AtomicLong();
    private final AtomicLong queryQuantizationTimeNs = new AtomicLong();
    private final AtomicLong centroidReadTimeNs = new AtomicLong();
    private final AtomicLong resetPostingsScorerTimeNs = new AtomicLong();

    // --- HNSW-specific ---
    private final AtomicInteger hnswGraphSearchSegments = new AtomicInteger();
    private final AtomicLong hnswPerLeafSearchTimeNs = new AtomicLong();
    private final AtomicLong hnswNodesVisited = new AtomicLong();
    private final AtomicInteger hnswLeafResultsFound = new AtomicInteger();
    private final AtomicLong hnswMaxLeafSearchTimeNs = new AtomicLong();
    private final AtomicLong hnswMinLeafSearchTimeNs = new AtomicLong(Long.MAX_VALUE);
    private volatile int hnswNumCandidates;
    private volatile int hnswK;
    private volatile boolean hnswHasFilter;

    // ---- setters for query-level data ----

    public void setFilterTimeNs(long ns) {
        this.filterTimeNs = ns;
    }

    public void setTotalSearchTimeNs(long ns) {
        this.totalSearchTimeNs = ns;
    }

    public void setMergeTimeNs(long ns) {
        this.mergeTimeNs = ns;
    }

    public void setVisitRatioUsed(float ratio) {
        this.visitRatioUsed = ratio;
    }

    public void setEarlyTerminated(boolean terminated) {
        this.earlyTerminated = terminated;
    }

    public void setAlgorithmType(String type) {
        this.algorithmType = type;
    }

    public void setQuantization(String quantization) {
        this.quantization = quantization;
    }

    public void setScorer(String scorer) {
        this.scorer = scorer;
    }

    /**
     * Classifies a vector scorer instance into the implementation family that actually ran:
     * {@code "native"} (native SIMD via the vec library), {@code "panama"} (JDK Vector API), or
     * {@code "scalar"} (plain-Java fallback). Panama implementations live in the
     * {@code ...simdvec.internal.vectorization} package; native ones carry {@code Native} in their name.
     */
    public static String scorerImplementation(Object scorer) {
        if (scorer == null) {
            return null;
        }
        String className = scorer.getClass().getName();
        if (className.contains("Native")) {
            return "native";
        }
        if (className.contains(".vectorization.")) {
            return "panama";
        }
        return "scalar";
    }

    // ---- accumulation methods for per-leaf / per-centroid data ----

    public void addSegmentSearched() {
        segmentsSearched.incrementAndGet();
    }

    public void addApproximateSearchTimeNs(long ns) {
        approximateSearchTimeNs.addAndGet(ns);
    }

    public void addCentroidsEvaluated(int count) {
        centroidsEvaluated.addAndGet(count);
    }

    public void addCentroidIteratorCreateTimeNs(long ns) {
        centroidIteratorCreateTimeNs.addAndGet(ns);
    }

    public void addPostingVisitTimeNs(long ns) {
        postingVisitTimeNs.addAndGet(ns);
    }

    public void addQuantizationTimeNs(long ns) {
        quantizationTimeNs.addAndGet(ns);
    }

    public void addPostingsScored(long count) {
        postingsScored.addAndGet(count);
    }

    public void addExpectedDocs(long count) {
        expectedDocsTotal.addAndGet(count);
    }

    public void addDocIdReadTimeNs(long ns) {
        docIdReadTimeNs.addAndGet(ns);
    }

    public void addScoringTimeNs(long ns) {
        scoringTimeNs.addAndGet(ns);
    }

    public void addQueryQuantizationTimeNs(long ns) {
        queryQuantizationTimeNs.addAndGet(ns);
    }

    public void addCentroidReadTimeNs(long ns) {
        centroidReadTimeNs.addAndGet(ns);
    }

    public void addResetPostingsScorerTimeNs(long ns) {
        resetPostingsScorerTimeNs.addAndGet(ns);
    }

    public void addHnswLeafSearch(long searchTimeNs, long nodesVisited, int resultsFound) {
        hnswGraphSearchSegments.incrementAndGet();
        hnswPerLeafSearchTimeNs.addAndGet(searchTimeNs);
        hnswNodesVisited.addAndGet(nodesVisited);
        hnswLeafResultsFound.addAndGet(resultsFound);
        // track min/max leaf search time for skew detection
        hnswMaxLeafSearchTimeNs.accumulateAndGet(searchTimeNs, Math::max);
        hnswMinLeafSearchTimeNs.accumulateAndGet(searchTimeNs, Math::min);
    }

    public void setHnswQueryParams(int k, int numCandidates, boolean hasFilter) {
        this.hnswK = k;
        this.hnswNumCandidates = numCandidates;
        this.hnswHasFilter = hasFilter;
    }

    /**
     * Converts the collected profile data into a map suitable for serialization
     * in profile output.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        if (algorithmType != null) {
            map.put("algorithm", algorithmType);
        }
        if (quantization != null) {
            map.put("quantization", quantization);
        }
        if (scorer != null) {
            map.put("scorer", scorer);
        }
        map.put("total_time_ns", totalSearchTimeNs);

        if ("ivf".equals(algorithmType)) {
            map.put("segments_searched", segmentsSearched.get());
            map.put("early_terminated", earlyTerminated);
            if (filterTimeNs > 0) {
                map.put("filter_time_ns", filterTimeNs);
            }
            map.put("approximate_search_time_ns", approximateSearchTimeNs.get());
            map.put("merge_time_ns", mergeTimeNs);

            Map<String, Object> ivf = new LinkedHashMap<>();
            ivf.put("visit_ratio_used", visitRatioUsed);
            ivf.put("centroids_evaluated", centroidsEvaluated.get());
            ivf.put("postings_scored", postingsScored.get());
            ivf.put("expected_docs_visited", expectedDocsTotal.get());

            Map<String, Object> timings = new LinkedHashMap<>();
            timings.put("centroid_iterator_create_ns", centroidIteratorCreateTimeNs.get());
            timings.put("centroid_read_ns", centroidReadTimeNs.get());
            timings.put("reset_postings_scorer_ns", resetPostingsScorerTimeNs.get());
            timings.put("posting_visit_ns", postingVisitTimeNs.get());
            timings.put("doc_id_read_ns", docIdReadTimeNs.get());
            timings.put("query_quantization_ns", queryQuantizationTimeNs.get());
            timings.put("scoring_ns", scoringTimeNs.get());
            ivf.put("timings", timings);

            map.put("ivf", ivf);
        }

        if ("hnsw".equals(algorithmType)) {
            int segments = hnswGraphSearchSegments.get();
            map.put("segments_searched", segments);
            map.put("early_terminated", earlyTerminated);
            if (filterTimeNs > 0) {
                map.put("filter_time_ns", filterTimeNs);
            }
            map.put("approximate_search_time_ns", hnswPerLeafSearchTimeNs.get());
            map.put("merge_time_ns", mergeTimeNs);

            Map<String, Object> hnsw = new LinkedHashMap<>();
            hnsw.put("k", hnswK);
            hnsw.put("num_candidates", hnswNumCandidates);
            hnsw.put("has_filter", hnswHasFilter);
            hnsw.put("leaf_searches", segments);
            hnsw.put("nodes_visited", hnswNodesVisited.get());
            hnsw.put("results_found_before_merge", hnswLeafResultsFound.get());

            Map<String, Object> timings = new LinkedHashMap<>();
            if (segments > 0) {
                timings.put("avg_leaf_search_ns", hnswPerLeafSearchTimeNs.get() / segments);
                timings.put("max_leaf_search_ns", hnswMaxLeafSearchTimeNs.get());
                long minVal = hnswMinLeafSearchTimeNs.get();
                timings.put("min_leaf_search_ns", minVal == Long.MAX_VALUE ? 0 : minVal);
            }
            long overhead = totalSearchTimeNs - hnswPerLeafSearchTimeNs.get() - mergeTimeNs;
            timings.put("filter_and_overhead_ns", Math.max(0, overhead));
            hnsw.put("timings", timings);

            map.put("hnsw", hnsw);
        }

        return map;
    }
}
