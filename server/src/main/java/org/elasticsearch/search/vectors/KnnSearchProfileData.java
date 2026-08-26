/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
    private volatile boolean earlyTerminated;
    private volatile String algorithmType;
    private volatile String quantization;
    private volatile String field;
    private final AtomicReference<String> scorer = new AtomicReference<>();
    private final ConcurrentLinkedQueue<Float> visitRatiosUsed = new ConcurrentLinkedQueue<>();
    private final ThreadLocal<Float> leafVisitRatio = new ThreadLocal<>();

    // --- per-leaf, accumulated across parallel tasks ---
    private final AtomicInteger segmentsSearched = new AtomicInteger();
    private final AtomicLong approximateSearchTimeNs = new AtomicLong();
    private final ConcurrentLinkedQueue<Map<String, Object>> segments = new ConcurrentLinkedQueue<>();

    // --- IVF-specific, accumulated from IVFVectorsReader.search() via strategy ---
    private final AtomicInteger centroidsEvaluated = new AtomicInteger();
    private final AtomicLong centroidIteratorCreateTimeNs = new AtomicLong();
    private final AtomicLong postingVisitTimeNs = new AtomicLong();
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

    /**
     * Records a per-leaf visit ratio. Parallel leaves may compute different dynamic ratios;
     * {@link #toMap()} emits the max as {@code visit_ratio_used} and {@code visit_ratio_min}
     * when they differ. The calling leaf's ratio is also attached to the next {@link #addSegment}.
     */
    public void setVisitRatioUsed(float ratio) {
        visitRatiosUsed.add(ratio);
        leafVisitRatio.set(ratio);
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

    public void setField(String field) {
        this.field = field;
    }

    /**
     * Records the scorer family for this query. Parallel leaves in one JVM use the same
     * implementation; the first non-null value wins so a race cannot flip the label.
     */
    public void setScorer(String scorer) {
        if (scorer != null) {
            this.scorer.compareAndSet(null, scorer);
        }
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

    /**
     * Records that a leaf was searched and appends a brief per-segment breakdown
     * ({@code name}, {@code doc_count}, {@code size_in_bytes}, vector stats, {@code search_time_ns}).
     */
    public void addSegmentSearched(LeafReaderContext ctx, String field, long searchTimeNs) {
        addSegmentSearched();
        addSegment(ctx, field, searchTimeNs, -1, -1);
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

    /**
     * HNSW per-leaf accumulation plus a brief per-segment breakdown for the leaf that just ran.
     */
    public void addHnswLeafSearch(LeafReaderContext ctx, String field, long searchTimeNs, long nodesVisited, int resultsFound) {
        addHnswLeafSearch(searchTimeNs, nodesVisited, resultsFound);
        addSegment(ctx, field, searchTimeNs, nodesVisited, resultsFound);
    }

    /**
     * Appends a pre-built per-segment map. Used by tests; production callers use the
     * {@link LeafReaderContext} overloads.
     */
    public void addSegment(Map<String, Object> segment) {
        segments.add(segment);
    }

    /**
     * Collects a brief per-segment snapshot: Lucene segment name, live doc count, on-disk segment
     * size, vector count/bytes for {@code field}, and this leaf's search time. {@code nodesVisited}
     * / {@code resultsFound} are HNSW-only; pass {@code -1} to omit them.
     */
    public void addSegment(LeafReaderContext ctx, String field, long searchTimeNs, long nodesVisited, int resultsFound) {
        Map<String, Object> seg = new LinkedHashMap<>();
        SegmentReader sr = Lucene.tryUnwrapSegmentReader(ctx.reader());
        if (sr != null) {
            seg.put("name", sr.getSegmentName());
            try {
                seg.put("size_in_bytes", sr.getSegmentInfo().sizeInBytes());
            } catch (IOException e) {
                // optional; omit rather than fail a profiled search
            }
        } else {
            seg.put("name", Integer.toString(ctx.ord));
        }
        seg.put("doc_count", ctx.reader().numDocs());
        try {
            FloatVectorValues floatValues = ctx.reader().getFloatVectorValues(field);
            if (floatValues != null) {
                seg.put("vector_count", floatValues.size());
                seg.put("vector_bytes", (long) floatValues.size() * floatValues.getVectorByteLength());
            } else {
                ByteVectorValues byteValues = ctx.reader().getByteVectorValues(field);
                if (byteValues != null) {
                    seg.put("vector_count", byteValues.size());
                    seg.put("vector_bytes", (long) byteValues.size() * byteValues.getVectorByteLength());
                }
            }
        } catch (IOException e) {
            // optional; omit rather than fail a profiled search
        }
        seg.put("search_time_ns", searchTimeNs);
        Float ratio = leafVisitRatio.get();
        if (ratio != null) {
            seg.put("visit_ratio_used", ratio);
            leafVisitRatio.remove();
        }
        if (nodesVisited >= 0) {
            seg.put("nodes_visited", nodesVisited);
        }
        if (resultsFound >= 0) {
            seg.put("results_found", resultsFound);
        }
        segments.add(seg);
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
        if (field != null) {
            map.put("field", field);
        }
        if (quantization != null) {
            map.put("quantization", quantization);
        }
        String scorerName = scorer.get();
        if (scorerName != null) {
            map.put("scorer", scorerName);
        }
        map.put("total_time_ns", totalSearchTimeNs);
        if (segments.isEmpty() == false) {
            map.put("segments", new ArrayList<>(segments));
        }

        if ("ivf".equals(algorithmType)) {
            map.put("segments_searched", segmentsSearched.get());
            map.put("early_terminated", earlyTerminated);
            if (filterTimeNs > 0) {
                map.put("filter_time_ns", filterTimeNs);
            }
            map.put("approximate_search_time_ns", approximateSearchTimeNs.get());
            map.put("merge_time_ns", mergeTimeNs);

            Map<String, Object> ivf = new LinkedHashMap<>();
            if (visitRatiosUsed.isEmpty() == false) {
                float min = Float.POSITIVE_INFINITY;
                float max = Float.NEGATIVE_INFINITY;
                for (float r : visitRatiosUsed) {
                    min = Math.min(min, r);
                    max = Math.max(max, r);
                }
                ivf.put("visit_ratio_used", max);
                if (Float.compare(min, max) != 0) {
                    ivf.put("visit_ratio_min", min);
                }
            }
            ivf.put("centroids_evaluated", centroidsEvaluated.get());
            ivf.put("postings_scored", postingsScored.get());
            ivf.put("expected_docs_visited", expectedDocsTotal.get());

            // Outer wrappers (centroid_iterator_create, reset_postings_scorer, posting_visit) enclose
            // the inner visitor timings (centroid_read, doc_id_read, query_quantization, scoring).
            // They are not additive. Inner keys are omitted when the visitor did not collect them
            // (older codecs, or profiling not enabled on that visitor).
            Map<String, Object> timings = new LinkedHashMap<>();
            timings.put("centroid_iterator_create_ns", centroidIteratorCreateTimeNs.get());
            putIfPositive(timings, "centroid_read_ns", centroidReadTimeNs.get());
            timings.put("reset_postings_scorer_ns", resetPostingsScorerTimeNs.get());
            timings.put("posting_visit_ns", postingVisitTimeNs.get());
            putIfPositive(timings, "doc_id_read_ns", docIdReadTimeNs.get());
            putIfPositive(timings, "query_quantization_ns", queryQuantizationTimeNs.get());
            putIfPositive(timings, "scoring_ns", scoringTimeNs.get());
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
            // Remainder after per-leaf search and merge: rewrite/thread-pool overhead.
            // HNSW applies the filter inside searchLeaf, so that cost is in approximate_search_time_ns.
            long overhead = totalSearchTimeNs - hnswPerLeafSearchTimeNs.get() - mergeTimeNs;
            timings.put("overhead_ns", Math.max(0, overhead));
            hnsw.put("timings", timings);

            map.put("hnsw", hnsw);
        }

        return map;
    }

    private static void putIfPositive(Map<String, Object> map, String key, long ns) {
        if (ns > 0) {
            map.put(key, ns);
        }
    }
}
