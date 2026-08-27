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
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
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

    /**
     * The approximate search algorithm a breakdown describes. It selects which algorithm-specific
     * section {@link #toMap()} emits, so it is an enum rather than a free-form string: a typo would
     * otherwise silently drop the whole section from the profile output.
     */
    public enum Algorithm {
        HNSW("hnsw"),
        IVF("ivf");

        private final String name;

        Algorithm(String name) {
            this.name = name;
        }

        /** The value surfaced as {@code knn_profile.algorithm}. */
        public String getName() {
            return name;
        }
    }

    /** Marks a {@link LeafSearch} counter that does not apply to the algorithm that ran. */
    static final int NOT_APPLICABLE = -1;

    /**
     * One per-leaf search. Segment metadata is resolved lazily in {@link #toMap()} rather than inside
     * the leaf task, so the file-stat'ing {@code sizeInBytes} lookup and the vector-values open are not
     * charged to the search being profiled.
     *
     * @param ord          leaf ordinal, used to emit {@code segments} in a stable order regardless of the
     *                     order in which the parallel leaf tasks happened to finish
     * @param ctx          the leaf that was searched, or {@code null} for records added directly by tests
     * @param visitRatio   the IVF visit ratio actually used for this leaf, or {@link Float#NaN} for HNSW
     * @param nodesVisited HNSW graph nodes visited, or {@link #NOT_APPLICABLE} for IVF
     * @param resultsFound results this leaf contributed before the merge, or {@link #NOT_APPLICABLE} for IVF
     */
    private record LeafSearch(
        int ord,
        @Nullable LeafReaderContext ctx,
        long searchTimeNs,
        float visitRatio,
        long nodesVisited,
        int resultsFound
    ) {}

    // --- query-level (set once, after all tasks) ---
    private volatile long filterTimeNs;
    private volatile long totalSearchTimeNs;
    private volatile long mergeTimeNs;
    private volatile boolean earlyTerminated;
    private volatile Algorithm algorithm;
    private volatile String quantization;
    private volatile String field;
    private final AtomicReference<String> scorer = new AtomicReference<>();

    /**
     * Per-leaf searches, appended concurrently by the parallel leaf tasks. Every per-leaf aggregate
     * ({@code segments_searched}, {@code approximate_search_time_ns}, HNSW node counts and min/max/avg
     * leaf times, IVF visit ratios) is derived from this one collection in {@link #toMap()}, so the
     * aggregates cannot drift from the per-segment detail.
     */
    private final ConcurrentLinkedQueue<LeafSearch> leafSearches = new ConcurrentLinkedQueue<>();
    private final AtomicInteger syntheticLeafOrd = new AtomicInteger();

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

    // --- HNSW query parameters ---
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

    public void setEarlyTerminated(boolean terminated) {
        this.earlyTerminated = terminated;
    }

    public void setAlgorithm(Algorithm algorithm) {
        this.algorithm = algorithm;
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

    /**
     * Records an IVF leaf search. {@code visitRatio} is the ratio the codec actually used for this leaf,
     * which may differ from the requested one when it is computed dynamically; pass {@link Float#NaN}
     * when it is not known.
     */
    public void addIvfLeafSearch(LeafReaderContext ctx, long searchTimeNs, float visitRatio) {
        leafSearches.add(new LeafSearch(ctx.ord, ctx, searchTimeNs, visitRatio, NOT_APPLICABLE, NOT_APPLICABLE));
    }

    /** Records an HNSW leaf (graph) search along with what that leaf visited and returned. */
    public void addHnswLeafSearch(LeafReaderContext ctx, long searchTimeNs, long nodesVisited, int resultsFound) {
        leafSearches.add(new LeafSearch(ctx.ord, ctx, searchTimeNs, Float.NaN, nodesVisited, resultsFound));
    }

    /**
     * Records an IVF leaf search with no reader context, so the aggregates can be exercised without building
     * an index. Package-private: production callers always have a {@link LeafReaderContext}.
     */
    void addIvfLeafSearchForTest(long searchTimeNs, float visitRatio) {
        leafSearches.add(
            new LeafSearch(syntheticLeafOrd.getAndIncrement(), null, searchTimeNs, visitRatio, NOT_APPLICABLE, NOT_APPLICABLE)
        );
    }

    /**
     * Records an HNSW leaf search with no reader context, so the aggregates can be exercised without building
     * an index. Package-private: production callers always have a {@link LeafReaderContext}.
     */
    void addHnswLeafSearchForTest(long searchTimeNs, long nodesVisited, int resultsFound) {
        leafSearches.add(new LeafSearch(syntheticLeafOrd.getAndIncrement(), null, searchTimeNs, Float.NaN, nodesVisited, resultsFound));
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
        List<LeafSearch> leaves = new ArrayList<>(leafSearches);
        // The parallel leaf tasks append in completion order; emit by leaf ordinal so repeated runs of the
        // same search produce comparable output.
        leaves.sort(Comparator.comparingInt(LeafSearch::ord));

        long approximateSearchTimeNs = 0;
        for (LeafSearch leaf : leaves) {
            approximateSearchTimeNs += leaf.searchTimeNs();
        }

        Map<String, Object> map = new LinkedHashMap<>();
        if (algorithm != null) {
            map.put("algorithm", algorithm.getName());
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
        map.put("segments_searched", leaves.size());
        map.put("early_terminated", earlyTerminated);
        if (filterTimeNs > 0) {
            map.put("filter_time_ns", filterTimeNs);
        }
        map.put("approximate_search_time_ns", approximateSearchTimeNs);
        map.put("merge_time_ns", mergeTimeNs);
        if (leaves.isEmpty() == false) {
            map.put("segments", segmentsToList(leaves));
        }

        if (algorithm == Algorithm.IVF) {
            map.put("ivf", ivfSection(leaves, approximateSearchTimeNs));
        } else if (algorithm == Algorithm.HNSW) {
            map.put("hnsw", hnswSection(leaves, approximateSearchTimeNs));
        }

        return map;
    }

    /**
     * Wall-clock time not attributed to any reported phase: per-leaf work outside the measured search
     * window, plus rewrite and thread-pool overhead. For IVF the per-leaf window covers the codec's
     * {@code searchNearestVectors} call only, so the collector drain, the doc-id dedup and any
     * preconditioner transform land here.
     * <p>
     * {@code filterTimeNs} is only ever set by IVF, which rewrites the filter up front. HNSW applies its
     * filter inside the per-leaf search, so that cost already sits in {@code approximateSearchTimeNs} and
     * subtracting a zero here is a no-op.
     * <p>
     * Clamped at zero: {@code approximateSearchTimeNs} is a sum over leaves, so when the leaves were
     * searched in parallel it can legitimately exceed the wall-clock total.
     */
    private long overheadNs(long approximateSearchTimeNs) {
        return Math.max(0, totalSearchTimeNs - filterTimeNs - approximateSearchTimeNs - mergeTimeNs);
    }

    private Map<String, Object> ivfSection(List<LeafSearch> leaves, long approximateSearchTimeNs) {
        Map<String, Object> ivf = new LinkedHashMap<>();
        float min = Float.POSITIVE_INFINITY;
        float max = Float.NEGATIVE_INFINITY;
        for (LeafSearch leaf : leaves) {
            if (Float.isNaN(leaf.visitRatio()) == false) {
                min = Math.min(min, leaf.visitRatio());
                max = Math.max(max, leaf.visitRatio());
            }
        }
        if (max != Float.NEGATIVE_INFINITY) {
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
        timings.put("overhead_ns", overheadNs(approximateSearchTimeNs));
        ivf.put("timings", timings);
        return ivf;
    }

    private Map<String, Object> hnswSection(List<LeafSearch> leaves, long approximateSearchTimeNs) {
        Map<String, Object> hnsw = new LinkedHashMap<>();
        hnsw.put("k", hnswK);
        hnsw.put("num_candidates", hnswNumCandidates);
        hnsw.put("has_filter", hnswHasFilter);

        long nodesVisited = 0;
        long resultsFound = 0;
        long maxLeafSearchNs = 0;
        long minLeafSearchNs = Long.MAX_VALUE;
        for (LeafSearch leaf : leaves) {
            nodesVisited += leaf.nodesVisited();
            resultsFound += leaf.resultsFound();
            maxLeafSearchNs = Math.max(maxLeafSearchNs, leaf.searchTimeNs());
            minLeafSearchNs = Math.min(minLeafSearchNs, leaf.searchTimeNs());
        }
        hnsw.put("nodes_visited", nodesVisited);
        hnsw.put("results_found_before_merge", resultsFound);

        Map<String, Object> timings = new LinkedHashMap<>();
        if (leaves.isEmpty() == false) {
            timings.put("avg_leaf_search_ns", approximateSearchTimeNs / leaves.size());
            timings.put("max_leaf_search_ns", maxLeafSearchNs);
            timings.put("min_leaf_search_ns", minLeafSearchNs);
        }
        timings.put("overhead_ns", overheadNs(approximateSearchTimeNs));
        hnsw.put("timings", timings);
        return hnsw;
    }

    /**
     * Materialises the per-segment breakdown: Lucene segment name, live doc count, on-disk segment size,
     * vector count/bytes for the searched field, and that leaf's search time (plus the HNSW-only or
     * IVF-only counters when they apply).
     */
    private List<Map<String, Object>> segmentsToList(List<LeafSearch> leaves) {
        List<Map<String, Object>> segments = new ArrayList<>(leaves.size());
        for (LeafSearch leaf : leaves) {
            Map<String, Object> seg = new LinkedHashMap<>();
            SegmentReader sr = leaf.ctx() == null ? null : Lucene.tryUnwrapSegmentReader(leaf.ctx().reader());
            if (sr != null) {
                seg.put("name", sr.getSegmentName());
                try {
                    seg.put("size_in_bytes", sr.getSegmentInfo().sizeInBytes());
                } catch (IOException e) {
                    // optional; omit rather than fail a profiled search
                }
            } else {
                seg.put("name", Integer.toString(leaf.ord()));
            }
            if (leaf.ctx() != null) {
                seg.put("doc_count", leaf.ctx().reader().numDocs());
                addVectorStats(seg, leaf.ctx());
            }
            seg.put("search_time_ns", leaf.searchTimeNs());
            if (Float.isNaN(leaf.visitRatio()) == false) {
                seg.put("visit_ratio_used", leaf.visitRatio());
            }
            if (leaf.nodesVisited() != NOT_APPLICABLE) {
                seg.put("nodes_visited", leaf.nodesVisited());
            }
            if (leaf.resultsFound() != NOT_APPLICABLE) {
                seg.put("results_found", leaf.resultsFound());
            }
            segments.add(seg);
        }
        return segments;
    }

    private void addVectorStats(Map<String, Object> seg, LeafReaderContext ctx) {
        if (field == null) {
            return;
        }
        try {
            FloatVectorValues floatValues = ctx.reader().getFloatVectorValues(field);
            if (floatValues != null) {
                seg.put("vector_count", floatValues.size());
                seg.put("vector_bytes", (long) floatValues.size() * floatValues.getVectorByteLength());
                return;
            }
            ByteVectorValues byteValues = ctx.reader().getByteVectorValues(field);
            if (byteValues != null) {
                seg.put("vector_count", byteValues.size());
                seg.put("vector_bytes", (long) byteValues.size() * byteValues.getVectorByteLength());
            }
        } catch (IOException e) {
            // optional; omit rather than fail a profiled search
        }
    }

    private static void putIfPositive(Map<String, Object> map, String key, long ns) {
        if (ns > 0) {
            map.put(key, ns);
        }
    }
}
