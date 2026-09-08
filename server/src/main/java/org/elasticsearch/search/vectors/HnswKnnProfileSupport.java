/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.profile.query.QueryProfiler;

/**
 * Shared {@code knn_profile} collection for the HNSW-backed ES vector queries.
 * <p>
 * The four of them ({@link ESKnnFloatVectorQuery}, {@link ESKnnByteVectorQuery} and the two
 * {@code ESDiversifyingChildren*KnnVectorQuery} variants) extend different Lucene base classes, so they
 * cannot share a superclass; each holds one of these instead and delegates its collection to it.
 * <p>
 * An instance exists only while a query is being profiled: it is created by {@code enableProfiling()},
 * either because a caller asked for profiling explicitly or because the query found a
 * {@link QueryProfiler} on the {@link org.elasticsearch.search.internal.ContextIndexSearcher} during
 * {@code rewrite()} (which is what makes profiling behave identically in the DFS and query phases). A
 * non-profiled search never allocates one, so all it costs those searches is a null check per call site.
 */
final class HnswKnnProfileSupport {

    private final KnnSearchProfileData profileData = new KnnSearchProfileData();

    /**
     * Captures the query-level labels that are known before the search runs. {@code quantization} is
     * snapshotted here rather than tracked, so it must be set on the query before profiling is enabled -
     * which is the case on every path, since the mapper labels a query as it builds it and the per-round
     * delegates {@link PostFilterKnnQuery} builds are labelled before they are enabled.
     */
    HnswKnnProfileSupport(String field, @Nullable String quantization, int k, int numCandidates, boolean hasFilter) {
        profileData.setAlgorithm(KnnSearchProfileData.Algorithm.HNSW);
        profileData.setField(field);
        profileData.setQuantization(quantization);
        profileData.setHnswQueryParams(k, numCandidates, hasFilter);
    }

    void recordTotalSearchTime(long startNs) {
        profileData.setTotalSearchTimeNs(System.nanoTime() - startNs);
    }

    void recordLeafSearch(LeafReaderContext ctx, long startNs, TopDocs leafResults) {
        // totalHits.value() is KnnCollector.visitedCount() - the number of HNSW graph nodes visited
        profileData.addHnswLeafSearch(ctx, System.nanoTime() - startNs, leafResults.totalHits.value(), leafResults.scoreDocs.length);
    }

    void recordMerge(long startNs, TopDocs topK) {
        profileData.setMergeTimeNs(System.nanoTime() - startNs);
        profileData.setEarlyTerminated(topK.totalHits.relation() == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
    }

    /**
     * Publishes the query's vector-op count and breakdown at the tail of {@code rewrite()}.
     * <p>
     * {@link org.apache.lucene.search.Query#rewrite} carries no once-only contract - {@code IndexSearcher}
     * rewrites to a fixpoint and nesting constructs can re-enter - but a duplicate breakdown cannot result:
     * the caller passes a non-null {@code profiler} only on the rewrite that created this instance, and it
     * is created once.
     */
    void publish(@Nullable QueryProfiler profiler, QueryProfilerProvider query) {
        if (profiler != null) {
            query.profile(profiler);
        }
    }

    /** Attaches the collected breakdown to {@code profiler}. */
    void addBreakdownTo(QueryProfiler profiler) {
        profiler.addKnnProfileBreakdown(profileData.toMap());
    }
}
