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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TimeLimitingKnnCollectorManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;

public class ESKnnFloatVectorQuery extends KnnFloatVectorQuery implements QueryProfilerProvider {
    private final int kParam;
    private long vectorOpsCount;
    private final boolean earlyTermination;
    private KnnSearchProfileData profileData;
    private String quantization;
    private boolean profilingSuppressed;

    public ESKnnFloatVectorQuery(String field, float[] target, int k, int numCands, Query filter, KnnSearchStrategy strategy) {
        this(field, target, k, numCands, filter, strategy, false);
    }

    public ESKnnFloatVectorQuery(
        String field,
        float[] target,
        int k,
        int numCands,
        Query filter,
        KnnSearchStrategy strategy,
        boolean earlyTermination
    ) {
        super(field, target, numCands, filter, strategy);
        this.kParam = k;
        this.earlyTermination = earlyTermination;
    }

    @Override
    public void enableProfiling() {
        profileData = new KnnSearchProfileData();
        profileData.setAlgorithmType("hnsw");
        profileData.setQuantization(quantization);
    }

    @Override
    public void setQuantization(String quantization) {
        this.quantization = quantization;
    }

    @Override
    public void setProfilingSuppressed(boolean suppressed) {
        this.profilingSuppressed = suppressed;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        // Self-enable when a profiler is attached to the searcher, so profiling works in both the DFS and
        // query phases without an explicit enableProfiling() call. Suppressed when driven by PostFilterKnnQuery.
        QueryProfiler profiler = QueryProfilerProvider.activeProfiler(indexSearcher);
        if (profiler != null && profilingSuppressed == false && profileData == null) {
            enableProfiling();
        }
        if (profileData != null) {
            profileData.setHnswQueryParams(kParam, getK(), getFilter() != null);
        }
        long start = profileData != null ? System.nanoTime() : 0;
        Query result = super.rewrite(indexSearcher);
        if (profileData != null) {
            profileData.setTotalSearchTimeNs(System.nanoTime() - start);
        }
        // Self-publish at the end of rewrite. ContextIndexSearcher invokes this query's rewrite() exactly
        // once (it returns a terminal KnnScoreDocQuery), matching the existing assumption that the search
        // runs once here.
        if (profiler != null && profilingSuppressed == false) {
            profile(profiler);
        }
        return result;
    }

    @Override
    protected TopDocs searchLeaf(LeafReaderContext ctx, Weight filterWeight, TimeLimitingKnnCollectorManager cm) throws IOException {
        long start = profileData != null ? System.nanoTime() : 0;
        TopDocs result = super.searchLeaf(ctx, filterWeight, cm);
        if (profileData != null) {
            // totalHits.value() is KnnCollector.visitedCount() — the number of HNSW graph nodes visited
            profileData.addHnswLeafSearch(System.nanoTime() - start, result.totalHits.value(), result.scoreDocs.length);
        }
        return result;
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
        long start = profileData != null ? System.nanoTime() : 0;
        TopDocs topK = TopDocs.merge(kParam, perLeafResults);
        if (profileData != null) {
            profileData.setMergeTimeNs(System.nanoTime() - start);
            profileData.setEarlyTerminated(topK.totalHits.relation() == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
        }
        vectorOpsCount = topK.totalHits.value();
        return topK;
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        queryProfiler.addVectorOpsCount(vectorOpsCount);
        if (profileData != null) {
            queryProfiler.setKnnProfileBreakdown(profileData.toMap());
        }
    }

    public int kParam() {
        return kParam;
    }

    public KnnSearchStrategy getStrategy() {
        return searchStrategy;
    }

    @Override
    protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
        KnnCollectorManager knnCollectorManager = super.getKnnCollectorManager(k, searcher);
        return earlyTermination ? PatienceCollectorManager.wrap(knnCollectorManager) : knnCollectorManager;
    }
}
