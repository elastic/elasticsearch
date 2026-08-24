/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TimeLimitingKnnCollectorManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.List;

public class ESKnnFloatVectorQuery extends KnnFloatVectorQuery implements QueryProfilerProvider, PostFilterableKnnQuery {
    private final int kParam;
    private final int numCandsParam;
    private long vectorOpsCount;
    private final boolean earlyTermination;
    private KnnSearchProfileData profileData;
    private String quantization;
    private boolean profilingSuppressed;
    private final int[][] seedDocsPerLeaf;
    private List<LeafReaderContext> leaves;
    private TopDocs[] rawPerLeafResults;

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
        this(field, target, k, numCands, filter, strategy, earlyTermination, null);
    }

    ESKnnFloatVectorQuery(
        String field,
        float[] target,
        int k,
        int numCands,
        Query filter,
        KnnSearchStrategy strategy,
        boolean earlyTermination,
        int[][] seedDocsPerLeaf
    ) {
        super(field, target, numCands, filter, strategy);
        this.kParam = k;
        this.numCandsParam = numCands;
        this.earlyTermination = earlyTermination;
        this.seedDocsPerLeaf = seedDocsPerLeaf;
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
        this.leaves = indexSearcher.getIndexReader().leaves();
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
        this.rawPerLeafResults = perLeafResults;
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

    @Override
    public Query createRetryQuery(IndexReader reader, int[] excludedDocs, int[][] seedDocsPerLeaf, int remainingK) {
        Query filter = excludedDocs != null && excludedDocs.length > 0 ? new ExcludeDocsQuery(excludedDocs, reader) : null;
        return new ESKnnFloatVectorQuery(
            field,
            getTargetCopy(),
            remainingK,
            numCandsParam,
            filter,
            searchStrategy,
            earlyTermination,
            seedDocsPerLeaf
        );
    }

    @Override
    public Query createPostFilterDelegate(float filterSelectivity) {
        var params = PostFilterableKnnQuery.computeOversampledParams(kParam, numCandsParam, filterSelectivity);
        return new ESKnnFloatVectorQuery(
            field,
            getTargetCopy(),
            params.scaledK(),
            params.scaledNumCands(),
            null,
            searchStrategy,
            earlyTermination,
            null
        );
    }

    @Override
    public ScoreDoc[][] getPostFilterCandidates() {
        return rawPerLeafResults == null
            ? new ScoreDoc[leaves.size()][]
            : PostFilterableKnnQuery.buildPerLeafCandidates(rawPerLeafResults, leaves);
    }

    @Override
    public int countTotalVectors(List<LeafReaderContext> leaves) throws IOException {
        int totalVectors = 0;
        for (LeafReaderContext leaf : leaves) {
            FloatVectorValues fvv = leaf.reader().getFloatVectorValues(field);
            if (fvv != null) {
                totalVectors += fvv.size();
            }
        }
        return totalVectors;
    }

    @Override
    public long totalVectorOps() {
        return vectorOpsCount;
    }

    @Override
    public int k() {
        return kParam;
    }

    @Override
    public int numCands() {
        return numCandsParam;
    }

    public int kParam() {
        return kParam;
    }

    public KnnSearchStrategy getStrategy() {
        return searchStrategy;
    }

    @Override
    protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
        KnnCollectorManager base = super.getKnnCollectorManager(k, searcher);
        if (PostFilterableKnnQuery.hasSeeds(seedDocsPerLeaf)) {
            base = new SeededRetryCollectorManager(base, seedDocsPerLeaf, field);
        }
        return earlyTermination ? PatienceCollectorManager.wrap(base) : base;
    }
}
