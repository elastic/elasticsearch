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
    private final int[][] seedDocsPerLeaf;
    private String quantization;
    /** Non-null only while this query is being profiled; see {@link HnswKnnProfileSupport}. */
    private HnswKnnProfileSupport profiling;
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
        profiling = new HnswKnnProfileSupport(field, quantization, kParam, getK(), getFilter() != null);
    }

    @Override
    public void setQuantization(String quantization) {
        assert profiling == null : "quantization must be set before profiling is enabled, it is snapshotted when it is";
        this.quantization = quantization;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        this.leaves = indexSearcher.getIndexReader().leaves();
        // Enabling ourselves here is also what earns us the right to publish: if profiling was already on, a
        // caller enabled it and will harvest us itself, so profiler stays null and we only collect.
        QueryProfiler profiler = profiling == null ? QueryProfilerProvider.activeProfiler(indexSearcher) : null;
        if (profiler != null) {
            enableProfiling();
        }
        long start = profiling == null ? 0 : System.nanoTime();
        Query result = super.rewrite(indexSearcher);
        if (profiling != null) {
            profiling.recordTotalSearchTime(start);
            profiling.publish(profiler, this);
        }
        return result;
    }

    @Override
    protected TopDocs searchLeaf(LeafReaderContext ctx, Weight filterWeight, TimeLimitingKnnCollectorManager cm) throws IOException {
        long start = profiling == null ? 0 : System.nanoTime();
        TopDocs result = super.searchLeaf(ctx, filterWeight, cm);
        if (profiling != null) {
            profiling.recordLeafSearch(ctx, start, result);
        }
        return result;
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
        this.rawPerLeafResults = perLeafResults;
        long start = profiling == null ? 0 : System.nanoTime();
        TopDocs topK = TopDocs.merge(kParam, perLeafResults);
        if (profiling != null) {
            profiling.recordMerge(start, topK);
        }
        vectorOpsCount = topK.totalHits.value();
        return topK;
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        queryProfiler.addVectorOpsCount(vectorOpsCount);
        if (profiling != null) {
            profiling.addBreakdownTo(queryProfiler);
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
