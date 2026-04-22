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
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.search.vectors.KnnSearchBuilder.NUM_CANDS_LIMIT;

public class ESKnnFloatVectorQuery extends KnnFloatVectorQuery implements QueryProfilerProvider, PostFilterableKnnQuery {
    private final int kParam;
    private final int numCandsParam;
    private long vectorOpsCount;
    private final boolean earlyTermination;
    private final int[] seenDocs;

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
        int[] seenDocs
    ) {
        super(field, target, numCands, filter, strategy);
        this.kParam = k;
        this.numCandsParam = numCands;
        this.earlyTermination = earlyTermination;
        this.seenDocs = seenDocs;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        var maybePostFilter = maybeRewriteAsPostFilter(indexSearcher, filter, kParam, field, null);
        return maybePostFilter == null ? super.rewrite(indexSearcher) : maybePostFilter;
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
        TopDocs topK = TopDocs.merge(kParam, perLeafResults);
        vectorOpsCount = topK.totalHits.value();
        return topK;
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        queryProfiler.addVectorOpsCount(vectorOpsCount);
    }

    @Override
    public Query createInnerQuery(IndexReader reader, int[] seenDocs) {
        var excludeDocsFilter = new ExcludeDocsQuery(seenDocs, reader);
        return new ESKnnFloatVectorQuery(
            field,
            getTargetCopy(),
            kParam,
            numCandsParam,
            excludeDocsFilter,
            searchStrategy,
            earlyTermination,
            seenDocs
        );
    }

    @Override
    public PostFilterableKnnQuery createPostFilterDelegate(float filterSelectivity) {
        int scaledK = (int) Math.max(NUM_CANDS_LIMIT, Math.ceil(kParam / filterSelectivity));
        int scaledNumCands = (int) Math.max(NUM_CANDS_LIMIT, Math.ceil((double) numCandsParam / filterSelectivity));
        return new ESKnnFloatVectorQuery(field, getTargetCopy(), scaledK, scaledNumCands, null, searchStrategy, earlyTermination, null);
    }

    @Override
    public long vectorOpsCount() {
        return vectorOpsCount;
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

    // --- Accessors ---

    public int kParam() {
        return kParam;
    }

    public KnnSearchStrategy getStrategy() {
        return searchStrategy;
    }

    @Override
    protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
        var base = super.getKnnCollectorManager(k, searcher);
        if (seenDocs != null && seenDocs.length > 0) {
            base = new SeededRetryCollectorManager(base, seenDocs, field);
        }
        return earlyTermination ? PatienceCollectorManager.wrap(base) : base;
    }
}
