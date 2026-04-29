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
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.search.vectors.KnnSearchBuilder.NUM_CANDS_LIMIT;

public class ESKnnFloatVectorQuery extends KnnFloatVectorQuery implements QueryProfilerProvider, PostFilterableKnnQuery {
    private final int kParam;
    private final int numCandsParam;
    private long vectorOpsCount;
    private final boolean earlyTermination;
    private final int[] seedDocs;

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
        int[] seedDocs
    ) {
        super(field, target, numCands, filter, strategy);
        this.kParam = k;
        this.numCandsParam = numCands;
        this.earlyTermination = earlyTermination;
        this.seedDocs = seedDocs;
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
    public Query createRetryQuery(IndexReader reader, int[] excludedDocs, int[] seedDocs, int requestK, int requestNumCands) {
        Query filter = excludedDocs != null && excludedDocs.length > 0 ? new ExcludeDocsQuery(excludedDocs, reader) : null;
        AtomicReference<DocTrackingCollectorManager> knnCollectorManagerRef = new AtomicReference<>();
        var knnQuery = new ESKnnFloatVectorQuery(
            field,
            target,
            requestK,
            requestNumCands,
            filter,
            searchStrategy,
            earlyTermination,
            seedDocs
        ) {
            @Override
            protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
                // super already applies SeededRetryCollectorManager (if seedDocs set) and PatienceCollectorManager
                // (if earlyTermination); we only need to layer DocTracking on top.
                var base = super.getKnnCollectorManager(k, searcher);
                DocTrackingCollectorManager knnCollectorManager = DocTrackingCollectorManager.wrap(base, k);
                knnCollectorManagerRef.set(knnCollectorManager);
                return knnCollectorManager;
            }
        };
        return new DocTrackingKnnQuery<>(knnQuery, knnCollectorManagerRef);
    }

    @Override
    public Query createPostFilterDelegate(float filterSelectivity) {
        // Round-1 oversample: max of a 20% floor and the binomial-variance approximation
        // m ≈ (k + Z · √(k · (1 - p) / p)) / p, which makes P(X ≥ k) ≈ Φ(Z).
        double zMargin = POST_FILTER_OVERSAMPLE_Z_SCORE * Math.sqrt(kParam * (1.0f - filterSelectivity) / filterSelectivity);
        int scaledK = (int) Math.min(
            NUM_CANDS_LIMIT,
            Math.max(Math.ceil(kParam * POST_FILTER_OVERSAMPLE_FLOOR), Math.ceil((kParam + zMargin) / filterSelectivity))
        );
        // Maintain the configured numCands/k ratio so HNSW exploration scales with K.
        int scaledNumCands = (int) Math.min(NUM_CANDS_LIMIT, Math.ceil((double) scaledK * numCandsParam / kParam));
        AtomicReference<DocTrackingCollectorManager> knnCollectorManagerRef = new AtomicReference<>();
        var knnQuery = new ESKnnFloatVectorQuery(field, target, scaledK, scaledNumCands, null, searchStrategy, earlyTermination, null) {
            @Override
            protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
                var base = super.getKnnCollectorManager(k, searcher);
                DocTrackingCollectorManager knnCollectorManager = DocTrackingCollectorManager.wrap(base, k);
                knnCollectorManagerRef.set(knnCollectorManager);
                return knnCollectorManager;
            }
        };
        return new DocTrackingKnnQuery<>(knnQuery, knnCollectorManagerRef);
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
        var base = super.getKnnCollectorManager(k, searcher);
        if (seedDocs != null && seedDocs.length > 0) {
            base = new SeededRetryCollectorManager(base, seedDocs, field);
        }
        return earlyTermination ? PatienceCollectorManager.wrap(base) : base;
    }
}
