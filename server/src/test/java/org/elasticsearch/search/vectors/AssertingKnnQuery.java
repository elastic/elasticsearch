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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

class AssertingKnnQuery extends KnnFloatVectorQuery implements PostFilterableKnnQuery {

    /**
     * Mutable record of which {@code PostFilterableKnnQuery} hooks fired across the lifetime of a
     * single {@code  PostFilterKnnQuery} rewrite and with what arguments.
     */
    static final class PostFilterMeta {
        private int postFilterDelegateCalls;
        private float postFilterDelegateSelectivity = Float.NaN;
        private int retryCalls;
        private int[] retryExcludedDocs;
        private int[] retrySeedDocs;
        private int retryRemainingK = -1;
        private int fallbackCalls;
        private int[] fallbackExcludedDocs;
        private int fallbackRemainingK = -1;

        void recordPostFilterDelegate(float selectivity) {
            postFilterDelegateCalls++;
            postFilterDelegateSelectivity = selectivity;
        }

        void recordRetry(int[] excluded, int[] seedDocs, int remainingK) {
            retryCalls++;
            retryExcludedDocs = excluded.clone();
            retrySeedDocs = seedDocs.clone();
            retryRemainingK = remainingK;
        }

        void recordFallback(int[] excluded, int remainingK) {
            fallbackCalls++;
            fallbackExcludedDocs = excluded.clone();
            fallbackRemainingK = remainingK;
        }

        int postFilterDelegateCalls() {
            return postFilterDelegateCalls;
        }

        float postFilterDelegateSelectivity() {
            return postFilterDelegateSelectivity;
        }

        int retryCalls() {
            return retryCalls;
        }

        int[] retryExcludedDocs() {
            return retryExcludedDocs;
        }

        int[] retrySeedDocs() {
            return retrySeedDocs;
        }

        int retryRemainingK() {
            return retryRemainingK;
        }

        int fallbackCalls() {
            return fallbackCalls;
        }

        int[] fallbackExcludedDocs() {
            return fallbackExcludedDocs;
        }

        int fallbackRemainingK() {
            return fallbackRemainingK;
        }
    }

    private final int kParam;
    private final int numCandsParam;
    private final float postFilterScale;
    private final PostFilterMeta postFilterMeta;
    private long vectorOpsCount;

    AssertingKnnQuery(String field, float[] target, int k, int numCands, Query filter, float postFilterScale) {
        this(field, target, k, numCands, filter, postFilterScale, new PostFilterMeta());
    }

    private AssertingKnnQuery(
        String field,
        float[] target,
        int k,
        int numCands,
        Query filter,
        float postFilterScale,
        PostFilterMeta postFilterMeta
    ) {
        super(field, target, numCands, filter);
        this.kParam = k;
        this.numCandsParam = numCands;
        this.postFilterScale = postFilterScale;
        this.postFilterMeta = postFilterMeta;
    }

    PostFilterMeta postFilterMeta() {
        return postFilterMeta;
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
        TopDocs topK = TopDocs.merge(kParam, perLeafResults);
        vectorOpsCount = topK.totalHits.value();
        return topK;
    }

    @Override
    public Query createPostFilterDelegate(float filterSelectivity) {
        postFilterMeta.recordPostFilterDelegate(filterSelectivity);
        int scaledK = Math.max(1, (int) Math.ceil(kParam * postFilterScale));
        return new AssertingKnnQuery(field, target, scaledK, numCandsParam, null, postFilterScale, postFilterMeta) {
            final AtomicReference<DocTrackingCollectorManager> collectorManager = new AtomicReference<>();

            @Override
            protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
                DocTrackingCollectorManager existing = collectorManager.get();
                if (existing != null) {
                    return existing;
                }
                KnnCollectorManager base = super.getKnnCollectorManager(k, searcher);
                DocTrackingCollectorManager wrapped = DocTrackingCollectorManager.wrap(base, searcher.getIndexReader().leaves().size());
                collectorManager.compareAndSet(null, wrapped);
                return collectorManager.get();
            }

            @Override
            public int[] getTrackedDocs() {
                DocTrackingCollectorManager mgr = collectorManager.get();
                return mgr == null ? new int[0] : mgr.getTrackedDocs();
            }
        };
    }

    @Override
    public Query createRetryQuery(IndexReader reader, int[] excluded, int[] seedDocs, int remainingK) {
        assert isSorted(excluded) : "excludedDocs must be sorted: " + Arrays.toString(excluded);
        assert isSorted(seedDocs) : "seedDocs must be sorted: " + Arrays.toString(seedDocs);
        assert remainingK > 0 : "remainingK must be > 0, got " + remainingK;
        postFilterMeta.recordRetry(excluded, seedDocs, remainingK);
        Query excludeFilter = excluded.length > 0 ? new ExcludeDocsQuery(excluded, reader) : null;
        return new AssertingKnnQuery(field, target, remainingK, numCandsParam, excludeFilter, postFilterScale, postFilterMeta);
    }

    @Override
    public Query createFallbackQuery(IndexReader reader, int[] excluded, int remainingK) {
        assert isSorted(excluded) : "excludedDocs must be sorted: " + Arrays.toString(excluded);
        assert remainingK > 0 : "remainingK must be > 0, got " + remainingK;
        postFilterMeta.recordFallback(excluded, remainingK);
        Query augmented = KnnQueryUtils.augmentFilter(getFilter(), excluded, reader);
        return new AssertingKnnQuery(field, target, remainingK, numCandsParam, augmented, postFilterScale, postFilterMeta);
    }

    @Override
    public int countTotalVectors(List<LeafReaderContext> leaves) throws IOException {
        int n = 0;
        for (LeafReaderContext leaf : leaves) {
            FloatVectorValues fvv = leaf.reader().getFloatVectorValues(field);
            if (fvv != null) {
                n += fvv.size();
            }
        }
        return n;
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

    @Override
    public String toString(String f) {
        return "AssertingKnnQuery[k=" + kParam + ", field=" + field + "]";
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    private static boolean isSorted(int[] arr) {
        for (int i = 1; i < arr.length; i++) {
            if (arr[i] < arr[i - 1]) return false;
        }
        return true;
    }
}
