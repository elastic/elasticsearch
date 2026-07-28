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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.knn.KnnSearchStrategy.Hnsw;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Composition-based test harness that implements {@link PostFilterableKnnQuery} for both float
 * and byte vector types. Delegates actual kNN search to real ES query classes
 * ({@link ESKnnFloatVectorQuery}, {@link ESKnnByteVectorQuery},
 * {@link ESDiversifyingChildrenFloatKnnVectorQuery}, or
 * {@link ESDiversifyingChildrenByteKnnVectorQuery}) and tracks which post-filter
 * hooks fired via {@link PostFilterMeta}.
 */
public class AssertingKnnQuery extends Query implements PostFilterableKnnQuery {

    public enum VectorType {
        FLOAT,
        BYTE,
        DIVERSIFYING_FLOAT,
        DIVERSIFYING_BYTE
    }

    static final class PostFilterMeta {
        private int postFilterDelegateCalls;
        private float postFilterDelegateSelectivity = Float.NaN;
        private int retryCalls;
        private int[] retryExcludedDocs;
        private int[][] retrySeedDocs;
        private int retryRemainingK = -1;

        void recordPostFilterDelegate(float selectivity) {
            postFilterDelegateCalls++;
            postFilterDelegateSelectivity = selectivity;
        }

        void recordRetry(int[] excluded, int[][] seedDocsPerLeaf, int remainingK) {
            retryCalls++;
            retryExcludedDocs = excluded.clone();
            retrySeedDocs = seedDocsPerLeaf.clone();
            retryRemainingK = remainingK;
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

        int[][] retrySeedDocs() {
            return retrySeedDocs;
        }

        int retryRemainingK() {
            return retryRemainingK;
        }
    }

    private final VectorType vectorType;
    private final String field;
    private final float[] target;
    private final int kParam;
    private final int numCandsParam;
    private final Query filter;
    private final float postFilterScale;
    private final PostFilterMeta postFilterMeta;
    private final BitSetProducer parentsFilter;

    private PostFilterableKnnQuery innerDelegate;

    AssertingKnnQuery(VectorType vectorType, String field, float[] target, int k, int numCands, Query filter, float postFilterScale) {
        this(vectorType, field, target, k, numCands, filter, postFilterScale, new PostFilterMeta(), null);
    }

    AssertingKnnQuery(
        VectorType vectorType,
        String field,
        float[] target,
        int k,
        int numCands,
        Query filter,
        float postFilterScale,
        BitSetProducer parentsFilter
    ) {
        this(vectorType, field, target, k, numCands, filter, postFilterScale, new PostFilterMeta(), parentsFilter);
    }

    private AssertingKnnQuery(
        VectorType vectorType,
        String field,
        float[] target,
        int k,
        int numCands,
        Query filter,
        float postFilterScale,
        PostFilterMeta postFilterMeta,
        BitSetProducer parentsFilter
    ) {
        this.vectorType = vectorType;
        this.field = field;
        this.target = target;
        this.kParam = k;
        this.numCandsParam = numCands;
        this.filter = filter;
        this.postFilterScale = postFilterScale;
        this.postFilterMeta = postFilterMeta;
        this.parentsFilter = parentsFilter;
    }

    PostFilterMeta postFilterMeta() {
        return postFilterMeta;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Query inner = createInnerQuery();
        this.innerDelegate = (PostFilterableKnnQuery) inner;
        return inner.rewrite(searcher);
    }

    private Query createInnerQuery() {
        return switch (vectorType) {
            case FLOAT -> new ESKnnFloatVectorQuery(field, target, kParam, numCandsParam, filter, Hnsw.DEFAULT);
            case BYTE -> new ESKnnByteVectorQuery(field, toByteArray(target), kParam, numCandsParam, filter, Hnsw.DEFAULT);
            case DIVERSIFYING_FLOAT -> new ESDiversifyingChildrenFloatKnnVectorQuery(
                field,
                target,
                filter,
                kParam,
                numCandsParam,
                parentsFilter,
                Hnsw.DEFAULT
            );
            case DIVERSIFYING_BYTE -> new ESDiversifyingChildrenByteKnnVectorQuery(
                field,
                toByteArray(target),
                filter,
                kParam,
                numCandsParam,
                parentsFilter,
                Hnsw.DEFAULT
            );
        };
    }

    @Override
    public ScoreDoc[][] getPostFilterCandidates() {
        return innerDelegate != null ? innerDelegate.getPostFilterCandidates() : null;
    }

    @Override
    public Query createPostFilterDelegate(float filterSelectivity) {
        postFilterMeta.recordPostFilterDelegate(filterSelectivity);
        int scaledK = Math.max(1, (int) Math.ceil(kParam * postFilterScale));
        return new AssertingKnnQuery(
            vectorType,
            field,
            target,
            scaledK,
            numCandsParam,
            null,
            postFilterScale,
            postFilterMeta,
            parentsFilter
        );
    }

    @Override
    public Query createRetryQuery(IndexReader reader, int[] excluded, int[][] seedDocsPerLeaf, int remainingK) {
        assert isSorted(excluded) : "excludedDocs must be sorted: " + Arrays.toString(excluded);
        assert allSorted(seedDocsPerLeaf) : "each leaf's seedDocs must be sorted: " + Arrays.deepToString(seedDocsPerLeaf);
        assert remainingK > 0 : "remainingK must be > 0, got " + remainingK;
        postFilterMeta.recordRetry(excluded, seedDocsPerLeaf, remainingK);
        Query excludeFilter = excluded.length > 0 ? new ExcludeDocsQuery(excluded, reader) : null;
        return new AssertingKnnQuery(
            vectorType,
            field,
            target,
            remainingK,
            numCandsParam,
            excludeFilter,
            postFilterScale,
            postFilterMeta,
            parentsFilter
        );
    }

    @Override
    public int countTotalVectors(List<LeafReaderContext> leaves) throws IOException {
        int n = 0;
        for (LeafReaderContext leaf : leaves) {
            switch (vectorType) {
                case FLOAT, DIVERSIFYING_FLOAT -> {
                    FloatVectorValues fvv = leaf.reader().getFloatVectorValues(field);
                    if (fvv != null) n += fvv.size();
                }
                case BYTE, DIVERSIFYING_BYTE -> {
                    ByteVectorValues bvv = leaf.reader().getByteVectorValues(field);
                    if (bvv != null) n += bvv.size();
                }
            }
        }
        return n;
    }

    @Override
    public long totalVectorOps() {
        return innerDelegate != null ? innerDelegate.totalVectorOps() : 0;
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
        return "AssertingKnnQuery[type=" + vectorType + ", k=" + kParam + ", field=" + field + "]";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    private static byte[] toByteArray(float[] floats) {
        byte[] bytes = new byte[floats.length];
        for (int i = 0; i < floats.length; i++) {
            bytes[i] = (byte) floats[i];
        }
        return bytes;
    }

    private static boolean isSorted(int[] arr) {
        for (int i = 1; i < arr.length; i++) {
            if (arr[i] < arr[i - 1]) return false;
        }
        return true;
    }

    private static boolean allSorted(int[][] perLeaf) {
        for (int[] leaf : perLeaf) {
            if (leaf != null && isSorted(leaf) == false) return false;
        }
        return true;
    }
}
