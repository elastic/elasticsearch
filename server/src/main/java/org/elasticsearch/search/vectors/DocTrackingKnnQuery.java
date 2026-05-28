/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

class DocTrackingKnnQuery<T extends Query & PostFilterableKnnQuery> extends Query implements PostFilterableKnnQuery {

    /**
     * Single-set mailbox used to bridge the {@link DocTrackingCollectorManager} created lazily by
     * the delegate's {@code getKnnCollectorManager} hook back to this wrapper, where it's drained
     * by {@link #rewrite}. Caller fills {@link #value} from the override; this query reads it
     * after {@code delegate.rewrite} returns.
     */
    static final class Holder<T> {
        T value;
    }

    private final T delegate;
    private final Holder<DocTrackingCollectorManager> collectorManagerHolder;
    private int[] trackedDocs = new int[0];

    DocTrackingKnnQuery(T delegate, Holder<DocTrackingCollectorManager> collectorManagerHolder) {
        this.delegate = delegate;
        this.collectorManagerHolder = collectorManagerHolder;
    }

    @Override
    public String toString(String field) {
        return "DocTrackingKnnQuery(" + delegate + ")";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        delegate.visit(visitor);
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        var rewritten = delegate.rewrite(searcher);
        trackedDocs = collectorManagerHolder.value.getTrackedDocs();
        return rewritten;
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null
            && obj.getClass() == DocTrackingKnnQuery.class
            && Objects.equals(delegate, ((DocTrackingKnnQuery<?>) obj).delegate);
    }

    @Override
    public int hashCode() {
        return 31 * classHash() + delegate.hashCode();
    }

    @Override
    public Query createRetryQuery(IndexReader reader, int[] excludedDocs, int[] seedDocs, int remainingK) {
        return delegate.createRetryQuery(reader, excludedDocs, seedDocs, remainingK);
    }

    @Override
    public Query createPostFilterDelegate(float filterSelectivity) {
        throw new IllegalStateException("[createPostFilterDelegate] should not be called directly on a DocTrackingKnnQuery");
    }

    @Override
    public Query createFallbackQuery(IndexReader reader, int[] excludedDocs, int remainingK) {
        // The augmented fallback is invoked on the outer inner query (which keeps the original
        // filter), not on a post-filter delegate wrapper that drops it.
        throw new IllegalStateException("[createFallbackQuery] should not be called directly on a DocTrackingKnnQuery");
    }

    @Override
    public int countTotalVectors(List<LeafReaderContext> leaves) throws IOException {
        return delegate.countTotalVectors(leaves);
    }

    @Override
    public long totalVectorOps() {
        return delegate.totalVectorOps();
    }

    @Override
    public int k() {
        return delegate.k();
    }

    @Override
    public int numCands() {
        return delegate.numCands();
    }

    public int[] getTrackedDocs() {
        return trackedDocs;
    }
}
