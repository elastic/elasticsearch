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
import java.util.concurrent.atomic.AtomicReference;

class DocTrackingKnnQuery<T extends Query & PostFilterableKnnQuery> extends Query implements PostFilterableKnnQuery {

    private final T delegate;
    private final AtomicReference<DocTrackingCollectorManager> collectorManagerRef;
    private int[] trackedDocs = new int[0];

    public DocTrackingKnnQuery(T delegate, AtomicReference<DocTrackingCollectorManager> collectorManagerRef) {
        this.delegate = delegate;
        this.collectorManagerRef = collectorManagerRef;
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
        trackedDocs = collectorManagerRef.get().getTrackedDocs();
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
