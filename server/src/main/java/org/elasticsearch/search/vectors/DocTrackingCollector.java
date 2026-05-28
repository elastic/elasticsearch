/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.hnsw.NeighborQueue;

public class DocTrackingCollector implements KnnCollector {

    private final KnnCollector delegate;
    private final NeighborQueue trackedDocs;

    public DocTrackingCollector(KnnCollector delegate, int maxCapacity) {
        this.delegate = delegate;
        this.trackedDocs = new NeighborQueue(maxCapacity, false);
    }

    @Override
    public boolean earlyTerminated() {
        return delegate.earlyTerminated();
    }

    @Override
    public void incVisitedCount(int count) {
        delegate.incVisitedCount(count);
    }

    @Override
    public long visitedCount() {
        return delegate.visitedCount();
    }

    @Override
    public long visitLimit() {
        return delegate.visitLimit();
    }

    @Override
    public int k() {
        return delegate.k();
    }

    @Override
    public boolean collect(int docId, float similarity) {
        trackedDocs.insertWithOverflow(docId, similarity);
        return delegate.collect(docId, similarity);
    }

    @Override
    public float minCompetitiveSimilarity() {
        return delegate.minCompetitiveSimilarity();
    }

    @Override
    public TopDocs topDocs() {
        return delegate.topDocs();
    }

    @Override
    public KnnSearchStrategy getSearchStrategy() {
        return delegate.getSearchStrategy();
    }

    public int trackedDocsSize() {
        return trackedDocs.size();
    }

    public float topTrackedScore() {
        return trackedDocs.topScore();
    }

    public int popTrackedDoc() {
        return trackedDocs.pop();
    }
}
