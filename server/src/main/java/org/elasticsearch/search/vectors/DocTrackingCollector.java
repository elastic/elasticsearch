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

public class DocTrackingCollector implements BulkKnnCollector {

    private final KnnCollector delegate;
    private int[] trackedDocs;

    public DocTrackingCollector(KnnCollector delegate) {
        this.delegate = delegate;
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
        return delegate.collect(docId, similarity);
    }

    @Override
    public float minCompetitiveSimilarity() {
        return delegate.minCompetitiveSimilarity();
    }

    @Override
    public TopDocs topDocs() {
        var topDocs = delegate.topDocs();
        trackedDocs = new int[topDocs.scoreDocs.length];
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            trackedDocs[i] = topDocs.scoreDocs[i].doc;
        }
        return topDocs;
    }

    @Override
    public KnnSearchStrategy getSearchStrategy() {
        return delegate.getSearchStrategy();
    }

    public int[] getTrackedDocs() {
        return trackedDocs;
    }

    @Override
    public int bulkCollect(int[] docs, float[] scores, int count, float bestScore) {
        if (delegate instanceof BulkKnnCollector bulkDelegate) {
            return bulkDelegate.bulkCollect(docs, scores, count, bestScore);
        }
        int accepted = 0;
        for (int i = 0; i < count; i++) {
            if (delegate.collect(docs[i], scores[i])) {
                accepted++;
            }
        }
        return accepted;
    }

    @Override
    public TopDocs unsortedTopK() {
        TopDocs topDocs = (delegate instanceof BulkKnnCollector bulkDelegate) ? bulkDelegate.unsortedTopK() : delegate.topDocs();
        if (topDocs == null) {
            trackedDocs = new int[0];
            return null;
        }
        trackedDocs = new int[topDocs.scoreDocs.length];
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            trackedDocs[i] = topDocs.scoreDocs[i].doc;
        }
        return topDocs;
    }
}
