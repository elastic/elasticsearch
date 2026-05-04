/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.elasticsearch.index.codec.vectors.cluster.NeighborQueue;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class DocTrackingCollectorManager implements KnnCollectorManager {

    record DocTrackingMeta(DocTrackingCollector collector, int docBase) {}

    public static final int MAX_DOCS_TRACKED = 1000;

    private final KnnCollectorManager delegate;
    private final Map<Integer, DocTrackingMeta> collectors;
    private final int docsTracked;

    DocTrackingCollectorManager(KnnCollectorManager delegate, int docsTracked) {
        this.delegate = delegate;
        this.docsTracked = Math.min(docsTracked, MAX_DOCS_TRACKED);
        this.collectors = new ConcurrentHashMap<>();
    }

    @Override
    public KnnCollector newCollector(int visitLimit, KnnSearchStrategy searchStrategy, LeafReaderContext ctx) throws IOException {
        var baseCollector = delegate.newCollector(visitLimit, searchStrategy, ctx);
        var docTrackingCollector = new DocTrackingCollector(baseCollector, docsTracked);
        collectors.put(ctx.ord, new DocTrackingMeta(docTrackingCollector, ctx.docBase));
        return docTrackingCollector;
    }

    @Override
    public KnnCollector newOptimisticCollector(int visitLimit, KnnSearchStrategy searchStrategy, LeafReaderContext ctx, int k)
        throws IOException {
        var baseCollector = delegate.newOptimisticCollector(visitLimit, searchStrategy, ctx, k);
        var docTrackingCollector = new DocTrackingCollector(baseCollector, docsTracked);
        collectors.put(ctx.ord, new DocTrackingMeta(docTrackingCollector, ctx.docBase));
        return docTrackingCollector;
    }

    @Override
    public boolean isOptimistic() {
        return delegate.isOptimistic();
    }

    public int[] getTrackedDocs() {
        NeighborQueue mergeQueue = new NeighborQueue(docsTracked, false);
        for (DocTrackingMeta meta : collectors.values()) {
            DocTrackingCollector collector = meta.collector();
            int docBase = meta.docBase();
            while (collector.trackedDocsSize() > 0) {
                float score = collector.topTrackedScore();
                int docId = collector.popTrackedDoc() + docBase;
                mergeQueue.insertWithOverflow(docId, score);
            }
        }
        int size = mergeQueue.size();
        if (size == 0) {
            return new int[0];
        }
        int[] result = new int[size];
        for (int i = size - 1; i >= 0; i--) {
            result[i] = mergeQueue.pop();
        }
        return result;
    }

    public static DocTrackingCollectorManager wrap(KnnCollectorManager delegate, int docsTracked) {
        return new DocTrackingCollectorManager(delegate, docsTracked);
    }
}
