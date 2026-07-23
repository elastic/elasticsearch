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
import org.elasticsearch.common.util.concurrent.AtomicArray;

import java.io.IOException;

class DocTrackingCollectorManager implements KnnCollectorManager {

    record DocTrackingMeta(DocTrackingCollector collector, int docBase) {}

    private final KnnCollectorManager delegate;
    private final AtomicArray<DocTrackingMeta> collectors;

    public static final int MAX_DOCS_TRACKED = 1000;

    DocTrackingCollectorManager(KnnCollectorManager delegate, int numLeaves) {
        this.delegate = delegate;
        this.collectors = new AtomicArray<>(numLeaves);
    }

    @Override
    public KnnCollector newCollector(int visitLimit, KnnSearchStrategy searchStrategy, LeafReaderContext ctx) throws IOException {
        var baseCollector = delegate.newCollector(visitLimit, searchStrategy, ctx);
        var docTrackingCollector = new DocTrackingCollector(baseCollector);
        collectors.set(ctx.ord, new DocTrackingMeta(docTrackingCollector, ctx.docBase));
        return docTrackingCollector;
    }

    @Override
    public KnnCollector newOptimisticCollector(int visitLimit, KnnSearchStrategy searchStrategy, LeafReaderContext ctx, int k)
        throws IOException {
        var baseCollector = delegate.newOptimisticCollector(visitLimit, searchStrategy, ctx, k);
        var docTrackingCollector = new DocTrackingCollector(baseCollector);
        collectors.set(ctx.ord, new DocTrackingMeta(docTrackingCollector, ctx.docBase));
        return docTrackingCollector;
    }

    @Override
    public boolean isOptimistic() {
        return delegate.isOptimistic();
    }

    public int[] getTrackedDocs() {
        var metas = collectors.asList();
        int total = 0;
        for (DocTrackingMeta meta : metas) {
            int[] leafTrackedDocs = meta.collector().getTrackedDocs();
            if (leafTrackedDocs != null) {
                total += leafTrackedDocs.length;
            }
        }
        if (total == 0) {
            return new int[0];
        }
        int[] result = new int[total];
        int pos = 0;
        for (DocTrackingMeta meta : metas) {
            int[] leafTrackedDocs = meta.collector().getTrackedDocs();
            if (leafTrackedDocs == null || leafTrackedDocs.length == 0) {
                continue;
            }
            int docBase = meta.docBase();
            for (int leafDoc : leafTrackedDocs) {
                result[pos++] = leafDoc + docBase;
            }
        }
        return result;
    }

    public static DocTrackingCollectorManager wrap(KnnCollectorManager delegate, int numLeaves) {
        return new DocTrackingCollectorManager(delegate, numLeaves);
    }
}
