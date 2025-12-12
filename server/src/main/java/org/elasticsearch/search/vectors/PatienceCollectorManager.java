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

import java.io.IOException;

/**
 * This is a decorator for the {@link KnnCollectorManager} that early terminates the wrapped {@link KnnCollector}
 * based on a saturation threshold and a patience factor. It is designed
 * to improve the efficiency of approximate nearest neighbor (KNN) searches by monitoring queue saturation
 * during the search process.
 * This applies a patience-based logic to both optimistic and regular KNN collectors.
 * The saturation threshold defines the percentage of saturation at which the collector's patience is
 * tested for termination.
 */
class PatienceCollectorManager implements KnnCollectorManager {

    private final KnnCollectorManager knnCollectorManager;

    PatienceCollectorManager(KnnCollectorManager knnCollectorManager) {
        this.knnCollectorManager = knnCollectorManager;
    }

    static KnnCollectorManager wrap(KnnCollectorManager knnCollectorManager) {
        return new PatienceCollectorManager(knnCollectorManager);
    }

    @Override
    public KnnCollector newCollector(int visitLimit, KnnSearchStrategy searchStrategy, LeafReaderContext ctx) throws IOException {
        return new AdaptiveHnswQueueSaturationCollector(
            knnCollectorManager.newCollector(visitLimit, searchStrategy, ctx)
        );
    }

    @Override
    public KnnCollector newOptimisticCollector(int visitLimit, KnnSearchStrategy searchStrategy, LeafReaderContext ctx, int k)
        throws IOException {
        if (knnCollectorManager.isOptimistic()) {
            return new AdaptiveHnswQueueSaturationCollector(knnCollectorManager.newOptimisticCollector(visitLimit, searchStrategy, ctx, k));
        } else {
            return null;
        }
    }

    @Override
    public boolean isOptimistic() {
        return knnCollectorManager.isOptimistic();
    }
}
