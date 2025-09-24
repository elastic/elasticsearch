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
import org.apache.lucene.search.HnswQueueSaturationCollector;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;

import java.io.IOException;

public class PatienceCollectorManager implements KnnCollectorManager {
    private  static final double DEFAULT_SATURATION_THRESHOLD = 0.995;

    private final KnnCollectorManager knnCollectorManager;
    private final int patience;
    private final double saturationThreshold;

    PatienceCollectorManager(KnnCollectorManager knnCollectorManager, int patience, double saturationThreshold) {
        this.knnCollectorManager = knnCollectorManager;
        this.patience = patience;
        this.saturationThreshold = saturationThreshold;
    }

    public static KnnCollectorManager wrap(KnnCollectorManager knnCollectorManager, int k) {
        return new PatienceCollectorManager(knnCollectorManager, Math.max(7, (int) (k * 0.3)), DEFAULT_SATURATION_THRESHOLD);
    }

    @Override
    public KnnCollector newCollector(int visitLimit, KnnSearchStrategy searchStrategy, LeafReaderContext ctx) throws IOException {
        return new HnswQueueSaturationCollector(
            knnCollectorManager.newCollector(visitLimit, searchStrategy, ctx),
            saturationThreshold,
            patience
        );
    }

    @Override
    public KnnCollector newOptimisticCollector(int visitLimit, KnnSearchStrategy searchStrategy, LeafReaderContext ctx, int k)
        throws IOException {
        if (knnCollectorManager.isOptimistic()) {
            return new HnswQueueSaturationCollector(
                knnCollectorManager.newOptimisticCollector(visitLimit, searchStrategy, ctx, k),
                saturationThreshold,
                patience
            );
        } else {
            return null;
        }
    }

    @Override
    public boolean isOptimistic() {
        return knnCollectorManager.isOptimistic();
    }
}
