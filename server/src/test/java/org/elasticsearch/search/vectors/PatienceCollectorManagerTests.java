/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.HnswQueueSaturationCollector;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.search.knn.TopKnnCollectorManager;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class PatienceCollectorManagerTests extends ESTestCase {

    public void testEarlyTermination() throws IOException {
        int k = randomIntBetween(1, 10);
        PatienceCollectorManager patienceCollectorManager = new PatienceCollectorManager(new TopKnnCollectorManager(k, null));
        HnswQueueSaturationCollector knnCollector = (HnswQueueSaturationCollector) patienceCollectorManager.newCollector(
            randomIntBetween(100, 1000),
            new KnnSearchStrategy.Hnsw(10),
            null
        );

        for (int i = 0; i < 100; i++) {
            knnCollector.collect(i, 1 - i);
            if (i % 10 == 0) {
                knnCollector.nextCandidate();
            }
        }
        assertTrue(knnCollector.earlyTerminated());
    }
}
