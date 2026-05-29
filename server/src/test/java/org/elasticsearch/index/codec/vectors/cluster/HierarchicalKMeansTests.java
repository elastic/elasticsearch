/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public License
 * v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

public class HierarchicalKMeansTests extends ESTestCase {

    public void testNumClustersForTargetSize() {
        assertEquals(32, HierarchicalKMeans.numClustersForTargetSize(8192, 256));
        assertEquals(2, HierarchicalKMeans.numClustersForTargetSize(400, 256));
    }

    public void testWarmStartMatchesColdStartClusterCount() throws IOException {
        float[][] rows = {
            { 1f, 0f, 0f, 0f },
            { 0.9f, 0.1f, 0f, 0f },
            { 0.8f, 0.2f, 0f, 0f },
            { 0.7f, 0.3f, 0f, 0f },
            { 0.6f, 0.4f, 0f, 0f },
            { 0.5f, 0.5f, 0f, 0f },
            { 0.4f, 0.6f, 0f, 0f },
            { 0.3f, 0.7f, 0f, 0f },
            { 0.2f, 0.8f, 0f, 0f },
            { 0.1f, 0.9f, 0f, 0f },
            { 0f, 1f, 0f, 0f },
            { -0.1f, 0.9f, 0f, 0f } };
        KMeansFloatVectorValues vectors = KMeansFloatVectorValues.build(List.of(rows), null, 4);
        HierarchicalKMeans kmeans = HierarchicalKMeans.ofSerial(4);
        KMeansResult cold = kmeans.cluster(vectors, 4);
        KMeansResult warm = kmeans.cluster(vectors, 4, cold.centroids());
        assertEquals(cold.centroids().length, warm.centroids().length);
        assertEquals(cold.assignments().length, warm.assignments().length);
    }
}
