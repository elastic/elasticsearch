/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.vectors.cluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FloatHierarchicalKMeansTests extends AbstractHierarchicalKMeansTestCase<float[]> {

    @Override
    protected CentroidOps<float[]> centroidOps() {
        return CentroidOps.FLOAT;
    }

    @Override
    protected ClusteringVectorValues<float[]> generateData(int nSamples, int nDims, int nClusters) {
        return KMeansTestData.generateFloatData(nSamples, nDims, nClusters);
    }

    @Override
    protected ClusteringVectorValues<float[]> wrapAsView(float[][] centroids, int dim) {
        return KMeansFloatVectorValues.build(Arrays.asList(centroids), null, dim);
    }

    @Override
    protected ClusteringVectorValues<float[]> generateFewDistinctData(int nVectors, int dims, int diffValues) {
        float[][] values = new float[diffValues][dims];
        for (int i = 0; i < diffValues; i++) {
            for (int j = 0; j < dims; j++) {
                values[i][j] = random().nextFloat();
            }
        }
        List<float[]> vectorList = new ArrayList<>(nVectors);
        for (int i = 0; i < nVectors; i++) {
            vectorList.add(values[random().nextInt(diffValues)]);
        }
        return KMeansFloatVectorValues.build(vectorList, null, dims);
    }
}
