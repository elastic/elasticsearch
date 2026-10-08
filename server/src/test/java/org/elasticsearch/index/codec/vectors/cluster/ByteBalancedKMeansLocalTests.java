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
import java.util.List;

/**
 * Byte-specific balanced KMeans tests. Ensures byte clustering always gets exercised.
 */
public class ByteBalancedKMeansLocalTests extends AbstractBalancedKMeansLocalTestCase<byte[]> {

    @Override
    protected CentroidOps<byte[]> centroidOps() {
        return CentroidOps.BYTE;
    }

    @Override
    protected ClusteringVectorValues<byte[]> generateData(int nSamples, int nDims, int nClusters) {
        return KMeansTestData.generateByteData(nSamples, nDims, nClusters);
    }

    @Override
    protected ClusteringVectorValues<byte[]> generateZeroData(int nVectors, int dims) {
        List<byte[]> vectors = new ArrayList<>(nVectors);
        for (int i = 0; i < nVectors; i++) {
            vectors.add(new byte[dims]);
        }
        return KMeansByteVectorValues.build(vectors, null, dims);
    }

    @Override
    protected ClusteringVectorValues<byte[]> buildEmptyVectors(int dims) {
        return KMeansByteVectorValues.build(List.of(), null, dims);
    }

    @Override
    protected void assertCentroidsAreZero(byte[][] centroids) {
        for (byte[] centroid : centroids) {
            for (byte v : centroid) {
                assertEquals(0, v);
            }
        }
    }
}
