/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.index.codec.vectors.cluster.KMeansTestData.runKMeans;
import static org.hamcrest.Matchers.containsString;

/**
 * Abstract base class for LloydKMeansLocal tests, parameterized by vector type.
 * Subclasses provide the concrete {@link CentroidOps} and data generation.
 */
public abstract class AbstractLloydKMeansLocalTestCase<V> extends ESTestCase {

    protected abstract CentroidOps<V> centroidOps();

    protected abstract ClusteringVectorValues<V> generateData(int nSamples, int nDims, int nClusters);

    protected abstract ClusteringVectorValues<V> generateZeroData(int nVectors, int dims);

    protected abstract ClusteringVectorValues<V> buildEmptyVectors(int dims);

    protected abstract void assertCentroidsAreZero(V[] centroids);

    public void testIllegalClustersPerNeighborhood() {
        CentroidOps<V> ops = centroidOps();
        KMeansLocal<V> kMeansLocal = new LloydKMeansLocalSerial<>(ops, randomInt(), randomInt(), randomFloat());
        V[] emptyCentroids = ops.newCentroidArrayShallow(0);
        KMeansResult<V> kMeansResult = new KMeansResult<>(emptyCentroids, new int[0]);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> kMeansLocal.cluster(buildEmptyVectors(randomInt(1024)), kMeansResult, randomIntBetween(Integer.MIN_VALUE, 1))
        );
        assertThat(ex.getMessage(), containsString("clustersPerNeighborhood must be at least 2"));
    }

    public void testKMeansNeighbors() throws IOException {
        int nClusters = random().nextInt(1, 10);
        int nVectors = random().nextInt(nClusters * 100, nClusters * 200);
        int dims = random().nextInt(2, 20);
        int sampleSize = random().nextInt(100, nVectors + 1);
        int maxIterations = random().nextInt(0, 100);
        int clustersPerNeighborhood = random().nextInt(2, 512);
        float soarLambda = random().nextFloat(0.5f, 1.5f);

        CentroidOps<V> ops = centroidOps();
        ClusteringVectorValues<V> vectors = generateData(nVectors, dims, nClusters);
        V[] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters, ops);
        runKMeans(vectors, new LloydKMeansLocalSerial<>(ops, sampleSize, maxIterations, soarLambda), centroids);

        int[] assignments = new int[vectors.size()];
        for (int i = 0; i < vectors.size(); i++) {
            float minDist = Float.MAX_VALUE;
            int ord = -1;
            V vec = vectors.vectorValue(i);
            for (int j = 0; j < centroids.length; j++) {
                float dist = ops.squareDistance(vec, centroids[j]);
                if (dist < minDist) {
                    minDist = dist;
                    ord = j;
                }
            }
            assignments[i] = ord;
        }

        KMeansResult<V> kMeansResult = new KMeansResult<>(centroids, assignments);
        KMeansLocal<V> kMeansLocal = new LloydKMeansLocalSerial<>(ops, sampleSize, maxIterations, soarLambda);
        var result = kMeansLocal.cluster(vectors, kMeansResult, clustersPerNeighborhood);

        assertEquals(nClusters, centroids.length);
    }

    public void testKMeansNeighborsAllZero() throws IOException {
        int nClusters = 10;
        int maxIterations = 10;
        int clustersPerNeighborhood = 128;
        float soarLambda = 1.0f;
        int nVectors = 1000;
        int dims = 5;

        CentroidOps<V> ops = centroidOps();
        ClusteringVectorValues<V> vectors = generateZeroData(nVectors, dims);

        V[] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters, ops);
        runKMeans(vectors, new LloydKMeansLocalSerial<>(ops, nVectors, maxIterations, soarLambda), centroids);

        int[] assignments = new int[nVectors];
        for (int i = 0; i < nVectors; i++) {
            float minDist = Float.MAX_VALUE;
            int ord = -1;
            V vec = vectors.vectorValue(i);
            for (int j = 0; j < centroids.length; j++) {
                float dist = ops.squareDistance(vec, centroids[j]);
                if (dist < minDist) {
                    minDist = dist;
                    ord = j;
                }
            }
            assignments[i] = ord;
        }

        KMeansResult<V> kMeansResult = new KMeansResult<>(centroids, assignments);
        KMeansLocal<V> kMeansLocal = new LloydKMeansLocalSerial<>(ops, nVectors, maxIterations, soarLambda);
        kMeansLocal.cluster(vectors, kMeansResult, clustersPerNeighborhood);

        assertEquals(nClusters, centroids.length);
        assertCentroidsAreZero(centroids);
    }

}
