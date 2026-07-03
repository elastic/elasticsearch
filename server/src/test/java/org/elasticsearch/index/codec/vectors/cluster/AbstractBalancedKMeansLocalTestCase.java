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

import static org.hamcrest.Matchers.containsString;

/**
 * Abstract base class for BalancedKMeansLocal tests, parameterized by vector type.
 * Subclasses provide the concrete {@link CentroidOps} and data generation.
 */
public abstract class AbstractBalancedKMeansLocalTestCase<V> extends ESTestCase {

    protected abstract CentroidOps<V> centroidOps();

    protected abstract ClusteringVectorValues<V> generateData(int nSamples, int nDims, int nClusters);

    protected abstract ClusteringVectorValues<V> generateZeroData(int nVectors, int dims);

    protected abstract ClusteringVectorValues<V> buildEmptyVectors(int dims);

    protected abstract void assertCentroidsAreZero(V[] centroids);

    public void testIllegalClustersPerNeighborhood() {
        CentroidOps<V> ops = centroidOps();
        KMeansLocal<V> kMeansLocal = new BalancedOTKMeansLocalSerial<>(ops, randomInt(), randomInt());
        V[] emptyCentroids = ops.newCentroidArrayShallow(0);
        KMeansIntermediate<V> kMeansIntermediate = new KMeansIntermediate<>(emptyCentroids, new int[0], i -> i);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> kMeansLocal.cluster(
                buildEmptyVectors(randomInt(1024)),
                kMeansIntermediate,
                randomIntBetween(Integer.MIN_VALUE, 1),
                randomFloat()
            )
        );
        assertThat(ex.getMessage(), containsString("clustersPerNeighborhood must be at least 2"));
    }

    public void testBalancedOTKMeansLocal() throws IOException {
        int nClusters = randomIntBetween(2, 10);
        int nVectors = nClusters * randomIntBetween(50, 200);
        int dims = randomIntBetween(4, 32);
        int sampleSize = randomIntBetween(100, nVectors);
        int maxIterations = randomIntBetween(2, 20);

        CentroidOps<V> ops = centroidOps();
        ClusteringVectorValues<V> vectors = generateData(nVectors, dims, nClusters);
        V[] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters, ops);
        int[] assignments = new int[nVectors];
        KMeansIntermediate<V> kMeansIntermediate = new KMeansIntermediate<>(centroids, assignments);

        KMeansLocal<V> kMeansLocal = new BalancedOTKMeansLocalSerial<>(ops, sampleSize, maxIterations);
        kMeansLocal.cluster(vectors, kMeansIntermediate, nClusters, -1f);

        for (int a : kMeansIntermediate.assignments()) {
            assertTrue("Invalid assignment: " + a, a >= 0 && a < centroids.length);
        }
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
        int sampleSize = nVectors;

        V[] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters, ops);
        BalancedOTKMeansLocal.cluster(vectors, ops, centroids, sampleSize, maxIterations);

        int[] assignments = new int[nVectors];
        int[] assignmentOrdinals = new int[nVectors];
        for (int i = 0; i < nVectors; i++) {
            assignments[i] = 0;
            assignmentOrdinals[i] = i;
        }

        KMeansIntermediate<V> kMeansIntermediate = new KMeansIntermediate<>(centroids, assignments, i -> assignmentOrdinals[i]);
        KMeansLocal<V> kMeansLocal = new BalancedOTKMeansLocalSerial<>(ops, sampleSize, maxIterations);
        var result = kMeansLocal.cluster(vectors, kMeansIntermediate, clustersPerNeighborhood, soarLambda);

        assertEquals(nClusters, centroids.length);
        assertNotNull(result.soarAssignments());
        assertCentroidsAreZero(centroids);
    }

    public void testKMeansNeighbors() throws IOException {
        int nClusters = random().nextInt(1, 10);
        int nVectors = random().nextInt(nClusters * 100, nClusters * 200);
        int dims = random().nextInt(2, 20);
        int sampleSize = random().nextInt(100, nVectors + 1);
        int maxIterations = random().nextInt(0, 100);
        // We require clustersPerNeighborhood > nClusters so that neighborhoods are not used in BalancedOTKMeansLocalSerial
        // (this path is not used in production, just for the test).
        int clustersPerNeighborhood = random().nextInt(11, 512);
        float soarLambda = random().nextFloat(0.5f, 1.5f);

        CentroidOps<V> ops = centroidOps();
        ClusteringVectorValues<V> vectors = generateData(nVectors, dims, nClusters);
        V[] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters, ops);
        LloydKMeansLocal.cluster(vectors, ops, centroids, sampleSize, maxIterations);

        int[] assignments = new int[vectors.size()];
        int[] assignmentOrdinals = new int[vectors.size()];
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
            assignmentOrdinals[i] = i;
        }

        KMeansIntermediate<V> kMeansIntermediate = new KMeansIntermediate<>(centroids, assignments, i -> assignmentOrdinals[i]);
        KMeansLocal<V> kMeansLocal = new BalancedOTKMeansLocalSerial<>(ops, sampleSize, maxIterations);
        var result = kMeansLocal.cluster(vectors, kMeansIntermediate, clustersPerNeighborhood, soarLambda);

        assertEquals(nClusters, centroids.length);
        assertNotNull(result.soarAssignments());
    }

}
