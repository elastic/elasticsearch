/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.search.TaskExecutor;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans.NO_SOAR_ASSIGNMENT;

/**
 * Abstract base class for HierarchicalKMeans tests, parameterized by vector type.
 */
public abstract class AbstractHierarchicalKMeansTestCase<V> extends ESTestCase {

    protected abstract CentroidOps<V> centroidOps();

    protected abstract ClusteringVectorValues<V> generateData(int nSamples, int nDims, int nClusters);

    public void testHKmeans() throws IOException {
        int nClusters = random().nextInt(1, 10);
        int nVectors = random().nextInt(nClusters, nClusters * 200);
        int dims = random().nextInt(2, 20);
        int sampleSize = random().nextInt(Math.min(nVectors, 100), nVectors + 1);
        int maxIterations = random().nextInt(1, 100);
        int clustersPerNeighborhood = random().nextInt(2, 512);
        float soarLambda = random().nextFloat(0.5f, 1.5f);
        int targetSize = (int) ((float) nVectors / (float) nClusters);

        CentroidOps<V> ops = centroidOps();
        ClusteringVectorValues<V> vectors = generateData(nVectors, dims, nClusters);

        HierarchicalKMeans<V> hkmeansSerial = HierarchicalKMeans.ofSerial(
            ops,
            dims,
            maxIterations,
            sampleSize,
            clustersPerNeighborhood,
            soarLambda
        );
        var serialResult = hkmeansSerial.cluster(vectors, targetSize);
        assertKMeansResultValid(serialResult, nVectors, nClusters);

        int[] serialClusterSizes = new int[serialResult.centroids().length];
        for (int k : serialResult.assignments()) {
            serialClusterSizes[k]++;
        }

        int numWorker = randomIntBetween(2, 8);
        try (ExecutorService service = Executors.newFixedThreadPool(numWorker)) {
            TaskExecutor executor = new TaskExecutor(service);
            HierarchicalKMeans<V> hkmeansConcurrent = HierarchicalKMeans.ofConcurrent(
                ops,
                dims,
                executor,
                numWorker,
                maxIterations,
                sampleSize,
                clustersPerNeighborhood,
                soarLambda
            );
            var concurrentResult = hkmeansConcurrent.cluster(vectors, targetSize);
            assertKMeansResultValid(concurrentResult, nVectors, nClusters);

            int[] concurrentClusterSizes = new int[concurrentResult.centroids().length];
            for (int k : concurrentResult.assignments()) {
                concurrentClusterSizes[k]++;
            }

            assertEquals(
                clusterSizesStandardDeviation(serialClusterSizes),
                clusterSizesStandardDeviation(concurrentClusterSizes),
                1e-1 * clusterSizesStandardDeviation(serialClusterSizes)
            );
        }
    }

    public void testFewDifferentValues() throws IOException {
        int nVectors = random().nextInt(100, 1000);
        int targetSize = random().nextInt(4, 64);
        int dims = random().nextInt(2, 20);
        int diffValues = randomIntBetween(1, 5);

        CentroidOps<V> ops = centroidOps();
        ClusteringVectorValues<V> vectors = generateFewDistinctData(nVectors, dims, diffValues);

        HierarchicalKMeans<V> hkmeans = HierarchicalKMeans.ofSerial(
            ops,
            dims,
            random().nextInt(1, 100),
            random().nextInt(Math.min(nVectors, 100), nVectors + 1),
            random().nextInt(2, 512),
            random().nextFloat(0.5f, 1.5f)
        );

        var result = hkmeans.cluster(vectors, targetSize);
        assertKMeansResultValid(result, nVectors, -1);
    }

    /**
     * Verify that SOAR assignments never collide with primary assignments after empty clusters
     * are removed. This exercises the neighborhood remapping in removeEmptyClusters: when empty
     * centroids are compacted out and neighbor indices are remapped, no neighbor should be mapped
     * to a vector's own primary centroid.
     *
     * The test creates a dataset with fewer natural clusters than what the algorithm targets,
     * uses a small clustersPerNeighborhood to force neighborhood-aware SOAR, and repeats across
     * random parameters to cover different empty-cluster scenarios.
     */
    public void testSoarAssignmentsValidAfterEmptyClusterRemoval() throws IOException {
        CentroidOps<V> ops = centroidOps();
        for (int trial = 0; trial < 200; trial++) {
            // Use few natural clusters but many vectors, so the algorithm over-partitions
            // and some clusters end up empty after refinement.
            int naturalClusters = randomIntBetween(2, 4);
            int nVectors = randomIntBetween(200, 1000);
            int dims = randomIntBetween(4, 32);

            ClusteringVectorValues<V> vectors = generateData(nVectors, dims, naturalClusters);

            // Small clustersPerNeighborhood ensures neighborhoods are active when centroids > this value
            int clustersPerNeighborhood = 2;
            // Very small target size forces many centroids, maximizing chance of empty clusters
            int targetSize = randomIntBetween(3, 10);
            float soarLambda = randomFloat() * 0.5f + 0.5f;
            // Low maxIterations increases chance of poorly-converged clusters that become empty
            int maxIterations = randomIntBetween(1, 5);

            HierarchicalKMeans<V> hkmeans = HierarchicalKMeans.ofSerial(
                ops,
                dims,
                maxIterations,
                randomIntBetween(50, nVectors),
                clustersPerNeighborhood,
                soarLambda
            );

            var result = hkmeans.cluster(vectors, targetSize);

            int[] assignments = result.assignments();
            int[] soarAssignments = result.soarAssignments();

            if (result.centroids().length > 1 && result.centroids().length < nVectors) {
                assertEquals(nVectors, soarAssignments.length);
                for (int i = 0; i < assignments.length; i++) {
                    int soar = soarAssignments[i];
                    if (soar != NO_SOAR_ASSIGNMENT) {
                        assertNotEquals(
                            "SOAR assignment collides with primary assignment for vector "
                                + i
                                + " (both assigned to centroid "
                                + assignments[i]
                                + ")",
                            assignments[i],
                            soar
                        );
                    }
                }
            }
        }
    }

    protected abstract ClusteringVectorValues<V> generateFewDistinctData(int nVectors, int dims, int diffValues);

    // ---- Helpers ----

    protected static <V> void assertKMeansResultValid(KMeansWithOverspill<V> result, int nVectors, int expectedClusters) {
        V[] centroids = result.centroids();
        int[] assignments = result.assignments();
        int[] soarAssignments = result.soarAssignments();

        if (expectedClusters > 0) {
            assertEquals(Math.min(expectedClusters, nVectors), centroids.length, 25);
        }
        assertTrue("Expected at least 1 centroid", centroids.length >= 1);
        assertEquals(nVectors, assignments.length);

        for (int assignment : assignments) {
            assertTrue(assignment >= 0 && assignment < centroids.length);
        }

        // Verify no empty clusters
        int[] counts = new int[centroids.length];
        for (int a : assignments) {
            counts[a]++;
        }
        for (int count : counts) {
            assertTrue("Empty cluster found", count > 0);
        }
        assertArrayEquals(counts, result.result().clusterCounts());

        if (centroids.length > 1 && centroids.length < nVectors) {
            assertEquals(nVectors, soarAssignments.length);
            // verify no duplicates exist
            for (int i = 0; i < assignments.length; i++) {
                int soarAssignment = soarAssignments[i];
                assertTrue(soarAssignment == NO_SOAR_ASSIGNMENT || (soarAssignment >= 0 && soarAssignment < centroids.length));
                assertNotEquals(assignments[i], soarAssignment);
            }
        } else {
            assertEquals(0, soarAssignments.length);
        }
    }

    static float clusterSizesStandardDeviation(int[] clusterSizes) {
        double avgSize = Arrays.stream(clusterSizes).asDoubleStream().sum() / clusterSizes.length;
        double varSize = Arrays.stream(clusterSizes).asDoubleStream().map(e -> Math.pow(e - avgSize, 2)).sum() / clusterSizes.length;
        return (float) Math.sqrt(varSize);
    }

}
