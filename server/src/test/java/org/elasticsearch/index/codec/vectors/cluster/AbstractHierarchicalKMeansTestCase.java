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

    /** Wraps an array of centroids into a ClusteringVectorValues view for testing. */
    protected abstract ClusteringVectorValues<V> wrapAsView(V[] centroids, int dim);

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
        KMeansResult<V> serialResult = hkmeansSerial.cluster(vectors, targetSize);
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
            KMeansResult<V> concurrentResult = hkmeansConcurrent.cluster(vectors, targetSize);
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

        KMeansResult<V> result = hkmeans.cluster(vectors, targetSize);
        assertKMeansResultValid(result, nVectors, -1);
    }

    protected abstract ClusteringVectorValues<V> generateFewDistinctData(int nVectors, int dims, int diffValues);

    public void testClusterByInsertion() throws IOException {
        int nClusters = random().nextInt(2, 8);
        int nVectors = random().nextInt(nClusters * 10, nClusters * 200);
        int dims = random().nextInt(2, 20);
        int targetSize = (int) ((float) nVectors / nClusters);

        CentroidOps<V> ops = centroidOps();
        ClusteringVectorValues<V> vectors = generateData(nVectors, dims, nClusters);

        // First, do a full cluster to get "initial centroids" (simulating a dominant segment's priors)
        HierarchicalKMeans<V> hkmeans = HierarchicalKMeans.ofSerial(ops, dims);
        KMeansResult<V> fullResult = hkmeans.cluster(vectors, targetSize);
        assertKMeansResultValid(fullResult, nVectors, nClusters);

        // Now use those centroids as initial seeds for clusterByInsertion
        ClusteringVectorValues<V> priorView = wrapAsView(fullResult.centroids(), dims);
        KMeansResult<V> insertionResult = hkmeans.clusterByInsertion(vectors, priorView, targetSize);
        assertKMeansResultValid(insertionResult, nVectors, nClusters);
    }

    public void testClusterByConcatenation() throws IOException {
        int nClusters = random().nextInt(2, 8);
        int nVectors = random().nextInt(nClusters * 10, nClusters * 200);
        int dims = random().nextInt(2, 20);
        int targetSize = (int) ((float) nVectors / nClusters);

        CentroidOps<V> ops = centroidOps();
        ClusteringVectorValues<V> vectors = generateData(nVectors, dims, nClusters);

        // Full cluster to get "prior centroids" simulating concatenated priors from multiple segments
        HierarchicalKMeans<V> hkmeans = HierarchicalKMeans.ofSerial(ops, dims);
        KMeansResult<V> fullResult = hkmeans.cluster(vectors, targetSize);
        assertKMeansResultValid(fullResult, nVectors, nClusters);

        int[] clusterSizes = fullResult.clusterCounts();
        ClusteringVectorValues<V> priorView = wrapAsView(fullResult.centroids(), dims);

        KMeansResult<V> concatResult = hkmeans.clusterByConcatenation(vectors, priorView, clusterSizes, nVectors, targetSize);
        assertKMeansResultValid(concatResult, nVectors, nClusters);
    }

    // ---- Helpers ----

    protected static <V> void assertKMeansResultValid(KMeansResult<V> result, int nVectors, int expectedClusters) {
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
        assertArrayEquals(counts, result.clusterCounts());

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
