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
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans.NO_SOAR_ASSIGNMENT;

public class HierarchicalKMeansTests extends ESTestCase {

    public void testHKmeans() throws IOException {
        int nClusters = random().nextInt(1, 10);
        int nVectors = random().nextInt(nClusters, nClusters * 200);
        int dims = random().nextInt(2, 20);
        int sampleSize = random().nextInt(Math.min(nVectors, 100), nVectors + 1);
        int maxIterations = random().nextInt(1, 100);
        int clustersPerNeighborhood = random().nextInt(2, 512);
        float soarLambda = random().nextFloat(0.5f, 1.5f);
        int targetSize = (int) ((float) nVectors / (float) nClusters);

        boolean useByte = randomBoolean();
        if (useByte) {
            KMeansByteVectorValues vectors = generateByteData(nVectors, dims, nClusters);
            HierarchicalKMeans<byte[]> hkmeansSerial = HierarchicalKMeans.ofSerial(
                CentroidOps.BYTE,
                dims,
                maxIterations,
                sampleSize,
                clustersPerNeighborhood,
                soarLambda
            );
            KMeansResult<byte[]> serialResult = hkmeansSerial.cluster(vectors, targetSize);
            assertKMeansResultValid(serialResult, nVectors, nClusters);

            int[] serialClusterSizes = new int[serialResult.centroids().length];
            for (int k : serialResult.assignments()) {
                serialClusterSizes[k]++;
            }

            int numWorker = randomIntBetween(2, 8);
            try (ExecutorService service = Executors.newFixedThreadPool(numWorker)) {
                TaskExecutor executor = new TaskExecutor(service);
                HierarchicalKMeans<byte[]> hkmeansConcurrent = HierarchicalKMeans.ofConcurrent(
                    CentroidOps.BYTE,
                    dims,
                    executor,
                    numWorker,
                    maxIterations,
                    sampleSize,
                    clustersPerNeighborhood,
                    soarLambda
                );
                KMeansResult<byte[]> concurrentResult = hkmeansConcurrent.cluster(vectors, targetSize);
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
        } else {
            KMeansFloatVectorValues vectors = generateFloatData(nVectors, dims, nClusters);
            HierarchicalKMeans<float[]> hkmeansSerial = HierarchicalKMeans.ofSerial(
                CentroidOps.FLOAT,
                dims,
                maxIterations,
                sampleSize,
                clustersPerNeighborhood,
                soarLambda
            );
            KMeansResult<float[]> serialResult = hkmeansSerial.cluster(vectors, targetSize);
            assertKMeansResultValid(serialResult, nVectors, nClusters);

            int[] serialClusterSizes = new int[serialResult.centroids().length];
            for (int k : serialResult.assignments()) {
                serialClusterSizes[k]++;
            }

            int numWorker = randomIntBetween(2, 8);
            try (ExecutorService service = Executors.newFixedThreadPool(numWorker)) {
                TaskExecutor executor = new TaskExecutor(service);
                HierarchicalKMeans<float[]> hkmeansConcurrent = HierarchicalKMeans.ofConcurrent(
                    CentroidOps.FLOAT,
                    dims,
                    executor,
                    numWorker,
                    maxIterations,
                    sampleSize,
                    clustersPerNeighborhood,
                    soarLambda
                );
                KMeansResult<float[]> concurrentResult = hkmeansConcurrent.cluster(vectors, targetSize);
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
    }

    public void testFewDifferentValues() throws IOException {
        int nVectors = random().nextInt(100, 1000);
        int targetSize = random().nextInt(4, 64);
        int dims = random().nextInt(2, 20);
        int diffValues = randomIntBetween(1, 5);

        boolean useByte = randomBoolean();
        if (useByte) {
            byte[][] values = new byte[diffValues][dims];
            for (int i = 0; i < diffValues; i++) {
                for (int j = 0; j < dims; j++) {
                    values[i][j] = (byte) randomIntBetween(-128, 127);
                }
            }
            List<byte[]> vectorList = new ArrayList<>(nVectors);
            for (int i = 0; i < nVectors; i++) {
                vectorList.add(values[random().nextInt(diffValues)]);
            }
            KMeansByteVectorValues vectors = KMeansByteVectorValues.build(vectorList, null, dims);

            HierarchicalKMeans<byte[]> hkmeans = HierarchicalKMeans.ofSerial(
                CentroidOps.BYTE,
                dims,
                random().nextInt(1, 100),
                random().nextInt(Math.min(nVectors, 100), nVectors + 1),
                random().nextInt(2, 512),
                random().nextFloat(0.5f, 1.5f)
            );

            KMeansResult<byte[]> result = hkmeans.cluster(vectors, targetSize);
            assertKMeansResultValid(result, nVectors, -1);
        } else {
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
            KMeansFloatVectorValues vectors = KMeansFloatVectorValues.build(vectorList, null, dims);

            HierarchicalKMeans<float[]> hkmeans = HierarchicalKMeans.ofSerial(
                CentroidOps.FLOAT,
                dims,
                random().nextInt(1, 100),
                random().nextInt(Math.min(nVectors, 100), nVectors + 1),
                random().nextInt(2, 512),
                random().nextFloat(0.5f, 1.5f)
            );

            KMeansResult<float[]> result = hkmeans.cluster(vectors, targetSize);
            assertKMeansResultValid(result, nVectors, -1);
        }
    }

    public void testClusterQualityByte() throws IOException {
        // Verifies that byte clustering produces reasonable assignments:
        // vectors near the same true centroid should mostly get the same assignment.
        int nClusters = 4;
        int nVectors = nClusters * 200;
        int dims = 16;

        // Generate well-separated clusters
        byte[][] trueCentroids = new byte[nClusters][dims];
        for (int i = 0; i < nClusters; i++) {
            for (int j = 0; j < dims; j++) {
                // Spread centroids far apart
                trueCentroids[i][j] = (byte) ((i * 64) - 128 + randomIntBetween(-5, 5));
            }
        }

        List<byte[]> vectorList = new ArrayList<>(nVectors);
        int[] trueLabels = new int[nVectors];
        for (int i = 0; i < nVectors; i++) {
            int cluster = i % nClusters;
            trueLabels[i] = cluster;
            byte[] vector = new byte[dims];
            for (int j = 0; j < dims; j++) {
                vector[j] = (byte) Math.clamp(trueCentroids[cluster][j] + randomIntBetween(-3, 3), -128, 127);
            }
            vectorList.add(vector);
        }

        KMeansByteVectorValues vectors = KMeansByteVectorValues.build(vectorList, null, dims);

        HierarchicalKMeans<byte[]> hkmeans = HierarchicalKMeans.ofSerial(CentroidOps.BYTE, dims, 10, nVectors, 512, 1.0f);
        KMeansResult<byte[]> result = hkmeans.cluster(vectors, nVectors / nClusters);

        int[] assignments = result.assignments();

        // Check that vectors in the same true cluster mostly get the same assignment
        int[][] assignmentCounts = new int[nClusters][result.centroids().length];
        for (int i = 0; i < nVectors; i++) {
            assignmentCounts[trueLabels[i]][assignments[i]]++;
        }

        int correctCount = 0;
        for (int c = 0; c < nClusters; c++) {
            int maxCount = Arrays.stream(assignmentCounts[c]).max().orElse(0);
            correctCount += maxCount;
        }
        // Expect at least 70% of vectors assigned to their correct cluster
        float accuracy = (float) correctCount / nVectors;
        assertTrue("Byte KMeans accuracy too low: " + accuracy, accuracy >= 0.7f);
    }

    public void testRemoveEmptyClustersWithByte() throws IOException {
        // Test that removeEmptyClusters works correctly when V=byte[]
        int dims = 8;
        int nVectors = 100;

        // Create vectors that naturally cluster into 2 groups, but request more clusters
        List<byte[]> vectorList = new ArrayList<>(nVectors);
        for (int i = 0; i < nVectors; i++) {
            byte[] vector = new byte[dims];
            byte base = (i < nVectors / 2) ? (byte) -50 : (byte) 50;
            for (int j = 0; j < dims; j++) {
                vector[j] = (byte) Math.clamp(base + randomIntBetween(-5, 5), -128, 127);
            }
            vectorList.add(vector);
        }

        KMeansByteVectorValues vectors = KMeansByteVectorValues.build(vectorList, null, dims);

        // Request more clusters than natural groups — some should end up empty and get removed
        int targetSize = nVectors / 10;
        HierarchicalKMeans<byte[]> hkmeans = HierarchicalKMeans.ofSerial(CentroidOps.BYTE, dims, 5, nVectors, 512, -1f);
        KMeansResult<byte[]> result = hkmeans.cluster(vectors, targetSize);

        // Should not throw ClassCastException
        assertNotNull(result);
        assertTrue(result.centroids().length > 0);

        // All assignments valid
        for (int a : result.assignments()) {
            assertTrue(a >= 0 && a < result.centroids().length);
        }
    }

    // ---- Helpers ----

    private static <V> void assertKMeansResultValid(KMeansResult<V> result, int nVectors, int expectedClusters) {
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

    static float kMeansMeanInertia(KMeansFloatVectorValues vectors, KMeansResult<float[]> kMeansIntermediate) throws IOException {
        int[] assignments = kMeansIntermediate.assignments();
        float[][] centroids = kMeansIntermediate.centroids();

        float mse = 0;
        for (int i = 0; i < vectors.size(); i++) {
            float[] vec = vectors.vectorValue(i);
            float[] cent = centroids[assignments[i]];
            float dist = VectorUtil.squareDistance(vec, cent);
            mse += dist / vectors.size();
        }
        return mse;
    }

    private static KMeansFloatVectorValues generateFloatData(int nSamples, int nDims, int nClusters) {
        List<float[]> vectors = new ArrayList<>(nSamples);
        float[][] centroids = new float[nClusters][nDims];
        // Generate random centroids
        for (int i = 0; i < nClusters; i++) {
            for (int j = 0; j < nDims; j++) {
                centroids[i][j] = random().nextFloat() * 100;
            }
        }
        // Generate data points around centroids
        for (int i = 0; i < nSamples; i++) {
            int cluster = random().nextInt(nClusters);
            float[] vector = new float[nDims];
            for (int j = 0; j < nDims; j++) {
                vector[j] = centroids[cluster][j] + (float) random().nextGaussian() * 10 - 5;
            }
            vectors.add(vector);
        }
        return KMeansFloatVectorValues.build(vectors, null, nDims);
    }

    private static KMeansByteVectorValues generateByteData(int nSamples, int nDims, int nClusters) {
        List<byte[]> vectors = new ArrayList<>(nSamples);
        byte[][] centroids = new byte[nClusters][nDims];
        // Generate random centroids
        for (int i = 0; i < nClusters; i++) {
            for (int j = 0; j < nDims; j++) {
                centroids[i][j] = (byte) randomIntBetween(-128, 127);
            }
        }
        // Generate data points around centroids
        for (int i = 0; i < nSamples; i++) {
            int cluster = random().nextInt(nClusters);
            byte[] vector = new byte[nDims];
            for (int j = 0; j < nDims; j++) {
                vector[j] = (byte) Math.clamp(centroids[cluster][j] + randomIntBetween(-10, 10), -128, 127);
            }
            vectors.add(vector);
        }
        return KMeansByteVectorValues.build(vectors, null, nDims);
    }
}
