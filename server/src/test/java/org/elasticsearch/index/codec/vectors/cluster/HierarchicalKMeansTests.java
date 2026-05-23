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

    public void testHierarchicalKMeansWithBalancing() throws IOException {
        int nVectors = random().nextInt(100, 10000);
        int dims = random().nextInt(2, 20);
        int nClusters = random().nextInt(2, 50);
        int sampleSize = random().nextInt(Math.min(nVectors, 100), nVectors + 1);
        int maxIterations = random().nextInt(1, 100);
        int clustersPerNeighborhood = random().nextInt(2, 512);
        float soarLambda = random().nextFloat(0.5f, 1.5f);

        KMeansFloatVectorValues vectors = generateFloatData(nVectors, dims, nClusters);
        int targetSize = (int) ((float) nVectors / (float) nClusters);
        HierarchicalKMeans<float[]> hkmeans = HierarchicalKMeans.ofSerial(
            CentroidOps.FLOAT,
            dims,
            maxIterations,
            sampleSize,
            clustersPerNeighborhood,
            soarLambda
        );
        KMeansResult<float[]> result = hkmeans.cluster(vectors, targetSize);
        assertKMeansResultValid(result, nVectors, nClusters);
    }

    public void testHierarchicalKMeansConcurrency() throws IOException {
        int nVectors = random().nextInt(100, 10000);
        int dims = random().nextInt(2, 20);
        int nClusters = random().nextInt(2, 50);
        int sampleSize = random().nextInt(Math.min(nVectors, 100), nVectors + 1);
        int maxIterations = random().nextInt(1, 100);
        int clustersPerNeighborhood = random().nextInt(2, 512);
        float soarLambda = random().nextFloat(0.5f, 1.5f);

        KMeansFloatVectorValues vectors = generateFloatData(nVectors, dims, nClusters);

        int targetSize = (int) ((float) nVectors / (float) nClusters);
        HierarchicalKMeans<float[]> hkmeansSerial = HierarchicalKMeans.ofSerial(
            CentroidOps.FLOAT,
            dims,
            maxIterations,
            sampleSize,
            clustersPerNeighborhood,
            soarLambda
        );

        KMeansResult<float[]> serialResult = hkmeansSerial.cluster(vectors, targetSize);

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
        for (int i = 0; i < nClusters; i++) {
            for (int j = 0; j < nDims; j++) {
                centroids[i][j] = random().nextFloat() * 100;
            }
        }
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
}
