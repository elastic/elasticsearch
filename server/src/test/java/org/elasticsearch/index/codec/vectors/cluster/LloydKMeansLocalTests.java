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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class LloydKMeansLocalTests extends ESTestCase {

    public void testIllegalClustersPerNeighborhood() {
        KMeansLocal<float[]> kMeansLocal = new LloydKMeansLocalSerial<>(CentroidOps.FLOAT, randomInt(), randomInt());
        KMeansIntermediate<float[]> kMeansIntermediate = new KMeansIntermediate<>(new float[0][], new int[0], i -> i);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> kMeansLocal.cluster(
                KMeansFloatVectorValues.build(List.of(), null, randomInt(1024)),
                kMeansIntermediate,
                randomIntBetween(Integer.MIN_VALUE, 1),
                randomFloat()
            )
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

        boolean useByte = randomBoolean();
        if (useByte) {
            KMeansByteVectorValues vectors = generateByteData(nVectors, dims, nClusters);
            byte[][] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters, CentroidOps.BYTE);
            LloydKMeansLocal.cluster(vectors, CentroidOps.BYTE, centroids, sampleSize, maxIterations);

            int[] assignments = new int[vectors.size()];
            int[] assignmentOrdinals = new int[vectors.size()];
            for (int i = 0; i < vectors.size(); i++) {
                float minDist = Float.MAX_VALUE;
                int ord = -1;
                byte[] vec = vectors.vectorValue(i);
                for (int j = 0; j < centroids.length; j++) {
                    float dist = CentroidOps.BYTE.squareDistance(vec, centroids[j]);
                    if (dist < minDist) {
                        minDist = dist;
                        ord = j;
                    }
                }
                assignments[i] = ord;
                assignmentOrdinals[i] = i;
            }

            KMeansIntermediate<byte[]> kMeansIntermediate = new KMeansIntermediate<>(centroids, assignments, i -> assignmentOrdinals[i]);
            KMeansLocal<byte[]> kMeansLocal = new LloydKMeansLocalSerial<>(CentroidOps.BYTE, sampleSize, maxIterations);
            kMeansLocal.cluster(vectors, kMeansIntermediate, clustersPerNeighborhood, soarLambda);

            assertEquals(nClusters, centroids.length);
            assertNotNull(kMeansIntermediate.soarAssignments());
        } else {
            KMeansFloatVectorValues vectors = generateFloatData(nVectors, dims, nClusters);
            float[][] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters, CentroidOps.FLOAT);
            LloydKMeansLocal.cluster(vectors, CentroidOps.FLOAT, centroids, sampleSize, maxIterations);

            int[] assignments = new int[vectors.size()];
            int[] assignmentOrdinals = new int[vectors.size()];
            for (int i = 0; i < vectors.size(); i++) {
                float minDist = Float.MAX_VALUE;
                int ord = -1;
                for (int j = 0; j < centroids.length; j++) {
                    float dist = VectorUtil.squareDistance(vectors.vectorValue(i), centroids[j]);
                    if (dist < minDist) {
                        minDist = dist;
                        ord = j;
                    }
                }
                assignments[i] = ord;
                assignmentOrdinals[i] = i;
            }

            KMeansIntermediate<float[]> kMeansIntermediate = new KMeansIntermediate<>(centroids, assignments, i -> assignmentOrdinals[i]);
            KMeansLocal<float[]> kMeansLocal = new LloydKMeansLocalSerial<>(CentroidOps.FLOAT, sampleSize, maxIterations);
            kMeansLocal.cluster(vectors, kMeansIntermediate, clustersPerNeighborhood, soarLambda);

            assertEquals(nClusters, centroids.length);
            assertNotNull(kMeansIntermediate.soarAssignments());
        }
    }

    public void testKMeansNeighborsAllZero() throws IOException {
        int nClusters = 10;
        int maxIterations = 10;
        int clustersPerNeighborhood = 128;
        float soarLambda = 1.0f;
        int nVectors = 1000;

        boolean useByte = randomBoolean();
        if (useByte) {
            List<byte[]> vectors = new ArrayList<>();
            for (int i = 0; i < nVectors; i++) {
                vectors.add(new byte[5]);
            }
            int sampleSize = vectors.size();
            KMeansByteVectorValues bvv = KMeansByteVectorValues.build(vectors, null, 5);

            byte[][] centroids = KMeansLocal.pickInitialCentroids(bvv, nClusters, CentroidOps.BYTE);
            LloydKMeansLocal.cluster(bvv, CentroidOps.BYTE, centroids, sampleSize, maxIterations);

            int[] assignments = new int[vectors.size()];
            int[] assignmentOrdinals = new int[vectors.size()];
            for (int i = 0; i < vectors.size(); i++) {
                assignments[i] = 0; // all zeros -> all same distance -> first centroid
                assignmentOrdinals[i] = i;
            }

            KMeansIntermediate<byte[]> kMeansIntermediate = new KMeansIntermediate<>(centroids, assignments, i -> assignmentOrdinals[i]);
            KMeansLocal<byte[]> kMeansLocal = new LloydKMeansLocalSerial<>(CentroidOps.BYTE, sampleSize, maxIterations);
            kMeansLocal.cluster(bvv, kMeansIntermediate, clustersPerNeighborhood, soarLambda);

            assertEquals(nClusters, centroids.length);
            assertNotNull(kMeansIntermediate.soarAssignments());
            for (byte[] centroid : centroids) {
                for (byte v : centroid) {
                    assertEquals(0, v);
                }
            }
        } else {
            List<float[]> vectors = new ArrayList<>();
            for (int i = 0; i < nVectors; i++) {
                vectors.add(new float[5]);
            }
            int sampleSize = vectors.size();
            KMeansFloatVectorValues fvv = KMeansFloatVectorValues.build(vectors, null, 5);

            float[][] centroids = KMeansLocal.pickInitialCentroids(fvv, nClusters, CentroidOps.FLOAT);
            LloydKMeansLocal.cluster(fvv, CentroidOps.FLOAT, centroids, sampleSize, maxIterations);

            int[] assignments = new int[vectors.size()];
            int[] assignmentOrdinals = new int[vectors.size()];
            for (int i = 0; i < vectors.size(); i++) {
                float minDist = Float.MAX_VALUE;
                int ord = -1;
                for (int j = 0; j < centroids.length; j++) {
                    float dist = VectorUtil.squareDistance(fvv.vectorValue(i), centroids[j]);
                    if (dist < minDist) {
                        minDist = dist;
                        ord = j;
                    }
                }
                assignments[i] = ord;
                assignmentOrdinals[i] = i;
            }

            KMeansIntermediate<float[]> kMeansIntermediate = new KMeansIntermediate<>(centroids, assignments, i -> assignmentOrdinals[i]);
            KMeansLocal<float[]> kMeansLocal = new LloydKMeansLocalSerial<>(CentroidOps.FLOAT, sampleSize, maxIterations);
            kMeansLocal.cluster(fvv, kMeansIntermediate, clustersPerNeighborhood, soarLambda);

            assertEquals(nClusters, centroids.length);
            assertNotNull(kMeansIntermediate.soarAssignments());
            for (float[] centroid : centroids) {
                for (float v : centroid) {
                    if (v > 0.0000001f) {
                        assertEquals(0.0f, v, 0.00000001f);
                    }
                }
            }
        }
    }

    public void testComputeNeighbours() throws IOException {
        int numCentroids = randomIntBetween(1000, 2000);
        int dims = randomIntBetween(10, 200);
        float[][] vectors = new float[numCentroids][dims];
        for (int i = 0; i < numCentroids; i++) {
            for (int j = 0; j < dims; j++) {
                vectors[i][j] = randomFloat();
            }
        }
        int clustersPerNeighbour = randomIntBetween(64, 128);
        NeighborHood[] neighborHoodsGraph = NeighborHood.computeNeighborhoodsGraph(vectors, clustersPerNeighbour);
        NeighborHood[] neighborHoodsBruteForce = NeighborHood.computeNeighborhoodsBruteForce(vectors, clustersPerNeighbour);
        assertEquals(neighborHoodsGraph.length, neighborHoodsBruteForce.length);
        for (int i = 0; i < neighborHoodsGraph.length; i++) {
            assertEquals(neighborHoodsBruteForce[i].neighbors().length, neighborHoodsGraph[i].neighbors().length);
            int matched = compareNN(i, neighborHoodsBruteForce[i].neighbors(), neighborHoodsGraph[i].neighbors());
            double recall = (double) matched / neighborHoodsGraph[i].neighbors().length;
            assertThat(recall, greaterThanOrEqualTo(0.5));
            if (recall == 1.0) {
                // we cannot assert on array equality as there can be small differences due to numerical errors
                assertEquals(neighborHoodsBruteForce[i].maxIntraDistance(), neighborHoodsGraph[i].maxIntraDistance(), 1e-4f);
            }
        }
        int numThreads = randomIntBetween(2, 8);
        try (ExecutorService executorService = Executors.newFixedThreadPool(numThreads)) {
            TaskExecutor taskExecutor = new TaskExecutor(executorService);
            NeighborHood[] neighborHoodsGraphConcurrent = NeighborHood.computeNeighborhoodsGraph(
                taskExecutor,
                numThreads,
                vectors,
                clustersPerNeighbour
            );
            assertEquals(neighborHoodsGraph.length, neighborHoodsGraphConcurrent.length);
            for (int i = 0; i < neighborHoodsGraph.length; i++) {
                assertArrayEquals(neighborHoodsGraph[i].neighbors(), neighborHoodsGraphConcurrent[i].neighbors());
                assertEquals(neighborHoodsGraph[i].maxIntraDistance(), neighborHoodsGraphConcurrent[i].maxIntraDistance(), 0f);
            }
        }
    }

    private static int compareNN(int currentId, int[] expected, int[] results) {
        int matched = 0;
        Set<Integer> expectedSet = new HashSet<>();
        Set<Integer> alreadySeen = new HashSet<>();
        for (int i : expected) {
            assertNotEquals(currentId, i);
            assertTrue(expectedSet.add(i));
        }
        for (int i : results) {
            assertNotEquals(currentId, i);
            assertTrue(alreadySeen.add(i));
            if (expectedSet.contains(i)) {
                ++matched;
            }
        }
        return matched;
    }

    public void testComputeNeighboursThreadSafety() throws IOException {
        int numCentroids = randomIntBetween(500, 1500);
        int dims = randomIntBetween(10, 50);
        float[][] vectors = new float[numCentroids][dims];
        for (int i = 0; i < numCentroids; i++) {
            for (int j = 0; j < dims; j++) {
                vectors[i][j] = randomFloat();
            }
        }
        int clustersPerNeighbour = randomIntBetween(32, 64);

        // sequential version
        NeighborHood[] neighborHoodsGraph = NeighborHood.computeNeighborhoodsGraph(vectors, clustersPerNeighbour);

        // multiple concurrent executions for consistency
        for (int iter = 0; iter < 50; iter++) {
            int numThreads = randomIntBetween(2, 8);
            try (ExecutorService executorService = Executors.newFixedThreadPool(numThreads)) {
                TaskExecutor taskExecutor = new TaskExecutor(executorService);
                NeighborHood[] neighborHoodsGraphConcurrent = NeighborHood.computeNeighborhoodsGraph(
                    taskExecutor,
                    numThreads,
                    vectors,
                    clustersPerNeighbour
                );

                assertEquals(neighborHoodsGraph.length, neighborHoodsGraphConcurrent.length);
                for (int i = 0; i < neighborHoodsGraph.length; i++) {
                    assertArrayEquals(
                        "Iteration " + iter + ", thread count " + numThreads + ": Different neighbors at index " + i,
                        neighborHoodsGraph[i].neighbors(),
                        neighborHoodsGraphConcurrent[i].neighbors()
                    );
                    assertEquals(
                        "Iteration " + iter + ", thread count " + numThreads + ": Different maxIntraDistance at index " + i,
                        neighborHoodsGraph[i].maxIntraDistance(),
                        neighborHoodsGraphConcurrent[i].maxIntraDistance(),
                        1e-5f
                    );
                }
            }
        }
    }

    // ---- Data generators ----

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
                vector[j] = centroids[cluster][j] + random().nextFloat() * 10 - 5;
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
