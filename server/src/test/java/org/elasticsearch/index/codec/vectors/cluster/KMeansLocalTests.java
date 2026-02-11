/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.index.FloatVectorValues;
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

public class KMeansLocalTests extends ESTestCase {

    public void testIllegalClustersPerNeighborhood() {
        KMeansLocal kMeansLocal = new KMeansLocalSerial(randomInt(), randomInt());
        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(new float[0][], new int[0], i -> i);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> kMeansLocal.cluster(
                FloatVectorValues.fromFloats(List.of(), randomInt(1024)),
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
        FloatVectorValues vectors = generateData(nVectors, dims, nClusters);

        float[][] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters);
        KMeansLocal.cluster(vectors, centroids, sampleSize, maxIterations);

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

        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(centroids, assignments, i -> assignmentOrdinals[i]);
        KMeansLocal kMeansLocal = new KMeansLocalSerial(sampleSize, maxIterations);
        kMeansLocal.cluster(vectors, kMeansIntermediate, clustersPerNeighborhood, soarLambda);

        assertEquals(nClusters, centroids.length);
        assertNotNull(kMeansIntermediate.soarAssignments());
    }

    public void testKMeansNeighborsAllZero() throws IOException {
        int nClusters = 10;
        int maxIterations = 10;
        int clustersPerNeighborhood = 128;
        float soarLambda = 1.0f;
        int nVectors = 1000;
        List<float[]> vectors = new ArrayList<>();
        for (int i = 0; i < nVectors; i++) {
            float[] vector = new float[5];
            vectors.add(vector);
        }
        int sampleSize = vectors.size();
        FloatVectorValues fvv = FloatVectorValues.fromFloats(vectors, 5);

        float[][] centroids = KMeansLocal.pickInitialCentroids(fvv, nClusters);
        KMeansLocal.cluster(fvv, centroids, sampleSize, maxIterations);

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

        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(centroids, assignments, i -> assignmentOrdinals[i]);
        KMeansLocal kMeansLocal = new KMeansLocalSerial(sampleSize, maxIterations);
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

    private static FloatVectorValues generateData(int nSamples, int nDims, int nClusters) {
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
        return FloatVectorValues.fromFloats(vectors, nDims);
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
                assertEquals(neighborHoodsBruteForce[i].maxIntraDistance(), neighborHoodsGraph[i].maxIntraDistance(), 1e-5f);
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
}
