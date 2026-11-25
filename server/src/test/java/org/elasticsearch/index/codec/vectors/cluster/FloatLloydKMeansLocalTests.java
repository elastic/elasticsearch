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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class FloatLloydKMeansLocalTests extends AbstractLloydKMeansLocalTestCase<float[]> {

    @Override
    protected CentroidOps<float[]> centroidOps() {
        return CentroidOps.FLOAT;
    }

    @Override
    protected ClusteringVectorValues<float[]> generateData(int nSamples, int nDims, int nClusters) {
        return KMeansTestData.generateFloatData(nSamples, nDims, nClusters);
    }

    @Override
    protected ClusteringVectorValues<float[]> generateZeroData(int nVectors, int dims) {
        List<float[]> vectors = new ArrayList<>(nVectors);
        for (int i = 0; i < nVectors; i++) {
            vectors.add(new float[dims]);
        }
        return KMeansFloatVectorValues.build(vectors, null, dims);
    }

    @Override
    protected ClusteringVectorValues<float[]> buildEmptyVectors(int dims) {
        return KMeansFloatVectorValues.build(List.of(), null, dims);
    }

    @Override
    protected void assertCentroidsAreZero(float[][] centroids) {
        for (float[] centroid : centroids) {
            for (float v : centroid) {
                if (v > 0.0000001f) {
                    assertEquals(0.0f, v, 0.00000001f);
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
        NeighborHood[] neighborHoodsGraph = NeighborHood.computeNeighborhoodsGraph(CentroidOps.FLOAT, vectors, clustersPerNeighbour);
        NeighborHood[] neighborHoodsBruteForce = NeighborHood.computeNeighborhoodsBruteForce(
            CentroidOps.FLOAT,
            vectors,
            clustersPerNeighbour
        );
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
                CentroidOps.FLOAT,
                taskExecutor,
                numThreads,
                vectors,
                clustersPerNeighbour
            );
            // The concurrent HNSW builder (HnswConcurrentMergeBuilder) is non-deterministic due to
            // thread scheduling, so it may produce a different graph than the serial builder and
            // therefore different (but equally valid) approximate neighbors. Verify against brute
            // force instead of asserting exact equality with the serial result.
            assertEquals(neighborHoodsBruteForce.length, neighborHoodsGraphConcurrent.length);
            for (int i = 0; i < neighborHoodsGraphConcurrent.length; i++) {
                assertEquals(neighborHoodsBruteForce[i].neighbors().length, neighborHoodsGraphConcurrent[i].neighbors().length);
                int matched = compareNN(i, neighborHoodsBruteForce[i].neighbors(), neighborHoodsGraphConcurrent[i].neighbors());
                double recall = (double) matched / neighborHoodsGraphConcurrent[i].neighbors().length;
                assertThat(recall, greaterThanOrEqualTo(0.5));
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

        // brute force as the ground truth reference
        NeighborHood[] neighborHoodsBruteForce = NeighborHood.computeNeighborhoodsBruteForce(
            CentroidOps.FLOAT,
            vectors,
            clustersPerNeighbour
        );

        // multiple concurrent executions: each must have good recall against brute force;
        // exact equality with the serial build is not guaranteed due to non-deterministic thread
        // scheduling in HnswConcurrentMergeBuilder
        for (int iter = 0; iter < 50; iter++) {
            int numThreads = randomIntBetween(2, 8);
            try (ExecutorService executorService = Executors.newFixedThreadPool(numThreads)) {
                TaskExecutor taskExecutor = new TaskExecutor(executorService);
                NeighborHood[] neighborHoodsGraphConcurrent = NeighborHood.computeNeighborhoodsGraph(
                    CentroidOps.FLOAT,
                    taskExecutor,
                    numThreads,
                    vectors,
                    clustersPerNeighbour
                );

                assertEquals(neighborHoodsBruteForce.length, neighborHoodsGraphConcurrent.length);
                for (int i = 0; i < neighborHoodsGraphConcurrent.length; i++) {
                    assertEquals(neighborHoodsBruteForce[i].neighbors().length, neighborHoodsGraphConcurrent[i].neighbors().length);
                    int matched = compareNN(i, neighborHoodsBruteForce[i].neighbors(), neighborHoodsGraphConcurrent[i].neighbors());
                    double recall = (double) matched / neighborHoodsGraphConcurrent[i].neighbors().length;
                    assertThat(
                        "Iteration " + iter + ", thread count " + numThreads + ": recall too low at index " + i,
                        recall,
                        greaterThanOrEqualTo(0.5)
                    );
                }
            }
        }
    }
}
