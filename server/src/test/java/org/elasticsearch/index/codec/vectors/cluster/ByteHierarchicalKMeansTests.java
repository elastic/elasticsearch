/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.vectors.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Byte-specific HierarchicalKMeans tests. Ensures byte clustering always gets exercised
 * and includes byte-specific quality assertions.
 */
public class ByteHierarchicalKMeansTests extends AbstractHierarchicalKMeansTestCase<byte[]> {

    @Override
    protected CentroidOps<byte[]> centroidOps() {
        return CentroidOps.BYTE;
    }

    @Override
    protected ClusteringVectorValues<byte[]> generateData(int nSamples, int nDims, int nClusters) {
        return KMeansTestData.generateByteData(nSamples, nDims, nClusters);
    }

    @Override
    protected ClusteringVectorValues<byte[]> generateFewDistinctData(int nVectors, int dims, int diffValues) {
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
        return KMeansByteVectorValues.build(vectorList, null, dims);
    }

    /**
     * Verifies that byte clustering produces reasonable assignments:
     * vectors near the same true centroid should mostly get the same assignment.
     */
    public void testClusterQuality() throws IOException {
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
        var result = hkmeans.cluster(vectors, nVectors / nClusters);

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

    public void testRemoveEmptyClusters() throws IOException {
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
        var result = hkmeans.cluster(vectors, targetSize);

        // Should not throw ClassCastException
        assertNotNull(result);
        assertTrue(result.centroids().length > 0);

        // All assignments valid
        for (int a : result.assignments()) {
            assertTrue(a >= 0 && a < result.centroids().length);
        }
    }
}
