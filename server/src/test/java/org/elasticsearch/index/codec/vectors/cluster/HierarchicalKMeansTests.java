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
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HierarchicalKMeansTests extends ESTestCase {

    public void testHKmeans() throws IOException {
        int nClusters = random().nextInt(1, 10);
        int nVectors = random().nextInt(1, nClusters * 200);
        int dims = random().nextInt(2, 20);
        int sampleSize = random().nextInt(Math.min(nVectors, 100), nVectors + 1);
        int maxIterations = random().nextInt(1, 100);
        int clustersPerNeighborhood = random().nextInt(2, 512);
        float soarLambda = random().nextFloat(0.5f, 1.5f);
        FloatVectorValues vectors = generateData(nVectors, dims, nClusters);

        int targetSize = (int) ((float) nVectors / (float) nClusters);
        HierarchicalKMeans hkmeans = new HierarchicalKMeans(dims, maxIterations, sampleSize, clustersPerNeighborhood, soarLambda);

        KMeansResult result = hkmeans.cluster(vectors, targetSize);

        float[][] centroids = result.centroids();
        int[] assignments = result.assignments();
        int[] soarAssignments = result.soarAssignments();

        assertEquals(Math.min(nClusters, nVectors), centroids.length, 8);
        assertEquals(nVectors, assignments.length);

        for (int assignment : assignments) {
            assertTrue(assignment >= 0 && assignment < centroids.length);
        }
        if (centroids.length > 1 && centroids.length < nVectors) {
            assertEquals(nVectors, soarAssignments.length);
            // verify no duplicates exist
            for (int i = 0; i < assignments.length; i++) {
                assertTrue(soarAssignments[i] >= 0 && soarAssignments[i] < centroids.length);
                assertNotEquals(assignments[i], soarAssignments[i]);
            }
        } else {
            assertEquals(0, soarAssignments.length);
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
}
