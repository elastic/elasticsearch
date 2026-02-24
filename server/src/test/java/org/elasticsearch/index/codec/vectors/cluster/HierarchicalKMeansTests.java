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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans.NO_SOAR_ASSIGNMENT;

public class HierarchicalKMeansTests extends ESTestCase {

    public void testHKmeans() throws IOException {
        int nClusters = random().nextInt(1, 10);
        int nVectors = random().nextInt(1, nClusters * 200);
        int dims = random().nextInt(2, 20);
        int sampleSize = random().nextInt(Math.min(nVectors, 100), nVectors + 1);
        int maxIterations = random().nextInt(1, 100);
        int clustersPerNeighborhood = random().nextInt(2, 512);
        float soarLambda = random().nextFloat(0.5f, 1.5f);
        KMeansFloatVectorValues vectors = generateData(nVectors, dims, nClusters);

        int targetSize = (int) ((float) nVectors / (float) nClusters);
        HierarchicalKMeans hkmeans = HierarchicalKMeans.ofSerial(dims, maxIterations, sampleSize, clustersPerNeighborhood, soarLambda);

        KMeansResult result = hkmeans.cluster(vectors, targetSize);

        float[][] centroids = result.centroids();
        int[] assignments = result.assignments();
        int[] soarAssignments = result.soarAssignments();

        assertEquals(Math.min(nClusters, nVectors), centroids.length, 10);
        assertEquals(nVectors, assignments.length);

        for (int assignment : assignments) {
            assertTrue(assignment >= 0 && assignment < centroids.length);
        }
        if (centroids.length > 1 && centroids.length < nVectors) {
            assertEquals(nVectors, soarAssignments.length);
            // verify no duplicates exist
            for (int i = 0; i < assignments.length; i++) {
                int soarAssignment = soarAssignments[i];
                assertTrue(soarAssignment == -1 || (soarAssignment >= 0 && soarAssignment < centroids.length));
                assertNotEquals(assignments[i], soarAssignment);
            }
        } else {
            assertEquals(0, soarAssignments.length);
        }

        int numWorker = randomIntBetween(2, 8);
        try (ExecutorService service = Executors.newFixedThreadPool(numWorker)) {
            TaskExecutor executor = new TaskExecutor(service);
            hkmeans = HierarchicalKMeans.ofConcurrent(
                dims,
                executor,
                numWorker,
                maxIterations,
                sampleSize,
                clustersPerNeighborhood,
                soarLambda
            );
            KMeansResult resultConcurrency = hkmeans.cluster(vectors, targetSize);
            assertArrayEquals(result.centroids(), resultConcurrency.centroids());
            assertArrayEquals(result.assignments(), resultConcurrency.assignments());
            assertArrayEquals(result.soarAssignments(), resultConcurrency.soarAssignments());
        }
    }

    private static KMeansFloatVectorValues generateData(int nSamples, int nDims, int nClusters) {
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

        HierarchicalKMeans hkmeans = HierarchicalKMeans.ofSerial(
            dims,
            random().nextInt(1, 100),
            random().nextInt(Math.min(nVectors, 100), nVectors + 1),
            random().nextInt(2, 512),
            random().nextFloat(0.5f, 1.5f)
        );

        KMeansResult result = hkmeans.cluster(vectors, targetSize);

        float[][] centroids = result.centroids();
        int[] assignments = result.assignments();
        int[] soarAssignments = result.soarAssignments();

        int[] counts = new int[centroids.length];
        for (int i = 0; i < assignments.length; i++) {
            counts[assignments[i]]++;
        }
        int totalCount = 0;
        for (int count : counts) {
            totalCount += count;
            assertTrue(count > 0);
        }
        assertEquals(nVectors, totalCount);

        assertEquals(nVectors, assignments.length);

        for (int assignment : assignments) {
            assertTrue(assignment >= 0 && assignment < centroids.length);
        }
        if (centroids.length > 1 && centroids.length < nVectors) {
            assertEquals(nVectors, soarAssignments.length);
            // verify no duplicates exist
            for (int i = 0; i < assignments.length; i++) {
                assertTrue((soarAssignments[i] == NO_SOAR_ASSIGNMENT || soarAssignments[i] >= 0) && soarAssignments[i] < centroids.length);
                assertNotEquals(assignments[i], soarAssignments[i]);
            }
        } else {
            assertEquals(0, soarAssignments.length);
        }
    }
}
