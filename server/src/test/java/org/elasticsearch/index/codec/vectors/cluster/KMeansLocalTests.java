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
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansLocalTests extends ESTestCase {

    public void testKMeansLocal() throws IOException {
        int nClusters = random().nextInt(1, 10);
        int nVectors = random().nextInt(nClusters * 100, nClusters * 200);
        int dims = random().nextInt(2, 20);
        int sampleSize = random().nextInt(100, nVectors);
        int maxIterations = random().nextInt(0, 100);
        short clustersPerNeighborhood = (short) random().nextInt(0, 512);
        FloatVectorValues vectors = generateData(nVectors, dims, nClusters);

        float[][] centroids = KMeans.pickInitialCentroids(vectors, sampleSize, nClusters);
        KMeans.cluster(vectors, centroids, sampleSize, maxIterations);

        int[] assignments = new int[vectors.size()];
        int[] assignmentOrdinals = new int[vectors.size()];
        for (int i = 0; i < vectors.size(); i++) {
            float minDist = Float.MAX_VALUE;
            short ord = -1;
            for (short j = 0; j < centroids.length; j++) {
                float dist = VectorUtil.squareDistance(vectors.vectorValue(i), centroids[j]);
                if (dist < minDist) {
                    minDist = dist;
                    ord = j;
                }
            }
            assignments[i] = ord;
            assignmentOrdinals[i] = i;
        }

        KMeansResult kMeansResult = new KMeansResult(centroids, assignments, assignmentOrdinals);
        KMeansLocal kMeansLocal = new KMeansLocal(sampleSize, maxIterations, clustersPerNeighborhood);
        kMeansLocal.cluster(vectors, kMeansResult);

        assertEquals(nClusters, centroids.length);
        assertNotNull(kMeansResult.soarAssignments());
    }

    public void testKMeansLocalAllZero() throws IOException {
        int nClusters = 10;
        int maxIterations = 10;
        short clustersPerNeighborhood = 128;
        int nVectors = 1000;
        List<float[]> vectors = new ArrayList<>();
        for (int i = 0; i < nVectors; i++) {
            float[] vector = new float[5];
            vectors.add(vector);
        }
        int sampleSize = vectors.size();
        FloatVectorValues fvv = FloatVectorValues.fromFloats(vectors, 5);

        float[][] centroids = KMeans.pickInitialCentroids(fvv, sampleSize, nClusters);
        KMeans.cluster(fvv, centroids, sampleSize, maxIterations);

        int[] assignments = new int[vectors.size()];
        int[] assignmentOrdinals = new int[vectors.size()];
        for (int i = 0; i < vectors.size(); i++) {
            float minDist = Float.MAX_VALUE;
            short ord = -1;
            for (short j = 0; j < centroids.length; j++) {
                float dist = VectorUtil.squareDistance(fvv.vectorValue(i), centroids[j]);
                if (dist < minDist) {
                    minDist = dist;
                    ord = j;
                }
            }
            assignments[i] = ord;
            assignmentOrdinals[i] = i;
        }

        KMeansResult kMeansResult = new KMeansResult(centroids, assignments, assignmentOrdinals);
        KMeansLocal kMeansLocal = new KMeansLocal(sampleSize, maxIterations, clustersPerNeighborhood);
        kMeansLocal.cluster(fvv, kMeansResult);

        assertEquals(nClusters, centroids.length);
        assertNotNull(kMeansResult.soarAssignments());
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
}
