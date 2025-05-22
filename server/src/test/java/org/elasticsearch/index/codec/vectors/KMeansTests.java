/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansTests extends ESTestCase {

    public void testKMeansAPI() throws IOException {
        int nClusters = random().nextInt(1, 10);
        int nVectors = random().nextInt(nClusters * 100, nClusters * 200);
        int dims = random().nextInt(2, 20);
        int randIdx = random().nextInt(VectorSimilarityFunction.values().length);
        VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.values()[randIdx];
        FloatVectorValues vectors = generateData(nVectors, dims, nClusters);

        // default case
        {
            KMeans.Results results = KMeans.cluster(vectors, similarityFunction, nClusters);
            assertResults(results, nClusters, nVectors, true);
            assertEquals(nClusters, results.centroids().length);
            assertEquals(nClusters, results.centroidsSize().length);
            assertEquals(nVectors, results.vectorCentroids().length);
        }
        // expert case
        {
            boolean assignCentroidsToVectors = random().nextBoolean();
            int randIdx2 = random().nextInt(KMeans.KmeansInitializationMethod.values().length);
            KMeans.KmeansInitializationMethod initializationMethod = KMeans.KmeansInitializationMethod.values()[randIdx2];
            int restarts = random().nextInt(1, 6);
            int iters = random().nextInt(1, 10);
            int sampleSize = random().nextInt(10, nVectors * 2);

            KMeans.Results results = KMeans.cluster(
                vectors,
                nClusters,
                assignCentroidsToVectors,
                random().nextLong(),
                initializationMethod,
                null,
                similarityFunction == VectorSimilarityFunction.COSINE,
                restarts,
                iters,
                sampleSize
            );
            assertResults(results, nClusters, nVectors, assignCentroidsToVectors);
        }
    }

    private void assertResults(KMeans.Results results, int nClusters, int nVectors, boolean assignCentroidsToVectors) {
        assertEquals(nClusters, results.centroids().length);
        if (assignCentroidsToVectors) {
            assertEquals(nClusters, results.centroidsSize().length);
            assertEquals(nVectors, results.vectorCentroids().length);
            int[] centroidsSize = new int[nClusters];
            for (int i = 0; i < nVectors; i++) {
                centroidsSize[results.vectorCentroids()[i]]++;
            }
            assertArrayEquals(centroidsSize, results.centroidsSize());
        } else {
            assertNull(results.vectorCentroids());
        }
    }

    public void testKMeansSpecialCases() throws IOException {
        {
            // nClusters > nVectors
            int nClusters = 20;
            int nVectors = 10;
            FloatVectorValues vectors = generateData(nVectors, 5, nClusters);
            KMeans.Results results = KMeans.cluster(vectors, VectorSimilarityFunction.EUCLIDEAN, nClusters);
            // assert that we get 1 centroid, as nClusters will be adjusted
            assertEquals(1, results.centroids().length);
            assertEquals(nVectors, results.vectorCentroids().length);
        }
        {
            // small sample size
            int sampleSize = 2;
            int nClusters = 2;
            int nVectors = 300;
            FloatVectorValues vectors = generateData(nVectors, 5, nClusters);
            KMeans.KmeansInitializationMethod initializationMethod = KMeans.KmeansInitializationMethod.PLUS_PLUS;
            KMeans.Results results = KMeans.cluster(
                vectors,
                nClusters,
                true,
                random().nextLong(),
                initializationMethod,
                null,
                false,
                1,
                2,
                sampleSize
            );
            assertResults(results, nClusters, nVectors, true);
        }
    }

    public void testKMeansSAllZero() throws IOException {
        int nClusters = 10;
        List<float[]> vectors = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            float[] vector = new float[5];
            vectors.add(vector);
        }
        KMeans.Results results = KMeans.cluster(FloatVectorValues.fromFloats(vectors, 5), VectorSimilarityFunction.EUCLIDEAN, nClusters);
        assertResults(results, nClusters, 1000, true);
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
