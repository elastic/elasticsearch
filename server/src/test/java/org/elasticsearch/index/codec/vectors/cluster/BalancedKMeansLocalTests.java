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

import static org.hamcrest.Matchers.containsString;

public class BalancedKMeansLocalTests extends ESTestCase {

    public void testIllegalClustersPerNeighborhood() {
        KMeansLocal kMeansLocal = new BalancedOTKMeansLocalSerial(randomInt(), randomInt());
        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(new float[0][], new int[0], i -> i);
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

    public void testBalancedKMeans() throws IOException {
        // Test to compare LloydKMeansLocal and BalancedKMeansLocal.

        int nClusters = 20;
        int nVectors = nClusters * 256;
        final int sampleSize = nClusters * 64;
        int dims = 256;
        int maxIterations = 20;
        float soarLambda = -1;

        var methods = List.of(
            new LloydKMeansLocalSerial(sampleSize, maxIterations), // reference method
            new BalancedOTKMeansLocalSerial(sampleSize, maxIterations)
        );

        float[] inertias = new float[methods.size()];
        float[] stdClusterSizes = new float[methods.size()];
        // Both geometric means are computed in log domain for numerical stability.
        double geoMeanInertiaRatio = 0; // the geometric mean of the ratios between both inertias
        double geoMeanStdClusterSizes = 0; // the geometric mean of the ratios between the standard deviations of the cluster sizes

        int nTrials = 10;

        for (int trials = 0; trials < nTrials; trials++) {
            KMeansFloatVectorValues vectors = generateData(nVectors, dims, nClusters, 0.5f);

            for (int j = 0; j < methods.size(); j++) {
                float[][] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters);
                int[] assignments = new int[vectors.size()];
                KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(centroids, assignments);
                methods.get(j).cluster(vectors, kMeansIntermediate, nClusters, soarLambda);

                inertias[j] = kMeansMeanInertia(vectors, kMeansIntermediate);
                int[] clusterSizes = clusterSizes(kMeansIntermediate);
                stdClusterSizes[j] = clusterSizesStandardDeviation(clusterSizes);
            }

            geoMeanInertiaRatio += Math.log(inertias[0] / inertias[1]);
            // adding a small constant in case stdClusterSizes[0] == 0:
            geoMeanStdClusterSizes += Math.log((stdClusterSizes[1] + 1e-6f) / (stdClusterSizes[0] + 1e-6f));
        }

        geoMeanInertiaRatio = Math.exp(geoMeanInertiaRatio / nTrials);
        geoMeanStdClusterSizes = Math.exp(geoMeanStdClusterSizes / nTrials);

        // We allow up to 20% increase in inertia:
        assertTrue(geoMeanInertiaRatio > 0.8);
        // We ask for 3X decrease in the size distribution.
        assertTrue(geoMeanStdClusterSizes < 0.33);
    }

    static float kMeansMeanInertia(KMeansFloatVectorValues vectors, KMeansIntermediate kMeansIntermediate) throws IOException {
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

    static int[] clusterSizes(KMeansIntermediate kMeansIntermediate) {
        int[] assignments = kMeansIntermediate.assignments();
        float[][] centroids = kMeansIntermediate.centroids();

        int[] clusterSizes = new int[centroids.length];
        for (int i = 0; i < assignments.length; i++) {
            clusterSizes[assignments[i]]++;
        }
        return clusterSizes;
    }

    static float clusterSizesStandardDeviation(int[] clusterSizes) {
        double avgSize = Arrays.stream(clusterSizes).asDoubleStream().sum() / clusterSizes.length;
        double varSize = Arrays.stream(clusterSizes).asDoubleStream().map(e -> Math.pow(e - avgSize, 2)).sum() / clusterSizes.length;
        return (float) Math.sqrt(varSize);
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
        KMeansFloatVectorValues fvv = KMeansFloatVectorValues.build(vectors, null, 5);

        float[][] centroids = KMeansLocal.pickInitialCentroids(fvv, nClusters);
        BalancedOTKMeansLocal.cluster(fvv, centroids, sampleSize, maxIterations);

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
        KMeansLocal kMeansLocal = new BalancedOTKMeansLocalSerial(sampleSize, maxIterations);
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

    public void testKMeansNeighbors() throws IOException {
        int nClusters = random().nextInt(1, 10);
        int nVectors = random().nextInt(nClusters * 100, nClusters * 200);
        int dims = random().nextInt(2, 20);
        int sampleSize = random().nextInt(100, nVectors + 1);
        int maxIterations = random().nextInt(0, 100);
        int clustersPerNeighborhood = random().nextInt(2, 512);
        float soarLambda = random().nextFloat(0.5f, 1.5f);
        KMeansFloatVectorValues vectors = generateData(nVectors, dims, nClusters, 0.5f);

        float[][] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters);
        LloydKMeansLocal.cluster(vectors, centroids, sampleSize, maxIterations);

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
        KMeansLocal kMeansLocal = new BalancedOTKMeansLocalSerial(sampleSize, maxIterations);
        kMeansLocal.cluster(vectors, kMeansIntermediate, clustersPerNeighborhood, soarLambda);

        assertEquals(nClusters, centroids.length);
        assertNotNull(kMeansIntermediate.soarAssignments());
    }

    private static KMeansFloatVectorValues generateData(int nSamples, int nDims, int nClusters, float standardDeviationPreCluster) {
        List<float[]> vectors = new ArrayList<>(nSamples);
        float[][] centroids = new float[nClusters][nDims];
        // Generate random centroids
        for (int i = 0; i < nClusters; i++) {
            for (int j = 0; j < nDims; j++) {
                centroids[i][j] = random().nextFloat();
            }
        }
        // Generate data points around centroids
        for (int i = 0; i < nSamples; i++) {
            int cluster = random().nextInt(nClusters);
            float[] vector = new float[nDims];
            for (int j = 0; j < nDims; j++) {
                vector[j] = centroids[cluster][j] + (float) random().nextGaussian() * standardDeviationPreCluster;
            }
            vectors.add(vector);
        }

        return KMeansFloatVectorValues.build(vectors, null, nDims);
    }

    public void testConcurrency() throws IOException {
        // Test to compare BalancedKMeansLocalSerial and BalancedKMeansLocalConcurrent.
        final int nClusters = 16;
        final int nVectors = nClusters * 500;
        final int nSamples = nClusters * 256;
        final int dims = 1024;
        final int maxIterations = 10;
        final float soarLambda = -1;
        final int numThreads = randomIntBetween(2, 8);

        KMeansFloatVectorValues vectors = generateData(nVectors, dims, 1, 0.2f);

        try (ExecutorService executorService = Executors.newFixedThreadPool(numThreads)) {
            TaskExecutor taskExecutor = new TaskExecutor(executorService);

            var methods = List.of(
                new BalancedOTKMeansLocalSerial(nSamples / 2, maxIterations),
                new BalancedOTKMeansLocalConcurrent(taskExecutor, numThreads, nSamples / 2, maxIterations)
            );

            double[] inertias = new double[methods.size()];
            double[] stdClusterSizes = new double[methods.size()];
            int[] minClusterSizes = new int[methods.size()];

            for (int j = 0; j < methods.size(); j++) {
                float[][] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters);
                int[] assignments = new int[vectors.size()];
                KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(centroids, assignments);
                methods.get(j).cluster(vectors, kMeansIntermediate, nClusters, soarLambda);

                inertias[j] = kMeansMeanInertia(vectors, kMeansIntermediate);
                int[] clusterSizes = clusterSizes(kMeansIntermediate);

                stdClusterSizes[j] = clusterSizesStandardDeviation(clusterSizes);
                minClusterSizes[j] = Arrays.stream(clusterSizes).min().getAsInt();
            }

            assertEquals(inertias[0], inertias[1], 2e-4 * inertias[0]);
            assertEquals(stdClusterSizes[0], stdClusterSizes[1], 2e-1 * stdClusterSizes[0]);
            assertEquals(minClusterSizes[0], minClusterSizes[1], 2e-1 * minClusterSizes[0]);
        }
    }
}
