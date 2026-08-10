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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Float-specific balanced KMeans tests, including inertia comparison and concurrency tests
 * that rely on {@link VectorUtil#squareDistance}.
 */
public class FloatBalancedKMeansLocalTests extends AbstractBalancedKMeansLocalTestCase<float[]> {

    @Override
    protected CentroidOps<float[]> centroidOps() {
        return CentroidOps.FLOAT;
    }

    @Override
    protected ClusteringVectorValues<float[]> generateData(int nSamples, int nDims, int nClusters) {
        return KMeansTestData.generateFloatDataWithStdDev(nSamples, nDims, nClusters, 0.5f);
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

    /**
     * Compares inertia and cluster balance between LloydKMeansLocal and BalancedOTKMeansLocal.
     * This test is float-only because it computes inertia via VectorUtil.squareDistance.
     */
    public void testBalancedKMeans() throws IOException {
        int nClusters = 20;
        int nVectors = nClusters * 256;
        final int sampleSize = nClusters * 64;
        int dims = 256;
        int maxIterations = 20;

        var methods = List.of(
            new LloydKMeansLocalSerial<>(CentroidOps.FLOAT, sampleSize, maxIterations), // reference method
            new BalancedOTKMeansLocalSerial<>(CentroidOps.FLOAT, sampleSize, maxIterations)
        );

        float[] inertias = new float[methods.size()];
        float[] stdClusterSizes = new float[methods.size()];
        // Both geometric means are computed in log domain for numerical stability.
        double geoMeanInertiaRatio = 0; // the geometric mean of the ratios between both inertias
        double geoMeanStdClusterSizes = 0; // the geometric mean of the ratios between the standard deviations of the cluster sizes

        int nTrials = 10;

        for (int trials = 0; trials < nTrials; trials++) {
            KMeansFloatVectorValues vectors = KMeansTestData.generateFloatDataWithStdDev(nVectors, dims, nClusters, 0.5f);

            for (int j = 0; j < methods.size(); j++) {
                float[][] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters, CentroidOps.FLOAT);
                int[] assignments = new int[vectors.size()];
                KMeansResult<float[]> kMeansResult = new KMeansResult<>(centroids, assignments);
                methods.get(j).cluster(vectors, kMeansResult, nClusters);

                inertias[j] = kMeansMeanInertia(vectors, kMeansResult);
                int[] clusterSizes = clusterSizes(kMeansResult);
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

    // Test to compare BalancedKMeansLocalSerial and BalancedKMeansLocalConcurrent.
    public void testConcurrency() throws IOException {
        final int nClusters = 16;
        final int nVectors = nClusters * 500;
        final int nSamples = nClusters * 256;
        final int dims = 1024;
        final int maxIterations = 10;
        final int numThreads = randomIntBetween(2, 8);

        KMeansFloatVectorValues vectors = KMeansTestData.generateFloatDataWithStdDev(nVectors, dims, 1, 0.2f);

        try (ExecutorService executorService = Executors.newFixedThreadPool(numThreads)) {
            TaskExecutor taskExecutor = new TaskExecutor(executorService);

            var methods = List.of(
                new BalancedOTKMeansLocalSerial<>(CentroidOps.FLOAT, nSamples / 2, maxIterations),
                new BalancedOTKMeansLocalConcurrent<>(CentroidOps.FLOAT, taskExecutor, numThreads, nSamples / 2, maxIterations)
            );

            double[] inertias = new double[methods.size()];
            double[] stdClusterSizesArr = new double[methods.size()];
            int[] minClusterSizes = new int[methods.size()];

            for (int j = 0; j < methods.size(); j++) {
                float[][] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters, CentroidOps.FLOAT);
                int[] assignments = new int[vectors.size()];
                KMeansResult<float[]> kMeansResult = new KMeansResult<>(centroids, assignments);
                methods.get(j).cluster(vectors, kMeansResult, nClusters);

                inertias[j] = kMeansMeanInertia(vectors, kMeansResult);
                int[] clusterSizesArr = clusterSizes(kMeansResult);

                stdClusterSizesArr[j] = clusterSizesStandardDeviation(clusterSizesArr);
                minClusterSizes[j] = Arrays.stream(clusterSizesArr).min().getAsInt();
            }

            assertEquals(inertias[0], inertias[1], 2e-4 * inertias[0]);
            assertEquals(stdClusterSizesArr[0], stdClusterSizesArr[1], 2e-1 * stdClusterSizesArr[0]);
            assertEquals(minClusterSizes[0], minClusterSizes[1], 2e-1 * minClusterSizes[0]);
        }
    }

    // ---- Helpers ----

    static float kMeansMeanInertia(KMeansFloatVectorValues vectors, KMeansResult<float[]> kMeansResult) throws IOException {
        int[] assignments = kMeansResult.assignments();
        float[][] centroids = kMeansResult.centroids();

        float mse = 0;
        for (int i = 0; i < vectors.size(); i++) {
            float[] vec = vectors.vectorValue(i);
            float[] cent = centroids[assignments[i]];
            float dist = VectorUtil.squareDistance(vec, cent);
            mse += dist / vectors.size();
        }
        return mse;
    }

    static int[] clusterSizes(KMeansResult<float[]> kMeansResult) {
        int[] assignments = kMeansResult.assignments();
        float[][] centroids = kMeansResult.centroids();

        int[] clusterSizes = new int[centroids.length];
        for (int assignment : assignments) {
            clusterSizes[assignment]++;
        }
        return clusterSizes;
    }

    static float clusterSizesStandardDeviation(int[] clusterSizes) {
        double avgSize = Arrays.stream(clusterSizes).asDoubleStream().sum() / clusterSizes.length;
        double varSize = Arrays.stream(clusterSizes).asDoubleStream().map(e -> Math.pow(e - avgSize, 2)).sum() / clusterSizes.length;
        return (float) Math.sqrt(varSize);
    }
}
