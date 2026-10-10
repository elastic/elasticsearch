/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.outlierdetection;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Benchmark-style tests that validate algorithm quality and performance characteristics.
 * These are not micro-benchmarks but correctness + quality tests for the outlier detection approach.
 */
public class OutlierDetectionBenchmarkTests extends ESTestCase {

    /**
     * Tests that sampling-based kNN outlier detection recovers known outliers
     * when injected into a Gaussian cluster. Validates quality vs full computation.
     */
    public void testSamplingQualityVsFullComputation() {
        int n = 500;
        int dims = 10;
        int numOutliers = 5;
        int k = 5;
        long seed = 42L;
        Random rng = new Random(seed);

        float[][] allVectors = new float[n][dims];
        Set<Integer> outlierIndices = new HashSet<>();
        for (int i = 0; i < n - numOutliers; i++) {
            for (int d = 0; d < dims; d++) {
                allVectors[i][d] = (float) (rng.nextGaussian() * 0.5);
            }
        }
        for (int i = n - numOutliers; i < n; i++) {
            outlierIndices.add(i);
            for (int d = 0; d < dims; d++) {
                allVectors[i][d] = (float) (20 + rng.nextGaussian() * 0.1);
            }
        }

        double[] fullScores = OutlierDetectionAggregator.computeKnnDistances(allVectors, k);

        int[] fullTopIndices = topN(fullScores, numOutliers);
        Set<Integer> fullTopSet = new HashSet<>();
        for (int idx : fullTopIndices) {
            fullTopSet.add(idx);
        }

        for (int outlierIdx : outlierIndices) {
            assertTrue("Full computation should find outlier at index " + outlierIdx, fullTopSet.contains(outlierIdx));
        }

        int sampleSize = (int) Math.ceil(Math.sqrt(n));
        int[] sampleIndices = reservoirSample(n, sampleSize, seed);
        float[][] sampleVectors = new float[sampleSize][];
        for (int i = 0; i < sampleSize; i++) {
            sampleVectors[i] = allVectors[sampleIndices[i]];
        }

        Set<Integer> sampledTopSet = new HashSet<>();
        double[] sampledScores = new double[n];
        for (int i = 0; i < n; i++) {
            sampledScores[i] = InternalOutlierDetection.computeKthNnDistance(allVectors[i], sampleVectors, k);
        }
        int[] sampledTopIndices = topN(sampledScores, numOutliers);
        for (int idx : sampledTopIndices) {
            sampledTopSet.add(idx);
        }

        for (int outlierIdx : outlierIndices) {
            assertTrue("Sampling-based approach should find outlier at index " + outlierIdx, sampledTopSet.contains(outlierIdx));
        }
    }

    /**
     * Tests that random projection preserves outlier ranking.
     */
    public void testRandomProjectionPreservesOutlierRanking() {
        int n = 200;
        int originalDims = 100;
        int projectedDims = 20;
        int numOutliers = 3;
        int k = 5;
        long seed = 42L;
        Random rng = new Random(seed);

        float[][] vectors = new float[n][originalDims];
        Set<Integer> outlierIndices = new HashSet<>();

        for (int i = 0; i < n - numOutliers; i++) {
            for (int d = 0; d < originalDims; d++) {
                vectors[i][d] = (float) (rng.nextGaussian() * 0.5);
            }
        }
        for (int i = n - numOutliers; i < n; i++) {
            outlierIndices.add(i);
            for (int d = 0; d < originalDims; d++) {
                vectors[i][d] = (float) (15 + rng.nextGaussian() * 0.1);
            }
        }

        RandomProjection projection = new RandomProjection(originalDims, projectedDims, seed);
        float[][] projected = projection.projectBatch(vectors);

        double[] projectedScores = OutlierDetectionAggregator.computeKnnDistances(projected, k);
        int[] topIndices = topN(projectedScores, numOutliers);
        Set<Integer> topSet = new HashSet<>();
        for (int idx : topIndices) {
            topSet.add(idx);
        }

        for (int outlierIdx : outlierIndices) {
            assertTrue("Projected kNN should find outlier at index " + outlierIdx, topSet.contains(outlierIdx));
        }
    }

    /**
     * Smoke test: verify kNN computation completes at different data sizes
     * and that outliers injected at each size are detected.
     */
    public void testScalingBehavior() {
        int[] sizes = { 50, 100, 500 };
        int dims = 10;
        int k = 5;
        Random rng = new Random(42L);

        for (int size : sizes) {
            float[][] vectors = new float[size][dims];
            for (int i = 0; i < size - 1; i++) {
                for (int d = 0; d < dims; d++) {
                    vectors[i][d] = (float) (rng.nextGaussian() * 0.5);
                }
            }
            for (int d = 0; d < dims; d++) {
                vectors[size - 1][d] = 50.0f;
            }

            double[] scores = OutlierDetectionAggregator.computeKnnDistances(vectors, k);
            assertEquals(size, scores.length);

            int maxIdx = 0;
            for (int i = 1; i < scores.length; i++) {
                if (scores[i] > scores[maxIdx]) {
                    maxIdx = i;
                }
            }
            assertEquals("Outlier should be detected at size " + size, size - 1, maxIdx);
        }
    }

    private static int[] topN(double[] scores, int n) {
        int[][] indexed = new int[scores.length][1];
        for (int i = 0; i < scores.length; i++) {
            indexed[i][0] = i;
        }
        Arrays.sort(indexed, (a, b) -> Double.compare(scores[b[0]], scores[a[0]]));
        int[] result = new int[Math.min(n, scores.length)];
        for (int i = 0; i < result.length; i++) {
            result[i] = indexed[i][0];
        }
        return result;
    }

    private static int[] reservoirSample(int n, int k, long seed) {
        Random rng = new Random(seed);
        int[] reservoir = new int[k];
        for (int i = 0; i < k; i++) {
            reservoir[i] = i;
        }
        for (int i = k; i < n; i++) {
            int r = rng.nextInt(i + 1);
            if (r < k) {
                reservoir[r] = i;
            }
        }
        return reservoir;
    }
}
