/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.outlierdetection;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Performance and accuracy benchmark comparing three outlier detection approaches
 * across multiple data distributions:
 * <ol>
 *   <li><b>Brute-force kNN</b> — O(n²), exact, baseline</li>
 *   <li><b>Sampling + kNN</b> — reservoir sample then kNN on sample</li>
 *   <li><b>Sampling + projection + kNN</b> — sample, reduce dims, then kNN</li>
 * </ol>
 *
 * Distributions tested:
 * <ul>
 *   <li>Gaussian cluster with isolated outliers</li>
 *   <li>Zipf / power-law heavy-tailed magnitudes</li>
 *   <li>Multi-cluster with inter-cluster outliers</li>
 *   <li>Uniform background with extreme outliers</li>
 *   <li>Log-normal (skewed) distribution</li>
 *   <li>Mixed-variance clusters (heteroscedastic)</li>
 *   <li>Zipf per-dimension correlated heavy tails</li>
 * </ul>
 */
public class OutlierDetectionPerformanceBenchmarkTests extends ESTestCase {

    private Path reportDir;

    private Path getReportPath() {
        if (reportDir == null) {
            reportDir = createTempDir("outlier-bench");
        }
        return reportDir.resolve("outlier_bench_full.txt");
    }

    // ────────────────────────────────────────────────────────
    // Scenario 1: Gaussian cluster + isolated directional outliers
    // ────────────────────────────────────────────────────────
    public void testGaussianClusterWithIsolatedOutliers() {
        int[] sizes = { 200, 500, 1000, 2000, 5000 };
        int dims = 50;
        int projDims = 15;
        int numOutliers = 10;
        int k = 5;
        long seed = 42L;

        StringBuilder report = header("SCENARIO 1: Gaussian cluster + isolated directional outliers");

        for (int n : sizes) {
            Random rng = new Random(seed);
            float[][] vectors = new float[n][dims];
            Set<Integer> trueOutliers = new HashSet<>();

            for (int i = 0; i < n - numOutliers; i++) {
                for (int d = 0; d < dims; d++) {
                    vectors[i][d] = (float) (rng.nextGaussian() * 0.5);
                }
            }
            for (int i = n - numOutliers; i < n; i++) {
                trueOutliers.add(i);
                int oi = i - (n - numOutliers);
                for (int d = 0; d < dims; d++) {
                    float offset = (d % numOutliers == oi) ? 50.0f : 0.0f;
                    vectors[i][d] = offset + (float) (rng.nextGaussian() * 0.1);
                }
            }

            runAndAppend(report, vectors, trueOutliers, n, dims, projDims, numOutliers, k, seed, 0.8);
        }

        footer(report, dims, projDims, k, numOutliers);
        writeReport(report, false);
        logReport("Scenario 1 complete. Report at: " + getReportPath());
    }

    // ────────────────────────────────────────────────────────
    // Scenario 2: Zipf / power-law magnitude distribution
    // Most vectors have small magnitudes; a few have huge ones.
    // Outliers are pushed far beyond the natural Zipf tail.
    // ────────────────────────────────────────────────────────
    public void testZipfPowerLawDistribution() {
        int[] sizes = { 200, 500, 1000, 3000 };
        int dims = 30;
        int projDims = 10;
        int numOutliers = 8;
        int k = 5;
        long seed = 77L;
        double alpha = 1.5;

        StringBuilder report = header("SCENARIO 2: Zipf / power-law magnitude (alpha=" + alpha + ")");

        for (int n : sizes) {
            Random rng = new Random(seed);
            float[][] vectors = new float[n][dims];
            Set<Integer> trueOutliers = new HashSet<>();

            // Assign Zipf magnitudes: shuffle ranks, magnitude = 100 / rank^alpha
            int normalCount = n - numOutliers;
            int[] rankOrder = new int[normalCount];
            for (int i = 0; i < normalCount; i++) {
                rankOrder[i] = i;
            }
            for (int i = normalCount - 1; i > 0; i--) {
                int j = rng.nextInt(i + 1);
                int tmp = rankOrder[i];
                rankOrder[i] = rankOrder[j];
                rankOrder[j] = tmp;
            }

            for (int i = 0; i < normalCount; i++) {
                int rank = rankOrder[i] + 1;
                double mag = 100.0 / Math.pow(rank, alpha);
                float[] dir = randomUnitVector(dims, rng);
                for (int d = 0; d < dims; d++) {
                    vectors[i][d] = (float) (dir[d] * mag);
                }
            }

            // Outliers: far beyond the Zipf tail, each in a unique direction
            for (int i = normalCount; i < n; i++) {
                trueOutliers.add(i);
                int oi = i - normalCount;
                float[] dir = randomUnitVector(dims, rng);
                for (int d = 0; d < dims; d++) {
                    vectors[i][d] = dir[d] * 500.0f + ((d % numOutliers == oi) ? 200.0f : 0.0f);
                }
            }

            runAndAppend(report, vectors, trueOutliers, n, dims, projDims, numOutliers, k, seed, 0.7);
        }

        footer(report, dims, projDims, k, numOutliers);
        writeReport(report, true);
    }

    // ────────────────────────────────────────────────────────
    // Scenario 3: Multiple Gaussian clusters + inter-cluster outliers
    // ────────────────────────────────────────────────────────
    public void testMultiClusterWithInterClusterOutliers() {
        int[] sizes = { 300, 600, 1500, 3000 };
        int dims = 40;
        int projDims = 12;
        int numOutliers = 6;
        int k = 5;
        long seed = 99L;
        int numClusters = 3;

        StringBuilder report = header("SCENARIO 3: " + numClusters + " clusters + inter-cluster outliers");

        for (int n : sizes) {
            Random rng = new Random(seed);
            float[][] vectors = new float[n][dims];
            Set<Integer> trueOutliers = new HashSet<>();

            float[][] centers = new float[numClusters][dims];
            for (int c = 0; c < numClusters; c++) {
                for (int d = 0; d < dims; d++) {
                    centers[c][d] = (d % numClusters == c) ? 30.0f : 0.0f;
                }
            }

            int normalCount = n - numOutliers;
            for (int i = 0; i < normalCount; i++) {
                int cluster = i % numClusters;
                for (int d = 0; d < dims; d++) {
                    vectors[i][d] = centers[cluster][d] + (float) (rng.nextGaussian() * 0.4);
                }
            }

            for (int i = normalCount; i < n; i++) {
                trueOutliers.add(i);
                int oi = i - normalCount;
                for (int d = 0; d < dims; d++) {
                    vectors[i][d] = 80.0f + ((d % numOutliers == oi) ? 40.0f : 0.0f) + (float) (rng.nextGaussian() * 0.05);
                }
            }

            runAndAppend(report, vectors, trueOutliers, n, dims, projDims, numOutliers, k, seed, 0.7);
        }

        footer(report, dims, projDims, k, numOutliers);
        writeReport(report, true);
    }

    // ────────────────────────────────────────────────────────
    // Scenario 4: Uniform hypercube + extreme outliers
    // ────────────────────────────────────────────────────────
    public void testUniformBackgroundWithExtremeOutliers() {
        int[] sizes = { 200, 500, 1000, 3000 };
        int dims = 20;
        int projDims = 8;
        int numOutliers = 5;
        int k = 5;
        long seed = 55L;

        StringBuilder report = header("SCENARIO 4: Uniform hypercube + extreme outliers");

        for (int n : sizes) {
            Random rng = new Random(seed);
            float[][] vectors = new float[n][dims];
            Set<Integer> trueOutliers = new HashSet<>();

            int normalCount = n - numOutliers;
            for (int i = 0; i < normalCount; i++) {
                for (int d = 0; d < dims; d++) {
                    vectors[i][d] = (rng.nextFloat() * 2.0f) - 1.0f;
                }
            }

            for (int i = normalCount; i < n; i++) {
                trueOutliers.add(i);
                int oi = i - normalCount;
                for (int d = 0; d < dims; d++) {
                    vectors[i][d] = ((d % numOutliers == oi) ? 100.0f : 0.0f) + (float) (rng.nextGaussian() * 0.01);
                }
            }

            runAndAppend(report, vectors, trueOutliers, n, dims, projDims, numOutliers, k, seed, 0.8);
        }

        footer(report, dims, projDims, k, numOutliers);
        writeReport(report, true);
    }

    // ────────────────────────────────────────────────────────
    // Scenario 5: Log-normal (right-skewed) distribution
    // ────────────────────────────────────────────────────────
    public void testLogNormalSkewedDistribution() {
        int[] sizes = { 200, 500, 1000, 3000 };
        int dims = 25;
        int projDims = 10;
        int numOutliers = 8;
        int k = 5;
        long seed = 33L;

        StringBuilder report = header("SCENARIO 5: Log-normal (skewed) coordinates");

        for (int n : sizes) {
            Random rng = new Random(seed);
            float[][] vectors = new float[n][dims];
            Set<Integer> trueOutliers = new HashSet<>();

            int normalCount = n - numOutliers;
            for (int i = 0; i < normalCount; i++) {
                for (int d = 0; d < dims; d++) {
                    vectors[i][d] = (float) Math.exp(rng.nextGaussian() * 0.5);
                }
            }

            for (int i = normalCount; i < n; i++) {
                trueOutliers.add(i);
                int oi = i - normalCount;
                for (int d = 0; d < dims; d++) {
                    float base = (float) Math.exp(rng.nextGaussian() * 0.5);
                    vectors[i][d] = base + ((d % numOutliers == oi) ? 200.0f : 0.0f);
                }
            }

            runAndAppend(report, vectors, trueOutliers, n, dims, projDims, numOutliers, k, seed, 0.7);
        }

        footer(report, dims, projDims, k, numOutliers);
        writeReport(report, true);
    }

    // ────────────────────────────────────────────────────────
    // Scenario 6: Mixed-variance (tight + diffuse) clusters
    // ────────────────────────────────────────────────────────
    public void testMixedVarianceClusters() {
        int[] sizes = { 300, 600, 1500, 3000 };
        int dims = 30;
        int projDims = 10;
        int numOutliers = 6;
        int k = 5;
        long seed = 88L;

        StringBuilder report = header("SCENARIO 6: Mixed-variance (tight + diffuse) clusters");

        for (int n : sizes) {
            Random rng = new Random(seed);
            float[][] vectors = new float[n][dims];
            Set<Integer> trueOutliers = new HashSet<>();

            int normalCount = n - numOutliers;
            int half = normalCount / 2;

            for (int i = 0; i < half; i++) {
                for (int d = 0; d < dims; d++) {
                    vectors[i][d] = (float) (rng.nextGaussian() * 0.3);
                }
            }

            for (int i = half; i < normalCount; i++) {
                for (int d = 0; d < dims; d++) {
                    vectors[i][d] = 10.0f + (float) (rng.nextGaussian() * 3.0);
                }
            }

            for (int i = normalCount; i < n; i++) {
                trueOutliers.add(i);
                int oi = i - normalCount;
                for (int d = 0; d < dims; d++) {
                    vectors[i][d] = 80.0f + ((d % numOutliers == oi) ? 60.0f : 0.0f) + (float) (rng.nextGaussian() * 0.05);
                }
            }

            runAndAppend(report, vectors, trueOutliers, n, dims, projDims, numOutliers, k, seed, 0.7);
        }

        footer(report, dims, projDims, k, numOutliers);
        writeReport(report, true);
    }

    // ────────────────────────────────────────────────────────
    // Scenario 7: Zipf per-dimension (correlated heavy tails)
    // Each coordinate independently drawn with rank-based magnitude.
    // ────────────────────────────────────────────────────────
    public void testZipfPerDimensionCorrelatedTails() {
        int[] sizes = { 200, 500, 1000, 3000 };
        int dims = 30;
        int projDims = 10;
        int numOutliers = 8;
        int k = 5;
        long seed = 111L;

        StringBuilder report = header("SCENARIO 7: Zipf per-dimension (correlated heavy tails)");

        for (int n : sizes) {
            Random rng = new Random(seed);
            float[][] vectors = new float[n][dims];
            Set<Integer> trueOutliers = new HashSet<>();

            int normalCount = n - numOutliers;
            for (int i = 0; i < normalCount; i++) {
                for (int d = 0; d < dims; d++) {
                    int rank = rng.nextInt(normalCount) + 1;
                    double sign = rng.nextBoolean() ? 1.0 : -1.0;
                    vectors[i][d] = (float) (sign * 10.0 / Math.pow(rank, 0.8));
                }
            }

            for (int i = normalCount; i < n; i++) {
                trueOutliers.add(i);
                int oi = i - normalCount;
                for (int d = 0; d < dims; d++) {
                    vectors[i][d] = ((d % numOutliers == oi) ? 300.0f : 0.0f) + (float) (rng.nextGaussian() * 0.1);
                }
            }

            runAndAppend(report, vectors, trueOutliers, n, dims, projDims, numOutliers, k, seed, 0.7);
        }

        footer(report, dims, projDims, k, numOutliers);
        writeReport(report, true);
    }

    // ════════════════════════════════════════════════════════
    // Speedup test
    // ════════════════════════════════════════════════════════
    public void testSpeedupRatio() {
        int n = 3000;
        int dims = 50;
        int projDims = 15;
        int k = 5;
        int numOutliers = 10;
        long seed = 123L;
        Random rng = new Random(seed);

        float[][] vectors = new float[n][dims];
        for (int i = 0; i < n - numOutliers; i++) {
            for (int d = 0; d < dims; d++) {
                vectors[i][d] = (float) (rng.nextGaussian() * 0.5);
            }
        }
        for (int i = n - numOutliers; i < n; i++) {
            int oi = i - (n - numOutliers);
            for (int d = 0; d < dims; d++) {
                float offset = (d % numOutliers == oi) ? 50.0f : 0.0f;
                vectors[i][d] = offset + (float) (rng.nextGaussian() * 0.1);
            }
        }

        long t0 = System.nanoTime();
        OutlierDetectionAggregator.computeKnnDistances(vectors, k);
        long bfNs = System.nanoTime() - t0;

        int sampleSize = (int) Math.ceil(Math.sqrt(n));
        long t1 = System.nanoTime();
        int[] sampleIdx = reservoirSample(n, sampleSize, seed);
        RandomProjection proj = new RandomProjection(dims, projDims, seed);
        float[][] projAll = proj.projectBatch(vectors);
        float[][] projSample = new float[sampleSize][];
        for (int i = 0; i < sampleSize; i++) {
            projSample[i] = projAll[sampleIdx[i]];
        }
        for (int i = 0; i < n; i++) {
            InternalOutlierDetection.computeKthNnDistance(projAll[i], projSample, k);
        }
        long projNs = System.nanoTime() - t1;

        double speedup = (double) bfNs / projNs;
        String msg = String.format(
            "\nSpeedup at N=%d: brute-force=%.2fms, sample+project=%.2fms, speedup=%.1fx\n",
            n,
            bfNs / 1e6,
            projNs / 1e6,
            speedup
        );
        writeReport(new StringBuilder(msg), true);
        assertTrue("Expected speedup > 1.0, got " + speedup, speedup > 1.0);
    }

    // ════════════════════════════════════════════════════════
    // Scenario 8: Projection dimension sweep
    // Fixed distribution (Gaussian, D=50, N=2000), vary D'
    // ════════════════════════════════════════════════════════
    public void testProjectionDimensionSweep() {
        int n = 2000;
        int dims = 50;
        int[] projDimsArray = { 3, 5, 10, 15, 25, 40, 50 };
        int numOutliers = 10;
        int k = 5;
        long seed = 42L;

        // Generate data once — Gaussian cluster + isolated directional outliers
        Random rng = new Random(seed);
        float[][] vectors = new float[n][dims];
        Set<Integer> trueOutliers = new HashSet<>();

        for (int i = 0; i < n - numOutliers; i++) {
            for (int d = 0; d < dims; d++) {
                vectors[i][d] = (float) (rng.nextGaussian() * 0.5);
            }
        }
        for (int i = n - numOutliers; i < n; i++) {
            trueOutliers.add(i);
            int oi = i - (n - numOutliers);
            for (int d = 0; d < dims; d++) {
                float offset = (d % numOutliers == oi) ? 50.0f : 0.0f;
                vectors[i][d] = offset + (float) (rng.nextGaussian() * 0.1);
            }
        }

        // Brute-force baseline (once)
        long t0 = System.nanoTime();
        double[] bfScores = OutlierDetectionAggregator.computeKnnDistances(vectors, k);
        long bfNs = System.nanoTime() - t0;
        Set<Integer> bfTop = topNSet(bfScores, numOutliers);
        double bfPrec = precision(bfTop, trueOutliers);
        double bfRec = recall(bfTop, trueOutliers);

        int sampleSize = Math.max(k + 1, (int) Math.ceil(Math.sqrt(n)));
        int[] sampleIdx = reservoirSample(n, sampleSize, seed);

        StringBuilder report = new StringBuilder();
        report.append("\n").append("=".repeat(110)).append("\n");
        report.append("SCENARIO 8: Projection dimension sweep (fixed Gaussian, N=").append(n);
        report.append(", D=").append(dims).append(", k=").append(k);
        report.append(", outliers=").append(numOutliers).append(", sample=").append(sampleSize).append(")\n");
        report.append(String.format("%-6s | %-22s | %-22s | %8s | %s%n", "D'", "Brute-Force", "Sample+Project", "Speedup", "Notes"));
        report.append("-".repeat(110)).append("\n");

        // Brute-force row
        report.append(
            String.format(
                "%-6s | %8.2f  P=%.2f R=%.2f | %8s  %4s %4s | %8s | %s%n",
                "N/A",
                bfNs / 1e6,
                bfPrec,
                bfRec,
                "—",
                "—",
                "—",
                "1.0x",
                "exact baseline"
            )
        );

        for (int projDims : projDimsArray) {
            long t1 = System.nanoTime();
            RandomProjection projection = (projDims < dims) ? new RandomProjection(dims, projDims, seed) : null;
            float[][] projAll = new float[n][];
            for (int i = 0; i < n; i++) {
                projAll[i] = projection != null ? projection.project(vectors[i]) : vectors[i];
            }
            float[][] projSample = new float[sampleSize][];
            for (int i = 0; i < sampleSize; i++) {
                projSample[i] = projAll[sampleIdx[i]];
            }
            double[] projScores = new double[n];
            for (int i = 0; i < n; i++) {
                projScores[i] = InternalOutlierDetection.computeKthNnDistance(projAll[i], projSample, k);
            }
            long projNs = System.nanoTime() - t1;

            Set<Integer> projTop = topNSet(projScores, numOutliers);
            double projPrec = precision(projTop, trueOutliers);
            double projRec = recall(projTop, trueOutliers);
            double speedup = (double) bfNs / Math.max(projNs, 1);

            String note = (projDims == dims) ? "no projection (identity)" : "";
            report.append(
                String.format(
                    "%-6d | %8.2f  P=%.2f R=%.2f | %8.2f  P=%.2f R=%.2f | %6.1fx  | %s%n",
                    projDims,
                    bfNs / 1e6,
                    bfPrec,
                    bfRec,
                    projNs / 1e6,
                    projPrec,
                    projRec,
                    speedup,
                    note
                )
            );

            assertTrue("Projection recall too low at D'=" + projDims + " rec=" + projRec, projRec >= 0.5);
        }

        report.append("-".repeat(110)).append("\n");
        report.append("=".repeat(110)).append("\n\n");
        writeReport(report, true);
    }

    // ════════════════════════════════════════════════════════
    // Core benchmark runner
    // ════════════════════════════════════════════════════════

    private void runAndAppend(
        StringBuilder report,
        float[][] vectors,
        Set<Integer> trueOutliers,
        int n,
        int dims,
        int projDims,
        int numOutliers,
        int k,
        long seed,
        double minRecall
    ) {
        // --- 1. Brute-force kNN ---
        long t0 = System.nanoTime();
        double[] bfScores = OutlierDetectionAggregator.computeKnnDistances(vectors, k);
        long bfNs = System.nanoTime() - t0;
        Set<Integer> bfTop = topNSet(bfScores, numOutliers);
        double bfPrec = precision(bfTop, trueOutliers);
        double bfRec = recall(bfTop, trueOutliers);

        // --- 2. Sampling + kNN ---
        int sampleSize = Math.max(k + 1, (int) Math.ceil(Math.sqrt(n)));
        long t1 = System.nanoTime();
        int[] sampleIdx = reservoirSample(n, sampleSize, seed);
        float[][] sampleVecs = new float[sampleSize][];
        for (int i = 0; i < sampleSize; i++) {
            sampleVecs[i] = vectors[sampleIdx[i]];
        }
        double[] sampScores = new double[n];
        for (int i = 0; i < n; i++) {
            sampScores[i] = InternalOutlierDetection.computeKthNnDistance(vectors[i], sampleVecs, k);
        }
        long sampNs = System.nanoTime() - t1;
        Set<Integer> sampTop = topNSet(sampScores, numOutliers);
        double sampPrec = precision(sampTop, trueOutliers);
        double sampRec = recall(sampTop, trueOutliers);

        // --- 3. Sampling + Projection + kNN ---
        long t2 = System.nanoTime();
        RandomProjection projection = new RandomProjection(dims, projDims, seed);
        float[][] projAll = projection.projectBatch(vectors);
        float[][] projSample = new float[sampleSize][];
        for (int i = 0; i < sampleSize; i++) {
            projSample[i] = projAll[sampleIdx[i]];
        }
        double[] projScores = new double[n];
        for (int i = 0; i < n; i++) {
            projScores[i] = InternalOutlierDetection.computeKthNnDistance(projAll[i], projSample, k);
        }
        long projNs = System.nanoTime() - t2;
        Set<Integer> projTop = topNSet(projScores, numOutliers);
        double projPrec = precision(projTop, trueOutliers);
        double projRec = recall(projTop, trueOutliers);

        report.append(
            String.format(
                "%-6d | %8.2f  P=%.2f R=%.2f | %8.2f  P=%.2f R=%.2f | %8.2f  P=%.2f R=%.2f | %5.1fx%n",
                n,
                bfNs / 1e6,
                bfPrec,
                bfRec,
                sampNs / 1e6,
                sampPrec,
                sampRec,
                projNs / 1e6,
                projPrec,
                projRec,
                (double) bfNs / Math.max(projNs, 1)
            )
        );

        assertTrue("BF recall too low at n=" + n + " rec=" + bfRec, bfRec >= minRecall);
        assertTrue("Sampling recall too low at n=" + n + " rec=" + sampRec, sampRec >= minRecall);
        assertTrue("Projection recall too low at n=" + n + " rec=" + projRec, projRec >= minRecall);
    }

    // ════════════════════════════════════════════════════════
    // Helpers
    // ════════════════════════════════════════════════════════

    private static StringBuilder header(String title) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n").append("=".repeat(100)).append("\n");
        sb.append(title).append("\n");
        sb.append(String.format("%-6s | %-22s | %-22s | %-22s | %s%n", "N", "Brute-Force", "Sampling", "Sample+Project", "Speedup"));
        sb.append("-".repeat(100)).append("\n");
        return sb;
    }

    private static void footer(StringBuilder sb, int dims, int projDims, int k, int numOutliers) {
        sb.append("-".repeat(100)).append("\n");
        sb.append(String.format("dims=%d -> %d | k=%d | outliers=%d | sample=ceil(sqrt(N))%n", dims, projDims, k, numOutliers));
        sb.append("=".repeat(100)).append("\n\n");
    }

    private void writeReport(StringBuilder sb, boolean append) {
        try {
            Path path = getReportPath();
            if (append) {
                Files.writeString(path, sb.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } else {
                Files.writeString(path, sb.toString(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            }
        } catch (IOException e) {
            // ignore in test — report is also logged
        }
        // Always log to test output
        logReport(sb.toString());
    }

    private static final Logger benchLogger = LogManager.getLogger(OutlierDetectionPerformanceBenchmarkTests.class);

    private static void logReport(String msg) {
        benchLogger.info(() -> msg);
    }

    private static float[] randomUnitVector(int dims, Random rng) {
        float[] v = new float[dims];
        float norm = 0;
        for (int d = 0; d < dims; d++) {
            v[d] = (float) rng.nextGaussian();
            norm += v[d] * v[d];
        }
        norm = (float) Math.sqrt(norm);
        if (norm > 0) {
            for (int d = 0; d < dims; d++) {
                v[d] /= norm;
            }
        }
        return v;
    }

    private static Set<Integer> topNSet(double[] scores, int n) {
        int[][] indexed = new int[scores.length][1];
        for (int i = 0; i < scores.length; i++) {
            indexed[i][0] = i;
        }
        Arrays.sort(indexed, (a, b) -> Double.compare(scores[b[0]], scores[a[0]]));
        Set<Integer> result = new HashSet<>();
        for (int i = 0; i < Math.min(n, scores.length); i++) {
            result.add(indexed[i][0]);
        }
        return result;
    }

    private static double precision(Set<Integer> predicted, Set<Integer> actual) {
        if (predicted.isEmpty()) return 0.0;
        long tp = predicted.stream().filter(actual::contains).count();
        return (double) tp / predicted.size();
    }

    private static double recall(Set<Integer> predicted, Set<Integer> actual) {
        if (actual.isEmpty()) return 1.0;
        long tp = predicted.stream().filter(actual::contains).count();
        return (double) tp / actual.size();
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
