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
 * Benchmark comparing all four outlier detection methods (KthNN, TNN, LDoF, LoF)
 * across multiple distributions, measuring:
 * <ul>
 *   <li>Detection accuracy (precision / recall vs planted outliers)</li>
 *   <li>Speedup of the sampling + projection framework over brute-force</li>
 * </ul>
 *
 * For each method, three approaches are timed:
 * <ol>
 *   <li><b>Brute-force</b> — exact scoring on all N vectors in original dims</li>
 *   <li><b>Sample+Project</b> — reservoir sample (sqrt(N)), project to lower dims, then score</li>
 * </ol>
 */
public class OutlierDetectionMethodBenchmarkTests extends ESTestCase {

    private static final Logger benchLogger = LogManager.getLogger(OutlierDetectionMethodBenchmarkTests.class);
    private static final OutlierDetectionMethod[] ALL_METHODS = OutlierDetectionMethod.values();

    private Path reportDir;

    private Path getReportPath() {
        if (reportDir == null) {
            reportDir = createTempDir("method-bench");
        }
        return reportDir.resolve("method_benchmark.txt");
    }

    // ────────────────────────────────────────────────────────
    // Scenario A: Gaussian cluster + isolated directional outliers
    // ────────────────────────────────────────────────────────
    public void testGaussianCluster() {
        int n = 2000;
        int dims = 50;
        int projDims = 15;
        int numOutliers = 10;
        int k = 5;
        long seed = 42L;

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

        StringBuilder report = header("SCENARIO A: Gaussian cluster + isolated outliers (N=" + n + ", D=" + dims + ")");
        runAllMethods(report, vectors, trueOutliers, n, dims, projDims, numOutliers, k, seed);
        footer(report, dims, projDims, k, numOutliers);
        writeReport(report, false);
    }

    // ────────────────────────────────────────────────────────
    // Scenario B: Zipf / power-law magnitude distribution
    // ────────────────────────────────────────────────────────
    public void testZipfDistribution() {
        int n = 2000;
        int dims = 30;
        int projDims = 10;
        int numOutliers = 8;
        int k = 5;
        long seed = 77L;
        double alpha = 1.5;

        Random rng = new Random(seed);
        float[][] vectors = new float[n][dims];
        Set<Integer> trueOutliers = new HashSet<>();

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

        for (int i = normalCount; i < n; i++) {
            trueOutliers.add(i);
            int oi = i - normalCount;
            float[] dir = randomUnitVector(dims, rng);
            for (int d = 0; d < dims; d++) {
                vectors[i][d] = dir[d] * 500.0f + ((d % numOutliers == oi) ? 200.0f : 0.0f);
            }
        }

        StringBuilder report = header("SCENARIO B: Zipf / power-law (N=" + n + ", D=" + dims + ", alpha=" + alpha + ")");
        runAllMethods(report, vectors, trueOutliers, n, dims, projDims, numOutliers, k, seed);
        footer(report, dims, projDims, k, numOutliers);
        writeReport(report, true);
    }

    // ────────────────────────────────────────────────────────
    // Scenario C: Multi-cluster + inter-cluster outliers
    // ────────────────────────────────────────────────────────
    public void testMultiCluster() {
        int n = 2000;
        int dims = 40;
        int projDims = 12;
        int numOutliers = 6;
        int k = 5;
        long seed = 99L;
        int numClusters = 3;

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

        StringBuilder report = header("SCENARIO C: Multi-cluster + inter-cluster outliers (N=" + n + ", D=" + dims + ")");
        runAllMethods(report, vectors, trueOutliers, n, dims, projDims, numOutliers, k, seed);
        footer(report, dims, projDims, k, numOutliers);
        writeReport(report, true);
    }

    // ────────────────────────────────────────────────────────
    // Scenario D: Mixed-variance (tight + diffuse) clusters
    // ────────────────────────────────────────────────────────
    public void testMixedVariance() {
        int n = 2000;
        int dims = 30;
        int projDims = 10;
        int numOutliers = 6;
        int k = 5;
        long seed = 88L;

        Random rng = new Random(seed);
        float[][] vectors = new float[n][dims];
        Set<Integer> trueOutliers = new HashSet<>();

        int normalCount = n - numOutliers;
        int half = normalCount / 2;

        // Tight cluster
        for (int i = 0; i < half; i++) {
            for (int d = 0; d < dims; d++) {
                vectors[i][d] = (float) (rng.nextGaussian() * 0.3);
            }
        }
        // Diffuse cluster
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

        StringBuilder report = header("SCENARIO D: Mixed-variance clusters (N=" + n + ", D=" + dims + ")");
        runAllMethods(report, vectors, trueOutliers, n, dims, projDims, numOutliers, k, seed);
        footer(report, dims, projDims, k, numOutliers);
        writeReport(report, true);
    }

    // ────────────────────────────────────────────────────────
    // Scenario E: Log-normal skewed distribution
    // ────────────────────────────────────────────────────────
    public void testLogNormal() {
        int n = 2000;
        int dims = 25;
        int projDims = 10;
        int numOutliers = 8;
        int k = 5;
        long seed = 33L;

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

        StringBuilder report = header("SCENARIO E: Log-normal skewed (N=" + n + ", D=" + dims + ")");
        runAllMethods(report, vectors, trueOutliers, n, dims, projDims, numOutliers, k, seed);
        footer(report, dims, projDims, k, numOutliers);
        writeReport(report, true);
    }

    // ────────────────────────────────────────────────────────
    // Scenario F: Summary table across all distributions
    // For a single size, compare all methods side-by-side.
    // ────────────────────────────────────────────────────────
    public void testSummaryComparison() {
        long seed = 42L;
        int k = 5;
        int numOutliers = 10;

        // We'll use a single representative distribution: Gaussian N=3000
        int n = 3000;
        int dims = 50;
        int projDims = 15;

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

        int sampleSize = Math.max(k + 1, (int) Math.ceil(Math.sqrt(n)));
        int[] sampleIdx = reservoirSample(n, sampleSize, seed);

        StringBuilder report = new StringBuilder();
        report.append("\n").append("=".repeat(120)).append("\n");
        report.append("SUMMARY: All methods comparison (N=").append(n).append(", D=").append(dims);
        report.append("→").append(projDims).append(", k=").append(k).append(", outliers=").append(numOutliers);
        report.append(", sample=").append(sampleSize).append(")\n");
        report.append(
            String.format(
                "%-8s | %26s | %26s | %8s | %s%n",
                "Method",
                "Brute-Force (ms P R)",
                "Sample+Project (ms P R)",
                "Speedup",
                "Fidelity"
            )
        );
        report.append("-".repeat(120)).append("\n");

        for (OutlierDetectionMethod method : ALL_METHODS) {
            // Brute-force: exact scoring on all vectors, original dims
            long t0 = System.nanoTime();
            double[] bfScores = OutlierDetectionAggregator.computeScores(vectors, k, method);
            long bfNs = System.nanoTime() - t0;
            Set<Integer> bfTop = topNSet(bfScores, numOutliers);
            double bfPrec = precision(bfTop, trueOutliers);
            double bfRec = recall(bfTop, trueOutliers);

            // Sample + Project
            long t1 = System.nanoTime();
            RandomProjection projection = new RandomProjection(dims, projDims, seed);
            float[][] projAll = projection.projectBatch(vectors);
            float[][] projSample = new float[sampleSize][];
            for (int i = 0; i < sampleSize; i++) {
                projSample[i] = projAll[sampleIdx[i]];
            }
            // Score all vectors against the sample using the method
            double[] projScores = scoreAllAgainstSample(projAll, projSample, k, method);
            long projNs = System.nanoTime() - t1;
            Set<Integer> projTop = topNSet(projScores, numOutliers);
            double projPrec = precision(projTop, trueOutliers);
            double projRec = recall(projTop, trueOutliers);

            double speedup = (double) bfNs / Math.max(projNs, 1);

            report.append(
                String.format(
                    "%-8s | %8.2f  P=%.2f R=%.2f   | %8.2f  P=%.2f R=%.2f   | %6.1fx | F=%.2f%n",
                    method.name(),
                    bfNs / 1e6,
                    bfPrec,
                    bfRec,
                    projNs / 1e6,
                    projPrec,
                    projRec,
                    speedup,
                    fidelity(projTop, bfTop)
                )
            );

            // KthNN and TNN should always detect well-separated outliers
            if (method == OutlierDetectionMethod.KTH_NN || method == OutlierDetectionMethod.TNN) {
                assertTrue("[" + method + "] BF recall too low: " + bfRec, bfRec >= 0.5);
            }
            // Framework should preserve brute-force results (fidelity)
            double fidelity = fidelity(projTop, bfTop);
            assertTrue("[" + method + "] Framework fidelity too low: " + fidelity, fidelity >= 0.3);
        }

        report.append("-".repeat(120)).append("\n");
        report.append("=".repeat(120)).append("\n\n");
        writeReport(report, true);
    }

    // ════════════════════════════════════════════════════════
    // Core runner: for each method, compare brute-force vs sampling+projection
    // ════════════════════════════════════════════════════════

    private void runAllMethods(
        StringBuilder report,
        float[][] vectors,
        Set<Integer> trueOutliers,
        int n,
        int dims,
        int projDims,
        int numOutliers,
        int k,
        long seed
    ) {
        int sampleSize = Math.max(k + 1, (int) Math.ceil(Math.sqrt(n)));
        int[] sampleIdx = reservoirSample(n, sampleSize, seed);

        for (OutlierDetectionMethod method : ALL_METHODS) {
            // --- Brute-force: exact scoring on all N vectors in original dims ---
            long t0 = System.nanoTime();
            double[] bfScores = OutlierDetectionAggregator.computeScores(vectors, k, method);
            long bfNs = System.nanoTime() - t0;
            Set<Integer> bfTop = topNSet(bfScores, numOutliers);
            double bfPrec = precision(bfTop, trueOutliers);
            double bfRec = recall(bfTop, trueOutliers);

            // --- Sample + Projection: scoring on sample in reduced dims ---
            long t1 = System.nanoTime();
            RandomProjection projection = new RandomProjection(dims, projDims, seed);
            float[][] projAll = projection.projectBatch(vectors);
            float[][] projSample = new float[sampleSize][];
            for (int i = 0; i < sampleSize; i++) {
                projSample[i] = projAll[sampleIdx[i]];
            }
            double[] projScores = scoreAllAgainstSample(projAll, projSample, k, method);
            long projNs = System.nanoTime() - t1;
            Set<Integer> projTop = topNSet(projScores, numOutliers);
            double projPrec = precision(projTop, trueOutliers);
            double projRec = recall(projTop, trueOutliers);

            // Framework fidelity: overlap between framework top-N and brute-force top-N
            double fidelity = fidelity(projTop, bfTop);

            double speedup = (double) bfNs / Math.max(projNs, 1);

            report.append(
                String.format(
                    "  %-8s | %8.2f  P=%.2f R=%.2f | %8.2f  P=%.2f R=%.2f | %6.1fx | F=%.2f%n",
                    method.name(),
                    bfNs / 1e6,
                    bfPrec,
                    bfRec,
                    projNs / 1e6,
                    projPrec,
                    projRec,
                    speedup,
                    fidelity
                )
            );

            // KthNN and TNN should always detect well-separated outliers
            if (method == OutlierDetectionMethod.KTH_NN || method == OutlierDetectionMethod.TNN) {
                assertTrue("[" + method + "] BF recall too low: " + bfRec, bfRec >= 0.5);
                assertTrue("[" + method + "] Framework recall too low: " + projRec, projRec >= 0.5);
            }
        }
    }

    /**
     * Score every vector against the sample using the specified method.
     * For KthNN and TNN this is per-point.
     * For LDoF and LoF this requires precomputed sample-level data.
     */
    private static double[] scoreAllAgainstSample(float[][] allVectors, float[][] sample, int k, OutlierDetectionMethod method) {
        int n = allVectors.length;
        double[] scores = new double[n];

        // Precompute sample-level information as needed
        double[] sampleTnn = null;
        double[] sampleKDist = null;
        double[] sampleLrd = null;

        if (method == OutlierDetectionMethod.LDOF) {
            sampleTnn = InternalOutlierDetection.precomputeSampleTnn(sample, k);
        } else if (method == OutlierDetectionMethod.LOF) {
            sampleKDist = InternalOutlierDetection.precomputeSampleKDist(sample, k);
            sampleLrd = InternalOutlierDetection.precomputeSampleLrd(sample, k, sampleKDist);
        }

        for (int i = 0; i < n; i++) {
            scores[i] = InternalOutlierDetection.reScoreCandidate(allVectors[i], sample, k, method, sampleTnn, sampleKDist, sampleLrd);
        }
        return scores;
    }

    // ════════════════════════════════════════════════════════
    // Helpers
    // ════════════════════════════════════════════════════════

    private static StringBuilder header(String title) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n").append("=".repeat(110)).append("\n");
        sb.append(title).append("\n");
        sb.append(String.format("  %-8s | %-22s | %-22s | %s | %s%n", "Method", "Brute-Force", "Sample+Project", "Speedup", "Fidelity"));
        sb.append("-".repeat(110)).append("\n");
        return sb;
    }

    private static void footer(StringBuilder sb, int dims, int projDims, int k, int numOutliers) {
        sb.append("-".repeat(110)).append("\n");
        sb.append(String.format("  dims=%d -> %d | k=%d | outliers=%d | sample=ceil(sqrt(N))%n", dims, projDims, k, numOutliers));
        sb.append("  Fidelity = overlap between framework top-N and brute-force top-N (same method)\n");
        sb.append("=".repeat(110)).append("\n\n");
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
        benchLogger.info(() -> sb.toString());
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

    /**
     * Fidelity: fraction of the brute-force top-N also found by the framework top-N.
     */
    private static double fidelity(Set<Integer> framework, Set<Integer> bruteForce) {
        if (bruteForce.isEmpty()) return 1.0;
        long overlap = framework.stream().filter(bruteForce::contains).count();
        return (double) overlap / bruteForce.size();
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
