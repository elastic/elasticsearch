/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.elasticsearch.test.ESTestCase;

import java.util.Random;
import java.util.function.IntFunction;

/**
 * Tests for the core ASH algorithm components: SVD, quantizers, and the full pipeline.
 */
public class AsymmetricHashingQuantizerTests extends ESTestCase {

    public void testSvdIdentity() {
        // SVD of identity should give identity
        float[][] identity = { { 1, 0, 0 }, { 0, 1, 0 }, { 0, 0, 1 } };
        SvdUtil.SvdResult result = SvdUtil.thinSvd(identity, 3, 3);
        // All singular values should be 1
        for (float s : result.s()) {
            assertEquals(1.0f, s, 1e-5f);
        }
    }

    public void testSvdRank1() {
        // Rank-1 matrix: outer product
        float[][] a = new float[4][3];
        float[] u = { 1, 2, 3, 4 };
        float[] v = { 0.5f, 0.3f, 0.1f };
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 3; j++) {
                a[i][j] = u[i] * v[j];
            }
        }
        SvdUtil.SvdResult result = SvdUtil.thinSvd(a, 4, 3);
        // Only first singular value should be non-zero
        assertTrue(result.s()[0] > 0.1f);
        assertEquals(0.0f, result.s()[1], 1e-4f);
        assertEquals(0.0f, result.s()[2], 1e-4f);
    }

    public void testProcrustesOrthogonal() {
        // Procrustes of a random matrix should return orthogonal matrix (R^T R = I)
        Random rng = new Random(42);
        int k = 5;
        float[][] m = new float[k][k];
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < k; j++) {
                m[i][j] = (float) rng.nextGaussian();
            }
        }
        float[][] r = SvdUtil.procrustes(m, k);
        // Check R^T R ~= I
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < k; j++) {
                double dot = 0;
                for (int l = 0; l < k; l++) {
                    dot += (double) r[l][i] * r[l][j];
                }
                float expected = (i == j) ? 1.0f : 0.0f;
                assertEquals(expected, (float) dot, 1e-4f);
            }
        }
    }

    public void testBinaryQuantizer() {
        AshBinaryQuantizer bq = new AshBinaryQuantizer();
        float[][] x = { { 1.0f, -2.0f, 0.5f, -0.1f }, { -1.0f, 3.0f, -0.5f, 0.1f } };
        AshDimQuantizer.QuantizeResult result = bq.encode(x);

        // Signs should be preserved
        assertEquals(1.0f, result.centeredCodes()[0][0], 0);
        assertEquals(-1.0f, result.centeredCodes()[0][1], 0);
        assertEquals(1.0f, result.centeredCodes()[0][2], 0);
        assertEquals(-1.0f, result.centeredCodes()[0][3], 0);

        // Norm of {1,-1,1,-1} = 2
        assertEquals(2.0f, result.codeNorms()[0], 1e-5f);
    }

    public void testSphericalScalarQuantizer2Bit() {
        AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(2);
        float[][] x = { { 0.8f, -0.5f, 0.3f, -0.9f } };
        AshDimQuantizer.QuantizeResult result = ssq.encode(x);

        // Codes should be centered: sign * (0.5 + level)
        // With 2 bits, levels are 0 or 1, so magnitudes are 0.5 or 1.5
        for (float val : result.centeredCodes()[0]) {
            float absMag = Math.abs(val);
            assertTrue("Expected magnitude 0.5 or 1.5 but got " + absMag, absMag == 0.5f || absMag == 1.5f);
        }
        assertTrue(result.codeNorms()[0] > 0);
    }

    public void testFullPipelineRandomMethod() {
        int nVectors = 100;
        int dim = 16;
        float projectedDimsFraction = 0.25f; // 16 * 0.25 = 4 projected dims
        int bitsPerDim = 1;
        Random rng = new Random(123);

        float[][] vectors = new float[nVectors][dim];
        for (int i = 0; i < nVectors; i++) {
            for (int j = 0; j < dim; j++) {
                vectors[i][j] = (float) rng.nextGaussian();
            }
        }

        // Single centroid (mean)
        float[][] centroids = new float[1][dim];
        for (int i = 0; i < nVectors; i++) {
            for (int j = 0; j < dim; j++) {
                centroids[0][j] += vectors[i][j];
            }
        }
        for (int j = 0; j < dim; j++) {
            centroids[0][j] /= nVectors;
        }
        int[] assignments = new int[nVectors]; // all zero

        IntFunction<float[]> centroidGetter = (i) -> centroids[assignments[i]];

        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            projectedDimsFraction,
            bitsPerDim,
            AsymmetricHashingQuantizer.Method.RANDOM,
            0,
            10,
            42L
        );

        float[][] w = quantizer.train(vectors, centroidGetter);
        assertNotNull(w);
        assertEquals(dim, w.length);

        int expectedNDims = (int) (dim * projectedDimsFraction);
        assertEquals(expectedNDims, w[0].length);

        AsymmetricHashingResult result = quantizer.encode(vectors, centroidGetter, assignments, w);
        assertEquals(nVectors, result.encodedVectors().length);
        assertEquals(nVectors, result.scales().length);
        assertEquals(nVectors, result.offsets().length);
    }

    public void testFullPipelineLearnedMethod() {
        int nVectors = 200;
        int dim = 32;
        float projectedDimsFraction = 0.25f; // 32 * 0.25 = 8 projected dims
        int bitsPerDim = 1;
        Random rng = new Random(456);

        float[][] vectors = new float[nVectors][dim];
        for (int i = 0; i < nVectors; i++) {
            for (int j = 0; j < dim; j++) {
                vectors[i][j] = (float) rng.nextGaussian();
            }
        }

        float[][] centroids = new float[1][dim];
        for (int i = 0; i < nVectors; i++) {
            for (int j = 0; j < dim; j++) {
                centroids[0][j] += vectors[i][j];
            }
        }
        for (int j = 0; j < dim; j++) {
            centroids[0][j] /= nVectors;
        }
        int[] assignments = new int[nVectors];

        IntFunction<float[]> centroidGetter = (i) -> centroids[assignments[i]];

        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            projectedDimsFraction,
            bitsPerDim,
            AsymmetricHashingQuantizer.Method.LEARNED,
            5,
            10,
            42L
        );

        float[][] w = quantizer.train(vectors, centroidGetter);
        AsymmetricHashingResult result = quantizer.encode(vectors, centroidGetter, assignments, w);

        // Score a query against the encoded vectors
        float[] query = new float[dim];
        for (int j = 0; j < dim; j++) {
            query[j] = (float) rng.nextGaussian();
        }
        float[] scores = AsymmetricHashingScorer.score(
            query,
            w,
            centroids,
            assignments,
            result.encodedVectors(),
            result.scales(),
            result.offsets()
        );
        assertEquals(nVectors, scores.length);

        // Verify approximate dot products correlate with exact ones
        double correlation = computeRankCorrelation(vectors, query, scores);
        // With learned method, expect reasonable correlation
        assertTrue("Expected positive rank correlation, got " + correlation, correlation > 0.3);
    }

    public void testScorerSingleVector() {
        int dim = 4;
        int nDims = 2;
        float[] query = { 1.0f, 0.5f, -0.3f, 0.8f };
        float[][] w = { { 1, 0 }, { 0, 1 }, { 0, 0 }, { 0, 0 } }; // project to first 2 dims
        float[][] centroids = { { 0, 0, 0, 0 } };
        float[] encodedVector = { 1.0f, -1.0f };
        float scale = 1.0f;
        float offset = 0.0f;

        // queryTransformed = (query - centroid) @ W = [1.0, 0.5]
        // dot = 1.0*1.0 + 0.5*(-1.0) = 0.5
        // result = 0.5 * scale + queryDotCentroid + offset = 0.5 + 0 + 0 = 0.5
        float score = AsymmetricHashingScorer.scoreOneVector(new float[] { 1.0f, 0.5f }, 0.0f, encodedVector, scale, offset);
        assertEquals(0.5f, score, 1e-5f);
    }

    public void testBinaryPackAndScore() {
        // Test bit-packing roundtrip and binary scorer equivalence
        float[] codes = { 1.0f, -1.0f, 1.0f, 1.0f, -1.0f, -1.0f, 1.0f, -1.0f, 1.0f, -1.0f };
        byte[] packed = AsymmetricHashingScorer.packBinaryCodes(codes);
        assertEquals(2, packed.length); // ceil(10/8) = 2

        // Score with binary scorer should match float scorer
        float[] qt = { 0.5f, 0.3f, -0.2f, 0.8f, 0.1f, -0.4f, 0.6f, -0.7f, 0.9f, -0.1f };
        float scale = 1.5f;
        float offset = 0.2f;
        float qdc = 0.3f;

        float floatScore = AsymmetricHashingScorer.scoreOneVector(qt, qdc, codes, scale, offset);
        float binaryScore = AsymmetricHashingScorer.scoreOneVectorBinary(qt, qdc, packed, codes.length, scale, offset);
        assertEquals(floatScore, binaryScore, 1e-5f);
    }

    public void testFallbackToRandomWhenTooFewVectors() {
        // With only 2 vectors and nDims=4, learned method should fall back to random
        int dim = 16;
        float[][] vectors = {
            { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 },
            { 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 } };
        float[][] centroids = { new float[dim] };
        int[] assignments = { 0, 0 };

        IntFunction<float[]> centroidGetter = (i) -> centroids[assignments[i]];

        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            0.25f,
            1,
            AsymmetricHashingQuantizer.Method.LEARNED,
            5,
            10,
            42L
        );

        // Should not throw -- falls back to random
        float[][] w = quantizer.train(vectors, centroidGetter);
        assertNotNull(w);
        assertEquals(dim, w.length);
    }

    public void testMultiBitPackAndScore() {
        // 2-bit quantizer: levels are -1.5, -0.5, 0.5, 1.5
        int bitsPerDim = 2;
        int nDims = 10;
        float[] codes = { 0.5f, -1.5f, 1.5f, -0.5f, 0.5f, 1.5f, -0.5f, -1.5f, 0.5f, 1.5f };
        byte[] packed = AsymmetricHashingScorer.packMultiBitCodes(codes, bitsPerDim);
        assertEquals(bitsPerDim * ((nDims + 7) >>> 3), packed.length);

        float[] qt = { 0.5f, 0.3f, -0.2f, 0.8f, 0.1f, -0.4f, 0.6f, -0.7f, 0.9f, -0.1f };
        float scale = 1.2f;
        float offset = -0.1f;
        float qdc = 0.4f;

        float floatScore = AsymmetricHashingScorer.scoreOneVector(qt, qdc, codes, scale, offset);
        float multiBitScore = AsymmetricHashingScorer.scoreOneVectorMultiBit(qt, qdc, packed, nDims, bitsPerDim, scale, offset);
        assertEquals(floatScore, multiBitScore, 1e-4f);
    }

    public void testProjectionMatrixSerializationRoundtrip() throws Exception {
        Random rng = new Random(77);
        int originalDim = 8;
        int nDims = 3;
        int nClusters = 2;

        float[][] w = new float[originalDim][nDims];
        for (int i = 0; i < originalDim; i++) {
            for (int j = 0; j < nDims; j++) {
                w[i][j] = (float) rng.nextGaussian();
            }
        }
        float[][] centroids = new float[nClusters][originalDim];
        for (int c = 0; c < nClusters; c++) {
            for (int d = 0; d < originalDim; d++) {
                centroids[c][d] = (float) rng.nextGaussian();
            }
        }

        AshProjectionMatrix original = new AshProjectionMatrix(w, centroids);

        // Write
        ByteBuffersDataOutput dataOut = new ByteBuffersDataOutput();
        try (ByteBuffersIndexOutput out = new ByteBuffersIndexOutput(dataOut, "test", "test")) {
            original.write(out);
        }

        // Read
        ByteBuffersIndexInput in = new ByteBuffersIndexInput(dataOut.toDataInput(), "test");
        AshProjectionMatrix restored = AshProjectionMatrix.read(in);

        assertEquals(originalDim, restored.originalDim());
        assertEquals(nDims, restored.nDims());
        assertNotNull(restored.ashCentroids());
        assertEquals(nClusters, restored.ashCentroids().length);

        for (int i = 0; i < originalDim; i++) {
            for (int j = 0; j < nDims; j++) {
                assertEquals(w[i][j], restored.w()[i][j], 0f);
            }
        }
        for (int c = 0; c < nClusters; c++) {
            for (int d = 0; d < originalDim; d++) {
                assertEquals(centroids[c][d], restored.ashCentroids()[c][d], 0f);
            }
        }
    }

    public void testTopKRightSingularVectors() {
        // Known matrix: diagonal with descending values
        int m = 6;
        int n = 4;
        float[][] a = new float[m][n];
        a[0][0] = 4.0f;
        a[1][1] = 3.0f;
        a[2][2] = 2.0f;
        a[3][3] = 1.0f;

        // Top-2 right singular vectors should be close to e0 and e1
        float[][] topK = SvdUtil.topKRightSingularVectors(a, m, n, 2, 42L);
        assertEquals(2, topK.length);
        assertEquals(n, topK[0].length);

        // First vector should be dominated by dim 0 (corresponding to singular value 4)
        assertTrue(Math.abs(topK[0][0]) > 0.9f);
        // Second vector should be dominated by dim 1 (singular value 3)
        assertTrue(Math.abs(topK[1][1]) > 0.9f);
    }

    public void testScoreReconstructsDotProduct() {
        int dim = 128;
        int nVectors = 1000;
        int nQueries = 100;
        int nClusters = 4;
        int bitsPerDim = 2;
        float projectedDimsFraction = 0.5f;
        long seed = 42L;
        // Thresholds chosen so a correct implementation passes comfortably but a
        // broken one (missing offset, wrong sign, untrained W) drops near zero.
        // Random unit vectors in 128-dim have very compressed dot-product range
        // (concentration of measure), so absolute correlation tops out around 0.8.
        double pearsonThreshold = 0.6;
        double recallThreshold = 0.2;
        int k = 10;

        Random rng = new Random(seed);

        // Random unit-norm vectors
        float[][] vectors = randomUnit(nVectors, dim, rng);
        float[][] queries = randomUnit(nQueries, dim, rng);

        // Simple centroids: average of a random partition
        int[] assignments = new int[nVectors];
        float[][] centroids = new float[nClusters][dim];
        int[] counts = new int[nClusters];
        for (int i = 0; i < nVectors; i++) {
            assignments[i] = rng.nextInt(nClusters);
            counts[assignments[i]]++;
            for (int d = 0; d < dim; d++) {
                centroids[assignments[i]][d] += vectors[i][d];
            }
        }
        for (int c = 0; c < nClusters; c++) {
            for (int d = 0; d < dim; d++) {
                centroids[c][d] /= Math.max(counts[c], 1);
            }
        }
        IntFunction<float[]> centroidGetter = i -> centroids[assignments[i]];

        // Train
        AsymmetricHashingQuantizer ash = new AsymmetricHashingQuantizer(
            projectedDimsFraction,
            bitsPerDim,
            AsymmetricHashingQuantizer.Method.LEARNED,
            5,
            10,
            seed
        );
        float[][] w = ash.train(vectors, centroidGetter);
        int nDims = w[0].length;

        // Pre-transform each query: qt = q @ W
        float[][] qt = new float[nQueries][nDims];
        for (int q = 0; q < nQueries; q++) {
            for (int j = 0; j < nDims; j++) {
                double s = 0;
                for (int d = 0; d < dim; d++) {
                    s += (double) queries[q][d] * w[d][j];
                }
                qt[q][j] = (float) s;
            }
        }

        // Score matrices: approx[q][i] = ASH-approximated dot(q, v_i), exact[q][i] = true dot
        double[][] exact = new double[nQueries][nVectors];
        double[][] approx = new double[nQueries][nVectors];
        for (int i = 0; i < nVectors; i++) {
            float[] c = centroids[assignments[i]];
            AsymmetricHashingQuantizer.EncodedVector enc = ash.encodeOne(vectors[i], c, w);
            byte[] packed = bitsPerDim == 1
                ? AsymmetricHashingScorer.packBinaryCodes(enc.xEnc())
                : AsymmetricHashingScorer.packMultiBitCodes(enc.xEnc(), bitsPerDim);

            for (int q = 0; q < nQueries; q++) {
                double exactDot = 0;
                for (int d = 0; d < dim; d++) {
                    exactDot += (double) queries[q][d] * vectors[i][d];
                }

                double qDotC = 0;
                for (int d = 0; d < dim; d++) {
                    qDotC += (double) queries[q][d] * c[d];
                }

                float approxScore = bitsPerDim == 1
                    ? AsymmetricHashingScorer.scoreOneVectorBinary(qt[q], (float) qDotC, packed, nDims, enc.scale(), enc.offset())
                    : AsymmetricHashingScorer.scoreOneVectorMultiBit(
                        qt[q],
                        (float) qDotC,
                        packed,
                        nDims,
                        bitsPerDim,
                        enc.scale(),
                        enc.offset()
                    );

                exact[q][i] = exactDot;
                approx[q][i] = approxScore;
            }
        }

        // Pearson correlation across all (query, vector) pairs
        double sumE = 0, sumA = 0, sumEE = 0, sumAA = 0, sumEA = 0, sumAbsDiff = 0;
        long n = 0;
        for (int q = 0; q < nQueries; q++) {
            for (int i = 0; i < nVectors; i++) {
                double e = exact[q][i], a = approx[q][i];
                sumE += e;
                sumA += a;
                sumEE += e * e;
                sumAA += a * a;
                sumEA += e * a;
                sumAbsDiff += Math.abs(e - a);
                n++;
            }
        }
        double meanE = sumE / n, meanA = sumA / n;
        double varE = sumEE / n - meanE * meanE;
        double varA = sumAA / n - meanA * meanA;
        double covEA = sumEA / n - meanE * meanA;
        double pearson = covEA / Math.sqrt(varE * varA);
        double recall = recallAtK(approx, exact, k);

        assertTrue("pearson " + pearson + " below " + pearsonThreshold, pearson > pearsonThreshold);
        assertTrue("recall@" + k + " " + recall + " below " + recallThreshold, recall > recallThreshold);
    }

    /** Average overlap@k between approx-top-k and exact-top-k, per query. */
    private static double recallAtK(double[][] approx, double[][] exact, int k) {
        int nQueries = approx.length;
        long hits = 0;
        for (int q = 0; q < nQueries; q++) {
            int[] approxTop = topKIndices(approx[q], k);
            int[] exactTop = topKIndices(exact[q], k);
            java.util.HashSet<Integer> e = new java.util.HashSet<>();
            for (int idx : exactTop) {
                e.add(idx);
            }
            for (int idx : approxTop) {
                if (e.contains(idx)) hits++;
            }
        }
        return (double) hits / ((long) nQueries * k);
    }

    /** Returns indices of the k largest values in scores, unordered. */
    private static int[] topKIndices(double[] scores, int k) {
        int n = scores.length;
        Integer[] idx = new Integer[n];
        for (int i = 0; i < n; i++) {
            idx[i] = i;
        }
        java.util.Arrays.sort(idx, (a, b) -> Double.compare(scores[b], scores[a]));
        int[] out = new int[k];
        for (int i = 0; i < k; i++) {
            out[i] = idx[i];
        }
        return out;
    }

    private static float[][] randomUnit(int n, int d, Random rng) {
        float[][] out = new float[n][d];
        for (int i = 0; i < n; i++) {
            double s = 0;
            for (int j = 0; j < d; j++) {
                out[i][j] = (float) rng.nextGaussian();
                s += out[i][j] * out[i][j];
            }
            float inv = (float) (1.0 / Math.sqrt(s));
            for (int j = 0; j < d; j++) {
                out[i][j] *= inv;
            }
        }
        return out;
    }

    private static double computeRankCorrelation(float[][] vectors, float[] query, float[] approxScores) {
        int n = vectors.length;
        float[] exactScores = new float[n];
        for (int i = 0; i < n; i++) {
            double dot = 0;
            for (int j = 0; j < query.length; j++) {
                dot += (double) vectors[i][j] * query[j];
            }
            exactScores[i] = (float) dot;
        }

        // Spearman rank correlation (simplified)
        int[] exactRanks = ranks(exactScores);
        int[] approxRanks = ranks(approxScores);
        double sumD2 = 0;
        for (int i = 0; i < n; i++) {
            double d = exactRanks[i] - approxRanks[i];
            sumD2 += d * d;
        }
        return 1.0 - 6.0 * sumD2 / (n * ((long) n * n - 1));
    }

    private static int[] ranks(float[] scores) {
        int n = scores.length;
        Integer[] indices = new Integer[n];
        for (int i = 0; i < n; i++) {
            indices[i] = i;
        }
        java.util.Arrays.sort(indices, (a, b) -> Float.compare(scores[b], scores[a]));
        int[] ranks = new int[n];
        for (int r = 0; r < n; r++) {
            ranks[indices[r]] = r;
        }
        return ranks;
    }
}
