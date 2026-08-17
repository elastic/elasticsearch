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
import org.apache.lucene.util.BitUtil;
import org.elasticsearch.common.CheckedIntFunction;
import org.elasticsearch.simdvec.AsymmetricHashingScorer;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.oneOf;

/**
 * Tests for the core ASH algorithm components: SVD, quantizers, and the full pipeline.
 */
public class AsymmetricHashingQuantizerTests extends ESTestCase {

    public void testSvdIdentity() {
        // SVD of identity should give identity
        float[] identity = { 1, 0, 0, 0, 1, 0, 0, 0, 1 };
        SvdUtil.SvdResult result = SvdUtil.thinSvd(identity, 3, 3);
        // All singular values should be 1
        for (float s : result.s()) {
            assertEquals(1.0f, s, 1e-5f);
        }
    }

    public void testSvdRank1() {
        // Rank-1 matrix: outer product
        int m = 4, n = 3;
        float[] a = new float[m * n];
        float[] u = { 1, 2, 3, 4 };
        float[] v = { 0.5f, 0.3f, 0.1f };
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                a[i * n + j] = u[i] * v[j];
            }
        }
        SvdUtil.SvdResult result = SvdUtil.thinSvd(a, m, n);
        // Only first singular value should be non-zero
        assertThat(result.s()[0], greaterThan(0.1f));
        assertEquals(0.0f, result.s()[1], 1e-4f);
        assertEquals(0.0f, result.s()[2], 1e-4f);
    }

    public void testSvdMatrixReconstruction() {
        int m = 5, n = 3;
        float[] a = SvdUtil.randomGaussians(random(), m * n);
        SvdUtil.SvdResult result = SvdUtil.thinSvd(a, m, n);

        assertEquals(m * n, result.u().length);
        assertEquals(n, result.s().length);
        assertEquals(n * n, result.vt().length);

        // Reconstruct: A_rec = U @ diag(S) @ Vt
        float[] rec = new float[m * n];
        for (int i = 0; i < m; i++) {
            for (int k = 0; k < n; k++) {
                float us = result.u()[i * n + k] * result.s()[k];
                for (int j = 0; j < n; j++) {
                    rec[i * n + j] += us * result.vt()[k * n + j];
                }
            }
        }
        for (int i = 0; i < m * n; i++) {
            assertEquals("index " + i, a[i], rec[i], 1e-4f);
        }
    }

    public void testSvdWideMatrixReconstruction() {
        int m = 3, n = 5;
        float[] a = SvdUtil.randomGaussians(random(), m * n);
        SvdUtil.SvdResult result = SvdUtil.thinSvd(a, m, n);

        assertEquals(m * m, result.u().length);
        assertEquals(m, result.s().length);
        assertEquals(m * n, result.vt().length);

        // Reconstruct: A_rec = U @ diag(S) @ Vt
        float[] rec = new float[m * n];
        for (int i = 0; i < m; i++) {
            for (int k = 0; k < m; k++) {
                float us = result.u()[i * m + k] * result.s()[k];
                for (int j = 0; j < n; j++) {
                    rec[i * n + j] += us * result.vt()[k * n + j];
                }
            }
        }
        for (int i = 0; i < m * n; i++) {
            assertEquals("index " + i, a[i], rec[i], 1e-4f);
        }
    }

    public void testProcrustesOrthogonal() {
        // Procrustes of a random matrix should return orthogonal matrix (R^T R = I)
        int k = 5;
        float[] m = SvdUtil.randomGaussians(random(), k * k);
        float[] r = SvdUtil.procrustes(m, k);
        // Check R^T R ~= I
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < k; j++) {
                double dot = 0;
                for (int l = 0; l < k; l++) {
                    dot += (double) r[l * k + i] * r[l * k + j];
                }
                float expected = (i == j) ? 1.0f : 0.0f;
                assertEquals(expected, (float) dot, 1e-4f);
            }
        }
    }

    public void testSphericalScalarQuantizer2Bit() {
        AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(2);
        float[] x = { 0.8f, -0.5f, 0.3f, -0.9f };
        AshSphericalScalarQuantizer.QuantizeResult result = ssq.encode(x, 1, x.length);

        // Codes should be centered: sign * (0.5 + level)
        // With 2 bits, levels are 0 or 1, so magnitudes are 0.5 or 1.5
        for (float val : result.centeredCodes()) {
            float absMag = Math.abs(val);
            assertThat(absMag, oneOf(0.5f, 1.5f));
        }
        assertThat(result.codeNorms()[0], greaterThan(0f));
    }

    public void testFullPipelineRandomMethod() throws IOException {
        int nVectors = 100;
        int dim = 16;
        float projectedDimsFraction = 0.25f; // 16 * 0.25 = 4 projected dims
        int bitsPerDim = 2;

        float[][] vectors = new float[nVectors][];
        for (int i = 0; i < nVectors; i++) {
            vectors[i] = SvdUtil.randomGaussians(random(), dim);
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

        CheckedIntFunction<float[], IOException> centroidGetter = (i) -> centroids[assignments[i]];

        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            projectedDimsFraction,
            bitsPerDim,
            AsymmetricHashingQuantizer.Method.RANDOM,
            0,
            10,
            42L
        );

        int expectedNDims = (int) (dim * projectedDimsFraction);
        float[] w = quantizer.train(vectors, centroidGetter);
        assertNotNull(w);
        assertEquals(dim * expectedNDims, w.length);

        // Encode per-cluster using the production path
        float[] wT = ESVectorUtil.transposeMatrix(w, dim, expectedNDims);
        AsymmetricHashingQuantizer.VectorAndNorm precomputed = AsymmetricHashingQuantizer.precomputeCentroid(centroids[0], wT);
        for (int i = 0; i < nVectors; i++) {
            AsymmetricHashingQuantizer.EncodedVector enc = quantizer.encode(vectors[i], centroids[0], wT, precomputed);
            assertNotNull(enc.xEnc());
            assertEquals(expectedNDims, enc.xEnc().length);
        }
    }

    public void testFullPipelineLearnedMethod() throws IOException {
        int nVectors = 200;
        int dim = 32;
        float projectedDimsFraction = 0.25f; // 32 * 0.25 = 8 projected dims
        int bitsPerDim = 2;

        float[][] vectors = new float[nVectors][];
        for (int i = 0; i < nVectors; i++) {
            vectors[i] = SvdUtil.randomGaussians(random(), dim);
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

        CheckedIntFunction<float[], IOException> centroidGetter = (i) -> centroids[assignments[i]];

        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            projectedDimsFraction,
            bitsPerDim,
            AsymmetricHashingQuantizer.Method.LEARNED,
            5,
            10,
            42L
        );

        float[] w = quantizer.train(vectors, centroidGetter);
        int nDims = quantizer.nDims(dim);

        // Encode per-cluster using the production path
        float[] wT = ESVectorUtil.transposeMatrix(w, dim, nDims);
        AsymmetricHashingQuantizer.VectorAndNorm precomputed = AsymmetricHashingQuantizer.precomputeCentroid(centroids[0], wT);
        float[][] encodedVectors = new float[nVectors][nDims];
        float[] scales = new float[nVectors];
        float[] offsets = new float[nVectors];
        for (int i = 0; i < nVectors; i++) {
            AsymmetricHashingQuantizer.EncodedVector enc = quantizer.encode(vectors[i], centroids[0], wT, precomputed);
            encodedVectors[i] = enc.xEnc();
            scales[i] = enc.scale();
            offsets[i] = enc.offset();
        }

        // Score a query against the encoded vectors using the production scoring path
        float[] query = SvdUtil.randomGaussians(random(), dim);

        // Project query: qt = query @ W (raw, not centered)
        float[] qt = new float[nDims];
        for (int j = 0; j < nDims; j++) {
            double s = 0;
            for (int d = 0; d < dim; d++) {
                s = Math.fma(query[d], w[d * nDims + j], s);
            }
            qt[j] = (float) s;
        }
        float queryDotCentroid = ESVectorUtil.dotProduct(query, centroids[0]);

        float[] scores = new float[nVectors];
        for (int i = 0; i < nVectors; i++) {
            byte[] packed = AsymmetricHashingScorer.pack(encodedVectors[i], bitsPerDim);
            scores[i] = AsymmetricHashingScorer.score(
                qt,
                new float[] { queryDotCentroid },
                packed,
                0,
                nDims,
                bitsPerDim,
                packCorrections(scales[i], offsets[i], 0),
                0
            );
        }
        assertEquals(nVectors, scores.length);

        // Verify approximate dot products correlate with exact ones
        double correlation = computeRankCorrelation(vectors, query, scores);
        // With learned method, expect reasonable correlation
        assertThat("Expected positive rank correlation", correlation, greaterThan(0.1));
    }

    public void testReconstructedDotProductApproximatesTrueDotProduct() throws IOException {
        // With no dimensionality reduction (projectedDimsFraction=1.0, so nDims == originalDim and W
        // is a random orthogonal matrix -- a pure rotation, not a projection), the only source of
        // reconstruction error is the quantization of the residual (vector - centroid). This
        // isolates the quantizer's fidelity, matching the near-linear ⟨q, x⟩ ~ ⟨q, quant(x)⟩
        // relationship reported for ASH (see https://arxiv.org/pdf/2606.07870, Figure 4 and the
        // surrounding "estimator bias" discussion, which notes the fit is not exact -- there's a
        // small, bitsPerDim-dependent bias -- but the pairs are tightly clustered around the line).
        //
        // Thresholds below were calibrated empirically (aggregate relative RMSE over many random
        // query/vector/centroid trials, stable across seeds) with a safety margin of roughly 2x
        // (4 bits) to 5x (8 bits) over the observed values, to catch a real regression without being
        // flaky.
        int dim = 128;
        int nVectors = 200;

        for (var config : new Object[][] { { 4, 0.35 }, { 8, 0.05 } }) {
            int bitsPerDim = (int) config[0];
            double relRmseThreshold = (double) config[1];

            AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
                1.0f,
                bitsPerDim,
                AsymmetricHashingQuantizer.Method.RANDOM,
                0,
                1,
                42L
            );
            float[] w = quantizer.train(new float[][] { new float[dim] }, i -> new float[dim]);
            int nDims = quantizer.nDims(dim); // == dim since projectedDimsFraction=1.0
            float[] wT = ESVectorUtil.transposeMatrix(w, dim, nDims);

            float[] centroid = SvdUtil.randomGaussians(random(), dim);
            float[] query = SvdUtil.randomGaussians(random(), dim);

            // Raw query projection: qt = query @ W
            float[] qt = new float[nDims];
            for (int j = 0; j < nDims; j++) {
                double sum = 0;
                for (int d = 0; d < dim; d++) {
                    sum = Math.fma(query[d], w[d * nDims + j], sum);
                }
                qt[j] = (float) sum;
            }
            float queryDotCentroid = ESVectorUtil.dotProduct(query, centroid, dim);
            AsymmetricHashingQuantizer.VectorAndNorm precomputed = AsymmetricHashingQuantizer.precomputeCentroid(centroid, wT);

            double sumSqErr = 0;
            double sumSqTrue = 0;
            for (int i = 0; i < nVectors; i++) {
                float[] vector = SvdUtil.randomGaussians(random(), dim);
                float trueDot = ESVectorUtil.dotProduct(query, vector, dim);

                AsymmetricHashingQuantizer.EncodedVector enc = quantizer.encode(vector, centroid, wT, precomputed);
                byte[] packed = AsymmetricHashingScorer.pack(enc.xEnc(), bitsPerDim);
                float reconstructed = AsymmetricHashingScorer.score(
                    qt,
                    new float[] { queryDotCentroid },
                    packed,
                    0,
                    nDims,
                    bitsPerDim,
                    packCorrections(enc.scale(), enc.offset(), 0),
                    0
                );

                double err = reconstructed - trueDot;
                sumSqErr += err * err;
                sumSqTrue += (double) trueDot * trueDot;
            }
            double relRmse = Math.sqrt(sumSqErr / sumSqTrue);
            assertThat("bitsPerDim=" + bitsPerDim + " relative RMSE too high", relRmse, lessThan(relRmseThreshold));
        }
    }

    public void testScorerSingleVector() {
        int nDims = 2;
        int bitsPerDim = 2;
        // Valid 2-bit centered codes: values from {-1.5, -0.5, 0.5, 1.5}
        float[] encodedVector = { 0.5f, -0.5f };
        float scale = 1.0f;
        float offset = 0.0f;

        // queryTransformed = [1.0, 0.5] (raw q @ W with zero centroid)
        // dot = 1.0*0.5 + 0.5*(-0.5) = 0.25
        // result = 0.25 * 1.0 + 0.0 + 0.0 = 0.25
        byte[] packed = AsymmetricHashingScorer.pack(encodedVector, bitsPerDim);
        float score = AsymmetricHashingScorer.score(
            new float[] { 1.0f, 0.5f },
            new float[] { 0.0f },
            packed,
            0,
            nDims,
            bitsPerDim,
            packCorrections(scale, offset, 0),
            0
        );
        assertEquals(0.25f, score, 1e-4f);
    }

    public void testFallbackToRandomWhenTooFewVectors() throws IOException {
        // With only 2 vectors and nDims=4, learned method should fall back to random
        int dim = 16;
        float[][] vectors = {
            { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 },
            { 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 } };
        float[][] centroids = { new float[dim] };
        int[] assignments = { 0, 0 };

        CheckedIntFunction<float[], IOException> centroidGetter = i -> centroids[assignments[i]];

        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            0.25f,
            2,
            AsymmetricHashingQuantizer.Method.LEARNED,
            5,
            10,
            42L
        );

        // Should not throw -- falls back to random
        float[] w = quantizer.train(vectors, centroidGetter);
        assertNotNull(w);
        int nDims = quantizer.nDims(dim);
        assertEquals(dim * nDims, w.length);
    }

    public void testMultiBitPackAndScore() {
        // 2-bit quantizer: levels are -1.5, -0.5, 0.5, 1.5
        int bitsPerDim = 2;
        int nDims = 10;
        float[] codes = { 0.5f, -1.5f, 1.5f, -0.5f, 0.5f, 1.5f, -0.5f, -1.5f, 0.5f, 1.5f };
        byte[] packed = AsymmetricHashingScorer.pack(codes, bitsPerDim);
        assertEquals(bitsPerDim * ((nDims + 7) >>> 3), packed.length);

        float[] qt = { 0.5f, 0.3f, -0.2f, 0.8f, 0.1f, -0.4f, 0.6f, -0.7f, 0.9f, -0.1f };
        float scale = 1.2f;
        float offset = -0.1f;
        float qdc = 0.4f;

        // Compute reference score via plain float dot product
        double dot = ESVectorUtil.dotProduct(qt, codes, nDims);
        float floatScore = (float) dot * scale + qdc + offset;
        float multiBitScore = AsymmetricHashingScorer.score(
            qt,
            new float[] { qdc },
            packed,
            0,
            nDims,
            bitsPerDim,
            packCorrections(scale, offset, 0),
            0
        );
        assertEquals(floatScore, multiBitScore, 1e-4f);
    }

    public void testProjectionMatrixSerializationRoundtrip() throws Exception {
        Random rng = random();
        int originalDim = 8;
        int nDims = 3;

        float[] w = SvdUtil.randomGaussians(random(), originalDim * nDims);

        AshProjectionMatrix original = new AshProjectionMatrix(w, originalDim, nDims);

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
        assertArrayEquals(w, restored.w(), 0f);
    }

    public void testTopKRightSingularVectors() {
        // Known matrix: diagonal with descending values
        int m = 6;
        int n = 4;
        float[] a = new float[m * n];
        a[0 * n + 0] = 4.0f;
        a[1 * n + 1] = 3.0f;
        a[2 * n + 2] = 2.0f;
        a[3 * n + 3] = 1.0f;

        // Top-2 right singular vectors should be close to e0 and e1
        float[] topK = SvdUtil.topKRightSingularVectors(a, m, n, 2, 42L);
        assertEquals(2 * n, topK.length);

        // First vector should be dominated by dim 0 (corresponding to singular value 4)
        assertThat(Math.abs(topK[0 * n + 0]), greaterThan(0.9f));
        // Second vector should be dominated by dim 1 (singular value 3)
        assertThat(Math.abs(topK[1 * n + 1]), greaterThan(0.9f));
    }

    public void testScoreReconstructsDotProduct() throws IOException {
        int dim = 128;
        int nVectors = 1000;
        int nQueries = 100;
        int nClusters = 4;
        int bitsPerDim = 2;
        float projectedDimsFraction = 0.5f;
        long seed = 42L;
        // Thresholds chosen so a correct implementation passes comfortably but a
        // broken one (missing offset, wrong sign, double-subtracted cross-term) fails.
        double pearsonThreshold = 0.6;
        double recallThreshold = 0.2;
        int k = 10;

        // Use non-unit vectors with meaningful magnitude to stress the offset formula.
        // Unit vectors make centroids near-zero which can mask offset bugs.
        float[][] vectors = new float[nVectors][];
        for (int i = 0; i < nVectors; i++) {
            vectors[i] = SvdUtil.randomGaussians(random(), dim);
        }
        float[][] queries = new float[nQueries][];
        for (int i = 0; i < nQueries; i++) {
            queries[i] = SvdUtil.randomGaussians(random(), dim);
        }

        // Non-trivial centroids with significant magnitude (shifted clusters)
        int[] assignments = new int[nVectors];
        float[][] centroids = new float[nClusters][dim];
        int[] counts = new int[nClusters];
        for (int i = 0; i < nVectors; i++) {
            assignments[i] = random().nextInt(nClusters);
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
        CheckedIntFunction<float[], IOException> centroidGetter = i -> centroids[assignments[i]];

        // Train
        AsymmetricHashingQuantizer ash = new AsymmetricHashingQuantizer(
            projectedDimsFraction,
            bitsPerDim,
            AsymmetricHashingQuantizer.Method.LEARNED,
            5,
            10,
            42L
        );
        float[] w = ash.train(vectors, centroidGetter);
        int nDims = ash.nDims(dim);

        // Pre-transform each query: qt = q @ W
        float[][] qt = new float[nQueries][nDims];
        for (int q = 0; q < nQueries; q++) {
            for (int j = 0; j < nDims; j++) {
                double s = 0;
                for (int d = 0; d < dim; d++) {
                    s = Math.fma(queries[q][d], w[d * nDims + j], s);
                }
                qt[q][j] = (float) s;
            }
        }

        // Score matrices: approx[q][i] = ASH-approximated dot(q, v_i), exact[q][i] = true dot
        double[][] exact = new double[nQueries][nVectors];
        double[][] approx = new double[nQueries][nVectors];

        // Precompute per-cluster values
        float[] wT = ESVectorUtil.transposeMatrix(w, dim, nDims);
        AsymmetricHashingQuantizer.VectorAndNorm[] precomputedPerCluster = new AsymmetricHashingQuantizer.VectorAndNorm[nClusters];
        for (int c = 0; c < nClusters; c++) {
            precomputedPerCluster[c] = AsymmetricHashingQuantizer.precomputeCentroid(centroids[c], wT);
        }

        for (int i = 0; i < nVectors; i++) {
            float[] c = centroids[assignments[i]];
            AsymmetricHashingQuantizer.EncodedVector enc = ash.encode(vectors[i], c, wT, precomputedPerCluster[assignments[i]]);
            byte[] packed = AsymmetricHashingScorer.pack(enc.xEnc(), bitsPerDim);

            for (int q = 0; q < nQueries; q++) {
                double exactDot = ESVectorUtil.dotProduct(queries[q], vectors[i]);
                double qDotC = ESVectorUtil.dotProduct(queries[q], c);

                float approxScore = AsymmetricHashingScorer.score(
                    qt[q],
                    new float[] { (float) qDotC },
                    packed,
                    0,
                    nDims,
                    bitsPerDim,
                    packCorrections(enc.scale(), enc.offset(), 0),
                    0
                );

                exact[q][i] = exactDot;
                approx[q][i] = approxScore;
            }
        }

        // Pearson correlation across all (query, vector) pairs
        double sumE = 0, sumA = 0, sumEE = 0, sumAA = 0, sumEA = 0;
        long n = 0;
        for (int q = 0; q < nQueries; q++) {
            for (int i = 0; i < nVectors; i++) {
                double e = exact[q][i], a = approx[q][i];
                sumE += e;
                sumA += a;
                sumEE += e * e;
                sumAA += a * a;
                sumEA += e * a;
                n++;
            }
        }
        double meanE = sumE / n, meanA = sumA / n;
        double varE = sumEE / n - meanE * meanE;
        double varA = sumAA / n - meanA * meanA;
        double covEA = sumEA / n - meanE * meanA;
        double pearson = covEA / Math.sqrt(varE * varA);
        double recall = recallAtK(approx, exact, k);

        assertThat(pearson, greaterThan(pearsonThreshold));
        assertThat("recall@" + k, recall, greaterThan(recallThreshold));
    }

    /** Average overlap@k between approx-top-k and exact-top-k, per query. */
    private static double recallAtK(double[][] approx, double[][] exact, int k) {
        int nQueries = approx.length;
        long hits = 0;
        for (int q = 0; q < nQueries; q++) {
            int[] approxTop = topKIndices(approx[q], k);
            int[] exactTop = topKIndices(exact[q], k);
            Set<Integer> e = Arrays.stream(exactTop).boxed().collect(Collectors.toSet());
            hits += Arrays.stream(approxTop).filter(e::contains).count();
        }
        return (double) hits / ((long) nQueries * k);
    }

    /** Returns indices of the k largest values in scores, unordered. */
    private static int[] topKIndices(double[] scores, int k) {
        return IntStream.range(0, scores.length)
            .boxed()
            .sorted(Comparator.comparingDouble(i -> scores[i]))
            .limit(k)
            .mapToInt(Integer::intValue)
            .toArray();
    }

    private static double computeRankCorrelation(float[][] vectors, float[] query, float[] approxScores) {
        int n = vectors.length;
        float[] exactScores = new float[n];
        for (int i = 0; i < n; i++) {
            exactScores[i] = ESVectorUtil.dotProduct(query, vectors[i]);
        }

        // Spearman rank correlation (simplified)
        int[] exactRanks = ranks(exactScores);
        int[] approxRanks = ranks(approxScores);
        double sumD2 = 0;
        for (int i = 0; i < n; i++) {
            // no ESVectorUtil.squareDistance method with ints :(
            double d = exactRanks[i] - approxRanks[i];
            sumD2 += d * d;
        }
        return 1.0 - 6.0 * sumD2 / (n * ((long) n * n - 1));
    }

    private static int[] ranks(float[] scores) {
        int[] indices = IntStream.range(0, scores.length)
            .boxed()
            .sorted(Comparator.comparingDouble(i -> scores[i]))
            .mapToInt(Integer::intValue)
            .toArray();
        int[] ranks = new int[indices.length];
        for (int r = 0; r < indices.length; r++) {
            ranks[indices[r]] = r;
        }
        return ranks;
    }

    private static byte[] packCorrections(float scale, float offset, int docSum) {
        byte[] corr = new byte[AsymmetricHashingScorer.CORRECTION_BYTES];
        BitUtil.VH_LE_INT.set(corr, AsymmetricHashingScorer.CORR_SCALE, Float.floatToIntBits(scale));
        BitUtil.VH_LE_INT.set(corr, AsymmetricHashingScorer.CORR_OFFSET, Float.floatToIntBits(offset));
        BitUtil.VH_LE_INT.set(corr, AsymmetricHashingScorer.CORR_DOC_SUM, docSum);
        return corr;
    }
}
