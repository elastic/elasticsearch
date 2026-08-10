/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.elasticsearch.simdvec.ESVectorUtil;

import java.util.Arrays;
import java.util.Random;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;

/**
 * Asymmetric Hashing quantizer. Learns a projection matrix W that maps vectors from
 * original space to a low-dimensional latent space optimized for quantization fidelity.
 * <p>
 * The algorithm:
 * <ol>
 *   <li>KMeans clustering partitions vectors (handled externally via HierarchicalKMeans)</li>
 *   <li>Vectors are centered by subtracting their cluster centroid and normalized</li>
 *   <li>A rotation matrix W is learned (PCA init + Procrustes iterations) to maximize
 *       inner product preservation after quantization in the projected space</li>
 *   <li>Vectors are projected via W, then quantized (binary or multi-bit spherical)</li>
 *   <li>Per-vector scale and offset are stored for dot product reconstruction</li>
 * </ol>
 * <p>
 * At query time, the query is projected via W but NOT quantized (asymmetric scoring),
 * yielding higher recall than symmetric approaches.
 */
public final class AsymmetricHashingQuantizer {

    /** Training method for the projection matrix W. */
    public enum Method {
        /** Learn W via PCA + iterative Procrustes optimization. */
        LEARNED,
        /** Use a random orthogonal matrix (no training iterations). */
        RANDOM
    }

    private final float projectedDimsFraction;
    private final Method method;
    private final int nTrainingIterations;
    private final int trainingFactor;
    private final long seed;
    private final AshSphericalScalarQuantizer quantizer;

    /**
     * Creates an ASH quantizer with the given configuration.
     *
     * @param projectedDimsFraction fraction of original dimensions to project to (e.g. 0.5 for half)
     * @param bitsPerDim bits per projected dimension in the body
     * @param method training method for W
     * @param nTrainingIterations number of Procrustes iterations (for LEARNED)
     * @param trainingFactor multiplier on dimension for training sample size
     * @param seed random seed
     */
    public AsymmetricHashingQuantizer(
        float projectedDimsFraction,
        int bitsPerDim,
        Method method,
        int nTrainingIterations,
        int trainingFactor,
        long seed
    ) {
        if (projectedDimsFraction <= 0 || projectedDimsFraction > 1.0f) {
            throw new IllegalArgumentException("projectedDimsFraction must be in (0, 1]");
        }
        if (bitsPerDim <= 0) {
            throw new IllegalArgumentException("bitsPerDim must be positive");
        }
        this.projectedDimsFraction = projectedDimsFraction;
        this.method = method;
        this.nTrainingIterations = nTrainingIterations;
        this.trainingFactor = trainingFactor;
        this.seed = seed;
        this.quantizer = new AshSphericalScalarQuantizer(bitsPerDim);
    }

    /**
     * Computes the number of projected dimensions for a given original dimension.
     *
     * @param originalDim the original vector dimensionality
     * @return the number of projected dimensions
     */
    public int nDims(int originalDim) {
        return (int) (originalDim * projectedDimsFraction);
    }

    /**
     * Trains the projection matrix W on the given vectors and their cluster assignments.
     *
     * @param vectors all vectors in the segment, shape (nVectors, originalDim)
     * @param centroids cluster centroids, fetched by vector ordinal
     * @return the learned projection matrix W, shape (originalDim, nDims)
     */
    public float[][] train(float[][] vectors, IntFunction<float[]> centroids) {
        int originalDim = vectors[0].length;
        int nDims = nDims(originalDim);

        if (method == Method.RANDOM) {
            return randomOrthogonal(originalDim, nDims);
        }

        // Too few vectors for meaningful PCA training; fall back to random projection
        if (method == Method.LEARNED && vectors.length < nDims * 2) {
            return randomOrthogonal(originalDim, nDims);
        }

        int trainingSize = Math.min(originalDim * trainingFactor, vectors.length);
        int[] sampleIndices = sampleIndices(vectors.length, trainingSize);

        // Center and normalize the sampled vectors into a fresh array. We must not mutate
        // `vectors` in place -- the writer reuses it for per-posting-list encoding later.
        float[][] xTraining = new float[trainingSize][originalDim];
        for (int i = 0; i < trainingSize; i++) {
            int srcIdx = sampleIndices[i];
            float[] centroid = centroids.apply(srcIdx);
            for (int d = 0; d < originalDim; d++) {
                xTraining[i][d] = vectors[srcIdx][d] - centroid[d];
            }
            ESVectorUtil.l2Normalize(xTraining[i]);
        }

        // LEARNED: PCA init + Procrustes
        return learnedTraining(xTraining, originalDim, nDims);
    }

    /**
     * Result of encoding a single (vector, centroid) pair.
     *
     * @param xEnc quantized code in latent space, shape (nDims,)
     * @param scale scale factor applied at scoring time (norm / codeNorm)
     * @param offset additive correction term for dot product reconstruction
     */
    public record EncodedVector(float[] xEnc, float scale, float offset) {}

    /**
     * Precomputed per-centroid values that are invariant across all vectors in a posting list.
     * Computing these once per posting list eliminates redundant work in {@link #encode}.
     *
     * @param centroidProjected centroid projected through W^T, shape (nDims,): centroid @ W
     * @param centroidNormSq squared L2 norm of the centroid: ||centroid||^2
     */
    public record PrecomputedCentroid(float[] centroidProjected, float centroidNormSq) {}

    /** Result of centering and normalizing a vector against its centroid. */
    private record CenteredVector(float[] normalized, float normSq) {}

    private static CenteredVector centralizeVector(float[] vector, float[] centroid) {
        int originalDim = vector.length;
        float[] centered = new float[originalDim];
        for (int d = 0; d < originalDim; d++) {
            centered[d] = vector[d] - centroid[d];
        }
        float normSq = ESVectorUtil.l2Normalize(centered);
        return normSq == 0f ? new CenteredVector(new float[originalDim], 0) : new CenteredVector(centered, normSq);
    }

    /**
     * Precomputes centroid-dependent values for a posting list. Call once per cluster,
     * then pass the result to {@link #encode} for each vector in that cluster.
     *
     * @param centroid the posting list centroid, length originalDim
     * @param wT transposed projection matrix, shape (nDims, originalDim)
     * @return precomputed values for this centroid
     */
    public static PrecomputedCentroid precomputeCentroid(float[] centroid, float[][] wT) {
        int nDims = wT.length;
        float[] centroidProjected = new float[nDims];
        for (int j = 0; j < nDims; j++) {
            centroidProjected[j] = ESVectorUtil.dotProduct(centroid, wT[j]);
        }
        float centroidNormSq = ESVectorUtil.dotProduct(centroid, centroid);
        return new PrecomputedCentroid(centroidProjected, centroidNormSq);
    }

    /**
     * Fast single-vector encoding using precomputed centroid values and transposed W.
     * This avoids recomputing centroid @ W and ||centroid||^2 for every vector in a posting list.
     *
     * @param vector the input vector, length originalDim
     * @param centroid the centroid (needed for centering), length originalDim
     * @param wT transposed projection matrix, shape (nDims, originalDim)
     * @param precomputed precomputed centroid projection and norm
     * @return xEnc/scale/offset for this (vector, centroid) pair
     */
    public EncodedVector encode(float[] vector, float[] centroid, float[][] wT, PrecomputedCentroid precomputed) {
        int nDims = wT.length;

        // Center and compute norm
        var centered = centralizeVector(vector, centroid);

        // Project using transposed W: xLatent[j] = dot(centered, wT[j])
        float[] xLatent = new float[nDims];
        for (int j = 0; j < nDims; j++) {
            xLatent[j] = ESVectorUtil.dotProduct(centered.normalized, wT[j]);
        }

        // Quantize
        AshSphericalScalarQuantizer.SingleQuantizeResult qr = quantizer.encodeOne(xLatent);
        float[] xEnc = qr.centeredCode();
        float codeNorm = qr.codeNorm();

        // Scale: norm / codeNorm
        float scale = codeNorm > 0 ? (float) Math.sqrt(centered.normSq) / codeNorm : 0;

        // Offset per ASH paper Equation 19: ⟨x, μ⟩ - scale * ⟨centroid@W, code⟩ - ‖μ‖²
        // The cross-term ⟨centroid@W, code⟩ accounts for using the raw projected query Wq (Eq. 18)
        // rather than the centered query W(q-μ). At query time the scorer computes ⟨Wq, code⟩,
        // and the centroid's contribution is pre-subtracted here so no per-posting-list centroid
        // recomputation is needed during search.
        float dotVecCent = ESVectorUtil.dotProduct(vector, centroid);
        float offset = dotVecCent - precomputed.centroidNormSq();
        float[] centroidProjected = precomputed.centroidProjected();
        double correction = 0;
        for (int j = 0; j < nDims; j++) {
            correction = Math.fma(centroidProjected[j], xEnc[j], correction);
        }
        offset -= (float) (scale * correction);

        return new EncodedVector(xEnc, scale, offset);
    }

    /**
     * Transposes W from (originalDim x nDims) to (nDims x originalDim).
     * The transposed layout enables SIMD-friendly row-wise dot products during encoding.
     *
     * @param w the projection matrix, shape (originalDim, nDims)
     * @return the transposed matrix, shape (nDims, originalDim)
     */
    static float[][] transposeW(float[][] w) {
        int originalDim = w.length;
        int nDims = w[0].length;
        float[][] wT = new float[nDims][originalDim];
        for (int i = 0; i < originalDim; i++) {
            for (int j = 0; j < nDims; j++) {
                wT[j][i] = w[i][j];
            }
        }
        return wT;
    }

    private float[][] learnedTraining(float[][] xTraining, int originalDim, int nDims) {
        // PCA initialization: extract top nDims right singular vectors via power iteration
        // This is much faster than full SVD when nDims << originalDim
        float[][] topVectors = SvdUtil.topKRightSingularVectors(xTraining, xTraining.length, originalDim, nDims, seed);
        // P = top nDims right singular vectors transposed: rows of topVectors are the vectors
        // P shape: (originalDim x nDims) where each column is a right singular vector
        float[][] p = new float[originalDim][nDims];
        for (int i = 0; i < originalDim; i++) {
            for (int j = 0; j < nDims; j++) {
                p[i][j] = topVectors[j][i];
            }
        }

        // Project training data: X_ld = xTraining @ P (nTraining x nDims)
        int nTraining = xTraining.length;
        float[][] xLd = matMul(xTraining, p, nTraining, originalDim, nDims);

        // Initialize random M (nDims x nDims)
        Random rng = new Random(seed);
        float[][] m = new float[nDims][nDims];
        for (int i = 0; i < nDims; i++) {
            for (int j = 0; j < nDims; j++) {
                m[i][j] = (float) rng.nextGaussian();
            }
        }

        // Iterative Procrustes
        float[][] r = null;
        for (int epoch = 0; epoch <= nTrainingIterations; epoch++) {
            // R = procrustes(M)
            r = SvdUtil.procrustes(m, nDims);

            if (epoch < nTrainingIterations) {
                // X_transformed = X_ld @ R
                float[][] xTransformed = matMul(xLd, r, nTraining, nDims, nDims);
                // Quantize
                AshSphericalScalarQuantizer.QuantizeResult qr = quantizer.encode(xTransformed);
                float[][] xEnc = qr.centeredCodes();
                float[] codeNorms = qr.codeNorms();
                // Normalize encoded: xEnc[i] /= codeNorms[i]
                for (int i = 0; i < nTraining; i++) {
                    if (codeNorms[i] > 0) {
                        float inv = 1.0f / codeNorms[i];
                        for (int j = 0; j < nDims; j++) {
                            xEnc[i][j] *= inv;
                        }
                    }
                }
                // M = X_ld.T @ X_enc (nDims x nDims)
                m = matMulTransposeA(xLd, xEnc, nTraining, nDims, nDims);
            }
        }

        // W = P @ R (originalDim x nDims)
        return matMul(p, r, originalDim, nDims, nDims);
    }

    private float[][] randomOrthogonal(int originalDim, int nDims) {
        Random rng = new Random(seed);
        // Generate random matrix and orthogonalize columns via modified Gram-Schmidt
        float[][] q = new float[originalDim][nDims];
        for (int i = 0; i < originalDim; i++) {
            for (int j = 0; j < nDims; j++) {
                q[i][j] = (float) rng.nextGaussian();
            }
        }
        // Modified Gram-Schmidt: orthogonalize column by column
        for (int j = 0; j < nDims; j++) {
            // Subtract projections of previous columns
            for (int prev = 0; prev < j; prev++) {
                float dot = 0;
                for (int i = 0; i < originalDim; i++) {
                    dot = Math.fma(q[i][j], q[i][prev], dot);
                }
                for (int i = 0; i < originalDim; i++) {
                    q[i][j] = Math.fma(-dot, q[i][prev], q[i][j]);
                }
            }
            // Normalize (note iterating across rows)
            double normSq = 0;
            for (int i = 0; i < originalDim; i++) {
                normSq = Math.fma(q[i][j], q[i][j], normSq);
            }
            float invNorm = (float) (1.0 / Math.sqrt(normSq));
            for (int i = 0; i < originalDim; i++) {
                q[i][j] *= invNorm;
            }
        }
        return q;
    }

    /**
     * Returns an array of {@code sampleSize} distinct indices in [0, n), chosen via a
     * Fisher-Yates partial shuffle seeded with this quantizer's seed. If {@code sampleSize >= n}
     * the returned array is just [0, n) in order.
     */
    private int[] sampleIndices(int n, int sampleSize) {
        if (sampleSize >= n) {
            int[] all = new int[n];
            Arrays.setAll(all, IntUnaryOperator.identity());
            return all;
        }
        Random rng = new Random(seed);
        int[] indices = new int[n];
        Arrays.setAll(indices, IntUnaryOperator.identity());
        for (int i = 0; i < sampleSize; i++) {
            int j = i + rng.nextInt(n - i);
            int tmp = indices[i];
            indices[i] = indices[j];
            indices[j] = tmp;
        }
        int[] picked = new int[sampleSize];
        System.arraycopy(indices, 0, picked, 0, sampleSize);
        return picked;
    }

    /** C = A @ B where A is (m x k), B is (k x n).
     *  Uses row-broadcast accumulation for JIT auto-vectorization of the inner loop. */
    private static float[][] matMul(float[][] a, float[][] b, int m, int k, int n) {
        float[][] c = new float[m][n];
        for (int i = 0; i < m; i++) {
            float[] aRow = a[i];
            float[] cRow = c[i];
            for (int l = 0; l < k; l++) {
                float aVal = aRow[l];
                float[] bRow = b[l];
                for (int j = 0; j < n; j++) {
                    cRow[j] = Math.fma(aVal, bRow[j], cRow[j]);
                }
            }
        }
        return c;
    }

    /** C = A.T @ B where A is (m x k), B is (m x n), result is (k x n).
     *  Uses row-broadcast accumulation for cache-friendly access patterns. */
    private static float[][] matMulTransposeA(float[][] a, float[][] b, int m, int k, int n) {
        float[][] c = new float[k][n];
        // Accumulate by iterating over shared dimension (rows of A and B) in the outer loop.
        // This gives sequential reads on both a[l] and b[l], and scattered writes to c[i].
        for (int l = 0; l < m; l++) {
            float[] aRow = a[l];
            float[] bRow = b[l];
            for (int i = 0; i < k; i++) {
                float aVal = aRow[i];
                float[] cRow = c[i];
                for (int j = 0; j < n; j++) {
                    cRow[j] = Math.fma(aVal, bRow[j], cRow[j]);
                }
            }
        }
        return c;
    }
}
