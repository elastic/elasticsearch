/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.elasticsearch.common.CheckedIntFunction;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfSegmentConfig;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
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
 * <p>
 * All matrices (W, Wt, P, R, etc.) are represented as flat row-major {@code float[]} of
 * length rows*cols.
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
     * @throws IllegalArgumentException if {@code bitsPerDim} is not a
     *         {@linkplain IvfSegmentConfig.AshConfig#isValidBitsPerDim(int) valid ASH bit width} or
     *         {@code projectedDimsFraction} is not in (0, 1]
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
        IvfSegmentConfig.AshConfig.validateBitsPerDim(bitsPerDim);
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
     * <p>
     * This method consumes draws from a per-call RNG seeded with the instance's seed, so
     * successive calls on the same instance will produce identical results.
     *
     * @param vectors all vectors in the segment, shape (nVectors, originalDim)
     * @param centroids cluster centroids, fetched by vector ordinal
     * @return the learned projection matrix W in row-major order, shape (originalDim, nDims)
     */
    public float[] train(float[][] vectors, CheckedIntFunction<float[], IOException> centroids) throws IOException {
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

        // Center and normalize the sampled vectors into a fresh flat array. We must not mutate
        // `vectors` in place -- the writer reuses it for per-posting-list encoding later.
        float[] xTraining = new float[trainingSize * originalDim];
        for (int i = 0; i < trainingSize; i++) {
            int srcIdx = sampleIndices[i];
            float[] centroid = centroids.apply(srcIdx);
            int base = i * originalDim;
            for (int d = 0; d < originalDim; d++) {
                xTraining[base + d] = vectors[srcIdx][d] - centroid[d];
            }
            ESVectorUtil.l2Normalize(xTraining, base, originalDim);
        }

        // LEARNED: PCA init + Procrustes
        return learnedTraining(xTraining, trainingSize, originalDim, nDims);
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
     * A vector with its precomputed squared norm
     * @param vector    The vector
     * @param normSq    Squared norm
     */
    public record VectorAndNorm(float[] vector, float normSq) {}

    private static VectorAndNorm centralizeVector(float[] vector, float[] centroid) {
        int originalDim = vector.length;
        float[] centered = new float[originalDim];
        for (int d = 0; d < originalDim; d++) {
            centered[d] = vector[d] - centroid[d];
        }
        float normSq = ESVectorUtil.l2Normalize(centered);
        return normSq == 0f ? new VectorAndNorm(new float[originalDim], 0) : new VectorAndNorm(centered, normSq);
    }

    /**
     * Precomputes centroid-dependent values for a posting list. Call once per cluster,
     * then pass the result to {@link #encode} for each vector in that cluster.
     *
     * @param centroid the posting list centroid, length originalDim
     * @param wT transposed projection matrix in row-major order, shape (nDims, originalDim)
     * @return precomputed values for this centroid
     */
    public static VectorAndNorm precomputeCentroid(float[] centroid, float[] wT) {
        int originalDim = centroid.length;
        int nDims = wT.length / originalDim;
        float[] centroidProjected = SvdUtil.matrixVectorMultiply(wT, nDims, originalDim, centroid);
        float centroidNormSq = ESVectorUtil.dotProduct(centroid, centroid);
        return new VectorAndNorm(centroidProjected, centroidNormSq);
    }

    /**
     * Fast single-vector encoding using precomputed centroid values and transposed W.
     * This avoids recomputing centroid @ W and ||centroid||^2 for every vector in a posting list.
     *
     * @param vector the input vector, length originalDim
     * @param centroid the centroid (needed for centering), length originalDim
     * @param wT transposed projection matrix in row-major order, shape (nDims, originalDim)
     * @param precomputed precomputed centroid projection and norm
     * @return xEnc/scale/offset for this (vector, centroid) pair
     */
    public EncodedVector encode(float[] vector, float[] centroid, float[] wT, VectorAndNorm precomputed) {
        int originalDim = centroid.length;
        int nDims = wT.length / originalDim;

        // Center and compute norm
        var centered = centralizeVector(vector, centroid);

        // Project using transposed W
        float[] xLatent = SvdUtil.matrixVectorMultiply(wT, nDims, originalDim, centered.vector());

        // Quantize
        AshSphericalScalarQuantizer.SingleQuantizeResult qr = quantizer.encodeOne(xLatent);
        float[] xEnc = qr.centeredCode();
        float codeNorm = qr.codeNorm();

        // Scale: norm / codeNorm
        float scale = codeNorm > 0 ? (float) Math.sqrt(centered.normSq()) / codeNorm : 0;

        // Offset per ASH paper Equation 19: ⟨x, μ⟩ - scale * ⟨centroid@W, code⟩ - ‖μ‖²
        // The cross-term ⟨centroid@W, code⟩ accounts for using the raw projected query Wq (Eq. 18)
        // rather than the centered query W(q-μ). At query time the scorer computes ⟨Wq, code⟩,
        // and the centroid's contribution is pre-subtracted here so no per-posting-list centroid
        // recomputation is needed during search.
        float dotVecCent = ESVectorUtil.dotProduct(vector, centroid);
        float offset = dotVecCent - precomputed.normSq();
        float[] centroidProjected = precomputed.vector();
        float correction = ESVectorUtil.dotProduct(centroidProjected, xEnc);
        offset -= scale * correction;

        return new EncodedVector(xEnc, scale, offset);
    }

    private float[] learnedTraining(float[] xTraining, int nTraining, int originalDim, int nDims) {
        // PCA initialization: extract top nDims right singular vectors via power iteration
        // This is much faster than full SVD when nDims << originalDim
        float[] topVectors = SvdUtil.topKRightSingularVectors(xTraining, nTraining, originalDim, nDims, seed);

        // P = top nDims right singular vectors transposed: rows of topVectors are the vectors
        // topVectors shape: (nDims x originalDim); P shape: (originalDim x nDims)
        float[] p = ESVectorUtil.transposeMatrix(topVectors, nDims, originalDim);

        // Project training data: X_ld = xTraining @ P (nTraining x nDims)
        float[] xLd = SvdUtil.matrixMultiply(xTraining, p, nTraining, originalDim, nDims);

        // Initialize random M (nDims x nDims)
        float[] m = SvdUtil.randomGaussians(new Random(seed), nDims * nDims);

        // Iterative Procrustes
        float[] r = null;
        for (int epoch = 0; epoch <= nTrainingIterations; epoch++) {
            // R = procrustes(M)
            r = SvdUtil.procrustes(m, nDims);

            if (epoch < nTrainingIterations) {
                // X_transformed = X_ld @ R (nTraining x nDims)
                float[] xTransformed = SvdUtil.matrixMultiply(xLd, r, nTraining, nDims, nDims);
                // Quantize
                AshSphericalScalarQuantizer.QuantizeResult qr = quantizer.encode(xTransformed, nTraining, nDims);
                float[] xEnc = qr.centeredCodes();
                float[] codeNorms = qr.codeNorms();
                // Normalize encoded: xEnc[i] /= codeNorms[i]
                for (int i = 0; i < nTraining; i++) {
                    if (codeNorms[i] > 0) {
                        float inv = 1.0f / codeNorms[i];
                        int base = i * nDims;
                        for (int j = 0; j < nDims; j++) {
                            xEnc[base + j] *= inv;
                        }
                    }
                }
                // M = X_ld.T @ X_enc (nDims x nDims)
                m = SvdUtil.matrixMultiplyTA(xLd, xEnc, nTraining, nDims, nDims);
            }
        }

        // W = P @ R (originalDim x nDims)
        return SvdUtil.matrixMultiply(p, r, originalDim, nDims, nDims);
    }

    private float[] randomOrthogonal(int originalDim, int nDims) {
        float[] q = SvdUtil.randomGaussians(new Random(seed), originalDim * nDims);
        SvdUtil.qrOrthogonalize(q, originalDim, nDims);
        return q;
    }

    /**
     * Returns an array of {@code sampleSize} distinct indices in [0, n), chosen via a
     * Fisher-Yates partial shuffle seeded with this quantizer's seed. If {@code sampleSize >= n}
     * the returned array is just [0, n) in order.
     */
    private int[] sampleIndices(int n, int sampleSize) {
        int[] indices = new int[n];
        Arrays.setAll(indices, IntUnaryOperator.identity());
        if (sampleSize >= n) {
            return indices;
        }
        Random rng = new Random(seed);
        for (int i = 0; i < sampleSize; i++) {
            int j = i + rng.nextInt(n - i);
            int tmp = indices[i];
            indices[i] = indices[j];
            indices[j] = tmp;
        }
        return Arrays.copyOf(indices, sampleSize);
    }

}
