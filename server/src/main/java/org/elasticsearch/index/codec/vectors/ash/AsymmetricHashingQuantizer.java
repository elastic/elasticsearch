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
import org.elasticsearch.simdvec.AshSphericalScalarQuantizer;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.simdvec.ESVectorizationProvider;
import org.elasticsearch.simdvec.VectorScorerFactory;

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

    private static final VectorScorerFactory FACTORY = ESVectorizationProvider.getInstance().getVectorScorerFactory();

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
     * Absolute cap on the number of vectors sampled for training W, independent of dimensionality.
     * {@code trainingFactor} scales the sample with {@code originalDim} (e.g. factor 10 at 3072 dims
     * would sample 30,720 vectors), which makes the training matrices — and their temporary
     * allocations during PCA/SVD — grow with dimension and pressure the heap. This cap bounds that
     * footprint for high-dimensional inputs; the projection subspace is well-estimated from a few
     * thousand samples, so the cap has negligible quality impact.
     * <p>
     * Note: at very high dimensionality (source dims >= 3072) the estimated subspace nDims grows large
     * and 8192 samples may be a thin margin; scaling this cap with nDims is a tracked follow-up (see #160522).
     */
    static final int MAX_TRAINING_SAMPLES = 8192;

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
        this.quantizer = FACTORY.newAshSphericalScalarQuantizer(bitsPerDim);
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
     * Result of training a projection matrix: the transposed matrix W^T together with whether it was
     * genuinely learned (PCA + Procrustes) or a random orthonormal fallback. The quantizer is the sole
     * owner of this distinction, so callers must not re-derive it. A random (non-learned) matrix must
     * not be inherited/warm-started at merge time.
     *
     * @param wT      the transposed projection matrix W^T in row-major order, shape (nDims, originalDim)
     * @param learned {@code true} if W was learned; {@code false} if it is a random orthonormal fallback
     */
    record TrainedProjection(float[] wT, boolean learned) {}

    /**
     * Trains the projection matrix W on the given vectors and their cluster assignments.
     * <p>
     * This method consumes draws from a per-call RNG seeded with the instance's seed, so
     * successive calls on the same instance will produce identical results.
     *
     * @param vectors random-access provider of the segment's vectors by ordinal
     * @param count number of vectors in the segment
     * @param originalDim vector dimensionality
     * @param centroids cluster centroids, fetched by vector ordinal
     * @return the trained projection (W^T plus whether it was learned or a random fallback)
     */
    TrainedProjection train(
        CheckedIntFunction<float[], IOException> vectors,
        int count,
        int originalDim,
        CheckedIntFunction<float[], IOException> centroids
    ) throws IOException {
        int nDims = nDims(originalDim);

        if (method == Method.RANDOM) {
            return new TrainedProjection(randomOrthogonal(originalDim, nDims), false);
        }

        // Too few vectors for meaningful PCA training; fall back to random projection
        if (method == Method.LEARNED && count < nDims * 2) {
            return new TrainedProjection(randomOrthogonal(originalDim, nDims), false);
        }

        int trainingSize = Math.min(Math.min(originalDim * trainingFactor, count), MAX_TRAINING_SAMPLES);
        float[] xTraining = buildTrainingMatrix(vectors, count, centroids, originalDim, trainingSize);

        // LEARNED: PCA init + Procrustes
        float[] wT = ESVectorUtil.transposeMatrix(learnedTraining(xTraining, trainingSize, originalDim, nDims), originalDim, nDims);
        return new TrainedProjection(wT, true);
    }

    /**
     * Warm-started training: refines an inherited projection matrix instead of learning W from
     * scratch. The supplied {@code inheritedWT} (transposed, shape {@code (nDims, originalDim)}) is
     * used directly as the orthonormal basis, skipping the expensive PCA / power-iteration
     * initialization, and a small number of Procrustes refinement iterations are run against the
     * current (merged) training data to re-fit the rotation.
     * <p>
     * Used at merge time to recover the recall of a freshly-trained W while retaining most of the
     * cost saving of reusing an input segment's matrix.
     *
     * @param vectors       random-access provider of the segment's vectors by ordinal
     * @param count         number of vectors in the segment
     * @param originalDim   vector dimensionality
     * @param centroids     cluster centroids, fetched by vector ordinal
     * @param inheritedWT   the inherited transposed projection matrix, shape (nDims, originalDim)
     * @param refineIterations number of Procrustes refinement iterations to run
     * @return the refined projection (W^T plus {@code learned=true}), or the inherited matrix unchanged
     *         if there are too few vectors to refine. The inherited matrix is always a learned matrix
     *         (the caller only warm-starts from learned inputs), so the result is always learned.
     */
    TrainedProjection trainWarmStart(
        CheckedIntFunction<float[], IOException> vectors,
        int count,
        int originalDim,
        CheckedIntFunction<float[], IOException> centroids,
        float[] inheritedWT,
        int refineIterations
    ) throws IOException {
        assert inheritedWT != null : "trainWarmStart requires a non-null inherited matrix to fall back to";
        int nDims = nDims(originalDim);

        // Warm-start: refine the inherited basis rather than recomputing it from scratch. We trust the
        // inherited W (the largest learned input segment's) as a good starting point. Follow-up (#160522):
        // add a residual-energy gate — compare energy retained by W (mean ||x·W||^2) against the total
        // (mean ||x||^2); if the inherited basis fits the merged set poorly, cold re-train instead of
        // warm-starting. This is a principled re-seed trigger to escape a poor local minimum.

        if (method != Method.LEARNED
            || inheritedWT == null
            || inheritedWT.length != originalDim * nDims
            || count < nDims * 2
            || refineIterations <= 0) {
            // Not refinable — return the inherited matrix as-is (caller already validated compatibility).
            return new TrainedProjection(inheritedWT, true);
        }

        int trainingSize = Math.min(Math.min(originalDim * trainingFactor, count), MAX_TRAINING_SAMPLES);
        float[] xTraining = buildTrainingMatrix(vectors, count, centroids, originalDim, trainingSize);

        // The basis P is the (originalDim x nDims) form of the inherited W^T.
        float[] p = ESVectorUtil.transposeMatrix(inheritedWT, nDims, originalDim);
        float[] w = learnedTrainingFromBasis(xTraining, p, trainingSize, originalDim, nDims, refineIterations);
        return new TrainedProjection(ESVectorUtil.transposeMatrix(w, originalDim, nDims), true);
    }

    /**
     * Samples {@code trainingSize} vectors, centers each by its cluster centroid, L2-normalizes it,
     * and returns them as a flat row-major {@code (trainingSize, originalDim)} array. Reads each
     * sampled vector once from {@code vectors} and copies it out, so it is safe with providers that
     * return a shared/live buffer.
     */
    private float[] buildTrainingMatrix(
        CheckedIntFunction<float[], IOException> vectors,
        int count,
        CheckedIntFunction<float[], IOException> centroids,
        int originalDim,
        int trainingSize
    ) throws IOException {
        int[] sampleIndices = sampleIndices(count, trainingSize);
        float[] xTraining = new float[trainingSize * originalDim];
        for (int i = 0; i < trainingSize; i++) {
            int srcIdx = sampleIndices[i];
            float[] centroid = centroids.apply(srcIdx);
            float[] vector = vectors.apply(srcIdx);
            int base = i * originalDim;
            for (int d = 0; d < originalDim; d++) {
                xTraining[base + d] = vector[d] - centroid[d];
            }
            ESVectorUtil.l2Normalize(xTraining, base, originalDim);
        }
        return xTraining;
    }

    /**
     * A vector with its precomputed squared norm
     * @param vector    The vector
     * @param normSq    Squared norm
     */
    public record VectorAndNorm(float[] vector, float normSq) {}

    private static VectorAndNorm centralizeVector(float[] vector, float[] centroid) {
        int originalDim = vector.length;
        float[] centered = new float[originalDim];
        float normSq = centralize(vector, centroid, centered, 0);
        return normSq == 0f ? new VectorAndNorm(new float[originalDim], 0) : new VectorAndNorm(centered, normSq);
    }

    /**
     * Centers {@code vector} by {@code centroid} into {@code out[outOffset..]} and L2-normalizes it in
     * place
     *
     * @return the squared norm of {@code vector - centroid}
     */
    private static float centralize(float[] vector, float[] centroid, float[] out, int outOffset) {
        int originalDim = vector.length;
        for (int d = 0; d < originalDim; d++) {
            out[outOffset + d] = vector[d] - centroid[d];
        }
        return ESVectorUtil.l2Normalize(out, outOffset, originalDim);
    }

    /** Scale applied at scoring time: the centered vector's norm relative to the norm of its code. */
    private static float computeScale(float normSq, float codeNorm) {
        return codeNorm > 0 ? (float) Math.sqrt(normSq) / codeNorm : 0;
    }

    /**
     * Additive correction for dot product reconstruction, per ASH paper Equation 19:
     * {@code ⟨x, μ⟩ - scale * ⟨centroid@W, code⟩ - ‖μ‖²}.
     * <p>
     * The cross-term ⟨centroid@W, code⟩ accounts for using the raw projected query Wq (Eq. 18)
     * rather than the centered query W(q-μ). At query time the scorer computes ⟨Wq, code⟩,
     * and the centroid's contribution is pre-subtracted here so no per-posting-list centroid
     * recomputation is needed during search.
     *
     * @param vecCentroidDot ⟨x, μ⟩ for the original, uncentered vector
     */
    private static float computeOffset(float vecCentroidDot, float scale, float[] code, VectorAndNorm precomputed) {
        float correction = ESVectorUtil.dotProduct(precomputed.vector(), code);
        return vecCentroidDot - precomputed.normSq() - scale * correction;
    }

    /**
     * Precomputes centroid-dependent values for a posting list. Call once per cluster,
     * then pass the result to {@link BlockEncoder#encode} for each vector in that cluster.
     *
     * @param centroid the posting list centroid, length originalDim
     * @param wT transposed projection matrix in row-major order, shape (nDims, originalDim)
     * @return precomputed values for this centroid
     */
    public static VectorAndNorm precomputeCentroid(float[] centroid, float[] wT) {
        int originalDim = centroid.length;
        int nDims = wT.length / originalDim;
        float[] centroidProjected = ESVectorUtil.matrixVectorMultiply(wT, nDims, originalDim, centroid);
        float centroidNormSq = ESVectorUtil.dotProduct(centroid, centroid);
        return new VectorAndNorm(centroidProjected, centroidNormSq);
    }

    /**
     * Creates a reusable encoder that projects and quantizes vectors a block at a time. The returned
     * encoder is tied to {@code wT}, so a caller must create a new one whenever W changes.
     *
     * @param wT the transposed projection matrix W^T in row-major order, shape (nDims, originalDim)
     * @param originalDim the original vector dimensionality
     * @param maxBlockSize the largest block the caller gathers before encoding
     */
    public BlockEncoder newBlockEncoder(float[] wT, int originalDim, int maxBlockSize) {
        return new BlockEncoder(wT, originalDim, maxBlockSize);
    }

    /**
     * Encodes vectors a block at a time using matrix multiplication
     */
    public final class BlockEncoder {

        private final int originalDim;
        private final int nDims;
        private final int maxBlockSize;
        /** W as (originalDim x nDims), the right-operand shape the block multiply needs. */
        private final float[] w;
        /** The gathered vectors, centered and L2-normalized, row-major (maxBlockSize x originalDim). */
        private final float[] centered;
        /** The block projected into latent space, row-major (maxBlockSize x nDims). */
        private final float[] latent;
        private final float[][] codes;
        private final float[] normSqs;
        private final float[] vecCentroidDots;
        private final float[] scales;
        private final float[] offsets;
        private int size;

        private BlockEncoder(float[] wT, int originalDim, int maxBlockSize) {
            assert wT.length == originalDim * nDims(originalDim)
                : "projection matrix length [" + wT.length + "] does not match originalDim [" + originalDim + "]";
            this.originalDim = originalDim;
            this.nDims = wT.length / originalDim;
            this.maxBlockSize = maxBlockSize;
            this.w = ESVectorUtil.transposeMatrix(wT, nDims, originalDim);
            this.centered = new float[maxBlockSize * originalDim];
            this.latent = new float[maxBlockSize * nDims];
            this.codes = new float[maxBlockSize][nDims];
            this.normSqs = new float[maxBlockSize];
            this.vecCentroidDots = new float[maxBlockSize];
            this.scales = new float[maxBlockSize];
            this.offsets = new float[maxBlockSize];
        }

        /** Discards the gathered block, ready for {@link #add}. */
        public void reset() {
            size = 0;
        }

        /**
         * Centers and normalizes {@code vector} into the next row of the block, and records the two
         * quantities that need the original vector: ‖vector - centroid‖² and ⟨vector, centroid⟩. The
         * caller may overwrite or reuse {@code vector} as soon as this returns, so a provider handing
         * out a shared buffer stays safe across a whole block.
         */
        public void add(float[] vector, float[] centroid) {
            assert size < maxBlockSize : "block is full";
            normSqs[size] = centralize(vector, centroid, centered, size * originalDim);
            vecCentroidDots[size] = ESVectorUtil.dotProduct(vector, centroid);
            size++;
        }

        /**
         * Projects, quantizes, and derives the scale and offset for every vector gathered since the
         * last {@link #reset}.
         *
         * @param precomputed the projected centroid and its squared norm for the posting list these
         *        vectors belong to, from {@link AsymmetricHashingQuantizer#precomputeCentroid}
         */
        public void encode(VectorAndNorm precomputed) {
            // The multiply doesn't have offsets, so the full matrix is calculated
            // Although this means that partial blocks multiply whatever is left in 'centered' after the tail,
            // only 'count' rows are read back
            ESVectorUtil.matrixMultiply(centered, w, maxBlockSize, originalDim, nDims, latent);

            for (int i = 0; i < size; i++) {
                float[] code = codes[i];
                float codeNorm = quantizer.quantizeExact(latent, i * nDims, code, 0, nDims);
                float scale = computeScale(normSqs[i], codeNorm);
                scales[i] = scale;
                offsets[i] = computeOffset(vecCentroidDots[i], scale, code, precomputed);
            }
        }

        /** Number of vectors gathered since the last {@link #reset}. */
        public int size() {
            return size;
        }

        /**
         * The quantized code of the {@code i}th vector of the block, length nDims. Valid until the
         * next {@link #encode} call.
         */
        public float[] code(int i) {
            assert i < size;
            return codes[i];
        }

        /** The scoring scale of the {@code i}th vector of the block. */
        public float scale(int i) {
            assert i < size;
            return scales[i];
        }

        /** The dot product reconstruction offset of the {@code i}th vector of the block. */
        public float offset(int i) {
            assert i < size;
            return offsets[i];
        }

        /** {@code ⟨x, μ⟩} for the {@code i}th vector of the block, taken before it was centered. */
        public float vecCentroidDot(int i) {
            assert i < size;
            return vecCentroidDots[i];
        }
    }

    private float[] learnedTraining(float[] xTraining, int nTraining, int originalDim, int nDims) {
        // PCA initialization: extract top nDims right singular vectors as columns (originalDim x nDims)
        // This is much faster than full SVD when nDims << originalDim
        float[] p = AshUtils.topKRightSingularVectors(xTraining, nTraining, originalDim, nDims, seed);
        return learnedTrainingFromBasis(xTraining, p, nTraining, originalDim, nDims, nTrainingIterations);
    }

    /**
     * Runs the iterative Procrustes refinement starting from a given orthonormal basis {@code p}
     * (shape {@code (originalDim, nDims)}), returning the learned projection matrix W = P @ R.
     * <p>
     * Factored out of {@link #learnedTraining} so a warm-started training can supply an inherited
     * projection matrix as the basis (skipping the expensive PCA / power-iteration initialization)
     * and run a small number of refinement iterations against the current training data.
     */
    private float[] learnedTrainingFromBasis(float[] xTraining, float[] p, int nTraining, int originalDim, int nDims, int nIterations) {
        // Project training data: X_ld = xTraining @ P (nTraining x nDims)
        float[] xLd = ESVectorUtil.matrixMultiply(xTraining, p, nTraining, originalDim, nDims);

        // Pre-transpose X_ld so that X_ld^T @ X_enc can use sequential memory access
        float[] xLdT = ESVectorUtil.transposeMatrix(xLd, nTraining, nDims);

        // Initialize random M (nDims x nDims)
        float[] m = AshUtils.randomGaussians(new Random(seed), nDims * nDims);

        // Iterative Procrustes
        float[] r = new float[nDims * nDims];
        float[] xTransformed = new float[nTraining * nDims];
        AshSphericalScalarQuantizer.QuantizeResult qr = new AshSphericalScalarQuantizer.QuantizeResult(nTraining, nDims);

        for (int epoch = 0; epoch <= nIterations; epoch++) {
            // R = procrustes(M)
            AshUtils.procrustes(m, nDims, r);

            if (epoch < nIterations) {
                // X_transformed = X_ld @ R (nTraining x nDims)
                ESVectorUtil.matrixMultiply(xLd, r, nTraining, nDims, nDims, xTransformed);
                // Quantize
                quantizer.encode(xTransformed, nTraining, nDims, qr);
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
                // M = X_ld^T @ X_enc (nDims x nDims) — uses pre-transposed X_ld for sequential access
                ESVectorUtil.matrixMultiply(xLdT, xEnc, nDims, nTraining, nDims, m);
            }
        }

        // W = P @ R (originalDim x nDims)
        return ESVectorUtil.matrixMultiply(p, r, originalDim, nDims, nDims);
    }

    private float[] randomOrthogonal(int originalDim, int nDims) {
        // qrOrthogonalize orthonormalizes rows, so it produces W^T (nDims x originalDim)
        float[] qT = AshUtils.randomGaussians(new Random(seed), originalDim * nDims);
        AshUtils.qrOrthogonalize(qT, originalDim, nDims);
        return qT;
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
