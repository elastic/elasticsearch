/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.apache.lucene.search.TaskExecutor;
import org.elasticsearch.common.CheckedIntFunction;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfSegmentConfig;
import org.elasticsearch.simdvec.AshSphericalScalarQuantizer;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.simdvec.ESVectorizationProvider;
import org.elasticsearch.simdvec.VectorScorerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
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
 *   <li>A projection matrix W = P R is built: P spans the dominant subspace of the (centered, normalized)
 *       training vectors, found by subspace iteration on their second-moment matrix, and R is an orthogonal
 *       rotation that spreads variance across the projected coordinates. R is random, which does so on average,
 *       unless Procrustes iterations are configured to refine it for inner product preservation after quantization</li>
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
        /** Learn the subspace of W by PCA of the training vectors, then rotate it (randomly, or refined by Procrustes iterations). */
        LEARNED,
        /** Use a random orthogonal matrix (no training). */
        RANDOM
    }

    private final float projectedDimsFraction;
    private final int maxProjectedDims;
    private final Method method;
    private final int bitsPerDim;
    private final int nTrainingIterations;
    private final int trainingFactor;
    private final long seed;
    private final AshSphericalScalarQuantizer quantizer;
    private final TaskExecutor executor;

    /**
     * Rows of the training sample gathered before they are folded into the second-moment matrix. The
     * sample itself is never held in memory, so the training footprint is O(originalDim^2) however
     * large the sample is; this only needs to be big enough for the block multiply to be efficient.
     */
    private static final int SECOND_MOMENT_BLOCK_ROWS = 1024;

    /**
     * Cap on the number of vectors in the projected training sample used by the Procrustes iterations (only when
     * {@code nTrainingIterations} is positive). Unlike the second-moment matrix, that sample is held in memory, and the
     * iterations cost O(vectors * nDims^2) each, so it is bounded however large the training sample for the subspace is.
     */
    static final int MAX_PROCRUSTES_SAMPLES = 8192;

    /**
     * The fewest vectors from which a projection is learned; with fewer, a random projection is used. A segment is
     * encoded with a projection learned from its own vectors, and with no more vectors than projected dimensions that
     * projection keeps all of every vector's residual (the residuals span at most that many dimensions), where a random
     * one keeps only about the fraction {@code nDims / originalDim} of it. A single vector has no residual from its own
     * centroid, so there is nothing to learn from, and two is the smallest sample that has one.
     */
    static final int MIN_PCA_VECTORS = 2;

    /**
     * Largest bit width for which blocks are projected with int8 arithmetic. At 8 bits the int8 rounding
     * error is as large as the quantization step, and doubles the dot product reconstruction error.
     */
    static final int MAX_INT8_PROJECTION_BITS = 4;

    /**
     * Creates an ASH quantizer with the given configuration.
     *
     * @param projectedDimsFraction fraction of original dimensions to project to (e.g. 0.5 for half)
     * @param maxProjectedDims
     *        maximum number of projected dims
     *        (calculated as {@code min(maxProjectedDims, originalDim * projectedDimsFraction)}
     * @param bitsPerDim bits per projected dimension in the body
     * @param method training method for W
     * @param nTrainingIterations number of Procrustes iterations refining the rotation of the PCA subspace (for LEARNED);
     *        zero keeps the rotation random
     * @param trainingFactor multiplier on dimension for training sample size
     * @param seed random seed
     * @throws IllegalArgumentException if any argument is outside its valid range
     */
    public AsymmetricHashingQuantizer(
        float projectedDimsFraction,
        int maxProjectedDims,
        int bitsPerDim,
        Method method,
        int nTrainingIterations,
        int trainingFactor,
        long seed,
        TaskExecutor executor
    ) {
        if (projectedDimsFraction <= 0 || projectedDimsFraction > 1.0f) {
            throw new IllegalArgumentException("projectedDimsFraction must be in (0, 1]");
        }
        if (maxProjectedDims <= 0) {
            throw new IllegalArgumentException("maxProjectedDims must be > 0");
        }
        IvfSegmentConfig.AshConfig.validateBitsPerDim(bitsPerDim);
        if (nTrainingIterations < 0) {
            throw new IllegalArgumentException("nTrainingIterations must not be negative");
        }
        this.projectedDimsFraction = projectedDimsFraction;
        this.maxProjectedDims = maxProjectedDims;
        this.method = method;
        this.bitsPerDim = bitsPerDim;
        this.nTrainingIterations = nTrainingIterations;
        this.trainingFactor = trainingFactor;
        this.seed = seed;
        this.quantizer = FACTORY.newAshSphericalScalarQuantizer(bitsPerDim);
        this.executor = executor;
    }

    /**
     * Computes the number of projected dimensions for a given original dimension.
     *
     * @param originalDim the original vector dimensionality
     * @return the number of projected dimensions
     */
    public int nDims(int originalDim) {
        return Math.min(maxProjectedDims, (int) (originalDim * projectedDimsFraction));
    }

    /**
     * Result of training a projection matrix: the matrix W and its transposition W^T together with whether it was
     * learned (PCA + rotation) from enough vectors to be inherited. The quantizer is the sole owner of this
     * distinction, so callers must not re-derive it. A matrix that is not learned, because it is a random orthonormal
     * fallback or was learned from fewer than twice as many vectors as projected dimensions, must not be
     * inherited/warm-started at merge time.
     *
     * @param w       the projection matrix W in row-major order, shape (originalDim, nDims)
     * @param wT      the transposed projection matrix W^T in row-major order, shape (nDims, originalDim)
     * @param learned {@code true} if W was learned from at least twice as many vectors as projected dimensions;
     *                {@code false} if it is a random orthonormal fallback or was learned from fewer vectors
     */
    record TrainedProjection(float[] w, float[] wT, boolean learned) {}

    /**
     * Trains the projection matrix W on the given vectors and their cluster assignments.
     * <p>
     * This method consumes draws from a per-call RNG seeded with the instance's seed, so successive calls on the
     * same instance produce the same result, up to floating-point rounding: the vectorized kernels can round
     * differently depending on how they were compiled.
     * <p>
     * With at least {@link #MIN_PCA_VECTORS} vectors the projection is learned, even from fewer vectors than
     * projected dimensions (the remaining directions are random). It is reported as {@code learned}, and so
     * eligible to be inherited at merge, only if there were at least twice as many vectors as projected dimensions.
     *
     * @param vectors random-access provider of the segment's vectors by ordinal
     * @param count number of vectors in the segment
     * @param originalDim vector dimensionality
     * @param centroids cluster centroids, fetched by vector ordinal
     * @return the trained projection (W^T plus whether it was learned from enough vectors to be inherited)
     */
    TrainedProjection train(
        CheckedIntFunction<float[], IOException> vectors,
        int count,
        int originalDim,
        CheckedIntFunction<float[], IOException> centroids
    ) throws IOException {
        int nDims = nDims(originalDim);

        if (method == Method.RANDOM) {
            return randomProjection(originalDim, nDims);
        }

        // Too few vectors for the sample to say anything about the data; use a random projection
        if (count < MIN_PCA_VECTORS) {
            return randomProjection(originalDim, nDims);
        }

        // Only a projection estimated from enough vectors to resolve all nDims directions may be inherited at merge:
        // warm-starting runs just a few iterations, which cannot repair a poorly estimated start
        boolean learned = count >= nDims * 2;

        // No dimensionality reduction: W is a pure rotation, and any orthonormal basis spans the whole space. Procrustes
        // iterations, when configured, still refine that rotation, so they take the general path below.
        if (nDims == originalDim && nTrainingIterations == 0) {
            float[] wT = randomOrthogonal(originalDim, nDims);
            return new TrainedProjection(ESVectorUtil.transposeMatrix(wT, nDims, originalDim), wT, learned);
        }

        int trainingSize = Math.min(originalDim * trainingFactor, count);
        float[] secondMoment = buildSecondMoment(vectors, count, centroids, originalDim, trainingSize);
        if (trace(secondMoment, originalDim) <= 0) {
            // every training vector is its own centroid: there is no variance to learn from
            return randomProjection(originalDim, nDims);
        }

        // Only the span of the top nDims eigenvectors matters, so we iterate to a subspace rather than
        // resolving eigenvectors, and then rotate it to spread the variance over the projected coordinates.
        // A sample of m vectors has a second-moment matrix of rank at most m, so with fewer vectors than nDims it can
        // only supply m directions. Take all of them, so that the projection keeps everything the sample spans, and
        // complete the basis with random directions orthogonal to them.
        int rank = Math.min(nDims, trainingSize);
        float[] pT = AshUtils.dominantSubspace(secondMoment, originalDim, rank, seed, executor);
        if (rank < nDims) {
            pT = AshUtils.completeBasis(pT, originalDim, rank, nDims, new Random(seed + 2), executor);
        }
        float[] wT = rotated(pT, vectors, count, centroids, originalDim, nDims, trainingSize);
        return new TrainedProjection(ESVectorUtil.transposeMatrix(wT, nDims, originalDim), wT, learned);
    }

    /**
     * Rotates the orthonormal basis {@code pT} (shape {@code (nDims, originalDim)}) into the projection W^T = R P^T.
     * The subspace is all that PCA determines; R then spreads the variance over the projected coordinates (a random
     * rotation does so on average), which the scalar quantization that follows needs. R is a random orthogonal matrix,
     * unless Procrustes iterations are configured and the projected sample has at least {@code nDims} vectors. Below
     * that the matrix whose polar factor the Procrustes step computes has rank less than {@code nDims}, and the factor
     * is not orthogonal.
     *
     * @return W^T, shape {@code (nDims, originalDim)}
     */
    private float[] rotated(
        float[] pT,
        CheckedIntFunction<float[], IOException> vectors,
        int count,
        CheckedIntFunction<float[], IOException> centroids,
        int originalDim,
        int nDims,
        int trainingSize
    ) throws IOException {
        int procrustesSize = Math.min(trainingSize, MAX_PROCRUSTES_SAMPLES);
        if (nTrainingIterations > 0 && procrustesSize >= nDims) {
            // Refine the rotation by Procrustes iterations on the sample projected onto the subspace
            float[] xTraining = buildTrainingMatrix(vectors, count, centroids, originalDim, procrustesSize);
            float[] p = ESVectorUtil.transposeMatrix(pT, nDims, originalDim);
            float[] w = learnedTrainingFromBasis(xTraining, p, procrustesSize, originalDim, nDims, nTrainingIterations);
            return ESVectorUtil.transposeMatrix(w, originalDim, nDims);
        }
        float[] rotation = AshUtils.randomOrthogonal(new Random(seed + 1), nDims, executor);
        return ESVectorUtil.matrixMultiply(rotation, pT, nDims, nDims, originalDim, executor);
    }

    /** The sum of the diagonal of a square row-major matrix: the total energy, for a second-moment matrix. */
    private static double trace(float[] matrix, int n) {
        double trace = 0;
        for (int i = 0; i < n; i++) {
            trace += matrix[i * n + i];
        }
        return trace;
    }

    /** A random orthonormal projection, reported as not learned so that it is never inherited at merge. */
    private TrainedProjection randomProjection(int originalDim, int nDims) {
        float[] wT = randomOrthogonal(originalDim, nDims);
        return new TrainedProjection(ESVectorUtil.transposeMatrix(wT, nDims, originalDim), wT, false);
    }

    /**
     * Warm-started training: refines an inherited projection matrix instead of learning W from
     * scratch. The supplied {@code inheritedWT} (transposed, shape {@code (nDims, originalDim)}) is
     * used as the starting subspace, and a small number of subspace iteration steps are run against the
     * second-moment matrix of the current (merged) training data to re-fit it. The iteration keeps the span of the
     * basis but not the basis itself: it pulls the rows toward the leading eigenvectors, which undoes the inherited
     * rotation and leaves the variance concentrated in the first coordinates. So a new rotation is applied
     * afterwards, in the same way as when training from scratch (random, or refined by Procrustes iterations).
     * <p>
     * Used at merge time to recover the recall of a freshly-trained W while needing far fewer
     * iterations than training W from scratch.
     *
     * @param vectors       random-access provider of the segment's vectors by ordinal
     * @param count         number of vectors in the segment
     * @param originalDim   vector dimensionality
     * @param centroids     cluster centroids, fetched by vector ordinal
     * @param inheritedWT   the inherited transposed projection matrix, shape (nDims, originalDim)
     * @param refineIterations most multiplications by the second-moment matrix to run
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
            return new TrainedProjection(ESVectorUtil.transposeMatrix(inheritedWT, nDims, originalDim), inheritedWT, true);
        }

        int trainingSize = Math.min(originalDim * trainingFactor, count);
        float[] secondMoment = buildSecondMoment(vectors, count, centroids, originalDim, trainingSize);
        if (trace(secondMoment, originalDim) <= 0) {
            // no variance in the merged vectors to refine the inherited matrix with
            return new TrainedProjection(ESVectorUtil.transposeMatrix(inheritedWT, nDims, originalDim), inheritedWT, true);
        }

        // refineSubspace overwrites its starting basis, and the caller still owns the inherited matrix
        float[] refinedT = AshUtils.refineSubspace(secondMoment, originalDim, nDims, inheritedWT.clone(), refineIterations, executor);
        float[] wT = rotated(refinedT, vectors, count, centroids, originalDim, nDims, trainingSize);
        return new TrainedProjection(ESVectorUtil.transposeMatrix(wT, nDims, originalDim), wT, true);
    }

    /**
     * Samples {@code trainingSize} vectors, centers each by its cluster centroid, L2-normalizes it, and
     * accumulates {@code sum(x x^T)} as a flat row-major {@code (originalDim, originalDim)} array.
     * <p>
     * The sample is streamed through in blocks and never held whole, so the memory needed is independent
     * of {@code trainingSize}. Each sampled vector is read once and copied out, so this is safe with
     * providers that return a shared/live buffer. The sampled ordinals are visited in increasing order,
     * which keeps access to off-heap vectors close to sequential; the sum does not depend on the order.
     * <p>
     * The matrix is left unscaled: the subspace it determines is the same as for the mean.
     * <p>
     * Package-private so that tests can check it against an independent sum.
     */
    float[] buildSecondMoment(
        CheckedIntFunction<float[], IOException> vectors,
        int count,
        CheckedIntFunction<float[], IOException> centroids,
        int originalDim,
        int trainingSize
    ) throws IOException {
        int[] sampleIndices = sampleIndices(count, trainingSize);
        Arrays.sort(sampleIndices);

        float[] secondMoment = new float[originalDim * originalDim];
        float[] blockProduct = new float[originalDim * originalDim];
        int blockRows = Math.min(trainingSize, SECOND_MOMENT_BLOCK_ROWS);
        float[] block = new float[blockRows * originalDim];
        float[] blockT = new float[blockRows * originalDim];

        for (int start = 0; start < trainingSize; start += blockRows) {
            int rows = Math.min(blockRows, trainingSize - start);
            if (rows != blockRows) {
                // the final, partial block: the multiply needs exactly-sized arrays
                block = new float[rows * originalDim];
                blockT = new float[rows * originalDim];
            }
            for (int i = 0; i < rows; i++) {
                int srcIdx = sampleIndices[start + i];
                float[] centroid = centroids.apply(srcIdx);
                float[] vector = vectors.apply(srcIdx);
                int base = i * originalDim;
                System.arraycopy(vector, 0, block, base, originalDim);
                ESVectorUtil.linearCombination(-1f, centroid, 0, block, base, originalDim); // block row = vector - centroid
                ESVectorUtil.l2Normalize(block, base, originalDim);
            }
            // secondMoment += block^T @ block, which is symmetric
            ESVectorUtil.transposeMatrix(block, rows, originalDim, blockT);
            ESVectorUtil.matrixMultiplySymmetric(blockT, block, originalDim, rows, blockProduct, executor);
            ESVectorUtil.linearCombination(1f, blockProduct, 1f, secondMoment);
        }
        return secondMoment;
    }

    /**
     * A vector with its precomputed squared norm
     * @param vector    The vector
     * @param normSq    Squared norm
     */
    public record VectorAndNorm(float[] vector, float normSq) {}

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
     * encoder is tied to {@code w}, so a caller must create a new one whenever W changes.
     *
     * @param w the projection matrix W in row-major order, shape (originalDim, nDims)
     * @param originalDim the original vector dimensionality
     * @param maxBlockSize the largest block the caller gathers before encoding
     */
    public BlockEncoder newBlockEncoder(float[] w, int originalDim, int maxBlockSize) {
        return newBlockEncoder(w, originalDim, maxBlockSize, bitsPerDim <= MAX_INT8_PROJECTION_BITS);
    }

    BlockEncoder newBlockEncoder(float[] w, int originalDim, int maxBlockSize, boolean int8) {
        return int8 ? new Int8BlockEncoder(w, originalDim, maxBlockSize) : new FloatBlockEncoder(w, originalDim, maxBlockSize);
    }

    /**
     * Encodes vectors a block at a time using matrix multiplication. Subclasses differ only in how a block is
     * projected into latent space; quantization and the scale and offset derivation are shared.
     */
    public abstract sealed class BlockEncoder implements Closeable permits FloatBlockEncoder, Int8BlockEncoder {

        protected final int originalDim;
        protected final int nDims;
        protected final int maxBlockSize;
        /** The block projected into latent space, row-major (maxBlockSize x nDims). Rows are only proportional to centered @ W. */
        protected final float[] latent;
        private final float[][] codes;
        private final float[] normSqs;
        private final float[] vecCentroidDots;
        private final float[] scales;
        private final float[] offsets;
        protected int size;

        private BlockEncoder(float[] w, int originalDim, int maxBlockSize) {
            assert w.length == originalDim * nDims(originalDim)
                : "projection matrix length [" + w.length + "] does not match originalDim [" + originalDim + "]";
            this.originalDim = originalDim;
            this.nDims = w.length / originalDim;
            this.maxBlockSize = maxBlockSize;
            this.latent = new float[maxBlockSize * nDims];
            this.codes = new float[maxBlockSize][nDims];
            this.normSqs = new float[maxBlockSize];
            this.vecCentroidDots = new float[maxBlockSize];
            this.scales = new float[maxBlockSize];
            this.offsets = new float[maxBlockSize];
        }

        /**
         * Stores {@code vector - centroid} as row {@code row} of the block, in whatever form {@link #project} needs.
         *
         * @return ‖vector - centroid‖²
         */
        protected abstract float addRow(float[] vector, float[] centroid, int row);

        /**
         * Fills the first {@code size} rows of {@link #latent} with the projection of the gathered rows.
         * Only the direction of each row matters, as quantization is invariant to a positive scale on a row.
         */
        protected abstract void project();

        /** Discards the gathered block, ready for {@link #add}. */
        public void reset() {
            size = 0;
        }

        /**
         * Centers {@code vector} into the next row of the block, and records the two
         * quantities that need the original vector: ‖vector - centroid‖² and ⟨vector, centroid⟩. The
         * caller may overwrite or reuse {@code vector} as soon as this returns, so a provider handing
         * out a shared buffer stays safe across a whole block.
         */
        public final void add(float[] vector, float[] centroid) {
            assert size < maxBlockSize : "block is full";
            normSqs[size] = addRow(vector, centroid, size);
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
        public final void encode(VectorAndNorm precomputed) {
            project();

            for (int i = 0; i < size; i++) {
                float[] code = codes[i];
                float codeNorm = quantizer.quantizeExact(latent, i * nDims, code, 0, nDims);
                float scale = codeNorm > 0 ? (float) Math.sqrt(normSqs[i]) / codeNorm : 0;
                scales[i] = scale;
                offsets[i] = computeOffset(vecCentroidDots[i], scale, code, precomputed);
            }
        }

        public int size() {
            return size;
        }

        public float[] code(int i) {
            assert i < size;
            return codes[i];
        }

        public float scale(int i) {
            assert i < size;
            return scales[i];
        }

        public float offset(int i) {
            assert i < size;
            return offsets[i];
        }

        public float vecCentroidDot(int i) {
            assert i < size;
            return vecCentroidDots[i];
        }

        @Override
        public void close() {}
    }

    /** Projects with a float matrix multiply. Used where int8 rounding error is comparable to the quantization step. */
    private final class FloatBlockEncoder extends BlockEncoder {
        /** W as (originalDim x nDims), the right-operand shape the block multiply needs. */
        private final float[] w;
        /** The gathered vectors, centered and L2-normalized, row-major (maxBlockSize x originalDim). */
        private final float[] centered;

        private FloatBlockEncoder(float[] w, int originalDim, int maxBlockSize) {
            super(w, originalDim, maxBlockSize);
            this.w = w;
            this.centered = new float[maxBlockSize * originalDim];
        }

        @Override
        protected float addRow(float[] vector, float[] centroid, int row) {
            int originalDim = vector.length;
            int outOffset = row * originalDim;
            for (int d = 0; d < originalDim; d++) {
                centered[outOffset + d] = vector[d] - centroid[d];
            }
            return ESVectorUtil.l2Normalize(centered, outOffset, originalDim);
        }

        @Override
        protected void project() {
            // The multiply doesn't have offsets, so the full matrix is calculated
            // Although this means that partial blocks multiply whatever is left in 'centered' after the tail,
            // only 'size' rows are read back
            ESVectorUtil.matrixMultiply(centered, w, maxBlockSize, originalDim, nDims, latent, executor);
        }
    }

    /**
     * Projects with an int8 matrix multiply, which is exact in int32 and faster than the float multiply.
     * W is scaled per column and each gathered row by its own maximum. A per-row scale does not change
     * the quantized code, so rows are not normalized and the row scale is not kept. The per-column scale
     * does change the row's shape, so it is applied when converting the integer products to floats.
     */
    private final class Int8BlockEncoder extends BlockEncoder {

        private static final float INT8_MAX = Byte.MAX_VALUE;

        private static byte toInt8(float value, float inverseScale) {
            // use [-MAX, MAX] so it's symmetric
            return (byte) Math.clamp(Math.round(value * inverseScale), -INT8_MAX, INT8_MAX);
        }

        // matrix mult may be multi-threaded, so arena needs to be shared
        private final Arena arena = Arena.ofShared();
        // quantized version of W
        private final MemorySegment wq;
        // dequantization scale of each column of W, 0 for an all-zero column. */
        private final float[] wScales;
        // gathered vectors, centered and quantized to int8, row-major (maxBlockSize x originalDim). */
        private final MemorySegment centeredQ;
        // temp bytes for a new row to be added to centeredQ
        private final byte[] rowBytes;
        // scratch array for storing element distances in addRow */
        private final float[] diff;
        // result array of the int matrix multiply
        private final MemorySegment product;
        private final int[] productBuffer;

        private Int8BlockEncoder(float[] w, int originalDim, int maxBlockSize) {
            super(w, originalDim, maxBlockSize);

            // record the scale for each column of W
            this.wScales = new float[nDims];
            for (int l = 0; l < originalDim; l++) {
                for (int j = 0; j < nDims; j++) {
                    wScales[j] = Math.max(wScales[j], Math.abs(w[l * nDims + j]));
                }
            }
            for (int j = 0; j < nDims; j++) {
                wScales[j] /= INT8_MAX;
            }

            // byte version of W, scaled per-column
            byte[] wqBytes = new byte[w.length];
            for (int l = 0; l < originalDim; l++) {
                for (int j = 0; j < nDims; j++) {
                    wqBytes[l * nDims + j] = toInt8(w[l * nDims + j], wScales[j] == 0 ? 0 : 1 / wScales[j]);
                }
            }
            this.wq = arena.allocate(w.length);
            MemorySegment.copy(wqBytes, 0, wq, ValueLayout.JAVA_BYTE, 0, w.length);

            this.centeredQ = arena.allocate((long) maxBlockSize * originalDim);
            this.rowBytes = new byte[originalDim];
            this.diff = new float[originalDim];
            this.product = arena.allocate((long) maxBlockSize * nDims * Integer.BYTES, Integer.BYTES);
            this.productBuffer = new int[nDims];
        }

        @Override
        protected float addRow(float[] vector, float[] centroid, int row) {
            float max = 0;
            float dot = 0;
            for (int d = 0; d < originalDim; d++) {
                float v = vector[d] - centroid[d];
                diff[d] = v;
                dot = Math.fma(v, v, dot);
                max = Math.max(max, Math.abs(v));
            }
            float inverseScale = max == 0 ? 0 : INT8_MAX / max;

            for (int d = 0; d < originalDim; d++) {
                rowBytes[d] = toInt8(diff[d], inverseScale);
            }
            MemorySegment.copy(rowBytes, 0, centeredQ, ValueLayout.JAVA_BYTE, (long) row * originalDim, originalDim);

            return dot;
        }

        @Override
        protected void project() {
            // As in FloatBlockEncoder, rows past 'size' hold stale data and are never read back
            ESVectorUtil.matrixMultiplyI8(centeredQ, wq, maxBlockSize, originalDim, nDims, product, executor);

            // scale the multiply results back to floats
            // copy to an array, then iterate through the array for speed
            for (int i = 0; i < size; i++) {
                int base = i * nDims;
                MemorySegment.copy(product, ValueLayout.JAVA_INT, (long) base * Integer.BYTES, productBuffer, 0, nDims);
                for (int j = 0; j < nDims; j++) {
                    latent[base + j] = productBuffer[j] * wScales[j];
                }
            }
        }

        @Override
        public void close() {
            arena.close();
        }
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
     * Runs the iterative Procrustes refinement starting from a given orthonormal basis {@code p}
     * (shape {@code (originalDim, nDims)}), returning the learned projection matrix W = P @ R.
     * <p>
     * Used by {@link #train} when {@code nTrainingIterations} is positive, with the PCA subspace as the basis.
     */
    private float[] learnedTrainingFromBasis(float[] xTraining, float[] p, int nTraining, int originalDim, int nDims, int nIterations) {
        // Project training data: X_ld = xTraining @ P (nTraining x nDims)
        float[] xLd = ESVectorUtil.matrixMultiply(xTraining, p, nTraining, originalDim, nDims, executor);

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
            AshUtils.procrustes(m, nDims, r, executor);

            if (epoch < nIterations) {
                // X_transformed = X_ld @ R (nTraining x nDims)
                ESVectorUtil.matrixMultiply(xLd, r, nTraining, nDims, nDims, xTransformed, executor);
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
                ESVectorUtil.matrixMultiply(xLdT, xEnc, nDims, nTraining, nDims, m, executor);
            }
        }

        // W = P @ R (originalDim x nDims)
        return ESVectorUtil.matrixMultiply(p, r, originalDim, nDims, nDims, executor);
    }

    private float[] randomOrthogonal(int originalDim, int nDims) {
        // orthonormalizeRows orthonormalizes rows, so it produces W^T (nDims x originalDim)
        float[] qT = AshUtils.randomGaussians(new Random(seed), originalDim * nDims);
        AshUtils.orthonormalizeRows(qT, originalDim, nDims, executor);
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
