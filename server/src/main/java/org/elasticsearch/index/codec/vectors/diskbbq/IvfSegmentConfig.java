/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.elasticsearch.core.Nullable;

import java.util.Set;

/**
 * Per-segment (per-field) IVF configuration persisted in {@code mivf}. It has four parts:
 * <ul>
 *     <li>{@link #centroidIndexFormat()} specifying the centroid indexing format</li>
 *     <li>{@link #quantConfig()} for the quantization strategy (OSQ or ASH)</li>
 *     <li>{@link #usePrecondition()} for whether a preconditioner is written and used on flush/merge and on the reader</li>
 *     <li>{@link #rescoreOversample()} for kNN rescore candidate expansion, read with query</li>
 * </ul>
 * The effective config from flush/merge is written to stay consistent with the quantization and
 * preconditioning data stored for the segment.
 * Search-time scoring for quant and preconditioning continues to follow the on-disk {@code mivf} and
 * reader.
 * When the stored rescore is not finite (e.g. {@code NaN}), query and mapping rescore then apply in the usual order.
 */
public record IvfSegmentConfig(
    CentroidIndexFormat centroidIndexFormat,
    QuantConfig quantConfig,
    boolean usePrecondition,
    float rescoreOversample
) {

    /**
     * Discriminated quantization configuration for an IVF segment.
     * Each IVF segment uses either OSQ (Optimal Scalar Quantization) or ASH (Asymmetric Scalar Hashing).
     */
    public sealed interface QuantConfig permits OsqConfig, AshConfig {}

    /**
     * OSQ (Optimal Scalar Quantization) configuration — used by BBQ writers/readers.
     * Wraps the existing {@link QuantEncoding} which provides packing, dimension math, and bit-width metadata.
     */
    public record OsqConfig(QuantEncoding encoding) implements QuantConfig {}

    /**
     * ASH (Asymmetric Scalar Hashing) configuration — used by ASH writers/readers.
     * ASH handles its own packing via {@code ESVectorUtil.ashPack} and does not use {@link QuantEncoding}.
     *
     * <p>Mirrors how {@link OsqConfig} wraps {@link QuantEncoding} with both document and query bit widths:
     * {@code bitsPerDim} controls document encoding while {@code queryBitsPerDim} controls query-time
     * quantization precision. The writer uses document-side parameters for indexing; the reader uses
     * {@code queryBitsPerDim} for scoring. Set {@code queryBitsPerDim} to 0 for the float scoring path.
     *
     * <p>Use {@link #of(int, int, float, int)} to construct with validation, analogous to
     * {@link QuantEncoding#fromDocAndQueryBits(byte, byte)} for OSQ.
     */
    public record AshConfig(
        float projectedDimsFraction,
        int maxProjectedDims,
        int bitsPerDim,
        int queryBitsPerDim,
        int trainingIterations,
        int trainingFactor
    ) implements QuantConfig {
        private static final Set<Integer> SUPPORTED_BITS_PER_DIM = Set.of(1, 2, 3, 4, 8);

        public static final float DEFAULT_PROJECTED_DIMS_FRACTION = 0.5f;
        public static final int DEFAULT_MAX_PROJECTED_DIMS = 1024;
        public static final int DEFAULT_BITS_PER_DIM = 2;
        public static final int DEFAULT_QUERY_BITS_PER_DIM = 4;
        /**
         * Procrustes iterations used to refine the rotation applied to the PCA subspace. With zero, the default, the
         * rotation is a random orthogonal matrix and no projected training sample is needed; any positive value runs
         * that many iterations on a projected training sample.
         */
        public static final int DEFAULT_TRAINING_ITERATIONS = 0;
        public static final int DEFAULT_TRAINING_FACTOR = 10;

        /**
         * Vector dimension below which {@link #forDims} does not reduce the dimension at all and encodes each dimension
         * with {@link #LOW_DIMS_BITS_PER_DIM} bit. There is little to gain from projecting such small vectors.
         */
        public static final int LOW_DIMS_THRESHOLD = 512;
        public static final int LOW_DIMS_BITS_PER_DIM = 1;

        /**
         * Vector dimension at and above which {@link #forDims} projects more aggressively and spends more bits on each
         * projected dimension. At these sizes building and applying the projection dominates indexing cost, and
         * {@link #HIGH_DIMS_BITS_PER_DIM} bits over {@link #HIGH_DIMS_PROJECTED_DIMS_FRACTION} of the dimensions keeps
         * the per-vector size of 1-bit OSQ while projecting to fewer dimensions.
         */
        public static final int HIGH_DIMS_THRESHOLD = 2048;
        public static final int HIGH_DIMS_BITS_PER_DIM = 4;
        public static final float HIGH_DIMS_PROJECTED_DIMS_FRACTION = 0.25f;

        /**
         * {@link #forDims} rounds the number of projected dimensions to a multiple of this, so that the packed codes
         * and the query projection stay aligned to SIMD-friendly widths.
         */
        public static final int PROJECTED_DIMS_ALIGNMENT = 64;

        /**
         * Training-sample factor used on the flush path. Training streams the sample through a
         * {@code originalDim x originalDim} second-moment matrix, so its memory no longer grows with the
         * sample size and flush can use the same factor as merge.
         */
        public static final int DEFAULT_FLUSH_TRAINING_FACTOR = 10;

        /**
         * Returns the training-sample factor to use when learning W at flush time. Merge uses
         * {@link #trainingFactor()}.
         */
        public int flushTrainingFactor() {
            return DEFAULT_FLUSH_TRAINING_FACTOR;
        }

        /**
         * Returns {@code true} if the given bits-per-dimension value is supported for ASH document encoding.
         */
        public static boolean isValidBitsPerDim(int bits) {
            return SUPPORTED_BITS_PER_DIM.contains(bits);
        }

        /**
         * Validates that the given bits-per-dimension value is supported for ASH document encoding.
         * @throws IllegalArgumentException if {@code bits} is not a supported value
         */
        public static void validateBitsPerDim(int bits) {
            if (isValidBitsPerDim(bits) == false) {
                throw new IllegalArgumentException("ASH bitsPerDim must be one of " + SUPPORTED_BITS_PER_DIM + ", got: " + bits);
            }
        }

        /**
         * Validates ASH parameters and returns an {@code AshConfig} with default training settings.
         * Analogous to {@link QuantEncoding#fromDocAndQueryBits(byte, byte)} for OSQ — callers get
         * validated configuration from a single factory call.
         *
         * @param bitsPerDim document encoding bits per projected dimension
         * @param queryBitsPerDim query quantization bits (0 for float scoring path)
         * @param projectedDimsFraction fraction of original dimensions to project to, in (0, 1]
         * @param maxProjectedDims maximum projected dims to use
         * @throws IllegalArgumentException if any parameter is out of range
         */
        public static AshConfig of(int bitsPerDim, int queryBitsPerDim, float projectedDimsFraction, int maxProjectedDims) {
            validateBitsPerDim(bitsPerDim);
            if (queryBitsPerDim < 0 || queryBitsPerDim > 8) {
                throw new IllegalArgumentException(
                    "ASH queryBitsPerDim must be between 0 and 8 (0 for float scoring path), got: " + queryBitsPerDim
                );
            }
            if (projectedDimsFraction <= 0 || projectedDimsFraction > 1.0f) {
                throw new IllegalArgumentException("ASH projectedDimsFraction must be in (0, 1], got: " + projectedDimsFraction);
            }
            return new AshConfig(
                projectedDimsFraction,
                maxProjectedDims,
                bitsPerDim,
                queryBitsPerDim,
                DEFAULT_TRAINING_ITERATIONS,
                DEFAULT_TRAINING_FACTOR
            );
        }

        /**
         * Returns the ASH configuration for a field with {@code dims} dimensions, deriving whatever the user left unset
         * from the dimension. Each of the three size tiers spends about one bit per original dimension:
         * <ul>
         *     <li>below {@link #LOW_DIMS_THRESHOLD}: {@link #LOW_DIMS_BITS_PER_DIM} bit, no reduction</li>
         *     <li>up to {@link #HIGH_DIMS_THRESHOLD}: {@link #DEFAULT_BITS_PER_DIM} bits over
         *     {@link #DEFAULT_PROJECTED_DIMS_FRACTION} of the dimensions</li>
         *     <li>from {@link #HIGH_DIMS_THRESHOLD}: {@link #HIGH_DIMS_BITS_PER_DIM} bits over
         *     {@link #HIGH_DIMS_PROJECTED_DIMS_FRACTION} of the dimensions</li>
         * </ul>
         * The number of projected dimensions always follows from {@code dims} alone, so that changing {@code bitsPerDim}
         * on a field never changes the shape of its projection.
         *
         * @param dims the vector dimension of the field
         * @param bitsPerDim the bits per projected dimension the user configured, or {@code null} to derive them
         * @param queryBitsPerDim query quantization bits (0 for the float scoring path)
         */
        public static AshConfig forDims(int dims, @Nullable Integer bitsPerDim, int queryBitsPerDim) {
            if (dims <= 0) {
                throw new IllegalArgumentException("ASH dims must be > 0, got: " + dims);
            }
            int bits = bitsPerDim != null ? bitsPerDim : defaultBitsPerDim(dims);
            // The projected dims are fixed exactly, so express them through maxProjectedDims with nothing left to
            // the fraction.
            return of(bits, queryBitsPerDim, 1.0f, defaultProjectedDims(dims));
        }

        /** Returns the bits per projected dimension {@link #forDims} uses when the user did not configure them. */
        public static int defaultBitsPerDim(int dims) {
            if (dims < LOW_DIMS_THRESHOLD) {
                return LOW_DIMS_BITS_PER_DIM;
            }
            return dims >= HIGH_DIMS_THRESHOLD ? HIGH_DIMS_BITS_PER_DIM : DEFAULT_BITS_PER_DIM;
        }

        /**
         * Returns the number of projected dimensions {@link #forDims} uses: all of {@code dims} below
         * {@link #LOW_DIMS_THRESHOLD}, otherwise the default fraction of {@code dims} for its size, capped at
         * {@link #DEFAULT_MAX_PROJECTED_DIMS} and rounded to the nearest multiple of {@link #PROJECTED_DIMS_ALIGNMENT}.
         */
        static int defaultProjectedDims(int dims) {
            if (dims < LOW_DIMS_THRESHOLD) {
                return dims;
            }
            float fraction = dims >= HIGH_DIMS_THRESHOLD ? HIGH_DIMS_PROJECTED_DIMS_FRACTION : DEFAULT_PROJECTED_DIMS_FRACTION;
            int target = Math.min(DEFAULT_MAX_PROJECTED_DIMS, (int) (dims * fraction));
            int aligned = Math.max(
                PROJECTED_DIMS_ALIGNMENT,
                Math.round((float) target / PROJECTED_DIMS_ALIGNMENT) * PROJECTED_DIMS_ALIGNMENT
            );
            return Math.min(aligned, dims);
        }

        /**
         * Returns the number of dimensions vectors of {@code originalDim} dimensions are projected to under this
         * configuration.
         */
        public int projectedDims(int originalDim) {
            return Math.min(maxProjectedDims, (int) (originalDim * projectedDimsFraction));
        }

        /** Returns an AshConfig with all default values. */
        public static AshConfig defaults() {
            return new AshConfig(
                DEFAULT_PROJECTED_DIMS_FRACTION,
                DEFAULT_MAX_PROJECTED_DIMS,
                DEFAULT_BITS_PER_DIM,
                DEFAULT_QUERY_BITS_PER_DIM,
                DEFAULT_TRAINING_ITERATIONS,
                DEFAULT_TRAINING_FACTOR
            );
        }
    }

    public static final IvfSegmentConfig NONE = new IvfSegmentConfig(
        CentroidIndexFormat.FLAT,
        new OsqConfig(QuantEncoding.ONE_BIT_4BIT_QUERY),
        false,
        Float.NaN
    );

    public static IvfSegmentConfig fromCodecDefaults(
        CentroidIndexFormat centroidIndexFormat,
        QuantConfig quantConfig,
        boolean doPrecondition
    ) {
        return new IvfSegmentConfig(centroidIndexFormat, quantConfig, doPrecondition, Float.NaN);
    }

    public static IvfSegmentConfig of(
        CentroidIndexFormat centroidIndexFormat,
        QuantConfig quantConfig,
        boolean usePrecondition,
        float rescoreOversample
    ) {
        return new IvfSegmentConfig(centroidIndexFormat, quantConfig, usePrecondition, rescoreOversample);
    }

    /**
     * Returns the {@link QuantEncoding} from an {@link OsqConfig}.
     * @throws ClassCastException if this config uses a different quantization strategy
     */
    public QuantEncoding osqEncoding() {
        return ((OsqConfig) quantConfig).encoding();
    }

    /**
     * Returns the {@link AshConfig} from this segment config.
     * @throws ClassCastException if this config uses a different quantization strategy
     */
    public AshConfig ashConfig() {
        return (AshConfig) quantConfig;
    }

    /**
     * Resolves oversample for search: query override, else finite persisted value, else mapping default.
     */
    public static float effectiveRescoreOversample(float persisted, Float queryOverride, float mappingDefault) {
        if (queryOverride != null) {
            return queryOverride;
        }
        if (Float.isFinite(persisted)) {
            return persisted;
        }
        return mappingDefault;
    }

    /**
     * Returns a copy of {@code raw} with {@link #rescoreOversample()} set to the effective value.
     */
    public static IvfSegmentConfig withEffectiveRescoreOversample(IvfSegmentConfig raw, Float queryOverride, float mappingDefault) {
        float effective = effectiveRescoreOversample(raw.rescoreOversample(), queryOverride, mappingDefault);
        return new IvfSegmentConfig(raw.centroidIndexFormat(), raw.quantConfig(), raw.usePrecondition(), effective);
    }

    /** Per-leaf IVF collector size (includes 2x factor for overspill duplicates). */
    public static int leafCollectorBudget(int resultK, float segmentOversample) {
        return Math.round(2f * resultK * Math.max(1, segmentOversample));
    }

    /** Shard-level merge cap across segments after approximate search. */
    public static int shardMergeBudget(int resultK, float maxSegmentOversample) {
        return (int) Math.ceil(resultK * Math.max(1, maxSegmentOversample));
    }
}
