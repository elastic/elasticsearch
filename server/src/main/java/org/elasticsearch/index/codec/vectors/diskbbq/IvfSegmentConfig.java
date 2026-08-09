/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

/**
 * Per-segment (per-field) IVF configuration persisted in {@code mivf}. It has four parts:
 * <ul>
 *     <li>{@link #centroidIndexFormat()} specifying the centroid indexing format</li>
 *     <li>{@link #quantEncoding()} for scalar quant used when indexing doc vectors</li>
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
    QuantEncoding quantEncoding,
    boolean usePrecondition,
    float rescoreOversample,
    boolean useAsh,
    float ashProjectedDimsFraction,
    int ashBitsPerDim,
    int ashTrainingIterations,
    int ashTrainingFactor,
    long ashSeed
) {

    // ASH (Asymmetric Scalar Hashing) defaults
    public static final float DEFAULT_ASH_PROJECTED_DIMS_FRACTION = 0.5f;
    public static final int DEFAULT_ASH_BITS_PER_DIM = 2;
    public static final int DEFAULT_ASH_TRAINING_ITERATIONS = 5;
    public static final int DEFAULT_ASH_TRAINING_FACTOR = 10;
    public static final long DEFAULT_ASH_SEED = 42L;

    public static final IvfSegmentConfig NONE = new IvfSegmentConfig(
        CentroidIndexFormat.FLAT,
        QuantEncoding.ONE_BIT_4BIT_QUERY,
        false,
        Float.NaN,
        false,
        DEFAULT_ASH_PROJECTED_DIMS_FRACTION,
        DEFAULT_ASH_BITS_PER_DIM,
        DEFAULT_ASH_TRAINING_ITERATIONS,
        DEFAULT_ASH_TRAINING_FACTOR,
        DEFAULT_ASH_SEED
    );

    public static IvfSegmentConfig fromCodecDefaults(
        CentroidIndexFormat centroidIndexFormat,
        QuantEncoding quantEncoding,
        boolean doPrecondition
    ) {
        return new IvfSegmentConfig(
            centroidIndexFormat,
            quantEncoding,
            doPrecondition,
            Float.NaN,
            false,
            DEFAULT_ASH_PROJECTED_DIMS_FRACTION,
            DEFAULT_ASH_BITS_PER_DIM,
            DEFAULT_ASH_TRAINING_ITERATIONS,
            DEFAULT_ASH_TRAINING_FACTOR,
            DEFAULT_ASH_SEED
        );
    }

    /** Convenience constructor for non-ASH configs (uses default ASH params, disabled). */
    public static IvfSegmentConfig of(
        CentroidIndexFormat centroidIndexFormat,
        QuantEncoding quantEncoding,
        boolean usePrecondition,
        float rescoreOversample
    ) {
        return new IvfSegmentConfig(
            centroidIndexFormat,
            quantEncoding,
            usePrecondition,
            rescoreOversample,
            false,
            DEFAULT_ASH_PROJECTED_DIMS_FRACTION,
            DEFAULT_ASH_BITS_PER_DIM,
            DEFAULT_ASH_TRAINING_ITERATIONS,
            DEFAULT_ASH_TRAINING_FACTOR,
            DEFAULT_ASH_SEED
        );
    }

    public static IvfSegmentConfig fromCodecDefaultsWithAsh(CentroidIndexFormat centroidIndexFormat, QuantEncoding quantEncoding) {
        return fromCodecDefaultsWithAsh(
            centroidIndexFormat,
            quantEncoding,
            DEFAULT_ASH_PROJECTED_DIMS_FRACTION,
            DEFAULT_ASH_BITS_PER_DIM,
            DEFAULT_ASH_TRAINING_ITERATIONS,
            DEFAULT_ASH_TRAINING_FACTOR,
            DEFAULT_ASH_SEED
        );
    }

    public static IvfSegmentConfig fromCodecDefaultsWithAsh(
        CentroidIndexFormat centroidIndexFormat,
        QuantEncoding quantEncoding,
        float ashProjectedDimsFraction,
        int ashBitsPerDim,
        int ashTrainingIterations,
        int ashTrainingFactor,
        long ashSeed
    ) {
        return new IvfSegmentConfig(
            centroidIndexFormat,
            quantEncoding,
            false,
            Float.NaN,
            true,
            ashProjectedDimsFraction,
            ashBitsPerDim,
            ashTrainingIterations,
            ashTrainingFactor,
            ashSeed
        );
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
        return new IvfSegmentConfig(
            raw.centroidIndexFormat(),
            raw.quantEncoding(),
            raw.usePrecondition(),
            effective,
            raw.useAsh(),
            raw.ashProjectedDimsFraction(),
            raw.ashBitsPerDim(),
            raw.ashTrainingIterations(),
            raw.ashTrainingFactor(),
            raw.ashSeed()
        );
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
