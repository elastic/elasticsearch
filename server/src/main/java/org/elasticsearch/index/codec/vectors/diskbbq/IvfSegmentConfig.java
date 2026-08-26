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
     * ASH handles its own packing via {@code AshPackingUtils} and does not use {@link QuantEncoding}.
     */
    public record AshConfig(float projectedDimsFraction, int bitsPerDim, int trainingIterations, int trainingFactor)
        implements
            QuantConfig {
        public static final float DEFAULT_PROJECTED_DIMS_FRACTION = 0.5f;
        public static final int DEFAULT_BITS_PER_DIM = 2;
        public static final int DEFAULT_TRAINING_ITERATIONS = 5;
        public static final int DEFAULT_TRAINING_FACTOR = 10;

        /** Returns an AshConfig with all default values. */
        public static AshConfig defaults() {
            return new AshConfig(
                DEFAULT_PROJECTED_DIMS_FRACTION,
                DEFAULT_BITS_PER_DIM,
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
