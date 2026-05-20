/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

/**
 * A {@link PipelineConfigResolver} that returns a per-field pipeline:
 *
 * <ul>
 *   <li>The {@code @timestamp} field uses {@code splitDelta > delta > offset > gcd > bitPack}
 *       to recover compression on TSDB boundary blocks (where {@code _tsid} transitions
 *       cause {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.DeltaCodecStage}
 *       to decline and bit-pack to use {@code log2(time_range)} bits per value).</li>
 *   <li>All other fields use the ES819 baseline {@code delta > offset > gcd > bitPack}.</li>
 * </ul>
 *
 * <p>The two production block sizes ({@code 128} and {@code 512}, see
 * {@code ES95TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT} and {@code NUMERIC_LARGE_BLOCK_SHIFT})
 * have their {@link PipelineConfig} precomputed at class load for both the baseline and
 * the timestamp variants, so the per-field write path reuses a single instance instead of
 * rebuilding the same builder chain on every call. Unknown block sizes (e.g. those used by
 * unit tests) fall back to a fresh build.
 */
public final class StaticPipelineConfigResolver implements PipelineConfigResolver {

    /** Shared stateless instance; mirrors the {@code INSTANCE} pattern used by stage classes. */
    public static final StaticPipelineConfigResolver INSTANCE = new StaticPipelineConfigResolver();

    // NOTE: matches the literal field name used in IndexSortConfig for the TSDB primary
    // sort. Kept local to avoid pulling cluster.metadata into the codec package.
    private static final String TIMESTAMP_FIELD_NAME = "@timestamp";

    private static final PipelineConfig BLOCK_128 = build(128);
    private static final PipelineConfig BLOCK_512 = build(512);
    private static final PipelineConfig TIMESTAMP_BLOCK_128 = buildTimestamp(128);
    private static final PipelineConfig TIMESTAMP_BLOCK_512 = buildTimestamp(512);

    private StaticPipelineConfigResolver() {}

    @Override
    public PipelineConfig resolve(final FieldContext context) {
        final int blockSize = context.blockSize();
        if (TIMESTAMP_FIELD_NAME.equals(context.fieldName())) {
            if (blockSize == 128) {
                return TIMESTAMP_BLOCK_128;
            }
            if (blockSize == 512) {
                return TIMESTAMP_BLOCK_512;
            }
            return buildTimestamp(blockSize);
        }
        if (blockSize == 128) {
            return BLOCK_128;
        }
        if (blockSize == 512) {
            return BLOCK_512;
        }
        return build(blockSize);
    }

    private static PipelineConfig build(final int blockSize) {
        return PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack();
    }

    private static PipelineConfig buildTimestamp(final int blockSize) {
        return PipelineConfig.forLongs(blockSize).splitDelta().delta().offset().gcd().bitPack();
    }
}
