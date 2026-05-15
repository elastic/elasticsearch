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
 * A {@link PipelineConfigResolver} that returns the same pipeline for all fields:
 * {@code delta > offset > gcd > bitpack}. This matches the ES819 baseline
 * encoding and is the default for ES95.
 *
 * <p>The two production block sizes ({@code 128} and {@code 512}, see
 * {@code ES95TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT} and
 * {@code NUMERIC_LARGE_BLOCK_SHIFT}) have their {@link PipelineConfig} precomputed
 * at class load so the per-field write path reuses a single instance instead of
 * rebuilding the same builder chain on every call. Unknown block sizes (e.g.
 * those used by unit tests) fall back to a fresh build.
 */
public final class StaticPipelineConfigResolver implements PipelineConfigResolver {

    /** Shared stateless instance; mirrors the {@code INSTANCE} pattern used by stage classes. */
    public static final StaticPipelineConfigResolver INSTANCE = new StaticPipelineConfigResolver();

    private static final PipelineConfig BLOCK_128 = build(128);
    private static final PipelineConfig BLOCK_512 = build(512);

    private StaticPipelineConfigResolver() {}

    @Override
    public PipelineConfig resolve(final FieldContext context) {
        final int blockSize = context.blockSize();
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
}
