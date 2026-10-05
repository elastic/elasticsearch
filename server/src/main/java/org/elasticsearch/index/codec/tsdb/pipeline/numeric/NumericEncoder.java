/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric;

import org.elasticsearch.index.codec.tsdb.pipeline.FieldDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;

/**
 * Write-path coordinator for pipeline-based numeric encoding. Bound to a single
 * {@link NumericEncodePipeline} for its lifetime; callers that need per-field
 * pipeline selection should build one instance per field via
 * {@link NumericCodecFactory#createEncoder(PipelineConfig)} after resolving the
 * config.
 *
 * <p>Both {@link #newBlockEncoder()} and {@link #descriptor()} read from the
 * same {@code pipeline} field, so a single instance covers both outputs without
 * re-resolving anything.
 *
 * <p>Instances are immutable and thread-safe. Per-field mutable state lives in
 * {@link NumericBlockEncoder}.
 */
public final class NumericEncoder {

    private final NumericEncodePipeline pipeline;

    NumericEncoder(final NumericEncodePipeline pipeline) {
        this.pipeline = pipeline;
    }

    /**
     * Builds an encoder bound to a single pipeline configuration.
     * Use {@link NumericCodecFactory#createEncoder} as the public entry point.
     *
     * @param config the pipeline configuration
     * @return the encoder
     */
    static NumericEncoder fromConfig(final PipelineConfig config) {
        return new NumericEncoder(NumericEncodePipeline.fromConfig(config));
    }

    /**
     * Creates a new block encoder with its own mutable encoding context.
     *
     * @return a fresh block encoder
     */
    public NumericBlockEncoder newBlockEncoder() {
        return new NumericBlockEncoder(pipeline);
    }

    /**
     * Returns the pipeline descriptor for persistence via {@link FieldDescriptor}.
     *
     * @return the pipeline descriptor
     */
    public PipelineDescriptor descriptor() {
        return pipeline.descriptor();
    }

    /**
     * Returns the number of values per block.
     *
     * @return the number of values per block
     */
    public int blockSize() {
        return pipeline.blockSize();
    }
}
