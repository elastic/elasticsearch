/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric;

import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;

/**
 * Read-path coordinator for pipeline-based numeric decoding. This is the
 * entry point used by the doc values producer to decode numeric fields. It
 * owns a {@link NumericDecodePipeline} and produces {@link NumericBlockDecoder}
 * instances for per-block decoding.
 *
 * <p>Instances are immutable and thread-safe. Per-field mutable state lives in
 * {@link NumericBlockDecoder}, which callers obtain via {@link #newBlockDecoder()}.
 *
 * <p>Created via {@link #fromDescriptor} or via {@link NumericCodecFactory#createDecoder}.
 */
public final class NumericDecoder {

    private final NumericDecodePipeline pipeline;

    NumericDecoder(final NumericDecodePipeline pipeline) {
        this.pipeline = pipeline;
    }

    /**
     * Reconstructs a decoder from a persisted descriptor.
     * Use {@link NumericCodecFactory#createDecoder} as the public entry point.
     *
     * @param descriptor the pipeline descriptor read from segment metadata
     * @return the decoder
     */
    static NumericDecoder fromDescriptor(final PipelineDescriptor descriptor) {
        return new NumericDecoder(NumericDecodePipeline.fromDescriptor(descriptor));
    }

    /**
     * Creates a new block decoder with its own mutable decoding context.
     *
     * @return a fresh block decoder
     */
    public NumericBlockDecoder newBlockDecoder() {
        return new NumericBlockDecoder(pipeline);
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
