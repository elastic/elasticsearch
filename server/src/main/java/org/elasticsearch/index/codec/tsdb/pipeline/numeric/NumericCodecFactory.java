/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric;

import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;

/**
 * Factory for creating encoder/decoder instances.
 *
 * <p>The two factory methods take different inputs because they serve different
 * paths: {@link #createEncoder} takes a {@link PipelineConfig} (the fluent
 * builder API used at index time to specify which stages to apply), while
 * {@link #createDecoder} takes a {@link PipelineDescriptor} (the compact
 * byte-level representation read from segment metadata at search time). The
 * encoder writes the descriptor; the decoder reads it back.
 */
public interface NumericCodecFactory {

    /** Default factory that delegates to {@link NumericEncoder#fromConfig} and {@link NumericDecoder#fromDescriptor}. */
    NumericCodecFactory DEFAULT = new NumericCodecFactory() {
        @Override
        public NumericEncoder createEncoder(PipelineConfig config) {
            return NumericEncoder.fromConfig(config);
        }

        @Override
        public NumericDecoder createDecoder(PipelineDescriptor descriptor) {
            return NumericDecoder.fromDescriptor(descriptor);
        }
    };

    /**
     * Creates an encoder bound to the given pipeline configuration. Callers
     * that need per-field pipeline selection should resolve the config per
     * field and call this once per field.
     *
     * @param config the pipeline configuration specifying which stages to apply
     * @return the encoder for the configured pipeline
     */
    NumericEncoder createEncoder(PipelineConfig config);

    /**
     * Creates a decoder from the given pipeline descriptor.
     *
     * @param descriptor the pipeline descriptor read from segment metadata
     * @return the decoder for the described pipeline
     */
    NumericDecoder createDecoder(PipelineDescriptor descriptor);
}
