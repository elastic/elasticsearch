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
 * Factory for creating encoder/decoder instances from pipeline configurations.
 * The consumer calls {@link #createEncoder} with a {@link PipelineConfig}.
 * The producer calls {@link #createDecoder} with a {@link PipelineDescriptor}.
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

    /** Creates an encoder from the given pipeline configuration. */
    NumericEncoder createEncoder(PipelineConfig config);

    /** Creates a decoder from the given pipeline descriptor. */
    NumericDecoder createDecoder(PipelineDescriptor descriptor);
}
