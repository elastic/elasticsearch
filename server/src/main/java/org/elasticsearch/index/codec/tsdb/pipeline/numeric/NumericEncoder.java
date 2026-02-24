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
 * Write-path coordinator: owns a {@link NumericEncodePipeline} and produces
 * {@link NumericBlockEncoder} instances for encoding blocks of values.
 * Created from a pipeline configuration; the consumer never constructs a decoder.
 */
public final class NumericEncoder {

    private final NumericEncodePipeline pipeline;

    NumericEncoder(final NumericEncodePipeline pipeline) {
        this.pipeline = pipeline;
    }

    public static NumericEncoder fromConfig(final PipelineConfig config) {
        return new NumericEncoder(NumericEncodePipeline.fromConfig(config));
    }

    public NumericBlockEncoder newBlockEncoder() {
        return new NumericBlockEncoder(pipeline);
    }

    public PipelineDescriptor descriptor() {
        return pipeline.descriptor();
    }

    public int blockSize() {
        return pipeline.blockSize();
    }

}
