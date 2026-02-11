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

import java.io.Closeable;
import java.io.IOException;

/**
 * Read-path coordinator: owns a {@link NumericDecodePipeline} and produces
 * {@link NumericBlockDecoder} instances for decoding blocks of values.
 * Created from a {@link PipelineDescriptor};
 * the producer never constructs an encoder.
 */
public final class NumericDecoder implements Closeable {

    private final NumericDecodePipeline pipeline;

    NumericDecoder(final NumericDecodePipeline pipeline) {
        this.pipeline = pipeline;
    }

    public static NumericDecoder fromDescriptor(final PipelineDescriptor descriptor) {
        return new NumericDecoder(NumericDecodePipeline.fromDescriptor(descriptor));
    }

    public NumericBlockDecoder newBlockDecoder() {
        return new NumericBlockDecoder(pipeline);
    }

    public int blockSize() {
        return pipeline.blockSize();
    }

    public boolean requiresExplicitClose() {
        return pipeline.requiresExplicitClose();
    }

    @Override
    public void close() throws IOException {
        pipeline.close();
    }
}
