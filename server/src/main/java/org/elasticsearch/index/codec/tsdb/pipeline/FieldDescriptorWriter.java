/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.lucene.store.DataOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;

import java.io.IOException;

public final class FieldDescriptorWriter {

    private final DataOutput meta;
    private final int blockSize;
    private final NumericEncoder encoder;

    private NumericBlockEncoder delegate;
    private PipelineDescriptor descriptor;

    public FieldDescriptorWriter(final DataOutput meta, final PipelineConfig config, int blockSize) {
        this.meta = meta;
        this.blockSize = blockSize;
        this.encoder = NumericEncoder.fromConfig(config);
    }

    public void encode(final long[] values, int valueCount, final DataOutput data) throws IOException {
        if (delegate == null) {
            descriptor = encoder.descriptor();
            FieldDescriptor.write(meta, descriptor);
            delegate = encoder.newBlockEncoder();
        }
        delegate.encode(values, valueCount, data);
    }

    public int blockSize() {
        return blockSize;
    }

    public PipelineDescriptor descriptor() {
        return descriptor;
    }
}
