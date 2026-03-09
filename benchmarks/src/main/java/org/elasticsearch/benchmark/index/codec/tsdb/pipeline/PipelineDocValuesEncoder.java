/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.pipeline;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;

import java.io.Closeable;
import java.io.IOException;

public final class PipelineDocValuesEncoder implements Closeable {

    private final NumericEncoder numericEncoder;
    private final NumericDecoder numericDecoder;
    private final NumericBlockEncoder encoder;
    private final NumericBlockDecoder decoder;

    public PipelineDocValuesEncoder(final PipelineConfig config) {
        this.numericEncoder = NumericEncoder.fromConfig(config);
        this.numericDecoder = NumericDecoder.fromDescriptor(numericEncoder.descriptor());
        this.encoder = numericEncoder.newBlockEncoder();
        this.decoder = numericDecoder.newBlockDecoder();
    }

    public PipelineDocValuesEncoder(int blockSize) {
        this(PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack());
    }

    public int getBlockSize() {
        return numericEncoder.blockSize();
    }

    public void encode(final long[] in, final DataOutput out) throws IOException {
        encoder.encode(in, in.length, out);
    }

    public void decode(final DataInput in, final long[] out) throws IOException {
        decoder.decode(out, in);
    }

    @Override
    public void close() {}
}
