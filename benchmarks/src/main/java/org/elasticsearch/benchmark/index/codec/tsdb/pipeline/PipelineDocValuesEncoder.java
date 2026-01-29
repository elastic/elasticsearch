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
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodec;

import java.io.Closeable;
import java.io.IOException;

public final class PipelineDocValuesEncoder implements Closeable {

    private final NumericCodec codec;
    private final NumericBlockEncoder encoder;
    private final NumericBlockDecoder decoder;

    public static NumericCodec createES87CompatibleCodec(int blockSize) {
        return NumericCodec.withBlockSize(blockSize).delta().offset().gcd().bitPack().build();
    }

    public PipelineDocValuesEncoder(NumericCodec codec) {
        this.codec = codec;
        this.encoder = codec.newEncoder();
        this.decoder = codec.newDecoder();
    }

    public PipelineDocValuesEncoder(int blockSize) {
        this(createES87CompatibleCodec(blockSize));
    }

    public int getBlockSize() {
        return codec.blockSize();
    }

    public void encode(long[] in, DataOutput out) throws IOException {
        encoder.encode(in, in.length, out);
    }

    public void decode(DataInput in, long[] out) throws IOException {
        decoder.decode(out, in);
    }

    @Override
    public void close() throws IOException {
        codec.close();
    }
}
