/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.index.codec.tsdb.internal;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;

public abstract class DecodeBenchmark extends AbstractDocValuesForUtilBenchmark {
    protected ByteArrayDataInput dataInput;
    protected long[] output;
    protected long[] input;

    @Override
    public void setupInvocation(int bitsPerValue) throws IOException {
        byte[] outputBuffer = new byte[Long.BYTES * blockSize];
        final DataOutput dataOutput = new ByteArrayDataOutput(outputBuffer);
        forUtil.encode(this.input, bitsPerValue, dataOutput);

        this.output = new long[blockSize];
        byte[] inputBuffer = new byte[Long.BYTES * blockSize];
        this.dataInput = new ByteArrayDataInput(inputBuffer);
        System.arraycopy(outputBuffer, 0, inputBuffer, 0, outputBuffer.length);
    }

    @Override
    public void benchmark(int bitsPerValue) throws IOException {
        forUtil.decode(bitsPerValue, this.dataInput, this.output);
    }
}
