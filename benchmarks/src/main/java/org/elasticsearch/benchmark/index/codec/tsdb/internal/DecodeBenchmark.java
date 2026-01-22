/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.internal;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Decoding benchmark for TSDB doc values.
 *
 * <p>Measures the performance of {@link org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder#decode},
 * which decompresses a byte buffer back into a block of long values.
 *
 * <p>During setup, input data is encoded to create a realistic compressed buffer for decoding.
 *
 * @see EncodeBenchmark
 */
public final class DecodeBenchmark extends AbstractTSDBCodecBenchmark {

    private ByteArrayDataInput dataInput;
    private long[] output;
    private byte[] encodedBuffer;

    @Override
    public void setupTrial(Supplier<long[]> arraySupplier) throws IOException {
        this.output = new long[blockSize];
        long[] input = arraySupplier.get();

        byte[] tempBuffer = new byte[Long.BYTES * blockSize + EXTRA_METADATA_SIZE];
        ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(tempBuffer);
        encoder.encode(input, dataOutput);
        int encodedLength = dataOutput.getPosition();

        this.encodedBuffer = new byte[encodedLength];
        System.arraycopy(tempBuffer, 0, encodedBuffer, 0, encodedLength);
        this.dataInput = new ByteArrayDataInput(this.encodedBuffer);
    }

    @Override
    public void setupInvocation() {
        this.dataInput.reset(this.encodedBuffer);
    }

    @Override
    public void run() throws IOException {
        encoder.decode(this.dataInput, this.output);
    }

    @Override
    protected Object getOutput() {
        return this.output;
    }

    @Override
    public int getEncodedSize() {
        return encodedBuffer.length;
    }
}
