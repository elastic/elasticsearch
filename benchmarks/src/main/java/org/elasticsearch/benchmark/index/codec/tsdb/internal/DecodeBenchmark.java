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

    private ByteArrayDataInput[] dataInputs;
    private long[][] outputs;
    private byte[] encodedBuffer;

    private int blocksPerInvocation;

    /**
     * Checksum of decoded values for the last benchmark invocation.
     * Accumulates a value from each decoded block to create a data dependency on all iterations,
     * preventing the JIT from optimizing away any decode operations.
     */
    private long lastDecodedChecksum;

    @Override
    public void setupTrial(Supplier<long[]> arraySupplier) throws IOException {
        long[] input = arraySupplier.get();

        byte[] tempBuffer = new byte[Long.BYTES * blockSize + EXTRA_METADATA_SIZE];
        final ByteArrayDataOutput dataOutput = new ByteArrayDataOutput(tempBuffer);
        encoder.encode(input, dataOutput);
        int encodedLength = dataOutput.getPosition();

        this.encodedBuffer = new byte[encodedLength];
        System.arraycopy(tempBuffer, 0, encodedBuffer, 0, encodedLength);
    }

    /**
     * Configures how many blocks are decoded in each measured benchmark invocation.
     *
     * <p>This is intentionally controlled by the JMH harness and may be tuned without
     * changing benchmark logic.
     */
    @Override
    public void setBlocksPerInvocation(int blocksPerInvocation) {
        if (this.blocksPerInvocation == blocksPerInvocation) {
            return; // already configured, skip reallocation
        }
        this.blocksPerInvocation = blocksPerInvocation;

        this.dataInputs = new ByteArrayDataInput[blocksPerInvocation];
        this.outputs = new long[blocksPerInvocation][blockSize];

        for (int i = 0; i < blocksPerInvocation; i++) {
            this.dataInputs[i] = new ByteArrayDataInput(encodedBuffer);
        }
    }

    @Override
    public void run() throws IOException {
        long checksum = 0;
        for (int i = 0; i < blocksPerInvocation; i++) {
            dataInputs[i].reset(encodedBuffer);
            encoder.decode(dataInputs[i], outputs[i]);
            checksum ^= outputs[i][0]; // xor is fast
        }
        lastDecodedChecksum = checksum;
    }

    @Override
    protected Object getOutput() {
        return lastDecodedChecksum;
    }

    @Override
    public int getEncodedSize() {
        return encodedBuffer.length;
    }

    public int getBlocksPerInvocation() {
        return blocksPerInvocation;
    }
}
