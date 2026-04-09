/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.internal;

import org.apache.lucene.store.ByteArrayDataOutput;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Encoding benchmark for TSDB doc values.
 *
 * <p>Measures the performance of {@link org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder#encode},
 * which compresses a block of long values into a byte buffer.
 *
 * @see DecodeBenchmark
 */
public final class EncodeBenchmark extends AbstractTSDBCodecBenchmark {

    private ByteArrayDataOutput[] dataOutputs;
    private long[] originalInput;
    private long[][] inputs;
    private byte[][] outputs;

    private int blocksPerInvocation;

    /**
     * Sum of encoded sizes for the last benchmark invocation.
     * Used for blackhole consumption and for computing aggregate throughput.
     */
    private int lastEncodedBytesSum;

    @Override
    public void setupTrial(Supplier<long[]> arraySupplier) throws IOException {
        this.originalInput = arraySupplier.get();
    }

    /**
     * Configures how many blocks are encoded in each measured benchmark invocation.
     *
     * <p>This is intentionally not a compile-time constant: it is controlled by the JMH harness
     * and may be tuned without changing benchmark logic.
     */
    @Override
    public void setBlocksPerInvocation(int blocksPerInvocation) {
        if (this.blocksPerInvocation == blocksPerInvocation) {
            return; // already configured, skip reallocation
        }
        this.blocksPerInvocation = blocksPerInvocation;

        this.inputs = new long[blocksPerInvocation][originalInput.length];
        this.outputs = new byte[blocksPerInvocation][Long.BYTES * blockSize + EXTRA_METADATA_SIZE];
        this.dataOutputs = new ByteArrayDataOutput[blocksPerInvocation];

        for (int i = 0; i < blocksPerInvocation; i++) {
            this.dataOutputs[i] = new ByteArrayDataOutput(this.outputs[i]);
        }
    }

    @Override
    public void run() throws IOException {
        int sum = 0;
        for (int i = 0; i < blocksPerInvocation; i++) {
            System.arraycopy(originalInput, 0, inputs[i], 0, originalInput.length);
            dataOutputs[i].reset(outputs[i]);
            encoder.encode(inputs[i], dataOutputs[i]);
            sum += dataOutputs[i].getPosition(); // sum, not xor: identical sizes would xor to zero
        }
        lastEncodedBytesSum = sum;
    }

    @Override
    protected Object getOutput() {
        return lastEncodedBytesSum;
    }

    @Override
    public int getEncodedSize() {
        return dataOutputs[0].getPosition();
    }

    public int getBlocksPerInvocation() {
        return blocksPerInvocation;
    }
}
