/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.pipeline;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodec;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Encoding benchmark for pipeline doc values.
 *
 * <p>Measures the performance of {@link PipelineDocValuesEncoder#encode},
 * which compresses a block of long values into a byte buffer.
 *
 * <p>Supports batching multiple blocks per invocation to reduce JMH harness overhead.
 */
public final class PipelineEncodeBenchmark extends AbstractPipelineBenchmark {

    private ByteArrayDataOutput[] dataOutputs;
    private long[] originalInput;
    private long[][] inputs;
    private byte[][] outputs;

    private int blocksPerInvocation;
    private int lastEncodedSize;

    /**
     * Checksum of encoded sizes for the last benchmark invocation.
     * Accumulates the encoded size from each block to create a data dependency on all iterations,
     * preventing the JIT from optimizing away any encode operations.
     */
    private long lastEncodedChecksum;

    public PipelineEncodeBenchmark(NumericCodec codec) {
        super(codec);
    }

    public PipelineEncodeBenchmark() {
        super();
    }

    @Override
    public void setupTrial(Supplier<long[]> arraySupplier) throws IOException {
        this.originalInput = arraySupplier.get();
    }

    @Override
    public void setBlocksPerInvocation(int blocksPerInvocation) {
        if (this.blocksPerInvocation == blocksPerInvocation) {
            return;
        }
        this.blocksPerInvocation = blocksPerInvocation;

        this.inputs = new long[blocksPerInvocation][blockSize];
        this.outputs = new byte[blocksPerInvocation][Long.BYTES * blockSize + EXTRA_METADATA_SIZE];
        this.dataOutputs = new ByteArrayDataOutput[blocksPerInvocation];

        for (int i = 0; i < blocksPerInvocation; i++) {
            this.dataOutputs[i] = new ByteArrayDataOutput(outputs[i]);
        }
    }

    @Override
    public void setupIteration() {
        lastEncodedChecksum = 0;
    }

    @Override
    public void run() throws IOException {
        long checksum = 0;
        int totalEncodedSize = 0;
        for (int i = 0; i < blocksPerInvocation; i++) {
            System.arraycopy(originalInput, 0, inputs[i], 0, originalInput.length);
            dataOutputs[i].reset(outputs[i]);
            encoder.encode(inputs[i], dataOutputs[i]);
            int size = dataOutputs[i].getPosition();
            totalEncodedSize += size;
            checksum ^= size;
        }
        lastEncodedChecksum = checksum;
        lastEncodedSize = totalEncodedSize / blocksPerInvocation;
    }

    @Override
    protected Object getOutput() {
        return lastEncodedChecksum;
    }

    @Override
    public int getEncodedSize() {
        return lastEncodedSize;
    }

    @Override
    public int getBlocksPerInvocation() {
        return blocksPerInvocation;
    }
}
