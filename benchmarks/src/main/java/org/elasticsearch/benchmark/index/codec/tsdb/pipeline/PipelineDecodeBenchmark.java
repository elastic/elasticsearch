/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.pipeline;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;

import java.io.IOException;
import java.util.function.Supplier;

public final class PipelineDecodeBenchmark extends AbstractPipelineBenchmark {

    private ByteArrayDataInput[] dataInputs;
    private long[][] outputs;
    private byte[] encodedData;
    private int encodedSize;

    private int blocksPerInvocation;
    private long lastDecodedChecksum;

    public PipelineDecodeBenchmark(PipelineConfig config) {
        super(config);
    }

    public PipelineDecodeBenchmark() {
        super();
    }

    @Override
    public void setupTrial(Supplier<long[]> arraySupplier) throws IOException {
        long[] input = arraySupplier.get();

        byte[] buffer = new byte[Long.BYTES * blockSize + EXTRA_METADATA_SIZE];
        ByteArrayDataOutput tempOutput = new ByteArrayDataOutput(buffer);

        encoder.encode(input, tempOutput);

        this.encodedSize = tempOutput.getPosition();
        this.encodedData = new byte[encodedSize];
        System.arraycopy(buffer, 0, encodedData, 0, encodedSize);
    }

    @Override
    public void setBlocksPerInvocation(int blocksPerInvocation) {
        if (this.blocksPerInvocation == blocksPerInvocation) {
            return;
        }
        this.blocksPerInvocation = blocksPerInvocation;

        this.dataInputs = new ByteArrayDataInput[blocksPerInvocation];
        this.outputs = new long[blocksPerInvocation][blockSize];

        for (int i = 0; i < blocksPerInvocation; i++) {
            this.dataInputs[i] = new ByteArrayDataInput(encodedData);
        }
    }

    @Override
    public void setupIteration() {
        lastDecodedChecksum = 0;
    }

    @Override
    public void run() throws IOException {
        long checksum = 0;
        for (int i = 0; i < blocksPerInvocation; i++) {
            dataInputs[i].reset(encodedData, 0, encodedSize);
            encoder.decode(dataInputs[i], outputs[i]);
            checksum ^= outputs[i][0];
        }
        lastDecodedChecksum = checksum;
    }

    @Override
    protected Object getOutput() {
        return lastDecodedChecksum;
    }

    @Override
    public int getEncodedSize() {
        return encodedSize;
    }

    @Override
    public int getBlocksPerInvocation() {
        return blocksPerInvocation;
    }
}
