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

    private ByteArrayDataOutput dataOutput;
    private long[] originalInput;
    private long[] input;
    private byte[] output;

    @Override
    public void setupTrial(Supplier<long[]> arraySupplier) throws IOException {
        this.originalInput = arraySupplier.get();
        this.input = new long[originalInput.length];
        this.output = new byte[Long.BYTES * blockSize + EXTRA_METADATA_SIZE];
        this.dataOutput = new ByteArrayDataOutput(this.output);
    }

    @Override
    public void setupInvocation() {
        System.arraycopy(originalInput, 0, input, 0, originalInput.length);
        dataOutput.reset(this.output);
    }

    @Override
    public void run() throws IOException {
        encoder.encode(this.input, this.dataOutput);
    }

    @Override
    protected Object getOutput() {
        return this.dataOutput;
    }

    @Override
    public int getEncodedSize() {
        return dataOutput.getPosition();
    }
}
