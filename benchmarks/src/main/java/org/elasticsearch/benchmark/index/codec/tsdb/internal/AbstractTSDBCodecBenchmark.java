/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.internal;

import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Base class for TSDB codec encode/decode benchmarks using Template Method pattern.
 * Subclasses implement {@link #run()} for the core operation and {@link #getOutput()}
 * for blackhole consumption. The {@link #benchmark(Blackhole)} method is the template
 * that combines both.
 */
public abstract class AbstractTSDBCodecBenchmark {
    protected final TSDBDocValuesEncoder encoder;
    protected final int blockSize;

    public AbstractTSDBCodecBenchmark() {
        this.blockSize = ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE;
        this.encoder = new TSDBDocValuesEncoder(blockSize);
    }

    /**
     * Execute the core encode or decode operation.
     * Can be called independently to measure encoded size before benchmarking.
     */
    public abstract void run() throws IOException;

    /**
     * Return the output to be consumed by the blackhole.
     */
    protected abstract Object getOutput();

    /**
     * Template method: runs the operation and consumes the result.
     */
    public void benchmark(Blackhole bh) throws IOException {
        run();
        bh.consume(getOutput());
    }

    public abstract void setupIteration(Supplier<long[]> arraySupplier) throws IOException;

    public abstract void setupInvocation() throws IOException;

    public int getBlockSize() {
        return blockSize;
    }

    public abstract int getEncodedBytes();
}
