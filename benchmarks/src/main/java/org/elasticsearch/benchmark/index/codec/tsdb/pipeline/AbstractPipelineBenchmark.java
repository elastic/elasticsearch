/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.pipeline;

import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodec;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Base class for pipeline codec encode/decode benchmarks.
 *
 * <p>Supports batching multiple blocks per JMH invocation to reduce harness overhead
 * when measuring fast operations (sub-microsecond). The reset logic is moved into
 * the {@link #run()} method rather than using {@code @Setup(Level.Invocation)}.
 */
public abstract class AbstractPipelineBenchmark {

    protected static final int EXTRA_METADATA_SIZE = 64;

    protected final PipelineDocValuesEncoder encoder;
    protected final int blockSize;

    public AbstractPipelineBenchmark(NumericCodec codec) {
        this.blockSize = codec.blockSize();
        this.encoder = new PipelineDocValuesEncoder(codec);
    }

    public AbstractPipelineBenchmark() {
        this.blockSize = ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE;
        this.encoder = new PipelineDocValuesEncoder(blockSize);
    }

    public abstract void run() throws IOException;

    protected abstract Object getOutput();

    public void benchmark(Blackhole bh) throws IOException {
        run();
        bh.consume(getOutput());
    }

    public abstract void setupTrial(Supplier<long[]> arraySupplier) throws IOException;

    /**
     * Configures how many blocks are processed in each measured benchmark invocation.
     * Called after setupTrial() to allocate batch arrays.
     */
    public abstract void setBlocksPerInvocation(int blocksPerInvocation);

    public void setupIteration() throws IOException {}

    public int getBlockSize() {
        return blockSize;
    }

    public abstract int getEncodedSize();

    public abstract int getBlocksPerInvocation();
}
