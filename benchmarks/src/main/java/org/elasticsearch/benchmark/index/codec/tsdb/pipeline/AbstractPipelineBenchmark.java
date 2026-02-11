/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.pipeline;

import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.function.Supplier;

public abstract class AbstractPipelineBenchmark {

    protected static final int EXTRA_METADATA_SIZE = 20 * 1024;

    protected final PipelineDocValuesEncoder encoder;
    protected final int blockSize;

    public AbstractPipelineBenchmark(PipelineConfig config) {
        this.encoder = new PipelineDocValuesEncoder(config);
        this.blockSize = encoder.getBlockSize();
    }

    public AbstractPipelineBenchmark() {
        this.blockSize = ES819TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE;
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
