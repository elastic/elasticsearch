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
 * Base class for TSDB codec encode/decode benchmarks using the Template Method pattern.
 *
 * <p>This abstract class provides the common structure for benchmarking the
 * {@link TSDBDocValuesEncoder}. It uses the Template Method pattern where:
 * <ul>
 *   <li>{@link #run()} - Subclasses implement the core encode or decode operation</li>
 *   <li>{@link #getOutput()} - Subclasses return the result for blackhole consumption</li>
 *   <li>{@link #benchmark(Blackhole)} - Template method that combines both</li>
 * </ul>
 *
 * @see EncodeBenchmark
 * @see DecodeBenchmark
 */
public abstract class AbstractTSDBCodecBenchmark {

    /**
     * Extra bytes allocated beyond the raw data size to accommodate encoding metadata.
     *
     * <p>The {@link TSDBDocValuesEncoder} writes metadata alongside bit-packed data for each
     * encoding step (delta, offset, GCD). This buffer headroom ensures we never overflow
     * during encoding. The theoretical maximum is ~32 bytes; we use 64 for safety.
     */
    protected static final int EXTRA_METADATA_SIZE = 64;

    /** The encoder instance used for all encode/decode operations. */
    protected final TSDBDocValuesEncoder encoder;

    /** Number of values per block (typically 128). */
    protected final int blockSize;

    /**
     * Creates a new benchmark instance with the standard TSDB block size.
     */
    public AbstractTSDBCodecBenchmark() {
        this.blockSize = ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE;
        this.encoder = new TSDBDocValuesEncoder(blockSize);
    }

    /**
     * Executes the core encode or decode operation.
     *
     * @throws IOException if encoding or decoding fails
     */
    public abstract void run() throws IOException;

    /**
     * Returns the output expected to be consumed by the JMH blackhole.
     *
     * @return the benchmark output (encoded bytes or decoded values)
     */
    protected abstract Object getOutput();

    /**
     * Template method that runs the operation and consumes the result (encode or decode).
     * This is the method called by JMH during benchmark invocations.
     *
     * @param bh the JMH blackhole for consuming results
     * @throws IOException if the operation fails
     */
    public void benchmark(Blackhole bh) throws IOException {
        run();
        bh.consume(getOutput());
    }

    /**
     * Sets up state for a new benchmark trial (once per parameter combination).
     * Called once at the start of each parameter combination to initialize input data.
     *
     * @param arraySupplier supplier that generates the input array for this trial
     * @throws IOException if setup fails
     */
    public abstract void setupTrial(Supplier<long[]> arraySupplier) throws IOException;

    /**
     * Returns the number of values per encoded block.
     *
     * @return the block size.
     */
    public int getBlockSize() {
        return blockSize;
    }

    /**
     * Returns the number of bytes produced by the last encode operation.
     *
     * @return encoded size in bytes
     */
    public abstract int getEncodedSize();

    /**
     * Configures how many blocks are processed in each measured benchmark invocation.
     * Implementations that support batching should override this method.
     *
     * @param blocksPerInvocation the number of blocks to process per invocation
     */
    public void setBlocksPerInvocation(int blocksPerInvocation) {}
}
