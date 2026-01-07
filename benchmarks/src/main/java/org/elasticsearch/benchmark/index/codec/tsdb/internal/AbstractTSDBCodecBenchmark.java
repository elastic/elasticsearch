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
 * <h2>Encoding Pipeline</h2>
 * <p>The {@link TSDBDocValuesEncoder} applies these transformations:
 * <ol>
 *   <li>Delta encoding - computes differences between consecutive values</li>
 *   <li>Offset removal - subtracts minimum value to reduce magnitude</li>
 *   <li>GCD compression - divides by greatest common divisor if beneficial</li>
 *   <li>Bit packing - packs values using minimum required bits</li>
 * </ol>
 *
 * @see EncodeBenchmark
 * @see DecodeBenchmark
 */
public abstract class AbstractTSDBCodecBenchmark {

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
     * This method can be called independently to measure encoded size before benchmarking.
     *
     * @throws IOException if encoding or decoding fails
     */
    public abstract void run() throws IOException;

    /**
     * Returns the output to be consumed by the JMH blackhole.
     * This prevents dead code elimination by the JIT compiler.
     *
     * @return the benchmark output (encoded bytes or decoded values)
     */
    protected abstract Object getOutput();

    /**
     * Template method that runs the operation and consumes the result.
     * This is the method called by JMH during benchmark iterations.
     *
     * @param bh the JMH blackhole for consuming results
     * @throws IOException if the operation fails
     */
    public void benchmark(Blackhole bh) throws IOException {
        run();
        bh.consume(getOutput());
    }

    /**
     * Sets up state for a new benchmark iteration.
     * Called once per iteration to initialize input data.
     *
     * @param arraySupplier supplier that generates the input array for this iteration
     * @throws IOException if setup fails
     */
    public abstract void setupIteration(Supplier<long[]> arraySupplier) throws IOException;

    /**
     * Resets state before each benchmark invocation.
     * Called before every single operation to reset buffers.
     *
     * @throws IOException if reset fails
     */
    public abstract void setupInvocation() throws IOException;

    /**
     * Returns the number of values per encoded block.
     *
     * @return the block size (typically 128)
     */
    public int getBlockSize() {
        return blockSize;
    }

    /**
     * Returns the number of bytes produced by the last encode operation.
     *
     * @return encoded size in bytes
     */
    public abstract int getEncodedBytes();
}
