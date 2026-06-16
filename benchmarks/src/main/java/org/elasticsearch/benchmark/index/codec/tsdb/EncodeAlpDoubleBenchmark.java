/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.AlpDecimalDoubleSupplier;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.CompressionMetrics;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Dedicated encode benchmark for the ALP double transform stage.
 *
 * <p>Runs an {@code alpDouble > delta > offset > gcd > bitpack} pipeline against decimal double
 * inputs. The {@code scale} and {@code exceptionFraction} parameters span the regimes
 * where ALP either round-trips cleanly, takes the exception path, or falls through to
 * the integer pipeline.
 *
 * <h2>Parameters</h2>
 * <ul>
 *   <li>{@code blockSize} - block size in values ({@code 128, 512, 1024, 2048})</li>
 *   <li>{@code scale} - decimal places ({@code 0, 2, 6})</li>
 *   <li>{@code exceptionFraction} - fraction of irrational outliers
 *     ({@code 0.0, 0.05, 0.5})</li>
 * </ul>
 *
 * <h2>Ready to run commands</h2>
 *
 * <pre>{@code
 * # Full matrix
 * ./gradlew :benchmarks:run --args="EncodeAlpDoubleBenchmark"
 *
 * # Production-leaning slice
 * ./gradlew :benchmarks:run --args="EncodeAlpDoubleBenchmark -p blockSize=512 -p scale=2 -p exceptionFraction=0.0"
 *
 * # Allocation rate
 * ./gradlew :benchmarks:run --args="EncodeAlpDoubleBenchmark -p blockSize=512 -p scale=2 -p exceptionFraction=0.0 -prof gc"
 *
 * # Compression-only run (writes one block and records aux counters)
 * ./gradlew :benchmarks:run --args="EncodeAlpDoubleBenchmark.compression -p blockSize=512 -p scale=2"
 *
 * # Quick smoke
 * ./gradlew :benchmarks:run --args="EncodeAlpDoubleBenchmark -wi 1 -i 1 -f 1 -w 1 -r 1 -p blockSize=512 -p scale=2 -p exceptionFraction=0.0"
 * }</pre>
 */
// Score is the average time to process one @Benchmark invocation, which encodes
// blocksPerInvocation blocks (100 by default). Lower is better. NANOSECONDS keeps
// per-block resolution visible: divide the score by blocksPerInvocation to read
// per-block cost, and again by blockSize for per-value cost.
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class EncodeAlpDoubleBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final int SEED = 17;
    private static final int EXTRA_METADATA_SIZE = 1024;

    @Param({ "128", "512", "1024", "2048" })
    private int blockSize;

    @Param({ "0", "2", "6" })
    private int scale;

    @Param({ "0.0", "0.05", "0.5" })
    private double exceptionFraction;

    @Param({ "100" })
    private int blocksPerInvocation;

    private long[][] inputs;
    private long[] template;
    private byte[][] outputBuffers;
    private ByteArrayDataOutput[] outputs;
    private NumericBlockEncoder blockEncoder;
    private int nominalBitsPerValue;
    private int lastEncodedSize;

    @Setup(Level.Trial)
    public void setupTrial() {
        final AlpDecimalDoubleSupplier supplier = AlpDecimalDoubleSupplier.builder(SEED, blockSize)
            .withScale(scale)
            .withMidpoint(10_000L)
            .withSpread(5_000L)
            .withExceptionFraction(exceptionFraction)
            .build();
        template = supplier.get();
        nominalBitsPerValue = supplier.getNominalBitsPerValue();

        inputs = new long[blocksPerInvocation][blockSize];
        outputBuffers = new byte[blocksPerInvocation][Long.BYTES * blockSize + EXTRA_METADATA_SIZE];
        outputs = new ByteArrayDataOutput[blocksPerInvocation];
        for (int i = 0; i < blocksPerInvocation; i++) {
            outputs[i] = new ByteArrayDataOutput(outputBuffers[i]);
        }

        final PipelineConfig config = PipelineConfig.forDoubles(blockSize).alpDoubleStage().delta().offset().gcd().bitPack();
        final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
        blockEncoder = encoder.newBlockEncoder();
    }

    @Benchmark
    public void encode(final Blackhole bh) throws IOException {
        int position = 0;
        for (int i = 0; i < blocksPerInvocation; i++) {
            System.arraycopy(template, 0, inputs[i], 0, blockSize);
            outputs[i].reset(outputBuffers[i]);
            blockEncoder.encode(inputs[i], blockSize, outputs[i]);
            position = outputs[i].getPosition();
            bh.consume(position);
        }
        lastEncodedSize = position;
    }

    /**
     * Reports compression metrics (encoded size, compression ratio, bits per value).
     *
     * <p>This benchmark exists only to collect compression statistics via {@link CompressionMetrics}.
     * The reported time is not meaningful and should be ignored. Metrics are per block.
     */
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 0)
    @Measurement(iterations = 1)
    public void compression(final Blackhole bh, final CompressionMetrics metrics) throws IOException {
        encode(bh);
        metrics.recordOperation(blockSize, lastEncodedSize, nominalBitsPerValue);
    }
}
