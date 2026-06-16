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
import org.elasticsearch.benchmark.index.codec.tsdb.internal.BoundaryBlockSupplier;
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
 * Dedicated encode benchmark for the {@code SplitDelta} stage.
 *
 * <p>Isolates {@code SplitDelta} CPU across the apply path ({@code k=1..kMax}) and the
 * two decline paths ({@code k=0}, the fully descending case, and {@code k>kMax}, the
 * over-cap case). The pipeline is {@code splitDelta>bitpack} so the throughput score
 * reflects the stage's contribution plus the unavoidable bit-pack tail.
 *
 * <h2>Parameters</h2>
 * <ul>
 *   <li>{@code flips} - number of upward boundary jumps in the block
 *     ({@code 1, 2, 4, 8, 16}), exercising the apply path up to {@code kMax=16}</li>
 *   <li>{@code blockSize} - block size in values ({@code 128, 512, 1024, 2048})</li>
 *   <li>{@code jitterMs} - per-sample timestamp jitter ({@code 0} for deterministic,
 *     {@code 1000} for realistic telemetry)</li>
 * </ul>
 *
 * <p>All {@code flips} values exercise the apply path: {@code SplitDelta} detects flips,
 * splits the block, delta encodes each sub run independently, and stores metadata. This
 * is the production hot path for boundary blocks in TSDB {@code @timestamp}.
 *
 * <h2>Ready to run commands</h2>
 *
 * <pre>{@code
 * # Full matrix
 * ./gradlew :benchmarks:run --args="EncodeSplitDeltaBenchmark"
 *
 * # Production block size only
 * ./gradlew :benchmarks:run --args="EncodeSplitDeltaBenchmark -p blockSize=512"
 *
 * # Realistic jittered input across all block sizes
 * ./gradlew :benchmarks:run --args="EncodeSplitDeltaBenchmark -p jitterMs=1000"
 *
 * # Allocation rate
 * ./gradlew :benchmarks:run --args="EncodeSplitDeltaBenchmark -p blockSize=512 -p flips=1 -prof gc"
 *
 * # Quick smoke
 * ./gradlew :benchmarks:run --args="EncodeSplitDeltaBenchmark -wi 1 -i 1 -f 1 -w 1 -r 1 -p blockSize=512 -p flips=1 -p jitterMs=0"
 * }</pre>
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class EncodeSplitDeltaBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final int SEED = 17;
    private static final int EXTRA_METADATA_SIZE = 512;
    private static final long BASE_TIMESTAMP = 1_700_000_000_000L;
    private static final long INTERVAL_MS = 10_000L;
    private static final long BOUNDARY_JUMP_MS = 240L * 60L * 1000L;

    @Param({ "1", "2", "4", "8", "16" })
    private int flips;

    @Param({ "128", "512", "1024", "2048" })
    private int blockSize;

    @Param({ "0", "1000" })
    private long jitterMs;

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
        final BoundaryBlockSupplier supplier = BoundaryBlockSupplier.builder(SEED, blockSize)
            .withFlips(flips)
            .withBaseTimestamp(BASE_TIMESTAMP)
            .withIntervalMs(INTERVAL_MS)
            .withBoundaryJumpMs(BOUNDARY_JUMP_MS)
            .withJitterMs(jitterMs)
            .build();
        template = supplier.get();
        nominalBitsPerValue = supplier.getNominalBitsPerValue();

        inputs = new long[blocksPerInvocation][blockSize];
        outputBuffers = new byte[blocksPerInvocation][Long.BYTES * blockSize + EXTRA_METADATA_SIZE];
        outputs = new ByteArrayDataOutput[blocksPerInvocation];
        for (int i = 0; i < blocksPerInvocation; i++) {
            outputs[i] = new ByteArrayDataOutput(outputBuffers[i]);
        }

        final PipelineConfig config = PipelineConfig.forLongs(blockSize).splitDelta().bitPack();
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
