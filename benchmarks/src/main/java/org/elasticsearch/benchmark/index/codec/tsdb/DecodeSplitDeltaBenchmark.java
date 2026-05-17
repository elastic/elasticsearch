/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.BoundaryBlockSupplier;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericDecoder;
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
 * Dedicated decode benchmark for the {@code SplitDelta} stage.
 *
 * <p>Decode is the read-path hot path: every time series query that reads
 * {@code @timestamp} decodes blocks. This benchmark isolates the inverse
 * {@code SplitDelta} transform (read metadata, prefix-sum each sub run against its
 * anchor) by running a {@code splitDelta>bitpack} pipeline and measuring decode
 * throughput on encoded payloads built during setup.
 *
 * <h2>Parameters</h2>
 * <ul>
 *   <li>{@code flips} - number of upward boundary jumps in the encoded block
 *     ({@code 1, 2, 4, 8, 16}), exercising the apply path</li>
 *   <li>{@code blockSize} - block size in values ({@code 128, 512, 1024, 2048})</li>
 *   <li>{@code jitterMs} - per-sample jitter at encode time ({@code 0} or {@code 1000})</li>
 * </ul>
 *
 * <h2>Ready to run commands</h2>
 *
 * <pre>{@code
 * # Full matrix
 * ./gradlew :benchmarks:run --args="DecodeSplitDeltaBenchmark"
 *
 * # Production block size, all flips
 * ./gradlew :benchmarks:run --args="DecodeSplitDeltaBenchmark -p blockSize=512"
 *
 * # Quick smoke
 * ./gradlew :benchmarks:run --args="DecodeSplitDeltaBenchmark -wi 1 -i 1 -f 1 -w 1 -r 1 -p blockSize=512 -p flips=1 -p jitterMs=0"
 * }</pre>
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class DecodeSplitDeltaBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final int SEED = 17;
    private static final int EXTRA_METADATA_SIZE = 512;

    @Param({ "1", "2", "4", "8", "16" })
    private int flips;

    @Param({ "128", "512", "1024", "2048" })
    private int blockSize;

    @Param({ "0", "1000" })
    private long jitterMs;

    @Param({ "100" })
    private int blocksPerInvocation;

    private byte[] encodedBlock;
    private int encodedLength;
    private ByteArrayDataInput[] inputs;
    private long[][] outputs;
    private NumericBlockDecoder blockDecoder;

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        final long[] template = BoundaryBlockSupplier.builder(SEED, blockSize).withFlips(flips).withJitterMs(jitterMs).build().get();

        final PipelineConfig config = PipelineConfig.forLongs(blockSize).splitDelta().bitPack();
        final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
        final NumericBlockEncoder blockEncoder = encoder.newBlockEncoder();

        final byte[] scratch = new byte[Long.BYTES * blockSize + EXTRA_METADATA_SIZE];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(scratch);
        final long[] encodeBuffer = new long[blockSize];
        System.arraycopy(template, 0, encodeBuffer, 0, blockSize);
        blockEncoder.encode(encodeBuffer, blockSize, out);
        encodedLength = out.getPosition();
        encodedBlock = new byte[encodedLength];
        System.arraycopy(scratch, 0, encodedBlock, 0, encodedLength);

        final NumericDecoder decoder = NumericCodecFactory.DEFAULT.createDecoder(encoder.descriptor());
        blockDecoder = decoder.newBlockDecoder();

        inputs = new ByteArrayDataInput[blocksPerInvocation];
        outputs = new long[blocksPerInvocation][blockSize];
        for (int i = 0; i < blocksPerInvocation; i++) {
            inputs[i] = new ByteArrayDataInput(encodedBlock);
        }
    }

    @Benchmark
    public void decode(final Blackhole bh) throws IOException {
        for (int i = 0; i < blocksPerInvocation; i++) {
            inputs[i].reset(encodedBlock);
            blockDecoder.decode(outputs[i], blockSize, inputs[i]);
            bh.consume(outputs[i][0]);
        }
    }
}
