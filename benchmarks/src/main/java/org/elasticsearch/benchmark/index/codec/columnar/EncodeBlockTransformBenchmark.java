/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;

/**
 * Per-stage encode benchmark for the ColumNAR pipeline.
 *
 * <p>Each invocation copies the template into {@code blocksPerInvocation} pre-allocated input
 * arrays and resets a pre-allocated output buffer before calling the encoder. Separate input
 * arrays per block prevent the JIT from eliminating the encode work via alias analysis or
 * dead-code elimination: a shared array would let the JIT observe that the encoder overwrites
 * the same memory on every call and hoist or elide it entirely. The copy and buffer-reset add
 * a constant overhead per block. Because that overhead is structurally identical between any
 * two runs of the same benchmark, any observed difference in throughput score comes exclusively
 * from changes to the encode path itself.
 *
 * <p>See {@link BlockTransformBenchmark} for the list of stages and block shapes.
 *
 * <h2>Ready to run</h2>
 *
 * <pre>{@code
 * # Full stage x pattern matrix
 * ./gradlew :benchmarks:run --args="EncodeBlockTransformBenchmark"
 *
 * # Single stage across all patterns
 * ./gradlew :benchmarks:run --args="EncodeBlockTransformBenchmark -p stage=splitDelta"
 *
 * # Compare stages on the TSDB split pattern
 * ./gradlew :benchmarks:run --args="EncodeBlockTransformBenchmark -p pattern=TSDB_SPLIT"
 *
 * # Allocation rate
 * ./gradlew :benchmarks:run --args="EncodeBlockTransformBenchmark -p stage=alp -p pattern=SENSOR_DOUBLES -prof gc"
 *
 * # Quick smoke
 * ./gradlew :benchmarks:run --args="EncodeBlockTransformBenchmark -wi 1 -i 1 -f 1 -w 1 -r 1 -p stage=delta -p pattern=MONOTONIC_TIMESTAMPS"
 * }</pre>
 */
public class EncodeBlockTransformBenchmark extends BlockTransformBenchmark {

    private long[][] inputs;
    private long[] template;
    private byte[][] outputBuffers;
    private ByteArrayDataOutput[] outputs;

    @Setup(Level.Trial)
    public void setupTrial() {
        template = NumericData.generate(pattern, blockSize);

        inputs = new long[blocksPerInvocation][blockSize];
        outputBuffers = new byte[blocksPerInvocation][Long.BYTES * blockSize + EXTRA_METADATA_SIZE];
        outputs = new ByteArrayDataOutput[blocksPerInvocation];
        for (int i = 0; i < blocksPerInvocation; i++) {
            outputs[i] = new ByteArrayDataOutput(outputBuffers[i]);
        }

        blockEncoder = encoderFor(stage, blockSize);
    }

    @Benchmark
    public void encode(final Blackhole bh) throws IOException {
        for (int i = 0; i < blocksPerInvocation; i++) {
            System.arraycopy(template, 0, inputs[i], 0, blockSize);
            outputs[i].reset(outputBuffers[i]);
            blockEncoder.encode(inputs[i], blockSize, outputs[i]);
            bh.consume(outputs[i].getPosition());
        }
    }
}
