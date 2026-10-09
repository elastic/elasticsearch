/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;

/**
 * Per-stage decode benchmark for the ColumNAR pipeline.
 *
 * <p>Mirror of {@link EncodeBlockTransformBenchmark} on the read path. Encodes one block at
 * setup time and measures the cost of decoding it repeatedly. Each invocation resets
 * {@code blocksPerInvocation} pre-allocated {@link ByteArrayDataInput} objects to the start of
 * the same encoded payload, then decodes into pre-allocated output arrays. Resetting the read
 * cursor rather than allocating a new input object avoids per-invocation GC pressure while
 * still presenting the decoder with a fresh byte sequence on every call, preventing the JIT
 * from eliding the decode work. The cursor-reset adds a constant, structurally fixed overhead
 * per block. Because that overhead is identical between any two runs of the same benchmark,
 * any observed difference in throughput score comes exclusively from changes to the decode
 * path itself.
 *
 * <p>See {@link BlockTransformBenchmark} for the list of stages and block shapes.
 *
 * <h2>Ready to run</h2>
 *
 * <pre>{@code
 * # Full stage x pattern matrix
 * ./gradlew :benchmarks:run --args="DecodeBlockTransformBenchmark"
 *
 * # Single stage across all patterns
 * ./gradlew :benchmarks:run --args="DecodeBlockTransformBenchmark -p stage=splitDelta"
 *
 * # Compare stages on the TSDB split pattern
 * ./gradlew :benchmarks:run --args="DecodeBlockTransformBenchmark -p pattern=TSDB_SPLIT"
 *
 * # Quick smoke
 * ./gradlew :benchmarks:run --args="DecodeBlockTransformBenchmark -wi 1 -i 1 -f 1 -w 1 -r 1 -p stage=delta -p pattern=MONOTONIC_TIMESTAMPS"
 * }</pre>
 */
public class DecodeBlockTransformBenchmark extends BlockTransformBenchmark {

    private byte[] encodedBlock;
    private ByteArrayDataInput[] inputs;
    private long[][] outputs;

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        final long[] template = NumericData.generate(pattern, blockSize);
        blockEncoder = encoderFor(stage, blockSize);

        final byte[] scratch = new byte[Long.BYTES * blockSize + EXTRA_METADATA_SIZE];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(scratch);
        final long[] encodeBuffer = new long[blockSize];
        System.arraycopy(template, 0, encodeBuffer, 0, blockSize);
        blockEncoder.encode(encodeBuffer, blockSize, out);
        final int encodedLength = out.getPosition();
        encodedBlock = new byte[encodedLength];
        System.arraycopy(scratch, 0, encodedBlock, 0, encodedLength);

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
            blockEncoder.decode(inputs[i], blockSize, outputs[i]);
            bh.consume(outputs[i][0]);
        }
    }
}
