/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.bytes;

import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.transport.BytesRefRecycler;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Compares {@link RecyclerBytesStreamOutput#repeatLastBytes} against writing the same block once per copy.
 *
 * <p>Both benchmarks emit exactly the same bytes, and both run {@code loopsPerInvocation} blocks so that an
 * invocation writes roughly {@link #TARGET_BYTES_PER_INVOCATION} regardless of the parameters; the score is
 * therefore only comparable between the two methods at the same parameter combination, not across
 * combinations. The stream is reset with {@code seek(0)} rather than reallocated, so after warm-up it reuses
 * its pages and the measurement excludes page allocation.
 *
 * <h2>Ready to run</h2>
 *
 * <pre>{@code
 * # Full matrix
 * ./gradlew :benchmarks:run --args="RecyclerBytesStreamOutputRepeatBenchmark"
 *
 * # The 8-byte block the fixed-width ESCF columns repeat
 * ./gradlew :benchmarks:run --args="RecyclerBytesStreamOutputRepeatBenchmark -p blockBytes=8"
 *
 * # Quick smoke
 * ./gradlew :benchmarks:run --args="RecyclerBytesStreamOutputRepeatBenchmark -wi 1 -i 1 -f 1 -w 1 -r 1 -p blockBytes=8 -p times=256"
 * }</pre>
 */
@BenchmarkMode(Mode.AverageTime)
@Warmup(time = 1)
@Measurement(time = 1)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(1)
public class RecyclerBytesStreamOutputRepeatBenchmark {

    private static final int TARGET_BYTES_PER_INVOCATION = 256 * 1024;

    @Param({ "8", "64", "1000" })
    public int blockBytes;

    @Param({ "4", "32", "256" })
    public int times;

    private byte[] block;
    private int loopsPerInvocation;
    private RecyclerBytesStreamOutput output;

    @Setup(Level.Trial)
    public void setUp() {
        block = new byte[blockBytes];
        ThreadLocalRandom.current().nextBytes(block);
        loopsPerInvocation = Math.max(1, TARGET_BYTES_PER_INVOCATION / (blockBytes * (times + 1)));
        output = new RecyclerBytesStreamOutput(new BytesRefRecycler(new PageCacheRecycler(Settings.EMPTY)));
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        output.close();
    }

    @Benchmark
    public void repeatLastBytes() {
        for (int i = 0; i < loopsPerInvocation; i++) {
            output.writeBytes(block, 0, blockBytes);
            output.repeatLastBytes(blockBytes, times);
        }
        output.seek(0);
    }

    @Benchmark
    public void writeOncePerCopy() {
        for (int i = 0; i < loopsPerInvocation; i++) {
            for (int copy = 0; copy <= times; copy++) {
                output.writeBytes(block, 0, blockBytes);
            }
        }
        output.seek(0);
    }
}
