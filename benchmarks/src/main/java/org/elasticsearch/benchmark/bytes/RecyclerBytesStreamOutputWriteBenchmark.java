/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.bytes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@Warmup(time = 1)
@Measurement(time = 1)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(1)
public class RecyclerBytesStreamOutputWriteBenchmark {

    // large enough to negate stream reset
    // and not too large to stay under stream limit of 2GB on worst case (5 bytes vint)
    private static final int WRITES_PER_ITERATION = 400_000;
    private static final int RANDOM_NUMS_PER_ITERATION = 1000;
    private static final int PAGE_SIZE = 16384;

    private ThreadLocalRandom random = ThreadLocalRandom.current();
    private int[] vint1Byte;
    private int[] vint2Bytes;
    private int[] vint3Bytes;
    private int[] vint4Bytes;
    private int[] vint5Bytes;
    private int[] vintNegBytes;
    private RecyclerBytesStreamOutput output = new RecyclerBytesStreamOutput(new SinglePageStream());

    private int randomVInt(int vIntByteSize, boolean isNeg) {
        return (isNeg ? -1 : 1) * switch (vIntByteSize) {
            case 1 -> random.nextInt(0, 1 << 7);
            case 2 -> random.nextInt(1 << 7, 1 << 14);
            case 3 -> random.nextInt(1 << 14, 1 << 21);
            case 4 -> random.nextInt(1 << 21, 1 << 28);
            case 5 -> random.nextInt(1 << 28, 1 << 30);
            default -> throw new IllegalArgumentException("number of bytes must be between 1 and 5");
        };
    }

    private int[] randomArray(int vIntByteSize, boolean isNeg) {
        final var out = new int[RANDOM_NUMS_PER_ITERATION];
        for (var i = 0; i < RANDOM_NUMS_PER_ITERATION; i++) {
            out[i] = randomVInt(vIntByteSize, isNeg);
        }
        return out;
    }

    @Setup(Level.Iteration)
    public void resetNums() {
        vint1Byte = randomArray(1, false);
        vint2Bytes = randomArray(2, false);
        vint3Bytes = randomArray(3, false);
        vint4Bytes = randomArray(4, false);
        vint5Bytes = randomArray(5, false);
        vintNegBytes = randomArray(random.nextInt(1, 6), true);
    }

    private void writeLoop(int[] nums) throws IOException {
        for (int reps = 0; reps < WRITES_PER_ITERATION / nums.length; reps++) {
            for (var n : nums) {
                output.writeVInt(n);
            }
        }
        output.seek(0);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVInt1() throws IOException {
        writeLoop(vint1Byte);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVInt2() throws IOException {
        writeLoop(vint2Bytes);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVInt3() throws IOException {
        writeLoop(vint3Bytes);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVInt4() throws IOException {
        writeLoop(vint4Bytes);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVInt5() throws IOException {
        writeLoop(vint5Bytes);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVIntNeg() throws IOException {
        writeLoop(vintNegBytes);
    }

    // recycle same page, we never read previous pages
    private static class SinglePageStream implements Recycler<BytesRef> {

        private final BytesRef page = new BytesRef(new byte[PAGE_SIZE], 0, PAGE_SIZE);

        @Override
        public V<BytesRef> obtain() {
            return new V<>() {
                @Override
                public BytesRef v() {
                    return page;
                }

                @Override
                public boolean isRecycled() {
                    return true;
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public int pageSize() {
            return PAGE_SIZE;
        }
    }
}
