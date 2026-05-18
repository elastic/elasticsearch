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

    // large enough to negate stream reset and not too large to stay under stream limit of 2GB on worst case (9 bytes vlong)
    private static final int WRITES_PER_ITERATION = 400_000;
    private static final int RANDOM_NUMS_PER_ITERATION = 1000;
    private static final int LOOPS_PER_ITERATION = WRITES_PER_ITERATION / RANDOM_NUMS_PER_ITERATION;
    private static final int PAGE_SIZE = 16384;

    private ThreadLocalRandom random = ThreadLocalRandom.current();
    private int[] vint1Byte;
    private int[] vint2Bytes;
    private int[] vint3Bytes;
    private int[] vint4Bytes;
    private int[] vint5Bytes;
    private int[] vintNegBytes;
    private long[] vlong1Byte;
    private long[] vlong2Bytes;
    private long[] vlong3Bytes;
    private long[] vlong4Bytes;
    private long[] vlong5Bytes;
    private long[] vlong6Bytes;
    private long[] vlong7Bytes;
    private long[] vlong8Bytes;
    private long[] vlong9Bytes;

    private RecyclerBytesStreamOutput output = new RecyclerBytesStreamOutput(new SinglePageStream());

    private int randomVInt(int vIntByteSize, boolean isNeg) {
        if (isNeg) {
            return random.nextInt(Integer.MIN_VALUE, 0);
        }
        return switch (vIntByteSize) {
            case 1 -> random.nextInt(0, 1 << 7);
            case 2 -> random.nextInt(1 << 7, 1 << 14);
            case 3 -> random.nextInt(1 << 14, 1 << 21);
            case 4 -> random.nextInt(1 << 21, 1 << 28);
            case 5 -> random.nextInt((1 << 28) - 1, Integer.MAX_VALUE) + 1; // ±1 because upper bound is exclusive
            default -> throw new IllegalArgumentException("number of bytes must be between 1 and 5");
        };
    }

    private int[] randomVInts(int vIntByteSize, boolean isNeg) {
        final var out = new int[RANDOM_NUMS_PER_ITERATION];
        for (var i = 0; i < RANDOM_NUMS_PER_ITERATION; i++) {
            out[i] = randomVInt(vIntByteSize, isNeg);
        }
        return out;
    }

    private long[] randomVLongs(int bytes) {
        final var out = new long[RANDOM_NUMS_PER_ITERATION];
        final long upperBound, lowerBound, offset;
        if (bytes == 1) {
            upperBound = 1 << 7;
            lowerBound = 0L;
            offset = 0L;
        } else if (bytes == 9) {
            upperBound = Long.MAX_VALUE;
            lowerBound = (1L << 56) - 1;
            offset = 1L; // ±1 because upper bound is exclusive
        } else {
            upperBound = 1L << (bytes * 7);
            lowerBound = upperBound >> 7;
            offset = 0L;
        }
        for (var i = 0; i < RANDOM_NUMS_PER_ITERATION; i++) {
            out[i] = random.nextLong(lowerBound, upperBound) + offset;
        }
        return out;
    }

    @Setup(Level.Iteration)
    public void resetNums() {
        vint1Byte = randomVInts(1, false);
        vint2Bytes = randomVInts(2, false);
        vint3Bytes = randomVInts(3, false);
        vint4Bytes = randomVInts(4, false);
        vint5Bytes = randomVInts(5, false);
        vintNegBytes = randomVInts(0, true);

        vlong1Byte = randomVLongs(1);
        vlong2Bytes = randomVLongs(2);
        vlong3Bytes = randomVLongs(3);
        vlong4Bytes = randomVLongs(4);
        vlong5Bytes = randomVLongs(5);
        vlong6Bytes = randomVLongs(6);
        vlong7Bytes = randomVLongs(7);
        vlong8Bytes = randomVLongs(8);
        vlong9Bytes = randomVLongs(9);
    }

    private void writeVIntLoop(int[] nums) throws IOException {
        for (int reps = 0; reps < LOOPS_PER_ITERATION; reps++) {
            for (var n : nums) {
                output.writeVInt(n);
            }
        }
        output.seek(0);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVInt1() throws IOException {
        writeVIntLoop(vint1Byte);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVInt2() throws IOException {
        writeVIntLoop(vint2Bytes);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVInt3() throws IOException {
        writeVIntLoop(vint3Bytes);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVInt4() throws IOException {
        writeVIntLoop(vint4Bytes);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVInt5() throws IOException {
        writeVIntLoop(vint5Bytes);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVIntNeg() throws IOException {
        writeVIntLoop(vintNegBytes);
    }

    private void writeVLongLoop(long[] nums) throws IOException {
        for (int reps = 0; reps < LOOPS_PER_ITERATION; reps++) {
            for (var n : nums) {
                output.writeVLong(n);
            }
        }
        output.seek(0);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVLong1() throws IOException {
        writeVLongLoop(vlong1Byte);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVLong2() throws IOException {
        writeVLongLoop(vlong2Bytes);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVLong3() throws IOException {
        writeVLongLoop(vlong3Bytes);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVLong4() throws IOException {
        writeVLongLoop(vlong4Bytes);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVLong5() throws IOException {
        writeVLongLoop(vlong5Bytes);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVLong6() throws IOException {
        writeVLongLoop(vlong6Bytes);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVLong7() throws IOException {
        writeVLongLoop(vlong7Bytes);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVLong8() throws IOException {
        writeVLongLoop(vlong8Bytes);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public void writeVLong9() throws IOException {
        writeVLongLoop(vlong9Bytes);
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
