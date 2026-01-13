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
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

// throughput is more representable metric, it amortizes pages recycling code path and reduces benchmark error
@BenchmarkMode(Mode.Throughput)
@Warmup(time = 1)
@Measurement(time = 1)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(1)
public class RecyclerBytesStreamOutputWriteBenchmark {
    private static final int PAGE_SIZE = 16384;

    // large enough to negate stream reset
    // and not too large to reach 2GB limit on worst case (5 bytes vint)
    private static final int WRITES_PER_ITERATION = 400_000;

    // must not be final - avoid constant fold
    // see https://github.com/openjdk/jmh/blob/master/jmh-samples/src/main/java/org/openjdk/jmh/samples/JMHSample_10_ConstantFold.java
    private int vint1Byte = 123; // any number between 0 and 127 (7 bit)
    private int vint2Bytes = (vint1Byte << 7) | vint1Byte;
    private int vint3Bytes = (vint2Bytes << 7) | vint2Bytes;
    private int vint4Bytes = (vint3Bytes << 7) | vint3Bytes;
    private int vint5Bytes = Integer.MIN_VALUE;
    private RecyclerBytesStreamOutput output = new RecyclerBytesStreamOutput(new SinglePageStream());

    private int writeLoop(int n) throws IOException {
        for (int i = 0; i < WRITES_PER_ITERATION; i++) {
            output.writeVInt(n);
        }
        output.seek(0);
        return n;
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public int writeVInt1() throws IOException {
        return writeLoop(vint1Byte);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public int writeVInt2() throws IOException {
        return writeLoop(vint2Bytes);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public int writeVInt3() throws IOException {
        return writeLoop(vint3Bytes);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public int writeVInt4() throws IOException {
        return writeLoop(vint4Bytes);
    }

    @Benchmark
    @OperationsPerInvocation(WRITES_PER_ITERATION)
    public int writeVInt5() throws IOException {
        return writeLoop(vint5Bytes);
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
