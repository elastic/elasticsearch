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
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

// throughput is more representable metric, it amortizes pages recycling code path and reduce benchmark error
@BenchmarkMode(Mode.Throughput)
// each operation is nanoseconds range, measuring within 1 second provides enough samples
@Warmup(time = 1)
@Measurement(time = 5)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(1)
public class RecyclerBytesStreamOutputWriteBenchmark {
    private static final int PAGE_SIZE = 16384;

    // must not be final - avoid constant fold
    // see https://github.com/openjdk/jmh/blob/master/jmh-samples/src/main/java/org/openjdk/jmh/samples/JMHSample_10_ConstantFold.java
    private int vint1 = 2;
    private int vint2 = vint1 << 8;
    private int vint3 = vint2 << 8;
    private int vint4 = vint3 << 8;
    private RecyclerBytesStreamOutput output = new RecyclerBytesStreamOutput(new SinglePageStream());

    // Ideally, we should not write own loop and let JMH do it right.
    // But there is a problem, output stream can hold only 2GB of data and needs occasional reset.
    // There is a lifecycle hook @TearDown(Level.Invocation) but it comes with own set of problems.
    private int writeVInt(int n) throws IOException {
        // write multiple pages
        for (int i = 0; i <= PAGE_SIZE; i++) {
            output.writeVInt(n);
        }
        output.seek(1);
        // return value back to benchmark so it can be Blackhole-ed.
        // see https://github.com/openjdk/jmh/blob/master/jmh-samples/src/main/java/org/openjdk/jmh/samples/JMHSample_09_Blackholes.java
        return n;
    }

    @Benchmark
    public int writeVInt1() throws IOException {
        return writeVInt(vint1);
    }

    @Benchmark
    public int writeVInt2() throws IOException {
        return writeVInt(vint2);
    }

    @Benchmark
    public int writeVInt3() throws IOException {
        return writeVInt(vint3);
    }

    @Benchmark
    public int writeVInt4() throws IOException {
        return writeVInt(vint4);
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
