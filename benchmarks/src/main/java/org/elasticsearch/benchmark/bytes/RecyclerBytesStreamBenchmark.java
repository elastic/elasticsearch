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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Warmup(iterations = 3)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class RecyclerBytesStreamBenchmark {

    private final AtomicReference<BytesRef> bytesRef = new AtomicReference<>(new BytesRef(16384));
    private RecyclerBytesStreamOutput streamOutput;
    private String shortString;
    private String longString;
    private byte[] bytes1;
    private byte[] bytes2;

    @Setup
    public void initResults() throws IOException {
        streamOutput = new RecyclerBytesStreamOutput(new BenchmarkRecycler(bytesRef));
        shortString = "short string";
        longString = "long string that is completely ascii text and nothing else for copying into buffer";
        ThreadLocalRandom random = ThreadLocalRandom.current();
        bytes1 =  new byte[random.nextInt(100, 500)];
        bytes2 =  new byte[random.nextInt(600, 800)];
        random.nextBytes(bytes1);
        random.nextBytes(bytes2);
    }

    @Benchmark
    public void writeBytes() throws IOException {
        streamOutput.seek(0);
        streamOutput.writeBytes(bytes1, 0,  bytes1.length);
        streamOutput.writeBytes(bytes2, 0,  bytes2.length);
    }

//    @Benchmark
//    public void writeString() throws IOException {
//        streamOutput.seek(0);
//        streamOutput.writeString(shortString);
//        streamOutput.writeString(longString);
//    }

    private record BenchmarkRecycler(AtomicReference<BytesRef> bytesRef) implements Recycler<BytesRef> {

        @Override
        public V<BytesRef> obtain() {
            BytesRef recycledBytesRef = bytesRef.getAndSet(null);
            final BytesRef localBytesRef;
            final boolean recycled;
            if (recycledBytesRef != null) {
                recycled = true;
                localBytesRef = recycledBytesRef;
            } else {
                recycled = false;
                localBytesRef = new BytesRef(16384);
            }
            return new V<>() {
                @Override
                public BytesRef v() {
                    return localBytesRef;
                }

                @Override
                public boolean isRecycled() {
                    return recycled;
                }

                @Override
                public void close() {
                    if (recycled) {
                        bytesRef.set(localBytesRef);
                    }
                }
            };
        }

        @Override
        public int pageSize() {
            return 16384;
        }
    }
}
