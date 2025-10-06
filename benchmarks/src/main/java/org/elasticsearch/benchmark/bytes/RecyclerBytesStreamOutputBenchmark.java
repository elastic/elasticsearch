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
public class RecyclerBytesStreamOutputBenchmark {

    private final AtomicReference<BytesRef> bytesRef = new AtomicReference<>(new BytesRef(16384));
    private RecyclerBytesStreamOutput streamOutput;
    private String shortString;
    private String longString;
    private String nonAsciiString;
    private String veryLongString;
    private byte[] bytes1;
    private byte[] bytes2;
    private byte[] bytes3;
    private byte[] multiPageBytes;
    private int[] vints;

    @Setup
    public void initResults() throws IOException {
        streamOutput = new RecyclerBytesStreamOutput(new BenchmarkRecycler(bytesRef));
        ThreadLocalRandom random = ThreadLocalRandom.current();

        bytes1 = new byte[327];
        bytes2 = new byte[712];
        bytes3 = new byte[1678];
        multiPageBytes = new byte[16387 * 4];
        random.nextBytes(bytes1);
        random.nextBytes(bytes2);
        random.nextBytes(bytes3);
        random.nextBytes(multiPageBytes);

        // We use weights to generate certain sized UTF-8 characters and vInts. However, there is still some non-determinism which could
        // impact direct comparisons run-to-run

        shortString = generateAsciiString(20);
        longString = generateAsciiString(100);
        nonAsciiString = generateUtf8String(200);
        veryLongString = generateAsciiString(800);
        // vint values for benchmarking
        vints = new int[1000];
        for (int i = 0; i < vints.length; i++) {
            if (random.nextBoolean()) {
                // 1-byte 50% of the time
                vints[i] = random.nextInt(128);
            } else if (random.nextBoolean()) {
                // 2-byte 25% of the time
                vints[i] = random.nextInt(128, 16384);
            } else {
                if (random.nextBoolean()) {
                    // 3-byte vints
                    vints[i] = random.nextInt(16384, 2097152);
                } else {
                    // All vint variants
                    vints[i] = random.nextInt();
                }
            }
        }
    }

    @Benchmark
    public void writeByte() throws IOException {
        streamOutput.seek(1);
        for (byte item : bytes1) {
            streamOutput.writeByte(item);
        }
        for (byte item : bytes2) {
            streamOutput.writeByte(item);
        }
        for (byte item : bytes3) {
            streamOutput.writeByte(item);
        }
    }

    @Benchmark
    public void writeBytes() throws IOException {
        streamOutput.seek(1);
        streamOutput.writeBytes(bytes1, 0, bytes1.length);
        streamOutput.writeBytes(bytes2, 0, bytes2.length);
        streamOutput.writeBytes(bytes3, 0, bytes3.length);
    }

    @Benchmark
    public void writeBytesAcrossPageBoundary() throws IOException {
        streamOutput.seek(16384 - 1000);
        streamOutput.writeBytes(bytes1, 0, bytes1.length);
        streamOutput.writeBytes(bytes2, 0, bytes2.length);
        streamOutput.writeBytes(bytes3, 0, bytes3.length);
    }

    @Benchmark
    public void writeBytesMultiPage() throws IOException {
        streamOutput.seek(16384 - 1000);
        streamOutput.writeBytes(multiPageBytes, 0, multiPageBytes.length);
    }

    @Benchmark
    public void writeString() throws IOException {
        streamOutput.seek(1);
        streamOutput.writeString(shortString);
        streamOutput.writeString(longString);
        streamOutput.writeString(nonAsciiString);
        streamOutput.writeString(veryLongString);
    }

    @Benchmark
    public void writeVInt() throws IOException {
        streamOutput.seek(1);
        for (int vint : vints) {
            streamOutput.writeVInt(vint);
        }
    }

    public static String generateAsciiString(int n) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        StringBuilder sb = new StringBuilder(n);

        for (int i = 0; i < n; i++) {
            int ascii = random.nextInt(128);
            sb.append((char) ascii);
        }

        return sb.toString();
    }

    public static String generateUtf8String(int n) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        StringBuilder sb = new StringBuilder(n);

        for (int i = 0; i < n; i++) {
            int codePoint;
            int probability = random.nextInt(100);

            if (probability < 85) {
                // 1-byte UTF-8 (ASCII range)
                // 0x0000 to 0x007F
                codePoint = random.nextInt(0x0080);
            } else if (probability < 95) {
                // 2-byte UTF-8
                // 0x0080 to 0x07FF
                codePoint = random.nextInt(0x0080, 0x0800);
            } else {
                // 3-byte UTF-8
                // 0x0800 to 0xFFFF
                do {
                    codePoint = random.nextInt(0x0800, 0x10000);
                    // Skip surrogate pairs (0xD800-0xDFFF)
                } while (codePoint >= 0xD800 && codePoint <= 0xDFFF);
            }

            sb.appendCodePoint(codePoint);
        }

        return sb.toString();
    }

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
