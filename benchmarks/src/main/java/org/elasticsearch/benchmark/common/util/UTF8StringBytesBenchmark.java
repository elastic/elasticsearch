/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.common.util;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.UUIDs;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 3)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 1)
public class UTF8StringBytesBenchmark {

    @State(Scope.Thread)
    public static class StringState {
        @Param({ "uuid", "short", "long", "nonAscii", "veryLong" })
        String stringType;

        String string;
        BytesRef bytes;

        @Setup
        public void setup() {
            string = switch (stringType) {
                case "uuid" -> UUIDs.base64UUID();
                case "short" -> generateAsciiString(20);
                case "long" -> generateAsciiString(100);
                case "nonAscii" -> generateUTF8String(200);
                case "veryLong" -> generateAsciiString(1000);
                default -> throw new IllegalArgumentException("Unknown stringType: " + stringType);
            };
            bytes = getBytes(string);
        }
    }

    @Benchmark
    public BytesRef getBytesJDK(StringState state) {
        byte[] bytes = state.string.getBytes(StandardCharsets.UTF_8);
        return new BytesRef(bytes, 0, bytes.length);
    }

    @Benchmark
    public BytesRef getBytesUnicodeUtils(StringState state) {
        String string = state.string;
        int length = string.length();
        int size = UnicodeUtil.calcUTF16toUTF8Length(string, 0, length);
        byte[] out = new byte[size];
        UnicodeUtil.UTF16toUTF8(string, 0, length, out, 0);
        return new BytesRef(out, 0, out.length);
    }

    @Benchmark
    public BytesRef getBytesByteBufferEncoder(StringState state) {
        var byteBuff = StandardCharsets.UTF_8.encode(state.string);
        assert byteBuff.hasArray();
        return new BytesRef(byteBuff.array(), byteBuff.arrayOffset() + byteBuff.position(), byteBuff.remaining());
    }

    @Benchmark
    public String getStringJDK(StringState state) {
        BytesRef bytes = state.bytes;
        return new String(bytes.bytes, bytes.offset, bytes.length, StandardCharsets.UTF_8);
    }

    @Benchmark
    public String getStringByteBufferDecoder(StringState state) {
        BytesRef bytes = state.bytes;
        var byteBuff = ByteBuffer.wrap(bytes.bytes, bytes.offset, bytes.length);
        return StandardCharsets.UTF_8.decode(byteBuff).toString();
    }

    private static BytesRef getBytes(String string) {
        int before = ThreadLocalRandom.current().nextInt(0, 50);
        int after = ThreadLocalRandom.current().nextInt(0, 50);
        byte[] stringBytes = string.getBytes(StandardCharsets.UTF_8);
        byte[] finalBytes = new byte[before + after + stringBytes.length];
        System.arraycopy(stringBytes, 0, finalBytes, before, stringBytes.length);
        return new BytesRef(finalBytes, before, stringBytes.length);
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

    public static String generateUTF8String(int n) {
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
}
