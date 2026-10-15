/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.xcontent;

import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.simdjson.JsonDocumentHandler;
import org.elasticsearch.simdjson.SimdJsonDirectWalker;
import org.elasticsearch.simdjson.SimdJsonParser;
import org.elasticsearch.simdjson.internal.fieldnames.FrozenFieldNameTable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Micro-benchmark for {@link SimdJsonDirectWalker}'s integer number parsing
 * ({@code handleNumber}/{@code handleArrayNumber}'s SWAR digit scanner), isolated from string
 * unescaping, double parsing, field-name resolution, and the rest of the encoder pipeline: every
 * field in every generated document is a plain JSON integer of a single, controlled digit width.
 *
 * <p>Parameterized by {@link #digitCount}, spanning the digit-count shapes seen in
 * ClickBench-style analytics documents: roughly 77% of numeric fields there are 1-2 digits
 * (booleans-as-0/1, small enum/status codes), with a long tail out past 10 digits (large
 * IDs/hashes). {@code digitCount=1,2} exercise the fast path added to
 * {@code handleNumber}/{@code handleArrayNumber}; {@code digitCount=5,10} exercise the
 * unchanged general (SWAR-loop) path, as a check that it isn't regressed.
 *
 * <pre>{@code
 * ./gradlew :libs:simdjson:benchmark --args "NumberFieldParsingBenchmark"
 * }</pre>
 */
@Fork(value = 3, jvmArgsAppend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class NumberFieldParsingBenchmark {

    private static final int FIELDS_PER_DOC = 64;
    private static final int DOC_COUNT = 200;

    @Param({ "1", "2", "5", "10" })
    private int digitCount;

    @Param({ "field", "array" })
    private String shape;

    private byte[][] docs;
    private SimdJsonParser parser;
    private SimdJsonDirectWalker walker;
    private ChecksumHandler handler;

    @Setup(Level.Trial)
    public void setUp() {
        BenchmarkLogging.configure();
        Random random = new Random(42);
        docs = new byte[DOC_COUNT][];
        int maxLen = 0;
        for (int i = 0; i < DOC_COUNT; i++) {
            String json = "array".equals(shape) ? generateArrayDoc(random, digitCount) : generateFieldDoc(random, digitCount);
            byte[] buf = json.getBytes(StandardCharsets.UTF_8);
            docs[i] = buf;
            maxLen = Math.max(maxLen, buf.length);
        }
        parser = new SimdJsonParser(maxLen);
        FrozenFieldNameTable parent = new FrozenFieldNameTable();
        walker = new SimdJsonDirectWalker(parent.makeChild());
        handler = new ChecksumHandler();
    }

    // e.g. digitCount=2 -> {"f0":42,"f1":17,...}: FIELDS_PER_DOC named integer fields, each
    // exactly digitCount digits (no sign, so the field path's fast/general dispatch is exercised
    // on its own; see testNegativeTwoDigitField etc. in SimdJsonDirectWalkerTests for sign
    // handling coverage).
    private static String generateFieldDoc(Random random, int digitCount) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (int i = 0; i < FIELDS_PER_DOC; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append("\"f").append(i).append("\":").append(randomDigits(random, digitCount));
        }
        sb.append('}');
        return sb.toString();
    }

    // Same digit-width control, but as one array of FIELDS_PER_DOC elements under a single field,
    // to exercise handleArrayNumber's fast path instead of handleNumber's.
    private static String generateArrayDoc(Random random, int digitCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"a\":[");
        for (int i = 0; i < FIELDS_PER_DOC; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(randomDigits(random, digitCount));
        }
        sb.append("]}");
        return sb.toString();
    }

    // A random digitCount-digit unsigned integer, e.g. digitCount=2 -> [10, 99], digitCount=1 ->
    // [0, 9]. The leading digit is forced non-zero (except for digitCount=1) so the value always
    // has exactly digitCount digits, keeping each @Param run monomorphic in shape.
    private static String randomDigits(Random random, int digitCount) {
        StringBuilder sb = new StringBuilder(digitCount);
        sb.append(digitCount == 1 ? random.nextInt(10) : 1 + random.nextInt(9));
        for (int i = 1; i < digitCount; i++) {
            sb.append(random.nextInt(10));
        }
        return sb.toString();
    }

    @Benchmark
    @OperationsPerInvocation(DOC_COUNT * FIELDS_PER_DOC)
    public void parse(Blackhole bh) {
        for (int i = 0; i < DOC_COUNT; i++) {
            byte[] buf = docs[i];
            parser.stage1(buf, 0, buf.length);
            parser.prepareDocumentWindow(0, buf.length);
            walker.walkDocument(buf, parser, handler);
        }
        bh.consume(handler.checksum);
    }

    /**
     * Discards every event except {@code longField}/{@code arrayElemLong} (the only ones these
     * benchmark documents produce), accumulating a checksum so the JIT can't dead-code-eliminate
     * the parse.
     */
    private static final class ChecksumHandler implements JsonDocumentHandler {
        long checksum;

        @Override
        public void longField(String fieldName, long value, boolean fitsInt, byte[] srcBuf, int srcOff, int srcLen) {
            checksum += value;
        }

        @Override
        public void arrayElemLong(long value, boolean fitsInt) {
            checksum += value;
        }

        @Override
        public void startObject(String fieldName) {}

        @Override
        public void endObject() {}

        @Override
        public void emptyObject(String fieldName) {}

        @Override
        public void stringField(String fieldName, byte[] buf, int off, int len) {}

        @Override
        public void bigIntegerField(String fieldName, BigInteger value, byte[] srcBuf, int srcOff, int srcLen) {}

        @Override
        public void doubleField(String fieldName, double value, boolean fitsFloat, byte[] srcBuf, int srcOff, int srcLen) {}

        @Override
        public void booleanField(String fieldName, boolean value, byte[] srcBuf, int srcOff, int srcLen) {}

        @Override
        public void nullField(String fieldName) {}

        @Override
        public void startArray(String fieldName) {}

        @Override
        public void endArray() {}

        @Override
        public void arrayElemString(byte[] buf, int off, int len) {}

        @Override
        public void arrayElemBigInteger(BigInteger value, byte[] srcBuf, int srcOff, int srcLen) {}

        @Override
        public void arrayElemDouble(double value, boolean fitsFloat) {}

        @Override
        public void arrayElemBoolean(boolean value) {}

        @Override
        public void arrayElemNull() {}

        @Override
        public void arrayElemStartObject() {}

        @Override
        public void arrayElemEndObject() {}

        @Override
        public void arrayElemStartArray() {}

        @Override
        public void arrayElemEndArray() {}
    }
}
