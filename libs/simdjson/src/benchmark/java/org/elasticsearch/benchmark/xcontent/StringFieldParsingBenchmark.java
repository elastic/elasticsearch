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
import org.elasticsearch.simdjson.JsonDocumentParser;
import org.elasticsearch.simdjson.SimdJsonParserPool;
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

import java.math.BigInteger;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Isolates {@link org.elasticsearch.simdjson.SimdJsonDirectWalker#walkObject} plus the vectorized
 * string-copy loop it drives in {@code StringParser.doParseString}.
 *
 * <p><strong>Running.</strong>
 * <pre>{@code
 * ./gradlew :libs:simdjson:benchmark --args "StringFieldParsingBenchmark"
 *
 * # To inspect C2's inlining decision for the walkObject -> StringParser call chain:
 * ./gradlew :libs:simdjson:benchmark --args "StringFieldParsingBenchmark -f 1 -wi 3 -i 1 \
 *   -jvmArgsAppend -XX:+UnlockDiagnosticVMOptions \
 *   -jvmArgsAppend -XX:+PrintCompilation \
 *   -jvmArgsAppend -XX:+PrintInlining" | tee /tmp/inlining.log
 * }</pre>
 */
@Fork(value = 1, jvmArgsAppend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class StringFieldParsingBenchmark {

    @Param({ "2000" })
    private int docCount;

    @Param({ "42" })
    private long seed;

    /** Percent of string field values that contain an escape, forcing the non-vectorized path. */
    @Param({ "0", "10" })
    private int escapePercent;

    private byte[][] docs;
    private JsonDocumentParser docParser;
    private final CountingHandler handler = new CountingHandler();

    @Setup
    public void setUp() {
        BenchmarkLogging.configure();
        Random random = new Random(seed);
        docs = new byte[docCount][];
        for (int i = 0; i < docCount; i++) {
            docs[i] = generateDoc(random, escapePercent).getBytes(UTF_8);
        }
        int maxLen = 0;
        for (byte[] d : docs) {
            maxLen = Math.max(maxLen, d.length);
        }
        SimdJsonParserPool pool = new SimdJsonParserPool(Math.max(maxLen, 4096));
        docParser = pool.forCurrentThread();
    }

    @Benchmark
    public long parseDocs() {
        for (byte[] doc : docs) {
            docParser.parseDocument(doc, doc.length, handler);
        }
        docParser.publishFieldNames();
        return handler.checksum;
    }

    private static String generateDoc(Random random, int escapePercent) {
        return String.format(
            Locale.ROOT,
            """
                {
                  "WatchID": %d, "CounterID": %d, "RegionID": %d,
                  "Title": "%s", "URL": "%s", "Referer": "%s",
                  "UserAgent": "%s", "SearchPhrase": "%s", "MobilePhoneModel": "%s",
                  "ResolutionWidth": %d, "ResolutionHeight": %d
                }""",
            random.nextLong(),
            random.nextInt(200000),
            random.nextInt(100000),
            longString(random, 60, escapePercent),
            longString(random, 90, escapePercent),
            longString(random, 90, escapePercent),
            longString(random, 110, escapePercent),
            longString(random, 40, escapePercent),
            longString(random, 30, escapePercent),
            random.nextInt(3840),
            random.nextInt(2160)
        );
    }

    private static final char[] ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/-._".toCharArray();

    /**
     * A long, mostly-plain-ASCII string with no JSON-special characters, optionally containing
     * one {@code \n} escape near the middle when {@code escapePercent} triggers for this call.
     */
    private static String longString(Random random, int len, int escapePercent) {
        StringBuilder sb = new StringBuilder(len + 2);
        boolean withEscape = random.nextInt(100) < escapePercent;
        int escapeAt = len / 2;
        for (int i = 0; i < len; i++) {
            if (withEscape && i == escapeAt) {
                sb.append("\\n");
            }
            sb.append(ALPHABET[random.nextInt(ALPHABET.length)]);
        }
        return sb.toString();
    }

    /**
     * Records just enough to prevent dead-code elimination of the walk; every method is otherwise
     * as cheap as possible so the benchmark measures {@code walkObject}/{@code StringParser}, not
     * a handler implementation.
     */
    private static final class CountingHandler implements JsonDocumentHandler {
        long checksum;

        @Override
        public void startObject(String fieldName) {}

        @Override
        public void endObject() {}

        @Override
        public void emptyObject(String fieldName) {}

        @Override
        public void stringField(String fieldName, byte[] buf, int off, int len) {
            checksum ^= len;
        }

        @Override
        public void longField(String fieldName, long value, boolean fitsInt, byte[] srcBuf, int srcOff, int srcLen) {
            checksum ^= value;
        }

        @Override
        public void bigIntegerField(String fieldName, BigInteger value, byte[] srcBuf, int srcOff, int srcLen) {
            checksum ^= value.longValue();
        }

        @Override
        public void doubleField(String fieldName, double value, boolean fitsFloat, byte[] srcBuf, int srcOff, int srcLen) {
            checksum ^= Double.doubleToRawLongBits(value);
        }

        @Override
        public void booleanField(String fieldName, boolean value, byte[] srcBuf, int srcOff, int srcLen) {
            checksum ^= value ? 1 : 0;
        }

        @Override
        public void nullField(String fieldName) {}

        @Override
        public void startArray(String fieldName) {}

        @Override
        public void endArray() {}

        @Override
        public void arrayElemString(byte[] buf, int off, int len) {
            checksum ^= len;
        }

        @Override
        public void arrayElemLong(long value, boolean fitsInt) {
            checksum ^= value;
        }

        @Override
        public void arrayElemBigInteger(BigInteger value, byte[] srcBuf, int srcOff, int srcLen) {
            checksum ^= value.longValue();
        }

        @Override
        public void arrayElemDouble(double value, boolean fitsFloat) {
            checksum ^= Double.doubleToRawLongBits(value);
        }

        @Override
        public void arrayElemBoolean(boolean value) {
            checksum ^= value ? 1 : 0;
        }

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
