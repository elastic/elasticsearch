/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.simdjson;

import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.simdjson.internal.parsers.StringParser;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The path for string values that contain escapes, which is what ClickBench's {@code URL} and {@code Referer}
 * (written with {@code \/}) take for nearly every document. Each op handles every top-level string value that contains
 * a backslash in {@code docs} documents from {@code dataFile}, the way the walker does it: probe, size, allocate,
 * unescape.
 *
 * <ul>
 *   <li>{@code vector}: the vector {@code parseString} the walker used before.</li>
 *   <li>{@code swar}: {@code rawLength} and {@code unescape}, as the walker does now.</li>
 *   <li>{@code *ReusingOutput}: the same without the fresh destination array, to show what the allocation costs.
 *       The walker cannot do this, since handlers keep the array until the row is committed.</li>
 * </ul>
 */
@Fork(value = 2, jvmArgsAppend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 4, time = 2)
@Measurement(iterations = 6, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class EscapedStringBenchmark {

    /** Newline-delimited JSON documents, such as ClickBench's {@code hits}; set with {@code -p dataFile=...}. */
    @Param({ "" })
    private String dataFile;

    @Param({ "2000" })
    private int docs;

    private static final Pattern STRING = Pattern.compile("\"[A-Za-z0-9_]+\":(\"(?:[^\"\\\\]|\\\\.)*\")");

    private byte[][] buffers;
    private int[] bufferOf;
    private int[] quoteAt;
    private int count;
    private StringParser parser;
    private byte[] scratch = new byte[1 << 16];

    @Setup
    public void setUp() throws IOException {
        BenchmarkLogging.configure();
        if (dataFile.isEmpty()) {
            throw new IllegalArgumentException("set -p dataFile=<path to NDJSON>");
        }
        parser = new StringParser();
        List<byte[]> bufs = new ArrayList<>();
        List<int[]> pairs = new ArrayList<>();
        try (var lines = Files.newBufferedReader(Path.of(dataFile))) {
            String line;
            while (bufs.size() < docs && (line = lines.readLine()) != null) {
                byte[] b = line.getBytes(StandardCharsets.UTF_8);
                // decode as ISO-8859-1 so that character offsets equal byte offsets
                String ascii = new String(b, StandardCharsets.ISO_8859_1);
                Matcher mb = STRING.matcher(ascii);
                while (mb.find()) {
                    if (mb.group(1).indexOf('\\') >= 0) {
                        pairs.add(new int[] { bufs.size(), mb.start(1) });
                    }
                }
                bufs.add(b);
            }
        }
        buffers = bufs.toArray(new byte[0][]);
        count = pairs.size();
        bufferOf = new int[count];
        quoteAt = new int[count];
        long bytes = 0;
        for (int i = 0; i < count; i++) {
            bufferOf[i] = pairs.get(i)[0];
            quoteAt[i] = pairs.get(i)[1];
            byte[] b = buffers[bufferOf[i]];
            int rawA = rawLengthOld(b, quoteAt[i]);
            int rawB = parser.rawLength(b, quoteAt[i]);
            if (rawA != rawB) {
                throw new AssertionError("raw length differs " + rawA + " vs " + rawB);
            }
            byte[] x = new byte[rawA + StringParser.DESTINATION_PADDING];
            byte[] y = new byte[rawA + StringParser.DESTINATION_PADDING];
            int la = parser.parseString(b, quoteAt[i], x);
            int lb = parser.unescape(b, quoteAt[i], y);
            if (la != lb || Arrays.equals(x, 0, la, y, 0, lb) == false) {
                throw new AssertionError("unescaped value differs at " + i);
            }
            bytes += rawA;
        }
        System.out.println("[escaped-string] escaped values=" + count + " avgRawBytes=" + (bytes / (double) count));
    }

    @Benchmark
    public long vector() {
        long acc = 0;
        for (int i = 0; i < count; i++) {
            byte[] b = buffers[bufferOf[i]];
            int q = quoteAt[i];
            int len = parser.scanUnescapedLength(b, q);
            if (len < 0) {
                int raw = rawLengthOld(b, q);
                byte[] out = new byte[raw + StringParser.DESTINATION_PADDING];
                acc += parser.parseString(b, q, out) + out[0];
            }
        }
        return acc;
    }

    @Benchmark
    public long vectorReusingOutput() {
        long acc = 0;
        for (int i = 0; i < count; i++) {
            byte[] b = buffers[bufferOf[i]];
            int q = quoteAt[i];
            int len = parser.scanUnescapedLength(b, q);
            if (len < 0) {
                acc += parser.parseString(b, q, scratch) + scratch[0];
            }
        }
        return acc;
    }

    @Benchmark
    public long swar() {
        long acc = 0;
        for (int i = 0; i < count; i++) {
            byte[] b = buffers[bufferOf[i]];
            int q = quoteAt[i];
            int len = parser.scanUnescapedLength(b, q);
            if (len < 0) {
                int raw = parser.rawLength(b, q);
                byte[] out = new byte[raw + StringParser.DESTINATION_PADDING];
                acc += parser.unescape(b, q, out) + out[0];
            }
        }
        return acc;
    }

    @Benchmark
    public long swarReusingOutput() {
        long acc = 0;
        for (int i = 0; i < count; i++) {
            byte[] b = buffers[bufferOf[i]];
            int q = quoteAt[i];
            acc += parser.unescape(b, q, scratch) + scratch[0];
        }
        return acc;
    }

    private static int rawLengthOld(byte[] buffer, int quoteIdx) {
        int i = quoteIdx + 1;
        int start = i;
        while (buffer[i] != '"') {
            if (buffer[i] == '\\') i += 2;
            else i++;
        }
        return i - start;
    }
}
