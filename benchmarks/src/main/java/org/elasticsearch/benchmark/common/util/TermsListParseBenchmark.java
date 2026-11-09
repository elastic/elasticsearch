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
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
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
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Parses the JSON array of a large {@code terms} query the way {@code TermsQueryBuilder.parseValues} does, once per term turning the
 * parser's {@link CharBuffer} into a {@link BytesRef}, to see what the conversion costs next to the JSON parsing it is part of. See
 * {@link CharBufferToBytesRefBenchmark} for the conversion on its own.
 * <ul>
 * <li>{@code tokenizeOnly}: no conversion, keeps the {@link CharBuffer}s, so only what the JSON parser costs.</li>
 * <li>{@code newBytesRef}, {@code exactTwoPass}, {@code exactScratchCopy}: as in {@link CharBufferToBytesRefBenchmark}.</li>
 * </ul>
 * Each operation is the parse of one whole list, so scores are per list, in milliseconds.
 */
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class TermsListParseBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    // Just under the default index.max_terms_count of 65536, as in LargeTermsFilterHeapPressureIT.
    private static final int MAX_TERMS = 62_000;
    /** Caps the chars per list, so that lists of long terms are not gigabytes. */
    private static final int MAX_CHARS = 4_000_000;

    /** Length in chars of each term, so a surrogate pair counts as 2. The last is {@code IndexWriter.MAX_TERM_LENGTH}. */
    @Param({ "20", "32", "64", "512", "1024", "16384", "32766" })
    public int length;

    /** ascii: 1 byte per char. latin: 2. cjk: 3. emoji: surrogate pairs. mixed: mostly ASCII with occasional chars of the others. */
    @Param({ "ascii", "latin", "cjk", "emoji", "mixed" })
    public String charset;

    /** Terms in the list: {@link #MAX_TERMS}, or fewer when the terms are long. */
    private int terms;
    private byte[] body;
    private byte[] scratch;

    @Setup
    public void setup() {
        Random random = new Random(1);
        terms = Math.min(MAX_TERMS, MAX_CHARS / length);
        StringBuilder json = new StringBuilder("{\"id\":[");
        for (int i = 0; i < terms; i++) {
            if (i > 0) {
                json.append(',');
            }
            json.append('"');
            appendTerm(json, random);
            json.append('"');
        }
        json.append("]}");
        body = json.toString().getBytes(StandardCharsets.UTF_8);
        scratch = new byte[UnicodeUtil.maxUTF8Length(length)];
    }

    private void appendTerm(StringBuilder out, Random random) {
        int chars = 0;
        while (chars < length) {
            String kind = charset.equals("mixed") ? mixedKind(random) : charset;
            switch (kind) {
                case "ascii" -> {
                    out.append((char) ('a' + random.nextInt(26)));
                    chars++;
                }
                case "cjk" -> {
                    out.append((char) (0x4E00 + random.nextInt(0x1000)));
                    chars++;
                }
                case "latin" -> {
                    out.append((char) (0xE0 + random.nextInt(0x1F)));
                    chars++;
                }
                case "emoji" -> {
                    if (chars + 1 < length) {
                        out.appendCodePoint(0x1F600 + random.nextInt(0x40));
                        chars += 2;
                    } else {
                        // a pair does not fit in the last char
                        out.append('a');
                        chars++;
                    }
                }
                default -> throw new IllegalArgumentException("unknown charset [" + kind + "]");
            }
        }
    }

    private static String mixedKind(Random random) {
        int roll = random.nextInt(100);
        if (roll < 85) {
            return "ascii";
        } else if (roll < 92) {
            return "latin";
        } else if (roll < 97) {
            return "cjk";
        }
        return "emoji";
    }

    @Benchmark
    public List<Object> tokenizeOnly() throws IOException {
        return parse(0);
    }

    @Benchmark
    public List<Object> newBytesRef() throws IOException {
        return parse(1);
    }

    @Benchmark
    public List<Object> exactTwoPass() throws IOException {
        return parse(2);
    }

    @Benchmark
    public List<Object> exactScratchCopy() throws IOException {
        return parse(3);
    }

    @Benchmark
    public List<Object> allocThenTrim() throws IOException {
        return parse(6);
    }

    /** The production parser, so that the variants above can be checked against what actually runs. */
    @Benchmark
    public TermsQueryBuilder productionFromXContent() throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, body)) {
            parser.nextToken();
            return TermsQueryBuilder.fromXContent(parser);
        }
    }

    @Benchmark
    public List<Object> asciiFastThenScratch() throws IOException {
        return parse(4);
    }

    @Benchmark
    public List<Object> fusedAsciiCopy() throws IOException {
        return parse(5);
    }

    private List<Object> parse(int mode) throws IOException {
        List<Object> values = new ArrayList<>();
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, body)) {
            while (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                // skip to the list
            }
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                CharBuffer value = (CharBuffer) parser.objectBytes();
                values.add(switch (mode) {
                    case 0 -> value;
                    case 1 -> new BytesRef(value);
                    case 2 -> BytesRefs.toExactSizedBytesRef(value);
                    case 3 -> {
                        int bytes = UnicodeUtil.UTF16toUTF8(value, 0, value.length(), scratch);
                        yield new BytesRef(Arrays.copyOf(scratch, bytes));
                    }
                    case 4 -> CharBufferToBytesRefBenchmark.asciiFastThenScratch(value, scratch);
                    case 5 -> CharBufferToBytesRefBenchmark.fusedAsciiCopy(value, scratch);
                    case 6 -> {
                        byte[] bytes = new byte[UnicodeUtil.maxUTF8Length(value.length())];
                        int used = UnicodeUtil.UTF16toUTF8(value, 0, value.length(), bytes);
                        yield new BytesRef(Arrays.copyOf(bytes, used));
                    }
                    default -> throw new IllegalArgumentException("unknown mode [" + mode + "]");
                });
            }
        }
        return values;
    }
}
