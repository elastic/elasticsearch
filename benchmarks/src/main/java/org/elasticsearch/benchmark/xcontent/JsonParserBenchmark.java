/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.xcontent;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Measures the cost of parsing a JSON document through {@link XContentParser}, decomposed into the access patterns
 * that stress different parts of a parser implementation: pure tokenization, field-name handling, string value
 * materialization, number value materialization, and full materialization into a {@code Map}.
 * <p>
 * The point of the decomposition is attribution. A change to the tokenizer, to field-name interning, or to string
 * decoding shows up in a different subset of these methods, and an aggregate "parse a document" number cannot tell
 * them apart.
 * <p>
 * Each {@code @Benchmark} method performs <b>exactly one</b> parse of exactly one document, so there is deliberately
 * no {@code @OperationsPerInvocation} here. An earlier version of this benchmark carried
 * {@code @OperationsPerInvocation(25000)} on a single-parse method, which made every absolute number it produced
 * 25,000 times too fast and invalidated a whole investigation. If a future method genuinely loops N times, the
 * annotation must be added with the real N; otherwise it must stay absent.
 * <p>
 * The corpus is selected with the {@code source} {@code @Param}; see {@code README.txt} next to the resource files
 * for what each document is meant to exercise. Because the documents differ in size, the primary {@code ns/op} score
 * is not comparable across values of {@code source}: use the {@code bytes} auxiliary counter instead. In
 * {@link Mode#AverageTime} JMH divides elapsed time by the counter, so that column reads as
 * <b>nanoseconds per source byte</b> — lower is better, and it is directly comparable across documents of different
 * sizes. (In a throughput mode the same counter would read as bytes/sec.)
 * <p>
 * Three forks are deliberate: a single fork cannot capture run-to-run JVM variance (JIT decisions, code layout and
 * GC ergonomics differ between launches), and this benchmark exists to support A/B comparisons where that variance
 * would otherwise be mistaken for signal. Iterations are shortened to two seconds from JMH's ten-second default.
 * <p>
 * The full matrix is five methods over six documents at three forks, which still takes roughly three quarters of an
 * hour. When iterating, narrow it — e.g.
 * {@code run --args 'JsonParserBenchmark.parseStrings -psource=monitor_cluster_stats.json'}.
 * <p>
 * The warmup budget (5 iterations x 2s = 10s per fork) is adequate for Jackson. It may not be for a Vector API
 * implementation, which needs C2 to intrinsify before it reaches steady state. When such an implementation is added,
 * verify convergence by inspecting the individual warmup iterations ({@code -v EXTRA}) rather than assuming.
 * <p>
 * Complementary to {@link OptimizedTextBenchmark}, which measures end-to-end indexing of {@code match_only_text}
 * through {@code MapperService}. That one includes mapping and field construction; this one isolates the parser.
 * Neither subsumes the other.
 */
@Fork(3)
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class JsonParserBenchmark {

    @Param(
        {
            "small_log_doc.json",
            "flat_log_batch.json",
            "escaped_unicode.json",
            "monitor_cluster_stats.json",
            "monitor_index_stats.json",
            "monitor_node_stats.json" }
    )
    public String source;

    private byte[] sourceBytes;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        sourceBytes = BytesReference.toBytes(Streams.readFully(JsonParserBenchmark.class.getResourceAsStream(source)));
    }

    /**
     * Reports the number of source bytes fed to the parser, as an extra score column named {@code bytes}. JMH
     * normalizes auxiliary {@link AuxCounters.Type#OPERATIONS} counters the same way it normalizes the primary
     * score, so under {@link Mode#AverageTime} the column is nanoseconds per source byte rather than a byte count.
     * That is the number to compare across {@code source} documents of different sizes.
     */
    @AuxCounters(AuxCounters.Type.OPERATIONS)
    @State(Scope.Thread)
    public static class ByteCounter {
        public long bytes;

        @Setup(Level.Iteration)
        public void reset() {
            bytes = 0;
        }
    }

    /**
     * The single place a parser is created. Step 3 of the SIMD JSON investigation adds a {@code @Param} over the
     * parser implementation so that alternatives are measured in the same fork as the baseline; keeping creation
     * here makes that a one-line change instead of a five-method edit.
     * <p>
     * Parsing is from a {@code byte[]}, not from an {@code InputStream}: the stream overload forces any
     * implementation that needs the whole document in memory to buffer it first, a whole-document copy that Jackson
     * does not pay and that would silently bias a comparison. The {@code byte[]} overload is also the one that
     * reaches {@code ESUTF8StreamJsonParser}, which is what production ingest runs on.
     */
    private XContentParser createParser() throws IOException {
        return XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, sourceBytes, 0, sourceBytes.length);
    }

    /**
     * Full materialization into an ordered {@code Map}. The pattern the original version of this benchmark measured.
     */
    @Benchmark
    public void parseToMap(Blackhole bh, ByteCounter counter) throws IOException {
        counter.bytes += sourceBytes.length;
        try (XContentParser parser = createParser()) {
            bh.consume(parser.mapOrdered());
        }
    }

    /**
     * Tokenization only, materializing no values. Isolates the cost of finding token boundaries from the cost of
     * turning them into Java objects, which is the axis a SIMD stage1 actually changes.
     */
    @Benchmark
    public void parseTokensOnly(Blackhole bh, ByteCounter counter) throws IOException {
        counter.bytes += sourceBytes.length;
        try (XContentParser parser = createParser()) {
            XContentParser.Token token;
            while ((token = parser.nextToken()) != null) {
                bh.consume(token);
            }
        }
    }

    /**
     * Field names only. Roughly half the tokens of a typical Elasticsearch document are field names, and Jackson
     * runs them through a {@code ByteQuadsCanonicalizer} symbol table so that a repeated key is interned rather than
     * re-decoded. A parser without an equivalent pays full decode cost per occurrence, which only this method shows.
     */
    @Benchmark
    public void parseFieldNames(Blackhole bh, ByteCounter counter) throws IOException {
        counter.bytes += sourceBytes.length;
        try (XContentParser parser = createParser()) {
            XContentParser.Token token;
            while ((token = parser.nextToken()) != null) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    bh.consume(parser.currentName());
                }
            }
        }
    }

    /**
     * String values via {@link XContentParser#optimizedText()}, not {@code text()}. The optimized accessor is the one
     * ingest uses ({@code KeywordFieldMapper} calls {@code optimizedTextOrNull()}); it hands back a view over the
     * parser's input buffer with no copy and no UTF-16 decode. Measuring {@code text()} would establish a baseline
     * slower than production and overstate any alternative implementation's win.
     */
    @Benchmark
    public void parseStrings(Blackhole bh, ByteCounter counter) throws IOException {
        counter.bytes += sourceBytes.length;
        try (XContentParser parser = createParser()) {
            XContentParser.Token token;
            while ((token = parser.nextToken()) != null) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    bh.consume(parser.optimizedText());
                }
            }
        }
    }

    /**
     * Number values, including the type classification the mappers branch on.
     */
    @Benchmark
    public void parseNumbers(Blackhole bh, ByteCounter counter) throws IOException {
        counter.bytes += sourceBytes.length;
        try (XContentParser parser = createParser()) {
            XContentParser.Token token;
            while ((token = parser.nextToken()) != null) {
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    bh.consume(parser.numberType());
                    bh.consume(parser.numberValue());
                }
            }
        }
    }
}
