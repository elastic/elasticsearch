/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.xcontent;

import org.elasticsearch.benchmark.Utils;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Measures the cost of parsing a JSON document through {@link XContentParser}, decomposed into the access patterns
 * that stress different parts of a parser implementation: pure tokenization, field-name handling, string value
 * materialization, number value materialization, and full materialization into a {@code Map}.
 * <p>
 * Decomposition makes it easier to pinpoint a hot-spot: the tokenizer, field-name interning, string
 * decoding, etc.
 * <p>
 * Each {@code @Benchmark} method parses all documents in the corpus exactly once per invocation, so there is
 * deliberately no {@code @OperationsPerInvocation} here.
 * <p>
 * The corpus is selected with the {@code source} {@code @Param}; see {@code README.txt} next to the resource files
 * for what each document is meant to exercise. A value not matching a classpath resource is treated as a filesystem
 * path and loaded as NDJSON (one JSON object per line). Because the documents differ in size, the primary
 * {@code ns/op} score is not comparable across values of {@code source}, so it uses the {@code bytes} auxiliary
 * counter instead: JMH divides elapsed time by the counter, so that column reads as
 * <b>nanoseconds per source byte</b> — lower is better, and it is directly comparable across documents of different
 * sizes. In a throughput mode the same counter would read as bytes/sec.
 * <p>
 * The {@code mode} {@code @Param} controls how an NDJSON file is parsed. {@code split} creates one
 * {@link XContentParser} per document; {@code stream} creates one parser for the whole file and drains it
 * with {@link XContentParser#nextToken()}. For the committed single-document classpath corpus the two modes
 * are equivalent. {@code parseToMap} does not support {@code stream} and throws if that combination is selected.
 * <p>
 * Three forks are deliberate: a single fork cannot capture run-to-run JVM variance (JIT decisions, code layout and
 * GC ergonomics differ between launches), and this benchmark exists to support A/B comparisons where that variance
 * would otherwise be mistaken for signal. Measurement iterations are shortened to one second from JMH's ten-second
 * default; warmup remains at two.
 * <p>
 * The warmup budget (5 iterations x 2s = 10s per fork) is adequate for Jackson. It may not be for a Vector API
 * implementation, which needs C2 to intrinsify before it reaches steady state. When such an implementation is added,
 * verify convergence by inspecting the individual warmup iterations ({@code -v EXTRA}) rather than assuming.
 */
@Fork(3)
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class JsonParserBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    @Param({ "small_log_doc.json", "flat_log_batch.json", "escaped_unicode.json", "monitor_cluster_stats.json" })
    public String source;

    @Param({ "split" })
    public String mode;

    /**
     * The corpus to parse. In {@code split} mode each element is one JSON document. In {@code stream} mode the single
     * element is the entire file; the token-loop methods drain it in one pass without recreating the parser between
     * documents.
     */
    byte[][] docs;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        if (Files.exists(Path.of(source))) {
            if (mode.equals("split")) {
                try (var lines = Files.lines(Path.of(source))) {
                    docs = lines.filter(l -> l.isBlank() == false).map(l -> l.getBytes(StandardCharsets.UTF_8)).toArray(byte[][]::new);
                }
            } else {
                docs = new byte[][] { Files.readAllBytes(Path.of(source)) };
            }
        } else {
            try (var in = JsonParserBenchmark.class.getResourceAsStream(source)) {
                if (in == null) {
                    throw new IllegalArgumentException(source + " is not a valid source");
                }
                docs = new byte[][] { BytesReference.toBytes(Streams.readFully(in)) };
            }
        }
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

    private XContentParser createParser(byte[] data) throws IOException {
        return XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, data, 0, data.length);
    }

    /**
     * Full materialization into an ordered {@code Map}. Not supported in {@code stream} mode: a single parser
     * over an NDJSON file would stop after the first document.
     */
    @Benchmark
    public void parseToMap(Blackhole bh, ByteCounter counter) throws IOException {
        if (mode.equals("stream")) throw new UnsupportedOperationException();
        for (byte[] doc : docs) {
            counter.bytes += doc.length;
            try (XContentParser parser = createParser(doc)) {
                bh.consume(parser.mapOrdered());
            }
        }
    }

    /**
     * Tokenization only, materializing no values. Isolates the cost of finding token boundaries from the cost of
     * turning them into Java objects.
     */
    @Benchmark
    public void parseTokensOnly(Blackhole bh, ByteCounter counter) throws IOException {
        for (byte[] doc : docs) {
            counter.bytes += doc.length;
            try (XContentParser parser = createParser(doc)) {
                XContentParser.Token token;
                while ((token = parser.nextToken()) != null) {
                    bh.consume(token);
                }
            }
        }
    }

    /**
     * Field names only. Roughly half the tokens of a typical Elasticsearch document are field names. A "smart"
     * parser could treat them differently (e.g. runs them through a symbol table - see {@code ByteQuadsCanonicalizer})
     * so that a repeated key is interned. This benchmark exercises that.
     */
    @Benchmark
    public void parseFieldNames(Blackhole bh, ByteCounter counter) throws IOException {
        for (byte[] doc : docs) {
            counter.bytes += doc.length;
            try (XContentParser parser = createParser(doc)) {
                XContentParser.Token token;
                while ((token = parser.nextToken()) != null) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        bh.consume(parser.currentName());
                    }
                }
            }
        }
    }

    /**
     * String values via {@link XContentParser#optimizedText()}, not {@code text()}. The optimized accessor is the one
     * ingest uses ({@code KeywordFieldMapper} calls {@code optimizedTextOrNull()}); implementations should provide
     * optimized access to text, as the name of the function implies, for "simple" UTF-8 strings. For example,
     * handing back a view over the parser's input buffer with no copy and no UTF-16 decode.
     */
    @Benchmark
    public void parseStrings(Blackhole bh, ByteCounter counter) throws IOException {
        for (byte[] doc : docs) {
            counter.bytes += doc.length;
            try (XContentParser parser = createParser(doc)) {
                XContentParser.Token token;
                while ((token = parser.nextToken()) != null) {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        bh.consume(parser.optimizedText());
                    }
                }
            }
        }
    }

    /**
     * Number values, including the type classification the mappers branch on.
     */
    @Benchmark
    public void parseNumbers(Blackhole bh, ByteCounter counter) throws IOException {
        for (byte[] doc : docs) {
            counter.bytes += doc.length;
            try (XContentParser parser = createParser(doc)) {
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
}
