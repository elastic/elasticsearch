/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.stream.Stream;

/**
 * Grouping throughput over a keyword column: one pass that assigns every document to a group, the way a
 * {@code STATS ... BY keyword} does. A format that can hand back ordinals hashes each distinct value once
 * per page and maps the rest of the rows through an array; one that can only hand back bytes hashes every
 * row. That difference is what this measures, so the formats are compared on the work a consumer does
 * rather than on how fast bytes come off disk.
 *
 * <p>Pages are the unit the ordinal form is decided over, so {@code pageSize} is a parameter: a page too
 * small to hold repeats gives an ordinal form nothing to exploit.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class ColumnarStringGroupingBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    @Param(
        {
            "LOG_LEVEL",
            "HIT_COLOR",
            "MOSTLY_EMPTY",
            "HOSTNAME",
            "POD_NAME",
            "URL",
            "TRACE_ID",
            "CLUSTERED_SERVICE",
            "SORTED_HOSTNAME",
            "SORTED_POD_NAME",
            "CLUSTERED_POD_NAME" }
    )
    private StringData data;

    // COLUMNAR is the format as it ships, choosing its own shape. COLUMNAR_PLAIN and COLUMNAR_DICTIONARY
    // force one shape each, including on columns that would never earn it, and are here to explain a
    // result rather than to stand for the format.
    @Param({ "COLUMNAR", "LUCENE_SORTED", "ES819_SORTED", "ES95_SORTED", "ES819_BINARY" })
    private StringFormat format;

    @Param({ "16384" })
    private int pageSize;

    @Param({ "2000000" })
    private int docCount;

    /**
     * Which term a match looks for. The tracks ask all three: {@code WHERE SearchPhrase != ""} tests the
     * value most of the column holds, a filter on a particular phrase tests one that few documents carry,
     * and a term absent from the segment is the case a dictionary answers without reading anything.
     */
    @Param({ "COMMON", "RARE", "ABSENT" })
    private Selectivity selectivity;

    public enum Selectivity {
        COMMON,
        RARE,
        ABSENT
    }

    private BytesRef term;
    private BytesRef prefix;
    private BytesRef contained;
    private Path path;
    private Directory directory;
    private StringFormat.Column column;
    private long storedBytes;

    @Setup
    public void setup() throws IOException {
        path = Files.createTempDirectory("columnar-string-grouping");
        directory = new MMapDirectory(path);
        final BytesRef[] values = data.generate(docCount, new Random(7));
        storedBytes = format.write(directory, values);
        column = format.open(directory, docCount, pageSize);
        term = switch (selectivity) {
            // The most frequent value, the least frequent one, and one the column does not hold.
            case COMMON -> mostFrequent(values);
            case RARE -> values[values.length - 1];
            case ABSENT -> new BytesRef("\uffff absent \uffff");
        };
        final int keep = Math.max(1, Math.min(term.length, 4));
        prefix = new BytesRef(Arrays.copyOfRange(term.bytes, term.offset, term.offset + keep));
        // Taken from the middle of the term, so it is a match no prefix or bisection could have found and
        // every value has to be searched for it.
        final int from = term.length / 3;
        final int to = Math.max(from + 1, term.length - term.length / 3);
        contained = term.length == 0
            ? new BytesRef("")
            : new BytesRef(Arrays.copyOfRange(term.bytes, term.offset + from, term.offset + to));
        // A shape that matches fewer documents than it should makes a format look instant, which is the
        // easiest way to misread a filter benchmark, and one that matches a term the column does not hold is
        // the same mistake read from the other side. So every shape that answers with a count is checked
        // against the values themselves, the absent term included, where the count to expect is zero. Here
        // rather than in a setup of its own, whose order is not defined.
        //
        // The traversal shapes are not checked: group, aggregate, scan and readPerDocument each answer with
        // an accumulator whose meaning is the consumer's rather than the column's, so there is nothing to
        // hold them to without fixing what every format has to accumulate.
        check("term", term, column.queryTerm(term), values, ColumnarStringGroupingBenchmark::equalTo);
        check("term", term, column.matchTerm(term), values, ColumnarStringGroupingBenchmark::equalTo);
        check("prefix", prefix, column.queryPrefix(prefix), values, ColumnarStringGroupingBenchmark::startsWith);
        check("prefix", prefix, column.matchPrefix(prefix), values, ColumnarStringGroupingBenchmark::startsWith);
        check("contains", contained, column.matchContains(contained), values, ColumnarStringGroupingBenchmark::contains);
    }

    /** What a shape answered, against what the values say it should have. */
    private void check(String shape, BytesRef probe, long matched, BytesRef[] values, BiPredicate<BytesRef, BytesRef> holds) {
        long expected = 0;
        for (BytesRef value : values) {
            if (holds.test(value, probe)) {
                expected++;
            }
        }
        if (matched != expected) {
            throw new AssertionError(
                shape + " [" + probe.utf8ToString() + "] matched " + matched + " of " + docCount + ", expected " + expected
            );
        }
    }

    private static boolean equalTo(BytesRef value, BytesRef probe) {
        return value.bytesEquals(probe);
    }

    private static boolean startsWith(BytesRef value, BytesRef probe) {
        return value.length >= probe.length
            && Arrays.equals(
                value.bytes,
                value.offset,
                value.offset + probe.length,
                probe.bytes,
                probe.offset,
                probe.offset + probe.length
            );
    }

    /** By the bytes, as the column searches: a probe carved out of a term can split a character. */
    private static boolean contains(BytesRef value, BytesRef probe) {
        if (probe.length == 0) {
            return true;
        }
        for (int at = 0; at + probe.length <= value.length; at++) {
            if (Arrays.equals(
                value.bytes,
                value.offset + at,
                value.offset + at + probe.length,
                probe.bytes,
                probe.offset,
                probe.offset + probe.length
            )) {
                return true;
            }
        }
        return false;
    }

    @Benchmark
    public void group(Blackhole bh) throws IOException {
        bh.consume(column.group());
    }

    /** A term match, the shape of a filter on a keyword field. */
    @Benchmark
    public void matchTerm(Blackhole bh) throws IOException {
        bh.consume(column.matchTerm(term));
    }

    @Benchmark
    public void matchContains(Blackhole bh) throws IOException {
        bh.consume(column.matchContains(contained));
    }

    /** The term as a query through a searcher, which is the path a filter actually takes. */
    @Benchmark
    public void queryTerm(Blackhole bh) throws IOException {
        bh.consume(column.queryTerm(term));
    }

    /** The prefix as a query through a searcher. */
    @Benchmark
    public void queryPrefix(Blackhole bh) throws IOException {
        bh.consume(column.queryPrefix(prefix));
    }

    /** A prefix match, the shape of {@code LIKE "x*"}. */
    @Benchmark
    public void matchPrefix(Blackhole bh) throws IOException {
        bh.consume(column.matchPrefix(prefix));
    }

    /** Format cost alone: every value read, nothing hashed. */
    /** One value at a time through the doc values API, the shape a fetch or a sort asks for. */
    @Benchmark
    public void readPerDocument(Blackhole bh) throws IOException {
        bh.consume(column.readPerDocument());
    }

    @Benchmark
    public void scan(Blackhole bh) throws IOException {
        bh.consume(column.scan());
    }

    /** The aggregation route: a counter array over stable ordinals, with a hash only where there are none. */
    @Benchmark
    public void aggregate(Blackhole bh) throws IOException {
        bh.consume(column.aggregate());
    }

    private static BytesRef mostFrequent(BytesRef[] values) {
        final Map<BytesRef, Integer> counts = new HashMap<>();
        for (BytesRef value : values) {
            counts.merge(value, 1, Integer::sum);
        }
        return counts.entrySet().stream().max(Map.Entry.comparingByValue()).orElseThrow().getKey();
    }

    @TearDown
    public void tearDown() throws IOException {
        // What the column occupies on disk, reported through the log rather than as a benchmark of its
        // own: a benchmark reports how long its method took, so returning the count would measure reading
        // a field.
        System.out.println("footprint " + data + " " + format + " " + column.shape() + " " + storedBytes + " bytes");
        column.close();
        directory.close();
        try (Stream<Path> files = Files.walk(path)) {
            files.sorted(Comparator.reverseOrder()).forEach(file -> {
                try {
                    Files.deleteIfExists(file);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            });
        }
    }
}
