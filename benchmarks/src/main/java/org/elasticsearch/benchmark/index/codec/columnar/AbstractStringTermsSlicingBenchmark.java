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
import org.openjdk.jmh.annotations.Level;
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
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Writes one column, opens it, and runs a {@code terms} query over it. The data shapes, the layouts and the
 * document counts are the ones {@link AbstractStringRangeSlicingBenchmark} uses, so a terms result sits beside
 * a range result on the same fixture.
 *
 * <p>The axis is the number of query terms rather than selectivity. A range takes selectivity as an input,
 * since bounds can be chosen to span whatever share of the values is wanted, while a term set only decides how
 * many terms it asks for and what it matches follows from that. Subclasses declare their own {@code @Param}
 * fields, as the range benchmarks do, because JMH does not allow a subclass to override an inherited one.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public abstract class AbstractStringTermsSlicingBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    /**
     * Whether the query asks for terms the column holds or for terms it does not. The term count decides what
     * resolution costs, but with terms the column holds it also decides how many documents match, and one
     * number cannot separate the two. Absent terms hold the match count at zero.
     */
    public enum Probe {
        PRESENT,
        ABSENT
    }

    @Param({ "100000", "1000000" })
    int numDocs;

    private Path path;
    private Directory directory;
    private StringFormat.Column column;
    private NavigableSet<BytesRef> terms;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        final BytesRef[] values = data().generate(numDocs, new Random(42));

        // NOTE: the terms are spread across the distinct values rather than taken from the front, so a set of
        // them reaches the whole dictionary and no bisection is favoured by where its target sits.
        final NavigableSet<BytesRef> distinct = new TreeSet<>();
        for (BytesRef value : values) {
            distinct.add(BytesRef.deepCopyOf(value));
        }
        if (distinct.size() < queryTerms()) {
            throw new IllegalStateException(data() + " holds " + distinct.size() + " distinct values, fewer than " + queryTerms());
        }
        final BytesRef[] ordered = distinct.toArray(new BytesRef[0]);
        terms = new TreeSet<>();
        for (int i = 0; i < queryTerms(); i++) {
            final BytesRef held = ordered[(int) ((long) i * ordered.length / queryTerms())];
            // NOTE: a trailing null byte names a value just past one the column holds, which no value can
            // equal, so an absent term still resolves against the same part of the dictionary.
            terms.add(probe() == Probe.PRESENT ? held : withTrailingZero(held));
        }

        path = Files.createTempDirectory("columnar-string-terms-slicing");
        directory = new MMapDirectory(path);
        format().write(directory, values);
        column = format().open(directory, numDocs, 1024);
        validateLayout(column);
    }

    private static BytesRef withTrailingZero(BytesRef value) {
        return new BytesRef(Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length + 1));
    }

    abstract StringData data();

    abstract StringFormat format();

    abstract int queryTerms();

    abstract Probe probe();

    /**
     * Validates the layout the column earned at write time. Throwing here is the JMH idiom for excluding an
     * invalid parameter combination from the run: a column whose layout does not match the benchmark's intent
     * would otherwise produce a mislabeled measurement.
     */
    abstract void validateLayout(StringFormat.Column column) throws IOException;

    @Benchmark
    public void termsQuery(Blackhole bh) throws IOException {
        bh.consume(column.queryTerms(terms));
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
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
