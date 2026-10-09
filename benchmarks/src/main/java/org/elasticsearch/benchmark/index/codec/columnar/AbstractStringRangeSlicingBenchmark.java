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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Shared harness for the string range slicing benchmarks: generates the data, derives rank-based
 * range bounds so selectivity is exact and stable across runs, writes and opens the column, and
 * runs the range query. Subclasses declare their own {@code @Param} fields (JMH does not allow a
 * subclass to override inherited {@code @Param} values) and surface them through the abstract
 * accessors.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public abstract class AbstractStringRangeSlicingBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    @Param({ "100000", "1000000" })
    int numDocs;

    private Path path;
    private Directory directory;
    private StringFormat.Column column;
    private BytesRef lower;
    private BytesRef upper;
    private final Random random;

    public AbstractStringRangeSlicingBenchmark() {
        this.random = new Random(42);
    }

    @Setup(Level.Trial)
    public void setup() throws IOException {
        final BytesRef[] values = data().generate(numDocs, this.random);

        final BytesRef[] sorted = values.clone();
        Arrays.sort(sorted);
        final int loRank = (int) (numDocs * rangeStart());
        final int hiRank = Math.min(numDocs - 1, loRank + (int) (numDocs * selectivity()));
        if (hiRank > loRank) {
            lower = BytesRef.deepCopyOf(sorted[loRank]);
            upper = BytesRef.deepCopyOf(sorted[hiRank]);
        } else {
            // NOTE: appending a null byte creates a value just past sorted[loRank] that cannot
            // exist in the dataset, so the range has 0 matches without triggering isEmptyRange()
            // (which exits before touching the column and defeats the measurement).
            final BytesRef lo = sorted[loRank];
            final byte[] buf = Arrays.copyOfRange(lo.bytes, lo.offset, lo.offset + lo.length + 1);
            lower = new BytesRef(buf);
            upper = BytesRef.deepCopyOf(lower);
        }

        path = Files.createTempDirectory("columnar-string-range-slicing");
        directory = new MMapDirectory(path);
        format().write(directory, values);
        column = format().open(directory, numDocs, 1024);
        validateLayout(column);
    }

    abstract StringData data();

    abstract double selectivity();

    abstract double rangeStart();

    abstract StringFormat format();

    /**
     * Validates the layout the column earned at write time. Throwing here is the JMH idiom for
     * excluding an invalid parameter combination from the run: a column whose layout does not
     * match the benchmark's intent would otherwise produce a mislabeled measurement.
     */
    abstract void validateLayout(StringFormat.Column column) throws IOException;

    @Benchmark
    public void rangeQuery(Blackhole bh) throws IOException {
        bh.consume(column.queryRange(lower, upper));
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
