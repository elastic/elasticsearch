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
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * What a keyword column costs to write, and to merge once written.
 *
 * <p>Both are measured because they are paid differently: writing happens once per refresh on the indexing
 * path, while merging happens repeatedly and reads everything back. A format that writes quickly by
 * deferring work to the merge has not saved anything.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
public class ColumnarStringIngestBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    @Param({ "HIT_COLOR", "MOSTLY_EMPTY", "HOSTNAME", "POD_NAME", "URL", "TRACE_ID", "SORTED_HOSTNAME" })
    private StringData data;

    @Param({ "COLUMNAR", "LUCENE_SORTED", "ES819_SORTED", "ES95_SORTED", "ES819_BINARY" })
    private StringFormat format;

    @Param({ "1000000" })
    private int docCount;

    /** Documents per segment, so a merge has several of them to read. */
    @Param({ "100000" })
    private int segmentSize;

    private BytesRef[] values;
    private Path path;
    private Path mergePath;
    private Directory mergeDirectory;

    @Setup(Level.Trial)
    public void generate() {
        values = data.generate(docCount, new Random(7));
    }

    /** One segment per {@code segmentSize} documents, with no merge. */
    @Benchmark
    public void write(Blackhole bh) throws IOException {
        try (Directory directory = open()) {
            bh.consume(format.writeSegments(directory, values, segmentSize, false));
        } finally {
            clean();
        }
    }

    /** The same, then merged down to one segment; the difference is what the merge costs. */
    @Benchmark
    public void writeAndMerge(Blackhole bh) throws IOException {
        try (Directory directory = open()) {
            bh.consume(format.writeSegments(directory, values, segmentSize, true));
        } finally {
            clean();
        }
    }

    /**
     * The merge on its own, over segments already written. What a merge costs is the question a format is
     * asked repeatedly, and writing the segments alongside it hides the answer inside a much larger number.
     */
    @Benchmark
    public void merge(Blackhole bh) throws IOException {
        bh.consume(format.mergeSegments(mergeDirectory));
    }

    /** Fresh segments for every measured merge, since merging them consumes them. */
    @Setup(Level.Invocation)
    public void buildSegments() throws IOException {
        mergePath = Files.createTempDirectory("columnar-string-merge");
        mergeDirectory = new MMapDirectory(mergePath);
        format.writeSegments(mergeDirectory, values, segmentSize, false);
        final int segments = StringFormat.segmentCount(mergeDirectory);
        if (segments < 2) {
            throw new AssertionError("a merge needs segments to merge, got " + segments);
        }
    }

    @TearDown(Level.Invocation)
    public void dropSegments() throws IOException {
        mergeDirectory.close();
        delete(mergePath);
    }

    private Directory open() throws IOException {
        path = Files.createTempDirectory("columnar-string-ingest");
        return new MMapDirectory(path);
    }

    private void clean() throws IOException {
        delete(path);
    }

    private static void delete(Path path) throws IOException {
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

    @TearDown
    public void tearDown() {}
}
