/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.StringBlockSink;
import org.elasticsearch.columnar.string.StringColumnReader;
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
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures the cost of codec read profiling on ColumNAR string reads of one field in one segment.
 *
 * <p>{@code mode} selects what a reader records:
 * <ul>
 *   <li>{@code off}: no scope is bound. This is the cost of a query that is not profiled.</li>
 *   <li>{@code counters}: counters only.</li>
 *   <li>{@code timers}: counters and elapsed-time timers, as a profiled request with no breakdown records.</li>
 *   <li>{@code cpu}: also CPU time for the timers that declare it.</li>
 *   <li>{@code narrow}: timers through a detail collector of the parent collector, as a reader of one selected segment
 *   and field records.</li>
 * </ul>
 *
 * <p>{@link #pageLoad} reads pages, {@link #sparseRandom} reads single documents, {@link #openReaders} opens
 * {@code readerOpens} readers, and {@link #skipRange} runs a range match. {@code ordinals} sets the value pattern:
 * {@code CLUSTERED}, {@code RANDOM} or {@code CYCLIC}.
 *
 * <p>Index writing and the frontend are excluded: the scope sends every reader to one collector, with no
 * {@code CodecReadAttribution} and no circuit breaker. The cost of many selected details with few calls each is not
 * measured.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class ColumnarProfilingOverheadBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    private static final String FIELD = StringFormat.FIELD;
    private static final int PAGE = 1024;
    private static final long SEED = 17;

    @Param({ "PLAIN", "DICTIONARY" })
    private String layout;

    @Param({ "off", "counters", "timers", "cpu", "narrow" })
    private String mode;

    @Param("CLUSTERED")
    private String ordinals;

    @Param("200000")
    private int docCount;

    @Param("64")
    private int readerOpens;

    private boolean counters;
    private boolean timers;
    private boolean cpuTime;
    private boolean detail;
    private Path tempPath;
    private Directory directory;
    private DirectoryReader reader;
    private LeafReader leaf;
    private int[] allDocs;
    private int[] sparseDocs;
    private BytesRef rangeLow;
    private BytesRef rangeHigh;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        counters = mode.equals("counters") || mode.equals("timers") || mode.equals("cpu") || mode.equals("narrow");
        timers = mode.equals("timers") || mode.equals("cpu") || mode.equals("narrow");
        cpuTime = mode.equals("cpu");
        detail = mode.equals("narrow");
        if (counters == false && mode.equals("off") == false) {
            throw new IllegalArgumentException("mode must be off, counters, timers, cpu or narrow, got [" + mode + "]");
        }
        if (counters && ProfilingScope.AVAILABLE == false) {
            throw new IllegalStateException("mode=" + mode + " needs a checkout with read profiling");
        }
        final StringFormat format = switch (layout) {
            case "PLAIN" -> StringFormat.COLUMNAR_PLAIN;
            case "DICTIONARY" -> StringFormat.COLUMNAR_DICTIONARY;
            default -> throw new IllegalArgumentException("layout must be PLAIN or DICTIONARY, got [" + layout + "]");
        };
        final BytesRef[] values = values();
        tempPath = Files.createTempDirectory("columnar-profiling-overhead-");
        directory = new MMapDirectory(tempPath);
        format.writeSegments(directory, values, Math.max(1, docCount / 4), true);
        reader = DirectoryReader.open(directory);
        if (reader.leaves().size() != 1) {
            throw new IllegalStateException("expected one segment, found " + reader.leaves().size());
        }
        leaf = reader.leaves().getFirst().reader();
        final StringColumnReader column = ((ColumnarStringBinaryDocValues) leaf.getBinaryDocValues(FIELD)).reader();
        if (column.hasDictionary() != layout.equals("DICTIONARY")) {
            throw new IllegalStateException(format + " was written as the other layout, so the numbers would describe it");
        }
        if (layout.equals("DICTIONARY") && ordinals.equals("CLUSTERED") == false) {
            final long inflated = ProfilingScope.inflatedBlocks(leaf, FIELD);
            if (inflated >= 0 && (inflated > 0) != ordinals.equals("CYCLIC")) {
                throw new IllegalStateException(
                    "ordinals="
                        + ordinals
                        + " was written with "
                        + (inflated > 0 ? "compressed" : "uncompressed")
                        + " blocks, so the numbers would describe the other"
                );
            }
        }
        allDocs = new int[docCount];
        for (int doc = 0; doc < docCount; doc++) {
            allDocs[doc] = doc;
        }
        final Random random = new Random(SEED);
        sparseDocs = allDocs.clone();
        for (int doc = 0; doc < docCount; doc++) {
            sparseDocs[doc] = random.nextInt(docCount);
        }
        Arrays.sort(sparseDocs);
        final BytesRef[] sorted = values.clone();
        Arrays.sort(sorted);
        rangeLow = sorted[docCount / 2];
        rangeHigh = sorted[docCount / 2 + docCount / 1000];
    }

    /**
     * CLUSTERED: pod names in sorted regions. RANDOM: 50 terms in random order, which the writer stores as plain ordinal
     * blocks. CYCLIC: 50 terms in a fixed rotation, which the writer stores as compressed ordinal blocks.
     */
    private BytesRef[] values() {
        return switch (ordinals) {
            case "CLUSTERED" -> StringData.CLUSTERED_POD_NAME.generate(docCount, new Random(SEED));
            case "RANDOM", "CYCLIC" -> {
                final Random random = new Random(SEED);
                final BytesRef[] terms = new BytesRef[docCount];
                for (int doc = 0; doc < docCount; doc++) {
                    final int term = ordinals.equals("RANDOM") ? random.nextInt(50) : doc % 50;
                    terms[doc] = new BytesRef(String.format(Locale.ROOT, "term-%06d", term));
                }
                yield terms;
            }
            default -> throw new IllegalArgumentException("ordinals must be CLUSTERED, RANDOM or CYCLIC, got [" + ordinals + "]");
        };
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        reader.close();
        directory.close();
        IOUtils.rm(tempPath);
    }

    @Benchmark
    public void pageLoad(Blackhole bh) throws IOException {
        final CountingSink sink = new CountingSink();
        try (ProfilingScope ignored = ProfilingScope.enter(counters, timers, cpuTime, detail)) {
            final StringColumnReader column = ((ColumnarStringBinaryDocValues) leaf.getBinaryDocValues(FIELD)).reader();
            for (int from = 0; from < docCount; from += PAGE) {
                if (column.readBlock(allDocs, from, Math.min(PAGE, docCount - from), sink) == false) {
                    throw new AssertionError("a layout declined a page");
                }
            }
        }
        bh.consume(sink.values);
    }

    @Benchmark
    public void sparseRandom(Blackhole bh) throws IOException {
        try (ProfilingScope ignored = ProfilingScope.enter(counters, timers, cpuTime, detail)) {
            final BinaryDocValues values = leaf.getBinaryDocValues(FIELD);
            int previous = -1;
            for (int i = 0; i < sparseDocs.length; i += 100) {
                final int doc = sparseDocs[i];
                if (doc != previous && values.advanceExact(doc)) {
                    bh.consume(values.binaryValue());
                }
                previous = doc;
            }
        }
    }

    @Benchmark
    public void openReaders(Blackhole bh) throws IOException {
        try (ProfilingScope ignored = ProfilingScope.enter(counters, timers, cpuTime, detail)) {
            for (int open = 0; open < readerOpens; open++) {
                bh.consume(leaf.getBinaryDocValues(FIELD));
            }
        }
    }

    @Benchmark
    public void skipRange(Blackhole bh) throws IOException {
        try (ProfilingScope ignored = ProfilingScope.enter(counters, timers, cpuTime, detail)) {
            final StringColumnReader column = ((ColumnarStringBinaryDocValues) leaf.getBinaryDocValues(FIELD)).reader();
            final DocIdSetIterator matches = column.matchRange(rangeLow, true, rangeHigh, true);
            long count = 0;
            for (int doc = matches.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = matches.nextDoc()) {
                count++;
            }
            bh.consume(count);
        }
    }

    private static final class CountingSink implements StringBlockSink {
        long values;

        @Override
        public void appendOrdinals(int[] ordinals, int valueCount, int[] valueCounts, int docCount, BytesRef[] dictionary, int size) {
            values += valueCount;
        }

        @Override
        public Values values(int valueCount, int[] valueCounts, int docCount) {
            values += valueCount;
            return new Values() {
                @Override
                public void append(BytesRef value) {}

                @Override
                public void finish() {}

                @Override
                public void close() {}
            };
        }
    }
}
