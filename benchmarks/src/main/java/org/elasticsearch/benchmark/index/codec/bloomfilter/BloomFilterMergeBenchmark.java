/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.bloomfilter;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.bloomfilter.ES94BloomFilterDocValuesFormat;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Benchmarks the optimized bloom filter segment merge (mergeOptimized/orRegion).
 *
 * Documents contain only a binary doc-values _id field (no postings, no stored fields)
 * to isolate bloom filter merge cost from other Lucene merge work.
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class BloomFilterMergeBenchmark {

    private static final String ID_FIELD = "_id";

    public enum DirType {
        MMAP,
        NIOFS
    }

    static {
        Utils.configureBenchmarkLogging();
    }

    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder().include(BloomFilterMergeBenchmark.class.getSimpleName()).build();
        new Runner(options).run();
    }

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        @Param({ "MMAP", "NIOFS" })
        private DirType dirType;

        @Param({ "16", "64" })
        private int numSegments;

        @Param({ "50000" })
        private int docsPerSegment;

        @Param({ "65536" })
        private int filterSizeBytes;

        @Param({ "false", "true" })
        private boolean mixedSizes;

        private Directory directory;
        private final Supplier<IndexWriterConfig> mergeConfig = () -> createMergeConfig(filterSizeBytes, mixedSizes);

        @Setup(Level.Invocation)
        public void setup() throws IOException {
            directory = openDirectory(dirType);
            createIndex(directory, numSegments, docsPerSegment, filterSizeBytes, mixedSizes);
        }

        @TearDown(Level.Invocation)
        public void tearDown() throws IOException {
            directory.close();
        }
    }

    @Benchmark
    public void merge(BenchmarkState state) throws IOException {
        try (var writer = new IndexWriter(state.directory, state.mergeConfig.get())) {
            writer.forceMerge(1);
        }
    }

    private static Directory openDirectory(DirType dirType) throws IOException {
        return switch (dirType) {
            case MMAP -> new MMapDirectory(Files.createTempDirectory("bloom-mmap-"));
            case NIOFS -> new NIOFSDirectory(Files.createTempDirectory("bloom-niofs-"));
        };
    }

    private static void createIndex(Directory directory, int numSegments, int docsPerSegment, int filterSizeBytes, boolean mixedSizes)
        throws IOException {
        var config = new IndexWriterConfig();
        config.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        config.setMergePolicy(NoMergePolicy.INSTANCE);
        config.setCodec(createCodec(filterSizeBytes, mixedSizes));

        Random rng = new Random(42);
        try (var writer = new IndexWriter(directory, config)) {
            for (int seg = 0; seg < numSegments; seg++) {
                for (int doc = 0; doc < docsPerSegment; doc++) {
                    Document d = new Document();
                    d.add(new BinaryDocValuesField(ID_FIELD, new BytesRef(new UUID(rng.nextLong(), rng.nextLong()).toString())));
                    writer.addDocument(d);
                }
                writer.flush();
            }
        }
    }

    private static IndexWriterConfig createMergeConfig(int filterSizeBytes, boolean mixedSizes) {
        var config = new IndexWriterConfig();
        config.setMergePolicy(new LogByteSizeMergePolicy());
        config.setCodec(createCodec(filterSizeBytes, mixedSizes));
        return config;
    }

    private static Elasticsearch93Lucene104Codec createCodec(int filterSizeBytes, boolean mixedSizes) {
        final int[] mixedFilterSizes = { 32768, 65536, 131072, 262144 };
        final AtomicInteger counter = new AtomicInteger();

        var format = new ES94BloomFilterDocValuesFormat(
            BigArrays.NON_RECYCLING_INSTANCE,
            ID_FIELD,
            true,
            ES94BloomFilterDocValuesFormat.DEFAULT_NUM_HASH_FUNCTIONS,
            ES94BloomFilterDocValuesFormat.DEFAULT_SMALL_SEGMENT_MAX_DOCS,
            ES94BloomFilterDocValuesFormat.DEFAULT_LARGE_SEGMENT_MIN_DOCS,
            ES94BloomFilterDocValuesFormat.DEFAULT_HIGH_BITS_PER_DOC,
            ES94BloomFilterDocValuesFormat.DEFAULT_LOW_BITS_PER_DOC,
            ByteSizeValue.ofMb(8)
        ) {
            @Override
            public int bloomFilterSizeInBytesForNewSegment(int numDocs) {
                if (mixedSizes) {
                    return mixedFilterSizes[counter.getAndIncrement() % mixedFilterSizes.length];
                }
                return filterSizeBytes > 0 ? filterSizeBytes : super.bloomFilterSizeInBytesForNewSegment(numDocs);
            }
        };

        return new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                if (field.equals(ID_FIELD)) {
                    return format;
                }
                return super.getDocValuesFormatForField(field);
            }
        };
    }
}
