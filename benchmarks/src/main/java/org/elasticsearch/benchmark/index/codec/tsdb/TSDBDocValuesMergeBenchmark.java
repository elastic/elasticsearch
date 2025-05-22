/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.Elasticsearch92Lucene103Codec;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 0)
@Measurement(iterations = 1)
public class TSDBDocValuesMergeBenchmark {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging();
        LogConfigurator.setNodeName("test");
    }

    private static final String TIMESTAMP_FIELD = "@timestamp";
    private static final String HOSTNAME_FIELD = "host.name";
    private static final long BASE_TIMESTAMP = 1704067200000L;

    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder().include(TSDBDocValuesMergeBenchmark.class.getSimpleName())
            .addProfiler(AsyncProfiler.class)
            .build();

        new Runner(options).run();
    }

    @State(Scope.Benchmark)
    public static class StateDenseWithoutOptimizeMerge {

        @Param("20431204")
        private int nDocs;

        @Param("1000")
        private int deltaTime;

        @Param("42")
        private int seed;

        private Directory directory;
        private final Supplier<IndexWriterConfig> iwc = () -> createIndexWriterConfig(false);

        @Setup(Level.Trial)
        public void setup() throws IOException {
            directory = FSDirectory.open(Files.createTempDirectory("temp2-"));
            createIndex(directory, iwc.get(), false, nDocs, deltaTime, seed);
        }

    }

    @Benchmark
    public void forceMergeDenseWithoutOptimizedMerge(StateDenseWithoutOptimizeMerge state) throws IOException {
        forceMerge(state.directory, state.iwc.get());
    }

    @State(Scope.Benchmark)
    public static class StateDenseWithOptimizeMerge {

        @Param("20431204")
        private int nDocs;

        @Param("1000")
        private int deltaTime;

        @Param("42")
        private int seed;

        private Directory directory;
        private final Supplier<IndexWriterConfig> iwc = () -> createIndexWriterConfig(true);

        @Setup(Level.Trial)
        public void setup() throws IOException {
            directory = FSDirectory.open(Files.createTempDirectory("temp1-"));
            createIndex(directory, iwc.get(), false, nDocs, deltaTime, seed);
        }

    }

    @Benchmark
    public void forceMergeDenseWithOptimizedMerge(StateDenseWithOptimizeMerge state) throws IOException {
        forceMerge(state.directory, state.iwc.get());
    }

    @State(Scope.Benchmark)
    public static class StateSparseWithoutOptimizeMerge {

        @Param("20431204")
        private int nDocs;

        @Param("1000")
        private int deltaTime;

        @Param("42")
        private int seed;

        private Directory directory;
        private final Supplier<IndexWriterConfig> iwc = () -> createIndexWriterConfig(false);

        @Setup(Level.Trial)
        public void setup() throws IOException {
            directory = FSDirectory.open(Files.createTempDirectory("temp4-"));
            createIndex(directory, iwc.get(), true, nDocs, deltaTime, seed);
        }

    }

    @Benchmark
    public void forceMergeSparseWithoutOptimizedMerge(StateSparseWithoutOptimizeMerge state) throws IOException {
        forceMerge(state.directory, state.iwc.get());
    }

    @State(Scope.Benchmark)
    public static class StateSparseWithOptimizeMerge {

        @Param("20431204")
        private int nDocs;

        @Param("1000")
        private int deltaTime;

        @Param("42")
        private int seed;

        private Directory directory;
        private final Supplier<IndexWriterConfig> iwc = () -> createIndexWriterConfig(true);

        @Setup(Level.Trial)
        public void setup() throws IOException {
            directory = FSDirectory.open(Files.createTempDirectory("temp3-"));
            createIndex(directory, iwc.get(), true, nDocs, deltaTime, seed);
        }

    }

    @Benchmark
    public void forceMergeSparseWithOptimizedMerge(StateSparseWithOptimizeMerge state) throws IOException {
        forceMerge(state.directory, state.iwc.get());
    }

    private void forceMerge(Directory directory, IndexWriterConfig config) throws IOException {
        try (var indexWriter = new IndexWriter(directory, config)) {
            indexWriter.forceMerge(1);
        }
    }

    static void createIndex(Directory directory, IndexWriterConfig iwc, boolean sparse, int nDocs, int deltaTime, int seed)
        throws IOException {
        long counter1 = 0;
        long counter2 = 10_000_000;
        long[] gauge1Values = new long[] { 2, 4, 6, 8, 10, 12, 14, 16 };
        long[] gauge2Values = new long[] { -2, -4, -6, -8, -10, -12, -14, -16 };
        int numHosts = 10000;
        String[] tags = new String[] { "tag_1", "tag_2", "tag_3", "tag_4", "tag_5", "tag_6", "tag_7", "tag_8" };

        final Random random = new Random(seed);
        try (var indexWriter = new IndexWriter(directory, iwc)) {
            for (int i = 0; i < nDocs; i++) {
                final Document doc = new Document();

                final int batchIndex = i % numHosts;
                final String hostName = "host-" + batchIndex;
                // Slightly vary the timestamp in each document
                final long timestamp = BASE_TIMESTAMP + ((i % numHosts) * deltaTime) + random.nextInt(0, deltaTime);

                doc.add(new SortedDocValuesField(HOSTNAME_FIELD, new BytesRef(hostName)));
                doc.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, timestamp));
                if (sparse == false || random.nextBoolean()) {
                    doc.add(new SortedNumericDocValuesField("counter_1", counter1++));
                }
                if (sparse == false || random.nextBoolean()) {
                    doc.add(new SortedNumericDocValuesField("counter_2", counter2++));
                }
                if (sparse == false || random.nextBoolean()) {
                    doc.add(new SortedNumericDocValuesField("gauge_1", gauge1Values[i % gauge1Values.length]));
                }
                if (sparse == false || random.nextBoolean()) {
                    doc.add(new SortedNumericDocValuesField("gauge_2", gauge2Values[i % gauge1Values.length]));
                }
                if (sparse == false || random.nextBoolean()) {
                    int numTags = tags.length % (i + 1);
                    for (int j = 0; j < numTags; j++) {
                        doc.add(new SortedSetDocValuesField("tags", new BytesRef(tags[j])));
                    }
                }
                indexWriter.addDocument(doc);

                if (i % 10000 == 0) {
                    indexWriter.commit();
                }
            }
        }
    }

    private static IndexWriterConfig createIndexWriterConfig(boolean optimizedMergeEnabled) {
        var config = new IndexWriterConfig(new StandardAnalyzer());
        // NOTE: index sort config matching LogsDB's sort order
        config.setIndexSort(
            new Sort(
                new SortField(HOSTNAME_FIELD, SortField.Type.STRING, false),
                new SortedNumericSortField(TIMESTAMP_FIELD, SortField.Type.LONG, true)
            )
        );
        config.setLeafSorter(DataStream.TIMESERIES_LEAF_READERS_SORTER);
        config.setMergePolicy(new LogByteSizeMergePolicy());
        var docValuesFormat = new ES819TSDBDocValuesFormat(4096, optimizedMergeEnabled);
        config.setCodec(new Elasticsearch92Lucene103Codec() {

            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return docValuesFormat;
            }
        });
        return config;
    }
}
