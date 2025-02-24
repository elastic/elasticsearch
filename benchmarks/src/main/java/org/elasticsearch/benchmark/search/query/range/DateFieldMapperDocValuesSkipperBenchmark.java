/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.query.range;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
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
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for measuring query performance with and without doc values skipper in Elasticsearch.
 * <p>
 * <b>Goal:</b> This benchmark is designed to **mimic and benchmark the execution of a range query in LogsDB**,
 * with and without a **sparse doc values index** on the `host.name` and `@timestamp` fields.
 * <p>
 * <b>Document Structure:</b>
 * - `host.name`: A keyword field (sorted, non-stored).
 * - `@timestamp`: A numeric field, indexed for range queries and using doc values with or without a doc values sparse index.
 * <p>
 * <b>Index Sorting:</b>
 * The index is sorted primarily by `host.name` (ascending) and secondarily by `@timestamp` (descending).
 * Documents are grouped into batches, where each hostname gets a dedicated batch of timestamps.
 * This is meant to simulate collection of logs from a set of hosts in a certain time interval.
 * <p>
 * <b>Batched Data Behavior:</b>
 * - The `host.name` value is generated in batches (e.g., "host-0", "host-1", ...).
 * - Each batch contains a fixed number of documents (`batchSize`).
 * - The `@timestamp` value resets to `BASE_TIMESTAMP` at the start of each batch.
 * - A random **timestamp delta** (0-{@code timestampIncrementMillis} ms) is added to ensure timestamps within each batch have slight
 * variation.
 * <p>
 * <b>Example Output:</b>
 * The table below shows a sample of generated documents (with a batch size of 10,000):
 *
 * <pre>
 * | Document # | host.name | @timestamp (ms since epoch) |
 * |-----------|----------|---------------------------|
 * | 1         | host-0   | 1704067200005             |
 * | 2         | host-0   | 1704067201053             |
 * | 3         | host-0   | 1704067202091             |
 * | ...       | ...      | ...                       |
 * | 10000     | host-0   | 1704077199568             |
 * | 10001     | host-1   | 1704067200042             |
 * | 10002     | host-1   | 1704067201099             |
 * | ...       | ...      | ...                       |
 * </pre>
 *
 * <p>
 * When running the range query we also retrieve just a fraction of the data, to simulate a real-world scenario where a
 * dashboard requires only the most recent logs.
 */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class DateFieldMapperDocValuesSkipperBenchmark {

    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder().include(DateFieldMapperDocValuesSkipperBenchmark.class.getSimpleName())
            .addProfiler(AsyncProfiler.class)
            .build();

        new Runner(options).run();
    }

    @Param("1343120")
    private int numberOfDocuments;

    @Param({"1340", "121300"})
    private int batchSize;

    @Param("1000")
    private int timestampIncrementMillis;

    @Param({ "0.01", "0.2", "0.8" })
    private double timestampRangeFraction;

    @Param({ "7390", "398470" })
    private int commitEvery;

    @Param("42")
    private int seed;

    private static final String TIMESTAMP_FIELD = "@timestamp";
    private static final String HOSTNAME_FIELD = "host.name";
    private static final long BASE_TIMESTAMP = 1704067200000L;

    private static final Sort QUERY_SORT = new Sort(new SortedNumericSortField(TIMESTAMP_FIELD, SortField.Type.LONG, true));

    private IndexSearcher indexSearcherWithoutDocValuesSkipper;
    private IndexSearcher indexSearcherWithDocValuesSkipper;
    private ExecutorService executorService;

    /**
     * Sets up the benchmark by creating Lucene indexes with and without doc values skipper.
     *
     * @throws IOException if an error occurs during index creation.
     */
    @Setup(Level.Trial)
    public void setup() throws IOException {
        executorService = Executors.newSingleThreadExecutor();
        Directory tempDirectoryWithoutDocValuesSkipper = FSDirectory.open(Files.createTempDirectory("temp1-"));
        Directory tempDirectoryWithDocValuesSkipper = FSDirectory.open(Files.createTempDirectory("temp2-"));

        indexSearcherWithoutDocValuesSkipper = createIndex(tempDirectoryWithoutDocValuesSkipper, false, commitEvery);
        indexSearcherWithDocValuesSkipper = createIndex(tempDirectoryWithDocValuesSkipper, true, commitEvery);
    }

    /**
     * Creates an {@link IndexSearcher} from a newly created {@link IndexWriter}. Documents
     * are added to the index and committed in batches of a specified size to generate multiple segments.
     *
     * @param directory            the Lucene {@link Directory} where the index will be written
     * @param withDocValuesSkipper indicates whether certain fields should skip doc values
     * @param commitEvery          the number of documents after which to force a commit
     * @return an {@link IndexSearcher} that can be used to query the newly created index
     * @throws IOException if an I/O error occurs during index writing or reading
     */
    private IndexSearcher createIndex(final Directory directory, final boolean withDocValuesSkipper, final int commitEvery)
        throws IOException {
        final IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        config.setIndexSort(
            new Sort(
                new SortField(HOSTNAME_FIELD, SortField.Type.STRING, false), // NOTE: `host.name` ascending
                new SortedNumericSortField(TIMESTAMP_FIELD, SortField.Type.LONG, true) // NOTE: `@timestamp` descending
            )
        );

        final Random random = new Random(seed);
        try (IndexWriter indexWriter = new IndexWriter(directory, config)) {
            int docCountSinceLastCommit = 0;
            for (int i = 0; i < numberOfDocuments; i++) {
                final Document doc = new Document();
                addFieldsToDocument(doc, i, withDocValuesSkipper, random);
                indexWriter.addDocument(doc);
                docCountSinceLastCommit++;

                // NOTE: make sure we have multiple Lucene segments
                if (docCountSinceLastCommit >= commitEvery) {
                    indexWriter.commit();
                    docCountSinceLastCommit = 0;
                }
            }

            indexWriter.commit();
            final DirectoryReader reader = DirectoryReader.open(indexWriter);
            // NOTE: internally Elasticsearch runs multiple search threads concurrently, (at least) one per Lucene segment.
            // Here we simplify the benchmark making sure we have a single-threaded search execution using a single thread
            // executor Service.
            return new IndexSearcher(reader, executorService);
        }
    }

    private void addFieldsToDocument(final Document doc, int docIndex, boolean withDocValuesSkipper, final Random random) {
        final int batchIndex = docIndex / batchSize;
        final String hostName = "host-" + batchIndex;
        final long timestampDelta = random.nextInt(0, timestampIncrementMillis);
        final long timestamp = BASE_TIMESTAMP + ((docIndex % batchSize) * timestampIncrementMillis) + timestampDelta;

        if (withDocValuesSkipper) {
            doc.add(SortedNumericDocValuesField.indexedField(TIMESTAMP_FIELD, timestamp)); // NOTE: doc values skipper on `@timestamp`
            doc.add(SortedDocValuesField.indexedField(HOSTNAME_FIELD, new BytesRef(hostName))); // NOTE: doc values skipper on `host.name`
        } else {
            doc.add(new LongPoint(TIMESTAMP_FIELD, timestamp)); // BKD tree on `@timestamp`
            doc.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, timestamp)); // NOTE: doc values without the doc values skipper on
                                                                                  // `@timestamp`
            doc.add(new SortedDocValuesField(HOSTNAME_FIELD, new BytesRef(hostName))); // NOTE: doc values without the doc values skipper on
                                                                                       // `host.name`
        }

        doc.add(new StringField(HOSTNAME_FIELD, hostName, Field.Store.NO));
    }

    /**
     * Computes a dynamic timestamp upper bound based on the batch size,
     * timestamp increment, and user-specified fraction.
     *
     * @return The computed upper bound for the timestamp range query.
     */
    private long rangeEndTimestamp() {
        return BASE_TIMESTAMP + ((long) (batchSize * timestampIncrementMillis * timestampRangeFraction));
    }

    @Benchmark
    public void rangeQueryWithoutDocValuesSkipper(final Blackhole bh) throws IOException {
        bh.consume(rangeQuery(indexSearcherWithoutDocValuesSkipper, BASE_TIMESTAMP, rangeEndTimestamp(), true));
    }

    @Benchmark
    public void rangeQueryWithDocValuesSkipper(final Blackhole bh) throws IOException {
        bh.consume(rangeQuery(indexSearcherWithDocValuesSkipper, BASE_TIMESTAMP, rangeEndTimestamp(), false));
    }

    private long rangeQuery(final IndexSearcher searcher, long rangeStartTimestamp, long rangeEndTimestamp, boolean isIndexed)
        throws IOException {
        assert rangeEndTimestamp > rangeStartTimestamp;
        final Query rangeQuery = isIndexed
            ? new IndexOrDocValuesQuery(
                LongPoint.newRangeQuery(TIMESTAMP_FIELD, rangeStartTimestamp, rangeEndTimestamp),
                SortedNumericDocValuesField.newSlowRangeQuery(TIMESTAMP_FIELD, rangeStartTimestamp, rangeEndTimestamp)
            )
            : SortedNumericDocValuesField.newSlowRangeQuery(TIMESTAMP_FIELD, rangeStartTimestamp, rangeEndTimestamp);
        final Query query = new IndexSortSortedNumericDocValuesRangeQuery(
            TIMESTAMP_FIELD,
            rangeStartTimestamp,
            rangeEndTimestamp,
            rangeQuery
        );
        return searcher.search(query, numberOfDocuments, QUERY_SORT).totalHits.value();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (executorService.awaitTermination(30, TimeUnit.SECONDS) == false) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
