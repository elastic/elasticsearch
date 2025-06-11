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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.lucene.search.XIndexSortSortedNumericDocValuesRangeQuery;
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
 *
 * <p><b>Goal:</b> This benchmark is designed to mimic and benchmark the execution of a range query in LogsDB,
 * with and without a sparse doc values index on the {@code host.name} and {@code @timestamp} fields.
 *
 * <p><b>Document Structure:</b>
 * <ul>
 *   <li>{@code host.name}: A keyword field (sorted, non-stored).</li>
 *   <li>{@code @timestamp}: A numeric field, indexed for range queries and using doc values with or without a doc values sparse index.</li>
 * </ul>
 *
 * <p><b>Index Sorting:</b>
 * The index is sorted primarily by {@code host.name} (ascending) and secondarily by {@code @timestamp} (descending).
 * Documents are grouped into batches, where each hostname gets a dedicated batch of timestamps.
 * This is meant to simulate collecting logs from a set of hosts over a certain time interval.
 *
 * <p><b>Batched Data Behavior:</b>
 * <ul>
 *   <li>The {@code host.name} value is generated in batches (e.g., "host-0", "host-1", ...).</li>
 *   <li>Each batch contains a fixed number of documents ({@code batchSize}).</li>
 *   <li>The {@code @timestamp} value resets to {@code BASE_TIMESTAMP} at the start of each batch.</li>
 *   <li>A random timestamp delta (0–{@code deltaTime} ms) is added so that each document in a batch differs slightly.</li>
 * </ul>
 *
 * <p><b>Example Output:</b>
 * <pre>
 * | Document # | host.name | @timestamp (ms since epoch) |
 * |-----------|-----------|------------------------------|
 * | 1         | host-0    | 1704067200005               |
 * | 2         | host-0    | 1704067201053               |
 * | 3         | host-0    | 1704067202091               |
 * | ...       | ...       | ...                          |
 * | 10000     | host-0    | 1704077199568               |
 * | 10001     | host-1    | 1704067200042               |
 * | 10002     | host-1    | 1704067201099               |
 * | ...       | ...       | ...                          |
 * </pre>
 *
 * <p>When running the range query, we retrieve only a fraction of the total data,
 * simulating a real-world scenario where a dashboard only needs the most recent logs.
 */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class DateFieldMapperDocValuesSkipperBenchmark {

    /**
     * Total number of documents to index.
     */
    @Param("1343120")
    private int nDocs;

    /**
     * Number of documents per hostname batch.
     */
    @Param({ "1340", "121300" })
    private int batchSize;

    /**
     * Maximum random increment (in milliseconds) added to each doc's timestamp.
     */
    @Param("1000")
    private int deltaTime;

    /**
     * Fraction of the total time range (derived from {@code batchSize * deltaTime}) that the range query will cover.
     */
    @Param({ "0.01", "0.2", "0.8" })
    private double queryRange;

    /**
     * Number of docs to index before forcing a commit, thus creating multiple Lucene segments.
     */
    @Param({ "7390", "398470" })
    private int commitEvery;

    /**
     * Seed for random data generation.
     */
    @Param("42")
    private int seed;

    private static final String TIMESTAMP_FIELD = "@timestamp";
    private static final String HOSTNAME_FIELD = "host.name";
    private static final long BASE_TIMESTAMP = 1704067200000L;

    private IndexSearcher indexSearcherWithoutDocValuesSkipper;
    private IndexSearcher indexSearcherWithDocValuesSkipper;
    private ExecutorService executorService;

    /**
     * Main entry point for running this benchmark via JMH.
     *
     * @param args command line arguments (unused)
     * @throws RunnerException if the benchmark fails to run
     */
    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder().include(DateFieldMapperDocValuesSkipperBenchmark.class.getSimpleName())
            .addProfiler(AsyncProfiler.class)
            .build();

        new Runner(options).run();
    }

    /**
     * Sets up the benchmark by creating Lucene indexes (with and without doc values skipper).
     * Sets up a single-threaded executor for searching the indexes and avoid concurrent search threads.
     *
     * @throws IOException if an error occurs while building the index
     */
    @Setup(Level.Trial)
    public void setup() throws IOException {
        executorService = Executors.newSingleThreadExecutor();

        final Directory tempDirectoryWithoutDocValuesSkipper = FSDirectory.open(Files.createTempDirectory("temp1-"));
        final Directory tempDirectoryWithDocValuesSkipper = FSDirectory.open(Files.createTempDirectory("temp2-"));

        indexSearcherWithoutDocValuesSkipper = createIndex(tempDirectoryWithoutDocValuesSkipper, false, commitEvery);
        indexSearcherWithDocValuesSkipper = createIndex(tempDirectoryWithDocValuesSkipper, true, commitEvery);
    }

    /**
     * Creates an {@link IndexSearcher} after indexing documents in batches.
     * Each batch commit forces multiple segments to be created.
     *
     * @param directory            the Lucene {@link Directory} for writing the index
     * @param withDocValuesSkipper true if we should enable doc values skipper on certain fields
     * @param commitEvery          number of documents after which to commit (and thus segment)
     * @return an {@link IndexSearcher} for querying the newly built index
     * @throws IOException if an I/O error occurs during index writing
     */
    private IndexSearcher createIndex(final Directory directory, final boolean withDocValuesSkipper, final int commitEvery)
        throws IOException {

        final IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        // NOTE: index sort config matching LogsDB's sort order
        config.setIndexSort(
            new Sort(
                new SortField(HOSTNAME_FIELD, SortField.Type.STRING, false),
                new SortedNumericSortField(TIMESTAMP_FIELD, SortField.Type.LONG, true)
            )
        );

        final Random random = new Random(seed);

        try (IndexWriter indexWriter = new IndexWriter(directory, config)) {
            int docCountSinceLastCommit = 0;

            for (int i = 0; i < nDocs; i++) {
                final Document doc = new Document();
                addFieldsToDocument(doc, i, withDocValuesSkipper, random);
                indexWriter.addDocument(doc);
                docCountSinceLastCommit++;

                // Force commit periodically to create multiple Lucene segments
                if (docCountSinceLastCommit >= commitEvery) {
                    indexWriter.commit();
                    docCountSinceLastCommit = 0;
                }
            }

            indexWriter.commit();

            // Open a reader and create a searcher on top of it using a single thread executor.
            DirectoryReader reader = DirectoryReader.open(indexWriter);
            return new IndexSearcher(reader, executorService);
        }
    }

    /**
     * Populates the given {@link Document} with fields, optionally using doc values skipper.
     *
     * @param doc                  the Lucene document to fill
     * @param docIndex             index of the document being added
     * @param withDocValuesSkipper true if doc values skipper is enabled
     * @param random               seeded {@link Random} for data variation
     */
    private void addFieldsToDocument(final Document doc, int docIndex, boolean withDocValuesSkipper, final Random random) {

        final int batchIndex = docIndex / batchSize;
        final String hostName = "host-" + batchIndex;

        // Slightly vary the timestamp in each document
        final long timestamp = BASE_TIMESTAMP + ((docIndex % batchSize) * deltaTime) + random.nextInt(0, deltaTime);

        if (withDocValuesSkipper) {
            // Sparse doc values index on `@timestamp` and `host.name`
            doc.add(SortedNumericDocValuesField.indexedField(TIMESTAMP_FIELD, timestamp));
            doc.add(SortedDocValuesField.indexedField(HOSTNAME_FIELD, new BytesRef(hostName)));
        } else {
            // Standard doc values, points and inverted index
            doc.add(new StringField(HOSTNAME_FIELD, hostName, Field.Store.NO));
            doc.add(new SortedDocValuesField(HOSTNAME_FIELD, new BytesRef(hostName)));
            doc.add(new LongPoint(TIMESTAMP_FIELD, timestamp));
            doc.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, timestamp));
        }
    }

    /**
     * Calculates the upper bound for the timestamp range query based on {@code batchSize},
     * {@code deltaTime}, and {@code queryRange}.
     *
     * @return the computed upper bound for the timestamp range query
     */
    private long rangeEndTimestamp() {
        return BASE_TIMESTAMP + (long) (batchSize * deltaTime * queryRange);
    }

    /**
     * Executes a range query without doc values skipper.
     *
     * @param bh the blackhole consuming the query result
     * @throws IOException if a search error occurs
     */
    @Benchmark
    public void rangeQueryWithoutDocValuesSkipper(final Blackhole bh) throws IOException {
        bh.consume(rangeQuery(indexSearcherWithoutDocValuesSkipper, BASE_TIMESTAMP, rangeEndTimestamp(), true));
    }

    /**
     * Executes a range query with doc values skipper enabled.
     *
     * @param bh the blackhole consuming the query result
     * @throws IOException if a search error occurs
     */
    @Benchmark
    public void rangeQueryWithDocValuesSkipper(final Blackhole bh) throws IOException {
        bh.consume(rangeQuery(indexSearcherWithDocValuesSkipper, BASE_TIMESTAMP, rangeEndTimestamp(), false));
    }

    /**
     * Runs the actual Lucene range query, optionally combining a {@link LongPoint} index query
     * with doc values ({@link SortedNumericDocValuesField}) via {@link IndexOrDocValuesQuery},
     * and then wrapping it with an {@link XIndexSortSortedNumericDocValuesRangeQuery} to utilize the index sort.
     *
     * @param searcher            the Lucene {@link IndexSearcher}
     * @param rangeStartTimestamp lower bound of the timestamp range
     * @param rangeEndTimestamp   upper bound of the timestamp range
     * @param isIndexed           true if we should combine indexed and doc value queries
     * @return the total number of matching documents
     * @throws IOException if a search error occurs
     */
    private long rangeQuery(final IndexSearcher searcher, long rangeStartTimestamp, long rangeEndTimestamp, boolean isIndexed)
        throws IOException {

        assert rangeEndTimestamp > rangeStartTimestamp;

        final Query rangeQuery = isIndexed
            ? new IndexOrDocValuesQuery(
                LongPoint.newRangeQuery(TIMESTAMP_FIELD, rangeStartTimestamp, rangeEndTimestamp),
                SortedNumericDocValuesField.newSlowRangeQuery(TIMESTAMP_FIELD, rangeStartTimestamp, rangeEndTimestamp)
            )
            : SortedNumericDocValuesField.newSlowRangeQuery(TIMESTAMP_FIELD, rangeStartTimestamp, rangeEndTimestamp);

        final Query query = new XIndexSortSortedNumericDocValuesRangeQuery(
            TIMESTAMP_FIELD,
            rangeStartTimestamp,
            rangeEndTimestamp,
            rangeQuery
        );

        return searcher.count(query);
    }

    /**
     * Shuts down the executor service after the trial completes.
     */
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
