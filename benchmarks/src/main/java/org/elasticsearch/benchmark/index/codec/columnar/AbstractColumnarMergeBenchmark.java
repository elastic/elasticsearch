/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.index.codec.Elasticsearch96Codec;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * A merge of one ColumNAR column under the index sorts a logs index is written with, timed alone.
 *
 * <p>The documents carry a {@code host.name} and an {@code @timestamp}, arriving roughly in time order, kept in
 * Lucene's own doc values since a logs index sorts on them; a subclass adds the one column the merge is measured
 * for. The sort decides how the segments' documents land once merged:
 * <ul>
 *   <li>{@link IndexSortShape#NONE} — each segment is appended whole.</li>
 *   <li>{@link IndexSortShape#TIMESTAMP} — {@code @timestamp} descending. Each flush covers a window of time of its
 *       own, so the merge reorders the segments but keeps each one together, apart from the few documents that
 *       arrived out of order around a window's edge.</li>
 *   <li>{@link IndexSortShape#HOSTNAME_TIMESTAMP} — {@code host.name} then {@code @timestamp} descending. Every
 *       segment holds every host, so the merge interleaves them, in runs of one host's documents from one segment;
 *       {@code hosts} decides how long those runs are.</li>
 * </ul>
 *
 * <p>The segments are written once per trial and copied afresh for every merge, since merging them consumes them;
 * each iteration is one merge, timed once. Most of a merge is not the measured column's: the sort, and the sort
 * fields' own doc values. How long the merge spent on doc values, as Lucene times it, is reported alongside, so a
 * change to the column is not lost in the rest.
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public abstract class AbstractColumnarMergeBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    static final String HOST_NAME = "host.name";
    static final String TIMESTAMP = "@timestamp";
    /** The ColumNAR column the merge is measured for. */
    static final String FIELD = "value";

    /** How the index is sorted, and so how the segments' documents land once merged. */
    public enum IndexSortShape {
        NONE {
            @Override
            Sort sort() {
                return null;
            }
        },
        TIMESTAMP {
            @Override
            Sort sort() {
                return new Sort(timestampDescending());
            }
        },
        HOSTNAME_TIMESTAMP {
            @Override
            Sort sort() {
                return new Sort(new SortedSetSortField(HOST_NAME, false, SortedSetSelector.Type.MIN), timestampDescending());
            }
        };

        abstract Sort sort();

        /** As a logs index sorts on its timestamp: newest first, by the largest value a document holds. */
        private static SortField timestampDescending() {
            return new SortedNumericSortField(
                AbstractColumnarMergeBenchmark.TIMESTAMP,
                SortField.Type.LONG,
                true,
                SortedNumericSelector.Type.MAX
            );
        }
    }

    @Param({ "NONE", "TIMESTAMP", "HOSTNAME_TIMESTAMP" })
    private IndexSortShape sort;

    /** Hosts the documents are spread over; under a sort on the host name this sets how long a run is. */
    @Param({ "100" })
    private int hosts;

    @Param({ "1000000" })
    private int docCount;

    /** Documents per flushed segment. */
    @Param({ "100000" })
    private int segmentSize;

    /** The segments as written, copied for every merge. */
    private Path template;
    private Path mergePath;
    private Directory mergeDirectory;

    /** How long one merge spent on doc values, as Lucene reports it; reported by JMH as a secondary metric. */
    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class MergeTimes {
        /**
         * Milliseconds the merge spent merging doc values, the measured column's and the sort fields'. JMH sums it over
         * the measured iterations, so a merge's share is the total divided by their count.
         */
        public double docValuesMillis;

        @Setup(Level.Iteration)
        public void reset() {
            docValuesMillis = 0;
        }
    }

    /** Takes the time the segment merger reports for doc values. */
    private static final class DocValuesTime extends InfoStream {
        long millis;

        @Override
        public void message(String component, String message) {
            // "<millis> ms to merge doc values [<docs> docs]"
            if (message.contains(" ms to merge doc values ")) {
                millis += Long.parseLong(message.substring(0, message.indexOf(' ')));
            }
        }

        @Override
        public boolean isEnabled(String component) {
            return "SM".equals(component);
        }

        @Override
        public void close() {}
    }

    /** Makes the values the measured column is written with, for {@code docCount} documents. */
    abstract void generate(int docCount) throws IOException;

    /** Adds the measured column's value for the {@code i}th document, which carries {@code timestamp}. */
    abstract void addField(Document doc, int i, long timestamp);

    /** The format the measured column is written and merged with. */
    abstract DocValuesFormat columnarFormat();

    /** Fails the trial if the segments do not hold the column shape the benchmark is about. */
    abstract void checkSegments(DirectoryReader reader) throws IOException;

    @Setup(Level.Trial)
    public void writeSegments() throws IOException {
        generate(docCount);
        final Random random = new Random(11);
        final String[] hostNames = new String[hosts];
        for (int i = 0; i < hosts; i++) {
            hostNames[i] = "host-" + i + ".eu-west-1.compute.internal";
        }
        template = Files.createTempDirectory("columnar-merge-template");
        final IndexWriterConfig iwc = config().setMergePolicy(NoMergePolicy.INSTANCE)
            // Flushed where asked and nowhere else, so every segment holds segmentSize documents.
            .setMaxBufferedDocs(segmentSize + 1)
            .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        try (Directory directory = new MMapDirectory(template); IndexWriter writer = new IndexWriter(directory, iwc)) {
            long timestamp = 1_700_000_000_000L;
            for (int i = 0; i < docCount; i++) {
                // Ten milliseconds apart on average, each arriving up to a second late, so the windows of time the
                // flushes cover overlap a little at their edges, as a shipper's batches do.
                timestamp += 10;
                final long docTimestamp = timestamp - random.nextInt(1000);
                final Document doc = new Document();
                doc.add(new SortedSetDocValuesField(HOST_NAME, new BytesRef(hostNames[random.nextInt(hosts)])));
                doc.add(new SortedNumericDocValuesField(TIMESTAMP, docTimestamp));
                addField(doc, i, docTimestamp);
                writer.addDocument(doc);
                if (i % segmentSize == segmentSize - 1) {
                    writer.flush();
                }
            }
            writer.commit();
        }
        try (Directory directory = new MMapDirectory(template); DirectoryReader reader = DirectoryReader.open(directory)) {
            if (reader.leaves().size() < 2) {
                throw new AssertionError("a merge needs segments to merge, got " + reader.leaves().size());
            }
            checkSegments(reader);
        }
    }

    /** Fresh segments for every merge, copied from the template, since merging them consumes them. */
    @Setup(Level.Iteration)
    public void copySegments() throws IOException {
        mergePath = Files.createTempDirectory("columnar-merge");
        try (Stream<Path> files = Files.list(template)) {
            for (Path file : files.toList()) {
                Files.copy(file, mergePath.resolve(file.getFileName()));
            }
        }
        mergeDirectory = new MMapDirectory(mergePath);
    }

    @Benchmark
    public void merge(MergeTimes times) throws IOException {
        final DocValuesTime docValues = new DocValuesTime();
        final IndexWriterConfig iwc = config().setMergePolicy(new LogByteSizeMergePolicy()).setInfoStream(docValues);
        try (IndexWriter writer = new IndexWriter(mergeDirectory, iwc)) {
            writer.forceMerge(1);
        }
        times.docValuesMillis = docValues.millis;
    }

    @TearDown(Level.Iteration)
    public void dropSegments() throws IOException {
        mergeDirectory.close();
        delete(mergePath);
    }

    @TearDown(Level.Trial)
    public void dropTemplate() throws IOException {
        delete(template);
    }

    /**
     * The same codec and sort for writing the segments and merging them. The sort fields keep Lucene's doc values,
     * as a logs index keeps them for its sort; only the measured column is ColumNAR's.
     */
    private IndexWriterConfig config() {
        final DocValuesFormat columnar = columnarFormat();
        final DocValuesFormat lucene = new Lucene90DocValuesFormat();
        final Codec codec = new Elasticsearch96Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return FIELD.equals(field) ? columnar : lucene;
            }
        };
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec)
            .setUseCompoundFile(false)
            // The merge runs on the benchmark's thread, so the time measured is the merge's and nothing else's.
            .setMergeScheduler(new SerialMergeScheduler());
        final Sort indexSort = sort.sort();
        if (indexSort != null) {
            iwc.setIndexSort(indexSort);
        }
        return iwc;
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
}
