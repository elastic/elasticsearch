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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
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
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.columnar.string.StringColumnOptionsSelector;
import org.elasticsearch.columnar.string.StringColumnWriter;
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
 * What a merge of plain keyword columns saves by copying chunks as they are stored rather than decompressing and
 * compressing them again, under the index sorts a logs index is written with.
 *
 * <p>The documents are shaped like a {@code logsdb_columnar} index: a {@code host.name} and an {@code @timestamp},
 * arriving roughly in time order, and a keyword field whose values do not repeat enough to earn a dictionary, so
 * its column is plain and written with the options a keyword field ships with. {@code copyChunks} runs the same
 * merge with copying on and off; the sort decides how much there is to copy:
 * <ul>
 *   <li>{@link IndexSortShape#NONE} — each segment is appended whole, so nearly every chunk is copied.</li>
 *   <li>{@link IndexSortShape#TIMESTAMP} — {@code @timestamp} descending, what a logs index sorts on by default.
 *       Each flush covers a window of time of its own, so the merge reorders the segments but keeps each one
 *       together, apart from the few documents that arrived out of order around a window's edge.</li>
 *   <li>{@link IndexSortShape#HOSTNAME_TIMESTAMP} — {@code host.name} then {@code @timestamp} descending, the sort a
 *       logs index takes with {@code index.logsdb.sort_on_host_name}. Every segment holds every host, so the merge
 *       interleaves them: a run is one host's documents from one segment, and only chunks inside such a run are
 *       copied. {@code hosts} decides how long those runs are.</li>
 * </ul>
 *
 * <p>Only the merge is measured: the segments are written once per trial and copied afresh for every merge, since
 * merging them consumes them. A merge takes hundreds of milliseconds and consumes its input, so each iteration is
 * one merge, timed once. What the merge copied is reported alongside, so a result that shows no gain can be told
 * apart from one where nothing was there to copy.
 *
 * <pre>
 * ./gradlew :benchmarks:run --args="ColumnarPlainStringMergeBenchmark"
 * ./gradlew :benchmarks:run --args="ColumnarPlainStringMergeBenchmark -p sort=HOSTNAME_TIMESTAMP -p hosts=10,100,1000"
 * </pre>
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class ColumnarPlainStringMergeBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    private static final String HOST_NAME = "host.name";
    private static final String TIMESTAMP = "@timestamp";
    private static final String FIELD = "message";

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
                ColumnarPlainStringMergeBenchmark.TIMESTAMP,
                SortField.Type.LONG,
                true,
                SortedNumericSelector.Type.MAX
            );
        }
    }

    @Param({ "NONE", "TIMESTAMP", "HOSTNAME_TIMESTAMP" })
    private IndexSortShape sort;

    /** On: chunks are copied where they can be. Off: every chunk is decompressed and compressed again, as before. */
    @Param({ "true", "false" })
    private boolean copyChunks;

    /** Values that stay plain under the options a keyword field ships with. */
    @Param({ "URL" })
    private StringData data;

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

    /** What one merge copied rather than compressed again; reported by JMH as secondary metrics. */
    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class Copied {
        /** Runs of slots whose chunks were copied. */
        public double copiedRuns;
        /** Bytes the copied chunks decode to. */
        public double copiedBytes;

        @Setup(Level.Iteration)
        public void reset() {
            copiedRuns = 0;
            copiedBytes = 0;
        }
    }

    /** Collects what the column writer reports copying. */
    private static final class CopyReport extends InfoStream {
        long runs;
        long bytes;

        @Override
        public void message(String component, String message) {
            // "copied [slots] plain slots, [chunks] chunks of [bytes] bytes as stored"
            final int bytesAt = message.lastIndexOf('[');
            bytes += Long.parseLong(message.substring(bytesAt + 1, message.indexOf(']', bytesAt)));
            runs++;
        }

        @Override
        public boolean isEnabled(String component) {
            return StringColumnWriter.INFO_STREAM_COMPONENT.equals(component);
        }

        @Override
        public void close() {}
    }

    @Setup(Level.Trial)
    public void writeSegments() throws IOException {
        final BytesRef[] values = data.generate(docCount, new Random(7));
        final Random random = new Random(11);
        final String[] hostNames = new String[hosts];
        for (int i = 0; i < hosts; i++) {
            hostNames[i] = "host-" + i + ".eu-west-1.compute.internal";
        }
        final FieldType binary = new FieldType();
        binary.setDocValuesType(DocValuesType.BINARY);
        binary.freeze();

        template = Files.createTempDirectory("columnar-plain-merge-template");
        final IndexWriterConfig iwc = config(null).setMergePolicy(NoMergePolicy.INSTANCE)
            // Flushed where asked and nowhere else, so every segment holds segmentSize documents.
            .setMaxBufferedDocs(segmentSize + 1)
            .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        try (Directory directory = new MMapDirectory(template); IndexWriter writer = new IndexWriter(directory, iwc)) {
            long timestamp = 1_700_000_000_000L;
            for (int i = 0; i < docCount; i++) {
                // Ten milliseconds apart on average, each arriving up to a second late, so the windows of time the
                // flushes cover overlap a little at their edges, as a shipper's batches do.
                timestamp += 10;
                final Document doc = new Document();
                doc.add(new SortedSetDocValuesField(HOST_NAME, new BytesRef(hostNames[random.nextInt(hosts)])));
                doc.add(new SortedNumericDocValuesField(TIMESTAMP, timestamp - random.nextInt(1000)));
                doc.add(new Field(FIELD, StringFormat.payload(values[i]), binary));
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
            // Copying only applies to a plain column, so a data shape that earns a dictionary measures nothing.
            for (LeafReaderContext leaf : reader.leaves()) {
                final BinaryDocValues column = leaf.reader().getBinaryDocValues(FIELD);
                final boolean plain = column instanceof ColumnarStringBinaryDocValues columnar
                    && columnar.reader().hasDictionary() == false;
                if (plain == false) {
                    throw new AssertionError("expected a plain columnar column for [" + data + "], got " + column);
                }
            }
        }
    }

    /** Fresh segments for every merge, copied from the template, since merging them consumes them. */
    @Setup(Level.Iteration)
    public void copySegments() throws IOException {
        mergePath = Files.createTempDirectory("columnar-plain-merge");
        try (Stream<Path> files = Files.list(template)) {
            for (Path file : files.toList()) {
                Files.copy(file, mergePath.resolve(file.getFileName()));
            }
        }
        mergeDirectory = new MMapDirectory(mergePath);
    }

    @Benchmark
    public void merge(Copied copied) throws IOException {
        final CopyReport report = new CopyReport();
        try (IndexWriter writer = new IndexWriter(mergeDirectory, config(report).setMergePolicy(new LogByteSizeMergePolicy()))) {
            writer.forceMerge(1);
        }
        copied.copiedRuns = report.runs;
        copied.copiedBytes = report.bytes;
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
     * The same codec and sort for writing the segments and merging them. The sort fields keep Lucene's sorted doc
     * values, as a logs index keeps them for its sort; only the keyword column is ColumNAR's.
     */
    private IndexWriterConfig config(InfoStream infoStream) {
        final DocValuesFormat columnar = new ColumNARDocValuesFormat(
            (fieldName, type) -> NumericPipeline::defaultPipeline,
            field -> ColumnarFieldType.STRING,
            ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
            StringColumnOptionsSelector.always(StringColumnOptions.DEFAULT),
            copyChunks
        );
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
        if (infoStream != null) {
            iwc.setInfoStream(infoStream);
        }
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
