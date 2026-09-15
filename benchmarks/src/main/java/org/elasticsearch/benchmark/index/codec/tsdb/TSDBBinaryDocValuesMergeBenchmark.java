/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.index.codec.Elasticsearch96Codec;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat;
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
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

/**
 * Measures what splicing whole compressed blocks during a merge is worth for binary doc values.
 *
 * <p>The two benchmark methods differ only in the {@code es.tsdb.binary_dv_block_splice.enabled} system property, set
 * per method through {@link Fork}, so a single run produces both arms side by side. With splicing off every source
 * block is ZSTD-decompressed and the target block compressed again; with it on, a block whose documents land
 * consecutively in the merged segment is copied across as bytes.
 *
 * <p>How much that helps depends almost entirely on run length — how many documents in a row come from one segment
 * once the index sort has interleaved them. {@code numHosts} is the knob for that: the generated index puts every
 * host in every segment with disjoint time windows, so after merging, each host contributes one run per segment of
 * {@code nDocs / segments / numHosts} documents. Sweep it to find where the win falls off, because a run shorter
 * than a block can never be spliced:
 *
 * <pre>
 * cd benchmarks
 * ../gradlew run --args "org.elasticsearch.benchmark.index.codec.tsdb.TSDBBinaryDocValuesMergeBenchmark \
 *     -p numHosts=8,32,128,512 -rf json -rff build/jmh-result.json" | tee /tmp/bench/binary_dv_merge_splice
 * </pre>
 *
 * <p>Each trial reports the spliced-to-total block ratio it actually achieved alongside the timing. Read it: a run
 * where the ratio is 0 says nothing about the optimization, only that this shape gave it nothing to do.
 *
 * <p>Note the index carries only the two sort fields and the binary field, so the numbers here are the isolated
 * binary doc values merge cost. A real index merges many other fields at the same time, which dilutes the same
 * absolute saving into a smaller relative one.
 *
 * <p>The generated index is cached under {@code java.io.tmpdir} keyed by the parameters, both so the two arms merge
 * byte-identical input and so repeat runs skip the build. Delete {@code $TMPDIR/tsdb-binary-merge-bench-*} to reclaim
 * the space; {@link #GENERATOR_VERSION} guards against reusing a cache built by an older generator.
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Threads(1)
// The spliced arm is several times faster than the baseline and needs the extra warmup iterations to settle;
// with fewer, its first measured iteration is still several times its steady state and swamps the error bars.
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class TSDBBinaryDocValuesMergeBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    private static final String TIMESTAMP_FIELD = "@timestamp";
    private static final String HOSTNAME_FIELD = "host.name";
    private static final String MESSAGE_FIELD = "message";
    private static final long BASE_TIMESTAMP = 1704067200000L;

    private static final String SPLICE_PROPERTY = "es.tsdb.binary_dv_block_splice.enabled";
    private static final String CONSUMER_LOGGER = "org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesConsumer";

    /** Bump when the generator changes, so cached indices from an older shape are not silently reused. */
    private static final int GENERATOR_VERSION = 1;

    @Benchmark
    @Fork(value = 1, jvmArgsAppend = { "-D" + SPLICE_PROPERTY + "=true", "-Xmx4g" })
    public void forceMergeWithBlockSplicing(MergeInput input) throws IOException {
        input.forceMerge();
    }

    @Benchmark
    @Fork(value = 1, jvmArgsAppend = { "-D" + SPLICE_PROPERTY + "=false", "-Xmx4g" })
    public void forceMergeWithoutBlockSplicing(MergeInput input) throws IOException {
        input.forceMerge();
    }

    @State(Scope.Benchmark)
    public static class MergeInput {

        @Param("1000000")
        public int nDocs;

        @Param("8")
        public int segments;

        /** Run length is {@code nDocs / segments / numHosts}; fewer hosts means longer runs and more splicing. */
        @Param("8")
        public int numHosts;

        @Param("100")
        public int valueLength;

        @Param("42")
        public int seed;

        /**
         * Where the merge runs. Splicing saves compression work, not bytes written, so {@code memory} isolates that CPU
         * saving; {@code disk} adds the file system back in for a more production-like, and considerably noisier, figure.
         */
        @Param("memory")
        public String directory;

        /**
         * {@code sorted} applies the LogsDB index sort, which interleaves the segments and leaves only runs of a single
         * segment's documents spliceable. {@code unsorted} drops the sort, so the segments simply concatenate and every
         * block is spliceable — the ceiling on what this optimization can be worth, and the path that the relaxed
         * {@code needsIndexSort == false} gate opened up.
         */
        @Param("sorted")
        public String indexSorting;

        private Path sourcePath;
        private Directory sourceDirectory;
        private Path workPath;
        private Directory workDirectory;
        private final SpliceCounter spliceCounter = new SpliceCounter();

        @Setup(Level.Trial)
        public void buildSourceIndex() throws IOException {
            spliceCounter.install();
            sourcePath = cachedIndexPath();
            if (Files.exists(sourcePath.resolve(COMPLETE_MARKER)) == false) {
                deleteRecursively(sourcePath);
                Files.createDirectories(sourcePath);
                createIndex(sourcePath, nDocs, segments, numHosts, valueLength, seed, sorted());
                Files.createFile(sourcePath.resolve(COMPLETE_MARKER));
            }
            sourceDirectory = FSDirectory.open(sourcePath);
            if (inMemory()) {
                // Pull the whole source into memory once, so that per-invocation copies never touch the file system.
                final Directory inMemorySource = new ByteBuffersDirectory();
                copyIndex(sourceDirectory, inMemorySource);
                sourceDirectory.close();
                sourceDirectory = inMemorySource;
            }
            System.out.printf(
                Locale.ROOT,
                "%n[source index] %d docs in %d segments, %d hosts, %s%n",
                totalDocs(),
                segments,
                numHosts,
                sorted()
                    ? String.format(
                        Locale.ROOT,
                        "index-sorted, run length %d docs (~%d KiB of values per run)",
                        runLength(),
                        (long) runLength() * valueLength / 1024
                    )
                    : "unsorted, so each segment is one run and every block is spliceable"
            );
        }

        /**
         * Copies the prebuilt index into a scratch directory, because a force merge consumes the segments it merges and
         * every measured invocation has to start from the same input. JMH leaves setup out of the reported time.
         */
        @Setup(Level.Invocation)
        public void copyToScratch() throws IOException {
            if (inMemory()) {
                workDirectory = new ByteBuffersDirectory();
            } else {
                workPath = Files.createTempDirectory("tsdb-binary-merge-bench-work-");
                workDirectory = FSDirectory.open(workPath);
            }
            copyIndex(sourceDirectory, workDirectory);
            spliceCounter.reset();
        }

        private void forceMerge() throws IOException {
            try (IndexWriter writer = new IndexWriter(workDirectory, mergeConfig(sorted()))) {
                writer.forceMerge(1);
            }
        }

        @TearDown(Level.Invocation)
        public void verifyAndDiscardScratch() throws IOException {
            spliceCounter.snapshot();
            verifyMergedIndex(workDirectory, totalDocs());
            workDirectory.close();
            workDirectory = null;
            deleteRecursively(workPath);
            workPath = null;
        }

        private boolean inMemory() {
            return switch (directory) {
                case "memory" -> true;
                case "disk" -> false;
                default -> throw new IllegalArgumentException("directory must be [memory] or [disk], got [" + directory + "]");
            };
        }

        private boolean sorted() {
            return switch (indexSorting) {
                case "sorted" -> true;
                case "unsorted" -> false;
                default -> throw new IllegalArgumentException("indexSorting must be [sorted] or [unsorted], got [" + indexSorting + "]");
            };
        }

        @TearDown(Level.Trial)
        public void reportSpliceRatio() throws IOException {
            sourceDirectory.close();
            System.out.printf(
                Locale.ROOT,
                "%n[%s=%s] spliced %d of %d binary doc values blocks (%.1f%%)%n",
                SPLICE_PROPERTY,
                System.getProperty(SPLICE_PROPERTY, "true"),
                spliceCounter.lastSpliced,
                spliceCounter.lastTotal,
                spliceCounter.lastTotal == 0 ? 0.0 : 100.0 * spliceCounter.lastSpliced / spliceCounter.lastTotal
            );
            spliceCounter.uninstall();
        }

        private int runLength() {
            return nDocs / segments / numHosts;
        }

        /** The generator emits whole runs, so integer division can leave this a little short of {@code nDocs}. */
        private int totalDocs() {
            return segments * numHosts * runLength();
        }

        private Path cachedIndexPath() {
            final String key = String.format(
                Locale.ROOT,
                "tsdb-binary-merge-bench-v%d-%d-%d-%d-%d-%d-%s",
                GENERATOR_VERSION,
                nDocs,
                segments,
                numHosts,
                valueLength,
                seed,
                indexSorting
            );
            return Path.of(System.getProperty("java.io.tmpdir"), key);
        }
    }

    private static final String COMPLETE_MARKER = "benchmark-index-complete";

    /** Copies every index file across, leaving behind the marker and lock that belong to the cache rather than the index. */
    private static void copyIndex(Directory source, Directory target) throws IOException {
        for (String name : source.listAll()) {
            if (name.equals(COMPLETE_MARKER) || name.equals(IndexWriter.WRITE_LOCK_NAME)) {
                continue;
            }
            target.copyFrom(source, name, name, IOContext.DEFAULT);
        }
    }

    /**
     * Reads every value back out of the merged segment. Untimed, and it cannot check the contents against expectations
     * the way the unit tests do, but it does force every block's offsets and payload to decode — enough to be sure the
     * numbers above describe a merge that actually produced a readable index.
     */
    private static void verifyMergedIndex(Directory directory, int expectedDocs) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            if (reader.leaves().size() != 1) {
                throw new AssertionError("expected a single segment after the force merge, got " + reader.leaves().size());
            }
            final LeafReader leaf = reader.leaves().get(0).reader();
            final BinaryDocValues values = leaf.getBinaryDocValues(MESSAGE_FIELD);
            long bytes = 0;
            int docs = 0;
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                bytes += values.binaryValue().length;
                docs++;
            }
            if (docs != expectedDocs || bytes == 0) {
                throw new AssertionError("merged index has " + docs + " values totalling " + bytes + " bytes");
            }
        }
    }

    /**
     * Writes {@code segments} segments, each holding every host over its own disjoint time window. Sorting by
     * {@code (host.name asc, @timestamp desc)} then groups each host's documents together, and within a host the
     * documents of one segment stay together because that segment owns a contiguous slice of the time range — which is
     * the run structure a merge of append-only log segments produces.
     */
    private static void createIndex(Path path, int nDocs, int segments, int numHosts, int valueLength, int seed, boolean sorted)
        throws IOException {
        final int docsPerSegment = nDocs / segments;
        final int docsPerHostPerSegment = docsPerSegment / numHosts;
        final Random random = new Random(seed);

        final IndexWriterConfig config = baseConfig(sorted);
        // One flush per segment exactly, so the run structure is what the parameters say it is.
        config.setMergePolicy(NoMergePolicy.INSTANCE);
        config.setMaxBufferedDocs(docsPerSegment);
        config.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);

        try (Directory directory = FSDirectory.open(path); IndexWriter writer = new IndexWriter(directory, config)) {
            for (int segment = 0; segment < segments; segment++) {
                // Each segment covers its own window, so a host's documents never interleave across segments.
                final long segmentStart = BASE_TIMESTAMP + (long) segment * docsPerHostPerSegment * 1000L;
                for (int host = 0; host < numHosts; host++) {
                    final String hostName = String.format(Locale.ROOT, "host-%04d", host);
                    for (int i = 0; i < docsPerHostPerSegment; i++) {
                        final long timestamp = segmentStart + i * 1000L + random.nextInt(1000);
                        final Document doc = new Document();
                        doc.add(new SortedDocValuesField(HOSTNAME_FIELD, new BytesRef(hostName)));
                        doc.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, timestamp));
                        doc.add(new BinaryDocValuesField(MESSAGE_FIELD, new BytesRef(logLine(random, hostName, timestamp, valueLength))));
                        writer.addDocument(doc);
                    }
                }
                writer.flush();
                writer.commit();
            }
        }

        try (Directory directory = FSDirectory.open(path); DirectoryReader reader = DirectoryReader.open(directory)) {
            System.out.printf(Locale.ROOT, "[source index] built %d segments%n", reader.leaves().size());
        }
    }

    private static final String[] LEVELS = { "INFO", "WARN", "DEBUG", "ERROR" };
    private static final String[] SERVICES = { "checkout", "catalog", "payments", "search", "auth", "shipping" };
    private static final String[] MESSAGES = {
        "request completed",
        "cache miss, falling back to origin",
        "connection reset by peer, retrying",
        "slow query detected",
        "token refreshed" };

    /**
     * A log-like line, so ZSTD does a realistic amount of work at a realistic ratio. Random bytes would understate the
     * compression cost the splice avoids, and a constant string would overstate it.
     */
    private static String logLine(Random random, String host, long timestamp, int length) {
        final StringBuilder line = new StringBuilder(length + 64);
        line.append(timestamp)
            .append(' ')
            .append(host)
            .append(' ')
            .append(LEVELS[random.nextInt(LEVELS.length)])
            .append(" [")
            .append(SERVICES[random.nextInt(SERVICES.length)])
            .append("] ")
            .append(MESSAGES[random.nextInt(MESSAGES.length)])
            .append(" status=")
            .append(200 + random.nextInt(5))
            .append(" latency=")
            .append(random.nextInt(5000))
            .append("ms");
        // Pad with high-entropy tokens rather than spaces, so the tail does not compress away to nothing.
        while (line.length() < length) {
            line.append(" trace=").append(Long.toHexString(random.nextLong()));
        }
        line.setLength(length);
        return line.toString();
    }

    private static IndexWriterConfig baseConfig(boolean sorted) {
        final IndexWriterConfig config = new IndexWriterConfig();
        if (sorted) {
            // The LogsDB sort order, which is what makes runs of one segment's documents appear in the merged output.
            config.setIndexSort(
                new Sort(
                    new SortField(HOSTNAME_FIELD, SortField.Type.STRING, false),
                    new SortedNumericSortField(TIMESTAMP_FIELD, SortField.Type.LONG, true)
                )
            );
            config.setLeafSorter(DataStream.TIMESERIES_LEAF_READERS_SORTER);
        }
        final DocValuesFormat docValuesFormat = new ES95TSDBDocValuesFormat();
        config.setCodec(new Elasticsearch96Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return docValuesFormat;
            }
        });
        return config;
    }

    private static IndexWriterConfig mergeConfig(boolean sorted) {
        final IndexWriterConfig config = baseConfig(sorted);
        config.setMergePolicy(new LogByteSizeMergePolicy());
        return config;
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (path == null || Files.exists(path) == false) {
            return;
        }
        try (Stream<Path> walk = Files.walk(path)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }

    /**
     * Counts the blocks a merge spliced, by listening for the debug line the consumer emits per binary field. Reads the
     * event's message parameters rather than its formatted text, so rewording the message does not silently zero the
     * count — a changed parameter shape shows up as a count of zero next to a non-zero total, which is visible.
     */
    private static final class SpliceCounter extends AbstractAppender {

        private final LongAdder spliced = new LongAdder();
        private final LongAdder total = new LongAdder();

        private long lastSpliced;
        private long lastTotal;

        SpliceCounter() {
            super("splice-counter", null, null, true, null);
        }

        void install() {
            start();
            final LoggerContext context = (LoggerContext) LogManager.getContext(false);
            context.getConfiguration()
                .addLoggerAppender((org.apache.logging.log4j.core.Logger) LogManager.getLogger(CONSUMER_LOGGER), this);
            Configurator.setLevel(CONSUMER_LOGGER, org.apache.logging.log4j.Level.DEBUG);
        }

        void uninstall() {
            stop();
        }

        void reset() {
            spliced.reset();
            total.reset();
        }

        void snapshot() {
            lastSpliced = spliced.sum();
            lastTotal = total.sum();
        }

        @Override
        public void append(LogEvent event) {
            final Object[] parameters = event.getMessage().getParameters();
            if (parameters != null
                && parameters.length == 3
                && parameters[0] instanceof Integer splicedBlocks
                && parameters[1] instanceof Integer totalBlocks) {
                spliced.add(splicedBlocks);
                total.add(totalBlocks);
            }
        }
    }
}
