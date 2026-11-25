/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.benchmark.esql.datasources;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.ValuesBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
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
import org.openjdk.jmh.annotations.Warmup;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Reads a dictionary-encoded Parquet string column through {@link ParquetFormatReader} and, optionally,
 * groups by it, to measure what threshold-based ordinal-dictionary compaction costs on the read side
 * and saves downstream. The layouts cover the cases where compaction does nothing, where it helps
 * most, and the threshold boundary:
 * <ul>
 *   <li>{@code small_random}: a small dictionary in random order; every batch references every entry,
 *       so batches share the row-group dictionary as before.</li>
 *   <li>{@code large_clustered}: a dictionary much larger than a batch with clustered values; each
 *       batch references a small slice of it.</li>
 *   <li>{@code random_500} and {@code random_1000}: random order with a dictionary between half the
 *       batch size and the batch size. Each batch references most entries and therefore shares the
 *       row-group dictionary under the threshold rule; the ordinal blocks of {@code random_1000}
 *       are not dense.</li>
 *   <li>{@code d<D>_w<W>}: a dictionary of D entries where the rows of each batch are drawn at random
 *       from a window of W consecutive entries, the window moving on by W with every batch (wrapping
 *       around the dictionary). They fill the range between the layouts above, so the fraction of the
 *       dictionary a batch references can be varied on its own; {@code d700_w700} is random over a
 *       700-entry dictionary. The setup prints the measured mean fraction a batch references.</li>
 *   <li>{@code transition_pause_max} and {@code transition_pause_min}: one 1,000-entry dictionary,
 *       with batches that reference exactly 650 entries before switching to exactly 20. They retain
 *       the longest- and shortest-delay layouts used to compare an eight-batch back-off; with the
 *       selected four-batch back-off they exercise four and one delayed sparse batches.</li>
 * </ul>
 * The setup logs the bytes the emitted blocks hold when every page of a read is buffered at once, as
 * an async consumer may do.
 */
@Fork(1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class ParquetOrdinalDictionaryBenchmark {

    private static final int BATCH_SIZE = 1000;
    private static final int ROW_COUNT = 100_000;
    private static final int SELF_TEST_ROW_COUNT = 20_000;
    private static final String COLUMN = "name";

    private static final GroupingAggregatorFunction.AddInput NOOP_ADD_INPUT = new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {}

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {}

        @Override
        public void add(int positionOffset, IntVector groupIds) {}

        @Override
        public void close() {}
    };

    static {
        // BlockFactory and the Parquet reader get static loggers, which need the provider installed first.
        BenchmarkLogging.configure();
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            selfTest();
        }
    }

    @Param(
        {
            "small_random",
            "large_clustered",
            "random_500",
            "random_1000",
            "d700_w700",
            "d1000_w650",
            "d1000_w250",
            "d2000_w200",
            "transition_pause_max",
            "transition_pause_min" }
    )
    public String layout;

    @Param({ "read", "hash", "values" })
    public String consumer;

    private final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(NoopCircuitBreaker.INSTANCE)
        .build();
    private StorageObject storageObject;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        BenchmarkLogging.configure();
        setup(ROW_COUNT);
        System.out.printf(Locale.ROOT, "%nlayout [%s]: mean referenced fraction per batch [%.3f]%n", layout, referencedFraction(layout));
        // Printed rather than logged: benchmark logging is quiet at info, and this is a per-trial measurement.
        System.out.printf(
            Locale.ROOT,
            "%nlayout [%s]: [%,d] bytes held by the blocks of one fully buffered read%n",
            layout,
            retainedBytes()
        );
    }

    private void setup(int rowCount) throws IOException {
        storageObject = new InMemoryStorageObject(fixture(layout, rowCount));
    }

    @Benchmark
    public int readColumn() throws IOException {
        return switch (consumer) {
            case "read" -> read();
            case "hash" -> hash();
            case "values" -> values();
            default -> throw new IllegalArgumentException("unknown consumer [" + consumer + "]");
        };
    }

    /**
     * Collects the column's values into one group, as {@code STATS VALUES(name)} would. Unlike the
     * hash consumer, {@code VALUES} hashes the whole dictionary of every ordinal page with no
     * density check, so it pays for every entry a page carries. Returns the number of distinct values.
     */
    private int values() throws IOException {
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        try (
            Operator aggregation = new HashAggregationOperator.Builder().mode(AggregatorMode.INITIAL)
                .aggregators(
                    List.of(new ValuesBytesRefAggregatorFunctionSupplier().groupingAggregatorFactory(AggregatorMode.INITIAL, List.of(0)))
                )
                .groups(List.of(new BlockHash.GroupSpec(1, ElementType.LONG)))
                .partialEmit(1024, 1.0)
                .maxPageSize(Integer.MAX_VALUE)
                .build()
                .get(driverContext);
            CloseableIterator<Page> pages = open(blockFactory)
        ) {
            while (pages.hasNext()) {
                Page page = pages.next();
                aggregation.addInput(page.appendBlock(blockFactory.newConstantLongBlockWith(0, page.getPositionCount())));
            }
            aggregation.finish();
            int distinct = 0;
            for (Page out = aggregation.getOutput(); out != null; out = aggregation.getOutput()) {
                try {
                    // Intermediate output: the group key, then the VALUES state, one position per group.
                    distinct += out.getBlock(1).getTotalValueCount();
                } finally {
                    out.releaseBlocks();
                }
            }
            return distinct;
        }
    }

    private int read() throws IOException {
        int rows = 0;
        try (CloseableIterator<Page> pages = open(blockFactory)) {
            while (pages.hasNext()) {
                Page page = pages.next();
                rows += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        return rows;
    }

    /** Groups by the column, as {@code STATS ... BY name} would. Returns the number of groups. */
    private int hash() throws IOException {
        try (
            BlockHash hash = BlockHash.build(List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF)), blockFactory, 16 * 1024, false);
            CloseableIterator<Page> pages = open(blockFactory)
        ) {
            while (pages.hasNext()) {
                Page page = pages.next();
                try {
                    hash.add(page, NOOP_ADD_INPUT);
                } finally {
                    page.releaseBlocks();
                }
            }
            return hash.numKeys();
        }
    }

    /** Breaker bytes held by the blocks of one read when every page is kept until the end. */
    private long retainedBytes() throws IOException {
        HierarchyCircuitBreakerService breakers = new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            Settings.EMPTY,
            List.of(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        CircuitBreaker breaker = breakers.getBreaker(CircuitBreaker.REQUEST);
        // Dictionary bytes live in BigArrays, so they must reach the same breaker as the blocks.
        BigArrays bigArrays = new BigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakers, CircuitBreaker.REQUEST)
            .withCircuitBreaking();
        BlockFactory tracking = BlockFactory.builder(bigArrays).breaker(breaker).build();
        List<Page> buffered = new ArrayList<>();
        try {
            try (CloseableIterator<Page> pages = open(tracking)) {
                while (pages.hasNext()) {
                    buffered.add(pages.next());
                }
            }
            return breaker.getUsed();
        } finally {
            buffered.forEach(Page::releaseBlocks);
        }
    }

    private CloseableIterator<Page> open(BlockFactory factory) throws IOException {
        FormatReadContext context = FormatReadContext.builder().projectedColumns(List.of(COLUMN)).batchSize(BATCH_SIZE).build();
        return new ParquetFormatReader(factory).read(storageObject, context);
    }

    static void selfTest() {
        for (String layout : Utils.possibleValues(ParquetOrdinalDictionaryBenchmark.class, "layout")) {
            ParquetOrdinalDictionaryBenchmark bench = new ParquetOrdinalDictionaryBenchmark();
            bench.layout = layout;
            try {
                // fixture() already fails if the writer fell back from dictionary encoding.
                TransitionLayout transition = transitionLayout(layout);
                int rowCount = transition == null ? SELF_TEST_ROW_COUNT : (transition.sparseStartBatch() + 1) * BATCH_SIZE;
                bench.setup(rowCount);
                for (String consumer : Utils.possibleValues(ParquetOrdinalDictionaryBenchmark.class, "consumer")) {
                    bench.consumer = consumer;
                    int result = bench.readColumn();
                    int expected = consumer.equals("read") ? rowCount : distinctValues(layout, rowCount);
                    if (result != expected) {
                        throw new AssertionError("[" + layout + "/" + consumer + "] returned " + result + ", expected " + expected);
                    }
                }
            } catch (IOException e) {
                throw new AssertionError("[" + layout + "] failed", e);
            }
        }
    }

    /** Layouts named {@code d<D>_w<W>}: D dictionary entries, each batch draws from a window of W of them. */
    private static int[] windowLayout(String layout) {
        if (layout.startsWith("d") == false) {
            return null;
        }
        String[] parts = layout.substring(1).split("_w");
        return new int[] { Integer.parseInt(parts[0]), Integer.parseInt(parts[1]) };
    }

    private static TransitionLayout transitionLayout(String layout) {
        return switch (layout) {
            // Pause-8 probes are batches 0, 9, 18, 27, 36, 45, 54, ...
            case "transition_pause_max" -> new TransitionLayout(1000, 46);
            case "transition_pause_min" -> new TransitionLayout(1000, 54);
            default -> null;
        };
    }

    /** Mean fraction of the dictionary a batch references, replaying the fixture's random sequence. */
    private static double referencedFraction(String layout) {
        Random random = new Random(42);
        int dictionary = distinctValues(layout, ROW_COUNT);
        int batches = ROW_COUNT / BATCH_SIZE;
        double sum = 0;
        for (int batch = 0; batch < batches; batch++) {
            BitSet referenced = new BitSet();
            for (int row = 0; row < BATCH_SIZE; row++) {
                referenced.set(valueIndex(layout, batch * BATCH_SIZE + row, random));
            }
            sum += referenced.cardinality() / (double) dictionary;
        }
        return sum / batches;
    }

    private static int distinctValues(String layout, int rowCount) {
        TransitionLayout transition = transitionLayout(layout);
        if (transition != null) {
            return transition.dictionarySize();
        }
        int[] window = windowLayout(layout);
        if (window != null) {
            return window[0];
        }
        return switch (layout) {
            case "small_random" -> 64;
            case "large_clustered" -> rowCount / 5;
            case "random_500" -> 500;
            case "random_1000" -> 1000;
            default -> throw new IllegalArgumentException("unknown layout [" + layout + "]");
        };
    }

    /** Value index of each row: random over the dictionary, or runs of five rows per value. */
    private static int valueIndex(String layout, int row, Random random) {
        TransitionLayout transition = transitionLayout(layout);
        if (transition != null) {
            int batch = row / BATCH_SIZE;
            int position = row % BATCH_SIZE;
            int entries = batch < transition.sparseStartBatch() ? 650 : 20;
            int start = (batch * 650) % transition.dictionarySize();
            // Seed every entry in the selected window once, then fill the rest at random. This makes
            // "650 entries" and "20 entries" exact cardinalities rather than random-window sizes.
            return (start + (position < entries ? position : random.nextInt(entries))) % transition.dictionarySize();
        }
        int[] window = windowLayout(layout);
        if (window != null) {
            int start = (int) (((long) (row / BATCH_SIZE) * window[1]) % window[0]);
            return (start + random.nextInt(window[1])) % window[0];
        }
        return switch (layout) {
            case "large_clustered" -> row / 5;
            case "small_random", "random_500", "random_1000" -> random.nextInt(distinctValues(layout, 0));
            default -> throw new IllegalArgumentException("unknown layout [" + layout + "]");
        };
    }

    private static byte[] fixture(String layout, int rowCount) throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named(COLUMN)
            .named("ordinal_dictionary_bench");
        // A fixed seed keeps the fixture identical across runs; with at least 20 rows per value, every value
        // shows up, which the self-test checks through the hash consumer's group count.
        Random random = new Random(42);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
            .withConf(new PlainParquetConfiguration())
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .withDictionaryEncoding(true)
            .build();
        try (writer) {
            for (int row = 0; row < rowCount; row++) {
                Group g = factory.newGroup();
                g.add(COLUMN, String.format(Locale.ROOT, "value-%018d", valueIndex(layout, row, random)));
                writer.write(g);
            }
        }
        // The writer silently falls back to PLAIN when it judges the dictionary not worth it; then the reader emits
        // plain blocks and the benchmark measures nothing about ordinal dictionaries.
        for (BlockMetaData rowGroup : writer.getFooter().getBlocks()) {
            for (ColumnChunkMetaData column : rowGroup.getColumns()) {
                if (column.getEncodingStats().hasNonDictionaryEncodedPages()) {
                    throw new AssertionError("[" + layout + "] fixture has non-dictionary-encoded pages");
                }
            }
        }
        if (writer.getFooter().getBlocks().size() != 1) {
            throw new AssertionError("[" + layout + "] fixture has [" + writer.getFooter().getBlocks().size() + "] row groups");
        }
        return out.toByteArray();
    }

    private record TransitionLayout(int dictionarySize, int sparseStartBatch) {}

    private static final class InMemoryStorageObject implements StorageObject {
        private record BenchIdentity() implements StorageIdentity {}

        private final byte[] data;

        InMemoryStorageObject(byte[] data) {
            this.data = data;
        }

        @Override
        public void attachMetrics(ExternalSourceMetrics metrics, String scheme) {}

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            int pos = Math.toIntExact(Math.min(position, data.length));
            return new ByteArrayInputStream(data, pos, (int) Math.min(length, data.length - pos));
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StorageIdentity storageIdentity() {
            return new BenchIdentity();
        }

        @Override
        public StoragePath path() {
            return StoragePath.of("memory://ordinal_dictionary_bench.parquet");
        }
    }

    private static OutputFile outputFile(ByteArrayOutputStream out) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) {
                        out.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        out.write(b, off, len);
                        position += len;
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };
    }
}
