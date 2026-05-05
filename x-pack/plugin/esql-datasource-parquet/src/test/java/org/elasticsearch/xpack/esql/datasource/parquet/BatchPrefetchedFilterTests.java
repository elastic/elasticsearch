/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

/**
 * Verifies that {@link BatchPrefetchedFilter} collapses per-row-group dictionary and bloom-filter
 * I/O into a single coalesced batched read. The instrumented {@link StorageObject} counts every
 * {@code newStream} and {@code readBytesAsync} call, so the tests can assert the I/O reduction
 * directly rather than measuring wall time.
 *
 * <p>The fixture parquet file is built with very small row groups (4 KiB) so that 300 rows
 * produce multiple row groups; bloom filters are written for the columns that the filter
 * predicate touches.
 */
public class BatchPrefetchedFilterTests extends ESTestCase {

    /**
     * Lies inside the per-row-group string min/max range for at least one row group: the writer
     * emits "_name_0".."_name_4999" so "_name_2NOT_PRESENT" sorts between "_name_2..." and
     * "_name_3..." lexicographically. With this predicate, STATISTICS lets at least one row
     * group through, dictionary/bloom is what eliminates all matches, and the test can
     * meaningfully assert the batch prefetch fired.
     */
    private static final String NON_EXISTENT_NAME = "x".repeat(64) + "_name_2NOT_PRESENT";

    private static final MessageType SCHEMA = Types.buildMessage()
        .required(INT64)
        .named("id")
        .required(INT32)
        .named("value")
        .required(BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("name")
        .named("test_schema");

    public void testBatchPathEliminatesPerRowGroupReads() throws Exception {
        byte[] data = createFile();
        try (CountingStorageObject counter = new CountingStorageObject(data); ParquetFileReader reader = openReader(counter)) {
            List<BlockMetaData> blocks = reader.getRowGroups();
            assertTrue("test fixture must produce at least 3 row groups", blocks.size() >= 3);

            // Predicate "name == bloom-rejected literal" — the value sits inside the per-row-group
            // string min/max range (it's a typical "name_*" pattern with a non-existent suffix), so
            // STATISTICS lets every row group through and the dictionary/bloom filter is what runs
            // afterwards. With the batch prefetch, every dictionary + bloom byte for the predicate
            // column is fetched in one parallel coalesced dispatch.
            FilterPredicate predicate = FilterApi.eq(FilterApi.binaryColumn("name"), Binary.fromString(NON_EXISTENT_NAME));
            FilterCompat.Filter filter = FilterCompat.get(predicate);

            counter.reset();
            boolean[] flags = BatchPrefetchedFilter.computeSurvivingRowGroups(
                counter,
                blocks,
                filter,
                reader.getFileMetaData().getSchema(),
                new PlainCompressionCodecFactory()
            );

            assertNotNull("batch path should engage with a real StorageObject", flags);
            int kept = 0;
            for (boolean flag : flags) {
                if (flag) {
                    kept++;
                }
            }
            assertEquals("bloom filter should reject every row group for an absent value", 0, kept);

            // The whole point: one batched async dispatch (or a small number after coalescing
            // per merged range), never N_row_groups synchronous newStream calls.
            assertEquals("no synchronous newStream() during the batched filter pass", 0, counter.syncStreamCalls.get());
            assertTrue(
                "expected the batch prefetch to issue strictly fewer async calls than row groups (got "
                    + counter.asyncCalls.get()
                    + " calls for "
                    + blocks.size()
                    + " row groups)",
                counter.asyncCalls.get() >= 1 && counter.asyncCalls.get() < blocks.size()
            );

            // Parity check: the batch path's per-row-group survival flags must match what
            // parquet-mr's RowGroupFilter would return for the same three filter levels. If
            // these ever diverge, the batch optimization is no longer a drop-in replacement.
            List<BlockMetaData> survivingViaParquetMr = RowGroupFilter.filterRowGroups(
                List.of(RowGroupFilter.FilterLevel.STATISTICS, RowGroupFilter.FilterLevel.DICTIONARY, RowGroupFilter.FilterLevel.BLOOMFILTER),
                filter,
                reader.getRowGroups(),
                reader
            );
            int parquetMrKept = survivingViaParquetMr.size();
            assertEquals("batch path must agree with RowGroupFilter on surviving row group count", parquetMrKept, kept);
        }
    }

    public void testBatchPathFallsBackWithoutStorageObject() throws Exception {
        byte[] data = createFile();
        try (CountingStorageObject counter = new CountingStorageObject(data); ParquetFileReader reader = openReader(counter)) {
            FilterPredicate predicate = FilterApi.eq(FilterApi.longColumn("id"), 999_999L);
            boolean[] flags = BatchPrefetchedFilter.computeSurvivingRowGroups(
                null,
                reader.getRowGroups(),
                FilterCompat.get(predicate),
                reader.getFileMetaData().getSchema(),
                new PlainCompressionCodecFactory()
            );
            assertNull("null storage object should opt out of the batch path", flags);
        }
    }

    public void testBatchPathFallsBackOnNonPredicateFilter() throws Exception {
        byte[] data = createFile();
        try (CountingStorageObject counter = new CountingStorageObject(data); ParquetFileReader reader = openReader(counter)) {
            boolean[] flags = BatchPrefetchedFilter.computeSurvivingRowGroups(
                counter,
                reader.getRowGroups(),
                FilterCompat.NOOP,
                reader.getFileMetaData().getSchema(),
                new PlainCompressionCodecFactory()
            );
            // FilterCompat.NOOP is not a FilterPredicateCompat — opt out, caller has to handle.
            assertNull(flags);
        }
    }

    public void testBatchPathKeepsRowGroupsForMatchingPredicate() throws Exception {
        byte[] data = createFile();
        try (CountingStorageObject counter = new CountingStorageObject(data); ParquetFileReader reader = openReader(counter)) {
            FilterPredicate predicate = FilterApi.eq(FilterApi.longColumn("id"), 150L);
            boolean[] flags = BatchPrefetchedFilter.computeSurvivingRowGroups(
                counter,
                reader.getRowGroups(),
                FilterCompat.get(predicate),
                reader.getFileMetaData().getSchema(),
                new PlainCompressionCodecFactory()
            );
            assertNotNull(flags);
            int kept = 0;
            for (boolean flag : flags) {
                if (flag) {
                    kept++;
                }
            }
            // We expect statistics to retain at most the row group(s) that contain id=150.
            assertTrue("at least one row group must survive a matching predicate", kept >= 1);
            assertTrue("row groups outside [min,max] for 150 must be dropped", kept < flags.length);
        }
    }

    public void testBatchPathHandlesDictionaryOnlyColumn() throws Exception {
        // Dict-only path: predicate column ("value") has no bloom filter configured, so the
        // batch prefetch only fetches dictionary bytes. We pick a value far enough above the
        // global max that statistics would be the obvious answer — but to also exercise the
        // dictionary path we use a value that lands inside one row group's [min, max] span
        // yet isn't actually present in any dictionary.
        byte[] data = createFile();
        try (CountingStorageObject counter = new CountingStorageObject(data); ParquetFileReader reader = openReader(counter)) {
            List<BlockMetaData> blocks = reader.getRowGroups();
            // value = i*10 → max within a row group's slice is some multiple of 10.
            // Pick something between two consecutive multiples (e.g. 7) to land within
            // a min/max span yet outside every row group's dictionary.
            FilterPredicate predicate = FilterApi.eq(FilterApi.intColumn("value"), 7);
            FilterCompat.Filter filter = FilterCompat.get(predicate);

            counter.reset();
            boolean[] flags = BatchPrefetchedFilter.computeSurvivingRowGroups(
                counter,
                blocks,
                filter,
                reader.getFileMetaData().getSchema(),
                new PlainCompressionCodecFactory()
            );
            assertNotNull(flags);

            // Parity check against RowGroupFilter: the dict-only column path must agree.
            List<BlockMetaData> survivingViaParquetMr = RowGroupFilter.filterRowGroups(
                List.of(RowGroupFilter.FilterLevel.STATISTICS, RowGroupFilter.FilterLevel.DICTIONARY, RowGroupFilter.FilterLevel.BLOOMFILTER),
                filter,
                blocks,
                reader
            );
            int kept = 0;
            for (boolean flag : flags) {
                if (flag) {
                    kept++;
                }
            }
            assertEquals(survivingViaParquetMr.size(), kept);
            assertEquals("no synchronous reads in the batch dict-only path", 0, counter.syncStreamCalls.get());
        }
    }

    public void testCollectPredicateColumnsCoversAllOperators() {
        // Sanity check: a compound predicate visits every leaf column.
        FilterPredicate predicate = FilterApi.and(
            FilterApi.or(
                FilterApi.eq(FilterApi.longColumn("id"), 1L),
                FilterApi.gtEq(FilterApi.intColumn("value"), 5)
            ),
            FilterApi.notEq(FilterApi.binaryColumn("name"), Binary.fromString("foo"))
        );
        Set<String> columns = BatchPrefetchedFilter.collectPredicateColumns(predicate);
        assertEquals(Set.of("id", "value", "name"), columns);
    }

    public void testStatisticsOnlyPathSkipsIO() throws Exception {
        byte[] data = createFile();
        try (CountingStorageObject counter = new CountingStorageObject(data); ParquetFileReader reader = openReader(counter)) {
            // value column has no bloom filter and writer was small enough that there is a
            // dictionary; using a predicate value outside the global range lets STATISTICS alone
            // rule out every row group.
            FilterPredicate predicate = FilterApi.gt(FilterApi.intColumn("value"), 1_000_000);
            counter.reset();
            boolean[] flags = BatchPrefetchedFilter.computeSurvivingRowGroups(
                counter,
                reader.getRowGroups(),
                FilterCompat.get(predicate),
                reader.getFileMetaData().getSchema(),
                new PlainCompressionCodecFactory()
            );
            assertNotNull(flags);
            int kept = 0;
            for (boolean flag : flags) {
                if (flag) {
                    kept++;
                }
            }
            assertEquals(0, kept);

            // The whole point of running STATISTICS up front: when min/max alone rules out
            // every row group, no dictionary or bloom byte ever needs to leave remote storage.
            assertEquals("no synchronous reads during the stats-only fast path", 0, counter.syncStreamCalls.get());
            assertEquals("no async reads during the stats-only fast path", 0, counter.asyncCalls.get());
        }
    }

    // -------------------- helpers --------------------

    private static ParquetFileReader openReader(StorageObject object) throws IOException {
        return ParquetFileReader.open(
            new ParquetStorageObjectAdapter(object),
            PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build()
        );
    }

    /**
     * Builds an in-memory parquet file with bloom filters on {@code id} and {@code name},
     * sized so that 300 rows produce multiple row groups.
     */
    private static byte[] createFile() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(out);
        SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new org.apache.parquet.conf.PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(SCHEMA)
                .withRowGroupSize(4096)
                .withPageSize(1024)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withBloomFilterEnabled("id", true)
                .withBloomFilterEnabled("name", true)
                .withBloomFilterNDV("id", 100L)
                .withBloomFilterNDV("name", 100L)
                .build()
        ) {
            // Padding + many rows ensures the writer produces >= 3 row groups against the
            // 4 KiB row-group limit; the writer's rotation tracks the in-memory column store
            // size, not the on-disk size, so we need both more rows and longer values to push
            // it past the limit reliably.
            String pad = "x".repeat(64);
            for (int i = 0; i < 5_000; i++) {
                writer.write(factory.newGroup().append("id", (long) i).append("value", i * 10).append("name", pad + "_name_" + i));
            }
        }
        return out.toByteArray();
    }

    /**
     * StorageObject decorator that increments a counter every time a synchronous
     * {@code newStream} or asynchronous {@code readBytesAsync} call is made. The default
     * {@code readBytesAsync} implementation funnels back through {@code newStream}, so the
     * test code carefully separates "filter pass" calls from the prior open/parse traffic via
     * {@link #reset()}.
     */
    private static final class CountingStorageObject implements StorageObject, AutoCloseable {
        final AtomicInteger syncStreamCalls = new AtomicInteger();
        final AtomicInteger asyncCalls = new AtomicInteger();
        private final byte[] data;

        CountingStorageObject(byte[] data) {
            this.data = data;
        }

        void reset() {
            syncStreamCalls.set(0);
            asyncCalls.set(0);
        }

        @Override
        public InputStream newStream() {
            syncStreamCalls.incrementAndGet();
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            syncStreamCalls.incrementAndGet();
            int pos = (int) position;
            int len = (int) Math.min(length, data.length - position);
            return new ByteArrayInputStream(data, pos, len);
        }

        @Override
        public void readBytesAsync(long position, long length, Executor executor, ActionListener<ByteBuffer> listener) {
            asyncCalls.incrementAndGet();
            // Serve the bytes directly without going through newStream, so the sync counter
            // measures only synchronous reads.
            int pos = (int) position;
            int len = (int) Math.min(length, data.length - position);
            byte[] copy = new byte[len];
            System.arraycopy(data, pos, copy, 0, len);
            listener.onResponse(ByteBuffer.wrap(copy));
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public Instant lastModified() {
            return Instant.now();
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of("memory://batch_filter_test.parquet");
        }

        @Override
        public void close() {
            // nothing
        }
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream outputStream) {
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
                    public void write(int b) throws IOException {
                        outputStream.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        outputStream.write(b, off, len);
                        position += len;
                    }

                    @Override
                    public void close() throws IOException {
                        outputStream.close();
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

            @Override
            public String getPath() {
                return "memory://batch_filter_test.parquet";
            }
        };
    }
}
