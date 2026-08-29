/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests that the optimized Parquet reader's prefetch pipeline correctly integrates with
 * the circuit breaker: reserving memory before async I/O, releasing on clear/cancel/failure,
 * and gracefully skipping prefetch when the breaker limit would be exceeded.
 */
public class PrefetchCircuitBreakerTests extends ESTestCase {

    private static final MessageType SCHEMA = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.INT64)
        .named("id")
        .required(PrimitiveType.PrimitiveTypeName.INT32)
        .named("value")
        .named("test_schema");

    /**
     * Reads a multi-row-group file with a limited breaker and verifies the breaker
     * returns to zero after full iteration completes.
     */
    public void testPrefetchReservesAndReleasesBreaker() throws Exception {
        byte[] parquetData = createMultiRowGroupFile(3000, 2048);
        var breaker = new TrackingBreaker("test", ByteSizeValue.ofMb(50));
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();

        StorageObject storage = createAsyncStorageObject(parquetData);
        int totalRows = 0;
        try (CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory, true).read(storage, FormatReadContext.of(null, 1024))) {
            while (iter.hasNext()) {
                Page page = iter.next();
                totalRows += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        assertTrue("Should have read rows", totalRows > 0);
        assertTrue("Breaker should have been used for prefetch", breaker.peakUsed.get() > 0);
        assertEquals("Breaker should return to zero after iteration", 0, breaker.getUsed());
    }

    /**
     * Verifies the optimized reader works correctly with a tight breaker limit. The prefetch
     * competes with Parquet-mr decode allocations and ESQL block creation for the same breaker
     * budget. The query may either complete normally (prefetch skipped for some row groups) or
     * throw a CircuitBreakingException if decode allocations exceed the limit. Either outcome
     * is acceptable — the key assertion is that the breaker returns to zero.
     */
    public void testPrefetchWithTightBreakerLimit() throws Exception {
        MessageType wideSchema = buildWideSchema(10);
        byte[] parquetData = createMultiRowGroupFile(wideSchema, 5000, 50 * 1024);
        // Cover the clamped window (file length, or 4 MiB if the object is larger) and leave ~2 MB
        // so decode or prefetch allocations may still trip the breaker.
        long windowCharge = Math.min(parquetData.length, ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE);
        var breaker = new TrackingBreaker("test", ByteSizeValue.ofBytes(windowCharge + 2 * 1024 * 1024));
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();

        StorageObject storage = createAsyncStorageObject(parquetData);
        try (CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory, true).read(storage, FormatReadContext.of(null, 1024))) {
            while (iter.hasNext()) {
                try {
                    Page page = iter.next();
                    page.releaseBlocks();
                } catch (CircuitBreakingException e) {
                    break;
                }
            }
        }
        assertEquals("Breaker should return to zero", 0, breaker.getUsed());
    }

    /**
     * Uses a failing storage object to test that the breaker reservation is released
     * when async prefetch I/O fails.
     */
    public void testPrefetchReleasesOnIOFailure() throws Exception {
        byte[] parquetData = createMultiRowGroupFile(2000, 2048);
        var breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(50));
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();

        StorageObject storage = createFailingAsyncStorageObject(parquetData);
        int totalRows = 0;
        try (CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory, true).read(storage, FormatReadContext.of(null, 1024))) {
            while (iter.hasNext()) {
                Page page = iter.next();
                totalRows += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        assertTrue("Should have read rows via sync fallback", totalRows > 0);
        assertEquals("Breaker should return to zero after iteration with failures", 0, breaker.getUsed());
    }

    /**
     * Tracks the maximum breaker usage during iteration and verifies it stays bounded
     * to approximately one row group's worth of prefetch data.
     */
    public void testBreakerUsageDuringIteration() throws Exception {
        byte[] parquetData = createMultiRowGroupFile(5000, 2048);
        var breaker = new TrackingBreaker("test", ByteSizeValue.ofMb(50));
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();

        StorageObject storage = createAsyncStorageObject(parquetData);
        int totalRows = 0;
        try (CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory, true).read(storage, FormatReadContext.of(null, 1024))) {
            while (iter.hasNext()) {
                Page page = iter.next();
                totalRows += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        assertTrue("Should have read rows", totalRows > 0);
        assertEquals("Breaker should return to zero", 0, breaker.getUsed());
        // Peak includes the clamped window (file length when the object fits, else 4 MiB) plus
        // prefetch/decode. Bound against that window, not the historical 4 MiB floor.
        long windowCharge = Math.min(parquetData.length, ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE);
        assertTrue(
            "Peak prefetch breaker usage should be bounded (was " + breaker.peakUsed + " bytes)",
            breaker.peakUsed.get() < parquetData.length + windowCharge
        );
    }

    /**
     * Closes the iterator mid-iteration (before exhausting all row groups) and verifies
     * the breaker returns to zero — catches leak paths on early close / query abort.
     */
    public void testPrefetchReleasedOnEarlyClose() throws Exception {
        byte[] parquetData = createMultiRowGroupFile(5000, 2048);
        var breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(50));
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();

        StorageObject storage = createAsyncStorageObject(parquetData);
        try (CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory, true).read(storage, FormatReadContext.of(null, 1024))) {
            if (iter.hasNext()) {
                Page page = iter.next();
                page.releaseBlocks();
            }
        }
        assertEquals("Breaker should return to zero after early close", 0, breaker.getUsed());
    }

    /**
     * N concurrent optimized readers share one breaker and one Arrow allocator. After every
     * thread finishes iterating, both accounts must return to baseline. Uses zstd so the
     * decompress path is exercised under concurrency, not only uncompressed prefetch.
     * Peak is not compared to N times a single-reader peak: N concurrent windows can
     * legitimately reach about N times one window. Sharing is the accounts returning to zero.
     */
    public void testConcurrentReadersShareBreaker() throws Exception {
        int readers = 4;
        ConcurrentPipeline pipeline = newConcurrentPipeline(3000);

        startInParallel(readers, i -> {
            int totalRows = 0;
            try {
                try (
                    CloseableIterator<Page> iter = new ParquetFormatReader(pipeline.blockFactory, true).read(
                        pipeline.storage(),
                        FormatReadContext.of(null, 1024)
                    )
                ) {
                    while (iter.hasNext()) {
                        Page page = iter.next();
                        totalRows += page.getPositionCount();
                        page.releaseBlocks();
                    }
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
            assertTrue("Should have read rows", totalRows > 0);
        });
        assertEquals("Breaker should return to zero after concurrent readers finish", 0, pipeline.breaker.getUsed());
        assertEquals(
            "Arrow allocator should return to baseline after concurrent readers finish",
            pipeline.allocBaseline,
            pipeline.blockFactory.arrowAllocator().getAllocatedMemory()
        );
        assertTrue("Prefetch should have reserved breaker bytes", pipeline.breaker.peakUsed.get() > 0);
    }

    /**
     * Concurrent early close of the optimized iterator after a synchronized partial read.
     * Each worker consumes one page, waits so all hold an open iterator, then closes on the
     * iterating thread. This is not {@code AsyncExternalSourceBuffer.discardPages} cancel
     * (covered in {@code AsyncExternalSourceBufferTests}) and not other-thread close.
     */
    public void testConcurrentEarlyCloseReclaimsMemory() throws Exception {
        int readers = 4;
        ConcurrentPipeline pipeline = newConcurrentPipeline(5000);

        CyclicBarrier hold = new CyclicBarrier(readers);
        startInParallel(readers, i -> {
            try {
                try (
                    CloseableIterator<Page> iter = new ParquetFormatReader(pipeline.blockFactory, true).read(
                        pipeline.storage(),
                        FormatReadContext.of(null, 1024)
                    )
                ) {
                    assertTrue("optimized reader must produce a page before early close", iter.hasNext());
                    Page page = iter.next();
                    assertTrue("page must contain rows", page.getPositionCount() > 0);
                    page.releaseBlocks();
                    hold.await(30, TimeUnit.SECONDS);
                }
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        assertEquals("Breaker should return to zero after concurrent early close", 0, pipeline.breaker.getUsed());
        assertEquals(
            "Arrow allocator should return to baseline after concurrent early close",
            pipeline.allocBaseline,
            pipeline.blockFactory.arrowAllocator().getAllocatedMemory()
        );
        assertTrue("Prefetch should have reserved breaker bytes before early close", pipeline.breaker.peakUsed.get() > 0);
    }

    // --- Helpers ---

    /**
     * Breaker that tracks peak usage for assertions.
     */
    static class TrackingBreaker extends LimitedBreaker {
        final AtomicLong peakUsed = new AtomicLong(0);

        TrackingBreaker(String name, ByteSizeValue limit) {
            super(name, limit);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            super.addEstimateBytesAndMaybeBreak(bytes, label);
            peakUsed.updateAndGet(peak -> Math.max(peak, getUsed()));
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            super.addWithoutBreaking(bytes);
            if (bytes > 0) {
                peakUsed.updateAndGet(peak -> Math.max(peak, getUsed()));
            }
        }
    }

    private StorageObject createAsyncStorageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new ByteArrayInputStream(data, (int) position, (int) Math.min(length, data.length - position));
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
                return StoragePath.of("memory://breaker-test.parquet");
            }

            @Override
            public void readBytesAsync(
                long position,
                long length,
                DirectBufferFactory factory,
                Executor executor,
                ActionListener<DirectReadBuffer> listener
            ) {
                executor.execute(() -> {
                    try {
                        int pos = (int) position;
                        int len = (int) Math.min(length, data.length - position);
                        ByteBuffer buffer = ByteBuffer.allocate(len);
                        buffer.put(data, pos, len);
                        buffer.flip();
                        listener.onResponse(new DirectReadBuffer(buffer, () -> {}));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                });
            }
        };
    }

    /**
     * Storage object whose async reads always fail, forcing the prefetch pipeline
     * to fall back to synchronous I/O. Sync reads work normally.
     */
    private StorageObject createFailingAsyncStorageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new ByteArrayInputStream(data, (int) position, (int) Math.min(length, data.length - position));
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
                return StoragePath.of("memory://failing-async-test.parquet");
            }

            @Override
            public void readBytesAsync(
                long position,
                long length,
                DirectBufferFactory factory,
                Executor executor,
                ActionListener<DirectReadBuffer> listener
            ) {
                executor.execute(() -> listener.onFailure(new IOException("Simulated async I/O failure")));
            }
        };
    }

    private static MessageType buildWideSchema(int numColumns) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (int c = 0; c < numColumns; c++) {
            builder.required(PrimitiveType.PrimitiveTypeName.INT64).named("col_" + c);
        }
        return builder.named("wide_schema");
    }

    /**
     * Allocator-backed in-memory storage: default {@code readBytesAsync} allocates through
     * {@link DirectBufferFactory}. Distinct from {@link #createAsyncStorageObject}, which uses a
     * heap {@code ByteBuffer} and a no-op closer so older breaker tests do not charge the breaker for
     * prefetch bytes. Do not merge the two stubs.
     */
    private static final class InMemoryStorageObject implements StorageObject {
        private final byte[] data;

        InMemoryStorageObject(byte[] data) {
            this.data = data;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            return new ByteArrayInputStream(data, (int) position, (int) Math.min(length, data.length - position));
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
        public StoragePath path() {
            return StoragePath.of("memory://breaker-concurrent.parquet");
        }
    }

    private ConcurrentPipeline newConcurrentPipeline(int rowCount) throws IOException {
        byte[] parquetData = createMultiRowGroupFile(buildWideSchema(8), rowCount, 2048, CompressionCodecName.ZSTD);
        var breaker = new TrackingBreaker("test", ByteSizeValue.ofMb(256));
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
        return new ConcurrentPipeline(parquetData, breaker, blockFactory, blockFactory.arrowAllocator().getAllocatedMemory());
    }

    private record ConcurrentPipeline(byte[] parquetData, TrackingBreaker breaker, BlockFactory blockFactory, long allocBaseline) {
        StorageObject storage() {
            return new InMemoryStorageObject(parquetData);
        }
    }

    private byte[] createMultiRowGroupFile(int rowCount, int rowGroupSize) throws IOException {
        return createMultiRowGroupFile(SCHEMA, rowCount, rowGroupSize, CompressionCodecName.UNCOMPRESSED);
    }

    private byte[] createMultiRowGroupFile(MessageType schema, int rowCount, int rowGroupSize) throws IOException {
        return createMultiRowGroupFile(schema, rowCount, rowGroupSize, CompressionCodecName.UNCOMPRESSED);
    }

    private byte[] createMultiRowGroupFile(MessageType schema, int rowCount, int rowGroupSize, CompressionCodecName codec)
        throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(outputStream);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        String[] columns = new String[schema.getFieldCount()];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = schema.getFieldName(i);
        }
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(codec)
                .withRowGroupSize(rowGroupSize)
                .withPageSize(256)
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                Group g = groupFactory.newGroup();
                for (String col : columns) {
                    switch (schema.getType(col).asPrimitiveType().getPrimitiveTypeName()) {
                        case INT64 -> g.add(col, (long) i);
                        case INT32 -> g.add(col, i * 10);
                        default -> throw new IllegalArgumentException("Unsupported type");
                    }
                }
                writer.write(g);
            }
        }
        return outputStream.toByteArray();
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream outputStream) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    @Override
                    public long getPos() {
                        return outputStream.size();
                    }

                    @Override
                    public void write(int b) {
                        outputStream.write(b);
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        outputStream.write(b, off, len);
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
                return "memory://breaker-test.parquet";
            }
        };
    }
}
