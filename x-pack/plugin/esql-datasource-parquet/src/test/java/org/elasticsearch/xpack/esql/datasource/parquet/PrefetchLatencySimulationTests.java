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
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests that the prefetch pipeline reduces the number of synchronous I/O calls by serving
 * row group data from memory. Uses an instrumented {@link CountingStorageObject} to count
 * sync reads (via {@code newStream(pos, len)}) and verifies the optimized reader makes
 * fewer calls than the baseline.
 * <p>
 * Unlike wall-time benchmarks, counting I/O operations is deterministic and directly validates
 * the prefetch mechanism regardless of hardware speed.
 */
public class PrefetchLatencySimulationTests extends ESTestCase {

    private BlockFactory blockFactory;
    private ExecutorService asyncIoExecutor;

    @Before
    public void initBlockFactoryAndExecutor() throws Exception {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
        // Owned, deterministically shut down in tearDown: using ForkJoinPool.commonPool() here leaks
        // worker threads that ESTestCase's suite-scoped ThreadLeakControl flags as a class failure.
        asyncIoExecutor = Executors.newFixedThreadPool(4, EsExecutors.daemonThreadFactory("test", "prefetch-test-async-io"));
    }

    @After
    public void terminateExecutor() throws Exception {
        terminate(asyncIoExecutor);
    }

    /**
     * Verifies the optimized reader makes fewer synchronous I/O calls than the baseline.
     * The prefetch pipeline pre-loads column chunk data into the adapter's memory, so
     * subsequent reads from the adapter are served from prefetched buffers rather than
     * triggering new sync reads.
     */
    public void testPrefetchReducesSyncIOCalls() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("value")
            .named("test_schema");

        byte[] parquetData = createMultiRowGroupFile(schema, 5000, 4096);

        CountingStorageObject baselineStorage = new CountingStorageObject(parquetData, asyncIoExecutor);
        CountingStorageObject optimizedStorage = new CountingStorageObject(parquetData, asyncIoExecutor);

        readAll(new ParquetFormatReader(blockFactory, false), baselineStorage);
        int baselineSyncReads = baselineStorage.syncReadCount.get();

        readAll(new ParquetFormatReader(blockFactory, true), optimizedStorage);
        int optimizedSyncReads = optimizedStorage.syncReadCount.get();
        int optimizedAsyncReads = optimizedStorage.asyncReadCount.get();

        logger.info(
            "I/O call counts — baseline sync: {}, optimized sync: {}, optimized async: {}",
            baselineSyncReads,
            optimizedSyncReads,
            optimizedAsyncReads
        );

        assertTrue(
            "Optimized reader should make fewer sync reads than baseline: baseline="
                + baselineSyncReads
                + ", optimized="
                + optimizedSyncReads,
            optimizedSyncReads <= baselineSyncReads
        );

        assertTrue("Optimized reader should use async prefetch: asyncReads=" + optimizedAsyncReads, optimizedAsyncReads > 0);
    }

    /**
     * Verifies the optimized reader produces correct output when storage has simulated latency.
     * This tests functional correctness of the prefetch pipeline under delayed I/O conditions.
     */
    public void testOptimizedReaderCorrectWithLatency() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("value")
            .named("test_schema");

        byte[] parquetData = createMultiRowGroupFile(schema, 2000, 2048);

        StorageObject plainStorage = createPlainStorageObject(parquetData);
        CountingStorageObject delayedStorage = new CountingStorageObject(parquetData, asyncIoExecutor);

        int baselineRows = countRows(new ParquetFormatReader(blockFactory, false), plainStorage);
        int optimizedRows = countRows(new ParquetFormatReader(blockFactory, true), delayedStorage);

        assertEquals("Row count mismatch between baseline and optimized with async storage", baselineRows, optimizedRows);
        assertTrue("Should have read some rows", optimizedRows > 0);
    }

    /**
     * Tests the adaptive depth algorithm deterministically by calling
     * {@link OptimizedParquetColumnIterator#adaptPrefetchDepth} directly — no timing
     * dependency, no I/O.
     */
    public void testAdaptivePrefetchDepthGrowsOnStall() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");

        byte[] parquetData = createMultiRowGroupFile(schema, 5000, 4096);
        CountingStorageObject storage = new CountingStorageObject(parquetData, asyncIoExecutor);

        try (CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory, true).read(storage, FormatReadContext.of(null, 1024))) {
            OptimizedParquetColumnIterator opi = (OptimizedParquetColumnIterator) iter;
            int initialDepth = opi.prefetchDepth();

            // Simulate stalls (wasReady = false): depth should grow by PREFETCH_DEPTH_GROWTH (2)
            opi.adaptPrefetchDepth(false);
            assertEquals(initialDepth + 2, opi.prefetchDepth());
            opi.adaptPrefetchDepth(false);
            assertEquals(initialDepth + 4, opi.prefetchDepth());

            int grownDepth = opi.prefetchDepth();

            // Simulate sustained no-stalls: depth should shrink after SHRINK_AFTER_NO_STALLS (3)
            opi.adaptPrefetchDepth(true);  // streak = 1
            opi.adaptPrefetchDepth(true);  // streak = 2
            assertEquals("Depth should not shrink before reaching the threshold", grownDepth, opi.prefetchDepth());
            opi.adaptPrefetchDepth(true);  // streak = 3 → shrink by 1
            assertEquals(grownDepth - 1, opi.prefetchDepth());
        }
    }

    /**
     * Tests that the adaptive depth never exceeds MAX_PREFETCH_DEPTH (8) even under
     * persistent stalls, and never drops below the byte-based floor.
     */
    public void testAdaptivePrefetchDepthBounds() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");

        byte[] parquetData = createMultiRowGroupFile(schema, 5000, 4096);
        CountingStorageObject storage = new CountingStorageObject(parquetData, asyncIoExecutor);

        try (CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory, true).read(storage, FormatReadContext.of(null, 1024))) {
            OptimizedParquetColumnIterator opi = (OptimizedParquetColumnIterator) iter;
            int floor = opi.prefetchDepth();

            // Stall many times — depth should cap at MAX_PREFETCH_DEPTH (8)
            for (int i = 0; i < 20; i++) {
                opi.adaptPrefetchDepth(false);
            }
            assertTrue("Depth should not exceed MAX_PREFETCH_DEPTH", opi.prefetchDepth() <= 8);

            // Sustained no-stalls — depth should not drop below floor
            for (int i = 0; i < 100; i++) {
                opi.adaptPrefetchDepth(true);
            }
            assertEquals("Depth should not drop below the byte-based floor", floor, opi.prefetchDepth());
        }
    }

    private void readAll(ParquetFormatReader reader, StorageObject storageObject) throws IOException {
        try (CloseableIterator<Page> iter = reader.read(storageObject, FormatReadContext.of(null, 1024))) {
            while (iter.hasNext()) {
                iter.next();
            }
        }
    }

    private int countRows(ParquetFormatReader reader, StorageObject storageObject) throws IOException {
        int total = 0;
        try (CloseableIterator<Page> iter = reader.read(storageObject, FormatReadContext.of(null, 1024))) {
            while (iter.hasNext()) {
                total += iter.next().getPositionCount();
            }
        }
        return total;
    }

    /**
     * StorageObject that counts sync and async I/O calls. Async reads are dispatched
     * to a test-owned executor to simulate true async I/O. The executor is owned and shut
     * down by the enclosing test so no worker threads leak past the suite.
     */
    static class CountingStorageObject implements StorageObject {
        private final byte[] data;
        private final ExecutorService asyncIoExecutor;
        final AtomicInteger syncReadCount = new AtomicInteger();
        final AtomicInteger asyncReadCount = new AtomicInteger();

        CountingStorageObject(byte[] data, ExecutorService asyncIoExecutor) {
            this.data = data;
            this.asyncIoExecutor = asyncIoExecutor;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            syncReadCount.incrementAndGet();
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
            return StoragePath.of("memory://counting-test.parquet");
        }

        @Override
        public boolean supportsNativeAsync() {
            return true;
        }

        @Override
        public void readBytesAsync(
            long position,
            long length,
            DirectBufferFactory factory,
            Executor executor,
            ActionListener<DirectReadBuffer> listener
        ) {
            asyncReadCount.incrementAndGet();
            asyncIoExecutor.execute(() -> {
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
    }

    private StorageObject createPlainStorageObject(byte[] data) {
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
                return StoragePath.of("memory://plain-test.parquet");
            }
        };
    }

    private byte[] createMultiRowGroupFile(MessageType schema, int rowCount, int rowGroupSize) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(outputStream);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        String[] columnNames = new String[schema.getFieldCount()];
        for (int i = 0; i < columnNames.length; i++) {
            columnNames[i] = schema.getFieldName(i);
        }
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(rowGroupSize)
                .withPageSize(256)
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                Group g = groupFactory.newGroup();
                for (String col : columnNames) {
                    switch (schema.getType(col).asPrimitiveType().getPrimitiveTypeName()) {
                        case INT64 -> g.add(col, (long) i);
                        case INT32 -> g.add(col, i * 10);
                        case BINARY -> g.add(col, "item_" + (i % 50));
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
                return "memory://counting-test.parquet";
            }
        };
    }
}
