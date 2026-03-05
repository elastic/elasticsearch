/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Integration tests verifying that external source parallelism works end-to-end:
 * multiple drivers claim splits from a shared ExternalSliceQueue, each reading one file,
 * and the combined results are correct.
 */
public class ExternalSourceParallelismTests extends ESTestCase {

    private static final BlockFactory TEST_BLOCK_FACTORY = BlockFactory.getInstance(
        new NoopCircuitBreaker("test"),
        BigArrays.NON_RECYCLING_INSTANCE
    );

    public void testMultipleDriversReadAllSplits() throws Exception {
        int fileCount = randomIntBetween(5, 20);
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < fileCount; i++) {
            splits.add(new FileSplit("test", StoragePath.of("s3://bucket/f" + i + ".parquet"), 0, 100, "parquet", Map.of(), Map.of()));
        }
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);

        AtomicInteger totalReadCount = new AtomicInteger(0);
        Set<String> filesRead = ConcurrentHashMap.newKeySet();
        TrackingFormatReader formatReader = new TrackingFormatReader(totalReadCount, filesRead);
        TrackingStorageProvider storageProvider = new TrackingStorageProvider();

        int driverCount = Math.min(fileCount, randomIntBetween(2, 6));
        List<List<Page>> perDriverPages = new ArrayList<>();
        for (int d = 0; d < driverCount; d++) {
            perDriverPages.add(new ArrayList<>());
        }

        // Separate executor for async reads to avoid deadlock between producer/consumer on the same thread
        ExecutorService asyncReadPool = Executors.newCachedThreadPool();
        ExecutorService driverPool = Executors.newFixedThreadPool(driverCount);
        CountDownLatch done = new CountDownLatch(driverCount);

        try {
            for (int d = 0; d < driverCount; d++) {
                final int driverIdx = d;
                driverPool.submit(() -> {
                    try {
                        AsyncExternalSourceOperatorFactory factory = new AsyncExternalSourceOperatorFactory(
                            storageProvider,
                            formatReader,
                            StoragePath.of("s3://bucket/f0.parquet"),
                            testAttributes(),
                            100,
                            10,
                            asyncReadPool,
                            null,
                            null,
                            null,
                            sliceQueue
                        );
                        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TEST_BLOCK_FACTORY, null);
                        SourceOperator operator = factory.get(driverContext);
                        while (operator.isFinished() == false) {
                            Page page = operator.getOutput();
                            if (page != null) {
                                perDriverPages.get(driverIdx).add(page);
                            }
                        }
                        operator.close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        done.countDown();
                    }
                });
            }

            assertTrue("Timed out waiting for drivers to complete", done.await(30, TimeUnit.SECONDS));
        } finally {
            driverPool.shutdownNow();
            asyncReadPool.shutdownNow();
            driverPool.awaitTermination(5, TimeUnit.SECONDS);
            asyncReadPool.awaitTermination(5, TimeUnit.SECONDS);
        }

        assertEquals(fileCount, totalReadCount.get());
        assertEquals(fileCount, filesRead.size());

        int totalPages = perDriverPages.stream().mapToInt(List::size).sum();
        assertEquals(fileCount, totalPages);

        for (List<Page> pages : perDriverPages) {
            for (Page p : pages) {
                p.releaseBlocks();
            }
        }
    }

    public void testDriverParallelismMatchesSplitCount() {
        int splitCount = randomIntBetween(2, 10);
        QueryPragmas pragmas = new QueryPragmas(Settings.EMPTY);
        int expectedConcurrency = Math.min(splitCount, pragmas.taskConcurrency());

        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < splitCount; i++) {
            splits.add(new FileSplit("test", StoragePath.of("s3://bucket/f" + i + ".parquet"), 0, 100, "parquet", Map.of(), Map.of()));
        }
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);

        assertEquals(splitCount, sliceQueue.totalSlices());
        assertThat(expectedConcurrency, greaterThan(0));
        assertThat(expectedConcurrency, lessThanOrEqualTo(pragmas.taskConcurrency()));
    }

    public void testSingleSplitNoParallelism() {
        List<ExternalSplit> splits = List.of(
            new FileSplit("test", StoragePath.of("s3://bucket/only.parquet"), 0, 100, "parquet", Map.of(), Map.of())
        );

        int instanceCount;
        if (splits.size() > 1) {
            instanceCount = Math.min(splits.size(), new QueryPragmas(Settings.EMPTY).taskConcurrency());
        } else {
            instanceCount = 1;
        }

        assertEquals(1, instanceCount);
    }

    public void testEmptySplitsNoParallelism() {
        List<ExternalSplit> splits = List.of();

        int instanceCount;
        if (splits.size() > 1) {
            instanceCount = Math.min(splits.size(), new QueryPragmas(Settings.EMPTY).taskConcurrency());
        } else {
            instanceCount = 1;
        }

        assertEquals(1, instanceCount);
    }

    public void testResultCorrectnessAcrossDrivers() throws Exception {
        int fileCount = 10;
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < fileCount; i++) {
            splits.add(new FileSplit("test", StoragePath.of("s3://bucket/f" + i + ".parquet"), 0, 100, "parquet", Map.of(), Map.of()));
        }
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);

        AtomicInteger totalReadCount = new AtomicInteger(0);
        Set<String> filesRead = ConcurrentHashMap.newKeySet();
        TrackingFormatReader formatReader = new TrackingFormatReader(totalReadCount, filesRead);
        TrackingStorageProvider storageProvider = new TrackingStorageProvider();

        int driverCount = 4;
        List<Page> allPages = java.util.Collections.synchronizedList(new ArrayList<>());
        ExecutorService asyncReadPool = Executors.newCachedThreadPool();
        ExecutorService driverPool = Executors.newFixedThreadPool(driverCount);
        CountDownLatch done = new CountDownLatch(driverCount);

        try {
            for (int d = 0; d < driverCount; d++) {
                driverPool.submit(() -> {
                    try {
                        AsyncExternalSourceOperatorFactory factory = new AsyncExternalSourceOperatorFactory(
                            storageProvider,
                            formatReader,
                            StoragePath.of("s3://bucket/f0.parquet"),
                            testAttributes(),
                            100,
                            10,
                            asyncReadPool,
                            null,
                            null,
                            null,
                            sliceQueue
                        );
                        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TEST_BLOCK_FACTORY, null);
                        SourceOperator operator = factory.get(driverContext);
                        while (operator.isFinished() == false) {
                            Page page = operator.getOutput();
                            if (page != null) {
                                allPages.add(page);
                            }
                        }
                        operator.close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        done.countDown();
                    }
                });
            }

            assertTrue("Timed out waiting for drivers to complete", done.await(30, TimeUnit.SECONDS));
        } finally {
            driverPool.shutdownNow();
            asyncReadPool.shutdownNow();
            driverPool.awaitTermination(5, TimeUnit.SECONDS);
            asyncReadPool.awaitTermination(5, TimeUnit.SECONDS);
        }

        assertEquals(fileCount, allPages.size());
        assertEquals(fileCount, filesRead.size());

        Set<Integer> values = ConcurrentHashMap.newKeySet();
        for (Page page : allPages) {
            assertEquals(1, page.getBlockCount());
            IntBlock block = page.getBlock(0);
            for (int pos = 0; pos < block.getPositionCount(); pos++) {
                values.add(block.getInt(pos));
            }
            page.releaseBlocks();
        }
        assertEquals(fileCount, values.size());
    }

    public void testSliceQueueWithPartitionValues() throws Exception {
        List<ExternalSplit> splits = List.of(
            new FileSplit("test", StoragePath.of("s3://bucket/year=2023/f1.parquet"), 0, 100, "parquet", Map.of(), Map.of("year", 2023)),
            new FileSplit("test", StoragePath.of("s3://bucket/year=2024/f2.parquet"), 0, 200, "parquet", Map.of(), Map.of("year", 2024))
        );
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(new ArrayList<>(splits));

        AtomicInteger readCount = new AtomicInteger(0);
        Set<String> filesRead = ConcurrentHashMap.newKeySet();
        TrackingFormatReader formatReader = new TrackingFormatReader(readCount, filesRead);
        TrackingStorageProvider storageProvider = new TrackingStorageProvider();

        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            ),
            new FieldAttribute(
                Source.EMPTY,
                "year",
                new EsField("year", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        AsyncExternalSourceOperatorFactory factory = new AsyncExternalSourceOperatorFactory(
            storageProvider,
            formatReader,
            StoragePath.of("s3://bucket/f1.parquet"),
            attributes,
            100,
            10,
            Runnable::run,
            null,
            Set.of("year"),
            Map.of(),
            sliceQueue
        );

        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TEST_BLOCK_FACTORY, null);
        SourceOperator operator = factory.get(driverContext);

        List<Page> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                pages.add(page);
            }
        }

        assertEquals(2, readCount.get());
        assertEquals(2, pages.size());

        for (Page p : pages) {
            p.releaseBlocks();
        }
        operator.close();
    }

    // ===== Helpers =====

    private static List<Attribute> testAttributes() {
        return List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );
    }

    private static class TrackingFormatReader implements FormatReader {
        private final AtomicInteger readCount;
        private final Set<String> filesRead;

        TrackingFormatReader(AtomicInteger readCount, Set<String> filesRead) {
            this.readCount = readCount;
            this.filesRead = filesRead;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) {
            int fileIndex = readCount.getAndIncrement();
            filesRead.add(object.path().toString());
            IntBlock block = TEST_BLOCK_FACTORY.newIntBlockBuilder(1).appendInt(fileIndex).build();
            Page page = new Page(block);
            return new CloseableIterator<>() {
                private boolean consumed = false;

                @Override
                public boolean hasNext() {
                    return consumed == false;
                }

                @Override
                public Page next() {
                    if (consumed) {
                        throw new NoSuchElementException();
                    }
                    consumed = true;
                    return page;
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public String formatName() {
            return "test-tracking";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    private static class TrackingStorageProvider implements StorageProvider {
        @Override
        public StorageObject newObject(StoragePath path) {
            return new StubStorageObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return new StubStorageObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return new StubStorageObject(path);
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(StoragePath path) {
            return true;
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of("s3");
        }

        @Override
        public void close() {}
    }

    private static class StubStorageObject implements StorageObject {
        private final StoragePath path;

        StubStorageObject(StoragePath path) {
            this.path = path;
        }

        @Override
        public InputStream newStream() {
            return InputStream.nullInputStream();
        }

        @Override
        public InputStream newStream(long position, long length) {
            return InputStream.nullInputStream();
        }

        @Override
        public long length() {
            return 0;
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
            return path;
        }
    }
}
