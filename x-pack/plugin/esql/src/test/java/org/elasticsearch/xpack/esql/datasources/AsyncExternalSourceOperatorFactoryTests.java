/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
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
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for AsyncExternalSourceOperatorFactory.
 *
 * Tests the dual-mode async factory that routes to sync wrapper or native async mode
 * based on FormatReader capabilities.
 */
public class AsyncExternalSourceOperatorFactoryTests extends ESTestCase {

    public void testConstructorValidation() {
        StorageProvider storageProvider = mock(StorageProvider.class);
        FormatReader formatReader = mock(FormatReader.class);
        StoragePath path = StoragePath.of("file:///test.csv");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "col1",
                new EsField("col1", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );
        Executor executor = Runnable::run;

        // Test null storage provider
        expectThrows(
            IllegalArgumentException.class,
            () -> new AsyncExternalSourceOperatorFactory(null, formatReader, path, attributes, 1000, 10, executor)
        );

        // Test null format reader
        expectThrows(
            IllegalArgumentException.class,
            () -> new AsyncExternalSourceOperatorFactory(storageProvider, null, path, attributes, 1000, 10, executor)
        );

        // Test null path
        expectThrows(
            IllegalArgumentException.class,
            () -> new AsyncExternalSourceOperatorFactory(storageProvider, formatReader, null, attributes, 1000, 10, executor)
        );

        // Test null attributes
        expectThrows(
            IllegalArgumentException.class,
            () -> new AsyncExternalSourceOperatorFactory(storageProvider, formatReader, path, null, 1000, 10, executor)
        );

        // Test null executor
        expectThrows(
            IllegalArgumentException.class,
            () -> new AsyncExternalSourceOperatorFactory(storageProvider, formatReader, path, attributes, 1000, 10, null)
        );

        // Test invalid batch size
        expectThrows(
            IllegalArgumentException.class,
            () -> new AsyncExternalSourceOperatorFactory(storageProvider, formatReader, path, attributes, 0, 10, executor)
        );

        expectThrows(
            IllegalArgumentException.class,
            () -> new AsyncExternalSourceOperatorFactory(storageProvider, formatReader, path, attributes, -1, 10, executor)
        );

        // Test invalid buffer size
        expectThrows(
            IllegalArgumentException.class,
            () -> new AsyncExternalSourceOperatorFactory(storageProvider, formatReader, path, attributes, 1000, 0, executor)
        );

        expectThrows(
            IllegalArgumentException.class,
            () -> new AsyncExternalSourceOperatorFactory(storageProvider, formatReader, path, attributes, 1000, -1, executor)
        );
    }

    public void testDescribeSyncWrapperMode() {
        StorageProvider storageProvider = mock(StorageProvider.class);
        FormatReader formatReader = mock(FormatReader.class);
        when(formatReader.formatName()).thenReturn("csv");
        when(formatReader.supportsNativeAsync()).thenReturn(false);

        StoragePath path = StoragePath.of("file:///data/test.csv");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "col1",
                new EsField("col1", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );
        Executor executor = Runnable::run;

        AsyncExternalSourceOperatorFactory factory = new AsyncExternalSourceOperatorFactory(
            storageProvider,
            formatReader,
            path,
            attributes,
            500,
            10,
            executor
        );

        String description = factory.describe();
        assertTrue(description.contains("AsyncExternalSourceOperator"));
        assertTrue(description.contains("csv"));
        assertTrue(description.contains("sync-wrapper"));
        assertTrue(description.contains("file:///data/test.csv"));
        assertTrue(description.contains("500"));
        assertTrue(description.contains("10"));
    }

    public void testDescribeNativeAsyncMode() {
        StorageProvider storageProvider = mock(StorageProvider.class);
        FormatReader formatReader = mock(FormatReader.class);
        when(formatReader.formatName()).thenReturn("parquet");
        when(formatReader.supportsNativeAsync()).thenReturn(true);

        StoragePath path = StoragePath.of("s3://bucket/data.parquet");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "col1",
                new EsField("col1", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );
        Executor executor = Runnable::run;

        AsyncExternalSourceOperatorFactory factory = new AsyncExternalSourceOperatorFactory(
            storageProvider,
            formatReader,
            path,
            attributes,
            1000,
            20,
            executor
        );

        String description = factory.describe();
        assertTrue(description.contains("AsyncExternalSourceOperator"));
        assertTrue(description.contains("parquet"));
        assertTrue(description.contains("native-async"));
        assertTrue(description.contains("s3://bucket/data.parquet"));
    }

    public void testAccessors() {
        StorageProvider storageProvider = mock(StorageProvider.class);
        FormatReader formatReader = mock(FormatReader.class);
        when(formatReader.formatName()).thenReturn("csv");

        StoragePath path = StoragePath.of("file:///test.csv");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "col1",
                new EsField("col1", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );
        Executor executor = Runnable::run;

        AsyncExternalSourceOperatorFactory factory = new AsyncExternalSourceOperatorFactory(
            storageProvider,
            formatReader,
            path,
            attributes,
            500,
            15,
            executor
        );

        assertSame(storageProvider, factory.storageProvider());
        assertSame(formatReader, factory.formatReader());
        assertEquals(path, factory.path());
        assertEquals(attributes, factory.attributes());
        assertEquals(500, factory.batchSize());
        assertEquals(15, factory.maxBufferSize());
        assertSame(executor, factory.executor());
    }

    public void testSyncWrapperModeCreatesOperator() throws Exception {
        // Create mock components
        StorageProvider storageProvider = mock(StorageProvider.class);
        StorageObject storageObject = mock(StorageObject.class);
        when(storageProvider.newObject(any())).thenReturn(storageObject);

        // Create a sync format reader (supportsNativeAsync = false)
        FormatReader formatReader = new TestSyncFormatReader();

        StoragePath path = StoragePath.of("file:///test.csv");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        // Use direct executor for testing
        Executor executor = Runnable::run;

        // Create mock driver context
        DriverContext driverContext = mock(DriverContext.class);
        BlockFactory blockFactory = mock(BlockFactory.class);
        when(driverContext.blockFactory()).thenReturn(blockFactory);

        AtomicBoolean asyncActionAdded = new AtomicBoolean(false);
        AtomicBoolean asyncActionRemoved = new AtomicBoolean(false);
        doAnswer(inv -> {
            asyncActionAdded.set(true);
            return null;
        }).when(driverContext).addAsyncAction();
        doAnswer(inv -> {
            asyncActionRemoved.set(true);
            return null;
        }).when(driverContext).removeAsyncAction();

        // Create factory
        AsyncExternalSourceOperatorFactory factory = new AsyncExternalSourceOperatorFactory(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            executor
        );

        // Create operator
        SourceOperator operator = factory.get(driverContext);

        // Verify operator was created
        assertNotNull(operator);
        assertTrue(operator instanceof AsyncExternalSourceOperator);
        assertTrue("Async action should be added", asyncActionAdded.get());

        // Clean up
        operator.close();
    }

    public void testNativeAsyncModeCreatesOperator() throws Exception {
        // Create mock components
        StorageProvider storageProvider = mock(StorageProvider.class);
        StorageObject storageObject = mock(StorageObject.class);
        when(storageProvider.newObject(any())).thenReturn(storageObject);

        // Create an async format reader (supportsNativeAsync = true)
        FormatReader formatReader = new TestAsyncFormatReader();

        StoragePath path = StoragePath.of("s3://bucket/test.parquet");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        // Use direct executor for testing
        Executor executor = Runnable::run;

        // Create mock driver context
        DriverContext driverContext = mock(DriverContext.class);
        BlockFactory blockFactory = mock(BlockFactory.class);
        when(driverContext.blockFactory()).thenReturn(blockFactory);

        AtomicBoolean asyncActionAdded = new AtomicBoolean(false);
        AtomicBoolean asyncActionRemoved = new AtomicBoolean(false);
        doAnswer(inv -> {
            asyncActionAdded.set(true);
            return null;
        }).when(driverContext).addAsyncAction();
        doAnswer(inv -> {
            asyncActionRemoved.set(true);
            return null;
        }).when(driverContext).removeAsyncAction();

        // Create factory
        AsyncExternalSourceOperatorFactory factory = new AsyncExternalSourceOperatorFactory(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            executor
        );

        // Create operator
        SourceOperator operator = factory.get(driverContext);

        // Verify operator was created
        assertNotNull(operator);
        assertTrue(operator instanceof AsyncExternalSourceOperator);
        assertTrue("Async action should be added", asyncActionAdded.get());

        // Clean up
        operator.close();
    }

    // ===== Multi-file iteration tests =====

    private static final BlockFactory TEST_BLOCK_FACTORY = BlockFactory.getInstance(
        new NoopCircuitBreaker("test"),
        BigArrays.NON_RECYCLING_INSTANCE
    );

    public void testMultiFileReadIteratesAllFiles() throws Exception {
        AtomicInteger readCount = new AtomicInteger(0);
        List<StorageEntry> entries = List.of(
            new StorageEntry(StoragePath.of("s3://bucket/data/f1.parquet"), 100, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/data/f2.parquet"), 200, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/data/f3.parquet"), 300, Instant.EPOCH)
        );
        FileSet fileSet = new FileSet(entries, "s3://bucket/data/*.parquet");

        FormatReader formatReader = new PageCountingFormatReader(readCount);
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/data/f1.parquet");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        DriverContext driverContext = mock(DriverContext.class);
        BlockFactory blockFactory = mock(BlockFactory.class);
        when(driverContext.blockFactory()).thenReturn(blockFactory);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = new AsyncExternalSourceOperatorFactory(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run(),
            fileSet
        );

        SourceOperator operator = factory.get(driverContext);
        assertNotNull(operator);

        List<Page> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                pages.add(page);
            }
        }

        assertEquals(3, readCount.get());
        assertEquals(3, pages.size());

        for (Page p : pages) {
            p.releaseBlocks();
        }
        operator.close();
    }

    public void testMultiFileReadUnresolvedFileSetFallsBackToSingleFile() throws Exception {
        AtomicInteger readCount = new AtomicInteger(0);

        FormatReader formatReader = new PageCountingFormatReader(readCount);
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/data/single.parquet");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        DriverContext driverContext = mock(DriverContext.class);
        BlockFactory blockFactory = mock(BlockFactory.class);
        when(driverContext.blockFactory()).thenReturn(blockFactory);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = new AsyncExternalSourceOperatorFactory(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        );

        SourceOperator operator = factory.get(driverContext);
        assertNotNull(operator);

        List<Page> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                pages.add(page);
            }
        }

        assertEquals(1, readCount.get());
        assertEquals(1, pages.size());

        for (Page p : pages) {
            p.releaseBlocks();
        }
        operator.close();
    }

    public void testMultiFileReadPropagatesReadError() throws Exception {
        List<StorageEntry> entries = List.of(
            new StorageEntry(StoragePath.of("s3://bucket/data/ok.parquet"), 100, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/data/bad.parquet"), 200, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/data/never.parquet"), 300, Instant.EPOCH)
        );
        FileSet fileSet = new FileSet(entries, "s3://bucket/data/*.parquet");

        FormatReader formatReader = new FailOnSecondFileFormatReader();
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/data/ok.parquet");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        DriverContext driverContext = mock(DriverContext.class);
        BlockFactory blockFactory = mock(BlockFactory.class);
        when(driverContext.blockFactory()).thenReturn(blockFactory);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = new AsyncExternalSourceOperatorFactory(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run(),
            fileSet
        );

        SourceOperator operator = factory.get(driverContext);
        assertNotNull(operator);

        List<Page> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                pages.add(page);
            }
        }

        AsyncExternalSourceOperator.Status status = (AsyncExternalSourceOperator.Status) operator.status();
        assertNotNull(status.failure());
        assertTrue(status.failure().getMessage().contains("Simulated read error"));

        for (Page p : pages) {
            p.releaseBlocks();
        }
        operator.close();
    }

    public void testMultiFileReadFileSetAccessor() {
        List<StorageEntry> entries = List.of(
            new StorageEntry(StoragePath.of("s3://bucket/a.parquet"), 10, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/b.parquet"), 20, Instant.EPOCH)
        );
        FileSet fileSet = new FileSet(entries, "s3://bucket/*.parquet");

        StorageProvider storageProvider = mock(StorageProvider.class);
        FormatReader formatReader = mock(FormatReader.class);
        when(formatReader.formatName()).thenReturn("parquet");

        AsyncExternalSourceOperatorFactory factory = new AsyncExternalSourceOperatorFactory(
            storageProvider,
            formatReader,
            StoragePath.of("s3://bucket/a.parquet"),
            List.of(
                new FieldAttribute(Source.EMPTY, "x", new EsField("x", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
            ),
            100,
            10,
            Runnable::run,
            fileSet
        );

        assertSame(fileSet, factory.fileSet());
        assertTrue(factory.fileSet().isResolved());
        assertEquals(2, factory.fileSet().size());
    }

    // ===== Helpers =====

    private static CloseableIterator<Page> emptyIterator() {
        return new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Page next() {
                throw new NoSuchElementException();
            }

            @Override
            public void close() {}
        };
    }

    private static Page createTestPage() {
        IntBlock block = TEST_BLOCK_FACTORY.newIntBlockBuilder(1).appendInt(42).build();
        return new Page(block);
    }

    private static class PageCountingFormatReader implements FormatReader {
        private final AtomicInteger readCount;

        PageCountingFormatReader(AtomicInteger readCount) {
            this.readCount = readCount;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) {
            readCount.incrementAndGet();
            Page page = createTestPage();
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
            return "test-counting";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    private static class FailOnSecondFileFormatReader implements FormatReader {
        private final AtomicInteger callCount = new AtomicInteger(0);

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
            int call = callCount.incrementAndGet();
            if (call >= 2) {
                throw new IOException("Simulated read error on file: " + object.path());
            }
            Page page = createTestPage();
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
            return "test-fail";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    private static class StubMultiFileStorageProvider implements StorageProvider {
        @Override
        public StorageObject newObject(StoragePath path) {
            return new StubMultiFileStorageObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return new StubMultiFileStorageObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return new StubMultiFileStorageObject(path);
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

    private static class StubMultiFileStorageObject implements StorageObject {
        private final StoragePath path;

        StubMultiFileStorageObject(StoragePath path) {
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

    /**
     * Test sync format reader that returns empty pages.
     */
    private static class TestSyncFormatReader implements FormatReader {
        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) {
            return emptyIterator();
        }

        @Override
        public String formatName() {
            return "test-sync";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".test");
        }

        @Override
        public boolean supportsNativeAsync() {
            return false;
        }

        @Override
        public void close() {}
    }

    /**
     * Test async format reader that returns empty pages via async callback.
     */
    private static class TestAsyncFormatReader implements FormatReader {
        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) {
            return emptyIterator();
        }

        @Override
        public void readAsync(
            StorageObject object,
            List<String> projectedColumns,
            int batchSize,
            Executor executor,
            ActionListener<CloseableIterator<Page>> listener
        ) {
            executor.execute(() -> { listener.onResponse(emptyIterator()); });
        }

        @Override
        public String formatName() {
            return "test-async";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".test");
        }

        @Override
        public boolean supportsNativeAsync() {
            return true;
        }

        @Override
        public void close() {}
    }
}
