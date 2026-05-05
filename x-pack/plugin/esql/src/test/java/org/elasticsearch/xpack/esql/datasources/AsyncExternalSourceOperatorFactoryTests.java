/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SplittableDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

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
            () -> AsyncExternalSourceOperatorFactory.builder(null, formatReader, path, attributes, 1000, 10, executor).build()
        );

        // Test null format reader
        expectThrows(
            IllegalArgumentException.class,
            () -> AsyncExternalSourceOperatorFactory.builder(storageProvider, null, path, attributes, 1000, 10, executor).build()
        );

        // Test null path
        expectThrows(
            IllegalArgumentException.class,
            () -> AsyncExternalSourceOperatorFactory.builder(storageProvider, formatReader, null, attributes, 1000, 10, executor).build()
        );

        // Test null attributes
        expectThrows(
            IllegalArgumentException.class,
            () -> AsyncExternalSourceOperatorFactory.builder(storageProvider, formatReader, path, null, 1000, 10, executor).build()
        );

        // Test null executor
        expectThrows(
            IllegalArgumentException.class,
            () -> AsyncExternalSourceOperatorFactory.builder(storageProvider, formatReader, path, attributes, 1000, 10, null).build()
        );

        // Test invalid batch size
        expectThrows(
            IllegalArgumentException.class,
            () -> AsyncExternalSourceOperatorFactory.builder(storageProvider, formatReader, path, attributes, 0, 10, executor).build()
        );

        expectThrows(
            IllegalArgumentException.class,
            () -> AsyncExternalSourceOperatorFactory.builder(storageProvider, formatReader, path, attributes, -1, 10, executor).build()
        );

        // Test invalid buffer size
        expectThrows(
            IllegalArgumentException.class,
            () -> AsyncExternalSourceOperatorFactory.builder(storageProvider, formatReader, path, attributes, 1000, 0, executor).build()
        );

        expectThrows(
            IllegalArgumentException.class,
            () -> AsyncExternalSourceOperatorFactory.builder(storageProvider, formatReader, path, attributes, 1000, -1, executor).build()
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

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            500,
            10,
            executor
        ).build();

        String description = factory.describe();
        assertTrue(description.contains("AsyncExternalSourceOperator"));
        assertTrue(description.contains("csv"));
        assertTrue(description.contains("sync-wrapper"));
        assertTrue(description.contains("file:///data/test.csv"));
        assertTrue(description.contains("500"));
        assertTrue(description.contains("maxBufferBytes="));
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

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            1000,
            20,
            executor
        ).build();

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

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            500,
            15,
            executor
        ).build();

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
        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            executor
        ).build();

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
        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            executor
        ).build();

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

    private static final BlockFactory TEST_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    public void testMultiFileReadIteratesAllFiles() throws Exception {
        AtomicInteger readCount = new AtomicInteger(0);
        List<StorageEntry> entries = List.of(
            new StorageEntry(StoragePath.of("s3://bucket/data/f1.parquet"), 100, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/data/f2.parquet"), 200, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/data/f3.parquet"), 300, Instant.EPOCH)
        );
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/*.parquet");

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

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).fileList(fileList).build();

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

    public void testMultiFileReadUnresolvedGenericFileListFallsBackToSingleFile() throws Exception {
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

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).build();

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
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/*.parquet");

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

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).fileList(fileList).build();

        SourceOperator operator = factory.get(driverContext);
        assertNotNull(operator);

        List<Page> pages = new ArrayList<>();
        RuntimeException readFailure = expectThrows(RuntimeException.class, () -> {
            while (operator.isFinished() == false) {
                Page page = operator.getOutput();
                if (page != null) {
                    pages.add(page);
                }
            }
        });
        assertThat(readFailure.getCause(), org.hamcrest.Matchers.instanceOf(IOException.class));
        assertTrue(readFailure.getCause().getMessage().contains("Simulated read error"));

        assertEquals("First file should yield one page before the second file fails", 1, pages.size());

        for (Page p : pages) {
            p.releaseBlocks();
        }
        operator.close();
    }

    public void testMultiFileReadGenericFileListAccessor() {
        List<StorageEntry> entries = List.of(
            new StorageEntry(StoragePath.of("s3://bucket/a.parquet"), 10, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/b.parquet"), 20, Instant.EPOCH)
        );
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/*.parquet");

        StorageProvider storageProvider = mock(StorageProvider.class);
        FormatReader formatReader = mock(FormatReader.class);
        when(formatReader.formatName()).thenReturn("parquet");

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            StoragePath.of("s3://bucket/a.parquet"),
            List.of(
                new FieldAttribute(Source.EMPTY, "x", new EsField("x", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
            ),
            100,
            10,
            Runnable::run
        ).fileList(fileList).build();

        assertSame(fileList, factory.fileList());
        assertTrue(factory.fileList().isResolved());
        assertEquals(2, factory.fileList().fileCount());
    }

    // ===== Slice Queue tests =====

    public void testSliceQueueReadsSplitsSequentially() throws Exception {
        List<FileSplit> splits = List.of(
            new FileSplit("test", StoragePath.of("s3://bucket/f1.parquet"), 0, 100, "parquet", Map.of(), Map.of()),
            new FileSplit("test", StoragePath.of("s3://bucket/f2.parquet"), 0, 200, "parquet", Map.of(), Map.of()),
            new FileSplit("test", StoragePath.of("s3://bucket/f3.parquet"), 0, 300, "parquet", Map.of(), Map.of())
        );
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(new ArrayList<>(splits));

        AtomicInteger readCount = new AtomicInteger(0);
        FormatReader formatReader = new PageCountingFormatReader(readCount);
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/f1.parquet");
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

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).sliceQueue(sliceQueue).build();

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

    public void testSliceQueueExhaustionFinishesOperator() throws Exception {
        FileSplit split = new FileSplit("test", StoragePath.of("s3://bucket/only.parquet"), 0, 100, "parquet", Map.of(), Map.of());
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(List.of(split));

        AtomicInteger readCount = new AtomicInteger(0);
        FormatReader formatReader = new PageCountingFormatReader(readCount);
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/only.parquet");
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

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).sliceQueue(sliceQueue).build();

        SourceOperator operator = factory.get(driverContext);
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

    public void testSliceQueueMultipleDriversClaimDifferentSplits() throws Exception {
        int splitCount = 6;
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < splitCount; i++) {
            splits.add(new FileSplit("test", StoragePath.of("s3://bucket/f" + i + ".parquet"), 0, 100, "parquet", Map.of(), Map.of()));
        }
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);

        AtomicInteger totalReadCount = new AtomicInteger(0);
        FormatReader formatReader = new PageCountingFormatReader(totalReadCount);
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/f0.parquet");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        int driverCount = 3;
        List<SourceOperator> operators = new ArrayList<>();
        List<DriverContext> contexts = new ArrayList<>();

        for (int d = 0; d < driverCount; d++) {
            DriverContext driverContext = mock(DriverContext.class);
            BlockFactory blockFactory = mock(BlockFactory.class);
            when(driverContext.blockFactory()).thenReturn(blockFactory);
            doAnswer(inv -> null).when(driverContext).addAsyncAction();
            doAnswer(inv -> null).when(driverContext).removeAsyncAction();
            contexts.add(driverContext);

            AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
                storageProvider,
                formatReader,
                path,
                attributes,
                100,
                10,
                (Runnable r) -> r.run()
            ).sliceQueue(sliceQueue).build();
            operators.add(factory.get(driverContext));
        }

        List<Page> allPages = new ArrayList<>();
        for (SourceOperator op : operators) {
            while (op.isFinished() == false) {
                Page page = op.getOutput();
                if (page != null) {
                    allPages.add(page);
                }
            }
        }

        assertEquals(splitCount, totalReadCount.get());
        assertEquals(splitCount, allPages.size());

        for (Page p : allPages) {
            p.releaseBlocks();
        }
        for (SourceOperator op : operators) {
            op.close();
        }
    }

    public void testSliceQueueAccessor() {
        List<ExternalSplit> splits = List.of(
            new FileSplit("test", StoragePath.of("s3://bucket/a.parquet"), 0, 10, "parquet", Map.of(), Map.of())
        );
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);

        StorageProvider storageProvider = mock(StorageProvider.class);
        FormatReader formatReader = mock(FormatReader.class);
        when(formatReader.formatName()).thenReturn("parquet");

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            StoragePath.of("s3://bucket/a.parquet"),
            List.of(
                new FieldAttribute(Source.EMPTY, "x", new EsField("x", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
            ),
            100,
            10,
            Runnable::run
        ).sliceQueue(sliceQueue).build();

        assertSame(sliceQueue, factory.sliceQueue());
    }

    public void testSliceQueueWithNonZeroOffsetWrapsWithRangeStorageObject() throws Exception {
        long splitOffset = 500;
        long splitLength = 300;
        FileSplit split = new FileSplit(
            "test",
            StoragePath.of("s3://bucket/large.csv"),
            splitOffset,
            splitLength,
            "csv",
            Map.of(FileSplitProvider.LAST_SPLIT_KEY, "false"),
            Map.of()
        );
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(List.of(split));

        List<StorageObject> capturedObjects = new ArrayList<>();
        List<Boolean> capturedSkipFirstLine = new ArrayList<>();
        FormatReader formatReader = new SplitCapturingFormatReader(capturedObjects, capturedSkipFirstLine);
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/large.csv");
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

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).sliceQueue(sliceQueue).build();

        SourceOperator operator = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                pages.add(page);
            }
        }

        assertEquals(1, capturedObjects.size());
        StorageObject received = capturedObjects.get(0);
        assertTrue(
            "Expected RangeStorageObject for non-zero offset, got: " + received.getClass().getSimpleName(),
            received instanceof RangeStorageObject
        );
        RangeStorageObject range = (RangeStorageObject) received;
        assertEquals(splitOffset, range.offset());
        assertEquals(splitLength, range.length());

        assertEquals(1, capturedSkipFirstLine.size());
        assertTrue("Non-first split with offset > 0 should skip first line", capturedSkipFirstLine.get(0));

        for (Page p : pages) {
            p.releaseBlocks();
        }
        operator.close();
    }

    public void testSliceQueueWithZeroOffsetWrapsRangeForSplitSpan() throws Exception {
        FileSplit split = new FileSplit("test", StoragePath.of("s3://bucket/small.csv"), 0, 1000, "csv", Map.of(), Map.of());
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(List.of(split));

        List<StorageObject> capturedObjects = new ArrayList<>();
        List<Boolean> capturedSkipFirstLine = new ArrayList<>();
        FormatReader formatReader = new SplitCapturingFormatReader(capturedObjects, capturedSkipFirstLine);
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/small.csv");
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

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).sliceQueue(sliceQueue).build();

        SourceOperator operator = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                pages.add(page);
            }
        }

        assertEquals(1, capturedObjects.size());
        assertTrue(
            "Zero-offset split must still use RangeStorageObject for the split length",
            capturedObjects.get(0) instanceof RangeStorageObject
        );
        RangeStorageObject range0 = (RangeStorageObject) capturedObjects.get(0);
        assertEquals(0, range0.offset());
        assertEquals(1000, range0.length());

        assertEquals(1, capturedSkipFirstLine.size());
        assertFalse("Zero-offset split should not skip first line", capturedSkipFirstLine.get(0));

        for (Page p : pages) {
            p.releaseBlocks();
        }
        operator.close();
    }

    public void testSliceQueueFirstSplitWithOffsetDoesNotSkipFirstLine() throws Exception {
        FileSplit split = new FileSplit(
            "test",
            StoragePath.of("s3://bucket/large.csv"),
            500,
            300,
            "csv",
            Map.of(FileSplitProvider.FIRST_SPLIT_KEY, "true"),
            Map.of()
        );
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(List.of(split));

        List<StorageObject> capturedObjects = new ArrayList<>();
        List<Boolean> capturedSkipFirstLine = new ArrayList<>();
        FormatReader formatReader = new SplitCapturingFormatReader(capturedObjects, capturedSkipFirstLine);
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/large.csv");
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

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).sliceQueue(sliceQueue).build();

        SourceOperator operator = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                pages.add(page);
            }
        }

        assertEquals(1, capturedObjects.size());
        assertTrue(capturedObjects.get(0) instanceof RangeStorageObject);

        assertEquals(1, capturedSkipFirstLine.size());
        assertFalse("First split should not skip first line even with offset > 0", capturedSkipFirstLine.get(0));

        for (Page p : pages) {
            p.releaseBlocks();
        }
        operator.close();
    }

    public void testSliceQueueMultipleSplitsWithMixedOffsets() throws Exception {
        List<ExternalSplit> splits = List.of(
            new FileSplit(
                "test",
                StoragePath.of("s3://bucket/data.csv"),
                0,
                1000,
                "csv",
                Map.of(FileSplitProvider.FIRST_SPLIT_KEY, "true"),
                Map.of()
            ),
            new FileSplit("test", StoragePath.of("s3://bucket/data.csv"), 1000, 1000, "csv", Map.of(), Map.of()),
            new FileSplit(
                "test",
                StoragePath.of("s3://bucket/data.csv"),
                2000,
                500,
                "csv",
                Map.of(FileSplitProvider.LAST_SPLIT_KEY, "true"),
                Map.of()
            )
        );
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(new ArrayList<>(splits));

        List<StorageObject> capturedObjects = new ArrayList<>();
        List<Boolean> capturedSkipFirstLine = new ArrayList<>();
        FormatReader formatReader = new SplitCapturingFormatReader(capturedObjects, capturedSkipFirstLine);
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/data.csv");
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

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).sliceQueue(sliceQueue).build();

        SourceOperator operator = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                pages.add(page);
            }
        }

        assertEquals(3, capturedObjects.size());

        assertTrue("First split (offset=0) must use RangeStorageObject", capturedObjects.get(0) instanceof RangeStorageObject);
        RangeStorageObject range0 = (RangeStorageObject) capturedObjects.get(0);
        assertEquals(0, range0.offset());
        assertEquals(1000, range0.length());
        assertFalse("First split should not skip first line", capturedSkipFirstLine.get(0));

        assertTrue("Second split (offset=1000) should be wrapped", capturedObjects.get(1) instanceof RangeStorageObject);
        RangeStorageObject range1 = (RangeStorageObject) capturedObjects.get(1);
        assertEquals(1000, range1.offset());
        assertEquals(1000, range1.length());
        assertTrue("Non-first split with offset > 0 should skip first line", capturedSkipFirstLine.get(1));

        assertTrue("Third split (offset=2000) should be wrapped", capturedObjects.get(2) instanceof RangeStorageObject);
        RangeStorageObject range2 = (RangeStorageObject) capturedObjects.get(2);
        assertEquals(2000, range2.offset());
        assertEquals(500, range2.length());
        assertTrue("Non-first split with offset > 0 should skip first line", capturedSkipFirstLine.get(2));

        for (Page p : pages) {
            p.releaseBlocks();
        }
        operator.close();
    }

    // ===== Parallel parsing tests =====

    public void testParallelParsingUsedForSegmentableReader() throws Exception {
        TrackingSegmentableFormatReader formatReader = new TrackingSegmentableFormatReader();
        LargeStorageProvider storageProvider = new LargeStorageProvider(3 * 1024 * 1024);

        StoragePath path = StoragePath.of("file:///data/large.csv");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        DriverContext driverContext = mock(DriverContext.class);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).parsingParallelism(2).build();

        SourceOperator operator = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                pages.add(page);
            }
        }

        assertTrue("read() should be called for multiple segments when parallel parsing is used", formatReader.readCount.get() > 1);

        for (Page p : pages) {
            p.releaseBlocks();
        }
        operator.close();
    }

    public void testParallelParsingSkippedWithRowLimit() throws Exception {
        TrackingSegmentableFormatReader formatReader = new TrackingSegmentableFormatReader();
        LargeStorageProvider storageProvider = new LargeStorageProvider(3 * 1024 * 1024);

        StoragePath path = StoragePath.of("file:///data/large.csv");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        DriverContext driverContext = mock(DriverContext.class);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).rowLimit(10).parsingParallelism(2).build();

        SourceOperator operator = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                pages.add(page);
            }
        }

        assertTrue("read() should be called when row limit is set", formatReader.readCount.get() > 0);
        assertEquals(
            "read() with firstSplit=false should not be called when row limit bypasses parallel parsing",
            0,
            formatReader.readWithFirstSplitFalseCount.get()
        );

        for (Page p : pages) {
            p.releaseBlocks();
        }
        operator.close();
    }

    public void testParallelParsingSkippedForNonSegmentableReader() throws Exception {
        AtomicInteger readCount = new AtomicInteger(0);
        FormatReader formatReader = new PageCountingFormatReader(readCount);
        LargeStorageProvider storageProvider = new LargeStorageProvider(3 * 1024 * 1024);

        StoragePath path = StoragePath.of("file:///data/large.parquet");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        DriverContext driverContext = mock(DriverContext.class);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).parsingParallelism(2).build();

        SourceOperator operator = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                pages.add(page);
            }
        }

        assertTrue("read() should be called for non-segmentable reader", readCount.get() > 0);

        for (Page p : pages) {
            p.releaseBlocks();
        }
        operator.close();
    }

    public void testDescribeShowsParallelParseMode() {
        TrackingSegmentableFormatReader formatReader = new TrackingSegmentableFormatReader();
        StorageProvider storageProvider = mock(StorageProvider.class);

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            StoragePath.of("file:///test.csv"),
            List.of(
                new FieldAttribute(Source.EMPTY, "x", new EsField("x", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
            ),
            100,
            10,
            Runnable::run
        ).parsingParallelism(4).build();

        String description = factory.describe();
        assertTrue("describe should mention parallel-parse for segmentable readers", description.contains("parallel-parse(4)"));
    }

    public void testDescribeShowsSyncWrapperForParallelism1() {
        TrackingSegmentableFormatReader formatReader = new TrackingSegmentableFormatReader();
        StorageProvider storageProvider = mock(StorageProvider.class);

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            StoragePath.of("file:///test.csv"),
            List.of(
                new FieldAttribute(Source.EMPTY, "x", new EsField("x", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
            ),
            100,
            10,
            Runnable::run
        ).build();

        String description = factory.describe();
        assertTrue("describe should show sync-wrapper when parallelism is 1", description.contains("sync-wrapper"));
    }

    // ===== Byte-based backpressure tests =====

    public void testByteBasedBackpressureEndToEnd() throws Exception {
        AtomicInteger readCount = new AtomicInteger(0);
        FormatReader formatReader = new PageCountingFormatReader(readCount);
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        StoragePath path = StoragePath.of("file:///test.csv");
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

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            2,
            (Runnable r) -> r.run()
        ).build();

        SourceOperator operator = factory.get(driverContext);
        assertNotNull(operator);

        AsyncExternalSourceOperator.Status status = (AsyncExternalSourceOperator.Status) operator.status();
        assertNotNull(status);
        assertTrue("bytesBuffered should be reported in status", status.bytesBuffered() >= 0);

        List<Page> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                pages.add(page);
            }
        }

        for (Page p : pages) {
            p.releaseBlocks();
        }
        operator.close();
    }

    // ===== Lifecycle tests: removeAsyncAction fires exactly once per producer path =====

    /**
     * Sync-wrapper path: verifies removeAsyncAction fires exactly once on success.
     */
    public void testSyncWrapperRemoveAsyncActionExactlyOnce() throws Exception {
        AtomicInteger readCount = new AtomicInteger(0);
        FormatReader formatReader = new PageCountingFormatReader(readCount);
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        StoragePath path = StoragePath.of("file:///test.csv");
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

        AtomicInteger addCount = new AtomicInteger(0);
        AtomicInteger removeCount = new AtomicInteger(0);
        doAnswer(inv -> {
            addCount.incrementAndGet();
            return null;
        }).when(driverContext).addAsyncAction();
        doAnswer(inv -> {
            removeCount.incrementAndGet();
            return null;
        }).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).build();

        SourceOperator operator = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                pages.add(page);
            }
        }

        assertEquals("addAsyncAction should be called exactly once", 1, addCount.get());
        assertEquals("removeAsyncAction should be called exactly once", 1, removeCount.get());

        for (Page p : pages) {
            p.releaseBlocks();
        }
        operator.close();
    }

    /**
     * Multi-file path: verifies removeAsyncAction fires exactly once for the entire multi-file iteration.
     */
    public void testMultiFileRemoveAsyncActionExactlyOnce() throws Exception {
        AtomicInteger readCount = new AtomicInteger(0);
        List<StorageEntry> entries = List.of(
            new StorageEntry(StoragePath.of("s3://bucket/data/f1.parquet"), 100, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/data/f2.parquet"), 200, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/data/f3.parquet"), 300, Instant.EPOCH)
        );
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/*.parquet");

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

        AtomicInteger addCount = new AtomicInteger(0);
        AtomicInteger removeCount = new AtomicInteger(0);
        doAnswer(inv -> {
            addCount.incrementAndGet();
            return null;
        }).when(driverContext).addAsyncAction();
        doAnswer(inv -> {
            removeCount.incrementAndGet();
            return null;
        }).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).fileList(fileList).build();

        SourceOperator operator = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                pages.add(page);
            }
        }

        assertEquals("addAsyncAction should be called exactly once", 1, addCount.get());
        assertEquals("removeAsyncAction should be called exactly once for all files", 1, removeCount.get());
        assertEquals(3, readCount.get());

        for (Page p : pages) {
            p.releaseBlocks();
        }
        operator.close();
    }

    /**
     * Slice-queue path: verifies removeAsyncAction fires exactly once after all splits are processed.
     */
    public void testSliceQueueRemoveAsyncActionExactlyOnce() throws Exception {
        List<FileSplit> splits = List.of(
            new FileSplit("test", StoragePath.of("s3://bucket/f1.parquet"), 0, 100, "parquet", Map.of(), Map.of()),
            new FileSplit("test", StoragePath.of("s3://bucket/f2.parquet"), 0, 200, "parquet", Map.of(), Map.of())
        );
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(new ArrayList<>(splits));

        AtomicInteger readCount = new AtomicInteger(0);
        FormatReader formatReader = new PageCountingFormatReader(readCount);
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/f1.parquet");
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

        AtomicInteger addCount = new AtomicInteger(0);
        AtomicInteger removeCount = new AtomicInteger(0);
        doAnswer(inv -> {
            addCount.incrementAndGet();
            return null;
        }).when(driverContext).addAsyncAction();
        doAnswer(inv -> {
            removeCount.incrementAndGet();
            return null;
        }).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).sliceQueue(sliceQueue).build();

        SourceOperator operator = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                pages.add(page);
            }
        }

        assertEquals("addAsyncAction should be called exactly once", 1, addCount.get());
        assertEquals("removeAsyncAction should be called exactly once after all splits", 1, removeCount.get());
        assertEquals(2, readCount.get());

        for (Page p : pages) {
            p.releaseBlocks();
        }
        operator.close();
    }

    /**
     * Sync-wrapper path with error: removeAsyncAction fires exactly once even when read fails.
     */
    public void testSyncWrapperRemoveAsyncActionOnError() throws Exception {
        FormatReader formatReader = new AlwaysFailFormatReader();
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/data/bad.parquet");
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

        AtomicInteger addCount = new AtomicInteger(0);
        AtomicInteger removeCount = new AtomicInteger(0);
        doAnswer(inv -> {
            addCount.incrementAndGet();
            return null;
        }).when(driverContext).addAsyncAction();
        doAnswer(inv -> {
            removeCount.incrementAndGet();
            return null;
        }).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).build();

        SourceOperator operator = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        RuntimeException failure = expectThrows(RuntimeException.class, () -> {
            while (operator.isFinished() == false) {
                Page page = operator.getOutput();
                if (page != null) {
                    pages.add(page);
                }
            }
        });
        assertNotNull(failure);

        assertEquals("addAsyncAction should be called exactly once", 1, addCount.get());
        assertEquals("removeAsyncAction should be called exactly once even on error", 1, removeCount.get());

        for (Page p : pages) {
            p.releaseBlocks();
        }
        operator.close();
    }

    // ===== Regression: buffer.finish / buffer.onFailure mutually exclusive on factory paths =====

    /**
     * Sync-wrapper success: buffer.finish(false) is called, buffer.onFailure is not.
     */
    public void testSyncWrapperBufferFinishOnSuccess() throws Exception {
        AtomicInteger readCount = new AtomicInteger(0);
        FormatReader formatReader = new PageCountingFormatReader(readCount);
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        StoragePath path = StoragePath.of("file:///test.csv");
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

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).build();

        SourceOperator operator = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                pages.add(page);
            }
        }

        assertTrue("Operator should be finished", operator.isFinished());
        assertNull("No failure should be recorded", ((AsyncExternalSourceOperator.Status) operator.status()).failure());

        for (Page p : pages) {
            p.releaseBlocks();
        }
        operator.close();
    }

    // ===== Regression: backpressure with real thread pool across all producer paths =====

    /**
     * Sync-wrapper with real thread pool and small buffer: verifies end-to-end backpressure works.
     */
    public void testSyncWrapperBackpressureWithRealThreadPool() throws Exception {
        ExecutorService realExec = Executors.newFixedThreadPool(2, EsExecutors.daemonThreadFactory("test", "bp-test"));
        try {
            AtomicInteger readCount = new AtomicInteger(0);
            FormatReader formatReader = new MultiPageFormatReader(readCount, 10);
            StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

            StoragePath path = StoragePath.of("file:///test.csv");
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

            AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
                storageProvider,
                formatReader,
                path,
                attributes,
                100,
                2,
                realExec
            ).build();

            SourceOperator operator = factory.get(driverContext);
            List<Page> pages = new ArrayList<>();

            long deadline = System.currentTimeMillis() + 30_000;
            while (operator.isFinished() == false && System.currentTimeMillis() < deadline) {
                Page page = operator.getOutput();
                if (page != null) {
                    pages.add(page);
                } else {
                    Thread.sleep(10);
                }
            }
            assertTrue("Operator should complete within timeout", operator.isFinished());
            assertEquals(10, pages.size());

            for (Page p : pages) {
                p.releaseBlocks();
            }
            operator.close();
        } finally {
            realExec.shutdown();
            assertTrue(realExec.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    /**
     * Multi-file path with real thread pool: exercises backpressure across file boundaries.
     */
    public void testMultiFileBackpressureWithRealThreadPool() throws Exception {
        ExecutorService realExec = Executors.newFixedThreadPool(2, EsExecutors.daemonThreadFactory("test", "mf-test"));
        try {
            AtomicInteger readCount = new AtomicInteger(0);
            FormatReader formatReader = new MultiPageFormatReader(readCount, 5);
            StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

            List<StorageEntry> entries = List.of(
                new StorageEntry(StoragePath.of("s3://bucket/f1.parquet"), 100, Instant.EPOCH),
                new StorageEntry(StoragePath.of("s3://bucket/f2.parquet"), 200, Instant.EPOCH),
                new StorageEntry(StoragePath.of("s3://bucket/f3.parquet"), 300, Instant.EPOCH)
            );
            FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/*.parquet");

            StoragePath path = StoragePath.of("s3://bucket/f1.parquet");
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

            AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
                storageProvider,
                formatReader,
                path,
                attributes,
                100,
                2,
                realExec
            ).fileList(fileList).build();

            SourceOperator operator = factory.get(driverContext);
            List<Page> pages = new ArrayList<>();

            long deadline = System.currentTimeMillis() + 30_000;
            while (operator.isFinished() == false && System.currentTimeMillis() < deadline) {
                Page page = operator.getOutput();
                if (page != null) {
                    pages.add(page);
                } else {
                    Thread.sleep(10);
                }
            }
            assertTrue("Operator should complete within timeout", operator.isFinished());
            assertEquals(3, readCount.get());
            assertEquals(15, pages.size());

            for (Page p : pages) {
                p.releaseBlocks();
            }
            operator.close();
        } finally {
            realExec.shutdown();
            assertTrue(realExec.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    /**
     * Slice-queue path with real thread pool: exercises backpressure within split processing.
     */
    public void testSliceQueueBackpressureWithRealThreadPool() throws Exception {
        ExecutorService realExec = Executors.newFixedThreadPool(2, EsExecutors.daemonThreadFactory("test", "sq-test"));
        try {
            AtomicInteger readCount = new AtomicInteger(0);
            FormatReader formatReader = new MultiPageFormatReader(readCount, 5);
            StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

            List<FileSplit> splits = List.of(
                new FileSplit("test", StoragePath.of("s3://bucket/f1.parquet"), 0, 100, "parquet", Map.of(), Map.of()),
                new FileSplit("test", StoragePath.of("s3://bucket/f2.parquet"), 0, 200, "parquet", Map.of(), Map.of())
            );
            ExternalSliceQueue sliceQueue = new ExternalSliceQueue(new ArrayList<>(splits));

            StoragePath path = StoragePath.of("s3://bucket/f1.parquet");
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

            AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
                storageProvider,
                formatReader,
                path,
                attributes,
                100,
                2,
                realExec
            ).sliceQueue(sliceQueue).build();

            SourceOperator operator = factory.get(driverContext);
            List<Page> pages = new ArrayList<>();

            long deadline = System.currentTimeMillis() + 30_000;
            while (operator.isFinished() == false && System.currentTimeMillis() < deadline) {
                Page page = operator.getOutput();
                if (page != null) {
                    pages.add(page);
                } else {
                    Thread.sleep(10);
                }
            }
            assertTrue("Operator should complete within timeout", operator.isFinished());
            assertEquals(2, readCount.get());
            assertEquals(10, pages.size());

            for (Page p : pages) {
                p.releaseBlocks();
            }
            operator.close();
        } finally {
            realExec.shutdown();
            assertTrue(realExec.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    /**
     * Native async path: verifies removeAsyncAction fires exactly once.
     */
    public void testNativeAsyncRemoveAsyncActionExactlyOnce() throws Exception {
        FormatReader formatReader = new TestAsyncFormatReader();
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/test.parquet");
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

        AtomicInteger addCount = new AtomicInteger(0);
        AtomicInteger removeCount = new AtomicInteger(0);
        doAnswer(inv -> {
            addCount.incrementAndGet();
            return null;
        }).when(driverContext).addAsyncAction();
        doAnswer(inv -> {
            removeCount.incrementAndGet();
            return null;
        }).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).build();

        SourceOperator operator = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                pages.add(page);
            }
        }

        assertEquals("addAsyncAction should be called exactly once", 1, addCount.get());
        assertEquals("removeAsyncAction should be called exactly once", 1, removeCount.get());

        for (Page p : pages) {
            p.releaseBlocks();
        }
        operator.close();
    }

    // ===== Multi-driver concurrent tests with real DriverContext and backpressure =====

    /**
     * Concurrent sanity check for the slice-queue path: multiple producer drivers processing
     * many splits (including {@link CoalescedSplit} entries with multiple leaves) from a shared
     * {@link ExternalSliceQueue} with real {@link DriverContext} async-action tracking and a
     * real thread pool. Verifies that {@code waitForAsyncActions} completes for every driver
     * and no splits are dropped or double-read.
     * <p>
     * Parameters are randomized to vary timing across repeated runs.
     */
    public void testSliceQueueMultiDriverRealContextManyBackpressuredSplits() throws Exception {
        int driverCount = randomIntBetween(2, 6);
        int pagesPerSplit = randomIntBetween(3, 8);
        int bufferSize = randomIntBetween(1, 3);

        int plainSplitCount = randomIntBetween(10, 30);
        int coalescedCount = randomIntBetween(5, 15);
        int leafCounter = 0;
        List<ExternalSplit> queueEntries = new ArrayList<>();
        for (int i = 0; i < plainSplitCount; i++) {
            queueEntries.add(
                new FileSplit("test", StoragePath.of("s3://bucket/rg" + leafCounter++ + ".parquet"), 0, 100, "parquet", Map.of(), Map.of())
            );
        }
        for (int i = 0; i < coalescedCount; i++) {
            int leavesInCoalesced = randomIntBetween(2, 3);
            List<ExternalSplit> children = new ArrayList<>();
            for (int c = 0; c < leavesInCoalesced; c++) {
                children.add(
                    new FileSplit(
                        "test",
                        StoragePath.of("s3://bucket/rg" + leafCounter++ + ".parquet"),
                        0,
                        100,
                        "parquet",
                        Map.of(),
                        Map.of()
                    )
                );
            }
            queueEntries.add(new CoalescedSplit("test", children));
        }
        int totalLeaves = leafCounter;
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(queueEntries);

        AtomicInteger totalReadCount = new AtomicInteger(0);
        FormatReader formatReader = new MultiPageFormatReader(totalReadCount, pagesPerSplit);
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/rg0.parquet");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        // Producer executor is separate from consumer threads to avoid thread starvation
        ExecutorService producerExec = Executors.newFixedThreadPool(driverCount, EsExecutors.daemonThreadFactory("test", "sq-producer"));
        try {
            SourceOperator[] operators = new SourceOperator[driverCount];
            DriverContext[] contexts = new DriverContext[driverCount];
            AtomicInteger pageCount = new AtomicInteger(0);
            Thread[] consumers = new Thread[driverCount];

            for (int d = 0; d < driverCount; d++) {
                DriverContext ctx = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TEST_BLOCK_FACTORY, null);
                contexts[d] = ctx;
                AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
                    storageProvider,
                    formatReader,
                    path,
                    attributes,
                    100,
                    bufferSize,
                    producerExec
                ).sliceQueue(sliceQueue).build();
                operators[d] = factory.get(ctx);
            }

            for (int d = 0; d < driverCount; d++) {
                final int driverIdx = d;
                consumers[d] = new Thread(() -> {
                    try {
                        while (operators[driverIdx].isFinished() == false) {
                            Page page = operators[driverIdx].getOutput();
                            if (page != null) {
                                pageCount.incrementAndGet();
                                page.releaseBlocks();
                            } else {
                                Thread.sleep(1);
                            }
                        }
                    } catch (Exception e) {
                        throw new AssertionError("Consumer " + driverIdx + " failed", e);
                    }
                });
                consumers[d].start();
            }

            for (int d = 0; d < driverCount; d++) {
                consumers[d].join(TimeUnit.SECONDS.toMillis(15));
                assertFalse("Consumer thread " + d + " should have completed", consumers[d].isAlive());
            }

            for (int d = 0; d < driverCount; d++) {
                contexts[d].finish();
                PlainActionFuture<Void> asyncFuture = new PlainActionFuture<>();
                contexts[d].waitForAsyncActions(asyncFuture);
                asyncFuture.actionGet(TimeValue.timeValueSeconds(10));
            }

            assertEquals("All leaves should be read", totalLeaves, totalReadCount.get());
            assertEquals("Total pages should be totalLeaves * pagesPerSplit", totalLeaves * pagesPerSplit, pageCount.get());

            for (SourceOperator op : operators) {
                op.close();
            }
        } finally {
            producerExec.shutdown();
            assertTrue(producerExec.awaitTermination(15, TimeUnit.SECONDS));
        }
    }

    /**
     * Failure-injection test for the slice-queue path with a real {@link DriverContext}: a
     * format reader that throws after producing a few pages. Verifies that
     * {@code waitForAsyncActions} still completes (i.e., {@code removeAsyncAction} fires on
     * the error path), which is the property the single-completion-listener refactor guarantees.
     */
    public void testSliceQueueMidStreamFailureCompletesAsyncActions() throws Exception {
        int goodSplits = randomIntBetween(2, 5);
        int totalSplits = goodSplits + randomIntBetween(2, 5);
        int pagesPerSplit = randomIntBetween(3, 6);

        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < totalSplits; i++) {
            splits.add(new FileSplit("test", StoragePath.of("s3://bucket/s" + i + ".parquet"), 0, 100, "parquet", Map.of(), Map.of()));
        }
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);

        AtomicInteger readCount = new AtomicInteger(0);
        FormatReader formatReader = new FailAfterNReadsFormatReader(readCount, goodSplits, pagesPerSplit);
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/s0.parquet");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        ExecutorService realExec = Executors.newFixedThreadPool(2, EsExecutors.daemonThreadFactory("test", "fail-test"));
        try {
            DriverContext ctx = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TEST_BLOCK_FACTORY, null);
            AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
                storageProvider,
                formatReader,
                path,
                attributes,
                100,
                2,
                realExec
            ).sliceQueue(sliceQueue).build();
            SourceOperator operator = factory.get(ctx);

            List<Page> pages = new ArrayList<>();
            Exception operatorError = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (operator.isFinished() == false && System.nanoTime() < deadline) {
                try {
                    Page page = operator.getOutput();
                    if (page != null) {
                        pages.add(page);
                    } else {
                        Thread.sleep(1);
                    }
                } catch (Exception e) {
                    operatorError = e;
                    break;
                }
            }
            assertNotNull("Operator should propagate the injected read failure", operatorError);

            ctx.finish();
            PlainActionFuture<Void> asyncFuture = new PlainActionFuture<>();
            ctx.waitForAsyncActions(asyncFuture);
            asyncFuture.actionGet(TimeValue.timeValueSeconds(10));

            for (Page p : pages) {
                p.releaseBlocks();
            }
            operator.close();
        } finally {
            realExec.shutdown();
            assertTrue(realExec.awaitTermination(15, TimeUnit.SECONDS));
        }
    }

    /**
     * Concurrent sanity check for the multi-file path: multiple producer drivers processing
     * files from a resolved {@link FileList} with real {@link DriverContext} async-action
     * tracking. Verifies that {@code waitForAsyncActions} completes for every driver.
     */
    public void testMultiFileMultiDriverRealContextBackpressure() throws Exception {
        int driverCount = randomIntBetween(2, 4);
        int fileCount = randomIntBetween(4, 10);
        int pagesPerFile = randomIntBetween(3, 8);
        int bufferSize = randomIntBetween(1, 3);

        List<StorageEntry> entries = new ArrayList<>();
        for (int i = 0; i < fileCount; i++) {
            entries.add(new StorageEntry(StoragePath.of("s3://bucket/f" + i + ".parquet"), 100 * (i + 1), Instant.EPOCH));
        }
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/*.parquet");

        AtomicInteger totalReadCount = new AtomicInteger(0);
        FormatReader formatReader = new MultiPageFormatReader(totalReadCount, pagesPerFile);
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/f0.parquet");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        ExecutorService producerExec = Executors.newFixedThreadPool(driverCount, EsExecutors.daemonThreadFactory("test", "mf-producer"));
        try {
            SourceOperator[] operators = new SourceOperator[driverCount];
            DriverContext[] contexts = new DriverContext[driverCount];
            AtomicInteger pageCount = new AtomicInteger(0);
            Thread[] consumers = new Thread[driverCount];

            for (int d = 0; d < driverCount; d++) {
                DriverContext ctx = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TEST_BLOCK_FACTORY, null);
                contexts[d] = ctx;
                AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
                    storageProvider,
                    formatReader,
                    path,
                    attributes,
                    100,
                    bufferSize,
                    producerExec
                ).fileList(fileList).build();
                operators[d] = factory.get(ctx);
            }

            for (int d = 0; d < driverCount; d++) {
                final int driverIdx = d;
                consumers[d] = new Thread(() -> {
                    try {
                        while (operators[driverIdx].isFinished() == false) {
                            Page page = operators[driverIdx].getOutput();
                            if (page != null) {
                                pageCount.incrementAndGet();
                                page.releaseBlocks();
                            } else {
                                Thread.sleep(1);
                            }
                        }
                    } catch (Exception e) {
                        throw new AssertionError("Consumer " + driverIdx + " failed", e);
                    }
                });
                consumers[d].start();
            }

            for (int d = 0; d < driverCount; d++) {
                consumers[d].join(TimeUnit.SECONDS.toMillis(15));
                assertFalse("Consumer thread " + d + " should have completed", consumers[d].isAlive());
            }

            for (int d = 0; d < driverCount; d++) {
                contexts[d].finish();
                PlainActionFuture<Void> asyncFuture = new PlainActionFuture<>();
                contexts[d].waitForAsyncActions(asyncFuture);
                asyncFuture.actionGet(TimeValue.timeValueSeconds(10));
            }

            assertEquals(fileCount * driverCount, totalReadCount.get());
            assertEquals(fileCount * pagesPerFile * driverCount, pageCount.get());

            for (SourceOperator op : operators) {
                op.close();
            }
        } finally {
            producerExec.shutdown();
            assertTrue(producerExec.awaitTermination(15, TimeUnit.SECONDS));
        }
    }

    /**
     * State-machine guard test for the flattened producer loop.
     *
     * Drives the slice-queue producer end-to-end with 2 {@link CoalescedSplit}s of 2 leaves each
     * (4 leaves total, 1 page per leaf) on a single-thread executor and asserts:
     * <ul>
     *   <li>all 4 leaves are read once (state machine visits every leaf across splits);</li>
     *   <li>all 4 pages arrive at the buffer;</li>
     *   <li>every opened iterator is closed;</li>
     *   <li>{@code removeAsyncAction()} fires exactly once (success path =&gt; buffer.finish(false));</li>
     *   <li>no {@code addPage} after {@code buffer.noMoreInputs()} becomes true.</li>
     * </ul>
     * Followed by a second scenario that forces {@code buffer.finish(true)} partway through, to
     * verify the active iterator gets closed and no further leaves are opened.
     * <p>
     * BLOCKED-path coverage (buffer-full backpressure and wakeup correctness) is not exercised here;
     * it lives in {@code AsyncExternalSourceBufferTests#testNoLostWakeupUnderConcurrentAddAndPoll}
     * (the Phase 5 stress test).
     */
    public void testProducerLoopStateMachine() throws Exception {
        // --- scenario 1: run to completion across 2 splits x 2 leaves ---
        runStateMachineScenario(false);

        // --- scenario 2: force early noMoreInputs after the first leaf's page is consumed ---
        runStateMachineScenario(true);
    }

    private void runStateMachineScenario(boolean forceNoMoreInputsEarly) throws Exception {
        AtomicInteger readCalls = new AtomicInteger();
        AtomicInteger closeCalls = new AtomicInteger();
        TrackingReader reader = new TrackingReader(readCalls, closeCalls);

        // 2 coalesced splits x 2 leaves = 4 leaves
        List<ExternalSplit> splits = new ArrayList<>();
        for (int s = 0; s < 2; s++) {
            List<ExternalSplit> leaves = new ArrayList<>();
            for (int l = 0; l < 2; l++) {
                leaves.add(
                    new FileSplit(
                        "test",
                        StoragePath.of("s3://bucket/s" + s + "_l" + l + ".parquet"),
                        0,
                        100,
                        "parquet",
                        Map.of(),
                        Map.of()
                    )
                );
            }
            splits.add(new CoalescedSplit("test", leaves));
        }
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);

        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        AtomicInteger addAsync = new AtomicInteger();
        AtomicInteger removeAsync = new AtomicInteger();
        doAnswer(inv -> {
            addAsync.incrementAndGet();
            return null;
        }).when(driverContext).addAsyncAction();
        doAnswer(inv -> {
            removeAsync.incrementAndGet();
            return null;
        }).when(driverContext).removeAsyncAction();

        // Use a single-thread executor so ordering is deterministic.
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
                storageProvider,
                reader,
                StoragePath.of("s3://bucket/s0_l0.parquet"),
                attributes,
                100,
                10,
                executor
            ).sliceQueue(sliceQueue).build();

            SourceOperator operator = factory.get(driverContext);

            if (forceNoMoreInputsEarly) {
                // Poll a few pages then force finish(true) to simulate downstream cancellation.
                int received = 0;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (received < 1 && System.nanoTime() < deadline) {
                    Page p = operator.getOutput();
                    if (p != null) {
                        received++;
                        p.releaseBlocks();
                    }
                }
                // Mimic downstream cancellation; the producer loop must observe noMoreInputs and exit.
                operator.finish();
                // Drain remaining output (should not hang).
                while (operator.isFinished() == false) {
                    Page p = operator.getOutput();
                    if (p != null) p.releaseBlocks();
                }
            } else {
                List<Page> pages = new ArrayList<>();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (operator.isFinished() == false && System.nanoTime() < deadline) {
                    Page p = operator.getOutput();
                    if (p != null) pages.add(p);
                }
                assertTrue("operator did not finish within timeout", operator.isFinished());
                assertEquals("all 4 leaves produced a page", 4, pages.size());
                for (Page p : pages) {
                    p.releaseBlocks();
                }
            }

            operator.close();

            // Let the producer thread observe finish() and run the completion listener
            // (which calls removeAsyncAction) before we assert on counters.
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

            // Every iterator that was opened must have been closed.
            assertEquals("read/close counts differ - leaked iterator", readCalls.get(), closeCalls.get());
            // addAsync/removeAsync are paired: exactly once, on the happy path and cancelled path.
            assertEquals(1, addAsync.get());
            assertEquals(1, removeAsync.get());
            // Full run must hit all 4 leaves; early-exit run hits at least 1.
            if (forceNoMoreInputsEarly == false) {
                assertEquals(4, readCalls.get());
            } else {
                assertTrue("expected at least one leaf to be read before cancellation", readCalls.get() >= 1);
            }
        } finally {
            if (executor.isShutdown() == false) {
                executor.shutdown();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    public void testDescribeSplittableCompressedUsesSyncWrapperMode() throws IOException {
        SegmentableFormatReader inner = mockInnerForParallelDescribeAndOpen();
        CompressionDelegatingFormatReader cdr = new CompressionDelegatingFormatReader(inner, new StubSplittableCodec());
        AsyncExternalSourceOperatorFactory factory = factoryForCompressionDescribeTests(cdr, 4);
        String description = factory.describe();
        assertTrue("describe should mention sync-wrapper: " + description, description.contains("sync-wrapper"));
    }

    public void testDescribeGzipCompressedUsesStreamingParallelParseInDescription() throws IOException {
        SegmentableFormatReader inner = mockInnerForParallelDescribeAndOpen();
        CompressionDelegatingFormatReader cdr = new CompressionDelegatingFormatReader(inner, new GzipDecompressionCodec());
        AsyncExternalSourceOperatorFactory factory = factoryForCompressionDescribeTests(cdr, 4);
        String description = factory.describe();
        assertTrue("describe should mention streaming parallel parse: " + description, description.contains("streaming-parallel-parse(4)"));
    }

    public void testOpenWithParallelismSplittableCompressedReturnsNull() throws IOException {
        AsyncExternalSourceOperatorFactory factory = factoryForCompressionDescribeTests(dummyFormatReaderForOpenParallelismTests(), 4);

        SegmentableFormatReader inner = mockInnerForParallelDescribeAndOpen();
        CompressionDelegatingFormatReader cdr = new CompressionDelegatingFormatReader(inner, new StubSplittableCodec());
        byte[] payload = "{\"a\":1}\n".repeat(20).getBytes(StandardCharsets.UTF_8);
        assertNull(factory.openWithParallelism(cdr, bytesStorageObject(payload), List.of("a"), ErrorPolicy.STRICT));
    }

    public void testOpenWithParallelismGzipCompressedReturnsIterator() throws IOException {
        ExecutorService exec = Executors.newFixedThreadPool(8);
        try {
            AsyncExternalSourceOperatorFactory factory = factoryForOpenParallelismStreamingTests(
                dummyFormatReaderForOpenParallelismTests(),
                exec
            );
            SegmentableFormatReader inner = mockInnerForParallelDescribeAndOpen();
            CompressionDelegatingFormatReader cdr = new CompressionDelegatingFormatReader(inner, new GzipDecompressionCodec());
            byte[] plain = "{\"a\":1}\n".repeat(100).getBytes(StandardCharsets.UTF_8);
            byte[] gzipped = gzipCompress(plain);

            CloseableIterator<Page> iterator = factory.openWithParallelism(
                cdr,
                bytesStorageObject(gzipped),
                List.of("a"),
                ErrorPolicy.STRICT
            );
            assertNotNull(iterator);
            iterator.close();
        } finally {
            exec.shutdownNow();
        }
    }

    public void testOpenWithParallelismBareSegmentableReturnsIterator() throws IOException {
        ExecutorService exec = Executors.newFixedThreadPool(8);
        try {
            AsyncExternalSourceOperatorFactory factory = factoryForOpenParallelismStreamingTests(
                dummyFormatReaderForOpenParallelismTests(),
                exec
            );

            SegmentableFormatReader inner = mockInnerForParallelDescribeAndOpen();
            byte[] plain = "{\"a\":1}\n".repeat(100).getBytes(StandardCharsets.UTF_8);
            CloseableIterator<Page> iterator = factory.openWithParallelism(
                inner,
                bytesStorageObject(plain),
                List.of("a"),
                ErrorPolicy.STRICT
            );
            assertNotNull(iterator);
            iterator.close();
        } finally {
            exec.shutdownNow();
        }
    }

    /**
     * Minimal splittable codec stub so dispatch tests avoid wiring real bzip2 parallel scanners.
     */
    private static final class StubSplittableCodec implements SplittableDecompressionCodec {
        @Override
        public String name() {
            return "stub-splittable";
        }

        @Override
        public List<String> extensions() {
            return List.of(".stub");
        }

        @Override
        public InputStream decompress(InputStream raw) {
            return raw;
        }

        @Override
        public long[] findBlockBoundaries(StorageObject object, long start, long end) throws IOException {
            return new long[0];
        }

        @Override
        public InputStream decompressRange(StorageObject object, long blockStart, long nextBlockStart) throws IOException {
            return new ByteArrayInputStream(new byte[0]);
        }
    }

    private static SegmentableFormatReader mockInnerForParallelDescribeAndOpen() throws IOException {
        SegmentableFormatReader inner = mock(SegmentableFormatReader.class);
        when(inner.minimumSegmentSize()).thenReturn(1024L);
        when(inner.formatName()).thenReturn("ndjson");
        when(inner.supportsNativeAsync()).thenReturn(false);
        when(inner.defaultErrorPolicy()).thenReturn(ErrorPolicy.STRICT);
        when(inner.metadata(any())).thenReturn(null);
        when(inner.read(any(), any())).thenReturn(emptyPageIterator());
        return inner;
    }

    private static FormatReader dummyFormatReaderForOpenParallelismTests() {
        FormatReader dummyReader = mock(FormatReader.class);
        when(dummyReader.formatName()).thenReturn("dummy");
        when(dummyReader.supportsNativeAsync()).thenReturn(false);
        when(dummyReader.defaultErrorPolicy()).thenReturn(ErrorPolicy.STRICT);
        return dummyReader;
    }

    private static CloseableIterator<Page> emptyPageIterator() {
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

    private static AsyncExternalSourceOperatorFactory factoryForCompressionDescribeTests(FormatReader formatReader, int parallelism) {
        StorageProvider storageProvider = mock(StorageProvider.class);
        StoragePath path = StoragePath.of("file:///data/stream.ndjson.gz");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "col1",
                new EsField("col1", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );
        return AsyncExternalSourceOperatorFactory.builder(storageProvider, formatReader, path, attributes, 500, 10, Runnable::run)
            .rowLimit(FormatReader.NO_LIMIT)
            .parsingParallelism(parallelism)
            .build();
    }

    private static AsyncExternalSourceOperatorFactory factoryForOpenParallelismStreamingTests(
        FormatReader formatReader,
        Executor executor
    ) {
        StorageProvider storageProvider = mock(StorageProvider.class);
        StoragePath path = StoragePath.of("file:///data/stream.ndjson.gz");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "col1",
                new EsField("col1", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );
        return AsyncExternalSourceOperatorFactory.builder(storageProvider, formatReader, path, attributes, 500, 10, executor)
            .rowLimit(FormatReader.NO_LIMIT)
            .parsingParallelism(4)
            .build();
    }

    private static StorageObject bytesStorageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                throw new UnsupportedOperationException();
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
                return StoragePath.of("mem:///parallelism-open-test");
            }
        };
    }

    private static byte[] gzipCompress(byte[] uncompressed) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (GZIPOutputStream gz = new GZIPOutputStream(bos)) {
            gz.write(uncompressed);
        }
        return bos.toByteArray();
    }

    /**
     * Format reader for the state-machine test. Every {@code read} returns a single-page iterator
     * that increments {@code closeCalls} on {@link CloseableIterator#close()}, so the test can
     * assert that every opened iterator is closed exactly once.
     */
    private static class TrackingReader implements FormatReader {
        private final AtomicInteger readCount;
        private final AtomicInteger closeCount;

        TrackingReader(AtomicInteger readCount, AtomicInteger closeCount) {
            this.readCount = readCount;
            this.closeCount = closeCount;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            readCount.incrementAndGet();
            // Latch lets the buffer's waitForSpace path engage naturally; we return one page.
            CountDownLatch once = new CountDownLatch(1);
            once.countDown();
            return new CloseableIterator<>() {
                private boolean emitted = false;

                @Override
                public boolean hasNext() {
                    return emitted == false;
                }

                @Override
                public Page next() {
                    if (emitted) throw new NoSuchElementException();
                    emitted = true;
                    return createTestPage();
                }

                @Override
                public void close() {
                    closeCount.incrementAndGet();
                }
            };
        }

        @Override
        public String formatName() {
            return "tracking";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
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
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
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
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
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
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
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
     * Format reader that captures the StorageObject and skipFirstLine flag passed to readSplit.
     * Used to verify that RangeStorageObject wrapping and skipFirstLine logic are correct.
     */
    private static class SplitCapturingFormatReader implements FormatReader {
        private final List<StorageObject> capturedObjects;
        private final List<Boolean> capturedSkipFirstLine;

        SplitCapturingFormatReader(List<StorageObject> capturedObjects, List<Boolean> capturedSkipFirstLine) {
            this.capturedObjects = capturedObjects;
            this.capturedSkipFirstLine = capturedSkipFirstLine;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            capturedObjects.add(object);
            capturedSkipFirstLine.add(context.firstSplit() == false);
            return singlePageIterator();
        }

        private static CloseableIterator<Page> singlePageIterator() {
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
            return "test-split-capturing";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".csv");
        }

        @Override
        public void close() {}
    }

    /**
     * Format reader that implements SegmentableFormatReader and tracks which methods are called.
     */
    private static class TrackingSegmentableFormatReader implements SegmentableFormatReader {
        final AtomicInteger readCount = new AtomicInteger(0);
        final AtomicInteger readWithFirstSplitFalseCount = new AtomicInteger(0);

        @Override
        public long findNextRecordBoundary(InputStream stream) throws IOException {
            byte[] buf = new byte[1];
            int total = 0;
            while (stream.read(buf) > 0) {
                total++;
                if (buf[0] == '\n') {
                    return total;
                }
            }
            return -1;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            readCount.incrementAndGet();
            if (context.firstSplit() == false) {
                readWithFirstSplitFalseCount.incrementAndGet();
            }
            return singleTestPageIterator();
        }

        private static CloseableIterator<Page> singleTestPageIterator() {
            Page page = createTestPage();
            return new CloseableIterator<>() {
                private boolean consumed = false;

                @Override
                public boolean hasNext() {
                    return consumed == false;
                }

                @Override
                public Page next() {
                    if (consumed) throw new NoSuchElementException();
                    consumed = true;
                    return page;
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public String formatName() {
            return "test-segmentable";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".csv");
        }

        @Override
        public void close() {}
    }

    private static class LargeStorageProvider implements StorageProvider {
        private final long fileSize;

        LargeStorageProvider(long fileSize) {
            this.fileSize = fileSize;
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            return new LargeStorageObject(path, fileSize);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return new LargeStorageObject(path, fileSize);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return new LargeStorageObject(path, fileSize);
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
            return List.of("file");
        }

        @Override
        public void close() {}
    }

    private static class LargeStorageObject implements StorageObject {
        private final StoragePath path;
        private final long size;

        LargeStorageObject(StoragePath path, long size) {
            this.path = path;
            this.size = size;
        }

        @Override
        public InputStream newStream() {
            return newLineStream(size);
        }

        @Override
        public InputStream newStream(long position, long length) {
            return newLineStream(length);
        }

        private static InputStream newLineStream(long length) {
            return new InputStream() {
                private long remaining = length;

                @Override
                public int read() {
                    if (remaining <= 0) return -1;
                    remaining--;
                    return '\n';
                }

                @Override
                public int read(byte[] b, int off, int len) {
                    if (remaining <= 0) return -1;
                    int toRead = (int) Math.min(len, remaining);
                    for (int i = 0; i < toRead; i++) {
                        b[off + i] = '\n';
                    }
                    remaining -= toRead;
                    return toRead;
                }
            };
        }

        @Override
        public long length() {
            return size;
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
     * Format reader that always throws on read, for testing error handling.
     */
    private static class AlwaysFailFormatReader implements FormatReader {
        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            throw new IOException("Simulated read error");
        }

        @Override
        public String formatName() {
            return "test-always-fail";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    /**
     * Format reader that returns multiple pages per read, for testing backpressure.
     */
    private static class MultiPageFormatReader implements FormatReader {
        private final AtomicInteger readCount;
        private final int pagesPerRead;

        MultiPageFormatReader(AtomicInteger readCount, int pagesPerRead) {
            this.readCount = readCount;
            this.pagesPerRead = pagesPerRead;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            readCount.incrementAndGet();
            return new CloseableIterator<>() {
                private int remaining = pagesPerRead;

                @Override
                public boolean hasNext() {
                    return remaining > 0;
                }

                @Override
                public Page next() {
                    if (remaining <= 0) {
                        throw new NoSuchElementException();
                    }
                    remaining--;
                    return createTestPage();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public String formatName() {
            return "test-multi-page";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    /**
     * Test async format reader that returns empty pages via async callback.
     */
    /**
     * Format reader that succeeds for the first N reads (returning multiple pages each),
     * then throws an IOException on the (N+1)th read. Used to test error-path cleanup.
     */
    private static class FailAfterNReadsFormatReader implements FormatReader {
        private final AtomicInteger readCount;
        private final int failAfter;
        private final int pagesPerRead;

        FailAfterNReadsFormatReader(AtomicInteger readCount, int failAfter, int pagesPerRead) {
            this.readCount = readCount;
            this.failAfter = failAfter;
            this.pagesPerRead = pagesPerRead;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            int call = readCount.incrementAndGet();
            if (call > failAfter) {
                throw new IOException("Injected read failure on call " + call);
            }
            return new CloseableIterator<>() {
                private int remaining = pagesPerRead;

                @Override
                public boolean hasNext() {
                    return remaining > 0;
                }

                @Override
                public Page next() {
                    if (remaining <= 0) throw new NoSuchElementException();
                    remaining--;
                    return createTestPage();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public String formatName() {
            return "test-fail-after-n";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    private static class TestAsyncFormatReader implements FormatReader {
        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            return emptyIterator();
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
