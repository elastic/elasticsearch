/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Limiter;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ExternalMetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDecompressionCodec;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonFormatReader;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractMeteredStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SplittableDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.hamcrest.Matchers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
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

    public void testResolveDispatchModeSequentialWhenSplitterNotStridedSafe() {
        // A quoted CSV/TSV reader reports a non-strided splitter, so the uncompressed file must be read as one
        // sequential stream through the streaming coordinator rather than segmented at arbitrary offsets.
        SegmentableFormatReader quoted = mock(SegmentableFormatReader.class);
        RecordSplitter nonStrided = mock(RecordSplitter.class);
        when(nonStrided.supportsStridedProbing()).thenReturn(false);
        when(quoted.recordSplitter()).thenReturn(nonStrided);
        assertEquals(
            AsyncExternalSourceOperatorFactory.ParallelDispatchMode.SEGMENTABLE_UNCOMPRESSED_SEQUENTIAL,
            AsyncExternalSourceOperatorFactory.resolveDispatchMode(quoted)
        );

        // A plain (quoting-off) reader keeps strided probing, so it stays on the offset-segmented parallel path.
        SegmentableFormatReader plain = mock(SegmentableFormatReader.class);
        RecordSplitter strided = mock(RecordSplitter.class);
        when(strided.supportsStridedProbing()).thenReturn(true);
        when(plain.recordSplitter()).thenReturn(strided);
        assertEquals(
            AsyncExternalSourceOperatorFactory.ParallelDispatchMode.SEGMENTABLE_UNCOMPRESSED,
            AsyncExternalSourceOperatorFactory.resolveDispatchMode(plain)
        );
    }

    public void testConstructorValidation() {
        StorageProvider storageProvider = mock(StorageProvider.class);
        FormatReader formatReader = mock(FormatReader.class);
        when(formatReader.rowPositionStrategy()).thenReturn(PassThroughRowPositionStrategy.INSTANCE);
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

    /**
     * Bound metadata names are unioned into {@code partitionColumnNames} so VirtualColumnIterator
     * materializes them. A physical {@code _file.*} column is not engine-owned and stays out of
     * that set, as does {@code _rowPosition} (a {@link MetadataAttribute}).
     */
    public void testPartitionColumnNamesUnionEngineOwnedAndExcludeRowPosition() {
        Attribute value = new FieldAttribute(
            Source.EMPTY,
            "value",
            new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
        Attribute boundIndex = new ExternalMetadataAttribute(Source.EMPTY, "_index", DataType.KEYWORD);
        Attribute physicalFileSize = new ReferenceAttribute(Source.EMPTY, FileMetadataColumns.SIZE, DataType.LONG);
        Attribute rowPosition = SyntheticColumns.newRowPositionMetadataAttribute(Source.EMPTY);

        AsyncExternalSourceOperatorFactory factory = factoryWithAttributes(List.of(value, boundIndex, physicalFileSize, rowPosition));

        assertTrue(rowPosition instanceof MetadataAttribute);
        assertThat(factory.partitionColumnNames(), Matchers.contains("_index"));
        assertFalse(factory.partitionColumnNames().contains(FileMetadataColumns.SIZE));
        assertFalse(factory.partitionColumnNames().contains(ColumnExtractor.ROW_POSITION_COLUMN));
    }

    public void testBoundFileMetadataEntersPartitionColumnNames() {
        Attribute path = new ExternalMetadataAttribute(Source.EMPTY, FileMetadataColumns.PATH, DataType.KEYWORD);
        AsyncExternalSourceOperatorFactory factory = factoryWithAttributes(List.of(path));
        assertThat(factory.partitionColumnNames(), Matchers.contains(FileMetadataColumns.PATH));
    }

    private static AsyncExternalSourceOperatorFactory factoryWithAttributes(List<Attribute> attributes) {
        StorageProvider storageProvider = mock(StorageProvider.class);
        FormatReader formatReader = mock(FormatReader.class);
        when(formatReader.rowPositionStrategy()).thenReturn(PassThroughRowPositionStrategy.INSTANCE);
        return AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            StoragePath.of("file:///test.csv"),
            attributes,
            1000,
            10,
            Runnable::run
        ).build();
    }

    public void testDescribeSyncWrapperMode() {
        StorageProvider storageProvider = mock(StorageProvider.class);
        FormatReader formatReader = mock(FormatReader.class);
        when(formatReader.rowPositionStrategy()).thenReturn(PassThroughRowPositionStrategy.INSTANCE);
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
        assertTrue(description.contains("ExternalDataSourceOperator"));
        assertTrue(description.contains("csv"));
        assertTrue(description.contains("sync-wrapper"));
        assertTrue(description.contains("test.csv"));
        assertTrue(description.contains("500"));
        assertTrue(description.contains("maxBufferBytes="));
    }

    public void testDescribeNativeAsyncMode() {
        StorageProvider storageProvider = mock(StorageProvider.class);
        FormatReader formatReader = mock(FormatReader.class);
        when(formatReader.rowPositionStrategy()).thenReturn(PassThroughRowPositionStrategy.INSTANCE);
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
        assertTrue(description.contains("ExternalDataSourceOperator"));
        assertTrue(description.contains("parquet"));
        assertTrue(description.contains("native-async"));
        assertTrue(description.contains("data.parquet"));
    }

    public void testAccessors() {
        StorageProvider storageProvider = mock(StorageProvider.class);
        FormatReader formatReader = mock(FormatReader.class);
        when(formatReader.rowPositionStrategy()).thenReturn(PassThroughRowPositionStrategy.INSTANCE);
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
        // No distinct consumer executor supplied: the drain shares the read/parse executor (prior single-pool behavior).
        assertSame(executor, factory.producerExecutor());
    }

    /**
     * The factory holds the unwrapped configured reader. Each listed object's compression is applied
     * from that object's name via {@code wrapForObject}, so gzip and plain csv of one format both
     * present decompressed bytes to the inner reader.
     */
    public void testPerFileWrapDecompressesGzipLeavingConfiguredReaderUnwrapped() throws Exception {
        byte[] plain = "n\n1\n".getBytes(StandardCharsets.UTF_8);
        byte[] gzipped = gzipCompress(plain);
        Map<String, byte[]> bodies = Map.of("s3://bucket/a.csv.gz", gzipped, "s3://bucket/b.csv", plain);
        ByteArrayStorageProvider storage = new ByteArrayStorageProvider(bodies);
        StreamPeekingFormatReader reader = new StreamPeekingFormatReader();
        DecompressionCodecRegistry codecs = new DecompressionCodecRegistry();
        codecs.register(new GzipDecompressionCodec());
        FormatReaderRegistry formatRegistry = new FormatReaderRegistry(codecs);

        FileList fileList = GlobExpander.fileListOf(
            List.of(
                new StorageEntry(StoragePath.of("s3://bucket/a.csv.gz"), gzipped.length, Instant.EPOCH),
                new StorageEntry(StoragePath.of("s3://bucket/b.csv"), plain.length, Instant.EPOCH)
            ),
            "s3://bucket/*.csv*"
        );

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(mock(BlockFactory.class));
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        List<Attribute> attributes = List.of(
            new FieldAttribute(Source.EMPTY, "n", new EsField("n", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
        );

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storage,
            reader,
            StoragePath.of("s3://bucket/*.csv*"),
            attributes,
            100,
            10,
            Runnable::run
        ).fileList(fileList).formatReaderRegistry(formatRegistry).build();

        assertSame("scan keeps the unwrapped configured reader", reader, factory.formatReader());

        SourceOperator operator = factory.get(driverContext);
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                page.releaseBlocks();
            }
        }
        operator.close();

        assertEquals(2, reader.peeked.size());
        for (int first : reader.peeked) {
            assertEquals("gzip magic must not reach the inner reader", (int) 'n', first);
        }
    }

    /**
     * Single-file {@code .csv.gz} also wraps from this object's name once the factory stops wrapping
     * from the resource leaf at construction.
     */
    public void testSingleFileWrapDecompressesGzipFromObjectName() throws Exception {
        byte[] plain = "n\n1\n".getBytes(StandardCharsets.UTF_8);
        byte[] gzipped = gzipCompress(plain);
        StoragePath path = StoragePath.of("s3://bucket/hits.csv.gz");
        ByteArrayStorageProvider storage = new ByteArrayStorageProvider(Map.of(path.toString(), gzipped));
        StreamPeekingFormatReader reader = new StreamPeekingFormatReader();
        DecompressionCodecRegistry codecs = new DecompressionCodecRegistry();
        codecs.register(new GzipDecompressionCodec());
        FormatReaderRegistry formatRegistry = new FormatReaderRegistry(codecs);

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(mock(BlockFactory.class));
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        List<Attribute> attributes = List.of(
            new FieldAttribute(Source.EMPTY, "n", new EsField("n", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
        );

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storage,
            reader,
            path,
            attributes,
            100,
            10,
            Runnable::run
        ).formatReaderRegistry(formatRegistry).build();

        assertSame(reader, factory.formatReader());
        SourceOperator operator = factory.get(driverContext);
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                page.releaseBlocks();
            }
        }
        operator.close();
        assertEquals(List.of((int) 'n'), reader.peeked);
    }

    public void testProducerExecutorWiredDistinctFromReadExecutor() {
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
        Executor readExecutor = Runnable::run;
        Executor producerExecutor = Runnable::run;

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            500,
            15,
            readExecutor
        ).producerExecutor(producerExecutor).build();

        // Production wires the drain onto a distinct pool (esql_worker) from the read/parse pool (esql_external_io);
        // the two accessors must return the two distinct executors, not collapse onto one.
        assertSame(readExecutor, factory.executor());
        assertSame(producerExecutor, factory.producerExecutor());
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
        .breaker(NoopCircuitBreaker.INSTANCE)
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

    /**
     * {@code openNextMultiFile} must call the length overload, including listed size {@code 0}.
     * A row-count-only assertion would pass on the path-only constructor.
     */
    public void testOpenNextMultiFileSeedsListedSizeIncludingEmpty() throws Exception {
        StoragePath sized = StoragePath.of("s3://bucket/data/f1.parquet");
        StoragePath empty = StoragePath.of("s3://bucket/data/empty.parquet");
        FileList fileList = GlobExpander.fileListOf(
            List.of(new StorageEntry(sized, 100, Instant.EPOCH), new StorageEntry(empty, 0, Instant.EPOCH)),
            "s3://bucket/data/*.parquet"
        );
        RecordingMultiFileStorageProvider storageProvider = new RecordingMultiFileStorageProvider();
        drainMultiFileOperator(storageProvider, fileList, sized);
        assertEquals(
            List.of(
                new RecordingMultiFileStorageProvider.Call(sized, 100L, null),
                new RecordingMultiFileStorageProvider.Call(empty, 0L, null)
            ),
            storageProvider.calls
        );
    }

    public void testOpenNextMultiFileSeedsMtimeWhenKnown() throws Exception {
        StoragePath path = StoragePath.of("s3://bucket/data/f1.parquet");
        Instant mtime = Instant.ofEpochMilli(1_700_000_000_000L);
        FileList fileList = GlobExpander.fileListOf(List.of(new StorageEntry(path, 100, mtime)), "s3://bucket/data/*.parquet");
        RecordingMultiFileStorageProvider storageProvider = new RecordingMultiFileStorageProvider();
        drainMultiFileOperator(storageProvider, fileList, path);
        assertEquals(List.of(new RecordingMultiFileStorageProvider.Call(path, 100L, mtime)), storageProvider.calls);
    }

    /**
     * Collision regression (the {@code date=<one-day>} shape). A Hive partition key {@code year}
     * shadows a same-named physical column, so the unified attributes are [id, value, year] with
     * {@code year} the appended partition column, while the file-backed {@link ColumnMapping} is
     * data-only (width 2). Before the fix, {@code queryDataSchema} kept the partition column
     * (width 3) and tripped {@link SchemaAdaptingIterator}'s size-vs-width guard at read time. The
     * factory must now build a data-only {@code queryDataSchema} (width 2) so the read succeeds and
     * the partition (path-derived) value wins.
     */
    public void testCollidingPartitionColumnReadsWithoutTrippingGuard() throws Exception {
        StoragePath filePath = StoragePath.of("s3://bucket/data/year=2024/f1.parquet");
        List<StorageEntry> entries = List.of(new StorageEntry(filePath, 100, Instant.EPOCH));
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/**/*.parquet");

        // File body has [id, value]; the physical 'year' was already shadowed/dropped by the resolver.
        Page filePage = new Page(
            2,
            new IntBlock[] {
                TEST_BLOCK_FACTORY.newIntArrayVector(new int[] { 7, 8 }, 2).asBlock(),
                TEST_BLOCK_FACTORY.newIntArrayVector(new int[] { 100, 200 }, 2).asBlock() }
        );
        FormatReader formatReader = new SinglePageReader(() -> filePage);
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        // Unified attributes: data columns followed by the appended partition 'year' (shadows physical).
        // id is LONG because the mapping casts INT -> LONG; attributes carry the plan's output type, not the file's physical type.
        List<Attribute> attributes = List.of(ref("id", DataType.LONG), ref("value", DataType.INTEGER), ref("year", DataType.INTEGER));

        // Non-identity, data-only mapping (cast id INT->LONG) so adaptSchema does not short-circuit.
        ExternalSchema fileSchema = new ExternalSchema(List.of(ref("id", DataType.INTEGER), ref("value", DataType.INTEGER)));
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, 1 }, new DataType[] { DataType.LONG, null });
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = Map.of(
            filePath,
            new SchemaReconciliation.FileSchemaInfo(fileSchema, mapping, null)
        );

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            filePath,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).fileList(fileList).schemaMap(schemaMap).partitionColumnNames(Set.of("year")).partitionValues(Map.of("year", 2024)).build();

        SourceOperator operator = factory.get(driverContext);
        assertNotNull(operator);

        List<Page> pages = new ArrayList<>();
        try {
            while (operator.isFinished() == false) {
                Page page = operator.getOutput();
                if (page != null) {
                    pages.add(page);
                }
            }

            assertEquals("one page produced", 1, pages.size());
            Page page = pages.get(0);
            assertEquals("data columns + injected partition column", 3, page.getBlockCount());
            assertEquals(2, page.getPositionCount());

            LongBlock idBlock = page.getBlock(0); // id widened INT -> LONG by the mapping
            assertEquals(7L, idBlock.getLong(0));
            assertEquals(8L, idBlock.getLong(1));
            IntBlock valueBlock = page.getBlock(1);
            assertEquals(100, valueBlock.getInt(0));
            // Partition value wins: 'year' carries the path-derived value, not a physical-column value.
            IntBlock yearBlock = page.getBlock(2);
            assertEquals(2024, yearBlock.getInt(0));
            assertEquals(2024, yearBlock.getInt(1));
        } finally {
            for (Page p : pages) {
                p.releaseBlocks();
            }
            operator.close();
        }
    }

    /**
     * FFW Hive partition key that is not last: physical {@code [a, city, b]}, {@code city}
     * partitioned. {@code computeMapping([a, b], physical)} is {@code [0, 2]} — same width as
     * {@code queryDataSchema} — while the reader emits the projected page {@code [a, b]}.
     * Unconditional {@code alignToQuery} must rewrite the mapping to {@code [0, 1]} so
     * {@code mapPage} does not ask for block 2 of a 2-block page.
     */
    public void testHiveMiddlePartitionKeyEqualWidthStillRealigns() throws Exception {
        StoragePath filePath = StoragePath.of("s3://bucket/data/city=10/f1.parquet");
        List<StorageEntry> entries = List.of(new StorageEntry(filePath, 100, Instant.EPOCH));
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/**/*.parquet");

        // Reader is asked for [a, b]; city is the partition key, not a physical block.
        Page filePage = new Page(
            2,
            new IntBlock[] {
                TEST_BLOCK_FACTORY.newIntArrayVector(new int[] { 1, 2 }, 2).asBlock(),
                TEST_BLOCK_FACTORY.newIntArrayVector(new int[] { 3, 4 }, 2).asBlock() }
        );
        FormatReader formatReader = new SinglePageReader(() -> filePage);
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        List<Attribute> attributes = List.of(ref("a", DataType.INTEGER), ref("b", DataType.INTEGER), ref("city", DataType.INTEGER));

        ExternalSchema fileSchema = new ExternalSchema(
            List.of(ref("a", DataType.INTEGER), ref("city", DataType.INTEGER), ref("b", DataType.INTEGER))
        );
        // File-natural mapping of data columns [a, b] into physical [a, city, b].
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, 2 }, null);
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = Map.of(
            filePath,
            new SchemaReconciliation.FileSchemaInfo(fileSchema, mapping, null)
        );

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            filePath,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).fileList(fileList).schemaMap(schemaMap).partitionColumnNames(Set.of("city")).partitionValues(Map.of("city", 10)).build();

        SourceOperator operator = factory.get(driverContext);
        assertNotNull(operator);

        List<Page> pages = new ArrayList<>();
        try {
            while (operator.isFinished() == false) {
                Page page = operator.getOutput();
                if (page != null) {
                    pages.add(page);
                }
            }

            assertEquals("one page produced", 1, pages.size());
            Page page = pages.get(0);
            assertEquals("data columns + injected partition column", 3, page.getBlockCount());
            assertEquals(2, page.getPositionCount());

            IntBlock aBlock = page.getBlock(0);
            assertEquals(1, aBlock.getInt(0));
            assertEquals(2, aBlock.getInt(1));
            IntBlock bBlock = page.getBlock(1);
            assertEquals(3, bBlock.getInt(0));
            assertEquals(4, bBlock.getInt(1));
            IntBlock cityBlock = page.getBlock(2);
            assertEquals(10, cityBlock.getInt(0));
            assertEquals(10, cityBlock.getInt(1));
        } finally {
            for (Page p : pages) {
                p.releaseBlocks();
            }
            operator.close();
        }
    }

    /**
     * Zero-split multi-file + unified-width {@code schemaMap} + narrow query. Discovery left a
     * resolved {@link FileList} and no splits, so the read takes {@code openNextMultiFile} which
     * used to pass the unified-width mapping to {@link SchemaAdaptingIterator} and trip the
     * size-vs-width guard. This file lacks the query column: {@code adaptSchema} must realign
     * to query width and null-fill instead of throwing {@link IllegalArgumentException}.
     */
    public void testZeroSplitUnifiedMappingNarrowQueryNullFillsInsteadOfTrippingGuard() throws Exception {
        StoragePath filePath = StoragePath.of("s3://bucket/data/f1.parquet");
        List<StorageEntry> entries = List.of(new StorageEntry(filePath, 100, Instant.EPOCH));
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/*.parquet");

        // File body is [id, extra]; unified schema also has city, which this file lacks.
        // Empty projection (city is not in the file) so the reader emits a position-only page.
        FormatReader formatReader = new SinglePageReader(() -> new Page(2));
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        List<Attribute> attributes = List.of(ref("city", DataType.INTEGER));

        ExternalSchema fileSchema = new ExternalSchema(List.of(ref("id", DataType.INTEGER), ref("extra", DataType.INTEGER)));
        // Unified-width non-identity mapping: id, city (missing), extra. Width 3 vs query width 1.
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, -1, 1 }, new DataType[] { DataType.LONG, null, null });
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = Map.of(
            filePath,
            new SchemaReconciliation.FileSchemaInfo(fileSchema, mapping, null)
        );

        // A real context rather than this suite's usual mock: the absent-column warning is delivered into its warning
        // sink, which a stub would swallow, leaving the assertion at the end of this method nothing to read.
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TEST_BLOCK_FACTORY, null);

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            filePath,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).fileList(fileList).schemaMap(schemaMap).build();

        SourceOperator operator = factory.get(driverContext);
        assertNotNull(operator);

        List<Page> pages = new ArrayList<>();
        try {
            while (operator.isFinished() == false) {
                Page page = operator.getOutput();
                if (page != null) {
                    pages.add(page);
                }
            }

            assertEquals("one page produced", 1, pages.size());
            Page page = pages.get(0);
            assertEquals("narrow query projects one column", 1, page.getBlockCount());
            assertEquals(2, page.getPositionCount());
            assertTrue("city is absent from this file so the adapter null-fills", page.getBlock(0).areAllValuesNull());
        } finally {
            for (Page p : pages) {
                p.releaseBlocks();
            }
            operator.close();
        }
        driverContext.finish();
        assertThat(driverContext.warnings(), contains(SkipWarnings.absentDeclaredColumnMessage("city")));
        Releasables.close(driverContext.getSnapshot());
    }

    /**
     * Same zero-split arm as {@link #testZeroSplitUnifiedMappingNarrowQueryNullFillsInsteadOfTrippingGuard},
     * but the one projected column is present in the file (the {@code STATS BY city} shape when
     * {@code city} is on disk). The adapter must emit the file values, not trip the width guard.
     */
    public void testZeroSplitUnifiedMappingNarrowsPresentColumnInsteadOfTrippingGuard() throws Exception {
        StoragePath filePath = StoragePath.of("s3://bucket/data/f1.parquet");
        List<StorageEntry> entries = List.of(new StorageEntry(filePath, 100, Instant.EPOCH));
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/*.parquet");

        // Reader is asked for per-file projection [city] only; emit that one block.
        FormatReader formatReader = new SinglePageReader(
            () -> new Page(2, TEST_BLOCK_FACTORY.newIntArrayVector(new int[] { 10, 20 }, 2).asBlock())
        );
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        List<Attribute> attributes = List.of(ref("city", DataType.INTEGER));

        ExternalSchema fileSchema = new ExternalSchema(
            List.of(ref("id", DataType.INTEGER), ref("city", DataType.INTEGER), ref("extra", DataType.INTEGER))
        );
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, 1, 2 }, new DataType[] { DataType.LONG, null, null });
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = Map.of(
            filePath,
            new SchemaReconciliation.FileSchemaInfo(fileSchema, mapping, null)
        );

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            filePath,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).fileList(fileList).schemaMap(schemaMap).build();

        SourceOperator operator = factory.get(driverContext);
        assertNotNull(operator);

        List<Page> pages = new ArrayList<>();
        try {
            while (operator.isFinished() == false) {
                Page page = operator.getOutput();
                if (page != null) {
                    pages.add(page);
                }
            }

            assertEquals("one page produced", 1, pages.size());
            Page page = pages.get(0);
            assertEquals("narrow query projects one column", 1, page.getBlockCount());
            assertEquals(2, page.getPositionCount());
            IntBlock cityBlock = page.getBlock(0);
            assertEquals(10, cityBlock.getInt(0));
            assertEquals(20, cityBlock.getInt(1));
        } finally {
            for (Page p : pages) {
                p.releaseBlocks();
            }
            operator.close();
        }
    }

    /**
     * Collision regression (the KEEP-partition-only shape). The query projects only the partition
     * column, so {@code queryDataSchema} is empty and {@code adaptSchema} short-circuits. The read
     * must still succeed and surface the partition column with its path-derived value.
     */
    public void testKeepPartitionColumnOnlyReadsWithoutGuard() throws Exception {
        StoragePath filePath = StoragePath.of("s3://bucket/data/year=2024/f1.parquet");
        List<StorageEntry> entries = List.of(new StorageEntry(filePath, 100, Instant.EPOCH));
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/**/*.parquet");

        // No data columns projected: the reader emits a position-only page (0 data blocks).
        FormatReader formatReader = new SinglePageReader(() -> new Page(2));
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        List<Attribute> attributes = List.of(ref("year", DataType.INTEGER));

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            filePath,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).fileList(fileList).partitionColumnNames(Set.of("year")).partitionValues(Map.of("year", 2024)).build();

        SourceOperator operator = factory.get(driverContext);
        assertNotNull(operator);

        List<Page> pages = new ArrayList<>();
        try {
            while (operator.isFinished() == false) {
                Page page = operator.getOutput();
                if (page != null) {
                    pages.add(page);
                }
            }

            assertEquals("one page produced", 1, pages.size());
            Page page = pages.get(0);
            assertEquals("only the injected partition column", 1, page.getBlockCount());
            assertEquals(2, page.getPositionCount());
            IntBlock yearBlock = page.getBlock(0);
            assertEquals(2024, yearBlock.getInt(0));
            assertEquals(2024, yearBlock.getInt(1));
        } finally {
            for (Page p : pages) {
                p.releaseBlocks();
            }
            operator.close();
        }
    }

    /**
     * A zero-projection {@code STATS COUNT(*)} over a Hive-partitioned dataset. The query references no
     * columns, so its output attribute list is empty, while the dataset's partition stamp keeps
     * {@code partitionColumnNames} non-empty. Before the fix, {@code wrapWithVirtualColumns} gated only
     * on the dataset axis and constructed a {@link VirtualColumnIterator} with the empty output as
     * {@code fullOutput}, tripping the constructor's {@code "fullOutput cannot be null or empty"} check
     * during {@code openNext*} — the exact partitioned-text {@code COUNT(*)} crash. The guard now also
     * skips the wrap on the output axis, forwarding the reader's position-only pages unchanged so the
     * row count rides {@code positionCount} to the count aggregator (mirroring the already-working
     * unpartitioned path). This is the factory-level behavioral pin for the fix.
     */
    public void testZeroProjectionCountStarOverPartitionedSourceForwardsPositionOnlyPages() throws Exception {
        StoragePath filePath = StoragePath.of("s3://bucket/data/year=2024/f1.parquet");
        List<StorageEntry> entries = List.of(new StorageEntry(filePath, 100, Instant.EPOCH));
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/**/*.parquet");

        // COUNT(*) projects zero columns: the reader emits a position-only page (0 data blocks).
        FormatReader formatReader = new SinglePageReader(() -> new Page(2));
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        // Empty output — the defining feature of a bare STATS COUNT(*) read — over a partitioned stamp.
        List<Attribute> attributes = List.of();

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            filePath,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).fileList(fileList).partitionColumnNames(Set.of("year")).partitionValues(Map.of("year", 2024)).build();

        SourceOperator operator = factory.get(driverContext);
        assertNotNull(operator);

        List<Page> pages = new ArrayList<>();
        try {
            while (operator.isFinished() == false) {
                Page page = operator.getOutput();
                if (page != null) {
                    pages.add(page);
                }
            }

            assertEquals("one page produced", 1, pages.size());
            Page page = pages.get(0);
            assertEquals("zero-projection read carries no data blocks", 0, page.getBlockCount());
            assertEquals("the row count rides positionCount", 2, page.getPositionCount());
        } finally {
            for (Page p : pages) {
                p.releaseBlocks();
            }
            operator.close();
        }
    }

    /**
     * COUNT(*) on a non-leading record-aligned macro-split already carries the coordinator pin on the
     * split. Execution must bind that pin via {@code withSchema} and must not call {@code metadata()}
     * (an unranged GET from byte 0).
     */
    public void testEmptyProjectionNonLeadingSplitWithReadSchemaSkipsMetadata() throws Exception {
        List<Attribute> pin = List.of(ref("col0", DataType.KEYWORD), ref("col1", DataType.INTEGER), ref("col2", DataType.DOUBLE));
        CountingBindAndSplitReader formatReader = runEmptyProjectionNonLeadingMacroSplit(pin);

        assertEquals("pinned empty-projection split must not re-infer via metadata()", 0, formatReader.metadataCalls());
        assertNotNull("withSchema must still receive the pin", formatReader.withSchemaReceived());
        assertEquals(pin.size(), formatReader.withSchemaReceived().size());
        for (int i = 0; i < pin.size(); i++) {
            assertEquals(pin.get(i).name(), formatReader.withSchemaReceived().get(i).name());
            assertEquals(pin.get(i).dataType(), formatReader.withSchemaReceived().get(i).dataType());
        }
        assertEquals("read() still emits a page", 1, formatReader.readCalls());
    }

    /**
     * Unpinned non-leading record-aligned macro-splits keep today's bind: {@code metadata()} then
     * {@code withSchema} from that inference. Mixed-version coordinators ship {@code readSchema == null}.
     */
    public void testEmptyProjectionNonLeadingSplitWithoutReadSchemaStillBinds() throws Exception {
        CountingBindAndSplitReader formatReader = runEmptyProjectionNonLeadingMacroSplit((List<Attribute>) null);

        assertEquals("unpinned empty-projection split still infers via metadata()", 1, formatReader.metadataCalls());
        assertNotNull(formatReader.withSchemaReceived());
        assertEquals(CountingBindAndSplitReader.INFERRED_SCHEMA.size(), formatReader.withSchemaReceived().size());
        for (int i = 0; i < CountingBindAndSplitReader.INFERRED_SCHEMA.size(); i++) {
            assertEquals(CountingBindAndSplitReader.INFERRED_SCHEMA.get(i).name(), formatReader.withSchemaReceived().get(i).name());
            assertEquals(CountingBindAndSplitReader.INFERRED_SCHEMA.get(i).dataType(), formatReader.withSchemaReceived().get(i).dataType());
        }
        assertEquals("read() still emits a page", 1, formatReader.readCalls());
    }

    /**
     * StreamInput can deserialize {@code readSchema = []} even though {@link FileSplit#withReadSchema}
     * collapses empty to null. The empty-projection bind must treat that like null (infer), not
     * {@code withSchema} width 0.
     */
    public void testEmptyProjectionNonLeadingSplitEmptyReadSchemaStillBinds() throws Exception {
        CountingBindAndSplitReader formatReader = runEmptyProjectionNonLeadingMacroSplit(fileSplitWithDeserializedEmptyReadSchema());

        assertEquals("empty List.of() pin must still infer via metadata()", 1, formatReader.metadataCalls());
        assertNotNull(formatReader.withSchemaReceived());
        assertEquals(
            "withSchema must receive inferred schema, not width 0",
            CountingBindAndSplitReader.INFERRED_SCHEMA.size(),
            formatReader.withSchemaReceived().size()
        );
        for (int i = 0; i < CountingBindAndSplitReader.INFERRED_SCHEMA.size(); i++) {
            assertEquals(CountingBindAndSplitReader.INFERRED_SCHEMA.get(i).name(), formatReader.withSchemaReceived().get(i).name());
            assertEquals(CountingBindAndSplitReader.INFERRED_SCHEMA.get(i).dataType(), formatReader.withSchemaReceived().get(i).dataType());
        }
        assertEquals("read() still emits a page", 1, formatReader.readCalls());
    }

    private CountingBindAndSplitReader runEmptyProjectionNonLeadingMacroSplit(List<Attribute> readSchema) throws Exception {
        StoragePath path = StoragePath.of("s3://bucket/data.csv");
        FileSplit split = FileSplit.withReadSchema(
            "file",
            path,
            // isFirstInFile is true when FIRST_SPLIT_KEY is "true" OR offset == 0; a leading split
            // skips this bind entirely. Offset must be non-zero and the first-split key must not
            // be "true" so the empty-projection non-leading gate actually runs.
            1024L,
            2048L,
            ".csv",
            Map.of(FileSplitProvider.RECORD_ALIGNED_MACRO_SPLIT_KEY, "true", FileSplitProvider.FIRST_SPLIT_KEY, "false"),
            Map.of(),
            null,
            readSchema
        );
        return runEmptyProjectionNonLeadingMacroSplit(split);
    }

    /**
     * Compact ctor nulls empty lists. StreamInput keeps {@code []} so mixed-version coordinators
     * can still ship a present-but-empty pin into the operator.
     */
    private static FileSplit fileSplitWithDeserializedEmptyReadSchema() throws IOException {
        StoragePath path = StoragePath.of("s3://bucket/data.csv");
        Map<String, Object> config = Map.of(
            FileSplitProvider.RECORD_ALIGNED_MACRO_SPLIT_KEY,
            "true",
            FileSplitProvider.FIRST_SPLIT_KEY,
            "false"
        );
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeString("file");
        out.writeString(path.toString());
        out.writeVLong(1024L);
        out.writeVLong(2048L);
        out.writeOptionalString(".csv");
        out.writeGenericMap(config);
        out.writeGenericMap(Map.of());
        out.writeBoolean(false);
        out.writeBoolean(false);
        out.writeBoolean(true);
        out.writeVInt(0);
        try (StreamInput in = out.bytes().streamInput()) {
            FileSplit split = new FileSplit(in);
            assertNotNull("deserialized empty list must not collapse to null", split.readSchema());
            assertTrue(split.readSchema().isEmpty());
            return split;
        }
    }

    private CountingBindAndSplitReader runEmptyProjectionNonLeadingMacroSplit(FileSplit split) throws Exception {
        CountingBindAndSplitReader formatReader = new CountingBindAndSplitReader();
        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            new StubMultiFileStorageProvider(),
            formatReader,
            split.path(),
            List.of(),
            100,
            10,
            (Runnable r) -> r.run()
        ).sliceQueue(new ExternalSliceQueue(List.of(split))).build();

        SourceOperator operator = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        try {
            while (operator.isFinished() == false) {
                Page page = operator.getOutput();
                if (page != null) {
                    pages.add(page);
                }
            }
            assertEquals("read() still emits a page", 1, pages.size());
        } finally {
            for (Page p : pages) {
                p.releaseBlocks();
            }
            operator.close();
        }
        return formatReader;
    }

    /**
     * Empty-projection COUNT(*) on a non-leading record-aligned split binds schema from a second
     * object. Folding those bytes must not drop the tracked split, so both reads appear in
     * {@code bytes_read}.
     */
    public void testEmptyProjectionBindBytesAndSplitBytesBothCounted() throws Exception {
        byte[] payload = new byte[200];
        StoragePath path = StoragePath.of("s3://bucket/data/events.ndjson");
        Map<String, Object> config = Map.of(
            FileSplitProvider.RECORD_ALIGNED_MACRO_SPLIT_KEY,
            "true",
            FileSplitProvider.FIRST_SPLIT_KEY,
            "false",
            FileSplitProvider.LAST_SPLIT_KEY,
            "true"
        );
        FileSplit split = FileSplit.withReadSchema("test", path, 100, 100, "ndjson", config, Map.of(), null, null);
        FormatReader formatReader = new DrainingBindAndSplitReader();
        StorageProvider storageProvider = new MeteredPayloadStorageProvider(path, payload);

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            List.of(),
            100,
            10,
            (Runnable r) -> r.run()
        ).sliceQueue(new ExternalSliceQueue(List.of(split))).build();

        SourceOperator operator = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (operator.isFinished() == false) {
                if (System.nanoTime() > deadline) {
                    fail("operator did not finish");
                }
                Page page = operator.getOutput();
                if (page != null) {
                    pages.add(page);
                }
            }
            assertThat(operator.status().bytesRead(), Matchers.equalTo(300L));
        } finally {
            for (Page page : pages) {
                page.releaseBlocks();
            }
            operator.close();
        }
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
        assertNull("the read failure must not be chained to prevent caused_by leaks", readFailure.getCause());
        assertTrue(readFailure.getMessage().contains("Simulated read error"));

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
        when(formatReader.rowPositionStrategy()).thenReturn(PassThroughRowPositionStrategy.INSTANCE);
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

    /**
     * Each slice-queue page takes {@code _file.path} from that split. The factory path is the glob, so a
     * page that showed it would mean the overlay used the source path instead of {@code fileSplit.path()}.
     */
    public void testSliceQueueFilePathComesFromEachSplit() throws Exception {
        StoragePath factoryPath = StoragePath.of("s3://bucket/*.parquet");
        StoragePath first = StoragePath.of("s3://bucket/f1.parquet");
        StoragePath second = StoragePath.of("s3://bucket/f2.parquet");
        List<FileSplit> splits = List.of(
            new FileSplit("test", first, 0, 100, "parquet", Map.of(), Map.of()),
            new FileSplit("test", second, 0, 200, "parquet", Map.of(), Map.of())
        );
        FormatReader formatReader = new SinglePageReader(() -> new Page(1));
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();
        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            factoryPath,
            List.of(new ExternalMetadataAttribute(Source.EMPTY, FileMetadataColumns.PATH, DataType.KEYWORD)),
            100,
            10,
            (Runnable r) -> r.run()
        ).sliceQueue(new ExternalSliceQueue(new ArrayList<>(splits))).build();

        SourceOperator operator = factory.get(driverContext);
        List<String> paths = new ArrayList<>();
        List<Page> pages = new ArrayList<>();
        BytesRef scratch = new BytesRef();
        try {
            while (operator.isFinished() == false) {
                Page page = operator.getOutput();
                if (page == null) {
                    continue;
                }
                pages.add(page);
                BytesRefBlock pathBlock = page.getBlock(0);
                paths.add(pathBlock.getBytesRef(0, scratch).utf8ToString());
            }
        } finally {
            for (Page page : pages) {
                page.releaseBlocks();
            }
            operator.close();
        }
        assertEquals(List.of(first.toString(), second.toString()), paths);
    }

    // ===== Slice Queue tests =====

    /**
     * A metadata-only projection leaves {@code queryDataSchema} empty, the same shape as
     * {@code COUNT(*)}. The slice-queue path must not hand {@code mapFilters} a unified-width
     * mapping in that case: the mapping indexes the query schema by slot, and an empty schema
     * with a missing-column mapping would throw.
     */
    public void testSliceQueueEmptyQuerySchemaDoesNotMapFiltersAgainstUnifiedMapping() throws Exception {
        ColumnMapping unifiedWidth = new ColumnMapping(new int[] { 0, -1 }, null);
        List<FileSplit> splits = List.of(
            new FileSplit("test", StoragePath.of("s3://bucket/f1.parquet"), 0, 100, "parquet", Map.of(), Map.of(), unifiedWidth),
            new FileSplit("test", StoragePath.of("s3://bucket/f2.parquet"), 0, 200, "parquet", Map.of(), Map.of(), unifiedWidth)
        );
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(new ArrayList<>(splits));

        FormatReader formatReader = new SinglePageReader(() -> new Page(2));
        StubMultiFileStorageProvider storageProvider = new StubMultiFileStorageProvider();

        Attribute value = new FieldAttribute(
            Source.EMPTY,
            "value",
            new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
        Expression pushed = new Equals(Source.EMPTY, value, new Literal(Source.EMPTY, 1, DataType.INTEGER));

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            StoragePath.of("s3://bucket/f1.parquet"),
            List.of(new ExternalMetadataAttribute(Source.EMPTY, "_index", DataType.KEYWORD)),
            100,
            10,
            (Runnable r) -> r.run()
        )
            .sliceQueue(sliceQueue)
            .pushedExpressions(List.of(pushed))
            .pushdownSupport(filters -> FilterPushdownSupport.PushdownResult.all("opaque"))
            .build();

        SourceOperator operator = factory.get(driverContext);
        assertNotNull(operator);

        List<Page> pages = new ArrayList<>();
        try {
            while (operator.isFinished() == false) {
                Page page = operator.getOutput();
                if (page != null) {
                    pages.add(page);
                }
            }
            assertThat(pages, Matchers.not(Matchers.empty()));
        } finally {
            for (Page p : pages) {
                p.releaseBlocks();
            }
            operator.close();
        }
    }

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

    /**
     * One shared factory, two drivers: the first producer's read fails inside {@code get()}
     * (sync executor). The storage lease must still be held so the second {@code get()} can
     * open its split. {@link #testSliceQueueMultipleDriversClaimDifferentSplits} builds a new
     * factory per driver and cannot catch this.
     */
    public void testOnCloseOutlivesAFastFailingFirstOperatorUntilTheNextIsCreated() {
        List<ExternalSplit> splits = List.of(
            new FileSplit("test", StoragePath.of("s3://bucket/f0.parquet"), 0, 100, "parquet", Map.of(), Map.of()),
            new FileSplit("test", StoragePath.of("s3://bucket/f1.parquet"), 0, 100, "parquet", Map.of(), Map.of())
        );
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(new ArrayList<>(splits));

        AtomicInteger onCloseCalls = new AtomicInteger();
        StorageProvider storageProvider = new StubMultiFileStorageProvider() {
            private void checkLease() {
                if (onCloseCalls.get() > 0) {
                    throw new IllegalStateException("storage lease already returned");
                }
            }

            @Override
            public StorageObject newObject(StoragePath path) {
                checkLease();
                return super.newObject(path);
            }

            @Override
            public StorageObject newObject(StoragePath path, long length) {
                checkLease();
                return super.newObject(path, length);
            }

            @Override
            public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
                checkLease();
                return super.newObject(path, length, lastModified);
            }
        };

        FormatReader formatReader = new FailOnFirstReadFormatReader();
        StoragePath path = StoragePath.of("s3://bucket/f0.parquet");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).sliceQueue(sliceQueue).onClose(() -> onCloseCalls.incrementAndGet()).build();

        DriverContext firstContext = mock(DriverContext.class);
        when(firstContext.blockFactory()).thenReturn(mock(BlockFactory.class));
        doAnswer(inv -> null).when(firstContext).addAsyncAction();
        doAnswer(inv -> null).when(firstContext).removeAsyncAction();

        DriverContext secondContext = mock(DriverContext.class);
        when(secondContext.blockFactory()).thenReturn(mock(BlockFactory.class));
        doAnswer(inv -> null).when(secondContext).addAsyncAction();
        doAnswer(inv -> null).when(secondContext).removeAsyncAction();

        SourceOperator first = factory.get(firstContext);
        assertEquals("first producer fail must not return the lease before later get()", 0, onCloseCalls.get());
        SourceOperator second = factory.get(secondContext);
        List<Page> pages = new ArrayList<>();
        try {
            assertEquals(0, onCloseCalls.get());

            RuntimeException firstFailure = expectThrows(RuntimeException.class, first::getOutput);
            assertNull("the read failure must not be chained to prevent caused_by leaks", firstFailure.getCause());
            assertTrue(firstFailure.getMessage().contains("injected first-read failure"));

            while (second.isFinished() == false) {
                Page page = second.getOutput();
                if (page != null) {
                    pages.add(page);
                }
            }
            assertEquals(1, pages.size());

            first.close();
            assertEquals(0, onCloseCalls.get());
            second.close();
            assertEquals(1, onCloseCalls.get());
        } finally {
            for (Page p : pages) {
                p.releaseBlocks();
            }
            first.close();
            second.close();
        }
    }

    /**
     * Dual-ref catch path: {@code newObject} throws before a producer starts. Both holds must
     * drop so {@code onClose} still runs once instead of pinning the storage lease forever.
     */
    public void testGetThrowBeforeReturnStillRunsOnCloseOnce() {
        StorageProvider storageProvider = mock(StorageProvider.class);
        when(storageProvider.newObject(any())).thenThrow(new IllegalStateException("open failed"));

        FormatReader formatReader = new PageCountingFormatReader(new AtomicInteger());
        StoragePath path = StoragePath.of("s3://bucket/f.parquet");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(mock(BlockFactory.class));
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AtomicInteger onCloseCalls = new AtomicInteger();
        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).onClose(() -> onCloseCalls.incrementAndGet()).build();

        expectThrows(IllegalStateException.class, () -> factory.get(driverContext));
        assertEquals(1, onCloseCalls.get());
    }

    public void testSliceQueueAccessor() {
        List<ExternalSplit> splits = List.of(
            new FileSplit("test", StoragePath.of("s3://bucket/a.parquet"), 0, 10, "parquet", Map.of(), Map.of())
        );
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);

        StorageProvider storageProvider = mock(StorageProvider.class);
        FormatReader formatReader = mock(FormatReader.class);
        when(formatReader.rowPositionStrategy()).thenReturn(PassThroughRowPositionStrategy.INSTANCE);
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

    /**
     * A whole-file split reaches the reader marked as its file's last, so the reader keeps a final record with
     * no trailing terminator. Splits built before the position keys existed carry no markers at all, and this
     * is the path that recognises them — on the factory that production actually uses.
     */
    public void testLegacyUnstampedWholeFileSplitReachesTheReaderAsFileFinal() throws Exception {
        FileSplit split = new FileSplit(
            "test",
            StoragePath.of("s3://bucket/whole.ndjson"),
            0,
            1024,
            "ndjson",
            Map.of(), // no position keys — as produced before they were stamped
            Map.of()
        );

        List<StorageObject> capturedObjects = new ArrayList<>();
        List<Boolean> capturedSkipFirstLine = new ArrayList<>();
        SplitCapturingFormatReader formatReader = new SplitCapturingFormatReader(capturedObjects, capturedSkipFirstLine);

        DriverContext driverContext = mock(DriverContext.class);
        BlockFactory blockFactory = mock(BlockFactory.class);
        when(driverContext.blockFactory()).thenReturn(blockFactory);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            new StubMultiFileStorageProvider(),
            formatReader,
            StoragePath.of("s3://bucket/whole.ndjson"),
            List.of(
                new FieldAttribute(
                    Source.EMPTY,
                    "value",
                    new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
                )
            ),
            100,
            10,
            (Runnable r) -> r.run()
        ).sliceQueue(new ExternalSliceQueue(List.of(split))).build();

        SourceOperator operator = factory.get(driverContext);
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                page.releaseBlocks();
            }
        }

        assertEquals(1, formatReader.capturedLastSplit().size());
        assertTrue(
            "a split covering the whole file owns its trailing bytes, so the reader must be told it is the file's last",
            formatReader.capturedLastSplit().get(0)
        );
        // Same fact, second consumer: it closes the file's trailing stats stripe. Derived from one place so the
        // two cannot disagree — they used to, and a mid-file stripe was closed as if it were the file's last.
        assertTrue("a whole-file read closes the file's final stats stripe", formatReader.capturedStatsFileFinal().get(0));
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

    public void testObservedLimiterStopsParallelParseMidWindow() throws Exception {
        CountDownLatch entered = new CountDownLatch(AsyncExternalSourceOperatorFactory.FILTERED_LIMIT_SEGMENT_WINDOW);
        CountDownLatch proceed = new CountDownLatch(1);
        LatchedSmallSegmentReader formatReader = new LatchedSmallSegmentReader(entered, proceed);
        LargeStorageProvider storageProvider = new LargeStorageProvider(3 * 1024 * 1024);

        StoragePath path = StoragePath.of("file:///data/large.csv");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
                storageProvider,
                formatReader,
                path,
                attributes,
                100,
                10,
                pool
            ).parsingParallelism(8).maxConcurrentOpenSegments(8).build();
            Limiter observed = new Limiter(1);
            factory.setObservedLimiter(observed);

            DriverContext driverContext = mockLimitBudgetDriverContext();
            SourceOperator operator = factory.get(driverContext);
            assertTrue(entered.await(30, TimeUnit.SECONDS));
            observed.tryAccumulateHits(1);
            proceed.countDown();
            drainRemaining(operator);
            operator.close();

            assertThat(formatReader.readCount.get(), lessThanOrEqualTo(AsyncExternalSourceOperatorFactory.FILTERED_LIMIT_SEGMENT_WINDOW));
            assertThat(formatReader.readCount.get(), greaterThan(0));
            assertThat("parallel parsing still used under observed LIMIT", formatReader.readWithFirstSplitFalseCount.get(), greaterThan(0));
        } finally {
            proceed.countDown();
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    public void testRowLimitCompletionDoesNotWaitOnDrainLatch() throws Exception {
        assertCompletionDoesNotWaitOnDrainLatch(true);
    }

    public void testExternalFinishCompletionDoesNotWaitOnDrainLatch() throws Exception {
        assertCompletionDoesNotWaitOnDrainLatch(false);
    }

    /**
     * {@code drainCurrentUnit} DONE closes the page iterator then fires the completion listener.
     * Uncompressed CSV/NDJSON LIMIT (and external {@code finish()}) must abort leftover GETs so
     * that close does not block on a drain latch. The slice-queue producer is the path that
     * {@code drainCurrentUnit} actually runs; the whole-file {@code drainPagesAsync} rail reads
     * until the buffer fills (or EOF) and would consume a small object before {@code finish()}.
     */
    private void assertCompletionDoesNotWaitOnDrainLatch(boolean rowLimit) throws Exception {
        StringBuilder csv = new StringBuilder("id:long,name:keyword\n");
        // Wide rows so decoded pages exceed the 256 KiB buffer (maxBufferSize=1) before EOF.
        // External finish() must run while the GET is still open; a short file is fully consumed
        // on the producer thread before the test thread can call finish().
        String pad = "n".repeat(256);
        for (int i = 0; i < 8_000; i++) {
            csv.append(i).append(',').append(pad).append('\n');
        }
        byte[] payload = csv.toString().getBytes(StandardCharsets.UTF_8);
        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        CountDownLatch drainLatch = new CountDownLatch(1);
        tracking.drainLatch = drainLatch;

        ConcurrencyLimiter limiter = new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(4, false));
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(4, 60_000L, null);
        int startPermits = limiter.availablePermits();
        StoragePath path = StoragePath.of("s3://bucket/data.csv");
        StorageProvider storageProvider = new QueryBudgetedStorageProvider(
            new ConcurrencyLimitedStorageProvider(new DrainFixtureStorageProvider(payload, tracking, path), limiter),
            budget
        );
        FileSplit split = new FileSplit("test", path, 0, payload.length, "csv", Map.of(), Map.of());
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(List.of(split));

        List<Attribute> attributes = List.of(
            new FieldAttribute(Source.EMPTY, "id", new EsField("id", DataType.LONG, Map.of(), false, EsField.TimeSeriesFieldType.NONE)),
            new FieldAttribute(
                Source.EMPTY,
                "name",
                new EsField("name", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );
        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        ExecutorService exec = Executors.newSingleThreadExecutor();
        SourceOperator operator = null;
        try {
            AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
                storageProvider,
                new CsvFormatReader(TEST_BLOCK_FACTORY),
                path,
                attributes,
                100,
                1,
                exec
            ).sliceQueue(sliceQueue).parsingParallelism(1).rowLimit(rowLimit ? 5 : FormatReader.NO_LIMIT).build();
            operator = factory.get(driverContext);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            int pages = 0;
            long rows = 0;
            while (tracking.aborted.get() == false || limiter.availablePermits() != startPermits || budget.inFlight() != 0) {
                if (System.nanoTime() > deadline) {
                    fail(
                        "completion blocked on drain latch; rowLimit="
                            + rowLimit
                            + " consumed="
                            + tracking.bytesConsumed.get()
                            + "/"
                            + payload.length
                            + " closed="
                            + tracking.closed.get()
                            + " finished="
                            + operator.isFinished()
                    );
                }
                Page page = operator.getOutput();
                if (page != null) {
                    pages++;
                    rows += page.getPositionCount();
                    page.releaseBlocks();
                    if (rowLimit == false && pages >= 1) {
                        operator.finish();
                    }
                }
            }
            assertEquals("abort-on-close must not wait on the drain latch", 1, drainLatch.getCount());
            assertEquals(startPermits, limiter.availablePermits());
            assertEquals(0, budget.inFlight());
            // Under LIMIT the producer buffers its first page and aborts the GET in the same task, so the loop
            // above can exit before this thread polls that page. Drain the rest before counting what was delivered.
            while (operator.isFinished() == false) {
                if (System.nanoTime() > deadline) {
                    fail("operator did not finish after abort; rowLimit=" + rowLimit + " pages=" + pages);
                }
                Page page = operator.getOutput();
                if (page != null) {
                    pages++;
                    rows += page.getPositionCount();
                    page.releaseBlocks();
                }
            }
            assertThat(pages, Matchers.greaterThan(0));
            if (rowLimit) {
                assertThat(rows, Matchers.greaterThanOrEqualTo(5L));
            }
        } finally {
            drainLatch.countDown();
            if (operator != null) {
                operator.close();
            }
            exec.shutdownNow();
            assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /**
     * Truncation leaves a GB-scale tail split. Two drivers claim the leading split and the tail;
     * LIMIT then {@code close()}s the tail. 2174 abort-on-close must discard leftover &gt;64 KiB
     * rather than drain it. Splits are stamped like {@code buildNewlineMacroSplits} so the tail is
     * a non-first record-aligned CSV split. Each driver still returns exactly its {@code rowLimit}
     * rows (page size matches the limit).
     */
    public void testMultiDriverLimitAbortsTheUnreadTail() throws Exception {
        String header = "id:long,name:keyword\n";
        String pad = "n".repeat(256);
        StringBuilder head = new StringBuilder(header);
        for (int i = 0; i < 20; i++) {
            head.append(i).append(',').append(pad).append('\n');
        }
        StringBuilder tail = new StringBuilder();
        for (int i = 20; i < 8_000; i++) {
            tail.append(i).append(',').append(pad).append('\n');
        }
        byte[] headBytes = head.toString().getBytes(StandardCharsets.UTF_8);
        byte[] tailBytes = tail.toString().getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[headBytes.length + tailBytes.length];
        System.arraycopy(headBytes, 0, payload, 0, headBytes.length);
        System.arraycopy(tailBytes, 0, payload, headBytes.length, tailBytes.length);
        long headLen = headBytes.length;

        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(4, false));
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(4, 60_000L, null);
        int startPermits = limiter.availablePermits();
        StoragePath path = StoragePath.of("s3://bucket/data.csv");
        StorageProvider storageProvider = new QueryBudgetedStorageProvider(
            new ConcurrencyLimitedStorageProvider(new DrainFixtureStorageProvider(payload, tracking, path), limiter),
            budget
        );
        List<Attribute> attributes = List.of(
            new FieldAttribute(Source.EMPTY, "id", new EsField("id", DataType.LONG, Map.of(), false, EsField.TimeSeriesFieldType.NONE)),
            new FieldAttribute(
                Source.EMPTY,
                "name",
                new EsField("name", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );
        Map<String, Object> firstCfg = new HashMap<>();
        firstCfg.put(FileSplitProvider.RECORD_ALIGNED_MACRO_SPLIT_KEY, "true");
        firstCfg.put(FileSplitProvider.FILE_LENGTH_KEY, Long.toString(payload.length));
        firstCfg.put(FileSplitProvider.FIRST_SPLIT_KEY, "true");
        Map<String, Object> lastCfg = new HashMap<>();
        lastCfg.put(FileSplitProvider.RECORD_ALIGNED_MACRO_SPLIT_KEY, "true");
        lastCfg.put(FileSplitProvider.FILE_LENGTH_KEY, Long.toString(payload.length));
        lastCfg.put(FileSplitProvider.LAST_SPLIT_KEY, "true");
        List<ExternalSplit> splits = List.of(
            FileSplit.withReadSchema("test", path, 0, headLen, "csv", firstCfg, Map.of(), null, attributes),
            FileSplit.withReadSchema("test", path, headLen, payload.length - headLen, "csv", lastCfg, Map.of(), null, attributes)
        );
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);

        int rowLimit = 5;
        ExecutorService exec = Executors.newFixedThreadPool(2);
        List<SourceOperator> operators = new ArrayList<>();
        try {
            for (int d = 0; d < 2; d++) {
                DriverContext driverContext = mock(DriverContext.class);
                when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
                doAnswer(inv -> null).when(driverContext).addAsyncAction();
                doAnswer(inv -> null).when(driverContext).removeAsyncAction();
                AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
                    storageProvider,
                    new CsvFormatReader(TEST_BLOCK_FACTORY),
                    path,
                    attributes,
                    rowLimit,
                    1,
                    exec
                ).sliceQueue(sliceQueue).parsingParallelism(1).rowLimit(rowLimit).build();
                operators.add(factory.get(driverContext));
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            int[] rows = new int[2];
            boolean done = false;
            while (done == false) {
                if (System.nanoTime() > deadline) {
                    fail(
                        "multi-driver LIMIT did not finish; consumed="
                            + tracking.bytesConsumed.get()
                            + "/"
                            + payload.length
                            + " aborted="
                            + tracking.aborted.get()
                    );
                }
                done = true;
                for (int i = 0; i < operators.size(); i++) {
                    SourceOperator op = operators.get(i);
                    if (op.isFinished() == false) {
                        done = false;
                        Page page = op.getOutput();
                        if (page != null) {
                            rows[i] += page.getPositionCount();
                            page.releaseBlocks();
                        }
                    }
                }
            }
            assertEquals("each driver returns its LIMIT rows", rowLimit, rows[0]);
            assertEquals("each driver returns its LIMIT rows", rowLimit, rows[1]);
            assertTrue("the unread tail leftover must abort rather than drain", tracking.aborted.get());
            assertThat(
                "drain must not consume the unread tail",
                tracking.bytesConsumed.get(),
                Matchers.lessThan((long) payload.length / 2)
            );
            assertThat(
                payload.length - tracking.bytesConsumed.get(),
                Matchers.greaterThan((long) DecompressingStorageObject.MAX_TRAILING_DRAIN_BYTES)
            );
            assertEquals(startPermits, limiter.availablePermits());
            assertEquals(0, budget.inFlight());
        } finally {
            for (SourceOperator op : operators) {
                op.close();
            }
            exec.shutdownNow();
            assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));
        }
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

    /**
     * A quoting-on reader hands out a non-strided splitter, so with parsing parallelism the factory must
     * dispatch to the sequential (whole-file, quote-aware) branch. This asserts the {@code describe()} label
     * the end-to-end IT relies on but cannot itself observe (the profile shows the clean operator name,
     * not the parse mode).
     */
    public void testDescribeShowsQuotedSequentialParseMode() {
        SegmentableFormatReader formatReader = new NonStridedSegmentableFormatReader();
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
        assertTrue(
            "describe should mention quoted-sequential-parse for non-strided readers: " + description,
            description.contains("quoted-sequential-parse(4)")
        );
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

    /**
     * A producer parked on {@link CloseableIterator#waitForReady()} must release its
     * {@code producerExecutor} thread so other work can run.
     */
    public void testParkedProducerReleasesExecutorThread() throws Exception {
        CountDownLatch allowPage = new CountDownLatch(1);
        ParkingReader reader = new ParkingReader(allowPage);
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(
            List.of(new FileSplit("test", StoragePath.of("s3://bucket/park.parquet"), 0, 100, "parquet", Map.of(), Map.of()))
        );
        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        ExecutorService ioExec = Executors.newSingleThreadExecutor(EsExecutors.daemonThreadFactory("test", "park-io"));
        ExecutorService producerExec = Executors.newSingleThreadExecutor(EsExecutors.daemonThreadFactory("test", "park-producer"));
        try {
            AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
                new StubMultiFileStorageProvider(),
                reader,
                StoragePath.of("s3://bucket/park.parquet"),
                List.of(
                    new FieldAttribute(
                        Source.EMPTY,
                        "value",
                        new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
                    )
                ),
                100,
                10,
                ioExec
            ).sliceQueue(sliceQueue).producerExecutor(producerExec).build();

            SourceOperator operator = factory.get(driverContext);
            assertBusy(() -> assertTrue("producer must park on waitForReady", reader.parked.get()), 5, TimeUnit.SECONDS);

            CountDownLatch otherWork = new CountDownLatch(1);
            producerExec.execute(otherWork::countDown);
            assertTrue("parked producer must not pin the consumer thread", otherWork.await(5, TimeUnit.SECONDS));

            allowPage.countDown();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (operator.isFinished() == false && System.nanoTime() < deadline) {
                Page p = operator.getOutput();
                if (p != null) {
                    p.releaseBlocks();
                }
            }
            assertTrue(operator.isFinished());
            operator.close();
        } finally {
            allowPage.countDown();
            ioExec.shutdownNow();
            producerExec.shutdownNow();
            assertTrue(ioExec.awaitTermination(5, TimeUnit.SECONDS));
            assertTrue(producerExec.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /**
     * T8: park resume only. Saturate a 1-thread {@link EsThreadPoolExecutor} with queue capacity 0
     * after the producer has parked; the force-execution resume still runs. Does not claim
     * start-of-producer liveness — the initial submit is not force-execution.
     */
    public void testForcedResubmitRunsWhenProducerQueueIsFull() throws Exception {
        CountDownLatch allowPage = new CountDownLatch(1);
        ParkingReader reader = new ParkingReader(allowPage);
        EsThreadPoolExecutor producerExec = EsExecutors.newFixed(
            "test-t8",
            1,
            0,
            EsExecutors.daemonThreadFactory("test", "t8"),
            new ThreadContext(Settings.EMPTY),
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(
            List.of(new FileSplit("test", StoragePath.of("s3://bucket/force.parquet"), 0, 100, "parquet", Map.of(), Map.of()))
        );
        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();
        ExecutorService ioExec = Executors.newSingleThreadExecutor(EsExecutors.daemonThreadFactory("test", "force-io"));
        CountDownLatch occupied = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        try {
            AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
                new StubMultiFileStorageProvider(),
                reader,
                StoragePath.of("s3://bucket/force.parquet"),
                List.of(
                    new FieldAttribute(
                        Source.EMPTY,
                        "value",
                        new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
                    )
                ),
                100,
                10,
                ioExec
            ).sliceQueue(sliceQueue).producerExecutor(producerExec).build();

            SourceOperator operator = factory.get(driverContext);
            assertBusy(() -> assertTrue(reader.parked.get()), 5, TimeUnit.SECONDS);

            producerExec.execute(new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                    occupied.countDown();
                    if (releaseBlocker.await(15, TimeUnit.SECONDS) == false) {
                        throw new AssertionError("blocker not released");
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    occupied.countDown();
                }
            });
            assertTrue("producer thread must be occupied after park", occupied.await(5, TimeUnit.SECONDS));

            allowPage.countDown();
            assertFalse("force resume must wait behind the occupied thread", operator.isFinished());

            releaseBlocker.countDown();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (operator.isFinished() == false && System.nanoTime() < deadline) {
                Page p = operator.getOutput();
                if (p != null) {
                    p.releaseBlocks();
                }
            }
            assertTrue(operator.isFinished());
            operator.close();
        } finally {
            allowPage.countDown();
            releaseBlocker.countDown();
            ioExec.shutdownNow();
            producerExec.shutdownNow();
            assertTrue(ioExec.awaitTermination(5, TimeUnit.SECONDS));
            assertTrue(producerExec.awaitTermination(5, TimeUnit.SECONDS));
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
        assertNull(
            factory.openWithParallelism(
                cdr,
                bytesStorageObject(payload),
                List.of("a"),
                ErrorPolicy.STRICT,
                false,
                true,
                true,
                null,
                0L,
                null,
                null,
                null,
                ExternalReadCounters.NOOP,
                null
            )
        );
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
                ErrorPolicy.STRICT,
                false,
                true,
                true,
                null,
                0L,
                null,
                null,
                null,
                ExternalReadCounters.NOOP,
                null
            );
            assertNotNull(iterator);
            iterator.close();
        } finally {
            exec.shutdownNow();
        }
    }

    /**
     * Parallel gzip rail identity: coordinator {@code closeStream} must abort the Abortable raw
     * GET. Passing the inner S3-shaped object with a {@code DecompressedStream} falls through
     * {@code instanceof Abortable} and drains. Tests must not use {@link DrainSimulatingStorageObject}
     * as the coordinator storage object — that fixture ignores Abortable identity and false-passes.
     */
    public void testOpenWithParallelismGzipEarlyCloseAbortsAbortableRawStream() throws Exception {
        ExecutorService exec = Executors.newFixedThreadPool(8);
        try {
            AsyncExternalSourceOperatorFactory factory = factoryForOpenParallelismStreamingTests(
                dummyFormatReaderForOpenParallelismTests(),
                exec
            );
            List<Attribute> schema = List.of(new ReferenceAttribute(Source.EMPTY, "a", DataType.INTEGER));
            CompressionDelegatingFormatReader cdr = new CompressionDelegatingFormatReader(
                new NdJsonFormatReader(Settings.EMPTY, TEST_BLOCK_FACTORY, schema),
                new GzipDecompressionCodec()
            );
            byte[] gzipped = gzipCompress("{\"a\":1}\n".repeat(2_000).getBytes(StandardCharsets.UTF_8));

            S3ShapedAbortableStorageObject object = new S3ShapedAbortableStorageObject(gzipped);
            CloseableIterator<Page> iterator = factory.openWithParallelism(
                cdr,
                object,
                List.of("a"),
                ErrorPolicy.STRICT,
                false,
                true,
                true,
                null,
                0L,
                null,
                null,
                null,
                ExternalReadCounters.NOOP,
                null
            );
            assertNotNull(iterator);
            try {
                assertTrue(iterator.hasNext());
                Page page = iterator.next();
                try {
                    assertThat(page.getPositionCount(), Matchers.greaterThan(0));
                } finally {
                    page.releaseBlocks();
                }
            } finally {
                iterator.close();
            }

            assertFalse("abortStream must receive the Abortable raw GET, not a decompressed wrapper", object.sawNonAbortable.get());
            assertTrue("abortStream must hit Abortable.abort() on the raw GET", object.sawAbortable.get());
            assertTrue(object.abortCalled.get());
        } finally {
            exec.shutdownNow();
        }
    }

    /**
     * Parallel gzip rail, full read: the JDK gzip decoder reports end-of-stream without reading the raw body to
     * {@code -1}, so the coordinator's {@code closeStream} abort used to arrive while HttpClient still held the
     * connection and discard it. The raw body must reach end-of-body before the abort so S3 pools the connection.
     */
    public void testOpenWithParallelismGzipFullReadReachesEndOfBodyBeforeAbort() throws Exception {
        ExecutorService exec = Executors.newFixedThreadPool(8);
        try {
            AsyncExternalSourceOperatorFactory factory = factoryForOpenParallelismStreamingTests(
                dummyFormatReaderForOpenParallelismTests(),
                exec
            );
            List<Attribute> schema = List.of(new ReferenceAttribute(Source.EMPTY, "a", DataType.INTEGER));
            CompressionDelegatingFormatReader cdr = new CompressionDelegatingFormatReader(
                new NdJsonFormatReader(Settings.EMPTY, TEST_BLOCK_FACTORY, schema),
                new GzipDecompressionCodec()
            );
            int rows = between(1, 50_000);
            StringBuilder ndjson = new StringBuilder();
            for (int i = 0; i < rows; i++) {
                ndjson.append("{\"a\":").append(i).append("}\n");
            }
            byte[] gzipped = gzipCompress(ndjson.toString().getBytes(StandardCharsets.UTF_8));

            S3ShapedAbortableStorageObject object = new S3ShapedAbortableStorageObject(gzipped);
            CloseableIterator<Page> iterator = factory.openWithParallelism(
                cdr,
                object,
                List.of("a"),
                ErrorPolicy.STRICT,
                false,
                true,
                true,
                null,
                0L,
                null,
                null,
                null,
                ExternalReadCounters.NOOP,
                null
            );
            assertNotNull(iterator);
            long seen = 0;
            try {
                while (iterator.hasNext()) {
                    Page page = iterator.next();
                    try {
                        seen += page.getPositionCount();
                    } finally {
                        page.releaseBlocks();
                    }
                }
            } finally {
                iterator.close();
            }

            assertEquals(rows, seen);
            assertTrue("abortStream must hit Abortable.abort() on the raw GET", object.sawAbortable.get());
            assertTrue(
                "a fully read gzip body must reach end-of-body before the abort, or the connection is discarded",
                object.endOfBodyReadBeforeAbort.get()
            );
            assertEquals("the full body is read exactly once", gzipped.length, object.bytesConsumed.get());
        } finally {
            exec.shutdownNow();
        }
    }

    /**
     * Regression guard: if stream-only decompression fails after opening the raw object stream,
     * cleanup must abort (not drain) the underlying connection. Open now happens inside the
     * admitted segmentator, so the failure surfaces on first {@code hasNext()}, not from
     * {@code openWithParallelism} itself.
     */
    public void testOpenWithParallelismGzipDecompressFailureAbortsRawStream() throws IOException {
        AsyncExternalSourceOperatorFactory factory = factoryForOpenParallelismStreamingTests(
            dummyFormatReaderForOpenParallelismTests(),
            Runnable::run
        );
        SegmentableFormatReader inner = mockInnerForParallelDescribeAndOpen();
        CompressionDelegatingFormatReader cdr = new CompressionDelegatingFormatReader(inner, new FailingStreamOnlyCodec());

        byte[] plain = "{\"a\":1}\n".repeat(100).getBytes(StandardCharsets.UTF_8);
        byte[] gzipped = gzipCompress(plain);

        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject object = DrainSimulatingStorageObject.create(gzipped, tracking);

        CloseableIterator<Page> iterator = factory.openWithParallelism(
            cdr,
            object,
            List.of("a"),
            ErrorPolicy.STRICT,
            false,
            true,
            true,
            null,
            0L,
            null,
            null,
            null,
            ExternalReadCounters.NOOP,
            null
        );
        assertNotNull(iterator);
        IOException thrown = expectThrows(IOException.class, iterator::hasNext);
        assertEquals("decompress failed", thrown.getMessage());
        iterator.close();
        assertTrue("raw stream must be aborted when decompression fails", tracking.aborted.get());
        assertEquals("abortStream must be invoked exactly once", 1, tracking.abortCalls.get());
    }

    /**
     * If {@code parallelRead} construction fails before the opener runs (e.g. {@code minimumSegmentSize}
     * throws), the raw stream must not be opened or aborted — there is no GET yet.
     */
    public void testOpenWithParallelismDecompressorReleasedOnParallelReadFailure() throws IOException {
        AsyncExternalSourceOperatorFactory factory = factoryForOpenParallelismStreamingTests(
            dummyFormatReaderForOpenParallelismTests(),
            Runnable::run
        );
        SegmentableFormatReader inner = mockInnerForParallelDescribeAndOpen();
        when(inner.minimumSegmentSize()).thenThrow(new RuntimeException("simulated parallelRead construction failure"));

        AtomicBoolean wrapperClosed = new AtomicBoolean(false);
        DecompressionCodec trackingPassThroughCodec = new DecompressionCodec() {
            @Override
            public String name() {
                return "test-pass-through";
            }

            @Override
            public List<String> extensions() {
                return List.of(".gz");
            }

            @Override
            public InputStream decompress(InputStream raw) {
                return new InputStream() {
                    @Override
                    public int read() throws IOException {
                        return raw.read();
                    }

                    @Override
                    public int read(byte[] buf, int off, int len) throws IOException {
                        return raw.read(buf, off, len);
                    }

                    @Override
                    public void close() throws IOException {
                        wrapperClosed.set(true);
                        raw.close();
                    }
                };
            }
        };
        CompressionDelegatingFormatReader cdr = new CompressionDelegatingFormatReader(inner, trackingPassThroughCodec);

        byte[] payload = "{\"a\":1}\n".repeat(100).getBytes(StandardCharsets.UTF_8);
        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject object = DrainSimulatingStorageObject.create(payload, tracking);

        RuntimeException thrown = expectThrows(
            RuntimeException.class,
            () -> factory.openWithParallelism(
                cdr,
                object,
                List.of("a"),
                ErrorPolicy.STRICT,
                false,
                true,
                true,
                null,
                0L,
                null,
                null,
                null,
                ExternalReadCounters.NOOP,
                null
            )
        );
        assertEquals("simulated parallelRead construction failure", thrown.getMessage());
        assertFalse("opener must not run when parallelRead fails before admission", wrapperClosed.get());
        assertFalse("raw stream must not be aborted if never opened", tracking.aborted.get());
        assertEquals(0, tracking.abortCalls.get());
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
                ErrorPolicy.STRICT,
                false,
                true,
                true,
                null,
                0L,
                null,
                null,
                null,
                ExternalReadCounters.NOOP,
                null
            );
            assertNotNull(iterator);
            iterator.close();
        } finally {
            exec.shutdownNow();
        }
    }

    /**
     * Both streaming rails must not GET on the thread that invokes {@code openWithParallelism}:
     * {@code newStream()} is the admitted segmentator's first action.
     */
    public void testOpenWithParallelismStreamingBranchesDoNotCallNewStreamOnOpenThread() throws Exception {
        assertNewStreamNotCalledOnOpenThread(
            new CompressionDelegatingFormatReader(mockInnerForParallelDescribeAndOpen(), new GzipDecompressionCodec()),
            gzipCompress("{\"a\":1}\n".repeat(20).getBytes(StandardCharsets.UTF_8))
        );
        assertNewStreamNotCalledOnOpenThread(new NonStridedSegmentableFormatReader(), "\"a\nb\",c\nd,e\n".getBytes(StandardCharsets.UTF_8));
    }

    private static void assertNewStreamNotCalledOnOpenThread(FormatReader reader, byte[] payload) throws Exception {
        Thread openThread = Thread.currentThread();
        AtomicBoolean calledOnOpenThread = new AtomicBoolean();
        AtomicInteger newStreamCalls = new AtomicInteger();
        StorageObject object = countingNewStream(bytesStorageObject(payload), openThread, calledOnOpenThread, newStreamCalls);
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            AsyncExternalSourceOperatorFactory factory = factoryForOpenParallelismStreamingTests(
                dummyFormatReaderForOpenParallelismTests(),
                pool
            );
            CloseableIterator<Page> iterator = factory.openWithParallelism(
                reader,
                object,
                List.of("a"),
                ErrorPolicy.STRICT,
                false,
                true,
                true,
                null,
                0L,
                null,
                null,
                null,
                ExternalReadCounters.NOOP,
                null
            );
            assertNotNull(iterator);
            assertFalse("newStream must not run on the openWithParallelism thread", calledOnOpenThread.get());
            try {
                iterator.hasNext();
            } catch (Exception ignored) {
                // Parse may fail; we only need the opener to have run on a pool thread.
            }
            assertThat("segmentator must GET after admission", newStreamCalls.get(), Matchers.greaterThan(0));
            assertFalse("newStream must not run on the openWithParallelism thread", calledOnOpenThread.get());
            iterator.close();
        } finally {
            pool.shutdownNow();
        }
    }

    private static StorageObject countingNewStream(
        StorageObject inner,
        Thread openThread,
        AtomicBoolean calledOnOpenThread,
        AtomicInteger newStreamCalls
    ) {
        return new StorageObject() {
            @Override
            public StorageIdentity storageIdentity() {
                return inner.storageIdentity();
            }

            @Override
            public InputStream newStream() throws IOException {
                newStreamCalls.incrementAndGet();
                if (Thread.currentThread() == openThread) {
                    calledOnOpenThread.set(true);
                }
                return inner.newStream();
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                newStreamCalls.incrementAndGet();
                if (Thread.currentThread() == openThread) {
                    calledOnOpenThread.set(true);
                }
                return inner.newStream(position, length);
            }

            @Override
            public long length() throws IOException {
                return inner.length();
            }

            @Override
            public Instant lastModified() throws IOException {
                return inner.lastModified();
            }

            @Override
            public boolean exists() throws IOException {
                return inner.exists();
            }

            @Override
            public StoragePath path() {
                return inner.path();
            }

            @Override
            public void abortStream(InputStream stream) throws IOException {
                inner.abortStream(stream);
            }
        };
    }

    /**
     * A quoted/escaped (non-strided) uncompressed reader must only ever be handed a whole-file split: split
     * discovery routes such files through {@code requiresSequentialWholeFileRead} to a single leader-bearing
     * split at offset 0. A partial split (no file leader, a non-zero offset, or a record-aligned macro-split
     * that covers only part of the file) can only arrive from an older coordinator in a mixed-version
     * cluster. Reading one mid-file would misread an in-quote newline as a record terminator, so the
     * sequential branch fails loud rather than reading mid-file and silently miscounting.
     */
    public void testOpenWithParallelismQuotedSequentialRejectsPartialSplits() throws IOException {
        AsyncExternalSourceOperatorFactory factory = factoryForOpenParallelismStreamingTests(
            new NonStridedSegmentableFormatReader(),
            Runnable::run
        );
        byte[] payload = "\"a\nb\",c\nd,e\n".getBytes(StandardCharsets.UTF_8);

        // recordAlignedMacroSplit, splitIncludesFileLeader, baseFileOffset: each of the three
        // whole-file invariants, violated in isolation, must be rejected.
        assertRejectsNonWholeFileSplit(factory, payload, false, false, 0L); // no file leader
        assertRejectsNonWholeFileSplit(factory, payload, false, true, 128L); // non-zero file offset
        assertRejectsNonWholeFileSplit(factory, payload, true, true, 0L); // record-aligned macro-split
    }

    private static void assertRejectsNonWholeFileSplit(
        AsyncExternalSourceOperatorFactory factory,
        byte[] payload,
        boolean recordAlignedMacroSplit,
        boolean splitIncludesFileLeader,
        long baseFileOffset
    ) {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> factory.openWithParallelism(
                new NonStridedSegmentableFormatReader(),
                bytesStorageObject(payload),
                List.of("a"),
                ErrorPolicy.STRICT,
                recordAlignedMacroSplit,
                splitIncludesFileLeader,
                false,
                null,
                baseFileOffset,
                null,
                null,
                null,
                ExternalReadCounters.NOOP,
                null
            )
        );
        assertTrue(
            "unexpected message: " + e.getMessage(),
            e.getMessage().startsWith("quoted uncompressed reads must be whole-file splits")
        );
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
        public long[] findBlockBoundaries(StorageObject object, long start, long end, LongConsumer ignored) throws IOException {
            return new long[0];
        }

        @Override
        public InputStream decompressRange(StorageObject object, long blockStart, long nextBlockStart) throws IOException {
            return new ByteArrayInputStream(new byte[0]);
        }
    }

    /** Stream-only codec that fails during {@link #decompress(InputStream)} for abort-path tests. */
    private static final class FailingStreamOnlyCodec implements DecompressionCodec {
        private final GzipDecompressionCodec delegate = new GzipDecompressionCodec();

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public List<String> extensions() {
            return delegate.extensions();
        }

        @Override
        public InputStream decompress(InputStream raw) throws IOException {
            throw new IOException("decompress failed");
        }
    }

    private static SegmentableFormatReader mockInnerForParallelDescribeAndOpen() throws IOException {
        SegmentableFormatReader inner = mock(SegmentableFormatReader.class);
        when(inner.rowPositionStrategy()).thenReturn(PassThroughRowPositionStrategy.INSTANCE);
        when(inner.minimumSegmentSize()).thenReturn(1024L);
        when(inner.formatName()).thenReturn("ndjson");
        when(inner.supportsNativeAsync()).thenReturn(false);
        when(inner.defaultErrorPolicy()).thenReturn(ErrorPolicy.STRICT);
        when(inner.metadata(any())).thenReturn(null);
        when(inner.read(any(), any())).thenReturn(emptyPageIterator());
        when(inner.recordSplitter(anyInt())).thenAnswer(invocation -> TestRecordSplitters.newlineSplitter(invocation.getArgument(0)));
        return inner;
    }

    private static FormatReader dummyFormatReaderForOpenParallelismTests() {
        FormatReader dummyReader = mock(FormatReader.class);
        when(dummyReader.rowPositionStrategy()).thenReturn(PassThroughRowPositionStrategy.INSTANCE);
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
            public StorageIdentity storageIdentity() {
                return AbstractTestStorageObject.NOOP;
            }

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

    /**
     * S3-shaped {@link StorageObject}: {@code abortStream} only calls {@code abort()} when the
     * argument implements {@link Abortable}. Wrappers such as {@code DecompressedStream} miss
     * that cast and fall back to a draining {@code close()}.
     */
    private static final class S3ShapedAbortableStorageObject extends AbstractTestStorageObject {
        interface Abortable {
            void abort();
        }

        final byte[] bytes;
        final AtomicBoolean abortCalled = new AtomicBoolean();
        final AtomicBoolean sawAbortable = new AtomicBoolean();
        final AtomicBoolean sawNonAbortable = new AtomicBoolean();
        final AtomicLong bytesConsumed = new AtomicLong();
        /** Set when a read of the raw body returns {@code -1}, where Apache HttpClient pools the connection. */
        final AtomicBoolean endOfBodyRead = new AtomicBoolean();
        /** {@link #endOfBodyRead} as of the first abort: {@code false} means the abort discarded the connection. */
        final AtomicBoolean endOfBodyReadBeforeAbort = new AtomicBoolean();

        S3ShapedAbortableStorageObject(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public InputStream newStream() {
            return new AbortableDrainStream(bytes, abortCalled, bytesConsumed, endOfBodyRead, endOfBodyReadBeforeAbort);
        }

        @Override
        public InputStream newStream(long position, long length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void abortStream(InputStream stream) throws IOException {
            if (stream instanceof Abortable abortable) {
                sawAbortable.set(true);
                abortable.abort();
            } else {
                sawNonAbortable.set(true);
                stream.close();
            }
        }

        @Override
        public long length() {
            return bytes.length;
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
            return StoragePath.of("s3://bucket/stream.ndjson.gz");
        }
    }

    private static final class AbortableDrainStream extends InputStream implements S3ShapedAbortableStorageObject.Abortable {
        private final ByteArrayInputStream inner;
        private final AtomicBoolean abortCalled;
        private final AtomicLong bytesConsumed;
        private final AtomicBoolean endOfBodyRead;
        private final AtomicBoolean endOfBodyReadBeforeAbort;
        private boolean closed;

        AbortableDrainStream(
            byte[] bytes,
            AtomicBoolean abortCalled,
            AtomicLong bytesConsumed,
            AtomicBoolean endOfBodyRead,
            AtomicBoolean endOfBodyReadBeforeAbort
        ) {
            this.inner = new ByteArrayInputStream(bytes);
            this.abortCalled = abortCalled;
            this.bytesConsumed = bytesConsumed;
            this.endOfBodyRead = endOfBodyRead;
            this.endOfBodyReadBeforeAbort = endOfBodyReadBeforeAbort;
        }

        @Override
        public int read() {
            int b = inner.read();
            if (b >= 0) {
                bytesConsumed.incrementAndGet();
            } else {
                endOfBodyRead.set(true);
            }
            return b;
        }

        @Override
        public int read(byte[] buf, int off, int len) {
            int n = inner.read(buf, off, len);
            if (n > 0) {
                bytesConsumed.addAndGet(n);
            } else if (n < 0) {
                endOfBodyRead.set(true);
            }
            return n;
        }

        @Override
        public void abort() {
            if (abortCalled.getAndSet(true) == false) {
                endOfBodyReadBeforeAbort.set(endOfBodyRead.get());
            }
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            if (abortCalled.get()) {
                return;
            }
            byte[] drain = new byte[8192];
            int n;
            while ((n = inner.read(drain, 0, drain.length)) != -1) {
                bytesConsumed.addAndGet(n);
            }
        }
    }

    private static byte[] gzipCompress(byte[] uncompressed) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (GZIPOutputStream gz = new GZIPOutputStream(bos)) {
            gz.write(uncompressed);
        }
        return bos.toByteArray();
    }

    /**
     * UBN: query projection includes a column missing from this file. The helper drops the
     * missing name so the reader is only asked for columns the file actually has. Without
     * this narrowing, CsvFormatReader.initProjection throws "Column not found".
     */
    public void testPerFileQueryProjectionDropsColumnsMissingFromFile() {
        List<Attribute> fileSchema = List.of(attr("name", DataType.KEYWORD), attr("age", DataType.INTEGER));
        List<String> queryProjection = List.of("name", "age", "city");
        List<String> result = AsyncExternalSourceOperatorFactory.perFileQueryProjection(queryProjection, fileSchema);
        assertEquals(List.of("name", "age"), result);
    }

    /**
     * UBN: file's natural column order differs from the unified projection order. The helper
     * preserves the file's natural order so the adapter's ColumnMapping (which indexes into
     * the file's natural schema) lines up with the reader's output.
     */
    public void testPerFileQueryProjectionPreservesFileNaturalOrder() {
        List<Attribute> fileSchema = List.of(attr("age", DataType.LONG), attr("name", DataType.KEYWORD), attr("city", DataType.KEYWORD));
        List<String> queryProjection = List.of("name", "city");
        List<String> result = AsyncExternalSourceOperatorFactory.perFileQueryProjection(queryProjection, fileSchema);
        assertEquals(List.of("name", "city"), result);
    }

    /**
     * Null read schema (no coordinator pin) is a pass-through: the helper returns the original
     * projection unchanged so behavior matches the pre-UBN state.
     */
    public void testPerFileQueryProjectionPassesThroughWhenReadSchemaNull() {
        List<String> queryProjection = List.of("a", "b", "c");
        assertSame(queryProjection, AsyncExternalSourceOperatorFactory.perFileQueryProjection(queryProjection, null));
    }

    /**
     * Identity case (every projected column is present in the file): the helper returns the
     * projection ordered by the file's natural layout. Confirms FFW/STRICT behavior is unchanged.
     */
    public void testPerFileQueryProjectionIdentityWhenFileHasEveryProjectedColumn() {
        List<Attribute> fileSchema = List.of(attr("name", DataType.KEYWORD), attr("age", DataType.LONG), attr("city", DataType.KEYWORD));
        List<String> queryProjection = List.of("name", "age", "city");
        List<String> result = AsyncExternalSourceOperatorFactory.perFileQueryProjection(queryProjection, fileSchema);
        assertEquals(List.of("name", "age", "city"), result);
    }

    public void testPushedLimitSharedAcrossTwoProducers() throws Exception {
        int splitCount = 20;
        int limit = 3;
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < splitCount; i++) {
            splits.add(new FileSplit("test", StoragePath.of("s3://bucket/f" + i + ".parquet"), 0, 100, "parquet", Map.of(), Map.of()));
        }
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);
        AtomicInteger readCount = new AtomicInteger();
        CountDownLatch bothEnteredRead = new CountDownLatch(2);
        CountDownLatch startDeliver = new CountDownLatch(1);
        LatchedPageReader formatReader = new LatchedPageReader(readCount, bothEnteredRead, startDeliver, 1);
        ExecutorService pool = Executors.newCachedThreadPool(EsExecutors.daemonThreadFactory("test", "limit-budget"));
        try {
            AsyncExternalSourceOperatorFactory factory = limitBudgetFactory(formatReader, sliceQueue, pool, limit, 10);
            assertNotNull(factory.sourceLimiter());
            assertEquals(limit, factory.sourceLimiter().limit());

            DriverContext ctx1 = mockLimitBudgetDriverContext();
            DriverContext ctx2 = mockLimitBudgetDriverContext();
            CountDownLatch bothDone = new CountDownLatch(2);
            doAnswer(inv -> {
                bothDone.countDown();
                return null;
            }).when(ctx1).removeAsyncAction();
            doAnswer(inv -> {
                bothDone.countDown();
                return null;
            }).when(ctx2).removeAsyncAction();

            SourceOperator op1 = factory.get(ctx1);
            SourceOperator op2 = factory.get(ctx2);
            assertTrue(bothEnteredRead.await(30, TimeUnit.SECONDS));
            startDeliver.countDown();
            assertTrue(bothDone.await(30, TimeUnit.SECONDS));

            int rows = drainRemaining(op1) + drainRemaining(op2);
            op1.close();
            op2.close();

            assertThat(factory.sourceLimiter().remaining(), equalTo(0));
            assertThat(readCount.get(), lessThanOrEqualTo(4));
            assertThat(sliceQueue.remaining(), equalTo(splitCount - readCount.get()));
            assertThat(rows, equalTo(limit));
        } finally {
            startDeliver.countDown();
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    public void testSkipRowDropsStillReachLimit() throws Exception {
        int limit = 4;
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            splits.add(new FileSplit("test", StoragePath.of("s3://bucket/f" + i + ".parquet"), 0, 100, "parquet", Map.of(), Map.of()));
        }
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);
        List<Integer> seenRowLimits = Collections.synchronizedList(new ArrayList<>());
        DroppingPageReader formatReader = new DroppingPageReader(seenRowLimits, 3, 1);
        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            new StubMultiFileStorageProvider(),
            formatReader,
            StoragePath.of("s3://bucket/f0.parquet"),
            limitBudgetAttributes(),
            100,
            10,
            Runnable::run
        ).sliceQueue(sliceQueue).rowLimit(limit).errorPolicy(ErrorPolicy.LENIENT).producerBlockFactory(TEST_BLOCK_FACTORY).build();

        DriverContext ctx = mockLimitBudgetDriverContext();
        SourceOperator op = factory.get(ctx);
        int rows = drainRemaining(op);
        op.close();

        assertFalse("reader must be opened", seenRowLimits.isEmpty());
        for (int seen : seenRowLimits) {
            assertEquals("slice-queue text path must not cap the reader; producer limiter is the cap", FormatReader.NO_LIMIT, seen);
        }
        assertThat(factory.sourceLimiter().remaining(), equalTo(0));
        assertThat(rows, equalTo(limit));
    }

    public void testSkipRowLenientMultiFileReaderNotCappedByRemaining() throws Exception {
        int limit = 10;
        FileList fileList = GlobExpander.fileListOf(
            List.of(new StorageEntry(StoragePath.of("s3://bucket/data/f1.parquet"), 100, Instant.EPOCH)),
            "s3://bucket/data/*.parquet"
        );
        List<Integer> seenRowLimits = Collections.synchronizedList(new ArrayList<>());
        DroppingPageReader formatReader = new DroppingPageReader(seenRowLimits, 20, 1);
        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            new StubMultiFileStorageProvider(),
            formatReader,
            StoragePath.of("s3://bucket/data/f1.parquet"),
            limitBudgetAttributes(),
            100,
            10,
            Runnable::run
        ).fileList(fileList).rowLimit(limit).errorPolicy(ErrorPolicy.LENIENT).producerBlockFactory(TEST_BLOCK_FACTORY).build();

        assertEquals(FormatReader.NO_LIMIT, factory.sourceReaderRowLimit());
        DriverContext ctx = mockLimitBudgetDriverContext();
        SourceOperator op = factory.get(ctx);
        int rows = drainRemaining(op);
        op.close();

        assertFalse(seenRowLimits.isEmpty());
        for (int seen : seenRowLimits) {
            assertEquals("lenient reader must not prefetch-clip at remaining()", FormatReader.NO_LIMIT, seen);
        }
        assertThat(factory.sourceLimiter().remaining(), equalTo(0));
        assertThat(rows, equalTo(limit));
    }

    public void testSkipRowLenientRangeReaderNotCappedByRemaining() throws Exception {
        int limit = 10;
        FileSplit rangeSplit = new FileSplit(
            "test",
            StoragePath.of("s3://bucket/f0.parquet"),
            0,
            100,
            "parquet",
            Map.of(FileSplitProvider.RANGE_SPLIT_KEY, "true"),
            Map.of()
        );
        List<Integer> seenRowLimits = Collections.synchronizedList(new ArrayList<>());
        RecordingRangeReader formatReader = new RecordingRangeReader(seenRowLimits, 20);
        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            new StubMultiFileStorageProvider(),
            formatReader,
            StoragePath.of("s3://bucket/f0.parquet"),
            limitBudgetAttributes(),
            100,
            10,
            Runnable::run
        )
            .sliceQueue(new ExternalSliceQueue(List.of(rangeSplit)))
            .rowLimit(limit)
            .errorPolicy(ErrorPolicy.LENIENT)
            .producerBlockFactory(TEST_BLOCK_FACTORY)
            .build();

        assertEquals(FormatReader.NO_LIMIT, factory.sourceReaderRowLimit());
        DriverContext ctx = mockLimitBudgetDriverContext();
        SourceOperator op = factory.get(ctx);
        int rows = drainRemaining(op);
        op.close();

        assertFalse("range path must call readRange", seenRowLimits.isEmpty());
        for (int seen : seenRowLimits) {
            assertEquals("lenient range reader must not prefetch-clip at remaining()", FormatReader.NO_LIMIT, seen);
        }
        assertThat(factory.sourceLimiter().remaining(), equalTo(0));
        assertThat("source may over-deliver a page; LimitOperator clips to N", rows, greaterThanOrEqualTo(limit));
    }

    public void testStrictPushedLimitPrefetchClipsReaderRemaining() {
        List<ExternalSplit> splits = List.of(
            new FileSplit("test", StoragePath.of("s3://bucket/f0.parquet"), 0, 100, "parquet", Map.of(), Map.of())
        );
        AsyncExternalSourceOperatorFactory factory = limitBudgetFactory(
            new PageCountingFormatReader(new AtomicInteger()),
            new ExternalSliceQueue(splits),
            Runnable::run,
            4,
            10
        );
        assertEquals(4, factory.sourceReaderRowLimit());
    }

    public void testLimitStopRemovesAsyncActionWithoutCancelling() throws Exception {
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            splits.add(new FileSplit("test", StoragePath.of("s3://bucket/f" + i + ".parquet"), 0, 100, "parquet", Map.of(), Map.of()));
        }
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);
        AtomicInteger readCount = new AtomicInteger();
        FormatReader formatReader = new PageCountingFormatReader(readCount);
        AsyncExternalSourceOperatorFactory factory = limitBudgetFactory(formatReader, sliceQueue, Runnable::run, 1, 10);

        DriverContext ctx = mockLimitBudgetDriverContext();
        AtomicInteger removeCount = new AtomicInteger();
        doAnswer(inv -> {
            removeCount.incrementAndGet();
            return null;
        }).when(ctx).removeAsyncAction();

        SourceOperator op = factory.get(ctx);
        int rows = drainRemaining(op);
        assertThat(removeCount.get(), equalTo(1));
        assertThat(factory.sourceLimiter().remaining(), equalTo(0));
        assertThat(rows, equalTo(1));
        assertThat(readCount.get(), equalTo(1));
        op.close();
        assertThat(removeCount.get(), equalTo(1));
    }

    public void testParkWithPageReleasesWhenRemainingZero() throws Exception {
        AtomicInteger nextCalls = new AtomicInteger();
        CountDownLatch enteredSecondNext = new CountDownLatch(1);
        TwoPageHugeReader formatReader = new TwoPageHugeReader(nextCalls, enteredSecondNext);
        List<ExternalSplit> splits = List.of(
            new FileSplit("test", StoragePath.of("s3://bucket/f0.parquet"), 0, 100, "parquet", Map.of(), Map.of())
        );
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);
        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            new StubMultiFileStorageProvider(),
            formatReader,
            StoragePath.of("s3://bucket/f0.parquet"),
            limitBudgetAttributes(),
            100,
            1,
            Runnable::run
        ).sliceQueue(sliceQueue).producerBlockFactory(TEST_BLOCK_FACTORY).build();
        Limiter observed = new Limiter(100);
        factory.setObservedLimiter(observed);

        DriverContext ctx = mockLimitBudgetDriverContext();
        SourceOperator op = factory.get(ctx);
        assertTrue(enteredSecondNext.await(30, TimeUnit.SECONDS));
        observed.tryAccumulateHits(observed.remaining());
        int rows = drainRemaining(op);
        op.close();
        assertThat(nextCalls.get(), equalTo(2));
        assertThat("parked second page must be released, not delivered", rows, equalTo(TwoPageHugeReader.ROWS));
    }

    public void testObservedLimiterStopsClaimingSplits() throws Exception {
        int splitCount = 12;
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < splitCount; i++) {
            splits.add(new FileSplit("test", StoragePath.of("s3://bucket/f" + i + ".parquet"), 0, 100, "parquet", Map.of(), Map.of()));
        }
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);
        AtomicInteger readCount = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        LatchedPageReader formatReader = new LatchedPageReader(readCount, entered, proceed, 1);
        ExecutorService pool = Executors.newCachedThreadPool(EsExecutors.daemonThreadFactory("test", "observed-limit"));
        try {
            AsyncExternalSourceOperatorFactory factory = limitBudgetFactory(formatReader, sliceQueue, pool, FormatReader.NO_LIMIT, 10);
            Limiter observed = new Limiter(1);
            factory.setObservedLimiter(observed);
            assertNull(factory.sourceLimiter());

            DriverContext ctx = mockLimitBudgetDriverContext();
            CountDownLatch done = new CountDownLatch(1);
            doAnswer(inv -> {
                done.countDown();
                return null;
            }).when(ctx).removeAsyncAction();

            SourceOperator op = factory.get(ctx);
            assertTrue(entered.await(30, TimeUnit.SECONDS));
            observed.tryAccumulateHits(1);
            proceed.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS));
            drainRemaining(op);
            op.close();
            assertThat(readCount.get(), equalTo(1));
            assertThat(sliceQueue.remaining(), equalTo(splitCount - 1));
        } finally {
            proceed.countDown();
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    public void testPushedLimiterIgnoresObservedLimiter() throws Exception {
        List<ExternalSplit> splits = List.of(
            new FileSplit("test", StoragePath.of("s3://bucket/f0.parquet"), 0, 100, "parquet", Map.of(), Map.of()),
            new FileSplit("test", StoragePath.of("s3://bucket/f1.parquet"), 0, 100, "parquet", Map.of(), Map.of()),
            new FileSplit("test", StoragePath.of("s3://bucket/f2.parquet"), 0, 100, "parquet", Map.of(), Map.of())
        );
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);
        AtomicInteger readCount = new AtomicInteger();
        FormatReader formatReader = new PageCountingFormatReader(readCount);
        AsyncExternalSourceOperatorFactory factory = limitBudgetFactory(formatReader, sliceQueue, Runnable::run, 3, 10);
        Limiter observed = new Limiter(1);
        observed.tryAccumulateHits(1);
        factory.setObservedLimiter(observed);

        DriverContext ctx = mockLimitBudgetDriverContext();
        SourceOperator op = factory.get(ctx);
        int rows = drainRemaining(op);
        op.close();
        assertThat(factory.sourceLimiter().remaining(), equalTo(0));
        assertThat(rows, equalTo(3));
        assertThat(readCount.get(), equalTo(3));
    }

    public void testPushedLimitDeliversFullPageWithoutSlicing() throws Exception {
        List<ExternalSplit> splits = List.of(
            new FileSplit("test", StoragePath.of("s3://bucket/f0.parquet"), 0, 100, "parquet", Map.of(), Map.of())
        );
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);
        AtomicInteger readCount = new AtomicInteger();
        FormatReader formatReader = new LatchedPageReader(readCount, new CountDownLatch(1), null, 5);
        AsyncExternalSourceOperatorFactory factory = limitBudgetFactory(formatReader, sliceQueue, Runnable::run, 3, 10);

        DriverContext ctx = mockLimitBudgetDriverContext();
        SourceOperator op = factory.get(ctx);
        int rows = drainRemaining(op);
        op.close();
        assertThat("source must forward the full page; LimitOperator slices overflow", rows, equalTo(5));
        assertThat(factory.sourceLimiter().remaining(), equalTo(0));
        assertThat(readCount.get(), equalTo(1));
    }

    private static List<Attribute> limitBudgetAttributes() {
        return List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );
    }

    private static AsyncExternalSourceOperatorFactory limitBudgetFactory(
        FormatReader formatReader,
        ExternalSliceQueue sliceQueue,
        Executor executor,
        int rowLimit,
        int maxBufferSize
    ) {
        return AsyncExternalSourceOperatorFactory.builder(
            new StubMultiFileStorageProvider(),
            formatReader,
            StoragePath.of("s3://bucket/f0.parquet"),
            limitBudgetAttributes(),
            100,
            maxBufferSize,
            executor
        ).sliceQueue(sliceQueue).rowLimit(rowLimit).producerBlockFactory(TEST_BLOCK_FACTORY).build();
    }

    private static DriverContext mockLimitBudgetDriverContext() {
        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();
        return driverContext;
    }

    private static int drainRemaining(SourceOperator operator) {
        int rows = 0;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (true) {
            Page page = operator.getOutput();
            if (page != null) {
                rows += page.getPositionCount();
                page.releaseBlocks();
                continue;
            }
            if (operator.isFinished()) {
                return rows;
            }
            Thread.yield();
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out draining operator");
            }
        }
    }

    private static Attribute attr(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private static class LatchedPageReader implements NoConfigFormatReader {
        private final AtomicInteger readCount;
        private final CountDownLatch entered;
        @Nullable
        private final CountDownLatch proceed;
        private final int rows;

        LatchedPageReader(AtomicInteger readCount, CountDownLatch entered, @Nullable CountDownLatch proceed, int rows) {
            this.readCount = readCount;
            this.entered = entered;
            this.proceed = proceed;
            this.rows = rows;
        }

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            readCount.incrementAndGet();
            entered.countDown();
            if (proceed != null) {
                try {
                    if (proceed.await(30, TimeUnit.SECONDS) == false) {
                        throw new AssertionError("timed out waiting to emit page");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
            int[] values = new int[rows];
            Page page = new Page(TEST_BLOCK_FACTORY.newIntArrayVector(values, rows).asBlock());
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
            return "latched-page";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    /**
     * Emits {@code rawRows} then keeps only every other row, simulating skip_row adapter drops
     * after the reader. Honours {@link FormatReadContext#rowLimit()} so a factory that caps the
     * reader with remaining rows cannot reach N survivors.
     */
    private static class DroppingPageReader implements NoConfigFormatReader {
        private final List<Integer> seenRowLimits;
        private final int rawRows;
        private final int keepEvery;

        DroppingPageReader(List<Integer> seenRowLimits, int rawRows, int keepEvery) {
            this.seenRowLimits = seenRowLimits;
            this.rawRows = rawRows;
            this.keepEvery = keepEvery;
        }

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            seenRowLimits.add(context.rowLimit());
            int cap = context.rowLimit() == FormatReader.NO_LIMIT ? rawRows : Math.min(rawRows, Math.max(0, context.rowLimit()));
            int kept = 0;
            for (int i = 0; i < cap; i++) {
                if (i % (keepEvery + 1) == 0) {
                    kept++;
                }
            }
            int[] values = new int[kept];
            Page page = new Page(TEST_BLOCK_FACTORY.newIntArrayVector(values, kept).asBlock());
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
            return "dropping-page";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    /**
     * Range-aware reader that records {@link RangeReadContext#rowLimit()} so skip_row can
     * assert the range rail gets {@link FormatReader#NO_LIMIT}, not remaining().
     */
    private static class RecordingRangeReader implements RangeAwareFormatReader, NoConfigFormatReader {
        private final List<Integer> seenRowLimits;
        private final int rawRows;

        RecordingRangeReader(List<Integer> seenRowLimits, int rawRows) {
            this.seenRowLimits = seenRowLimits;
            this.rawRows = rawRows;
        }

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public List<SplitRange> discoverSplitRanges(StorageObject object) {
            return List.of();
        }

        @Override
        public CloseableIterator<Page> readRange(StorageObject object, RangeReadContext context) {
            seenRowLimits.add(context.rowLimit());
            int cap = context.rowLimit() == FormatReader.NO_LIMIT ? rawRows : Math.min(rawRows, Math.max(0, context.rowLimit()));
            Page page = new Page(TEST_BLOCK_FACTORY.newIntArrayVector(new int[cap], cap).asBlock());
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
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            throw new AssertionError("range split must use readRange, not read");
        }

        @Override
        public String formatName() {
            return "parquet";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    /**
     * One iterator, two huge pages. With {@code maxBufferSize=1} the second {@code next()}
     * fills the park-with-page path once the first page occupies the buffer.
     */
    private static class TwoPageHugeReader implements NoConfigFormatReader {
        static final int ROWS = 80_000;
        private final AtomicInteger nextCalls;
        private final CountDownLatch enteredSecondNext;

        TwoPageHugeReader(AtomicInteger nextCalls, CountDownLatch enteredSecondNext) {
            this.nextCalls = nextCalls;
            this.enteredSecondNext = enteredSecondNext;
        }

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            return new CloseableIterator<>() {
                private int emitted = 0;

                @Override
                public boolean hasNext() {
                    return emitted < 2;
                }

                @Override
                public Page next() {
                    if (emitted >= 2) {
                        throw new NoSuchElementException();
                    }
                    int n = nextCalls.incrementAndGet();
                    if (n == 2) {
                        enteredSecondNext.countDown();
                    }
                    emitted++;
                    int[] values = new int[ROWS];
                    return new Page(TEST_BLOCK_FACTORY.newIntArrayVector(values, ROWS).asBlock());
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public String formatName() {
            return "two-page-huge";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    /**
     * Iterator whose {@link CloseableIterator#waitForReady()} stays incomplete until {@code allowPage}
     * counts down. Used to park the AESOF producer without pinning the consumer thread.
     */
    private static final class ParkingReader implements NoConfigFormatReader {
        private final CountDownLatch allowPage;
        private final AtomicBoolean parked = new AtomicBoolean();

        private ParkingReader(CountDownLatch allowPage) {
            this.allowPage = allowPage;
        }

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            SubscribableListener<Void> ready = new SubscribableListener<>();
            Thread releaser = new Thread(() -> {
                try {
                    parked.set(true);
                    allowPage.await();
                    ready.onResponse(null);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    ready.onFailure(e);
                }
            }, "parking-reader-release");
            releaser.setDaemon(true);
            releaser.start();
            return new CloseableIterator<>() {
                private boolean emitted;

                @Override
                public SubscribableListener<Void> waitForReady() {
                    return ready.isDone() ? SubscribableListener.newSucceeded(null) : ready;
                }

                @Override
                public Page tryAdvance() {
                    if (ready.isDone() == false || emitted) {
                        return null;
                    }
                    emitted = true;
                    return createTestPage();
                }

                @Override
                public boolean hasNext() {
                    return emitted == false && ready.isDone();
                }

                @Override
                public Page next() {
                    Page page = tryAdvance();
                    if (page == null) {
                        throw new NoSuchElementException();
                    }
                    return page;
                }

                @Override
                public void close() {
                    allowPage.countDown();
                }
            };
        }

        @Override
        public String formatName() {
            return "parking";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    /**
     * Format reader for the state-machine test. Every {@code read} returns a single-page iterator
     * that increments {@code closeCalls} on {@link CloseableIterator#close()}, so the test can
     * assert that every opened iterator is closed exactly once.
     */
    private static class TrackingReader implements NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

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

    private static void drainMultiFileOperator(StorageProvider storageProvider, FileList fileList, StoragePath path) {
        FormatReader formatReader = new PageCountingFormatReader(new AtomicInteger());
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );
        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(mock(BlockFactory.class));
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

    private static ReferenceAttribute ref(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type);
    }

    /**
     * Counts {@code metadata()} and records {@code withSchema(...)} so empty-projection bind tests can
     * distinguish a coordinator pin from a re-inferred schema. Default {@link FormatReader#withSchema}
     * is identity; this override is required or the pin would never be observed.
     */
    private static class CountingBindAndSplitReader implements NoConfigFormatReader {
        static final List<Attribute> INFERRED_SCHEMA = List.of(ref("inferred_a", DataType.KEYWORD), ref("inferred_b", DataType.LONG));

        private final AtomicInteger metadataCalls;
        private final AtomicInteger readCalls;
        private volatile List<Attribute> withSchemaReceived;
        private volatile boolean replaced;

        CountingBindAndSplitReader() {
            this(new AtomicInteger(), new AtomicInteger());
        }

        CountingBindAndSplitReader(AtomicInteger metadataCalls, AtomicInteger readCalls) {
            this.metadataCalls = metadataCalls;
            this.readCalls = readCalls;
        }

        int metadataCalls() {
            return metadataCalls.get();
        }

        int readCalls() {
            return readCalls.get();
        }

        List<Attribute> withSchemaReceived() {
            return withSchemaReceived;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            metadataCalls.incrementAndGet();
            return new SourceMetadata() {
                @Override
                public List<Attribute> schema() {
                    return INFERRED_SCHEMA;
                }

                @Override
                public String sourceType() {
                    return "csv";
                }

                @Override
                public String location() {
                    return "s3://bucket/data.csv";
                }
            };
        }

        @Override
        public FormatReader withSchema(List<Attribute> schema) {
            withSchemaReceived = schema;
            // A distinct instance: production must assign fileReader = withSchema(...). Returning this
            // would let a dropped assignment still pass, because the original would both record and read.
            CountingBindAndSplitReader next = new CountingBindAndSplitReader(metadataCalls, readCalls);
            next.withSchemaReceived = schema;
            replaced = true;
            return next;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            if (replaced) {
                throw new AssertionError("read() after withSchema must use the returned instance");
            }
            readCalls.incrementAndGet();
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
            return "counting-bind";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".csv");
        }

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public void close() {}
    }

    /**
     * Format reader that emits a single, caller-supplied page (rebuilt per read via the supplier so
     * each read owns fresh, releasable blocks). Used by the partition-collision tests to control
     * the exact file-body page shape the factory adapts.
     */
    private static class SinglePageReader implements NoConfigFormatReader {

        private final Supplier<Page> pageSupplier;

        SinglePageReader(Supplier<Page> pageSupplier) {
            this.pageSupplier = pageSupplier;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            Page page = pageSupplier.get();
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
            return "single-page";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public void close() {}
    }

    private static class PageCountingFormatReader implements NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

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

    /**
     * First {@link #read} throws {@link IOException}; later reads emit one page, matching
     * {@link PageCountingFormatReader}. Used to fail the first parallel operator during {@code get()}.
     */
    private static class FailOnFirstReadFormatReader implements NoConfigFormatReader {
        private final AtomicInteger readCount = new AtomicInteger();

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            if (readCount.incrementAndGet() == 1) {
                throw new IOException("injected first-read failure");
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
            return "test-fail-first";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    private static class FailOnSecondFileFormatReader implements NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        private final AtomicInteger callCount = new AtomicInteger(0);

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            int call = callCount.incrementAndGet();
            if (call >= 2) {
                throw new IOException("Simulated read error on file: " + object.path().objectName());
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

    private static class RecordingMultiFileStorageProvider extends StubMultiFileStorageProvider {
        record Call(StoragePath path, Long length, Instant lastModified) {}

        final List<Call> calls = new ArrayList<>();

        @Override
        public StorageObject newObject(StoragePath path) {
            calls.add(new Call(path, null, null));
            return super.newObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            calls.add(new Call(path, length, null));
            return super.newObject(path, length);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            calls.add(new Call(path, length, lastModified));
            return super.newObject(path, length, lastModified);
        }
    }

    /** Storage provider that serves canned bytes keyed by {@link StoragePath#toString()}. */
    private static class ByteArrayStorageProvider implements StorageProvider {
        private final Map<String, byte[]> bodies;

        ByteArrayStorageProvider(Map<String, byte[]> bodies) {
            this.bodies = bodies;
        }

        private StorageObject object(StoragePath path) {
            byte[] bytes = bodies.getOrDefault(path.toString(), new byte[0]);
            return new ByteArrayStorageObject(path, bytes);
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            return object(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return object(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return object(path);
        }

        @Override
        public StorageChildren listChildren(StoragePath prefix, int limit) {
            return null; // directory-aware listing is irrelevant to this test double
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(StoragePath path) {
            return bodies.containsKey(path.toString());
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of("s3");
        }

        @Override
        public void close() {}
    }

    private static class ByteArrayStorageObject extends AbstractTestStorageObject {
        private final StoragePath path;
        private final byte[] bytes;

        ByteArrayStorageObject(StoragePath path, byte[] bytes) {
            this.path = path;
            this.bytes = bytes;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(bytes);
        }

        @Override
        public InputStream newStream(long position, long length) {
            int start = Math.toIntExact(position);
            int len = Math.toIntExact(length);
            return new ByteArrayInputStream(bytes, start, len);
        }

        @Override
        public long length() {
            return bytes.length;
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
     * Records the first byte of each object's stream so wrap-for-object tests can see whether gzip
     * was stripped before the inner reader ran.
     */
    private static class StreamPeekingFormatReader implements NoConfigFormatReader {
        final List<Integer> peeked = new ArrayList<>();

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            try (InputStream in = object.newStream()) {
                peeked.add(in.read());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return emptyIterator();
        }

        @Override
        public String formatName() {
            return "csv";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".csv");
        }

        @Override
        public boolean supportsWholeFileCompression() {
            return true;
        }

        @Override
        public void close() {}
    }

    /**
     * Reads the object's stream in both {@code metadata} and {@code read} so schema-bind and
     * split bytes are real received counts.
     */
    private static final class DrainingBindAndSplitReader implements NoConfigFormatReader {
        @Override
        public SourceMetadata metadata(StorageObject object) {
            drain(object);
            return new SimpleSourceMetadata(
                List.of(
                    new FieldAttribute(
                        Source.EMPTY,
                        "n",
                        new EsField("n", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
                    )
                ),
                "ndjson",
                object.path().toString()
            );
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            drain(object);
            Page page = new Page(1);
            return new CloseableIterator<>() {
                private boolean consumed;

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

        private static void drain(StorageObject object) {
            try (InputStream in = object.newStream()) {
                in.readAllBytes();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public String formatName() {
            return "ndjson";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".ndjson");
        }

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public void close() {}
    }

    private static final class MeteredPayloadStorageProvider implements StorageProvider {
        private final StoragePath path;
        private final byte[] payload;

        MeteredPayloadStorageProvider(StoragePath path, byte[] payload) {
            this.path = path;
            this.payload = payload;
        }

        @Override
        public StorageChildren listChildren(StoragePath prefix, int limit) {
            return null;
        }

        @Override
        public StorageObject newObject(StoragePath requested) {
            return new MeteredPayloadStorageObject(requested, payload);
        }

        @Override
        public StorageObject newObject(StoragePath requested, long length) {
            return new MeteredPayloadStorageObject(requested, payload);
        }

        @Override
        public StorageObject newObject(StoragePath requested, long length, Instant lastModified) {
            return new MeteredPayloadStorageObject(requested, payload);
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(StoragePath requested) {
            return path.equals(requested);
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of("s3");
        }

        @Override
        public void close() {}
    }

    private static final class MeteredPayloadStorageObject extends AbstractMeteredStorageObject {
        private final StoragePath path;
        private final byte[] payload;

        MeteredPayloadStorageObject(StoragePath path, byte[] payload) {
            this.path = path;
            this.payload = payload;
        }

        @Override
        public StorageIdentity storageIdentity() {
            return StorageIdentity.unique();
        }

        @Override
        public InputStream newStream() {
            counters.addRequest(1L, 0L);
            return metered(new ByteArrayInputStream(payload));
        }

        @Override
        public InputStream newStream(long position, long length) {
            counters.addRequest(1L, 0L);
            int from = Math.toIntExact(position);
            int to = Math.toIntExact(Math.min(payload.length, position + length));
            return metered(new ByteArrayInputStream(payload, from, Math.max(0, to - from)));
        }

        @Override
        public long length() {
            return payload.length;
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

    private static class StubMultiFileStorageProvider implements StorageProvider {
        @Override
        public StorageChildren listChildren(StoragePath prefix, int limit) {
            return null; // directory-aware listing is irrelevant to this test double
        }

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

    private static class StubMultiFileStorageObject extends AbstractTestStorageObject {
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
    private static class TestSyncFormatReader implements NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

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
    private static class SplitCapturingFormatReader implements NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        private final List<StorageObject> capturedObjects;
        private final List<Boolean> capturedSkipFirstLine;
        private final List<Boolean> capturedLastSplit = new ArrayList<>();
        private final List<Boolean> capturedStatsFileFinal = new ArrayList<>();

        SplitCapturingFormatReader(List<StorageObject> capturedObjects, List<Boolean> capturedSkipFirstLine) {
            this.capturedObjects = capturedObjects;
            this.capturedSkipFirstLine = capturedSkipFirstLine;
        }

        List<Boolean> capturedLastSplit() {
            return capturedLastSplit;
        }

        List<Boolean> capturedStatsFileFinal() {
            return capturedStatsFileFinal;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            capturedObjects.add(object);
            capturedSkipFirstLine.add(context.firstSplit() == false);
            capturedLastSplit.add(context.lastSplit());
            capturedStatsFileFinal.add(context.statsFileFinal());
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
     * Format reader that implements SegmentableFormatReader, NoConfigFormatReader and tracks which methods are called.
     */
    private static class LatchedSmallSegmentReader extends TrackingSegmentableFormatReader {
        private final CountDownLatch entered;
        private final CountDownLatch proceed;

        LatchedSmallSegmentReader(CountDownLatch entered, CountDownLatch proceed) {
            this.entered = entered;
            this.proceed = proceed;
        }

        @Override
        public long minimumSegmentSize() {
            return 1024;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            entered.countDown();
            try {
                if (proceed.await(30, TimeUnit.SECONDS) == false) {
                    throw new AssertionError("timed out waiting to proceed");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            return super.read(object, context);
        }
    }

    /**
     * Format reader that implements SegmentableFormatReader, NoConfigFormatReader and tracks which methods are called.
     */
    private static class TrackingSegmentableFormatReader implements SegmentableFormatReader, NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        final AtomicInteger readCount = new AtomicInteger(0);
        final AtomicInteger readWithFirstSplitFalseCount = new AtomicInteger(0);

        @Override
        public RecordSplitter recordSplitter(int maxRecordBytes) {
            return TestRecordSplitters.newlineSplitter(maxRecordBytes);
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

    /**
     * A segmentable reader whose splitter reports {@code supportsStridedProbing() == false}, mirroring a
     * quoting-on CSV/TSV reader. Drives the factory onto the {@code SEGMENTABLE_UNCOMPRESSED_SEQUENTIAL}
     * dispatch branch.
     */
    private static class NonStridedSegmentableFormatReader extends TrackingSegmentableFormatReader {
        @Override
        public RecordSplitter recordSplitter(int maxRecordBytes) {
            return TestRecordSplitters.nonStridedSplitter(maxRecordBytes);
        }
    }

    /** Storage provider that serves one drain-simulating object for abort-on-close tests. */
    private static class DrainFixtureStorageProvider implements StorageProvider {
        private final byte[] payload;
        private final DrainSimulatingStorageObject.Tracking tracking;
        private final StoragePath objectPath;

        DrainFixtureStorageProvider(byte[] payload, DrainSimulatingStorageObject.Tracking tracking, StoragePath objectPath) {
            this.payload = payload;
            this.tracking = tracking;
            this.objectPath = objectPath;
        }

        private StorageObject object() {
            return DrainSimulatingStorageObject.create(payload, tracking, objectPath);
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            return object();
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return object();
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return object();
        }

        @Override
        public StorageChildren listChildren(StoragePath prefix, int limit) {
            return null;
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

    private static class LargeStorageProvider implements StorageProvider {
        @Override
        public StorageChildren listChildren(StoragePath prefix, int limit) {
            return null; // directory-aware listing is irrelevant to this test double
        }

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

    private static class LargeStorageObject extends AbstractTestStorageObject {
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
    private static class AlwaysFailFormatReader implements NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

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
    private static class MultiPageFormatReader implements NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

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
    private static class FailAfterNReadsFormatReader implements NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

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

    private static class TestAsyncFormatReader implements NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

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
