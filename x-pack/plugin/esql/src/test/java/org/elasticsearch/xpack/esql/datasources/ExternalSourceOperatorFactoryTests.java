/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Tests for ExternalSourceOperatorFactory.
 *
 * This demonstrates the integration of StorageProvider and FormatReader
 * to create source operators for external data.
 */
public class ExternalSourceOperatorFactoryTests extends ESTestCase {

    public void testCreateOperatorWithMockedStorageAndFormat() throws IOException {
        // Create a temporary CSV file
        Path tempFile = createTempFile("test", ".csv");
        String csvContent = """
            name,age,city
            Alice,30,NYC
            Bob,25,LA
            Charlie,35,SF
            """;
        Files.writeString(tempFile, csvContent);

        // Create mock storage provider and format reader
        StorageProvider storageProvider = Mockito.mock(StorageProvider.class);
        Mockito.when(storageProvider.supportedSchemes()).thenReturn(List.of("file"));
        StorageObject storageObject = Mockito.mock(StorageObject.class);
        StoragePath path = StoragePath.of(StoragePath.fileUri(tempFile));
        Mockito.when(storageProvider.newObject(Mockito.any(StoragePath.class))).thenReturn(storageObject);

        FormatReader formatReader = Mockito.mock(FormatReader.class);
        Mockito.when(formatReader.formatName()).thenReturn("csv");
        @SuppressWarnings("unchecked")
        CloseableIterator<org.elasticsearch.compute.data.Page> emptyIterator = Mockito.mock(CloseableIterator.class);
        Mockito.when(emptyIterator.hasNext()).thenReturn(false);
        Mockito.when(formatReader.read(Mockito.any(), Mockito.any(), Mockito.anyInt())).thenReturn(emptyIterator);

        // Define attributes (schema)
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "name",
                new EsField("name", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            ),
            new FieldAttribute(
                Source.EMPTY,
                "age",
                new EsField("age", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            ),
            new FieldAttribute(
                Source.EMPTY,
                "city",
                new EsField("city", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        // Create operator factory
        ExternalSourceOperatorFactory factory = new ExternalSourceOperatorFactory(
            storageProvider,
            formatReader,
            path,
            attributes,
            1000  // batch size
        );

        // Create a mock driver context
        BlockFactory blockFactory = Mockito.mock(BlockFactory.class);
        DriverContext driverContext = Mockito.mock(DriverContext.class);
        Mockito.when(driverContext.blockFactory()).thenReturn(blockFactory);

        // Create the operator
        SourceOperator operator = factory.get(driverContext);
        assertNotNull(operator);

        // Verify the factory description
        String description = factory.describe();
        assertTrue(description.contains("csv"));
        assertTrue(description.contains("file://"));
    }

    public void testFactoryValidation() {
        StorageProvider storageProvider = Mockito.mock(StorageProvider.class);
        FormatReader formatReader = Mockito.mock(FormatReader.class);
        StoragePath path = StoragePath.of("file:///tmp/test.csv");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "name",
                new EsField("name", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        // Test null storage provider
        expectThrows(IllegalArgumentException.class, () -> new ExternalSourceOperatorFactory(null, formatReader, path, attributes, 1000));

        // Test null format reader
        expectThrows(
            IllegalArgumentException.class,
            () -> new ExternalSourceOperatorFactory(storageProvider, null, path, attributes, 1000)
        );

        // Test null path
        expectThrows(
            IllegalArgumentException.class,
            () -> new ExternalSourceOperatorFactory(storageProvider, formatReader, null, attributes, 1000)
        );

        // Test null attributes
        expectThrows(
            IllegalArgumentException.class,
            () -> new ExternalSourceOperatorFactory(storageProvider, formatReader, path, null, 1000)
        );

        // Test invalid batch size
        expectThrows(
            IllegalArgumentException.class,
            () -> new ExternalSourceOperatorFactory(storageProvider, formatReader, path, attributes, 0)
        );

        expectThrows(
            IllegalArgumentException.class,
            () -> new ExternalSourceOperatorFactory(storageProvider, formatReader, path, attributes, -1)
        );
    }

    private static final BlockFactory TEST_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

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
        SplitCapturingFormatReader formatReader = new SplitCapturingFormatReader(capturedObjects, capturedSkipFirstLine);
        StubStorageProvider storageProvider = new StubStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/large.csv");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        BlockFactory blockFactory = Mockito.mock(BlockFactory.class);
        DriverContext driverContext = Mockito.mock(DriverContext.class);
        Mockito.when(driverContext.blockFactory()).thenReturn(blockFactory);

        ExternalSourceOperatorFactory factory = new ExternalSourceOperatorFactory(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            FormatReader.NO_LIMIT,
            sliceQueue
        );

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

        assertTrue("Non-first split with offset > 0 should skip first line", capturedSkipFirstLine.get(0));

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
        SplitCapturingFormatReader formatReader = new SplitCapturingFormatReader(capturedObjects, capturedSkipFirstLine);
        StubStorageProvider storageProvider = new StubStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/data.csv");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        BlockFactory blockFactory = Mockito.mock(BlockFactory.class);
        DriverContext driverContext = Mockito.mock(DriverContext.class);
        Mockito.when(driverContext.blockFactory()).thenReturn(blockFactory);

        ExternalSourceOperatorFactory factory = new ExternalSourceOperatorFactory(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            FormatReader.NO_LIMIT,
            sliceQueue
        );

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

    public void testDescribe() {
        StorageProvider storageProvider = Mockito.mock(StorageProvider.class);
        FormatReader formatReader = Mockito.mock(FormatReader.class);
        Mockito.when(formatReader.formatName()).thenReturn("csv");
        StoragePath path = StoragePath.of("file:///tmp/data.csv");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "col1",
                new EsField("col1", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        ExternalSourceOperatorFactory factory = new ExternalSourceOperatorFactory(storageProvider, formatReader, path, attributes, 500);

        String description = factory.describe();
        assertTrue(description.contains("ExternalSourceOperator"));
        assertTrue(description.contains("csv"));
        assertTrue(description.contains("file:///tmp/data.csv"));
        assertTrue(description.contains("500"));
    }

    // ===== Helpers =====

    private static Page createTestPage() {
        IntBlock block = TEST_BLOCK_FACTORY.newIntBlockBuilder(1).appendInt(42).build();
        return new Page(block);
    }

    private static class SplitCapturingFormatReader implements NoConfigFormatReader {

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

    private static class StubStorageProvider implements StorageProvider {
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
