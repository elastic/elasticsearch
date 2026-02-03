/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.BlockFactory;
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

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
        when(storageProvider.newObject(org.mockito.ArgumentMatchers.any())).thenReturn(storageObject);

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
        when(storageProvider.newObject(org.mockito.ArgumentMatchers.any())).thenReturn(storageObject);

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
            // Return empty iterator
            return new CloseableIterator<>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public Page next() {
                    throw new java.util.NoSuchElementException();
                }

                @Override
                public void close() {
                    // Nothing to close
                }
            };
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
        public void close() {
            // Nothing to close
        }
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
            // Return empty iterator
            return new CloseableIterator<>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public Page next() {
                    throw new java.util.NoSuchElementException();
                }

                @Override
                public void close() {
                    // Nothing to close
                }
            };
        }

        @Override
        public void readAsync(
            StorageObject object,
            List<String> projectedColumns,
            int batchSize,
            Executor executor,
            ActionListener<CloseableIterator<Page>> listener
        ) {
            // Native async implementation - execute on provided executor
            executor.execute(() -> {
                listener.onResponse(new CloseableIterator<>() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public Page next() {
                        throw new java.util.NoSuchElementException();
                    }

                    @Override
                    public void close() {
                        // Nothing to close
                    }
                });
            });
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
        public void close() {
            // Nothing to close
        }
    }
}
