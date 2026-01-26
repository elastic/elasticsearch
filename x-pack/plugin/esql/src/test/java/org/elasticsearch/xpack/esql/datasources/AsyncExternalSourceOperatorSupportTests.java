/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
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
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link AsyncExternalSourceOperatorSupport}.
 * 
 * Tests the shared infrastructure for creating async source operators
 * that read Arrow data from external sources.
 */
public class AsyncExternalSourceOperatorSupportTests extends ESTestCase {

    @BeforeClass
    public static void initArrowAllocator() {
        // Initialize Arrow to use unsafe allocator instead of netty
        // This may fail if JVM doesn't have --add-opens=java.base/java.nio=ALL-UNNAMED
        try {
            ArrowAllocationManagerShim.init();
        } catch (ExceptionInInitializerError | NoClassDefFoundError e) {
            // Arrow initialization failed - tests will be skipped
            assumeTrue("Arrow memory initialization failed - skipping tests. " +
                "Run with --add-opens=java.base/java.nio=ALL-UNNAMED to enable.", false);
        }
    }

    public void testCreateAsyncSourceOperatorWithEmptyData() {
        // Create test components
        Executor executor = Runnable::run; // Direct execution for testing
        
        // Create empty data supplier
        Supplier<CloseableIterable<VectorSchemaRoot>> emptySupplier = () -> new EmptyCloseableIterable();
        
        // Create schema
        Schema schema = new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get())
        );
        
        // Create attributes
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new FieldAttribute(Source.EMPTY, "id", new EsField("id", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE)));
        
        // Mock DriverContext
        DriverContext driverContext = mock(DriverContext.class);
        BlockFactory blockFactory = mock(BlockFactory.class);
        when(driverContext.blockFactory()).thenReturn(blockFactory);
        
        // Track async action registration
        AtomicBoolean asyncActionAdded = new AtomicBoolean(false);
        AtomicBoolean asyncActionRemoved = new AtomicBoolean(false);
        doAnswer(inv -> { asyncActionAdded.set(true); return null; }).when(driverContext).addAsyncAction();
        doAnswer(inv -> { asyncActionRemoved.set(true); return null; }).when(driverContext).removeAsyncAction();
        
        // Create operator
        SourceOperator operator = AsyncExternalSourceOperatorSupport.createAsyncSourceOperator(
            driverContext,
            executor,
            emptySupplier,
            schema,
            attributes,
            1000,
            10
        );
        
        // Verify operator was created
        assertNotNull(operator);
        assertTrue("Async action should be added", asyncActionAdded.get());
        assertTrue("Async action should be removed after completion", asyncActionRemoved.get());
        
        // Verify operator is an AsyncExternalSourceOperator
        assertTrue(operator instanceof AsyncExternalSourceOperator);
        
        // Clean up
        operator.close();
    }

    public void testCreateAsyncSourceOperatorWithData() throws Exception {
        // Create test components
        Executor executor = Runnable::run;
        
        // Create data supplier with one batch
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Supplier<CloseableIterable<VectorSchemaRoot>> dataSupplier = () -> {
            // Create Arrow schema
            List<Field> fields = Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
            );
            org.apache.arrow.vector.types.pojo.Schema arrowSchema = 
                new org.apache.arrow.vector.types.pojo.Schema(fields);
            
            // Create VectorSchemaRoot with test data
            VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
            root.allocateNew();
            
            BigIntVector idVector = (BigIntVector) root.getVector("id");
            VarCharVector nameVector = (VarCharVector) root.getVector("name");
            
            idVector.setSafe(0, 1L);
            nameVector.setSafe(0, "Alice".getBytes(StandardCharsets.UTF_8));
            idVector.setSafe(1, 2L);
            nameVector.setSafe(1, "Bob".getBytes(StandardCharsets.UTF_8));
            
            root.setRowCount(2);
            
            return new SingleBatchIterable(root);
        };
        
        // Create schema
        Schema schema = new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get())
        );
        
        // Create attributes
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new FieldAttribute(Source.EMPTY, "id", new EsField("id", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE)));
        attributes.add(new FieldAttribute(Source.EMPTY, "name", new EsField("name", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)));
        
        // Create real BlockFactory for this test
        BlockFactory blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test-noop"), BigArrays.NON_RECYCLING_INSTANCE);
        
        // Mock DriverContext
        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(blockFactory);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();
        
        // Create operator
        SourceOperator operator = AsyncExternalSourceOperatorSupport.createAsyncSourceOperator(
            driverContext,
            executor,
            dataSupplier,
            schema,
            attributes,
            1000,
            10
        );
        
        // Get output - should have data
        Page page = operator.getOutput();
        assertNotNull("Should have output page", page);
        assertEquals("Should have 2 rows", 2, page.getPositionCount());
        assertEquals("Should have 2 blocks (columns)", 2, page.getBlockCount());
        
        // Clean up
        page.releaseBlocks();
        operator.close();
        allocator.close();
    }

    /**
     * Empty CloseableIterable for testing.
     */
    private static class EmptyCloseableIterable implements CloseableIterable<VectorSchemaRoot> {
        @Override
        public CloseableIterator<VectorSchemaRoot> iterator() {
            return new CloseableIterator<>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public VectorSchemaRoot next() {
                    throw new java.util.NoSuchElementException();
                }

                @Override
                public void close() {
                    // Nothing to close
                }
            };
        }

        @Override
        public void close() {
            // Nothing to close
        }
    }

    /**
     * CloseableIterable that returns a single VectorSchemaRoot batch.
     */
    private static class SingleBatchIterable implements CloseableIterable<VectorSchemaRoot> {
        private final VectorSchemaRoot batch;
        private boolean consumed = false;

        SingleBatchIterable(VectorSchemaRoot batch) {
            this.batch = batch;
        }

        @Override
        public CloseableIterator<VectorSchemaRoot> iterator() {
            return new CloseableIterator<>() {
                @Override
                public boolean hasNext() {
                    return !consumed;
                }

                @Override
                public VectorSchemaRoot next() {
                    if (consumed) {
                        throw new java.util.NoSuchElementException();
                    }
                    consumed = true;
                    return batch;
                }

                @Override
                public void close() {
                    // Don't close the batch here - it's managed by the test
                }
            };
        }

        @Override
        public void close() {
            // Don't close the batch here - it's managed by the test
        }
    }
}
