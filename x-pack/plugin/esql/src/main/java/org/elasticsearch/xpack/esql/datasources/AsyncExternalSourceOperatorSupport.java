/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Utility class for creating async external source operators with shared infrastructure.
 * Used by both IcebergSourceOperatorFactory and ParquetSourceOperatorFactory.
 * 
 * <p>This class provides the common pattern for:
 * <ul>
 *   <li>Creating an {@link AsyncExternalSourceBuffer} for cross-thread page transfer</li>
 *   <li>Creating an {@link ArrowReaderTask} for background data reading</li>
 *   <li>Managing async action lifecycle with {@link DriverContext}</li>
 *   <li>Submitting the reader task to an executor</li>
 * </ul>
 */
public final class AsyncExternalSourceOperatorSupport {

    private AsyncExternalSourceOperatorSupport() {
        // Utility class - no instantiation
    }

    /**
     * Create an async external source operator with shared infrastructure.
     * 
     * @param driverContext The driver context for block factory and async action tracking
     * @param executor The executor for running background reads
     * @param dataSupplier Supplier that creates the data source (lazily initialized)
     * @param schema The Iceberg schema for type information
     * @param attributes The ESQL attributes (columns to read)
     * @param pageSize Number of rows per page (batch size)
     * @param maxBufferSize Maximum number of pages to buffer
     * @return A configured source operator
     */
    public static SourceOperator createAsyncSourceOperator(
        DriverContext driverContext,
        Executor executor,
        Supplier<CloseableIterable<VectorSchemaRoot>> dataSupplier,
        Schema schema,
        List<Attribute> attributes,
        int pageSize,
        int maxBufferSize
    ) {
        // Create buffer for this operator instance
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferSize);

        // Create background reader task
        // Note: We use the driver's blockFactory directly
        // Cross-thread memory management uses the parent factory pattern
        ArrowReaderTask readerTask = new ArrowReaderTask(
            buffer,
            driverContext.blockFactory(),
            dataSupplier,
            schema,
            attributes,
            pageSize
        );

        // Register async action with driver context for lifecycle tracking
        driverContext.addAsyncAction();

        // Submit reader task to executor
        executor.execute(() -> {
            try {
                readerTask.run();
            } finally {
                // Remove async action when task completes
                driverContext.removeAsyncAction();
            }
        });

        return new AsyncExternalSourceOperator(buffer);
    }
}
