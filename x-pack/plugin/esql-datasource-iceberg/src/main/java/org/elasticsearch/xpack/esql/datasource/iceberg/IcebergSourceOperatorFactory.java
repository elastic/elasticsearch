/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.iceberg;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.arrow.vectorized.ArrowReader;
import org.apache.iceberg.arrow.vectorized.ColumnVector;
import org.apache.iceberg.arrow.vectorized.ColumnarBatch;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Factory for creating async source operators for Iceberg tables.
 *
 * <p>This factory creates operators that read data from Iceberg tables or Parquet files using:
 * <ul>
 *   <li>Iceberg's {@link ArrowReader} for efficient vectorized columnar data reading</li>
 *   <li>Arrow format ({@link VectorSchemaRoot}) for in-memory representation</li>
 *   <li>Background executor thread to avoid blocking the Driver during S3 I/O</li>
 * </ul>
 *
 * <p>Each operator gets:
 * <ul>
 *   <li>A shared buffer for pages</li>
 *   <li>A background reader task that fills the buffer</li>
 *   <li>An executor to run the background task</li>
 * </ul>
 */
public class IcebergSourceOperatorFactory implements SourceOperator.SourceOperatorFactory {

    private final Executor executor;
    private final String tablePath;
    private final S3Configuration s3Config;
    private final String sourceType;
    private final Expression filter;
    private final Schema schema;
    private final List<Attribute> attributes;
    private final int pageSize;
    private final int maxBufferSize;

    /**
     * @param executor Executor for running background S3/Iceberg reads
     * @param tablePath Path to Iceberg table or Parquet file
     * @param s3Config S3 configuration (credentials, endpoint, region)
     * @param sourceType Type of source ("iceberg" or "parquet")
     * @param filter Iceberg filter expression (nullable)
     * @param schema Iceberg schema
     * @param attributes ESQL attributes (schema)
     * @param pageSize Number of rows per page (batch size for Vectorized Reader)
     * @param maxBufferSize Maximum number of pages to buffer
     */
    public IcebergSourceOperatorFactory(
        Executor executor,
        String tablePath,
        S3Configuration s3Config,
        String sourceType,
        Expression filter,
        Schema schema,
        List<Attribute> attributes,
        int pageSize,
        int maxBufferSize
    ) {
        this.executor = executor;
        this.tablePath = tablePath;
        this.s3Config = s3Config;
        this.sourceType = sourceType;
        this.filter = filter;
        this.schema = schema;
        this.attributes = attributes;
        this.pageSize = pageSize;
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public SourceOperator get(DriverContext driverContext) {
        // TODO: Implement async source operator creation
        // This requires integration with the ESQL async operator infrastructure.
        // For now, the Iceberg plugin provides TableCatalog functionality for schema discovery.
        // Full data reading support will be added in a future iteration.
        throw new UnsupportedOperationException(
            "Direct Iceberg source operator creation is not yet supported. "
                + "Use the generic async operator factory via OperatorFactoryRegistry."
        );
    }

    /**
     * Create a data supplier that provides Iceberg data using Vectorized Reader with Arrow format.
     * This supplier lazily initializes the Iceberg table scan and reader.
     */
    private Supplier<CloseableIterable<VectorSchemaRoot>> createDataSupplier() {
        return () -> {
            try {
                return createIcebergTableReader();
            } catch (Exception e) {
                throw new RuntimeException("Failed to create Iceberg data reader for: " + tablePath, e);
            }
        };
    }

    /**
     * Create a reader for an Iceberg table using Iceberg's ArrowReader.
     * Returns VectorSchemaRoot batches by converting ColumnarBatch from ArrowReader.
     */
    private CloseableIterable<VectorSchemaRoot> createIcebergTableReader() throws Exception {
        // Recreate the table from metadata location
        // Note: We need to recreate it here because we can't keep FileIO open across the entire query
        IcebergTableMetadata metadata = IcebergCatalogAdapter.resolveTable(tablePath, s3Config);

        // Recreate the Table object for scanning
        org.apache.iceberg.aws.s3.S3FileIO fileIO = S3FileIOFactory.create(s3Config);
        org.apache.iceberg.StaticTableOperations ops = new org.apache.iceberg.StaticTableOperations(metadata.metadataLocation(), fileIO);
        Table table = new org.apache.iceberg.BaseTable(ops, tablePath);

        // Use planWith() to set a direct (current-thread) executor, avoiding the default ThreadPool/shutdown hooks
        TableScan scan = table.newScan().planWith(org.elasticsearch.common.util.concurrent.EsExecutors.DIRECT_EXECUTOR_SERVICE);

        if (filter != null) {
            scan = scan.filter(filter);
        }

        // Project only the columns we need based on attributes
        if (attributes != null && attributes.isEmpty() == false) {
            List<String> columnNames = new ArrayList<>();
            for (Attribute attr : attributes) {
                columnNames.add(attr.name());
            }
            scan = scan.select(columnNames);
        }

        // Get the scan tasks - use planFiles() to get individual file tasks
        CloseableIterable<org.apache.iceberg.FileScanTask> fileTasks = scan.planFiles();

        // Convert FileScanTasks to CombinedScanTasks (each file as its own combined task)
        CloseableIterable<CombinedScanTask> tasks = org.apache.iceberg.io.CloseableIterable.transform(
            fileTasks,
            fileTask -> new org.apache.iceberg.BaseCombinedScanTask(java.util.Collections.singletonList(fileTask))
        );

        // Create ArrowReader with the specified page size (batch size)
        // reuseContainers=false for safety (true could reuse buffers across batches)
        ArrowReader arrowReader = new ArrowReader(scan, pageSize, /* reuseContainers */ false);

        // Create a buffer allocator for Arrow memory management
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

        // Open the reader to get an iterator of ColumnarBatch
        CloseableIterator<ColumnarBatch> batchIterator = arrowReader.open(tasks);

        // Wrap the ColumnarBatch iterator to return VectorSchemaRoot
        return new ColumnarBatchToVectorSchemaRootIterable(batchIterator, allocator, arrowReader);
    }

    @Override
    public String describe() {
        return "IcebergSourceOperator[path=" + tablePath + ", pageSize=" + pageSize + ", bufferSize=" + maxBufferSize + "]";
    }

    /**
     * Adapter that converts Iceberg's ColumnarBatch iterator to VectorSchemaRoot iterator.
     * This bridges between Iceberg's vectorized reader format and the Arrow format expected by ESQL.
     */
    private static class ColumnarBatchToVectorSchemaRootIterable implements CloseableIterable<VectorSchemaRoot> {
        private final CloseableIterator<ColumnarBatch> batchIterator;
        private final BufferAllocator allocator;
        private final ArrowReader arrowReader;

        ColumnarBatchToVectorSchemaRootIterable(
            CloseableIterator<ColumnarBatch> batchIterator,
            BufferAllocator allocator,
            ArrowReader arrowReader
        ) {
            this.batchIterator = batchIterator;
            this.allocator = allocator;
            this.arrowReader = arrowReader;
        }

        @Override
        public CloseableIterator<VectorSchemaRoot> iterator() {
            return new CloseableIterator<VectorSchemaRoot>() {
                @Override
                public boolean hasNext() {
                    return batchIterator.hasNext();
                }

                @Override
                public VectorSchemaRoot next() {
                    if (hasNext() == false) {
                        throw new NoSuchElementException();
                    }

                    ColumnarBatch batch = batchIterator.next();
                    return convertColumnarBatchToVectorSchemaRoot(batch);
                }

                @Override
                public void close() throws IOException {
                    try {
                        batchIterator.close();
                    } finally {
                        try {
                            arrowReader.close();
                        } finally {
                            allocator.close();
                        }
                    }
                }
            };
        }

        @Override
        public void close() throws IOException {
            iterator().close();
        }

        /**
         * Convert a ColumnarBatch (Iceberg's format) to VectorSchemaRoot (Arrow's format).
         * The ColumnarBatch wraps Arrow FieldVectors via ColumnVector wrappers.
         */
        private VectorSchemaRoot convertColumnarBatchToVectorSchemaRoot(ColumnarBatch batch) {
            int numRows = batch.numRows();
            int numColumns = batch.numCols();

            // Extract the underlying Arrow FieldVectors from the ColumnVector wrappers
            List<FieldVector> fieldVectors = new ArrayList<>(numColumns);
            for (int col = 0; col < numColumns; col++) {
                ColumnVector columnVector = batch.column(col);
                // Get the underlying Arrow FieldVector from the ColumnVector wrapper
                FieldVector fieldVector = columnVector.getFieldVector();
                fieldVectors.add(fieldVector);
            }

            // Create VectorSchemaRoot from the field vectors
            // Note: We pass the vectors directly; they are already allocated and populated
            return new VectorSchemaRoot(fieldVectors);
        }
    }

}
