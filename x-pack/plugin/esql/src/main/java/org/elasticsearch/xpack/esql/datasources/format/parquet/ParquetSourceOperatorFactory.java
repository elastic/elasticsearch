/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.format.parquet;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceOperatorSupport;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.http.HttpConfiguration;
import org.elasticsearch.xpack.esql.datasources.http.HttpStorageProvider;
import org.elasticsearch.xpack.esql.datasources.s3.S3Configuration;
import org.elasticsearch.xpack.esql.datasources.s3.S3FileIOFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * Factory for creating async source operators for standalone Parquet files.
 * Uses {@link AsyncExternalSourceOperatorSupport} for shared operator creation infrastructure.
 * 
 * <p>This factory creates operators that read data from standalone Parquet files using:
 * <ul>
 *   <li>Parquet's native ParquetFileReader for efficient columnar data reading</li>
 *   <li>Arrow format ({@link VectorSchemaRoot}) for in-memory representation</li>
 *   <li>Background executor thread to avoid blocking the Driver during S3 I/O</li>
 * </ul>
 * 
 * <p>Unlike {@link org.elasticsearch.xpack.esql.datasources.datalake.iceberg.IcebergSourceOperatorFactory},
 * this factory reads Parquet files directly without requiring Iceberg table metadata.
 * It uses Iceberg's S3FileIO for S3 access and Parquet's native reader for data reading.
 * 
 * <p>This implementation avoids using Iceberg's Parquet.read() API which requires parquet-avro
 * dependency. Instead, it uses ParquetFileReader directly with a custom InputFile adapter.
 */
public class ParquetSourceOperatorFactory implements SourceOperator.SourceOperatorFactory {

    private final Executor executor;
    private final String filePath;
    private final S3Configuration s3Config;
    private final Schema schema;
    private final List<Attribute> attributes;
    private final int pageSize;
    private final int maxBufferSize;

    /**
     * @param executor Executor for running background S3/Parquet reads
     * @param filePath Path to the Parquet file (S3 URI)
     * @param s3Config S3 configuration (credentials, endpoint, region)
     * @param schema Iceberg schema (derived from Parquet file schema)
     * @param attributes ESQL attributes (columns to read)
     * @param pageSize Number of rows per page (batch size)
     * @param maxBufferSize Maximum number of pages to buffer
     */
    public ParquetSourceOperatorFactory(
        Executor executor,
        String filePath,
        S3Configuration s3Config,
        Schema schema,
        List<Attribute> attributes,
        int pageSize,
        int maxBufferSize
    ) {
        this.executor = executor;
        this.filePath = filePath;
        this.s3Config = s3Config;
        this.schema = schema;
        this.attributes = attributes;
        this.pageSize = pageSize;
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public SourceOperator get(DriverContext driverContext) {
        Supplier<CloseableIterable<VectorSchemaRoot>> dataSupplier = this::createParquetReader;

        return AsyncExternalSourceOperatorSupport.createAsyncSourceOperator(
            driverContext,
            executor,
            dataSupplier,
            schema,
            attributes,
            pageSize,
            maxBufferSize
        );
    }

    /**
     * Create a reader for a standalone Parquet file.
     * Supports both S3 and HTTP/HTTPS URLs.
     * 
     * <p>For S3 URLs (s3://), uses Parquet's native ParquetFileReader with Iceberg's S3FileIO.
     * <p>For HTTP/HTTPS URLs, uses HttpStorageProvider with ParquetStorageObjectAdapter.
     * 
     * <p>The reader produces {@link VectorSchemaRoot} batches by:
     * <ol>
     *   <li>Opening the Parquet file via the appropriate storage provider</li>
     *   <li>Reading data using Parquet's native reader (avoiding Iceberg's Parquet.read() which requires Avro)</li>
     *   <li>Converting records to Arrow VectorSchemaRoot format</li>
     * </ol>
     * 
     * @return CloseableIterable of VectorSchemaRoot batches
     */
    private CloseableIterable<VectorSchemaRoot> createParquetReader() {
        try {
            // Detect the URL scheme to determine which storage provider to use
            String scheme = detectScheme(filePath);
            
            org.apache.parquet.io.InputFile parquetInputFile;
            Closeable storageResource; // Resource to close when done (S3FileIO or HttpStorageProvider)
            
            if (scheme.equals("http") || scheme.equals("https")) {
                // For HTTP/HTTPS URLs, use HttpStorageProvider with ParquetStorageObjectAdapter
                // Create a dedicated executor for HTTP operations
                ExecutorService httpExecutor = Executors.newSingleThreadExecutor(r -> {
                    Thread t = new Thread(r, "parquet-http-reader");
                    t.setDaemon(true);
                    return t;
                });
                
                HttpConfiguration httpConfig = HttpConfiguration.defaults();
                HttpStorageProvider httpProvider = new HttpStorageProvider(httpConfig, httpExecutor);
                StoragePath storagePath = StoragePath.of(filePath);
                StorageObject storageObject = httpProvider.newObject(storagePath);
                
                parquetInputFile = new ParquetStorageObjectAdapter(storageObject);
                // Create a composite closeable that closes both the provider and executor
                storageResource = () -> {
                    try {
                        httpProvider.close();
                    } finally {
                        httpExecutor.shutdown();
                    }
                };
            } else {
                // For S3 URLs (s3://, s3a://, s3n://), use S3FileIO
                S3FileIO fileIO = S3FileIOFactory.create(s3Config);
                InputFile inputFile = fileIO.newInputFile(filePath);
                parquetInputFile = new IcebergInputFileAdapter(inputFile);
                storageResource = fileIO;
            }

            // Build ParquetReadOptions - no special options needed for data reading
            ParquetReadOptions options = ParquetReadOptions.builder().build();

            // Open the Parquet file reader
            ParquetFileReader reader = ParquetFileReader.open(parquetInputFile, options);

            // Read the actual Parquet schema from the file and convert to Iceberg schema
            // This ensures we use the correct types (e.g., FLOAT vs DOUBLE) from the Parquet file
            MessageType parquetSchema = reader.getFileMetaData().getSchema();
            Schema actualSchema = org.apache.iceberg.parquet.ParquetSchemaUtil.convert(parquetSchema);

            // Create a buffer allocator for Arrow memory management
            // Note: ArrowAllocationManagerShim.init() is called in static initializer
            BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

            // Return an iterable that reads row groups and converts to VectorSchemaRoot
            // Use the actual schema from the Parquet file, not the reconstructed one
            return new ParquetRowGroupIterable(reader, actualSchema, attributes, allocator, pageSize, storageResource);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Parquet data reader for: " + filePath, e);
        }
    }
    
    /**
     * Detect the URI scheme from a file path.
     * 
     * @param path the file path
     * @return the scheme (e.g., "s3", "http", "https"), or "s3" as default
     */
    private String detectScheme(String path) {
        if (path == null) {
            return "s3"; // Default
        }
        
        int colonIndex = path.indexOf(':');
        if (colonIndex > 0) {
            return path.substring(0, colonIndex).toLowerCase(Locale.ROOT);
        }
        
        return "s3"; // Default for paths without explicit scheme
    }

    @Override
    public String describe() {
        return "ParquetSourceOperator[path=" + filePath + ", pageSize=" + pageSize + ", bufferSize=" + maxBufferSize + "]";
    }

    /**
     * Adapter that wraps Iceberg's InputFile to implement Parquet's InputFile interface.
     * This allows using Iceberg's S3FileIO with Parquet's ParquetFileReader.
     */
    private static class IcebergInputFileAdapter implements org.apache.parquet.io.InputFile {
        private final InputFile icebergInputFile;

        IcebergInputFileAdapter(InputFile icebergInputFile) {
            this.icebergInputFile = icebergInputFile;
        }

        @Override
        public long getLength() throws IOException {
            return icebergInputFile.getLength();
        }

        @Override
        public SeekableInputStream newStream() throws IOException {
            return new DelegatingSeekableInputStream(icebergInputFile.newStream()) {
                @Override
                public long getPos() throws IOException {
                    return ((org.apache.iceberg.io.SeekableInputStream) getStream()).getPos();
                }

                @Override
                public void seek(long newPos) throws IOException {
                    ((org.apache.iceberg.io.SeekableInputStream) getStream()).seek(newPos);
                }
            };
        }
    }

    /**
     * Iterable that reads Parquet row groups and converts to VectorSchemaRoot batches.
     */
    private static class ParquetRowGroupIterable implements CloseableIterable<VectorSchemaRoot> {
        private final ParquetFileReader reader;
        private final Schema schema;
        private final List<Attribute> attributes;
        private final BufferAllocator allocator;
        private final int batchSize;
        private final Closeable storageResource;

        ParquetRowGroupIterable(
            ParquetFileReader reader,
            Schema schema,
            List<Attribute> attributes,
            BufferAllocator allocator,
            int batchSize,
            Closeable storageResource
        ) {
            this.reader = reader;
            this.schema = schema;
            this.attributes = attributes;
            this.allocator = allocator;
            this.batchSize = batchSize;
            this.storageResource = storageResource;
        }

        @Override
        public CloseableIterator<VectorSchemaRoot> iterator() {
            return new ParquetBatchIterator(reader, schema, attributes, allocator, batchSize, storageResource);
        }

        @Override
        public void close() throws IOException {
            try {
                reader.close();
            } finally {
                try {
                    allocator.close();
                } finally {
                    if (storageResource != null) {
                        storageResource.close();
                    }
                }
            }
        }
    }

    /**
     * Iterator that reads Parquet data and produces Arrow VectorSchemaRoot batches.
     */
    private static class ParquetBatchIterator implements CloseableIterator<VectorSchemaRoot> {
        private static final Logger logger = LogManager.getLogger(ParquetBatchIterator.class);
        
        private final ParquetFileReader reader;
        private final Schema schema;
        private final List<Attribute> attributes;
        private final BufferAllocator allocator;
        private final int batchSize;
        private final Closeable storageResource;
        private final MessageType parquetSchema;
        private final MessageColumnIO columnIO;

        private PageReadStore currentRowGroup;
        private RecordReader<Group> recordReader;
        private long rowsRemainingInGroup;
        private boolean exhausted = false;

        ParquetBatchIterator(
            ParquetFileReader reader,
            Schema schema,
            List<Attribute> attributes,
            BufferAllocator allocator,
            int batchSize,
            Closeable storageResource
        ) {
            this.reader = reader;
            this.schema = schema;
            this.attributes = attributes;
            this.allocator = allocator;
            this.batchSize = batchSize;
            this.storageResource = storageResource;
            this.parquetSchema = reader.getFileMetaData().getSchema();
            this.columnIO = new ColumnIOFactory().getColumnIO(parquetSchema);
        }

        @Override
        public boolean hasNext() {
            if (exhausted) {
                return false;
            }
            // Check if we have rows in current group or can read more groups
            if (rowsRemainingInGroup > 0) {
                return true;
            }
            // Try to read next row group
            try {
                currentRowGroup = reader.readNextRowGroup();
                if (currentRowGroup == null) {
                    exhausted = true;
                    return false;
                }
                rowsRemainingInGroup = currentRowGroup.getRowCount();
                recordReader = columnIO.getRecordReader(currentRowGroup, new GroupRecordConverter(parquetSchema));
                return rowsRemainingInGroup > 0;
            } catch (IOException e) {
                throw new RuntimeException("Failed to read Parquet row group", e);
            }
        }

        @Override
        public VectorSchemaRoot next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            // Create Arrow schema from Iceberg schema
            org.apache.arrow.vector.types.pojo.Schema arrowSchema = 
                org.apache.iceberg.arrow.ArrowSchemaUtil.convert(schema);
            
            // Create VectorSchemaRoot with the Arrow schema
            VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
            
            try {
                // Read records up to batch size
                List<Group> batch = new ArrayList<>(batchSize);
                int rowsToRead = (int) Math.min(batchSize, rowsRemainingInGroup);
                
                for (int i = 0; i < rowsToRead; i++) {
                    Group group = recordReader.read();
                    if (group != null) {
                        batch.add(group);
                        rowsRemainingInGroup--;
                    }
                }

                if (batch.isEmpty()) {
                    root.close();
                    throw new NoSuchElementException("No more records");
                }

                // Allocate vectors and populate with data
                root.allocateNew();
                populateVectorSchemaRoot(root, batch);
                root.setRowCount(batch.size());

                return root;
            } catch (Exception e) {
                root.close();
                throw new RuntimeException("Failed to create VectorSchemaRoot batch", e);
            }
        }

        /**
         * Populate a VectorSchemaRoot with data from Parquet Groups.
         */
        private void populateVectorSchemaRoot(VectorSchemaRoot root, List<Group> batch) {
            List<FieldVector> vectors = root.getFieldVectors();
            List<org.apache.iceberg.types.Types.NestedField> fields = schema.columns();

            for (int col = 0; col < fields.size(); col++) {
                FieldVector vector = vectors.get(col);
                org.apache.iceberg.types.Types.NestedField field = fields.get(col);
                String fieldName = field.name();
                populateVector(vector, batch, fieldName, field.type());
            }
        }

        /**
         * Populate a single Arrow FieldVector with values from Parquet Groups.
         */
        private void populateVector(
            FieldVector vector,
            List<Group> batch,
            String fieldName,
            org.apache.iceberg.types.Type type
        ) {
            for (int row = 0; row < batch.size(); row++) {
                Group group = batch.get(row);
                setVectorValue(vector, row, group, fieldName, type);
            }
        }

        /**
         * Set a single value in an Arrow vector from a Parquet Group.
         */
        private void setVectorValue(
            FieldVector vector, 
            int index, 
            Group group, 
            String fieldName, 
            org.apache.iceberg.types.Type type
        ) {
            try {
                // Check if field exists and has a value
                // Note: For field names with dots (e.g., "height.float"), we need to find the field
                // by iterating through the schema, as getFieldIndex() may not handle dots correctly
                int fieldIndex = -1;
                org.apache.parquet.schema.Type parquetFieldType = null;
                
                // Try to find the field by name
                // Iterate through all fields to find the one with the matching name
                // This is necessary because getFieldIndex() doesn't handle field names with dots correctly
                for (int i = 0; i < group.getType().getFieldCount(); i++) {
                    org.apache.parquet.schema.Type fieldType = group.getType().getType(i);
                    if (fieldType.getName().equals(fieldName)) {
                        fieldIndex = i;
                        parquetFieldType = fieldType;
                        break;
                    }
                }
                
                // If field not found, return (leave as null)
                if (fieldIndex == -1) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Field '{}' not found in Parquet schema", fieldName);
                    }
                    return;
                }
                
                int repetitionCount = group.getFieldRepetitionCount(fieldIndex);
                if (repetitionCount == 0) {
                    // Null value - Arrow vectors handle nulls via validity buffer
                    return;
                }

                switch (type.typeId()) {
                    case BOOLEAN -> {
                        boolean value = group.getBoolean(fieldName, 0);
                        ((org.apache.arrow.vector.BitVector) vector).setSafe(index, value ? 1 : 0);
                    }
                    case INTEGER -> {
                        int value = group.getInteger(fieldName, 0);
                        ((org.apache.arrow.vector.IntVector) vector).setSafe(index, value);
                    }
                    case LONG -> {
                        long value = group.getLong(fieldName, 0);
                        ((org.apache.arrow.vector.BigIntVector) vector).setSafe(index, value);
                    }
                    case FLOAT -> {
                        float value = group.getFloat(fieldName, 0);
                        // ArrowSchemaUtil.convert() creates Float8Vector (DOUBLE) for Iceberg FLOAT types
                        // because ESQL maps FLOAT to DOUBLE. We need to handle both Float4Vector and Float8Vector.
                        if (vector instanceof org.apache.arrow.vector.Float8Vector f8v) {
                            // Iceberg's ArrowSchemaUtil converts FLOAT to Float8Vector (DOUBLE)
                            f8v.setSafe(index, (double) value);
                        } else if (vector instanceof org.apache.arrow.vector.Float4Vector f4v) {
                            // Direct Float4Vector (less common path)
                            f4v.setSafe(index, value);
                        }
                    }
                    case DOUBLE -> {
                        double value = group.getDouble(fieldName, 0);
                        ((org.apache.arrow.vector.Float8Vector) vector).setSafe(index, value);
                    }
                    case STRING -> {
                        String value = group.getString(fieldName, 0);
                        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                        ((org.apache.arrow.vector.VarCharVector) vector).setSafe(index, bytes);
                    }
                    case BINARY -> {
                        org.apache.parquet.io.api.Binary binary = group.getBinary(fieldName, 0);
                        byte[] bytes = binary.getBytes();
                        ((org.apache.arrow.vector.VarBinaryVector) vector).setSafe(index, bytes);
                    }
                    case DATE -> {
                        // Parquet stores dates as days since epoch
                        int value = group.getInteger(fieldName, 0);
                        ((org.apache.arrow.vector.DateDayVector) vector).setSafe(index, value);
                    }
                    case TIME -> {
                        // Parquet stores time as microseconds since midnight
                        long value = group.getLong(fieldName, 0);
                        ((org.apache.arrow.vector.TimeMicroVector) vector).setSafe(index, value);
                    }
                    case TIMESTAMP -> {
                        // Parquet can store timestamps in different formats. We need to check the Parquet schema
                        // to determine the physical type and logical type annotation.
                        // Note: parquetFieldType is already set from the field lookup above
                        long micros;
                        
                        if (parquetFieldType.asPrimitiveType().getPrimitiveTypeName() == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32) {
                            // INT32 physical type - likely DATE stored as days since epoch
                            int days = group.getInteger(fieldName, 0);
                            // Convert days to microseconds
                            micros = days * 86400L * 1000000L;
                        } else {
                            // INT64 or INT96 physical type
                            long value = group.getLong(fieldName, 0);
                            
                            // Check the logical type to determine the unit
                            org.apache.parquet.schema.LogicalTypeAnnotation logicalType = parquetFieldType.getLogicalTypeAnnotation();
                            if (logicalType instanceof org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampType) {
                                // Get the time unit from the logical type annotation
                                switch (timestampType.getUnit()) {
                                    case MILLIS:
                                        micros = value * 1000L; // milliseconds to microseconds
                                        break;
                                    case MICROS:
                                        micros = value; // already in microseconds
                                        break;
                                    case NANOS:
                                        micros = value / 1000L; // nanoseconds to microseconds
                                        break;
                                    default:
                                        micros = value; // assume microseconds as fallback
                                }
                            } else {
                                // No logical type annotation or not a timestamp - assume microseconds
                                micros = value;
                            }
                        }
                        
                        // Handle both timestamp with timezone (TimeStampMicroTZVector) and without (TimeStampMicroVector)
                        // ArrowSchemaUtil.convert() creates TimeStampMicroTZVector for Iceberg's timestamptz type
                        if (vector instanceof org.apache.arrow.vector.TimeStampMicroTZVector tzVector) {
                            tzVector.setSafe(index, micros);
                        } else if (vector instanceof org.apache.arrow.vector.TimeStampMicroVector tsVector) {
                            tsVector.setSafe(index, micros);
                        }
                    }
                    default -> {
                        // For unsupported types, leave as null
                        // Complex types (LIST, MAP, STRUCT) are not yet supported
                    }
                }
            } catch (ClassCastException e) {
                // Type mismatch between Arrow vector and Parquet data - log at warn level
                // This indicates a schema mismatch that should be investigated
                logger.warn("Type mismatch reading field '{}' from Parquet: {}", fieldName, e.getMessage());
                // Leave value as null rather than failing the entire query
            }
            // Note: Other exceptions (NPE, OOM, etc.) will propagate naturally
            // Field-not-found and null values are handled explicitly above
        }

        @Override
        public void close() throws IOException {
            try {
                reader.close();
            } finally {
                try {
                    allocator.close();
                } finally {
                    if (storageResource != null) {
                        storageResource.close();
                    }
                }
            }
        }
    }
}
