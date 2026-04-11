/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.datafusion;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.arrow.BooleanArrowBufBlock;
import org.elasticsearch.compute.data.arrow.BytesRefArrowBufBlock;
import org.elasticsearch.compute.data.arrow.DoubleArrowBufBlock;
import org.elasticsearch.compute.data.arrow.FloatArrowBufBlock;
import org.elasticsearch.compute.data.arrow.IntArrowBufBlock;
import org.elasticsearch.compute.data.arrow.LongArrowBufBlock;
import org.elasticsearch.compute.data.arrow.LongMul1kArrowBufBlock;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.OptionalLong;

/**
 * FormatReader backed by a Rust DataFusion native library via JNI.
 * <p>
 * Uses the Arrow C Data Interface for zero-copy transfer of RecordBatches from Rust.
 * Numeric and boolean types are wrapped directly as ESQL blocks via {@link IntArrowBufBlock},
 * {@link LongArrowBufBlock}, etc., with no data copying. String/binary types and timestamps
 * that require unit conversion use the same zero-copy {@link BytesRefArrowBufBlock} or
 * {@link LongMul1kArrowBufBlock} wrappers where possible.
 */
public class DataFusionFormatReader implements FormatReader {

    private static final Logger logger = LogManager.getLogger(DataFusionFormatReader.class);

    private final BlockFactory blockFactory;

    public DataFusionFormatReader(BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
    }

    @Override
    public SourceMetadata metadata(StorageObject object) throws IOException {
        String path = resolveLocalPath(object);
        String[] rawSchema = DataFusionBridge.getSchema(path);
        List<Attribute> attributes = parseSchema(rawSchema);

        long[] stats = DataFusionBridge.getStatistics(path);
        SourceStatistics statistics = null;
        if (stats != null && stats.length >= 2) {
            long totalRows = stats[0];
            long totalBytes = stats[1];
            statistics = new SourceStatistics() {
                @Override
                public OptionalLong rowCount() {
                    return OptionalLong.of(totalRows);
                }

                @Override
                public OptionalLong sizeInBytes() {
                    return OptionalLong.of(totalBytes);
                }
            };
        }

        return new SimpleSourceMetadata(attributes, formatName(), object.path().toString(), statistics, null);
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
        String path = resolveLocalPath(object);
        List<String> projectedColumns = context.projectedColumns();
        int batchSize = context.batchSize();
        int rowLimit = context.rowLimit();

        String[] columns = projectedColumns != null && projectedColumns.isEmpty() == false ? projectedColumns.toArray(new String[0]) : null;
        long limit = rowLimit == FormatReader.NO_LIMIT ? -1 : rowLimit;

        long handle = DataFusionBridge.openReader(path, columns, batchSize, limit);
        return new DataFusionBatchIterator(handle, blockFactory);
    }

    @Override
    public String formatName() {
        return "parquet";
    }

    @Override
    public List<String> fileExtensions() {
        return List.of(".parquet", ".parq");
    }

    @Override
    public void close() throws IOException {}

    private static String resolveLocalPath(StorageObject object) {
        String path = object.path().toString();
        if (path.startsWith("file://")) {
            path = path.substring(7);
        }
        return path;
    }

    private static List<Attribute> parseSchema(String[] rawSchema) {
        List<Attribute> attributes = new ArrayList<>();
        for (int i = 0; i + 2 < rawSchema.length; i += 3) {
            String name = rawSchema[i];
            int typeId = Integer.parseInt(rawSchema[i + 1]);
            DataType esqlType = mapNativeTypeToEsql(typeId);
            attributes.add(new ReferenceAttribute(Source.EMPTY, name, esqlType));
        }
        return attributes;
    }

    private static DataType mapNativeTypeToEsql(int typeId) {
        return switch (typeId) {
            case 1 -> DataType.BOOLEAN;
            case 2 -> DataType.INTEGER;
            case 3 -> DataType.LONG;
            case 4, 5 -> DataType.DOUBLE;
            case 6 -> DataType.KEYWORD;
            case 7 -> DataType.KEYWORD;
            case 8, 9, 10, 11 -> DataType.DATETIME;
            case 12 -> DataType.DOUBLE;
            default -> DataType.UNSUPPORTED;
        };
    }

    /**
     * Iterates over batches from the native DataFusion reader using the Arrow C Data Interface.
     * Each batch is imported as a VectorSchemaRoot, then columns are zero-copy wrapped as ESQL blocks.
     */
    private static class DataFusionBatchIterator implements CloseableIterator<Page> {
        private final long handle;
        private final BlockFactory blockFactory;
        private final BufferAllocator allocator;
        private boolean exhausted = false;
        private Page nextPage;

        DataFusionBatchIterator(long handle, BlockFactory blockFactory) {
            this.handle = handle;
            this.blockFactory = blockFactory;
            this.allocator = new RootAllocator(Long.MAX_VALUE);
        }

        @Override
        public boolean hasNext() {
            if (exhausted) {
                return false;
            }
            if (nextPage != null) {
                return true;
            }
            nextPage = readNextPage();
            if (nextPage == null) {
                exhausted = true;
                return false;
            }
            return true;
        }

        @Override
        public Page next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            Page page = nextPage;
            nextPage = null;
            return page;
        }

        private Page readNextPage() {
            try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator); ArrowArray ffiArray = ArrowArray.allocateNew(allocator)) {

                if (DataFusionBridge.nextBatch(handle, ffiSchema.memoryAddress(), ffiArray.memoryAddress()) == false) {
                    return null;
                }

                try (VectorSchemaRoot root = Data.importVectorSchemaRoot(allocator, ffiArray, ffiSchema, null)) {
                    int rowCount = root.getRowCount();
                    List<FieldVector> vectors = root.getFieldVectors();
                    Block[] blocks = new Block[vectors.size()];
                    try {
                        for (int col = 0; col < vectors.size(); col++) {
                            blocks[col] = wrapZeroCopy(vectors.get(col), rowCount);
                        }
                    } catch (Exception e) {
                        Releasables.closeExpectNoException(blocks);
                        throw new RuntimeException("Failed to wrap Arrow batch as ESQL blocks", e);
                    }
                    return new Page(rowCount, blocks);
                }
            }
        }

        /**
         * Zero-copy wrap of Arrow vectors as ESQL blocks.
         * Numeric, boolean, and string/binary types are wrapped directly.
         * Timestamps in microseconds are wrapped with a x1000 divisor via LongMul1kArrowBufBlock.
         */
        private Block wrapZeroCopy(FieldVector vector, int rowCount) {
            return switch (vector.getMinorType()) {
                case INT -> IntArrowBufBlock.of(vector, blockFactory);
                case BIGINT -> LongArrowBufBlock.of(vector, blockFactory);
                case FLOAT4 -> FloatArrowBufBlock.of(vector, blockFactory);
                case FLOAT8 -> DoubleArrowBufBlock.of(vector, blockFactory);
                case BIT -> BooleanArrowBufBlock.of((BitVector) vector, blockFactory);
                case VARCHAR -> BytesRefArrowBufBlock.of(vector, blockFactory);
                case VARBINARY -> BytesRefArrowBufBlock.of(vector, blockFactory);
                case TIMESTAMPMICRO -> LongMul1kArrowBufBlock.of(vector, blockFactory);
                case TIMESTAMPMICROTZ -> LongMul1kArrowBufBlock.of(vector, blockFactory);
                default -> copyConvert(vector, rowCount);
            };
        }

        private Block copyConvert(FieldVector vector, int rowCount) {
            return switch (vector.getMinorType()) {
                case LARGEVARCHAR, LARGEVARBINARY -> {
                    try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
                        for (int i = 0; i < rowCount; i++) {
                            if (vector.isNull(i)) {
                                builder.appendNull();
                            } else {
                                builder.appendBytesRef(new BytesRef((byte[]) vector.getObject(i)));
                            }
                        }
                        yield builder.build();
                    }
                }
                case TIMESTAMPMILLI -> {
                    try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(rowCount)) {
                        for (int i = 0; i < rowCount; i++) {
                            if (vector.isNull(i)) {
                                builder.appendNull();
                            } else {
                                builder.appendLong((long) vector.getObject(i));
                            }
                        }
                        yield builder.build();
                    }
                }
                default -> {
                    logger.warn("Unsupported Arrow type [{}], returning null block", vector.getMinorType());
                    yield blockFactory.newConstantNullBlock(rowCount);
                }
            };
        }

        @Override
        public void close() {
            DataFusionBridge.closeReader(handle);
        }
    }
}
