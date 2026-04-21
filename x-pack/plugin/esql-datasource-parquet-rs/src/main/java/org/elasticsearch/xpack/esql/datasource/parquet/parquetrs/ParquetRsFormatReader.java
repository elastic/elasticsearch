/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.arrow.ArrowToBlockConverter;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * FormatReader backed by a Rust parquet-rs native library via JNI.
 * <p>
 * Uses the Arrow C Data Interface for zero-copy transfer of RecordBatches from Rust.
 * Arrow vectors are converted to ESQL blocks via {@link ArrowToBlockConverter}.
 */
public class ParquetRsFormatReader implements FormatReader {

    private static final Logger logger = LogManager.getLogger(ParquetRsFormatReader.class);

    private final BlockFactory blockFactory;
    private final long filterHandle;
    private final String configJson;

    public ParquetRsFormatReader(BlockFactory blockFactory) {
        this(blockFactory, 0, null);
    }

    private ParquetRsFormatReader(BlockFactory blockFactory, long filterHandle, String configJson) {
        this.blockFactory = blockFactory;
        this.filterHandle = filterHandle;
        this.configJson = configJson;
    }

    @Override
    public FilterPushdownSupport filterPushdownSupport() {
        return new ParquetRsFilterPushdownSupport();
    }

    @Override
    public AggregatePushdownSupport aggregatePushdownSupport() {
        return new ParquetRsAggregatePushdownSupport();
    }

    @Override
    public FormatReader withPushedFilter(Object pushedFilter) {
        if (pushedFilter instanceof ParquetRsPushedFilter pf) {
            return new ParquetRsFormatReader(blockFactory, pf.exprHandle(), configJson);
        }
        return this;
    }

    @Override
    public FormatReader withConfig(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return this;
        }
        return new ParquetRsFormatReader(blockFactory, filterHandle, serializeConfig(config));
    }

    private static String serializeConfig(Map<String, Object> config) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            if (first == false) {
                sb.append(',');
            }
            sb.append('"')
                .append(escapeJson(entry.getKey()))
                .append("\":\"")
                .append(escapeJson(String.valueOf(entry.getValue())))
                .append('"');
            first = false;
        }
        sb.append('}');
        return sb.toString();
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }

    @Override
    public SourceMetadata metadata(StorageObject object) throws IOException {
        NativeLibLoader.ensureLoaded();
        String path = resolveReadPath(object);
        String[] rawSchema = ParquetRsBridge.getSchema(path, configJson);
        List<Attribute> attributes = parseSchema(rawSchema);

        long[] stats = ParquetRsBridge.getStatistics(path, configJson);
        SourceStatistics statistics = null;
        if (stats != null && stats.length >= 2) {
            long totalRows = stats[0];
            long totalBytes = stats[1];
            Map<String, SourceStatistics.ColumnStatistics> columnStats = parseColumnStatistics(path, attributes);

            statistics = new SourceStatistics() {
                @Override
                public OptionalLong rowCount() {
                    return OptionalLong.of(totalRows);
                }

                @Override
                public OptionalLong sizeInBytes() {
                    return OptionalLong.of(totalBytes);
                }

                @Override
                public Optional<Map<String, ColumnStatistics>> columnStatistics() {
                    return columnStats.isEmpty() ? Optional.empty() : Optional.of(columnStats);
                }
            };
        }

        return new SimpleSourceMetadata(attributes, formatName(), object.path().toString(), statistics, null);
    }

    private Map<String, SourceStatistics.ColumnStatistics> parseColumnStatistics(String path, List<Attribute> attributes) {
        String[] raw = ParquetRsBridge.getColumnStatistics(path, configJson);
        if (raw == null || raw.length == 0) {
            return Map.of();
        }

        Map<String, DataType> typeMap = new HashMap<>();
        for (Attribute attr : attributes) {
            typeMap.put(attr.name(), attr.dataType());
        }

        Map<String, SourceStatistics.ColumnStatistics> result = new HashMap<>();
        for (int i = 0; i + 3 < raw.length; i += 4) {
            String name = raw[i];
            long nullCount = Long.parseLong(raw[i + 1]);
            String minStr = raw[i + 2];
            String maxStr = raw[i + 3];
            DataType dt = typeMap.get(name);

            Object minVal = parseStatValue(minStr, dt);
            Object maxVal = parseStatValue(maxStr, dt);

            result.put(name, new SourceStatistics.ColumnStatistics() {
                @Override
                public OptionalLong nullCount() {
                    return OptionalLong.of(nullCount);
                }

                @Override
                public OptionalLong distinctCount() {
                    return OptionalLong.empty();
                }

                @Override
                public Optional<Object> minValue() {
                    return Optional.ofNullable(minVal);
                }

                @Override
                public Optional<Object> maxValue() {
                    return Optional.ofNullable(maxVal);
                }
            });
        }
        return result;
    }

    private static Object parseStatValue(String str, DataType dt) {
        if (str == null || str.isEmpty() || dt == null) {
            return null;
        }
        try {
            return switch (dt) {
                case INTEGER -> Integer.parseInt(str);
                case LONG, DATETIME -> Long.parseLong(str);
                case DOUBLE -> Double.parseDouble(str);
                case BOOLEAN -> Booleans.parseBoolean(str);
                case KEYWORD -> str;
                default -> null;
            };
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
        NativeLibLoader.ensureLoaded();
        String path = resolveReadPath(object);
        List<String> projectedColumns = context.projectedColumns();
        int batchSize = context.batchSize();
        int rowLimit = context.rowLimit();

        String[] columns = projectedColumns != null && projectedColumns.isEmpty() == false ? projectedColumns.toArray(new String[0]) : null;
        long limit = rowLimit == FormatReader.NO_LIMIT ? -1 : rowLimit;

        long handle = ParquetRsBridge.openReader(path, columns, batchSize, limit, filterHandle, configJson);
        return new ParquetRsBatchIterator(handle, blockFactory);
    }

    @Override
    public String formatName() {
        return EsqlPlugin.FORMAT_PARQUET_RS;
    }

    @Override
    public List<String> fileExtensions() {
        return List.of();
    }

    @Override
    public void close() throws IOException {
        if (filterHandle != 0) {
            ParquetRsBridge.freeExpr(filterHandle);
        }
    }

    private static String resolveReadPath(StorageObject object) {
        String uri = object.path().toString();
        if (uri.startsWith("file://")) {
            return uri.substring(7);
        }
        return uri;
    }

    private static final int TYPE_LIST = 13;

    private static List<Attribute> parseSchema(String[] rawSchema) {
        List<Attribute> attributes = new ArrayList<>();
        for (int i = 0; i + 2 < rawSchema.length; i += 3) {
            String name = rawSchema[i];
            int typeId = Integer.parseInt(rawSchema[i + 1]);
            int elementTypeId = Integer.parseInt(rawSchema[i + 2]);
            DataType esqlType = typeId == TYPE_LIST ? mapNativeTypeToEsql(elementTypeId) : mapNativeTypeToEsql(typeId);
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
     * Iterates over batches from the native parquet-rs reader using the Arrow C Data Interface.
     * Each batch is imported as a VectorSchemaRoot, then columns are zero-copy wrapped as ESQL blocks.
     */
    static class ParquetRsBatchIterator implements CloseableIterator<Page>, Describable {
        private final long handle;
        private final BlockFactory blockFactory;
        private final BufferAllocator allocator;
        private boolean exhausted = false;
        private Page nextPage;
        private String cachedPlan;

        ParquetRsBatchIterator(long handle, BlockFactory blockFactory) {
            this.handle = handle;
            this.blockFactory = blockFactory;
            this.allocator = blockFactory.arrowAllocator();
        }

        @Override
        public String describe() {
            if (cachedPlan == null) {
                cachedPlan = ParquetRsBridge.getReaderPlan(handle);
            }
            return cachedPlan;
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

                if (ParquetRsBridge.nextBatch(handle, ffiSchema.memoryAddress(), ffiArray.memoryAddress()) == false) {
                    return null;
                }

                try (VectorSchemaRoot root = Data.importVectorSchemaRoot(allocator, ffiArray, ffiSchema, null)) {
                    int rowCount = root.getRowCount();
                    List<FieldVector> vectors = root.getFieldVectors();
                    Block[] blocks = new Block[vectors.size()];
                    try {
                        for (int col = 0; col < vectors.size(); col++) {
                            blocks[col] = convertVector(vectors.get(col));
                        }
                    } catch (Exception e) {
                        Releasables.closeExpectNoException(blocks);
                        throw new RuntimeException("Failed to wrap Arrow batch as ESQL blocks", e);
                    }
                    return new Page(rowCount, blocks);
                }
            }
        }

        private Block convertVector(FieldVector vector) {
            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
            if (converter == null) {
                throw new UnsupportedOperationException("Unsupported Arrow type [" + vector.getMinorType() + "]");
            }
            return converter.convert(vector, blockFactory);
        }

        @Override
        public void close() {
            ParquetRsBridge.closeReader(handle);
        }
    }
}
