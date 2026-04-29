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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.arrow.ArrowToBlockConverter;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;
import org.elasticsearch.xpack.esql.datasources.arrow.ArrowToEsql;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * FormatReader backed by a Rust parquet-rs native library via JNI.
 * <p>
 * Uses the Arrow C Data Interface for zero-copy transfer of RecordBatches from Rust.
 * Arrow vectors are converted to ESQL blocks via {@link ArrowToBlockConverter}.
 */
public class ParquetRsFormatReader implements FormatReader {

    private static final Logger logger = LogManager.getLogger(ParquetRsFormatReader.class);

    private final BlockFactory blockFactory;
    /**
     * Filter expressions accepted for pushdown by {@link ParquetRsFilterPushdownSupport}. Translated
     * to a native {@code FilterExpr} on every {@link #read} call and freed in the same call, so the
     * reader itself never owns a JNI handle (and therefore cannot leak one).
     */
    private final List<Expression> pushedExpressions;
    private final String configJson;

    public ParquetRsFormatReader(BlockFactory blockFactory) {
        this(blockFactory, List.of(), null);
    }

    private ParquetRsFormatReader(BlockFactory blockFactory, List<Expression> pushedExpressions, String configJson) {
        this.blockFactory = blockFactory;
        this.pushedExpressions = pushedExpressions;
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
            return new ParquetRsFormatReader(blockFactory, pf.pushedExpressions(), configJson);
        }
        if (pushedFilter == null) {
            return pushedExpressions.isEmpty() ? this : new ParquetRsFormatReader(blockFactory, List.of(), configJson);
        }
        // ParquetRsFilterPushdownSupport.pushFilters() only ever produces ParquetRsPushedFilter or null;
        // anything else is a planner/optimizer bug. Fail fast rather than silently dropping the filter,
        // which would return more rows than the user's query asked for.
        throw new IllegalArgumentException("Unexpected pushedFilter type [" + pushedFilter.getClass().getName() + "]");
    }

    @Override
    public FormatReader withConfig(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return this;
        }
        return new ParquetRsFormatReader(blockFactory, pushedExpressions, serializeConfig(config));
    }

    /**
     * Serializes the config map as JSON for consumption by the native side. Values keep their
     * natural JSON types (string, number, boolean, ...) — the Rust {@code StorageConfig} decides
     * how to interpret them rather than having Java silently coerce everything to strings.
     * <p>
     * Entries with null values are dropped: the native side has no representation distinct from
     * "key absent", so an explicit {@code null} cannot mean "reset to default". If a future option
     * needs that semantic it must be encoded out-of-band (e.g. a sentinel value) rather than relying
     * on JSON {@code null}.
     */
    private static String serializeConfig(Map<String, Object> config) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            for (Map.Entry<String, Object> entry : config.entrySet()) {
                if (entry.getValue() == null) {
                    continue;
                }
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize parquet-rs config", e);
        }
    }

    @Override
    public SourceMetadata metadata(StorageObject object) throws IOException {
        NativeLibLoader.ensureLoaded();
        String path = resolveReadPath(object);
        List<Attribute> attributes = importSchema(path);

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
            // Per-tuple defensive parsing: a single malformed stat from the native side must not fail
            // the whole metadata() call, since these statistics are best-effort planner hints.
            try {
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
            } catch (RuntimeException e) {
                final int tupleIdx = i;
                logger.debug(() -> Strings.format("Skipping malformed parquet-rs column statistics tuple at index [%d]", tupleIdx), e);
            }
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
                // Booleans.parseBoolean throws IllegalArgumentException (not NumberFormatException) for
                // anything other than "true"/"false", so widen the catch below to IllegalArgumentException.
                case BOOLEAN -> Booleans.parseBoolean(str);
                case KEYWORD -> str;
                default -> null;
            };
        } catch (IllegalArgumentException e) {
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

        // Native FilterExpr ownership is bounded by this method: build it here, hand it to
        // openReader which clones it internally, then free our copy in the finally. This keeps
        // the reader stateless w.r.t. native memory so it cannot leak across queries / files.
        long filterHandle = 0;
        long readerHandle = 0;
        try {
            if (pushedExpressions.isEmpty() == false) {
                filterHandle = ParquetRsFilterPushdownSupport.translateExpressions(pushedExpressions);
            }
            readerHandle = ParquetRsBridge.openReader(path, columns, batchSize, limit, filterHandle, configJson);
            ParquetRsBatchIterator iterator = new ParquetRsBatchIterator(readerHandle, blockFactory);
            // Ownership of readerHandle has transferred to the iterator's close(); zero our copy
            // so the finally below doesn't double-free it.
            readerHandle = 0;
            return iterator;
        } finally {
            if (filterHandle != 0) {
                ParquetRsBridge.freeExpr(filterHandle);
            }
            if (readerHandle != 0) {
                ParquetRsBridge.closeReader(readerHandle);
            }
        }
    }

    @Override
    public String formatName() {
        return FormatNameResolver.FORMAT_PARQUET_RS;
    }

    @Override
    public List<String> fileExtensions() {
        return List.of();
    }

    @Override
    public void close() {
        // No long-lived native resources at the reader level: the native FilterExpr is allocated
        // and freed inside read(), and the per-batch iterator owns its own openReader handle.
        // Matches ParquetFormatReader#close / OrcFormatReader#close.
    }

    private static String resolveReadPath(StorageObject object) {
        String uri = object.path().toString();
        if (uri.startsWith("file://")) {
            return URI.create(uri).getPath();
        }
        return uri;
    }

    private List<Attribute> importSchema(String path) {
        BufferAllocator allocator = blockFactory.arrowAllocator();
        try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
            ParquetRsBridge.getSchemaFFI(path, configJson, ffiSchema.memoryAddress());
            Schema arrowSchema = Data.importSchema(allocator, ffiSchema, null);
            List<Attribute> attributes = new ArrayList<>(arrowSchema.getFields().size());
            for (Field field : arrowSchema.getFields()) {
                attributes.add(new ReferenceAttribute(Source.EMPTY, field.getName(), ArrowToEsql.dataTypeForField(field)));
            }
            return attributes;
        }
    }

    /**
     * Iterates over batches from the native parquet-rs reader using the Arrow C Data Interface.
     * Each batch is imported as a VectorSchemaRoot, then columns are zero-copy wrapped as ESQL blocks.
     */
    static class ParquetRsBatchIterator implements CloseableIterator<Page>, Describable {
        private final long handle;
        private final BlockFactory blockFactory;
        private final BufferAllocator allocator;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private boolean exhausted = false;
        private Page nextPage;
        // describe() can be called from a different thread than iteration (driver/profiler vs compute);
        // volatile gives us safe publication of the cached plan string.
        private volatile String cachedPlan;

        ParquetRsBatchIterator(long handle, BlockFactory blockFactory) {
            this.handle = handle;
            this.blockFactory = blockFactory;
            this.allocator = blockFactory.arrowAllocator();
        }

        @Override
        public String describe() {
            String plan = cachedPlan;
            if (plan == null) {
                plan = ParquetRsBridge.getReaderPlan(handle);
                cachedPlan = plan;
            }
            return plan;
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
                    } catch (RuntimeException e) {
                        Releasables.closeExpectNoException(blocks);
                        throw e;
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
            // Idempotent: the native side guards on handle != 0 but does not clear it for the caller,
            // so a double-call would double-free the ParquetReaderState. Use an AtomicBoolean so the
            // first close() wins regardless of which thread initiates it.
            if (closed.compareAndSet(false, true)) {
                ParquetRsBridge.closeReader(handle);
            }
        }
    }
}
