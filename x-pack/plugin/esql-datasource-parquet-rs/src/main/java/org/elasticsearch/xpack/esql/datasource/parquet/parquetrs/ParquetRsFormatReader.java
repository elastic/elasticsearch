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
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.arrow.ArrowToEsql;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader.SplitRange;
import org.elasticsearch.xpack.esql.datasources.spi.RangeReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * FormatReader backed by a Rust parquet-rs native library via JNI.
 * <p>
 * Uses the Arrow C Data Interface for zero-copy transfer of RecordBatches from Rust.
 * Arrow vectors are converted to ESQL blocks via {@link ArrowToBlockConverter}.
 */
public class ParquetRsFormatReader implements RangeAwareFormatReader {

    private static final Logger logger = LogManager.getLogger(ParquetRsFormatReader.class);

    private final BlockFactory blockFactory;
    /**
     * Filter expressions accepted for pushdown by {@link ParquetRsFilterPushdownSupport}. Translated
     * to a native {@code FilterExpr} on every {@link #read} call and freed in the same call, so the
     * reader itself never owns a JNI handle (and therefore cannot leak one).
     */
    private final List<Expression> pushedExpressions;
    private final String configJson;
    /**
     * Per-file cache of native {@code ArrowReaderMetadata} handles loaded via
     * {@link ParquetRsBridge#loadArrowMetadata}. Eliminates the per-split footer round-trip on remote
     * storage (S3 / GCS / Azure) — the footer is fetched once and reused for all splits of the same
     * file. Handles are freed in {@link #close()}.
     */
    private final ConcurrentHashMap<String, Long> metadataHandleCache = new ConcurrentHashMap<>();

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

    private static Map<String, DataType> columnTypes(List<Attribute> schema) {
        Map<String, DataType> types = new HashMap<>();
        for (Attribute attr : schema) {
            types.put(attr.name(), attr.dataType());
        }
        return types;
    }

    /** Fills split stats from native row-group column tuples; matches {@code ParquetFormatReader#buildRowGroupStats} keys. */
    private void putRowGroupColumnStatistics(Map<String, Object> stats, String[] flat, Map<String, DataType> types) {
        for (int i = 0; i + 4 < flat.length; i += 5) {
            String colName = flat[i];
            if (Strings.isEmpty(colName)) {
                continue;
            }
            String uncompressedBytes = flat[i + 1];
            String ncStr = flat[i + 2];
            String minStr = flat[i + 3];
            String maxStr = flat[i + 4];

            if (Strings.isEmpty(uncompressedBytes) == false) {
                try {
                    stats.put(SourceStatisticsSerializer.columnSizeBytesKey(colName), Long.parseLong(uncompressedBytes));
                } catch (NumberFormatException e) {
                    logger.debug("Ignoring bad chunk uncompressed size stat for column [{}]", colName, e);
                }
            }
            if (Strings.isEmpty(ncStr) == false) {
                try {
                    stats.put(SourceStatisticsSerializer.columnNullCountKey(colName), Long.parseLong(ncStr));
                } catch (NumberFormatException e) {
                    logger.debug("Ignoring bad null count stat for column [{}]", colName, e);
                }
            }

            DataType dt = types.get(colName);
            Object minVal = parseStatValue(minStr, dt);
            if (minVal != null) {
                stats.put(SourceStatisticsSerializer.columnMinKey(colName), minVal);
            }
            Object maxVal = parseStatValue(maxStr, dt);
            if (maxVal != null) {
                stats.put(SourceStatisticsSerializer.columnMaxKey(colName), maxVal);
            }
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

    // --- RangeAwareFormatReader ---

    /** Set to false to disable range-aware splitting and use the single-driver path for benchmarking. */
    static final boolean RANGE_AWARE = true;

    static final long DEFAULT_ROW_GROUP_MACRO_SPLIT_TARGET_BYTES = 32L * 1024 * 1024;

    @Override
    public List<SplitRange> discoverSplitRanges(StorageObject object) throws IOException {
        if (RANGE_AWARE == false) {
            return List.of();
        }
        NativeLibLoader.ensureLoaded();
        String path = resolveReadPath(object);

        Map<String, DataType> columnTypes = columnTypes(importSchema(path));

        Object[] pair = ParquetRsBridge.discoverRowGroupSplits(path, configJson);
        if (pair == null || pair.length != 2) {
            return List.of();
        }
        if ((pair[0] instanceof long[]) == false) {
            return List.of();
        }
        long[] quads = (long[]) pair[0];
        if (quads.length == 0 || quads.length % 4 != 0) {
            return List.of();
        }
        int numRowGroups = quads.length / 4;

        String[][] rgColumnStrings = null;
        if (pair[1] instanceof String[][]) {
            rgColumnStrings = (String[][]) pair[1];
            if (rgColumnStrings.length != numRowGroups) {
                rgColumnStrings = null;
            }
        }

        List<SplitRange> ranges = new ArrayList<>(numRowGroups);
        for (int rg = 0; rg < numRowGroups; rg++) {
            long offset = quads[rg * 4];
            long compressedLen = quads[rg * 4 + 1];
            long rowCount = quads[rg * 4 + 2];
            long rgUncompressed = quads[rg * 4 + 3];

            Map<String, Object> stats = new HashMap<>(8);
            stats.put(SourceStatisticsSerializer.STATS_ROW_COUNT, rowCount);
            stats.put(SourceStatisticsSerializer.STATS_SIZE_BYTES, rgUncompressed);

            if (rgColumnStrings != null) {
                String[] flat = rgColumnStrings[rg];
                if (flat != null) {
                    putRowGroupColumnStatistics(stats, flat, columnTypes);
                }
            }
            ranges.add(new SplitRange(offset, compressedLen, stats));
        }

        if (numRowGroups == 1) {
            return List.copyOf(ranges);
        }

        List<SplitRange> coalesced = coalesceRowGroupRanges(ranges, DEFAULT_ROW_GROUP_MACRO_SPLIT_TARGET_BYTES);
        return coalesced.size() < 2 ? ranges : coalesced;
    }

    @Override
    public CloseableIterator<Page> readRange(StorageObject object, RangeReadContext context) throws IOException {
        NativeLibLoader.ensureLoaded();
        long rangeStart = context.rangeStart();
        long rangeEnd = context.rangeEnd();
        List<String> projectedColumns = context.projectedColumns();
        int batchSize = context.batchSize();
        String path = resolveReadPath(object);
        String[] columns = projectedColumns != null && projectedColumns.isEmpty() == false ? projectedColumns.toArray(new String[0]) : null;
        long metaHandle = metadataHandleCache.computeIfAbsent(path, p -> ParquetRsBridge.loadArrowMetadata(p, configJson));
        long filterHandle = 0;
        long readerHandle = 0;
        try {
            if (pushedExpressions.isEmpty() == false) {
                filterHandle = ParquetRsFilterPushdownSupport.translateExpressions(pushedExpressions);
            }
            readerHandle = ParquetRsBridge.openReaderForRange(
                path,
                columns,
                batchSize,
                -1,
                filterHandle,
                configJson,
                rangeStart,
                rangeEnd,
                metaHandle
            );
            ParquetRsBatchIterator iterator = new ParquetRsBatchIterator(readerHandle, blockFactory);
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

    static List<SplitRange> coalesceRowGroupRanges(List<SplitRange> rowGroupRanges, long targetBytes) {
        if (rowGroupRanges == null || rowGroupRanges.size() <= 1) {
            return List.of();
        }
        if (targetBytes <= 0) {
            return List.copyOf(rowGroupRanges);
        }

        List<SplitRange> sorted = new ArrayList<>(rowGroupRanges);
        sorted.sort(Comparator.comparingLong(SplitRange::offset));

        List<SplitRange> out = new ArrayList<>();
        long groupStart = -1;
        long groupEnd = -1;
        List<Map<String, Object>> pendingStats = new ArrayList<>();

        for (SplitRange range : sorted) {
            long start = range.offset();
            long length = range.length();
            long end = start + length;

            if (length >= targetBytes) {
                if (groupStart >= 0) {
                    out.add(new SplitRange(groupStart, groupEnd - groupStart, SourceStatisticsSerializer.mergeStatistics(pendingStats)));
                    pendingStats.clear();
                }
                out.add(new SplitRange(start, length, range.statistics()));
                groupStart = -1;
                groupEnd = -1;
                continue;
            }

            if (groupStart < 0) {
                groupStart = start;
                groupEnd = end;
            } else {
                groupEnd = Math.max(groupEnd, end);
            }
            pendingStats.add(range.statistics());

            if (groupEnd - groupStart >= targetBytes) {
                out.add(new SplitRange(groupStart, groupEnd - groupStart, SourceStatisticsSerializer.mergeStatistics(pendingStats)));
                groupStart = -1;
                groupEnd = -1;
                pendingStats.clear();
            }
        }

        if (groupStart >= 0) {
            out.add(new SplitRange(groupStart, groupEnd - groupStart, SourceStatisticsSerializer.mergeStatistics(pendingStats)));
        }

        return out;
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
        if (metadataHandleCache.isEmpty() == false) {
            for (long handle : metadataHandleCache.values()) {
                ParquetRsBridge.freeArrowMetadata(handle);
            }
            metadataHandleCache.clear();
        }
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
