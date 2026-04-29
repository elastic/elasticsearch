/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnBlockConversions;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FileLayout;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader.SplitRange;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * FormatReader implementation for Parquet files.
 *
 * <p>Uses Parquet's native ParquetFileReader with our StorageObject abstraction.
 * Produces ESQL Page batches directly without requiring Arrow as an intermediate format.
 *
 * <p>Key features:
 * <ul>
 *   <li>Works with any StorageProvider (HTTP, S3, local)</li>
 *   <li>Efficient columnar reading with column projection</li>
 *   <li>No Hadoop dependencies in the core path</li>
 *   <li>Direct conversion from Parquet to ESQL blocks</li>
 * </ul>
 */
public class ParquetFormatReader implements RangeAwareFormatReader {

    private static final Logger logger = LogManager.getLogger(ParquetFormatReader.class);

    private final BlockFactory blockFactory;
    private final FilterCompat.Filter pushedFilter;
    private final ParquetPushedExpressions pushedExpressions;
    private final boolean forceBaselinePath;
    private final boolean optimizedReader;
    // Shared across all iterators created by this reader: holds lazy decompressor instances and
    // pays the per-codec init cost once. The factory is stateless across files/row groups.
    private final PlainCompressionCodecFactory codecFactory = new PlainCompressionCodecFactory();

    static final long DEFAULT_ROW_GROUP_MACRO_SPLIT_TARGET_BYTES = 32L * 1024 * 1024;

    static final String CONFIG_OPTIMIZED_READER = "optimized_reader";

    public ParquetFormatReader(BlockFactory blockFactory) {
        this(blockFactory, FilterCompat.NOOP, null, false, true);
    }

    ParquetFormatReader(BlockFactory blockFactory, boolean optimizedReader) {
        this(blockFactory, FilterCompat.NOOP, null, false, optimizedReader);
    }

    private ParquetFormatReader(
        BlockFactory blockFactory,
        FilterCompat.Filter pushedFilter,
        ParquetPushedExpressions pushedExpressions,
        boolean forceBaselinePath,
        boolean optimizedReader
    ) {
        this.blockFactory = blockFactory;
        this.pushedFilter = pushedFilter;
        this.pushedExpressions = pushedExpressions;
        this.forceBaselinePath = forceBaselinePath;
        this.optimizedReader = optimizedReader;
    }

    /**
     * Returns a reader that always uses the row-at-a-time {@code ColumnReader} path,
     * bypassing {@link PageColumnReader}. Package-private; intended for correctness
     * testing against the optimized path.
     */
    ParquetFormatReader withBaselinePath() {
        return new ParquetFormatReader(blockFactory, pushedFilter, pushedExpressions, true, optimizedReader);
    }

    @Override
    public ParquetFormatReader withPushedFilter(Object pushedFilter) {
        if (pushedFilter instanceof FilterCompat.Filter filter) {
            return new ParquetFormatReader(blockFactory, filter, null, forceBaselinePath, optimizedReader);
        }
        if (pushedFilter instanceof ParquetPushedExpressions exprs) {
            return new ParquetFormatReader(blockFactory, FilterCompat.NOOP, exprs, forceBaselinePath, optimizedReader);
        }
        return this;
    }

    @Override
    public FormatReader withConfig(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return this;
        }
        boolean newOptimized = parseBooleanConfig(config, CONFIG_OPTIMIZED_READER, optimizedReader);
        if (newOptimized == optimizedReader) {
            return this;
        }
        return new ParquetFormatReader(blockFactory, pushedFilter, pushedExpressions, forceBaselinePath, newOptimized);
    }

    private static boolean parseBooleanConfig(Map<String, Object> config, String key, boolean defaultValue) {
        Object value = config.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean b) {
            return b;
        }
        return org.elasticsearch.core.Booleans.parseBoolean(value.toString());
    }

    @Override
    public FilterPushdownSupport filterPushdownSupport() {
        return new ParquetFilterPushdownSupport();
    }

    /**
     * Creates a {@link PlainParquetReadOptions.Builder} initialized with an allocator backed by the
     * block factory's circuit breaker. Uses the Hadoop-free builder that bypasses
     * {@link ParquetReadOptions.Builder}'s unconditional loading of {@code ParquetInputFormat}
     * (which extends Hadoop's {@code FileInputFormat}) and {@code HadoopCodecs}.
     */
    private PlainParquetReadOptions.Builder readOptionsBuilder() {
        // parquet-mr defaults useColumnIndexFilter=true (since 1.12.0), so when a FilterPredicate
        // is set via withRecordFilter, page-index filtering (ColumnIndex/OffsetIndex) is automatically
        // active in addition to row-group level statistics, dictionary, and bloom filter checks.
        // Note: all read operations happen synchronously with the ESQL engine. If some operations
        // change to be async, we'll have to unwrap the breaker if it's a LocalBreaker.
        var breaker = blockFactory.breaker();
        var allocator = new CircuitBreakerByteBufferAllocator(new HeapByteBufferAllocator(), breaker);
        return PlainParquetReadOptions.builder(codecFactory).withAllocator(allocator);
    }

    /**
     * Opens a Parquet reader, mapping parquet-mr failures (checked and unchecked) to an
     * {@link IOException} that includes the storage object URI for operators and REST clients.
     */
    private static ParquetFileReader openParquetFile(StorageObject object, InputFile inputFile, ParquetReadOptions options)
        throws IOException {
        String uri = object.path().toString();
        try {
            return ParquetFileReader.open(inputFile, options);
        } catch (IOException e) {
            throw newInvalidParquetFileException(uri, e);
        } catch (RuntimeException e) {
            if (e instanceof CircuitBreakingException) {
                throw e;
            }
            if (e instanceof ElasticsearchException) {
                throw e;
            }
            throw newInvalidParquetFileException(uri, e);
        }
    }

    private static IOException newInvalidParquetFileException(String uri, Exception e) {
        String detail = e.getMessage();
        if (detail == null || detail.isEmpty()) {
            detail = e.getClass().getSimpleName();
        }
        return new IOException("Could not read [" + uri + "] as a Parquet file: " + detail, e);
    }

    /**
     * Resolves both schema/statistics and split ranges in a single pass over the Parquet file's
     * footer. This avoids the double footer read that would otherwise happen when {@code metadata}
     * and split discovery are invoked separately during planning.
     */
    @Override
    public FileLayout resolveFileLayout(StorageObject object) throws IOException {
        InputFile parquetInputFile = new ParquetStorageObjectAdapter(object);
        ParquetReadOptions options = readOptionsBuilder().build();

        try (ParquetFileReader reader = openParquetFile(object, parquetInputFile, options)) {
            FileMetaData fileMetaData = reader.getFileMetaData();
            MessageType parquetSchema = fileMetaData.getSchema();
            SourceMetadata metadata = buildSourceMetadata(reader, parquetSchema, object);
            List<SplitRange> ranges = buildSplitRangesFromRowGroups(reader);
            return new FileLayout(metadata, ranges);
        }
    }

    private SourceMetadata buildSourceMetadata(ParquetFileReader reader, MessageType parquetSchema, StorageObject object) {
        List<Attribute> schema = convertParquetSchemaToAttributes(parquetSchema);
        SourceStatistics statistics = extractStatistics(reader, parquetSchema);
        return new SimpleSourceMetadata(schema, formatName(), object.path().toString(), statistics, null);
    }

    @SuppressWarnings("rawtypes")
    private SourceStatistics extractStatistics(ParquetFileReader reader, MessageType schema) {
        List<BlockMetaData> rowGroups = reader.getRowGroups();
        if (rowGroups.isEmpty()) {
            return null;
        }

        long totalRows = 0;
        long totalSize = 0;
        Map<String, long[]> nullCounts = new HashMap<>();
        Map<String, Comparable[]> mins = new HashMap<>();
        Map<String, Comparable[]> maxs = new HashMap<>();
        Map<String, long[]> colSizes = new HashMap<>();

        for (BlockMetaData rowGroup : rowGroups) {
            totalRows += rowGroup.getRowCount();
            totalSize += rowGroup.getTotalByteSize();
            for (ColumnChunkMetaData col : rowGroup.getColumns()) {
                String colName = col.getPath().toDotString();
                colSizes.merge(colName, new long[] { col.getTotalUncompressedSize() }, (a, b) -> {
                    a[0] += b[0];
                    return a;
                });
                Statistics stats = col.getStatistics();
                if (stats == null || stats.isEmpty()) {
                    continue;
                }
                nullCounts.merge(colName, new long[] { stats.getNumNulls() }, (a, b) -> {
                    a[0] += b[0];
                    return a;
                });
                if (stats.hasNonNullValue()) {
                    mins.merge(colName, new Comparable[] { (Comparable) normalizeStatValue(stats.genericGetMin()) }, (a, b) -> {
                        @SuppressWarnings("unchecked")
                        int cmp = a[0].compareTo(b[0]);
                        if (cmp > 0) a[0] = b[0];
                        return a;
                    });
                    maxs.merge(colName, new Comparable[] { (Comparable) normalizeStatValue(stats.genericGetMax()) }, (a, b) -> {
                        @SuppressWarnings("unchecked")
                        int cmp = a[0].compareTo(b[0]);
                        if (cmp < 0) a[0] = b[0];
                        return a;
                    });
                }
            }
        }

        final long rowCount = totalRows;
        final long sizeBytes = totalSize;
        Map<String, SourceStatistics.ColumnStatistics> columnStats = new HashMap<>();
        for (Type field : schema.getFields()) {
            String name = field.getName();
            long[] nc = nullCounts.get(name);
            Comparable[] mn = mins.get(name);
            Comparable[] mx = maxs.get(name);
            long[] cs = colSizes.get(name);
            if (nc != null || mn != null || mx != null || cs != null) {
                final long nullCount = nc != null ? nc[0] : 0;
                final Object minVal = mn != null ? mn[0] : null;
                final Object maxVal = mx != null ? mx[0] : null;
                final long colSize = cs != null ? cs[0] : -1;
                columnStats.put(name, new SourceStatistics.ColumnStatistics() {
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

                    @Override
                    public OptionalLong sizeInBytes() {
                        return colSize >= 0 ? OptionalLong.of(colSize) : OptionalLong.empty();
                    }
                });
            }
        }

        return new SourceStatistics() {
            @Override
            public OptionalLong rowCount() {
                return OptionalLong.of(rowCount);
            }

            @Override
            public OptionalLong sizeInBytes() {
                return OptionalLong.of(sizeBytes);
            }

            @Override
            public Optional<Map<String, ColumnStatistics>> columnStatistics() {
                return columnStats.isEmpty() ? Optional.empty() : Optional.of(columnStats);
            }
        };
    }

    /**
     * Resolves the raw {@link FilterPredicate} either from deferred pushed expressions (using the
     * actual file schema) or by unwrapping a pre-built {@link FilterCompat.FilterPredicateCompat}.
     * The optimized iterator uses the predicate to compute per-row-group {@link RowRanges} via
     * the column index, mirroring parquet-mr's {@code useColumnIndexFilter=true} behavior that
     * we lose by not calling {@code readNextFilteredRowGroup()}.
     */
    private FilterPredicate resolveFilterPredicate(StorageObject object, MessageType schema) {
        if (pushedExpressions != null) {
            try {
                return pushedExpressions.toFilterPredicate(schema);
            } catch (Exception e) {
                logger.warn("Failed to resolve Parquet filter predicate for [{}], proceeding without pushdown: {}", object.path(), e);
                return null;
            }
        }
        if (pushedFilter instanceof FilterCompat.FilterPredicateCompat compat) {
            return compat.getFilterPredicate();
        }
        return null;
    }

    /**
     * Resolves the record filter from the pre-built {@link FilterCompat.Filter} field.
     * This is only called when {@code resolveFilterPredicate} returned null (no deferred
     * expressions), so it never re-translates pushed expressions.
     */
    private FilterCompat.Filter resolveRecordFilter() {
        return FilterCompat.isFilteringRequired(pushedFilter) ? pushedFilter : FilterCompat.NOOP;
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
        List<String> projectedColumns = context.projectedColumns();
        int batchSize = context.batchSize();
        int rowLimit = context.rowLimit();

        InputFile parquetInputFile = new ParquetStorageObjectAdapter(object);
        ParquetFileReader reader = openParquetFile(object, parquetInputFile, readOptionsBuilder().build());
        try {
            FileMetaData fileMetaData = reader.getFileMetaData();
            MessageType parquetSchema = fileMetaData.getSchema();

            boolean useOptimized = optimizedReader && forceBaselinePath == false;
            FilterPredicate filterPredicate = resolveFilterPredicate(object, parquetSchema);
            FilterCompat.Filter recordFilter = filterPredicate != null ? FilterCompat.get(filterPredicate) : resolveRecordFilter();

            // The optimized path drives row-group selection and page-level filtering itself
            // (via RowGroupFilter + ColumnIndexRowRangesComputer + PrefetchedRowGroupBuilder), so
            // parquet-mr never sees the filter and the reader does not need to be re-opened.
            if (useOptimized == false && FilterCompat.isFilteringRequired(recordFilter)) {
                reader.close();
                reader = openParquetFile(object, parquetInputFile, readOptionsBuilder().withRecordFilter(recordFilter).build());
                fileMetaData = reader.getFileMetaData();
                parquetSchema = fileMetaData.getSchema();
            }
            List<Attribute> attributes = convertParquetSchemaToAttributes(parquetSchema);

            List<Attribute> projectedAttributes;
            if (projectedColumns == null || projectedColumns.isEmpty()) {
                projectedAttributes = attributes;
            } else {
                projectedAttributes = new ArrayList<>();
                Map<String, Attribute> attributeMap = new HashMap<>();
                for (Attribute attr : attributes) {
                    attributeMap.put(attr.name(), attr);
                }
                for (String columnName : projectedColumns) {
                    Attribute attr = attributeMap.get(columnName);
                    attr = attr == null ? new ReferenceAttribute(Source.EMPTY, columnName, DataType.NULL) : attr;
                    projectedAttributes.add(attr);
                }
            }

            MessageType projectedSchema = buildProjectedSchema(parquetSchema, projectedAttributes);
            String createdBy = fileMetaData.getCreatedBy();
            boolean hasRecordFilter = forceBaselinePath || FilterCompat.isFilteringRequired(recordFilter);
            if (useOptimized) {
                return createOptimizedIterator(
                    reader,
                    projectedSchema,
                    projectedAttributes,
                    batchSize,
                    rowLimit,
                    createdBy,
                    object,
                    parquetInputFile,
                    filterPredicate,
                    recordFilter
                );
            }
            return new ParquetColumnIterator(
                reader,
                projectedSchema,
                projectedAttributes,
                batchSize,
                blockFactory,
                rowLimit,
                createdBy,
                object.path().toString(),
                hasRecordFilter
            );
        } catch (Throwable t) {
            reader.close();
            throw t;
        }
    }

    @Override
    public AggregatePushdownSupport aggregatePushdownSupport() {
        return new ParquetAggregatePushdownSupport();
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
    public void close() throws IOException {
        // No resources to close at the reader level
    }

    private static List<SplitRange> buildSplitRangesFromRowGroups(ParquetFileReader reader) {
        List<BlockMetaData> rowGroups = reader.getRowGroups();
        if (rowGroups.isEmpty()) {
            return List.of();
        }
        if (rowGroups.size() == 1) {
            BlockMetaData block = rowGroups.getFirst();
            Map<String, Object> stats = buildRowGroupStats(block);
            return List.of(new SplitRange(block.getStartingPos(), block.getCompressedSize(), stats));
        }
        List<SplitRange> ranges = new ArrayList<>(rowGroups.size());
        for (BlockMetaData block : rowGroups) {
            Map<String, Object> stats = buildRowGroupStats(block);
            // Use the compressed on-disk size for the SplitRange length: this value is fed to
            // readRange() which builds a byte range end = startingPos + length for Parquet's
            // withRange(rangeStart, rangeEnd) filter. That filter includes a row group when its
            // starting position lies in the range, so the end must land at or before the next
            // row group's starting position. getTotalByteSize() returns the uncompressed size
            // (much larger than what is actually on disk), which would make adjacent ranges
            // overlap in byte space and cause Parquet to select each row group from multiple
            // splits, producing duplicate rows.
            ranges.add(new SplitRange(block.getStartingPos(), block.getCompressedSize(), stats));
        }
        List<SplitRange> coalesced = coalesceRowGroupRanges(ranges, DEFAULT_ROW_GROUP_MACRO_SPLIT_TARGET_BYTES);
        return coalesced.size() < 2 ? ranges : coalesced;
    }

    @SuppressWarnings("rawtypes")
    private static Map<String, Object> buildRowGroupStats(BlockMetaData rowGroup) {
        Map<String, Object> stats = new HashMap<>();
        stats.put(SourceStatisticsSerializer.STATS_ROW_COUNT, rowGroup.getRowCount());
        stats.put(SourceStatisticsSerializer.STATS_SIZE_BYTES, rowGroup.getTotalByteSize());
        for (ColumnChunkMetaData col : rowGroup.getColumns()) {
            String colName = col.getPath().toDotString();
            stats.put(SourceStatisticsSerializer.columnSizeBytesKey(colName), col.getTotalUncompressedSize());
            Statistics colStats = col.getStatistics();
            if (colStats == null || colStats.isEmpty()) {
                continue;
            }
            stats.put(SourceStatisticsSerializer.columnNullCountKey(colName), colStats.getNumNulls());
            if (colStats.hasNonNullValue()) {
                stats.put(SourceStatisticsSerializer.columnMinKey(colName), normalizeStatValue(colStats.genericGetMin()));
                stats.put(SourceStatisticsSerializer.columnMaxKey(colName), normalizeStatValue(colStats.genericGetMax()));
            }
        }
        return Map.copyOf(stats);
    }

    /**
     * Normalizes Parquet-specific stat values to types that Elasticsearch can serialize.
     * Parquet {@link Binary} (used for BYTE_ARRAY / string columns) is converted to String;
     * these must be converted to {@code String} before entering the metadata map.
     */
    private static Object normalizeStatValue(Object value) {
        if (value instanceof Binary binary) {
            return binary.toStringUsingUTF8();
        }
        return value;
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

    /**
     * Reads only row groups whose starting position falls within {@code [rangeStart, rangeEnd)}.
     * errorPolicy is accepted for interface compliance but not applied — Parquet errors are
     * structural (corrupt page, schema mismatch) rather than row-level.
     */
    @Override
    public CloseableIterator<Page> readRange(
        StorageObject object,
        List<String> projectedColumns,
        int batchSize,
        long rangeStart,
        long rangeEnd,
        List<Attribute> resolvedAttributes,
        ErrorPolicy errorPolicy
    ) throws IOException {
        InputFile parquetInputFile = ParquetStorageObjectAdapter.forRange(object, rangeEnd - rangeStart);
        ParquetFileReader reader = openParquetFile(object, parquetInputFile, readOptionsBuilder().withRange(rangeStart, rangeEnd).build());
        try {
            FileMetaData fileMetaData = reader.getFileMetaData();
            MessageType parquetSchema = fileMetaData.getSchema();

            boolean useOptimized = optimizedReader && forceBaselinePath == false;
            FilterPredicate filterPredicate = resolveFilterPredicate(object, parquetSchema);
            FilterCompat.Filter recordFilter = filterPredicate != null ? FilterCompat.get(filterPredicate) : resolveRecordFilter();

            // The optimized path drives row-group selection and page-level filtering itself
            // (via RowGroupFilter + ColumnIndexRowRangesComputer + PrefetchedRowGroupBuilder), so
            // parquet-mr never sees the filter and the reader does not need to be re-opened.
            if (useOptimized == false && FilterCompat.isFilteringRequired(recordFilter)) {
                reader.close();
                reader = openParquetFile(
                    object,
                    parquetInputFile,
                    readOptionsBuilder().withRange(rangeStart, rangeEnd).withRecordFilter(recordFilter).build()
                );
                fileMetaData = reader.getFileMetaData();
                parquetSchema = fileMetaData.getSchema();
            }
            // The framework passes planning-time resolved attributes for this query (AsyncExternalSourceOperatorFactory).
            // Reuse them to avoid redundant schema conversion work per split. We still read Parquet metadata to drive row groups.
            final List<Attribute> attributes = resolvedAttributes != null && resolvedAttributes.isEmpty() == false
                ? resolvedAttributes
                : convertParquetSchemaToAttributes(parquetSchema);

            List<Attribute> projectedAttributes;
            if (projectedColumns == null || projectedColumns.isEmpty()) {
                projectedAttributes = attributes;
            } else {
                projectedAttributes = new ArrayList<>();
                Map<String, Attribute> attributeMap = new HashMap<>();
                for (Attribute attr : attributes) {
                    attributeMap.put(attr.name(), attr);
                }
                for (String columnName : projectedColumns) {
                    Attribute attr = attributeMap.get(columnName);
                    attr = attr == null ? new ReferenceAttribute(Source.EMPTY, columnName, DataType.NULL) : attr;
                    projectedAttributes.add(attr);
                }
            }

            MessageType projectedSchema = buildProjectedSchema(parquetSchema, projectedAttributes);
            String createdBy = fileMetaData.getCreatedBy();
            boolean hasRecordFilter = forceBaselinePath || FilterCompat.isFilteringRequired(recordFilter);
            if (useOptimized) {
                return createOptimizedIterator(
                    reader,
                    projectedSchema,
                    projectedAttributes,
                    batchSize,
                    NO_LIMIT,
                    createdBy,
                    object,
                    parquetInputFile,
                    filterPredicate,
                    recordFilter
                );
            }
            return new ParquetColumnIterator(
                reader,
                projectedSchema,
                projectedAttributes,
                batchSize,
                blockFactory,
                NO_LIMIT,
                createdBy,
                object.path().toString(),
                hasRecordFilter
            );
        } catch (Throwable t) {
            reader.close();
            throw t;
        }
    }

    private CloseableIterator<Page> createOptimizedIterator(
        ParquetFileReader reader,
        MessageType projectedSchema,
        List<Attribute> projectedAttributes,
        int batchSize,
        int rowLimit,
        String createdBy,
        StorageObject storageObject,
        InputFile inputFile,
        FilterPredicate filterPredicate,
        FilterCompat.Filter recordFilter
    ) {
        if (inputFile instanceof ParquetStorageObjectAdapter == false) {
            throw new ElasticsearchException(
                "optimized_reader requires ParquetStorageObjectAdapter but got [" + inputFile.getClass().getName() + "]"
            );
        }
        ColumnInfo[] columnInfos = buildColumnInfos(projectedSchema, projectedAttributes);
        validatePlannerTypesAgainstFile(logger, storageObject.path().toString(), reader, projectedAttributes, columnInfos);
        PreloadedRowGroupMetadata preloadedMetadata = PreloadedRowGroupMetadata.preload(reader, storageObject);

        List<BlockMetaData> blocks = reader.getRowGroups();
        boolean[] survivingRowGroups = computeSurvivingRowGroups(reader, blocks, recordFilter, projectedSchema);

        RowRanges[] allRowRanges = null;
        if (filterPredicate != null) {
            allRowRanges = new RowRanges[blocks.size()];
            for (int i = 0; i < blocks.size(); i++) {
                // Row groups dropped by stats/dictionary/bloom filters will never be opened by
                // the iterator, so there is no need to compute their column-index row ranges.
                if (survivingRowGroups[i] == false) {
                    continue;
                }
                allRowRanges[i] = ColumnIndexRowRangesComputer.compute(filterPredicate, preloadedMetadata, i, blocks.get(i).getRowCount());
            }
        }

        return new OptimizedParquetColumnIterator(
            reader,
            projectedSchema,
            projectedAttributes,
            batchSize,
            blockFactory,
            rowLimit,
            createdBy,
            storageObject.path().toString(),
            columnInfos,
            preloadedMetadata,
            storageObject,
            allRowRanges,
            survivingRowGroups,
            codecFactory
        );
    }

    /**
     * Computes the per-row-group survival flag using parquet-mr's {@link RowGroupFilter}, which
     * applies row-group level statistics, dictionary, and bloom filters (in that order). Page-level
     * column-index filtering is intentionally excluded — the optimized iterator handles that itself
     * via the {@link RowRanges} path. Returns {@code null} when no filter is set.
     */
    private static boolean[] computeSurvivingRowGroups(
        ParquetFileReader reader,
        List<BlockMetaData> blocks,
        FilterCompat.Filter recordFilter,
        MessageType schema
    ) {
        if (FilterCompat.isFilteringRequired(recordFilter) == false) {
            return null;
        }
        // RowGroupFilter accepts a list of FilterLevel; STATISTICS + DICTIONARY + BLOOMFILTER
        // mirrors parquet-mr's defaults when useColumnIndexFilter is disabled - matches what
        // readNextFilteredRowGroup() used to skip at row-group level on the previous read path.
        List<RowGroupFilter.FilterLevel> levels = List.of(
            RowGroupFilter.FilterLevel.STATISTICS,
            RowGroupFilter.FilterLevel.DICTIONARY,
            RowGroupFilter.FilterLevel.BLOOMFILTER
        );
        List<BlockMetaData> kept = RowGroupFilter.filterRowGroups(levels, recordFilter, blocks, reader);
        // RowGroupFilter preserves order; identity-comparing surviving blocks against the original
        // list correctly maps each ordinal to its survival flag without relying on equals().
        boolean[] flags = new boolean[blocks.size()];
        int keptIdx = 0;
        for (int i = 0; i < blocks.size() && keptIdx < kept.size(); i++) {
            if (kept.get(keptIdx) == blocks.get(i)) {
                flags[i] = true;
                keptIdx++;
            }
        }
        return flags;
    }

    static ColumnInfo[] buildColumnInfos(MessageType projectedSchema, List<Attribute> attributes) {
        ColumnInfo[] columnInfos = new ColumnInfo[attributes.size()];
        // Uses getPath()[0] (top-level name only), matching the baseline ParquetColumnIterator.
        // Nested Parquet fields have multi-segment paths, but ESQL currently only supports flat
        // columns and LIST(primitive) — nested struct types map to UNSUPPORTED and are skipped.
        Map<String, ColumnDescriptor> descByName = new HashMap<>();
        for (ColumnDescriptor desc : projectedSchema.getColumns()) {
            descByName.put(desc.getPath()[0], desc);
        }
        for (int i = 0; i < attributes.size(); i++) {
            Attribute attr = attributes.get(i);
            if (attr.dataType() == DataType.NULL || attr.dataType() == DataType.UNSUPPORTED) {
                continue;
            }
            ColumnDescriptor desc = descByName.get(attr.name());
            if (desc != null) {
                LogicalTypeAnnotation logicalType = desc.getPrimitiveType().getLogicalTypeAnnotation();
                columnInfos[i] = new ColumnInfo(
                    desc,
                    desc.getPrimitiveType().getPrimitiveTypeName(),
                    attr.dataType(),
                    desc.getMaxDefinitionLevel(),
                    desc.getMaxRepetitionLevel(),
                    logicalType
                );
            }
        }
        return columnInfos;
    }

    private static MessageType buildProjectedSchema(MessageType fullSchema, List<Attribute> projectedAttributes) {
        List<Type> projectedFields = new ArrayList<>();
        for (Attribute attr : projectedAttributes) {
            if (fullSchema.containsField(attr.name())) {
                projectedFields.add(fullSchema.getType(attr.name()));
            }
        }
        // Parquet requires at least one field; fall back to full schema when none match
        if (projectedFields.isEmpty()) {
            return fullSchema;
        }
        return new MessageType(fullSchema.getName(), projectedFields);
    }

    private List<Attribute> convertParquetSchemaToAttributes(MessageType schema) {
        List<Attribute> attributes = new ArrayList<>();
        for (Type field : schema.getFields()) {
            String name = field.getName();
            DataType esqlType = convertParquetTypeToEsql(field);
            attributes.add(new ReferenceAttribute(Source.EMPTY, name, esqlType));
        }
        return attributes;
    }

    private static DataType convertParquetTypeToEsql(Type parquetType) {
        if (parquetType.isPrimitive() == false) {
            return convertGroupTypeToEsql(parquetType.asGroupType());
        }
        PrimitiveType primitive = parquetType.asPrimitiveType();
        LogicalTypeAnnotation logical = primitive.getLogicalTypeAnnotation();

        return switch (primitive.getPrimitiveTypeName()) {
            case BOOLEAN -> DataType.BOOLEAN;
            case INT32 -> {
                if (logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                    yield DataType.DATETIME;
                } else if (logical instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    yield DataType.DOUBLE;
                }
                yield DataType.INTEGER;
            }
            case INT64 -> {
                if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                    yield DataType.DATETIME;
                } else if (logical instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    yield DataType.DOUBLE;
                }
                yield DataType.LONG;
            }
            case INT96 -> DataType.DATETIME;
            case FLOAT, DOUBLE -> DataType.DOUBLE;
            case BINARY, FIXED_LEN_BYTE_ARRAY -> {
                if (logical instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    yield DataType.DOUBLE;
                }
                if (logical instanceof LogicalTypeAnnotation.Float16LogicalTypeAnnotation) {
                    yield DataType.DOUBLE;
                }
                yield DataType.KEYWORD;
            }
            default -> DataType.UNSUPPORTED;
        };
    }

    /**
     * Handles Parquet group types. Supports LIST of primitives by extracting the element type.
     */
    private static DataType convertGroupTypeToEsql(GroupType groupType) {
        LogicalTypeAnnotation logical = groupType.getLogicalTypeAnnotation();
        if (logical instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation && groupType.getFieldCount() == 1) {
            GroupType repeatedGroup = groupType.getType(0).asGroupType();
            if (repeatedGroup.getFieldCount() == 1) {
                Type elementType = repeatedGroup.getType(0);
                if (elementType.isPrimitive()) {
                    return convertParquetTypeToEsql(elementType);
                }
            }
        }
        return DataType.UNSUPPORTED;
    }

    private static final long MILLIS_PER_DAY = Duration.ofDays(1).toMillis();
    private static final long NANOS_PER_MILLI = 1_000_000L;
    /** Julian day number for Unix epoch (1970-01-01). */
    private static final int JULIAN_EPOCH_OFFSET = 2_440_588;

    /**
     * When the query plan type cannot be satisfied from this file's Parquet-derived ESQL type (after
     * applying the same widening rules as {@link EsqlDataTypeConverter#commonType}, plus KEYWORD/TEXT
     * interchangeability), emit a response Warning header (and log a warning) and read the column as
     * null instead of failing at decode time. The warning header lets clients see — alongside other
     * recoverable ES|QL warnings — exactly which columns were silently null-replaced.
     */
    private static void validatePlannerTypesAgainstFile(
        Logger logger,
        String fileLocation,
        ParquetFileReader reader,
        List<Attribute> attributes,
        ColumnInfo[] columnInfos
    ) {
        MessageType fullSchema = reader.getFileMetaData().getSchema();
        SkipWarnings skipWarnings = null;
        for (int i = 0; i < attributes.size(); i++) {
            if (columnInfos[i] == null) {
                continue;
            }
            Attribute attr = attributes.get(i);
            if (attr.dataType() == DataType.NULL || attr.dataType() == DataType.UNSUPPORTED) {
                continue;
            }
            if (fullSchema.containsField(attr.name()) == false) {
                continue;
            }
            DataType actualInFile = convertParquetTypeToEsql(fullSchema.getType(attr.name()));
            if (plannerTypeCompatibleWithFileDerivedType(attr.dataType(), actualInFile) == false) {
                if (skipWarnings == null) {
                    skipWarnings = new SkipWarnings(
                        "Parquet file ["
                            + fileLocation
                            + "] has columns whose on-disk type is incompatible with the planner type; "
                            + "they are returned as null"
                    );
                }
                skipWarnings.add(
                    "Column ["
                        + attr.name()
                        + "] in file ["
                        + fileLocation
                        + "] has type ["
                        + actualInFile
                        + "] incompatible with planner type ["
                        + attr.dataType()
                        + "]; returning nulls for this column"
                );
                logger.warn(
                    "Column [{}] in file [{}] has type [{}] incompatible with planner type [{}] after widening; "
                        + "returning nulls for this column",
                    attr.name(),
                    fileLocation,
                    actualInFile,
                    attr.dataType()
                );
                columnInfos[i] = null;
            }
        }
    }

    /**
     * Whether values from a column whose Parquet schema maps to {@code fileDerived} can be read using
     * the planner's {@code planner} type (same widening notion as globbed external sources).
     */
    private static boolean plannerTypeCompatibleWithFileDerivedType(DataType planner, DataType fileDerived) {
        DataType unified = EsqlDataTypeConverter.commonType(planner, fileDerived);
        return unified != null && unified.equals(planner);
    }

    /**
     * Column-at-a-time Parquet iterator. Flat columns (maxRepLevel == 0) are decoded via
     * {@link PageColumnReader} which bulk-decodes definition levels and values directly from
     * page bytes, bypassing parquet-mr's row-at-a-time {@link ColumnReader}. List columns
     * (maxRepLevel > 0) continue using {@link ColumnReadStoreImpl} and {@link ColumnReader}
     * since they require stateful multi-value handling via repetition levels.
     */
    private static class ParquetColumnIterator implements CloseableIterator<Page> {
        private final ParquetFileReader reader;
        private final MessageType projectedSchema;
        private final List<Attribute> attributes;
        private final int batchSize;
        private final BlockFactory blockFactory;
        private final String createdBy;
        private final String fileLocation;
        private final boolean hasRecordFilter;
        private int rowBudget;

        /** Per-attribute column metadata; null for attributes not present in the file. */
        private final ColumnInfo[] columnInfos;
        private final boolean hasListColumns;

        private PageReadStore rowGroup;
        private PageColumnReader[] pageColumnReaders;
        private ColumnReader[] columnReaders;
        /** Per-column uncompressed byte size for the current row group (0 if unknown). */
        private long[] columnUncompressedBytes;
        private long rowsRemainingInGroup;
        private boolean exhausted = false;
        private int rowGroupOrdinal = -1;
        private int pageBatchIndexInRowGroup = 0;

        ParquetColumnIterator(
            ParquetFileReader reader,
            MessageType projectedSchema,
            List<Attribute> attributes,
            int batchSize,
            BlockFactory blockFactory,
            int rowLimit,
            String createdBy,
            String fileLocation,
            boolean hasRecordFilter
        ) {
            this.reader = reader;
            this.projectedSchema = projectedSchema;
            this.attributes = attributes;
            this.batchSize = batchSize;
            this.blockFactory = blockFactory;
            this.rowBudget = rowLimit;
            this.createdBy = createdBy != null ? createdBy : "";
            this.fileLocation = fileLocation;
            this.hasRecordFilter = hasRecordFilter;

            reader.setRequestedSchema(projectedSchema);

            this.columnInfos = new ColumnInfo[attributes.size()];
            Map<String, ColumnDescriptor> descByName = new HashMap<>();
            for (ColumnDescriptor desc : projectedSchema.getColumns()) {
                descByName.put(desc.getPath()[0], desc);
            }
            boolean foundListCol = false;
            for (int i = 0; i < attributes.size(); i++) {
                Attribute attr = attributes.get(i);
                if (attr.dataType() == DataType.NULL || attr.dataType() == DataType.UNSUPPORTED) {
                    continue;
                }
                ColumnDescriptor desc = descByName.get(attr.name());
                if (desc != null) {
                    LogicalTypeAnnotation logicalType = desc.getPrimitiveType().getLogicalTypeAnnotation();
                    columnInfos[i] = new ColumnInfo(
                        desc,
                        desc.getPrimitiveType().getPrimitiveTypeName(),
                        attr.dataType(),
                        desc.getMaxDefinitionLevel(),
                        desc.getMaxRepetitionLevel(),
                        logicalType
                    );
                    if (desc.getMaxRepetitionLevel() > 0) {
                        foundListCol = true;
                    }
                }
            }
            this.hasListColumns = foundListCol;
            validatePlannerTypesAgainstFile(logger, fileLocation, reader, attributes, columnInfos);
        }

        @Override
        public boolean hasNext() {
            if (exhausted) {
                return false;
            }
            if (rowBudget != FormatReader.NO_LIMIT && rowBudget <= 0) {
                exhausted = true;
                return false;
            }
            if (rowsRemainingInGroup > 0) {
                return true;
            }
            try {
                return advanceRowGroup();
            } catch (IOException e) {
                throw new ElasticsearchException(
                    "Failed to read Parquet row group [" + (rowGroupOrdinal + 1) + "] in file [" + fileLocation + "]: " + e.getMessage(),
                    e
                );
            }
        }

        private boolean advanceRowGroup() throws IOException {
            if (rowGroup != null) {
                rowGroup.close();
                rowGroup = null;
            }
            rowGroup = reader.readNextFilteredRowGroup();
            if (rowGroup == null) {
                exhausted = true;
                return false;
            }
            rowGroupOrdinal++;
            pageBatchIndexInRowGroup = 0;
            rowsRemainingInGroup = rowGroup.getRowCount();

            if (hasRecordFilter == false) {
                RowRanges allRows = RowRanges.all(rowsRemainingInGroup);
                pageColumnReaders = new PageColumnReader[columnInfos.length];
                for (int i = 0; i < columnInfos.length; i++) {
                    ColumnInfo ci = columnInfos[i];
                    if (ci != null && ci.maxRepLevel() == 0) {
                        PageReader pageReader = rowGroup.getPageReader(ci.descriptor());
                        pageColumnReaders[i] = new PageColumnReader(pageReader, ci.descriptor(), ci, allRows);
                    }
                }
            } else {
                pageColumnReaders = null;
            }

            if (hasRecordFilter || hasListColumns) {
                ColumnReadStoreImpl store = new ColumnReadStoreImpl(
                    rowGroup,
                    new ParquetColumnDecoding.NoOpGroupConverter(projectedSchema),
                    projectedSchema,
                    createdBy
                );
                columnReaders = new ColumnReader[columnInfos.length];
                columnUncompressedBytes = new long[columnInfos.length];

                // Best-effort: rowGroupOrdinal may not match the physical block index when
                // readNextFilteredRowGroup() skips entire row groups. A wrong hint only affects
                // pre-sizing (falls back to grow-on-demand), not correctness.
                List<BlockMetaData> rowGroups = reader.getRowGroups();
                Map<String, Long> chunkSizes = Map.of();
                if (rowGroupOrdinal >= 0 && rowGroupOrdinal < rowGroups.size()) {
                    BlockMetaData block = rowGroups.get(rowGroupOrdinal);
                    chunkSizes = new HashMap<>();
                    for (ColumnChunkMetaData chunk : block.getColumns()) {
                        chunkSizes.put(chunk.getPath().toDotString(), chunk.getTotalUncompressedSize());
                    }
                }

                for (int i = 0; i < columnInfos.length; i++) {
                    if (columnInfos[i] != null) {
                        boolean needColumnReader = hasRecordFilter
                            ? (pageColumnReaders == null || pageColumnReaders[i] == null)
                            : columnInfos[i].maxRepLevel() > 0;
                        if (needColumnReader) {
                            columnReaders[i] = store.getColumnReader(columnInfos[i].descriptor());
                            String colPath = String.join(".", columnInfos[i].descriptor().getPath());
                            Long size = chunkSizes.get(colPath);
                            columnUncompressedBytes[i] = size != null ? size : 0L;
                        }
                    }
                }
            } else {
                columnReaders = null;
                columnUncompressedBytes = null;
            }
            return rowsRemainingInGroup > 0;
        }

        @Override
        public Page next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            int effectiveBatch = batchSize;
            if (rowBudget != FormatReader.NO_LIMIT) {
                effectiveBatch = Math.min(effectiveBatch, rowBudget);
            }
            int rowsToRead = (int) Math.min(effectiveBatch, rowsRemainingInGroup);

            Block[] blocks = new Block[attributes.size()];
            try {
                for (int col = 0; col < columnInfos.length; col++) {
                    ColumnInfo info = columnInfos[col];
                    if (info == null) {
                        blocks[col] = blockFactory.newConstantNullBlock(rowsToRead);
                    } else {
                        try {
                            if (pageColumnReaders != null && pageColumnReaders[col] != null) {
                                blocks[col] = pageColumnReaders[col].readBatch(rowsToRead, blockFactory);
                            } else {
                                blocks[col] = readColumnBlock(columnReaders[col], info, rowsToRead, col);
                            }
                        } catch (Exception e) {
                            Releasables.closeExpectNoException(blocks);
                            Attribute attr = attributes.get(col);
                            throw new ElasticsearchException(
                                "Failed to read Parquet column ["
                                    + attr.name()
                                    + "] (type "
                                    + attr.dataType()
                                    + ") at row group ["
                                    + (rowGroupOrdinal + 1)
                                    + "] page batch ["
                                    + pageBatchIndexInRowGroup
                                    + "] in file ["
                                    + fileLocation
                                    + "]: "
                                    + e.getMessage(),
                                e
                            );
                        }
                    }
                }
            } catch (ElasticsearchException e) {
                throw e;
            } catch (Exception e) {
                Releasables.closeExpectNoException(blocks);
                throw new ElasticsearchException(
                    "Failed to create Page batch at row group ["
                        + (rowGroupOrdinal + 1)
                        + "] page batch ["
                        + pageBatchIndexInRowGroup
                        + "] in file ["
                        + fileLocation
                        + "]: "
                        + e.getMessage(),
                    e
                );
            }

            pageBatchIndexInRowGroup++;
            rowsRemainingInGroup -= rowsToRead;
            if (rowBudget != FormatReader.NO_LIMIT) {
                rowBudget -= rowsToRead;
            }
            return new Page(blocks);
        }

        private Block readColumnBlock(ColumnReader cr, ColumnInfo info, int rowsToRead, int colIndex) {
            if (info.maxRepLevel() > 0) {
                return ParquetColumnDecoding.readListColumn(cr, info, rowsToRead, blockFactory);
            }
            return switch (info.esqlType()) {
                case BOOLEAN -> readBooleanColumn(cr, info.maxDefLevel(), rowsToRead);
                case INTEGER -> readIntColumn(cr, info.maxDefLevel(), rowsToRead);
                case LONG -> {
                    if (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32) {
                        yield readInt32WidenedToLongColumn(cr, info.maxDefLevel(), rowsToRead);
                    }
                    yield readLongColumn(cr, info.maxDefLevel(), rowsToRead);
                }
                case DOUBLE -> readDoubleColumn(cr, info, rowsToRead);
                case KEYWORD, TEXT -> {
                    long totalRows = rowGroup.getRowCount();
                    long scaledHint = totalRows > 0 ? (columnUncompressedBytes[colIndex] * rowsToRead) / totalRows : 0L;
                    yield readBytesRefColumn(cr, info, rowsToRead, scaledHint);
                }
                case DATETIME -> readDatetimeColumn(cr, info, rowsToRead);
                default -> {
                    ParquetColumnDecoding.skipValues(cr, rowsToRead);
                    yield blockFactory.newConstantNullBlock(rowsToRead);
                }
            };
        }

        private Block readBooleanColumn(ColumnReader cr, int maxDef, int rows) {
            boolean[] values = new boolean[rows];
            boolean[] isNull = maxDef > 0 ? new boolean[rows] : null;
            boolean noNulls = true;
            for (int i = 0; i < rows; i++) {
                if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                    isNull[i] = true;
                    noNulls = false;
                } else {
                    values[i] = cr.getBoolean();
                }
                cr.consume();
            }
            if (noNulls) {
                return blockFactory.newBooleanArrayVector(values, rows).asBlock();
            }
            return blockFactory.newBooleanArrayBlock(
                values,
                rows,
                null,
                ParquetColumnDecoding.toBitSet(isNull, rows),
                Block.MvOrdering.UNORDERED
            );
        }

        private Block readIntColumn(ColumnReader cr, int maxDef, int rows) {
            int[] values = new int[rows];
            boolean[] isNull = maxDef > 0 ? new boolean[rows] : null;
            boolean noNulls = true;
            for (int i = 0; i < rows; i++) {
                if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                    isNull[i] = true;
                    noNulls = false;
                } else {
                    values[i] = cr.getInteger();
                }
                cr.consume();
            }
            if (noNulls) {
                return blockFactory.newIntArrayVector(values, rows).asBlock();
            }
            return blockFactory.newIntArrayBlock(
                values,
                rows,
                null,
                ParquetColumnDecoding.toBitSet(isNull, rows),
                Block.MvOrdering.UNORDERED
            );
        }

        /**
         * Parquet INT32 columns do not support {@link ColumnReader#getLong()}; widen safely to long for planner LONG.
         */
        private Block readInt32WidenedToLongColumn(ColumnReader cr, int maxDef, int rows) {
            long[] values = new long[rows];
            boolean[] isNull = maxDef > 0 ? new boolean[rows] : null;
            boolean noNulls = true;
            for (int i = 0; i < rows; i++) {
                if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                    isNull[i] = true;
                    noNulls = false;
                } else {
                    values[i] = cr.getInteger();
                }
                cr.consume();
            }
            return ColumnBlockConversions.longColumn(blockFactory, values, rows, noNulls, false, isNull);
        }

        private Block readLongColumn(ColumnReader cr, int maxDef, int rows) {
            long[] values = new long[rows];
            boolean[] isNull = maxDef > 0 ? new boolean[rows] : null;
            boolean noNulls = true;
            for (int i = 0; i < rows; i++) {
                if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                    isNull[i] = true;
                    noNulls = false;
                } else {
                    values[i] = cr.getLong();
                }
                cr.consume();
            }
            return ColumnBlockConversions.longColumn(blockFactory, values, rows, noNulls, false, isNull);
        }

        private Block readDoubleColumn(ColumnReader cr, ColumnInfo info, int rows) {
            LogicalTypeAnnotation logical = info.logicalType();
            if (logical instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
                return readDecimalAsDoubleColumn(cr, info, decimal.getScale(), rows);
            }
            if (logical instanceof LogicalTypeAnnotation.Float16LogicalTypeAnnotation) {
                return readFloat16Column(cr, info.maxDefLevel(), rows);
            }
            double[] values = new double[rows];
            boolean[] isNull = info.maxDefLevel() > 0 ? new boolean[rows] : null;
            boolean noNulls = true;
            boolean isFloat = info.parquetType() == PrimitiveType.PrimitiveTypeName.FLOAT;
            for (int i = 0; i < rows; i++) {
                if (info.maxDefLevel() > 0 && cr.getCurrentDefinitionLevel() < info.maxDefLevel()) {
                    isNull[i] = true;
                    noNulls = false;
                } else {
                    values[i] = isFloat ? cr.getFloat() : cr.getDouble();
                }
                cr.consume();
            }
            return ColumnBlockConversions.doubleColumn(blockFactory, values, rows, noNulls, false, isNull);
        }

        private Block readDecimalAsDoubleColumn(ColumnReader cr, ColumnInfo info, int scale, int rows) {
            double[] values = new double[rows];
            boolean[] isNull = info.maxDefLevel() > 0 ? new boolean[rows] : null;
            boolean noNulls = true;
            for (int i = 0; i < rows; i++) {
                if (info.maxDefLevel() > 0 && cr.getCurrentDefinitionLevel() < info.maxDefLevel()) {
                    isNull[i] = true;
                    noNulls = false;
                } else {
                    BigInteger unscaled = switch (info.parquetType()) {
                        case INT32 -> BigInteger.valueOf(cr.getInteger());
                        case INT64 -> BigInteger.valueOf(cr.getLong());
                        case BINARY, FIXED_LEN_BYTE_ARRAY -> new BigInteger(cr.getBinary().getBytes());
                        default -> throw new QlIllegalArgumentException("Unexpected DECIMAL backing type: " + info.parquetType());
                    };
                    values[i] = new java.math.BigDecimal(unscaled, scale).doubleValue();
                }
                cr.consume();
            }
            return ColumnBlockConversions.doubleColumn(blockFactory, values, rows, noNulls, false, isNull);
        }

        private Block readFloat16Column(ColumnReader cr, int maxDef, int rows) {
            double[] values = new double[rows];
            boolean[] isNull = maxDef > 0 ? new boolean[rows] : null;
            boolean noNulls = true;
            for (int i = 0; i < rows; i++) {
                if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                    isNull[i] = true;
                    noNulls = false;
                } else {
                    byte[] bytes = cr.getBinary().getBytes();
                    short float16Bits = (short) ((bytes[1] & 0xFF) << 8 | (bytes[0] & 0xFF));
                    values[i] = Float.float16ToFloat(float16Bits);
                }
                cr.consume();
            }
            return ColumnBlockConversions.doubleColumn(blockFactory, values, rows, noNulls, false, isNull);
        }

        private Block readBytesRefColumn(ColumnReader cr, ColumnInfo info, int rows, long byteHint) {
            boolean isUuid = info.logicalType() instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
            try (
                var builder = byteHint > 0
                    ? blockFactory.newBytesRefBlockBuilder(rows, byteHint)
                    : blockFactory.newBytesRefBlockBuilder(rows)
            ) {
                for (int i = 0; i < rows; i++) {
                    if (info.maxDefLevel() > 0 && cr.getCurrentDefinitionLevel() < info.maxDefLevel()) {
                        builder.appendNull();
                    } else if (isUuid) {
                        builder.appendBytesRef(new BytesRef(ParquetColumnDecoding.formatUuid(cr.getBinary().getBytes())));
                    } else {
                        builder.appendBytesRef(new BytesRef(cr.getBinary().getBytes()));
                    }
                    cr.consume();
                }
                return builder.build();
            }
        }

        private Block readDatetimeColumn(ColumnReader cr, ColumnInfo info, int rows) {
            if (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT96) {
                return readInt96TimestampColumn(cr, info.maxDefLevel(), rows);
            }
            long[] values = new long[rows];
            boolean[] isNull = info.maxDefLevel() > 0 ? new boolean[rows] : null;
            boolean noNulls = true;
            boolean isDate = info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32;
            for (int i = 0; i < rows; i++) {
                if (info.maxDefLevel() > 0 && cr.getCurrentDefinitionLevel() < info.maxDefLevel()) {
                    isNull[i] = true;
                    noNulls = false;
                } else if (isDate) {
                    values[i] = cr.getInteger() * MILLIS_PER_DAY;
                } else {
                    long raw = cr.getLong();
                    values[i] = ParquetColumnDecoding.convertTimestampToMillis(raw, info.logicalType());
                }
                cr.consume();
            }
            return ColumnBlockConversions.longColumn(blockFactory, values, rows, noNulls, false, isNull);
        }

        /**
         * Converts a Parquet INT96 value (12 bytes: 8 bytes nanos-of-day LE + 4 bytes Julian day LE)
         * to epoch milliseconds.
         */
        private Block readInt96TimestampColumn(ColumnReader cr, int maxDef, int rows) {
            long[] values = new long[rows];
            boolean[] isNull = maxDef > 0 ? new boolean[rows] : null;
            boolean noNulls = true;
            for (int i = 0; i < rows; i++) {
                if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                    isNull[i] = true;
                    noNulls = false;
                } else {
                    Binary bin = cr.getBinary();
                    ByteBuffer buf = ByteBuffer.wrap(bin.getBytes()).order(ByteOrder.LITTLE_ENDIAN);
                    long nanosOfDay = buf.getLong();
                    int julianDay = buf.getInt();
                    long epochDay = julianDay - JULIAN_EPOCH_OFFSET;
                    values[i] = epochDay * MILLIS_PER_DAY + nanosOfDay / NANOS_PER_MILLI;
                }
                cr.consume();
            }
            return ColumnBlockConversions.longColumn(blockFactory, values, rows, noNulls, false, isNull);
        }

        @Override
        public void close() throws IOException {
            try {
                if (rowGroup != null) {
                    rowGroup.close();
                }
            } finally {
                reader.close();
            }
        }
    }

}
