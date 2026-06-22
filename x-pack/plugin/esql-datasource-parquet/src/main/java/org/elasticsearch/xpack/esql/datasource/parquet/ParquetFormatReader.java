/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
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
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.UninitializedArrays;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.cache.ParsedFooterCache;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnBlockConversions;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorAware;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.DynamicThreshold;
import org.elasticsearch.xpack.esql.datasources.spi.DynamicThresholdAware;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeReadContext;
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
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;

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
public class ParquetFormatReader implements RangeAwareFormatReader, ColumnExtractorAware, DynamicThresholdAware {

    private static final Logger logger = LogManager.getLogger(ParquetFormatReader.class);

    /**
     * JVM-wide cache of parsed Parquet footers. Singleton — every {@link ParquetFormatReader}
     * instance reads from and writes to the same cache so that producer threads spawned from
     * different reader instances (e.g. across concurrent queries) still coalesce footer parses.
     */
    private static final ParsedFooterCache<ParquetMetadata> PARSED_FOOTERS = new ParsedFooterCache<>();

    private final BlockFactory blockFactory;
    private final FilterCompat.Filter pushedFilter;
    private final ParquetPushedExpressions pushedExpressions;
    private final boolean forceBaselinePath;
    private final boolean optimizedReader;
    private final boolean lateMaterializationEnabled;
    private final DynamicThreshold dynamicThreshold;
    // Shared across all iterators created by this reader: holds lazy decompressor instances and
    // pays the per-codec init cost once. The factory is stateless across files/row groups.
    private final PlainCompressionCodecFactory codecFactory = new PlainCompressionCodecFactory();
    // Mutable reader-level counters surfaced as a Map<String, Object> via {@link #statusSnapshot()}
    // and folded into AsyncExternalSourceOperator.Status.formatReader by the carrier. Per-reader
    // (one ParquetFormatReader instance owns one counter struct), incremented by each read /
    // readRange invocation that flows through this reader.
    private final ParquetReaderCounters counters = new ParquetReaderCounters();

    static final long DEFAULT_ROW_GROUP_MACRO_SPLIT_TARGET_BYTES = 32L * 1024 * 1024;

    static final String CONFIG_OPTIMIZED_READER = "optimized_reader";
    static final String CONFIG_LATE_MATERIALIZATION = "late_materialization";

    /** Keys recognised by {@link #withConfigTrackingConsumedKeys(Map)}. */
    static final Set<String> RECOGNIZED_KEYS = Set.of(CONFIG_OPTIMIZED_READER, CONFIG_LATE_MATERIALIZATION);

    /** Clears the parsed-footer cache. Intended for test isolation only. */
    static void clearParsedFooterCacheForTests() {
        PARSED_FOOTERS.invalidateAll();
    }

    /**
     * Shared decompressor factory exposed to {@link ParquetColumnExtractor} so it can build
     * row-group readers via {@link PrefetchedRowGroupBuilder} without re-instantiating its own
     * factory. Package-private on purpose: the extractor lives in this package and is the only
     * collaborator that needs it.
     */
    PlainCompressionCodecFactory codecFactory() {
        return codecFactory;
    }

    /**
     * Resolves the {@link ColumnInfo} for a column by name within a parquet {@link MessageType},
     * applying the same primitive-type mapping that the iterator uses (see
     * {@link #convertParquetTypeToEsql}). Returns {@code null} when the column is absent or maps
     * to {@link DataType#UNSUPPORTED} / {@link DataType#NULL}. The returned descriptor carries
     * {@code maxRepetitionLevel} so callers can route flat vs list paths off of it.
     * <p>
     * {@code columnName} may be a dotted struct-leaf path (e.g. {@code "event.action"}) — the
     * same D2 resolution rule as {@link #resolveFieldType} applies: literal top-level match wins
     * over dotted-path traversal.
     * <p>
     * Package-private because {@link ParquetColumnExtractor} is the only collaborator and we
     * deliberately keep this primitive-type mapping single-sourced — duplicating it in the
     * extractor risks subtle type drift the next time a logical type is added.
     */
    static ColumnInfo resolveColumnInfo(MessageType schema, String columnName) {
        Type field = resolveFieldType(schema, columnName);
        if (field == null) {
            return null;
        }
        DataType esqlType = convertParquetTypeToEsql(field);
        if (esqlType == DataType.UNSUPPORTED || esqlType == DataType.NULL) {
            return null;
        }
        // For top-level columns (flat scalars, literal-dot names, LIST<primitive>) match the
        // descriptor by first path segment. For dotted struct-leaf columns (e.g. "event.action")
        // the first segment is the struct name, not the full dotted attribute name, so match by
        // the full joined path instead.
        boolean isTopLevel = schema.containsField(columnName);
        ColumnDescriptor descriptor = null;
        for (ColumnDescriptor desc : schema.getColumns()) {
            String[] path = desc.getPath();
            if (isTopLevel ? (path.length > 0 && path[0].equals(columnName)) : String.join(".", path).equals(columnName)) {
                descriptor = desc;
                break;
            }
        }
        if (descriptor == null) {
            return null;
        }
        PrimitiveType prim = descriptor.getPrimitiveType();
        return new ColumnInfo(
            descriptor,
            prim.getPrimitiveTypeName(),
            esqlType,
            descriptor.getMaxDefinitionLevel(),
            descriptor.getMaxRepetitionLevel(),
            prim.getLogicalTypeAnnotation()
        );
    }

    public ParquetFormatReader(BlockFactory blockFactory) {
        this(blockFactory, FilterCompat.NOOP, null, false, true, true, null);
    }

    ParquetFormatReader(BlockFactory blockFactory, boolean optimizedReader) {
        this(blockFactory, FilterCompat.NOOP, null, false, optimizedReader, true, null);
    }

    private ParquetFormatReader(
        BlockFactory blockFactory,
        FilterCompat.Filter pushedFilter,
        ParquetPushedExpressions pushedExpressions,
        boolean forceBaselinePath,
        boolean optimizedReader,
        boolean lateMaterializationEnabled,
        DynamicThreshold dynamicThreshold
    ) {
        this.blockFactory = blockFactory;
        this.pushedFilter = pushedFilter;
        this.pushedExpressions = pushedExpressions;
        this.forceBaselinePath = forceBaselinePath;
        this.optimizedReader = optimizedReader;
        this.lateMaterializationEnabled = lateMaterializationEnabled;
        this.dynamicThreshold = dynamicThreshold;
    }

    /**
     * Returns a reader that always uses the row-at-a-time {@code ColumnReader} path,
     * bypassing {@link PageColumnReader}. Package-private; intended for correctness
     * testing against the optimized path.
     */
    ParquetFormatReader withBaselinePath() {
        return new ParquetFormatReader(
            blockFactory,
            pushedFilter,
            pushedExpressions,
            true,
            optimizedReader,
            lateMaterializationEnabled,
            dynamicThreshold
        );
    }

    @Override
    public ParquetFormatReader withPushedFilter(Object pushedFilter) {
        if (pushedFilter instanceof FilterCompat.Filter filter) {
            return new ParquetFormatReader(
                blockFactory,
                filter,
                null,
                forceBaselinePath,
                optimizedReader,
                lateMaterializationEnabled,
                dynamicThreshold
            );
        }
        if (pushedFilter instanceof ParquetPushedExpressions exprs) {
            return new ParquetFormatReader(
                blockFactory,
                FilterCompat.NOOP,
                exprs,
                forceBaselinePath,
                optimizedReader,
                lateMaterializationEnabled,
                dynamicThreshold
            );
        }
        return this;
    }

    @Override
    public FormatReader withDynamicThreshold(DynamicThreshold threshold) {
        return new ParquetFormatReader(
            blockFactory,
            pushedFilter,
            pushedExpressions,
            forceBaselinePath,
            optimizedReader,
            lateMaterializationEnabled,
            threshold
        );
    }

    @Override
    public Configured<FormatReader> withConfigTrackingConsumedKeys(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return Configured.empty(this);
        }
        boolean newOptimized = parseBooleanConfig(config, CONFIG_OPTIMIZED_READER, optimizedReader);
        boolean newLateMat = parseBooleanConfig(config, CONFIG_LATE_MATERIALIZATION, lateMaterializationEnabled);
        FormatReader result = (newOptimized == optimizedReader && newLateMat == lateMaterializationEnabled)
            ? this
            : new ParquetFormatReader(
                blockFactory,
                pushedFilter,
                pushedExpressions,
                forceBaselinePath,
                newOptimized,
                newLateMat,
                dynamicThreshold
            );
        return Configured.fromKnownSubset(result, config, RECOGNIZED_KEYS);
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
     * Returns an immutable typed snapshot of this reader's instrumentation counters. The carrier
     * ({@code AsyncExternalSourceOperator.Status}) folds it into the {@code format_reader}
     * sub-object on the operator profile.
     */
    @Override
    public ParquetReaderStatus statusSnapshot() {
        return counters.snapshot();
    }

    /** Returns {@link StorageObject#length()} when callable, otherwise 0 (length is best-effort). */
    private static long sizeOrZero(StorageObject object) {
        try {
            return Math.max(0L, object.length());
        } catch (Exception e) {
            return 0L;
        }
    }

    /**
     * Records per-column materialization mode (eager/late) for each projected, non-NULL column.
     * Per-page byte and time accounting is not wired here; see {@link ParquetReaderCounters} field
     * docs and the TODO in {@link #read(StorageObject, FormatReadContext)} for scope.
     */
    private void recordPerColumnMaterialization(List<Attribute> projectedAttributes, boolean useOptimized) {
        Set<String> predicateNames = pushedExpressions != null ? pushedExpressions.predicateColumnNames() : Set.of();
        boolean lateActive = useOptimized && lateMaterializationEnabled && pushedExpressions != null;
        for (Attribute attr : projectedAttributes) {
            if (attr.dataType() == DataType.NULL || attr.dataType() == DataType.UNSUPPORTED) {
                continue;
            }
            // A column is late-materialized when the late-mat path is active and the column is NOT
            // a predicate column (predicate columns are read eagerly to drive row narrowing first).
            String mode = lateActive && predicateNames.contains(attr.name()) == false
                ? PerColumnStatus.MATERIALIZATION_LATE
                : PerColumnStatus.MATERIALIZATION_EAGER;
            counters.perColumn(attr.name()).setMaterialization(mode);
        }
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
        var allocator = new CircuitBreakerByteBufferAllocator(new DirectByteBufferAllocator(), breaker);
        return PlainParquetReadOptions.builder(codecFactory).withAllocator(allocator);
    }

    /**
     * Opens a Parquet reader, mapping parquet-mr failures (checked and unchecked) to an
     * {@link IllegalArgumentException} that includes the storage object URI. This ensures
     * invalid/corrupt Parquet files produce HTTP 400 rather than 500.
     */
    private static ParquetFileReader openParquetFile(
        StorageObject object,
        InputFile inputFile,
        ParquetReadOptions options,
        ParquetMetadata cachedFooter
    ) throws IOException {
        String uri = object.path().toString();
        try {
            return ParquetFileReader.open(inputFile, cachedFooter, options, inputFile.newStream());
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

    /**
     * Opens a Parquet reader using the JVM-wide {@link #PARSED_FOOTERS} cache so that the Thrift
     * footer deserialization runs at most once per {@code (path, length)} key. On a cache miss the
     * loader parses the full footer (no range filter) so the cached {@link ParquetMetadata} remains
     * usable for any subsequent split of the same file; the per-call read options are then applied
     * when opening the reader against the cached metadata.
     * <p>
     * Range-restricted reads should call this with the loader-time options unrestricted but the
     * open-time options range-restricted; see {@code readRange} for an example.
     *
     * @param object the storage object — used only for error reporting
     * @param adapter the storage adapter that owns the byte-level cache key
     * @param options open-time read options (may include {@code withRange} / {@code withRecordFilter})
     */
    private ParquetFileReader openParquetFileCached(StorageObject object, ParquetStorageObjectAdapter adapter, ParquetReadOptions options)
        throws IOException {
        ParquetMetadata footer = loadFooter(object, adapter);
        return openParquetFile(object, adapter, options, footer);
    }

    /**
     * Loads the parsed Parquet footer for {@code adapter} via the JVM-wide {@link #PARSED_FOOTERS}
     * cache, parsing on a cache miss. The loader explicitly uses unranged read options so the
     * cached value is the full file footer (all row groups) and can be reused across split readers.
     *
     * <p>This method is intentionally non-static: the loader lambda calls {@link #readOptionsBuilder()},
     * which wires in this instance's {@link CircuitBreakerByteBufferAllocator}. Under cache miss,
     * the parse is therefore charged to whichever reader instance won the load race. In practice
     * all ESQL Parquet readers share a parent {@link org.elasticsearch.compute.data.BlockFactory}
     * breaker, so the breaker that wins is equivalent to any other; the cached
     * {@link ParquetMetadata} itself is independent of the allocator. If breakers ever become
     * per-query, the parse-time allocation accounting would need to move off of this instance's
     * builder.
     */
    private ParquetMetadata loadFooter(StorageObject object, ParquetStorageObjectAdapter adapter) throws IOException {
        // The loader runs only on a cache miss, so the flag distinguishes hit from miss.
        boolean[] missed = { false };
        try {
            ParquetMetadata footer = PARSED_FOOTERS.getOrLoad(adapter.cacheKey(), key -> {
                missed[0] = true;
                // Always parse the full footer (no range filter) so the cached entry is reusable
                // by any split. ParquetFileReader.open with a range only retains blocks whose
                // midpoint falls in the range, which would make getFooter() unusable for other
                // splits. The underlying FooterByteCache ensures the tail bytes are fetched from
                // storage only once on the first parse.
                //
                // Note: this variant of readFooter doesn't close the stream.
                try (SeekableInputStream stream = adapter.newStream()) {
                    return ParquetFileReader.readFooter(adapter, readOptionsBuilder().build(), stream);
                }
            });
            counters.recordFooterCache(missed[0] == false);
            return footer;
        } catch (ExecutionException e) {
            // rethrowStructural handles Error/IOException/CircuitBreakingException/
            // ElasticsearchException; any other cause (typically a plain RuntimeException from
            // parquet-mr signalling a malformed file) is returned here so we can apply the
            // parquet-specific wrapping that the prior in-line readFooter path used. The returned
            // throwable is never an Error (already rethrown) so the Exception cast is safe.
            // Callers see the same exception shapes regardless of who won the load race.
            throw newInvalidParquetFileException(object.path().toString(), (Exception) ParsedFooterCache.rethrowStructural(e));
        }
    }

    /**
     * Filters a cached footer's row groups to only those whose midpoint falls within [rangeStart, rangeEnd).
     * This mirrors parquet-mr's split assignment logic (see {@code ParquetInputFormat.getRowGroupInfo})
     * which is applied during footer parsing but skipped when using a pre-parsed {@link ParquetMetadata}.
     */
    private static ParquetMetadata filterBlocksByRange(ParquetMetadata metadata, long rangeStart, long rangeEnd) {
        List<BlockMetaData> filtered = new ArrayList<>();
        for (BlockMetaData block : metadata.getBlocks()) {
            long midpoint = block.getStartingPos() + block.getCompressedSize() / 2;
            if (midpoint >= rangeStart && midpoint < rangeEnd) {
                filtered.add(block);
            }
        }
        return new ParquetMetadata(metadata.getFileMetaData(), filtered);
    }

    /**
     * Computes file-global first-row offsets for every block whose midpoint falls in the supplied
     * range, indexed by the position of that block in the range-relative iteration order. The result
     * is what the deferred-extraction iterator hands to {@link OptimizedParquetColumnIterator} so
     * each emitted {@code _rowPosition} stays anchored to the FILE's prefix sum even when the
     * iterator only sees a subset of row groups. Index {@code length} holds the total file row
     * count, so the iterator can validate its own emit ranges if it ever needs to.
     */
    private static long[] computeRangeBlockFileGlobalOffsets(ParquetMetadata fullFooter, long rangeStart, long rangeEnd) {
        List<BlockMetaData> allBlocks = fullFooter.getBlocks();
        List<Long> ranged = new ArrayList<>();
        long sum = 0L;
        for (BlockMetaData block : allBlocks) {
            long midpoint = block.getStartingPos() + block.getCompressedSize() / 2;
            if (midpoint >= rangeStart && midpoint < rangeEnd) {
                ranged.add(sum);
            }
            sum += block.getRowCount();
        }
        long[] offsets = new long[ranged.size() + 1];
        for (int i = 0; i < ranged.size(); i++) {
            offsets[i] = ranged.get(i);
        }
        offsets[ranged.size()] = sum;
        return offsets;
    }

    private static IllegalArgumentException newInvalidParquetFileException(String uri, Exception e) {
        String detail = e.getMessage();
        if (detail == null || detail.isEmpty()) {
            detail = e.getClass().getSimpleName();
        }
        return new IllegalArgumentException("Could not read [" + uri + "] as a Parquet file: " + detail, e);
    }

    @Override
    public SourceMetadata metadata(StorageObject object) throws IOException {
        ParquetStorageObjectAdapter parquetInputFile = new ParquetStorageObjectAdapter(object, blockFactory.arrowAllocator());
        ParquetReadOptions options = readOptionsBuilder().build();

        try (ParquetFileReader reader = openParquetFileCached(object, parquetInputFile, options)) {
            FileMetaData fileMetaData = reader.getFileMetaData();
            MessageType parquetSchema = fileMetaData.getSchema();
            validateFooterIntegrity(object.path().toString(), parquetSchema, reader.getRowGroups());
            List<Attribute> schema = convertParquetSchemaToAttributes(parquetSchema);
            SourceStatistics statistics = extractStatistics(reader, schema);
            return new SimpleSourceMetadata(schema, formatName(), object.path().toString(), statistics, null);
        }
    }

    /**
     * Validates footer-level invariants that, if violated, indicate a corrupt file. Currently
     * checks that required columns (maxDefinitionLevel == 0) do not report non-zero null counts
     * in their row-group statistics. Such files pass parquet-mr footer parsing but produce
     * garbage or exceptions during data-page decoding.
     */
    static void validateFooterIntegrity(String uri, MessageType schema, List<BlockMetaData> rowGroups) {
        for (BlockMetaData rowGroup : rowGroups) {
            for (ColumnChunkMetaData col : rowGroup.getColumns()) {
                String[] path = col.getPath().toArray();
                ColumnDescriptor desc = schema.getColumnDescription(path);
                if (desc != null && desc.getMaxDefinitionLevel() == 0) {
                    Statistics<?> stats = col.getStatistics();
                    if (stats != null && stats.getNumNulls() > 0) {
                        throw new IllegalArgumentException(
                            "Could not read ["
                                + uri
                                + "] as a Parquet file: column ["
                                + col.getPath().toDotString()
                                + "] is declared required but row group reports "
                                + stats.getNumNulls()
                                + " null(s)"
                        );
                    }
                }
            }
        }
    }

    @SuppressWarnings("rawtypes")
    private SourceStatistics extractStatistics(ParquetFileReader reader, List<Attribute> attributes) {
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
        // Publish stats keyed by every dotted leaf the flattener produced an addressable
        // attribute for. Skip UNSUPPORTED — they will not bind to a planner attribute the
        // aggregate-pushdown layer can read stats off of, and over-depth / MAP / LIST<STRUCT>
        // groups surface as UNSUPPORTED here exactly so this filter removes them. Names match
        // the collection-loop's col.getPath().toDotString() exactly because
        // {@link #collectAttributes} concatenates the same Parquet child names with '.'.
        for (Attribute attribute : attributes) {
            if (attribute.dataType() == DataType.UNSUPPORTED) {
                continue;
            }
            String name = attribute.name();
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
        long startNanos = System.nanoTime();
        try {
            // The synthetic {@link ColumnExtractor#ROW_POSITION_COLUMN} flows through the regular
            // read path: {@link #buildProjectedAttributes} types it as LONG and {@link #buildColumnInfos}
            // recognises the slot, so the iterator emits per-row file-global identities the same way
            // it emits any other column. Pushed filters, late materialization, page skipping, and
            // row-group skipping all stay on — each surviving row carries its identity, and the
            // matching extractor binds those identities back to the file's full footer.
            ParquetStorageObjectAdapter parquetInputFile = new ParquetStorageObjectAdapter(object, blockFactory.arrowAllocator());
            long footerStartNanos = System.nanoTime();
            ParquetFileReader reader = openParquetFileCached(object, parquetInputFile, readOptionsBuilder().build());
            counters.addFooterRead(System.nanoTime() - footerStartNanos, sizeOrZero(object), reader.getFooter().getBlocks().size());
            return buildIterator(
                object,
                parquetInputFile,
                reader,
                context.projectedColumns(),
                context.batchSize(),
                context.rowLimit(),
                context.readSchema(),
                // For full-file reads the iterator's footer is the file's footer; the deferred
                // extractor scopes itself to the same set of row groups. {@link #readRange} below
                // threads the unranged footer separately so the extractor can address rows in the
                // file's full address space even on a range-restricted scan.
                reader.getFooter(),
                // Full-file reads need no per-block file-global offset override — the iterator's
                // own row-group ordering already matches the file footer.
                null,
                filter -> openParquetFileCached(object, parquetInputFile, readOptionsBuilder().withRecordFilter(filter).build())
            );
        } finally {
            // read_nanos covers the synchronous setup phase only; per-page decode/decompress time
            // is in the per-column counters, not here.
            counters.addTotalReadNanos(System.nanoTime() - startNanos);
        }
    }

    @Override
    public AggregatePushdownSupport aggregatePushdownSupport() {
        return new ParquetAggregatePushdownSupport();
    }

    @Override
    public boolean supportsWholeFileCompression() {
        return false;
    }

    @Override
    public String formatName() {
        return FormatNameResolver.FORMAT_PARQUET;
    }

    @Override
    public List<String> fileExtensions() {
        return List.of(".parquet", ".parq");
    }

    @Override
    public void close() throws IOException {
        // No resources to close at the reader level
    }

    @Override
    public List<SplitRange> discoverSplitRanges(StorageObject object) throws IOException {
        ParquetStorageObjectAdapter parquetInputFile = new ParquetStorageObjectAdapter(object, blockFactory.arrowAllocator());
        ParquetReadOptions options = readOptionsBuilder().build();
        try (ParquetFileReader reader = openParquetFileCached(object, parquetInputFile, options)) {
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
    public CloseableIterator<Page> readRange(StorageObject object, RangeReadContext context) throws IOException {
        long startNanos = System.nanoTime();
        try {
            long rangeStart = context.rangeStart();
            long rangeEnd = context.rangeEnd();

            // The synthetic {@link ColumnExtractor#ROW_POSITION_COLUMN} flows through the regular
            // range path: the iterator gets file-global per-row-group offsets from the format reader
            // (computed against the full footer) and emits identities that the matching extractor
            // resolves against the same full footer — independent of which split owns each row group.

            ParquetStorageObjectAdapter parquetInputFile = ParquetStorageObjectAdapter.forRange(
                object,
                rangeEnd - rangeStart,
                blockFactory.arrowAllocator()
            );
            ParquetReadOptions rangeOptions = readOptionsBuilder().withRange(rangeStart, rangeEnd).build();
            // Footer resolution order:
            // 1. context.fileContext() — per-producer fast path, single-writer/single-reader, no map
            // lookup; carries the footer across successive splits of the same file on one thread.
            // 2. ParsedParquetFooterCache — JVM-wide LRU keyed by (path, length); shared across
            // producer threads and across queries within the access TTL. The loader explicitly
            // uses unranged read options so the cached value is the full file footer (all row
            // groups) and is reusable by any split. The underlying FooterByteCache ensures the
            // tail bytes are fetched from storage only once on the first parse.
            // ParquetFileReader.open with a range only retains blocks whose midpoint falls in the
            // range, making getFooter() unusable for other splits — so we must always derive the
            // range-filtered metadata from the unranged full footer via filterBlocksByRange.
            ParquetMetadata fullFooter;
            if (context.fileContext() instanceof ParquetMetadata cachedFooter) {
                fullFooter = cachedFooter;
            } else {
                long footerStartNanos = System.nanoTime();
                fullFooter = loadFooter(object, parquetInputFile);
                counters.addFooterRead(System.nanoTime() - footerStartNanos, sizeOrZero(object), fullFooter.getBlocks().size());
            }
            context.setFileContext(fullFooter);
            ParquetMetadata rangeMetadata = filterBlocksByRange(fullFooter, rangeStart, rangeEnd);
            ParquetFileReader reader = openParquetFile(object, parquetInputFile, rangeOptions, rangeMetadata);
            // For range-restricted reads the iterator only sees a subset of the file's blocks, but
            // the deferred-extraction identities must remain file-global so the extractor can later
            // bind them to the full footer. Compute the per-range-block file-global offsets up front
            // and hand them to the iterator (it ignores them when the projection has no
            // {@code _rowPosition} column).
            long[] rangeBlockGlobalOffsets = context.projectedColumns() != null
                && context.projectedColumns().contains(ColumnExtractor.ROW_POSITION_COLUMN)
                    ? computeRangeBlockFileGlobalOffsets(fullFooter, rangeStart, rangeEnd)
                    : null;
            return buildIterator(
                object,
                parquetInputFile,
                reader,
                context.projectedColumns(),
                context.batchSize(),
                NO_LIMIT,
                context.resolvedAttributes(),
                // The deferred extractor scopes itself to the file's full footer rather than the
                // range-filtered subset, so the produced extractor can resolve any file-global
                // identity even one that lands outside this split's row groups.
                fullFooter,
                rangeBlockGlobalOffsets,
                // fullFooter was resolved (and stashed into the context) above, so the re-open path
                // always reuses it rather than risk a re-parse through the no-footer overload.
                filter -> openParquetFile(
                    object,
                    parquetInputFile,
                    readOptionsBuilder().withRange(rangeStart, rangeEnd).withRecordFilter(filter).build(),
                    filterBlocksByRange(fullFooter, rangeStart, rangeEnd)
                )
            );
        } finally {
            counters.addTotalReadNanos(System.nanoTime() - startNanos);
        }
    }

    /**
     * Hook used by the shared {@link #buildIterator} to re-open the underlying
     * {@link ParquetFileReader} with a record filter when the baseline (non-optimized) path
     * needs row-level filtering applied by parquet-mr. The caller closes the previous reader
     * before invoking the hook; the hook returns a fresh reader configured with the supplied
     * filter on top of whatever read options the originating {@link #read} or {@link #readRange}
     * call established (notably: {@code withRange} for {@link #readRange}). Centralising the
     * post-open construction lets {@code read} and {@code readRange} share filter resolution,
     * attribute derivation, and iterator construction without duplicating their divergent
     * setup logic.
     */
    @FunctionalInterface
    private interface FilteredReopener {
        ParquetFileReader reopen(FilterCompat.Filter recordFilter) throws IOException;
    }

    /**
     * Shared post-open machinery for {@link #read} and {@link #readRange}: resolves the pushed
     * predicate against the actual file schema, re-opens through {@code reopener} when the
     * baseline path requires record filtering, and instantiates the appropriate iterator
     * (optimized vs {@link ParquetColumnIterator}). The optimized path drives row-group
     * selection and page-level filtering itself via {@code RowGroupFilter +
     * ColumnIndexRowRangesComputer + PrefetchedRowGroupBuilder}, so parquet-mr never sees the
     * filter and the reader is not re-opened.
     *
     * @param resolvedAttributes planner-supplied attribute list for the column read path; when
     *                           {@code null} or empty, falls back to deriving attributes from
     *                           the file schema. {@link #read} always passes {@code null};
     *                           {@link #readRange} threads through the planner-resolved value
     *                           to skip redundant schema conversion across the splits of one
     *                           file (see {@code AsyncExternalSourceOperatorFactory}).
     * @param fullFooter         the unranged file footer used to scope deferred extraction.
     *                           {@link #read} passes {@code reader.getFooter()};
     *                           {@link #readRange} threads the cached unranged footer so the
     *                           extractor can resolve any file-global row identity even on
     *                           range-restricted reads.
     * @param rangeBlockGlobalOffsets per-row-group file-global first-row offsets when the
     *                                iterator only sees a subset of the file's row groups
     *                                (range-restricted read with {@code _rowPosition} in the
     *                                projection); {@code null} for full-file reads or when
     *                                {@code _rowPosition} is not requested.
     */
    private CloseableIterator<Page> buildIterator(
        StorageObject object,
        ParquetStorageObjectAdapter parquetInputFile,
        ParquetFileReader reader,
        List<String> projectedColumns,
        int batchSize,
        int rowLimit,
        List<Attribute> resolvedAttributes,
        ParquetMetadata fullFooter,
        long[] rangeBlockGlobalOffsets,
        FilteredReopener reopener
    ) throws IOException {
        counters.setLateMaterializationEnabled(lateMaterializationEnabled);
        try {
            FileMetaData fileMetaData = reader.getFileMetaData();
            MessageType parquetSchema = fileMetaData.getSchema();

            boolean useOptimized = optimizedReader && forceBaselinePath == false;
            FilterPredicate filterPredicate = resolveFilterPredicate(object, parquetSchema);
            if (filterPredicate != null) {
                counters.markPredicatePushdownUsed();
            }
            FilterCompat.Filter recordFilter = filterPredicate != null ? FilterCompat.get(filterPredicate) : resolveRecordFilter();

            // The optimized path drives row-group selection and page-level filtering itself
            // (via RowGroupFilter + ColumnIndexRowRangesComputer + PrefetchedRowGroupBuilder), so
            // parquet-mr never sees the filter and the reader does not need to be re-opened.
            if (useOptimized == false && FilterCompat.isFilteringRequired(recordFilter)) {
                reader.close();
                reader = reopener.reopen(recordFilter);
                fileMetaData = reader.getFileMetaData();
                parquetSchema = fileMetaData.getSchema();
            }
            // The framework passes planning-time resolved attributes for this query (AsyncExternalSourceOperatorFactory).
            // Reuse them to avoid redundant schema conversion work per split. We still read Parquet metadata to drive row groups.
            List<Attribute> attributes = resolvedAttributes != null && resolvedAttributes.isEmpty() == false
                ? resolvedAttributes
                : convertParquetSchemaToAttributes(parquetSchema);

            // Count-only fast path: when the planner asks for zero data columns (e.g. a query
            // that only references {@code _file.*} virtual columns or a bare {@code COUNT(*)}
            // atop the source) and no pushdown is active on this file, walk the row group
            // metadata and emit row-count-only pages instead of building a column iterator over
            // the entire file schema. Skipped when any predicate path is active (record filter,
            // FilterPredicate, or {@link ParquetPushedExpressions}) so we keep the row-group
            // pruning and YES-conjunct re-evaluation the column iterator performs - in those
            // cases the leak is plugged at the consumer side by
            // {@link org.elasticsearch.xpack.esql.datasources.VirtualColumnIterator} releasing
            // any surplus blocks the legacy "empty projection -> full schema" fallback emits.
            if (projectedColumns != null
                && projectedColumns.isEmpty()
                && filterPredicate == null
                && pushedExpressions == null
                && FilterCompat.isFilteringRequired(recordFilter) == false) {
                return new ParquetCountOnlyIterator(reader, batchSize, rowLimit);
            }
            List<Attribute> projectedAttributes = buildProjectedAttributes(projectedColumns, attributes);

            MessageType projectedSchema = buildProjectedSchema(parquetSchema, projectedAttributes);
            String createdBy = fileMetaData.getCreatedBy();
            boolean hasRecordFilter = forceBaselinePath || FilterCompat.isFilteringRequired(recordFilter);
            recordPerColumnMaterialization(projectedAttributes, useOptimized);
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
                    recordFilter,
                    rangeBlockGlobalOffsets,
                    fullFooter
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
                hasRecordFilter,
                rangeBlockGlobalOffsets,
                counters
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
        FilterCompat.Filter recordFilter,
        long[] rowGroupFirstRowGlobalOverride,
        ParquetMetadata fullFooter
    ) {
        if (inputFile instanceof ParquetStorageObjectAdapter == false) {
            throw new ElasticsearchException(
                "optimized_reader requires ParquetStorageObjectAdapter but got [" + inputFile.getClass().getName() + "]"
            );
        }
        ParquetStorageObjectAdapter adapter = (ParquetStorageObjectAdapter) inputFile;
        ColumnInfo[] columnInfos = buildColumnInfos(projectedSchema, projectedAttributes);
        validatePlannerTypesAgainstFile(logger, storageObject.path().toString(), reader, projectedAttributes, columnInfos);

        // Pass the predicate column names so the metadata preload also batch-fetches dictionary
        // pages (and bloom filters when their length is known) for those columns. Without this,
        // RowGroupFilter would issue one synchronous range GET per row group per predicate column,
        // dominating wall time on remote storage. The pre-fetched bytes are then installed on the
        // adapter so the subsequent reads are served from memory.
        // Note: when the filter is supplied via the legacy FilterPredicateCompat path (no
        // pushedExpressions), we cannot resolve predicate column names here and fall back to the
        // existing per-row-group sync read path inside parquet-mr's RowGroupFilter.
        Set<String> predicateColumnPaths = recordFilter != null && pushedExpressions != null
            ? pushedExpressions.predicateColumnNames()
            : null;
        if (predicateColumnPaths != null) {
            counters.addPredicateColumns(predicateColumnPaths);
        }

        // Gate ColumnIndex/OffsetIndex prefetch to the columns a plan actually consumes (see
        // computeIndexColumnPaths). A full scan with no filter and no threshold consumes none of
        // them, so this emits zero index ranges.
        IndexColumnPaths indexColumnPaths = computeIndexColumnPaths(
            FilterCompat.isFilteringRequired(recordFilter),
            filterPredicate != null,
            predicateColumnPaths,
            dynamicThreshold != null ? dynamicThreshold.columnName() : null,
            projectedSchema
        );

        PreloadedRowGroupMetadata preloadedMetadata = PreloadedRowGroupMetadata.preload(
            reader,
            storageObject,
            predicateColumnPaths,
            indexColumnPaths.columnIndexPaths(),
            indexColumnPaths.offsetIndexPaths(),
            blockFactory.arrowAllocator()
        );
        boolean metadataHandedOff = false;
        try {
            adapter.installPreWarmedChunks(preloadedMetadata.preWarmedChunks());

            List<BlockMetaData> blocks;
            boolean[] survivingRowGroups;
            try {
                blocks = reader.getRowGroups();
                survivingRowGroups = computeSurvivingRowGroups(reader, blocks, recordFilter, projectedSchema, counters);
            } finally {
                // Detach the pre-warmed chunks from the adapter so subsequent reads on any
                // WindowedSeekableInputStream skip the cache lookup. The ByteBuffers themselves remain
                // reachable via preloadedMetadata for the iterator's lifetime, but the data path uses
                // the async ColumnChunkPrefetcher rather than the sliding-window stream, so they have
                // no further reader.
                adapter.installPreWarmedChunks(null);
            }

            RowRanges[] allRowRanges = null;
            if (filterPredicate != null) {
                counters.markPageIndexUsed();
                allRowRanges = new RowRanges[blocks.size()];
                long rowsInKept = 0;
                long rowsAfter = 0;
                for (int i = 0; i < blocks.size(); i++) {
                    // Row groups dropped by stats/dictionary/bloom filters will never be opened by
                    // the iterator, so there is no need to compute their column-index row ranges.
                    if (survivingRowGroups != null && survivingRowGroups[i] == false) {
                        continue;
                    }
                    long rgRows = blocks.get(i).getRowCount();
                    rowsInKept += rgRows;
                    allRowRanges[i] = ColumnIndexRowRangesComputer.compute(filterPredicate, preloadedMetadata, i, rgRows);
                    rowsAfter += allRowRanges[i] != null ? allRowRanges[i].selectedRowCount() : rgRows;
                }
                counters.addPageIndexRows(rowsInKept, rowsAfter);
            }

            // The iterator owns the late-mat decision: it gates on the structural prerequisite
            // hasProjectionOnlyColumns (nothing to defer-decode otherwise) and gates the more expensive
            // two-phase prefetch on its own predicate-byte-ratio threshold (see
            // {@link OptimizedParquetColumnIterator#shouldUseTwoPhase} /
            // {@link OptimizedParquetColumnIterator#TWO_PHASE_PREDICATE_BYTE_RATIO_THRESHOLD}). A second
            // file-level byte-ratio gate here was a leftover from the Pushability.RECHECK era when every
            // surviving row paid the predicate cost twice (once in late-mat, once again in FilterExec).
            // With WildcardLike now pushed as Pushability.YES, FilterExec is dropped for that conjunct,
            // so suppressing late-mat at the file level would leak unfiltered rows past the source.
            ParquetPushedExpressions effectivePushed = lateMaterializationEnabled ? pushedExpressions : null;
            if (effectivePushed != null) {
                counters.markLateMaterializationUsed();
            }

            // Reuse the FilterPredicate already resolved at the file level so the trivially-passes
            // guard sees the same predicate that drove row-group pruning and column-index RowRanges.
            // Only pass it through when late materialization is actually active; otherwise the
            // iterator has no use for it.
            //
            // Additionally suppress the predicate when any YES conjunct in pushedExpressions did not
            // translate to a FilterPredicate (today: WildcardLike/Not(WildcardLike) AND'd with a
            // translatable conjunct). The trivially-passes shortcut would bypass the late-mat
            // evaluator on the strength of the FilterPredicate alone, but the missing YES conjunct
            // is no longer in FilterExec to catch the over-inclusion — so dropping the guard here
            // would silently leak rows that don't match the YES conjunct (e.g. URL LIKE "x*" rows
            // outside the prefix when AND'd with a stats-trivial status = 200).
            //
            // DO NOT REMOVE the hasYesConjunctOutsideFilterPredicate check — it is load-bearing for
            // correctness of YES-pushed predicates that have no parquet-FilterPredicate translation.
            // The integration regression test
            // OptimizedFilteredReaderTests.testPushedExpressionsLikeWithStatsTrivialEqDoesNotLeak
            // and the unit tests in ParquetPushedExpressionsTests cover this contract.
            MessageType fileSchema = reader.getFileMetaData().getSchema();
            FilterPredicate triviallyPassesPredicate = effectivePushed != null
                && filterPredicate != null
                && (pushedExpressions == null || pushedExpressions.hasYesConjunctOutsideFilterPredicate(fileSchema) == false)
                    ? filterPredicate
                    : null;

            OptimizedParquetColumnIterator iterator = new OptimizedParquetColumnIterator(
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
                rowGroupFirstRowGlobalOverride,
                codecFactory,
                effectivePushed,
                triviallyPassesPredicate,
                this,
                fullFooter,
                dynamicThreshold,
                resolveDynamicThresholdColumn(fileSchema, dynamicThreshold),
                counters
            );
            // Constructor succeeded — iterator now owns preloadedMetadata. Set the flag after
            // construction so that a throw inside the constructor does not suppress cleanup.
            metadataHandedOff = true;
            return iterator;
        } finally {
            if (metadataHandedOff == false) {
                preloadedMetadata.close();
            }
        }
    }

    /**
     * The dot-string column paths whose ColumnIndex / OffsetIndex should be prefetched. A
     * {@code null} set means "unrestricted" — fetch the index for every column (legacy behavior).
     */
    record IndexColumnPaths(Set<String> columnIndexPaths, Set<String> offsetIndexPaths) {}

    /**
     * Restricts page-index prefetch to the columns a plan actually consumes, so a full scan does not
     * pay for ColumnIndex/OffsetIndex bytes nothing reads. Index ranges are only used when a Parquet
     * {@link FilterPredicate} drives {@code RowRanges} ({@code pageRangeFilterActive}) or for the
     * dynamic-threshold sort column. Returns {@code null} sets (unrestricted, legacy behavior) when a
     * filter is active but its predicate columns can't be enumerated ({@code predicateColumnPaths == null}).
     */
    static IndexColumnPaths computeIndexColumnPaths(
        boolean filteringRequired,
        boolean pageRangeFilterActive,
        Set<String> predicateColumnPaths,
        String thresholdColumn,
        MessageType projectedSchema
    ) {
        if (filteringRequired && predicateColumnPaths == null) {
            return new IndexColumnPaths(null, null);
        }
        Set<String> columnIndexPaths = new HashSet<>();
        Set<String> offsetIndexPaths = new HashSet<>();
        if (pageRangeFilterActive && predicateColumnPaths != null) {
            columnIndexPaths.addAll(predicateColumnPaths);
            offsetIndexPaths.addAll(predicateColumnPaths);
        }
        if (thresholdColumn != null) {
            columnIndexPaths.add(thresholdColumn);
            offsetIndexPaths.add(thresholdColumn);
        }
        if (pageRangeFilterActive) {
            for (ColumnDescriptor descriptor : projectedSchema.getColumns()) {
                offsetIndexPaths.add(String.join(".", descriptor.getPath()));
            }
        }
        return new IndexColumnPaths(columnIndexPaths, offsetIndexPaths);
    }

    private static ColumnDescriptor resolveDynamicThresholdColumn(MessageType schema, DynamicThreshold dynamicThreshold) {
        if (dynamicThreshold == null) {
            return null;
        }
        String columnName = dynamicThreshold.columnName();
        for (ColumnDescriptor descriptor : schema.getColumns()) {
            String[] path = descriptor.getPath();
            if (String.join(".", path).equals(columnName) || (path.length == 1 && path[0].equals(columnName))) {
                return descriptor;
            }
        }
        return null;
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
        MessageType schema,
        ParquetReaderCounters counters
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
        // parquet-mr's RowGroupFilter returns only the combined survivor set, not which level
        // (stats / dictionary / bloom) rejected a group, so we track total and kept only.
        for (int i = 0; i < blocks.size(); i++) {
            counters.addRowGroupFiltered(flags[i]);
        }
        return flags;
    }

    /**
     * Maps each projected attribute to its {@link ColumnInfo}. Resolution is path-aware:
     * <ol>
     *   <li>Exact match against a top-level field name in {@code projectedSchema} (preserves
     *       files whose top-level fields literally contain a dot).</li>
     *   <li>Otherwise, the attribute name is interpreted as a dotted path and looked up against
     *       the full leaf paths of {@code projectedSchema}.</li>
     * </ol>
     * Top-level fields are recognised via {@link MessageType#containsField}; leaf paths are
     * joined with {@code "."} (mirroring {@link ColumnChunkPrefetcher}'s prefetch key).
     */
    static ColumnInfo[] buildColumnInfos(MessageType projectedSchema, List<Attribute> attributes) {
        ColumnInfo[] columnInfos = new ColumnInfo[attributes.size()];
        // Per-top-level-field descriptors handle LIST<primitive> and other cases where the
        // descriptor's first path segment is the user-visible field name. This map wins over
        // the dotted-path map and is the only source of truth for literal-dot top-level names.
        Map<String, ColumnDescriptor> descByTopLevel = new HashMap<>();
        Map<String, ColumnDescriptor> descByDottedPath = new HashMap<>();
        Set<String> topLevelNames = new HashSet<>();
        for (Type field : projectedSchema.getFields()) {
            topLevelNames.add(field.getName());
        }
        for (ColumnDescriptor desc : projectedSchema.getColumns()) {
            String[] path = desc.getPath();
            descByDottedPath.put(String.join(".", path), desc);
            if (path.length > 0 && topLevelNames.contains(path[0])) {
                descByTopLevel.putIfAbsent(path[0], desc);
            }
        }
        for (int i = 0; i < attributes.size(); i++) {
            Attribute attr = attributes.get(i);
            // Synthetic file-global row position. The iterator fills its slot from the row group
            // index + the per-row position info already in scope on each emit path; there is no
            // Parquet column to bind, so the descriptor stays null.
            if (ColumnExtractor.ROW_POSITION_COLUMN.equals(attr.name())) {
                columnInfos[i] = ColumnInfo.rowPosition();
                continue;
            }
            if (attr.dataType() == DataType.NULL || attr.dataType() == DataType.UNSUPPORTED) {
                continue;
            }
            ColumnDescriptor desc = descByTopLevel.get(attr.name());
            if (desc == null) {
                desc = descByDottedPath.get(attr.name());
            }
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

    private List<Attribute> buildProjectedAttributes(List<String> projectedColumns, List<Attribute> fileAttributes) {
        if (projectedColumns == null || projectedColumns.isEmpty()) {
            return fileAttributes;
        }
        Map<String, Attribute> attributeMap = new HashMap<>();
        for (Attribute attr : fileAttributes) {
            attributeMap.put(attr.name(), attr);
        }
        List<Attribute> result = new ArrayList<>();
        Set<String> included = new HashSet<>(projectedColumns);
        for (String columnName : projectedColumns) {
            Attribute attr = attributeMap.get(columnName);
            if (attr == null) {
                // The deferred-extraction synthetic column has no presence in the file's schema;
                // the iterator materialises it. We must give it a real {@link DataType#LONG} so
                // {@link #buildColumnInfos} routes it to {@link ColumnInfo#rowPosition()} instead
                // of skipping it as an absent column. The row-position column is always materialised
                // (non-nullable); an absent file column is always null at runtime (nullable).
                boolean isRowPosition = ColumnExtractor.ROW_POSITION_COLUMN.equals(columnName);
                DataType type = isRowPosition ? DataType.LONG : DataType.NULL;
                Nullability nullability = isRowPosition ? Nullability.FALSE : Nullability.TRUE;
                attr = new ReferenceAttribute(Source.EMPTY, null, columnName, type, nullability, null, false);
            }
            result.add(attr);
        }
        if (pushedExpressions != null) {
            for (String predCol : pushedExpressions.predicateColumnNames()) {
                if (included.contains(predCol) == false && attributeMap.containsKey(predCol)) {
                    result.add(attributeMap.get(predCol));
                    included.add(predCol);
                }
            }
        }
        return result;
    }

    private static MessageType buildProjectedSchema(MessageType fullSchema, List<Attribute> projectedAttributes) {
        // Resolve each attribute to a path of segments. Per D2, exact top-level match wins over
        // dotted-path traversal so that literal-dot top-level field names keep resolving to
        // themselves. Each entry maps the resolved top-level field name -> ordered, deduplicated
        // child paths (each a list of segments below that top-level field; empty for a literal
        // top-level match).
        Map<String, LinkedHashMap<List<String>, Boolean>> topLevelToChildPaths = new LinkedHashMap<>();
        for (Attribute attr : projectedAttributes) {
            String name = attr.name();
            if (fullSchema.containsField(name)) {
                topLevelToChildPaths.computeIfAbsent(name, k -> new LinkedHashMap<>()).put(List.of(), Boolean.TRUE);
                continue;
            }
            // Try dotted-path traversal: walk prefixes from shortest to longest and stop at the
            // first one that resolves to a STRUCT group whose remainder also matches. The literal
            // top-level name already won above; here we only deal with synthetic dotted names.
            // Walk dot positions directly with substring rather than splitting + re-joining each
            // probe; the segments array is still computed once for the remainder.
            String[] segments = name.split("\\.");
            int dotIdx = name.indexOf('.');
            int prefixLen = 1;
            while (dotIdx >= 0) {
                String topLevel = name.substring(0, dotIdx);
                if (fullSchema.containsField(topLevel)) {
                    Type field = fullSchema.getType(topLevel);
                    if (field.isPrimitive()) {
                        break;
                    }
                    List<String> remainder = List.of(Arrays.copyOfRange(segments, prefixLen, segments.length));
                    if (groupContainsPath(field.asGroupType(), remainder, 0)) {
                        topLevelToChildPaths.computeIfAbsent(topLevel, k -> new LinkedHashMap<>()).put(remainder, Boolean.TRUE);
                        break;
                    }
                }
                dotIdx = name.indexOf('.', dotIdx + 1);
                prefixLen++;
            }
        }
        List<Type> projectedFields = new ArrayList<>();
        for (Map.Entry<String, LinkedHashMap<List<String>, Boolean>> entry : topLevelToChildPaths.entrySet()) {
            Type field = fullSchema.getType(entry.getKey());
            // A literal top-level match (List.of()) reads the whole field.
            if (entry.getValue().containsKey(List.<String>of())) {
                projectedFields.add(field);
                continue;
            }
            if (field.isPrimitive()) {
                projectedFields.add(field);
                continue;
            }
            Type subset = buildSubsetGroup(field.asGroupType(), new ArrayList<>(entry.getValue().keySet()));
            projectedFields.add(subset != null ? subset : field);
        }
        // Parquet requires at least one field; fall back to full schema when none match
        if (projectedFields.isEmpty()) {
            return fullSchema;
        }
        return new MessageType(fullSchema.getName(), projectedFields);
    }

    /**
     * Resolves a (possibly dotted) attribute name to its {@link Type} in {@code fullSchema}.
     * Applies D2: literal top-level match wins over dotted-path traversal. Returns {@code null}
     * when the name has no match.
     */
    private static Type resolveFieldType(MessageType fullSchema, String name) {
        if (fullSchema.containsField(name)) {
            return fullSchema.getType(name);
        }
        String[] segments = name.split("\\.");
        int dotIdx = name.indexOf('.');
        int prefixLen = 1;
        while (dotIdx >= 0) {
            String topLevel = name.substring(0, dotIdx);
            if (fullSchema.containsField(topLevel)) {
                Type field = fullSchema.getType(topLevel);
                for (int i = prefixLen; i < segments.length; i++) {
                    if (field.isPrimitive()) {
                        return null;
                    }
                    GroupType group = field.asGroupType();
                    if (group.containsField(segments[i]) == false) {
                        return null;
                    }
                    field = group.getType(segments[i]);
                }
                return field;
            }
            dotIdx = name.indexOf('.', dotIdx + 1);
            prefixLen++;
        }
        return null;
    }

    private static boolean groupContainsPath(GroupType group, List<String> path, int idx) {
        if (idx >= path.size()) {
            return true;
        }
        String name = path.get(idx);
        if (group.containsField(name) == false) {
            return false;
        }
        Type child = group.getType(name);
        if (idx == path.size() - 1) {
            return true;
        }
        if (child.isPrimitive()) {
            return false;
        }
        return groupContainsPath(child.asGroupType(), path, idx + 1);
    }

    /**
     * Builds a projected copy of {@code group} containing only the leaves reachable via {@code paths}
     * (each path is the list of segments below {@code group}). The original {@link Type.Repetition}
     * is preserved on every reconstructed group so parquet-mr doesn't complain at read time. Returns
     * {@code null} when {@code paths} is empty.
     */
    private static Type buildSubsetGroup(GroupType group, List<List<String>> paths) {
        if (paths.isEmpty()) {
            return null;
        }
        // Group paths by first segment to recurse children once per branch, preserving insertion order.
        LinkedHashMap<String, List<List<String>>> byHead = new LinkedHashMap<>();
        for (List<String> path : paths) {
            if (path.isEmpty()) {
                // A leaf request at this group: include the whole group.
                return group;
            }
            byHead.computeIfAbsent(path.get(0), k -> new ArrayList<>()).add(path.subList(1, path.size()));
        }
        Types.GroupBuilder<GroupType> builder = Types.buildGroup(group.getRepetition());
        if (group.getLogicalTypeAnnotation() != null) {
            builder = builder.as(group.getLogicalTypeAnnotation());
        }
        for (Map.Entry<String, List<List<String>>> entry : byHead.entrySet()) {
            String childName = entry.getKey();
            if (group.containsField(childName) == false) {
                continue;
            }
            Type child = group.getType(childName);
            List<List<String>> childPaths = entry.getValue();
            boolean wholeChildRequested = childPaths.stream().anyMatch(List::isEmpty);
            if (wholeChildRequested || child.isPrimitive()) {
                builder = builder.addField(child);
            } else {
                Type subset = buildSubsetGroup(child.asGroupType(), childPaths);
                if (subset != null) {
                    builder = builder.addField(subset);
                }
            }
        }
        return builder.named(group.getName());
    }

    /**
     * Maximum recursion depth for nested STRUCT flattening. Beyond this, the offending group
     * surfaces as a single {@link DataType#UNSUPPORTED} attribute at its dotted path and a
     * DEBUG log entry is emitted; no infinite recursion or partial flattening occurs.
     *
     * <p>Depth counts from 1 at the schema's top-level children, so the deepest reachable
     * group sits at depth {@code MAX_STRUCT_FLATTENING_DEPTH}. The ORC flattener uses the
     * same convention so the two formats accept the same set of valid paths.
     */
    static final int MAX_STRUCT_FLATTENING_DEPTH = 64;

    /**
     * Recursively converts a Parquet {@link MessageType} into ESQL {@link Attribute}s, flattening
     * nested STRUCT groups into dotted attribute names (e.g. {@code event.action}). Primitive
     * fields and {@code LIST<primitive>} groups emit at their parent's dotted path; other groups
     * (MAP, {@code LIST<STRUCT>}, UNION, anything not understood) surface as a single
     * {@link DataType#UNSUPPORTED} attribute at the group's dotted path.
     *
     * <p>Recursion is bounded by {@link #MAX_STRUCT_FLATTENING_DEPTH}; groups deeper than the cap
     * are emitted as a single UNSUPPORTED attribute and a DEBUG log line is recorded.
     *
     * <p>Resolution rule for dotted projected names (see {@link #buildColumnInfos}): exact
     * top-level match against the file schema is attempted first, then dotted-path traversal.
     * This preserves files whose top-level field literally contains a dot — the ES|QL parser
     * cannot distinguish {@code event.action} from {@code `event.action`} so we cannot
     * disambiguate at parse time.
     *
     * <p>Nullability follows Parquet repetition: a dotted leaf is {@link Nullability#FALSE} iff
     * every field on the path is {@link Type.Repetition#REQUIRED}. Anything else
     * ({@link Type.Repetition#OPTIONAL} or {@link Type.Repetition#REPEATED} anywhere on the path)
     * is nullable from the planner's perspective. Element-level nullability inside a {@code LIST}
     * group is independent and not modelled here. Defaulting everything to non-nullable — as the
     * 3-arg {@link ReferenceAttribute} constructor does — would cause planner rules
     * (e.g. {@code COALESCE} simplification, {@code IS NULL}/{@code IS NOT NULL} rewriting) to
     * drop legitimate null rows for {@code OPTIONAL} columns.
     */
    private List<Attribute> convertParquetSchemaToAttributes(MessageType schema) {
        List<Attribute> attributes = new ArrayList<>();
        for (Type field : schema.getFields()) {
            collectAttributes(field, field.getName(), 1, field.isRepetition(Type.Repetition.REQUIRED), attributes);
        }
        return attributes;
    }

    private void collectAttributes(Type field, String dottedPath, int depth, boolean pathAllRequired, List<Attribute> out) {
        if (depth > MAX_STRUCT_FLATTENING_DEPTH) {
            logger.debug(
                "Parquet field [{}] exceeds STRUCT flattening depth cap [{}]; emitting as UNSUPPORTED",
                dottedPath,
                MAX_STRUCT_FLATTENING_DEPTH
            );
            out.add(new ReferenceAttribute(Source.EMPTY, null, dottedPath, DataType.UNSUPPORTED, Nullability.TRUE, null, false));
            return;
        }
        Nullability leafNullability = pathAllRequired ? Nullability.FALSE : Nullability.TRUE;
        if (field.isPrimitive()) {
            out.add(new ReferenceAttribute(Source.EMPTY, null, dottedPath, convertParquetTypeToEsql(field), leafNullability, null, false));
            return;
        }
        GroupType group = field.asGroupType();
        LogicalTypeAnnotation logical = group.getLogicalTypeAnnotation();
        if (logical instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation
            || logical instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
            out.add(new ReferenceAttribute(Source.EMPTY, null, dottedPath, convertGroupTypeToEsql(group), leafNullability, null, false));
            return;
        }
        for (Type child : group.getFields()) {
            boolean childAllRequired = pathAllRequired && child.isRepetition(Type.Repetition.REQUIRED);
            collectAttributes(child, dottedPath + "." + child.getName(), depth + 1, childAllRequired, out);
        }
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
                if (logical instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogical) {
                    if (intLogical.isSigned() == false && intLogical.getBitWidth() == 32) {
                        // Widen to long
                        yield DataType.LONG;
                    }
                } else if (logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                    yield DataType.DATETIME;
                } else if (logical instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    yield DataType.DOUBLE;
                }
                yield DataType.INTEGER;
            }
            case INT64 -> {
                if (logical instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogical) {
                    if (intLogical.isSigned() == false && intLogical.getBitWidth() == 64) {
                        yield DataType.UNSIGNED_LONG;
                    }
                } else if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
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
     * Handles Parquet group types. Supports LIST of primitives by extracting the element type;
     * everything else (MAP, LIST&lt;STRUCT&gt;, UNION) is UNSUPPORTED.
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
            Type resolved = resolveFieldType(fullSchema, attr.name());
            if (resolved == null) {
                continue;
            }
            DataType actualInFile = convertParquetTypeToEsql(resolved);
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
        private final ParquetReaderCounters counters;
        private int rowBudget;

        /** Per-attribute column metadata; null for attributes not present in the file. */
        private final ColumnInfo[] columnInfos;
        private final boolean hasListColumns;
        /**
         * File-global first-row offsets per row group; mirrors the field of the same name on
         * {@link OptimizedParquetColumnIterator}. {@code null} when the projection has no
         * {@code _rowPosition} column.
         */
        private final long[] rowGroupFirstRowGlobal;
        /** Index of the {@code _rowPosition} slot in {@link #columnInfos}, or {@code -1}. */
        private final int rowPositionColumnIndex;

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
            boolean hasRecordFilter,
            long[] rowGroupFirstRowGlobalOverride,
            ParquetReaderCounters counters
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
            this.counters = counters;

            reader.setRequestedSchema(projectedSchema);

            this.columnInfos = buildColumnInfos(projectedSchema, attributes);
            boolean foundListCol = false;
            int rowPosIdx = -1;
            for (int i = 0; i < columnInfos.length; i++) {
                if (columnInfos[i] != null) {
                    if (columnInfos[i].isRowPosition()) {
                        rowPosIdx = i;
                    } else if (columnInfos[i].maxRepLevel() > 0) {
                        foundListCol = true;
                    }
                }
            }
            this.hasListColumns = foundListCol;
            this.rowPositionColumnIndex = rowPosIdx;
            // The baseline iterator uses parquet-mr's readNextFilteredRowGroup, which does
            // stats-based row-group skipping when a record filter is set. Skipped row groups
            // would break our rowGroupOrdinal++ tracking and yield wrong file-global ids.
            // Production deferred extraction always goes through OptimizedParquetColumnIterator,
            // which drives row-group iteration itself; this guard keeps the baseline path honest
            // for tests and explicitly-forced fallbacks.
            if (rowPosIdx >= 0 && hasRecordFilter) {
                throw new IllegalStateException(
                    "Parquet baseline iterator cannot emit ["
                        + ColumnExtractor.ROW_POSITION_COLUMN
                        + "] when a record filter is active; route this read through the optimized iterator"
                );
            }
            // For full-file reads the prefix sum can be derived from the reader's blocks. For
            // range-restricted reads the caller passes the file-global offsets so the emitted
            // identities match the full-file address space the extractor uses.
            if (rowPosIdx >= 0) {
                if (rowGroupFirstRowGlobalOverride != null) {
                    this.rowGroupFirstRowGlobal = rowGroupFirstRowGlobalOverride;
                } else {
                    List<BlockMetaData> blocks = reader.getRowGroups();
                    long[] offsets = new long[blocks.size() + 1];
                    long sum = 0L;
                    for (int i = 0; i < blocks.size(); i++) {
                        offsets[i] = sum;
                        sum += blocks.get(i).getRowCount();
                    }
                    offsets[blocks.size()] = sum;
                    this.rowGroupFirstRowGlobal = offsets;
                }
            } else {
                this.rowGroupFirstRowGlobal = null;
            }
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
                throw new IllegalArgumentException(
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
                    if (ci != null && ci.isRowPosition() == false && ci.maxRepLevel() == 0) {
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
                    if (columnInfos[i] != null && columnInfos[i].isRowPosition() == false) {
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
            // Pre-decrement snapshot of the in-block index of this batch's first row. Used to
            // synthesise the {@code _rowPosition} block when present in the projection. The
            // baseline path requires hasRecordFilter==false (enforced in the constructor), so
            // rowGroupOrdinal is always the physical block index here.
            int firstRowOfBatchInRG = (int) (rowGroup.getRowCount() - rowsRemainingInGroup);

            Block[] blocks = new Block[attributes.size()];
            try {
                for (int col = 0; col < columnInfos.length; col++) {
                    ColumnInfo info = columnInfos[col];
                    if (info == null) {
                        blocks[col] = blockFactory.newConstantNullBlock(rowsToRead);
                    } else if (info.isRowPosition()) {
                        long base = rowGroupFirstRowGlobal[rowGroupOrdinal] + firstRowOfBatchInRG;
                        long[] values = new long[rowsToRead];
                        for (int i = 0; i < rowsToRead; i++) {
                            values[i] = base + i;
                        }
                        blocks[col] = blockFactory.newLongArrayVector(values, rowsToRead).asBlock();
                    } else {
                        try {
                            if (pageColumnReaders != null && pageColumnReaders[col] != null) {
                                blocks[col] = pageColumnReaders[col].readBatch(rowsToRead, blockFactory);
                            } else {
                                blocks[col] = readColumnBlock(columnReaders[col], info, rowsToRead, col);
                            }
                        } catch (CircuitBreakingException e) {
                            Releasables.closeExpectNoException(blocks);
                            throw e;
                        } catch (Exception e) {
                            Releasables.closeExpectNoException(blocks);
                            Attribute attr = attributes.get(col);
                            throw new IllegalArgumentException(
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
            } catch (IllegalArgumentException | CircuitBreakingException e) {
                throw e;
            } catch (Exception e) {
                Releasables.closeExpectNoException(blocks);
                throw new IllegalArgumentException(
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
            counters.addRowsEmitted(rowsToRead);
            return new Page(blocks);
        }

        private Block readColumnBlock(ColumnReader cr, ColumnInfo info, int rowsToRead, int colIndex) {
            if (info.maxRepLevel() > 0) {
                return ParquetColumnDecoding.readListColumn(cr, info, rowsToRead, blockFactory);
            }
            // WARNING: the dispatching logic below is duplicated in PageColumnReader#readBatch
            // KEEP IN SYNC!
            return switch (info.esqlType()) {
                case BOOLEAN -> readBooleanColumn(cr, info.maxDefLevel(), rowsToRead);
                case INTEGER -> readIntColumn(cr, info.maxDefLevel(), rowsToRead);
                case LONG, UNSIGNED_LONG -> {
                    var logicalType = (LogicalTypeAnnotation.IntLogicalTypeAnnotation) info.logicalType();
                    if (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32) {
                        // A plain INT32 with no logical-type annotation is historically "signed"
                        yield readInt32WidenedToLongColumn(
                            cr,
                            info.maxDefLevel(),
                            rowsToRead,
                            logicalType == null || logicalType.isSigned()
                        );
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
            boolean[] values = UninitializedArrays.newBooleanArray(rows);
            BitSet isNull = maxDef > 0 ? new BitSet(rows) : null;
            boolean noNulls = true;
            for (int i = 0; i < rows; i++) {
                if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                    isNull.set(i);
                    noNulls = false;
                } else {
                    values[i] = cr.getBoolean();
                }
                cr.consume();
            }
            if (noNulls) {
                return blockFactory.newBooleanArrayVector(values, rows).asBlock();
            }
            return blockFactory.newBooleanArrayBlock(values, rows, null, isNull, Block.MvOrdering.UNORDERED);
        }

        private Block readIntColumn(ColumnReader cr, int maxDef, int rows) {
            int[] values = UninitializedArrays.newIntArray(rows);
            BitSet isNull = maxDef > 0 ? new BitSet(rows) : null;
            boolean noNulls = true;
            for (int i = 0; i < rows; i++) {
                if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                    isNull.set(i);
                    noNulls = false;
                } else {
                    values[i] = cr.getInteger();
                }
                cr.consume();
            }
            if (noNulls) {
                return blockFactory.newIntArrayVector(values, rows).asBlock();
            }
            return blockFactory.newIntArrayBlock(values, rows, null, isNull, Block.MvOrdering.UNORDERED);
        }

        /**
         * Parquet INT32 columns do not support {@link ColumnReader#getLong()}; widen safely to long for planner LONG.
         */
        private Block readInt32WidenedToLongColumn(ColumnReader cr, int maxDef, int rows, boolean signed) {
            long[] values = UninitializedArrays.newLongArray(rows);
            BitSet isNull = maxDef > 0 ? new BitSet(rows) : null;
            boolean noNulls = true;
            for (int i = 0; i < rows; i++) {
                if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                    isNull.set(i);
                    noNulls = false;
                } else {
                    values[i] = signed ? cr.getInteger() : Integer.toUnsignedLong(cr.getInteger());
                }
                cr.consume();
            }
            return ColumnBlockConversions.longColumn(blockFactory, values, rows, noNulls, false, isNull, false);
        }

        private Block readLongColumn(ColumnReader cr, int maxDef, int rows) {
            long[] values = UninitializedArrays.newLongArray(rows);
            BitSet isNull = maxDef > 0 ? new BitSet(rows) : null;
            boolean noNulls = true;
            for (int i = 0; i < rows; i++) {
                if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                    isNull.set(i);
                    noNulls = false;
                } else {
                    values[i] = cr.getLong();
                }
                cr.consume();
            }
            return ColumnBlockConversions.longColumn(blockFactory, values, rows, noNulls, false, isNull, false);
        }

        private Block readDoubleColumn(ColumnReader cr, ColumnInfo info, int rows) {
            LogicalTypeAnnotation logical = info.logicalType();
            if (logical instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
                return readDecimalAsDoubleColumn(cr, info, decimal.getScale(), rows);
            }
            if (logical instanceof LogicalTypeAnnotation.Float16LogicalTypeAnnotation) {
                return readFloat16Column(cr, info.maxDefLevel(), rows);
            }
            double[] values = UninitializedArrays.newDoubleArray(rows);
            BitSet isNull = info.maxDefLevel() > 0 ? new BitSet(rows) : null;
            boolean noNulls = true;
            boolean isFloat = info.parquetType() == PrimitiveType.PrimitiveTypeName.FLOAT;
            for (int i = 0; i < rows; i++) {
                if (info.maxDefLevel() > 0 && cr.getCurrentDefinitionLevel() < info.maxDefLevel()) {
                    isNull.set(i);
                    noNulls = false;
                } else {
                    values[i] = isFloat ? cr.getFloat() : cr.getDouble();
                }
                cr.consume();
            }
            return ColumnBlockConversions.doubleColumn(blockFactory, values, rows, noNulls, false, isNull, false);
        }

        private Block readDecimalAsDoubleColumn(ColumnReader cr, ColumnInfo info, int scale, int rows) {
            double[] values = UninitializedArrays.newDoubleArray(rows);
            BitSet isNull = info.maxDefLevel() > 0 ? new BitSet(rows) : null;
            boolean noNulls = true;
            for (int i = 0; i < rows; i++) {
                if (info.maxDefLevel() > 0 && cr.getCurrentDefinitionLevel() < info.maxDefLevel()) {
                    isNull.set(i);
                    noNulls = false;
                } else {
                    BigInteger unscaled = switch (info.parquetType()) {
                        case INT32 -> BigInteger.valueOf(cr.getInteger());
                        case INT64 -> BigInteger.valueOf(cr.getLong());
                        case BINARY, FIXED_LEN_BYTE_ARRAY -> new BigInteger(cr.getBinary().getBytes());
                        default -> throw new IllegalArgumentException("Unexpected DECIMAL backing type: " + info.parquetType());
                    };
                    values[i] = new java.math.BigDecimal(unscaled, scale).doubleValue();
                }
                cr.consume();
            }
            return ColumnBlockConversions.doubleColumn(blockFactory, values, rows, noNulls, false, isNull, false);
        }

        private Block readFloat16Column(ColumnReader cr, int maxDef, int rows) {
            double[] values = UninitializedArrays.newDoubleArray(rows);
            BitSet isNull = maxDef > 0 ? new BitSet(rows) : null;
            boolean noNulls = true;
            for (int i = 0; i < rows; i++) {
                if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                    isNull.set(i);
                    noNulls = false;
                } else {
                    byte[] bytes = cr.getBinary().getBytes();
                    short float16Bits = (short) ((bytes[1] & 0xFF) << 8 | (bytes[0] & 0xFF));
                    values[i] = Float.float16ToFloat(float16Bits);
                }
                cr.consume();
            }
            return ColumnBlockConversions.doubleColumn(blockFactory, values, rows, noNulls, false, isNull, false);
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
            long[] values = UninitializedArrays.newLongArray(rows);
            BitSet isNull = info.maxDefLevel() > 0 ? new BitSet(rows) : null;
            boolean noNulls = true;
            boolean isDate = info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32;
            for (int i = 0; i < rows; i++) {
                if (info.maxDefLevel() > 0 && cr.getCurrentDefinitionLevel() < info.maxDefLevel()) {
                    isNull.set(i);
                    noNulls = false;
                } else if (isDate) {
                    values[i] = cr.getInteger() * MILLIS_PER_DAY;
                } else {
                    long raw = cr.getLong();
                    values[i] = ParquetColumnDecoding.convertTimestampToMillis(raw, info.logicalType());
                }
                cr.consume();
            }
            return ColumnBlockConversions.longColumn(blockFactory, values, rows, noNulls, false, isNull, false);
        }

        /**
         * Converts a Parquet INT96 value (12 bytes: 8 bytes nanos-of-day LE + 4 bytes Julian day LE)
         * to epoch milliseconds.
         */
        private Block readInt96TimestampColumn(ColumnReader cr, int maxDef, int rows) {
            long[] values = UninitializedArrays.newLongArray(rows);
            BitSet isNull = maxDef > 0 ? new BitSet(rows) : null;
            boolean noNulls = true;
            for (int i = 0; i < rows; i++) {
                if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                    isNull.set(i);
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
            return ColumnBlockConversions.longColumn(blockFactory, values, rows, noNulls, false, isNull, false);
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
