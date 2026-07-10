/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.orc.BooleanColumnStatistics;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.DoubleColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.ReaderImpl;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Utf8Sanitizer;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.SyntheticColumns;
import org.elasticsearch.xpack.esql.datasources.cache.ParsedFooterCache;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnBlockConversions;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.DynamicThreshold;
import org.elasticsearch.xpack.esql.datasources.spi.DynamicThresholdAware;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader.SplitRange;
import org.elasticsearch.xpack.esql.datasources.spi.RangeReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.DateTimeException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * {@link RangeAwareFormatReader} implementation for Apache ORC files.
 *
 * <p>Uses ORC's vectorized reader ({@link VectorizedRowBatch}) which maps naturally to
 * ESQL's columnar {@link Block}/{@link Page} model. Each ORC {@link ColumnVector} is
 * converted directly to the corresponding ESQL Block type.
 *
 * <p>Supports stripe-level split parallelism: {@link #discoverSplitRanges} exposes
 * per-stripe byte ranges, and {@link #readRange} restricts reading to stripes within
 * a given byte range via ORC's {@code Reader.Options.range()}.
 *
 * <p>Key features:
 * <ul>
 *   <li>Works with any StorageProvider (HTTP, S3, local) via {@link OrcStorageObjectAdapter}</li>
 *   <li>Efficient columnar reading with column projection</li>
 *   <li>Direct conversion from ORC VectorizedRowBatch to ESQL Page</li>
 *   <li>Stripe-level split parallelism for multi-stripe files</li>
 * </ul>
 */
public class OrcFormatReader implements RangeAwareFormatReader, NoConfigFormatReader, DynamicThresholdAware {

    private static final Logger LOGGER = LogManager.getLogger(OrcFormatReader.class);

    private static final long MILLIS_PER_DAY = Duration.ofDays(1).toMillis();

    /**
     * JVM-wide cache of parsed ORC tails ({@link OrcTail}). Singleton — every
     * {@link OrcFormatReader} instance reads from and writes to the same cache so that producer
     * threads spawned from different reader instances (e.g. across concurrent queries) still
     * coalesce footer parses.
     */
    private static final ParsedFooterCache<OrcTail> PARSED_FOOTERS = new ParsedFooterCache<>();

    /** Clears the parsed-footer cache. Intended for test isolation only. */
    static void clearParsedFooterCacheForTests() {
        PARSED_FOOTERS.invalidateAll();
    }

    private final BlockFactory blockFactory;
    private final SearchArgument pushedFilter;
    private final OrcPushedExpressions pushedExpressions;
    private final OrcReaderCounters counters = new OrcReaderCounters();
    private final DynamicThreshold dynamicThreshold;
    /** Declared per-column date parse patterns (physical name &rarr; pattern); see {@link #withDeclaredDateFormats}. */
    private final Map<String, String> declaredDateFormats;
    /** Physical names of declared-type columns (licensed to narrow toward their target); see {@link #withDeclaredTypeColumns}. */
    private final Set<String> declaredTypeColumns;

    public OrcFormatReader(BlockFactory blockFactory) {
        this(blockFactory, null, null, null, Map.of(), Set.of());
    }

    private OrcFormatReader(
        BlockFactory blockFactory,
        SearchArgument pushedFilter,
        OrcPushedExpressions pushedExpressions,
        DynamicThreshold dynamicThreshold,
        Map<String, String> declaredDateFormats,
        Set<String> declaredTypeColumns
    ) {
        this.blockFactory = blockFactory;
        this.pushedFilter = pushedFilter;
        this.pushedExpressions = pushedExpressions;
        this.dynamicThreshold = dynamicThreshold;
        this.declaredDateFormats = declaredDateFormats;
        this.declaredTypeColumns = declaredTypeColumns;
    }

    @Override
    public FormatReader withPushedFilter(Object pushedFilter) {
        if (pushedFilter instanceof SearchArgument sarg) {
            return new OrcFormatReader(this.blockFactory, sarg, null, dynamicThreshold, declaredDateFormats, declaredTypeColumns);
        }
        if (pushedFilter instanceof OrcPushedExpressions exprs) {
            return new OrcFormatReader(this.blockFactory, null, exprs, dynamicThreshold, declaredDateFormats, declaredTypeColumns);
        }
        return this;
    }

    @Override
    public FormatReader withDynamicThreshold(DynamicThreshold threshold) {
        return new OrcFormatReader(blockFactory, pushedFilter, pushedExpressions, threshold, declaredDateFormats, declaredTypeColumns);
    }

    /**
     * Declared per-column date parse patterns, keyed by physical (file) column name. Consumed by the
     * string&rarr;datetime declared coercion ({@code DeclaredTypeCoercions}): a string column whose planner attribute
     * is {@code datetime} parses each value with this pattern (ISO default when absent). Native TIMESTAMP/DATE columns
     * carry their own encoding and never text-parse.
     */
    @Override
    public FormatReader withDeclaredDateFormats(Map<String, String> physicalNameToPattern) {
        if (physicalNameToPattern == null || physicalNameToPattern.isEmpty()) {
            return this;
        }
        return new OrcFormatReader(
            blockFactory,
            pushedFilter,
            pushedExpressions,
            dynamicThreshold,
            Map.copyOf(physicalNameToPattern),
            declaredTypeColumns
        );
    }

    /**
     * The physical names of declared-type columns — the ones whose target type came from an explicit declaration and are
     * therefore licensed to coerce (including narrow) toward it. {@code validatePlannerTypesAgainstFile} keys its
     * whole-column incompatibility null-fill on this set: a declared column keeps the {@code DeclaredTypeCoercions}
     * escape, while an inferred column null-fills whenever the file type is not widening-compatible (a
     * {@code first_file_wins} cross-file clash must widen-or-null, never downcast).
     */
    @Override
    public FormatReader withDeclaredTypeColumns(Set<String> physicalDeclaredColumns) {
        if (physicalDeclaredColumns == null || physicalDeclaredColumns.isEmpty()) {
            return this;
        }
        return new OrcFormatReader(
            blockFactory,
            pushedFilter,
            pushedExpressions,
            dynamicThreshold,
            declaredDateFormats,
            Set.copyOf(physicalDeclaredColumns)
        );
    }

    @Override
    public SourceMetadata metadata(StorageObject object) throws IOException {
        OrcStorageObjectAdapter fs = new OrcStorageObjectAdapter(object);
        Path path = new Path(object.path().toString());
        try (Reader reader = openReaderCached(fs, path)) {
            TypeDescription schema = reader.getSchema();
            List<Attribute> attributes = convertOrcSchemaToAttributes(schema);
            SourceStatistics statistics = extractStatistics(reader, schema);
            return new SimpleSourceMetadata(attributes, formatName(), object.path().toString(), statistics, null);
        }
    }

    /**
     * Creates ORC reader options with consistent configuration.
     * <p>
     * Sets {@code useUTCTimestamp(true)} which is required for correct timestamp predicate
     * pushdown. This is a reader-level flag (not per-column) that controls how SargApplier
     * reads timestamp column statistics: {@code true} uses getMinimumUTC/getMaximumUTC,
     * {@code false} uses getMinimum/getMaximum which applies a local-timezone shift. Without
     * it, predicates against TIMESTAMP_INSTANT columns cause false stripe exclusions.
     * <p>
     * This is safe because the ORC files use TIMESTAMP_INSTANT (UTC-anchored) columns. If we
     * ever support files with plain TIMESTAMP columns (writer-local timezone), this flag would
     * incorrectly treat their statistics as UTC too — at that point we'd need per-column
     * evaluation by bypassing SearchArgument and reading stripe statistics directly (the Trino
     * approach).
     */
    private static OrcFile.ReaderOptions orcReaderOptions(OrcStorageObjectAdapter fs) {
        return OrcFile.readerOptions(new Configuration(false)).filesystem(fs).useUTCTimestamp(true);
    }

    /**
     * Opens an ORC {@link Reader} using the JVM-wide {@link #PARSED_FOOTERS} cache so that the
     * tail (postscript + footer + types + stripe directory) is deserialized at most once per
     * {@code (path, length)} key. On a cache miss the loader parses the tail by opening a reader
     * once and extracting the {@link OrcTail} from its serialized footer buffer; the parsed result
     * is then handed to subsequent {@code OrcFile.createReader} calls via
     * {@link OrcFile.ReaderOptions#orcTail}. When ORC sees a pre-supplied tail it skips
     * {@code ReaderImpl.extractFileTail(FileSystem, Path, long)} and the associated remote read.
     */
    private Reader openReaderCached(OrcStorageObjectAdapter fs, Path path) throws IOException {
        OrcTail tail = loadTail(fs, path);
        return OrcFile.createReader(path, orcReaderOptions(fs).orcTail(tail));
    }

    /**
     * Loads the parsed ORC tail for {@code fs} via the JVM-wide {@link #PARSED_FOOTERS} cache,
     * parsing on a cache miss. The first call for a given key opens an ORC reader (which parses
     * the tail) and immediately closes it after extracting the {@link OrcTail}; subsequent calls
     * reuse the cached tail.
     */
    private OrcTail loadTail(OrcStorageObjectAdapter fs, Path path) throws IOException {
        // The loader runs only on a cache miss, so the flag distinguishes hit from miss.
        boolean[] missed = { false };
        try {
            OrcTail tail = PARSED_FOOTERS.getOrLoad(fs.cacheKey(), key -> {
                missed[0] = true;
                // Open a reader once, extract the parsed tail, then close the reader. The
                // OrcTail itself retains the serialized buffer + parsed protobuf footer and is
                // safe to share across threads (treated as immutable by all callers).
                //
                // This deliberately parses the tail twice on a cache miss: once inside
                // OrcFile.createReader (which calls the protected ReaderImpl.extractFileTail
                // (FileSystem, Path, long) to fetch and parse from storage) and once via the
                // public ReaderImpl.extractFileTail(ByteBuffer) to produce a shareable OrcTail.
                // The single-parse alternative would require re-implementing ORC's tail-fetch
                // protocol (variable-length postscript, optional metadata sections, version
                // handling) outside the library — fragile across ORC versions. Since this only
                // runs on the cold path (first producer per file per TTL window), the second
                // parse is a small cost that pays off as every subsequent producer hits the
                // cache and skips both the parse and the remote read entirely.
                try (Reader r = OrcFile.createReader(path, orcReaderOptions(fs))) {
                    return ReaderImpl.extractFileTail(r.getSerializedFileFooter());
                }
            });
            counters.recordFooterCache(missed[0] == false);
            return tail;
        } catch (ExecutionException e) {
            // rethrowStructural handles Error/IOException/CircuitBreakingException/
            // ElasticsearchException; anything else (typically a plain RuntimeException from
            // orc-core indicating a corrupt tail) is returned for format-specific wrapping.
            // Unlike Parquet there is no orc-tagged exception factory; surface a structurally
            // tagged IOException so log lines clearly attribute the failure to ORC tail parsing.
            Throwable other = ParsedFooterCache.rethrowStructural(e);
            if (other instanceof RuntimeException re) {
                throw re;
            }
            throw new IOException("Failed to parse ORC tail for [" + path + "]", other);
        }
    }

    private static SourceStatistics extractStatistics(Reader reader, TypeDescription schema) {
        long rowCount = reader.getNumberOfRows();
        long sizeInBytes = reader.getContentLength();
        ColumnStatistics[] orcStats = reader.getStatistics();

        // Walk every dotted leaf the flattener emits, publishing stats at the same names the
        // planner sees as ESQL attributes. Without this, only top-level entries land in the
        // map and nested-leaf aggregate pushdown (e.g. MIN(event.id)) silently degrades.
        // STRUCT intermediates are skipped by walkDottedLeaves — their ColumnStatistics carry
        // no useful min/max (they aggregate child statistics into the parent's id) and they
        // bind to no ESQL attribute. Truncated over-cap groups are also skipped: the flattener
        // emits them as a single UNSUPPORTED attribute that the planner never reads stats for.
        Map<String, SourceStatistics.ColumnStatistics> columnStats = new HashMap<>();
        walkDottedLeaves(schema, (dottedPath, type, truncated) -> {
            if (truncated) {
                return;
            }
            collectLeafStatistics(type, dottedPath, rowCount, orcStats, columnStats);
        });

        return new SourceStatistics() {
            @Override
            public OptionalLong rowCount() {
                return OptionalLong.of(rowCount);
            }

            @Override
            public OptionalLong sizeInBytes() {
                return OptionalLong.of(sizeInBytes);
            }

            @Override
            public Optional<Map<String, SourceStatistics.ColumnStatistics>> columnStatistics() {
                return columnStats.isEmpty() ? Optional.empty() : Optional.of(columnStats);
            }
        };
    }

    /**
     * Publishes ORC column statistics for a single non-STRUCT leaf at its dotted attribute name.
     * Called from {@link #extractStatistics} via {@link #walkDottedLeaves}, so the keys match
     * exactly what {@link #convertOrcSchemaToAttributes} produces and the planner looks up.
     *
     * <p>MAP/LIST&lt;STRUCT&gt;/UNION leaves are still emitted with whatever ColumnStatistics ORC
     * computed for them — the planner sees those as UNSUPPORTED attributes and never reads the
     * stats, but emitting them does no harm and keeps this helper unconditional.
     */
    private static void collectLeafStatistics(
        TypeDescription type,
        String dottedPath,
        long rowCount,
        ColumnStatistics[] orcStats,
        Map<String, SourceStatistics.ColumnStatistics> out
    ) {
        int colId = type.getId();
        if (colId >= orcStats.length) {
            return;
        }
        ColumnStatistics cs = orcStats[colId];
        long totalValues = cs.getNumberOfValues();
        long nullCount = rowCount - totalValues;
        Object minVal = extractOrcMin(cs);
        Object maxVal = extractOrcMax(cs);
        long bytesOnDisk = cs.getBytesOnDisk();

        out.put(dottedPath, new SourceStatistics.ColumnStatistics() {
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
                return bytesOnDisk > 0 ? OptionalLong.of(bytesOnDisk) : OptionalLong.empty();
            }
        });
    }

    private static Object extractOrcMin(ColumnStatistics cs) {
        if (cs instanceof IntegerColumnStatistics intStats) {
            return intStats.getMinimum();
        } else if (cs instanceof DoubleColumnStatistics dblStats) {
            return dblStats.getMinimum();
        } else if (cs instanceof StringColumnStatistics strStats) {
            return strStats.getMinimum();
        }
        return null;
    }

    private static Object extractOrcMax(ColumnStatistics cs) {
        if (cs instanceof IntegerColumnStatistics intStats) {
            return intStats.getMaximum();
        } else if (cs instanceof DoubleColumnStatistics dblStats) {
            return dblStats.getMaximum();
        } else if (cs instanceof StringColumnStatistics strStats) {
            return strStats.getMaximum();
        }
        return null;
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
        List<String> projectedColumns = context.projectedColumns();
        int batchSize = context.batchSize();
        int rowLimit = context.rowLimit();

        OrcStorageObjectAdapter fs = new OrcStorageObjectAdapter(object);
        Path path = new Path(object.path().toString());
        long footerStartNanos = System.nanoTime();
        Reader reader = openReaderCached(fs, path);
        TypeDescription schema = reader.getSchema();
        List<Attribute> attributes = convertOrcSchemaToAttributes(schema);

        List<Attribute> projectedAttributes = resolveProjection(attributes, projectedColumns);
        boolean[] include = buildIncludeMask(schema, projectedColumns);

        Reader.Options readOptions = configureReadOptions(reader, batchSize, include, schema);
        long stripeCount = reader.getStripes().size();
        int totalColumns = schema.getFieldNames().size();
        counters.addFooterRead(System.nanoTime() - footerStartNanos, sizeOrZero(object), stripeCount);
        counters.addStripesTotal(stripeCount);
        counters.setColumnCounts(countProjected(include, totalColumns), totalColumns);
        RecordReader rows = reader.rows(readOptions);

        CloseableIterator<Page> iter = new OrcPageIterator(
            reader,
            rows,
            schema,
            projectedAttributes,
            batchSize,
            blockFactory,
            StripeSkipTable.build(reader, schema, dynamicThreshold, 0L, Long.MAX_VALUE),
            counters,
            declaredDateFormats,
            declaredTypeColumns,
            object.path().toString(),
            resolveErrorPolicy(context.errorPolicy())
        );
        return rowLimit != NO_LIMIT ? new RowLimitingIterator(iter, rowLimit) : iter;
    }

    private static int countProjected(boolean[] include, int totalColumns) {
        if (include == null) {
            return totalColumns;
        }
        int n = 0;
        for (int i = 1; i < include.length; i++) {
            if (include[i]) {
                n++;
            }
        }
        return n;
    }

    @Override
    public List<SplitRange> discoverSplitRanges(StorageObject object) throws IOException {
        OrcStorageObjectAdapter fs = new OrcStorageObjectAdapter(object);
        Path path = new Path(object.path().toString());
        try (Reader reader = openReaderCached(fs, path)) {
            List<StripeInformation> stripes = reader.getStripes();
            if (stripes.isEmpty()) {
                return List.of();
            }
            List<StripeStatistics> stripeStats = reader.getStripeStatistics();
            TypeDescription schema = reader.getSchema();
            if (stripes.size() == 1) {
                StripeInformation stripe = stripes.getFirst();
                Map<String, Object> stats = stripeStats.isEmpty() == false
                    ? buildStripeStats(stripe, stripeStats.getFirst(), schema)
                    : Map.of();
                return List.of(new SplitRange(stripe.getOffset(), stripe.getLength(), stats));
            }
            List<SplitRange> ranges = new ArrayList<>(stripes.size());
            for (int i = 0; i < stripes.size(); i++) {
                StripeInformation stripe = stripes.get(i);
                Map<String, Object> stats = (i < stripeStats.size()) ? buildStripeStats(stripe, stripeStats.get(i), schema) : Map.of();
                ranges.add(new SplitRange(stripe.getOffset(), stripe.getLength(), stats));
            }
            return ranges;
        }
    }

    private static Map<String, Object> buildStripeStats(StripeInformation stripe, StripeStatistics stats, TypeDescription schema) {
        Map<String, Object> map = new HashMap<>();
        map.put(SourceStatisticsSerializer.STATS_ROW_COUNT, stripe.getNumberOfRows());
        map.put(SourceStatisticsSerializer.STATS_SIZE_BYTES, stripe.getLength());
        List<String> fieldNames = schema.getFieldNames();
        List<TypeDescription> children = schema.getChildren();
        ColumnStatistics[] colStats = stats.getColumnStatistics();
        for (int i = 0; i < fieldNames.size(); i++) {
            String colName = fieldNames.get(i);
            int colId = children.get(i).getId();
            if (colId >= colStats.length) {
                continue;
            }
            ColumnStatistics cs = colStats[colId];
            if (cs == null) {
                continue;
            }
            long totalValues = cs.getNumberOfValues();
            long nullCount = stripe.getNumberOfRows() - totalValues;
            map.put(SourceStatisticsSerializer.columnNullCountKey(colName), nullCount);
            if (cs.getBytesOnDisk() > 0) {
                map.put(SourceStatisticsSerializer.columnSizeBytesKey(colName), cs.getBytesOnDisk());
            }
            Object minVal = extractOrcMin(cs);
            Object maxVal = extractOrcMax(cs);
            if (minVal != null) {
                map.put(SourceStatisticsSerializer.columnMinKey(colName), minVal);
            }
            if (maxVal != null) {
                map.put(SourceStatisticsSerializer.columnMaxKey(colName), maxVal);
            }
        }
        return Map.copyOf(map);
    }

    /**
     * Reads only the stripes whose byte range falls within {@code [rangeStart, rangeEnd)}.
     * The context's errorPolicy governs declared-coercion failures (fail_fast propagates,
     * anything else warns + nulls the cell); structural errors (corrupt stripe, bad tail) always
     * fail the read regardless of policy.
     */
    @Override
    public CloseableIterator<Page> readRange(StorageObject object, RangeReadContext context) throws IOException {
        long rangeStart = context.rangeStart();
        long rangeEnd = context.rangeEnd();
        List<String> projectedColumns = context.projectedColumns();
        int batchSize = context.batchSize();
        List<Attribute> resolvedAttributes = context.resolvedAttributes();

        if (rangeEnd <= rangeStart) {
            throw new IllegalArgumentException("rangeEnd [" + rangeEnd + "] must be greater than rangeStart [" + rangeStart + "]");
        }
        OrcStorageObjectAdapter fs = new OrcStorageObjectAdapter(object);
        Path path = new Path(object.path().toString());
        // Tail resolution order, mirroring the parquet reader:
        // 1. context.fileContext() — per-producer fast path, single-writer/single-reader, no map
        // lookup; carries the parsed tail across successive splits of the same file on one
        // thread.
        // 2. PARSED_FOOTERS — JVM-wide cache keyed by (path, length); shared across producer
        // threads and across queries within the access TTL.
        long footerStartNanos = System.nanoTime();
        OrcTail tail;
        if (context.fileContext() instanceof OrcTail cached) {
            tail = cached;
        } else {
            tail = loadTail(fs, path);
            context.setFileContext(tail);
        }
        Reader reader = OrcFile.createReader(path, orcReaderOptions(fs).orcTail(tail));
        TypeDescription schema = reader.getSchema();

        final List<Attribute> attributes = resolvedAttributes != null && resolvedAttributes.isEmpty() == false
            ? resolvedAttributes
            : convertOrcSchemaToAttributes(schema);

        List<Attribute> projectedAttributes = resolveProjection(attributes, projectedColumns);
        boolean[] include = buildIncludeMask(schema, projectedColumns);

        Reader.Options readOptions = configureReadOptions(reader, batchSize, include, schema);
        readOptions.range(rangeStart, rangeEnd - rangeStart);
        long stripesInRange = countStripesInRange(reader, rangeStart, rangeEnd);
        long stripesInFile = reader.getStripes().size();
        int totalColumns = schema.getFieldNames().size();
        counters.addFooterRead(System.nanoTime() - footerStartNanos, sizeOrZero(object), stripesInFile);
        counters.addStripesTotal(stripesInRange);
        counters.setColumnCounts(countProjected(include, totalColumns), totalColumns);
        RecordReader rows = reader.rows(readOptions);

        return new OrcPageIterator(
            reader,
            rows,
            schema,
            projectedAttributes,
            batchSize,
            blockFactory,
            StripeSkipTable.build(reader, schema, dynamicThreshold, rangeStart, rangeEnd),
            counters,
            declaredDateFormats,
            declaredTypeColumns,
            object.path().toString(),
            resolveErrorPolicy(context.errorPolicy())
        );
    }

    /** Returns {@code object.length()} if known, or 0 when unavailable. Best-effort sizing for
     *  the {@code footer_size_bytes} counter; never throws. */
    private static long sizeOrZero(StorageObject object) {
        try {
            long len = object.length();
            return len >= 0 ? len : 0;
        } catch (IOException e) {
            return 0;
        }
    }

    /** Counts stripes whose offset falls in {@code [rangeStart, rangeEnd)} — matches ORC's
     *  range-based read selection. */
    private static long countStripesInRange(Reader reader, long rangeStart, long rangeEnd) {
        long n = 0;
        for (StripeInformation stripe : reader.getStripes()) {
            long off = stripe.getOffset();
            if (off >= rangeStart && off < rangeEnd) {
                n++;
            }
        }
        return n;
    }

    private static List<Attribute> resolveProjection(List<Attribute> attributes, List<String> projectedColumns) {
        if (projectedColumns == null || projectedColumns.isEmpty()) {
            return attributes;
        }
        List<Attribute> projected = new ArrayList<>();
        Map<String, Attribute> attributeMap = new HashMap<>();
        for (Attribute attr : attributes) {
            attributeMap.put(attr.name(), attr);
        }
        for (String columnName : projectedColumns) {
            if (ColumnExtractor.ROW_POSITION_COLUMN.equals(columnName)) {
                // Synthetic file-global row index, not an ORC column. Typed LONG (not NULL) so the
                // producer pipeline reads it as a LongBlock; OrcPageIterator fills it from
                // RecordReader.getRowNumber() rather than from the ORC vectors.
                projected.add(SyntheticColumns.newRowPositionAttribute());
                continue;
            }
            Attribute attr = attributeMap.get(columnName);
            projected.add(
                attr != null ? attr : new ReferenceAttribute(Source.EMPTY, null, columnName, DataType.NULL, Nullability.TRUE, null, false)
            );
        }
        return projected;
    }

    /**
     * Builds the ORC include mask for a projection. Resolution is path-aware:
     * <ol>
     *   <li>Exact match against a top-level field name in {@code schema} (preserves files whose
     *       top-level fields literally contain a dot).</li>
     *   <li>Otherwise, the projected name is interpreted as a dotted path and looked up against
     *       the flattened dotted-name map.</li>
     * </ol>
     * ORC's {@code Reader.Options#include} requires every STRUCT ancestor ID to be set for a
     * nested leaf to be read; this method propagates the include flag up the ancestor chain.
     */
    private static boolean[] buildIncludeMask(TypeDescription schema, List<String> projectedColumns) {
        if (projectedColumns == null || projectedColumns.isEmpty()) {
            return null;
        }
        Map<String, Integer> topLevelToIndex = new HashMap<>();
        List<String> fieldNames = schema.getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            topLevelToIndex.put(fieldNames.get(i), i);
        }
        Map<String, TypeDescription> dottedNameToType = buildDottedNameToType(schema);
        // Per-type parent index built lazily so we only pay it for nested resolution.
        Map<Integer, TypeDescription> idToParent = null;
        boolean[] include = new boolean[schema.getMaximumId() + 1];
        include[0] = true;
        for (String columnName : projectedColumns) {
            Integer idx = topLevelToIndex.get(columnName);
            if (idx != null) {
                TypeDescription child = schema.getChildren().get(idx);
                includeColumnForType(include, child);
                continue;
            }
            TypeDescription leaf = dottedNameToType.get(columnName);
            if (leaf == null) {
                continue;
            }
            if (idToParent == null) {
                idToParent = new HashMap<>();
                indexParents(schema, idToParent);
            }
            includeColumnForType(include, leaf);
            // Walk up the STRUCT ancestor chain.
            TypeDescription parent = idToParent.get(leaf.getId());
            while (parent != null && parent != schema) {
                include[parent.getId()] = true;
                parent = idToParent.get(parent.getId());
            }
        }
        return include;
    }

    private static void indexParents(TypeDescription parent, Map<Integer, TypeDescription> out) {
        List<TypeDescription> children = parent.getChildren();
        if (children == null) {
            return;
        }
        for (TypeDescription child : children) {
            out.put(child.getId(), parent);
            indexParents(child, out);
        }
    }

    private Reader.Options configureReadOptions(Reader reader, int batchSize, boolean[] include, TypeDescription schema) {
        Reader.Options readOptions = reader.options().rowBatchSize(batchSize);
        if (include != null) {
            readOptions.include(include);
        }
        SearchArgument resolvedFilter = resolveSearchArgument(schema);
        if (resolvedFilter != null) {
            List<PredicateLeaf> leaves = resolvedFilter.getLeaves();
            LinkedHashSet<String> nameSet = new LinkedHashSet<>(leaves.size());
            for (PredicateLeaf leaf : leaves) {
                nameSet.add(leaf.getColumnName());
            }
            readOptions.searchArgument(resolvedFilter, nameSet.toArray(new String[0]));
            counters.markPredicatePushdownUsed();
            counters.addPredicateColumns(nameSet);
        }
        return readOptions;
    }

    /**
     * Include a column and its children in the ORC read mask.
     * For LIST and MAP types, recurses into children so list/map element data is read.
     * For scalars and other types, only the type itself is included.
     */
    private static void includeColumnForType(boolean[] include, TypeDescription type) {
        include[type.getId()] = true;
        var category = type.getCategory();
        if (category == TypeDescription.Category.LIST || category == TypeDescription.Category.MAP) {
            for (TypeDescription child : type.getChildren()) {
                includeColumnForType(include, child);
            }
        }
    }

    /**
     * Resolves the SearchArgument to use for a given file. If deferred expressions are present,
     * builds the SearchArgument using the actual file schema for correct DATE/DECIMAL mapping.
     */
    private SearchArgument resolveSearchArgument(TypeDescription schema) {
        if (pushedFilter != null) {
            return pushedFilter;
        }
        if (pushedExpressions != null) {
            return pushedExpressions.toSearchArgument(schema);
        }
        return null;
    }

    @Override
    public FilterPushdownSupport filterPushdownSupport() {
        return new OrcFilterPushdownSupport();
    }

    @Override
    public AggregatePushdownSupport aggregatePushdownSupport() {
        return new OrcAggregatePushdownSupport();
    }

    @Override
    public boolean supportsWholeFileCompression() {
        return false;
    }

    @Override
    public String formatName() {
        return "orc";
    }

    /** Mirrors the Parquet reader: a {@code null} context policy falls back to {@link #defaultErrorPolicy()}. */
    private ErrorPolicy resolveErrorPolicy(@Nullable ErrorPolicy contextPolicy) {
        return contextPolicy != null ? contextPolicy : defaultErrorPolicy();
    }

    /**
     * Returns an immutable typed snapshot of the ORC reader's counters for the operator-status
     * envelope. Zero counters, false flag, empty predicate columns before any read() / readRange()
     * has run.
     */
    @Override
    public OrcReaderStatus statusSnapshot() {
        return counters.snapshot();
    }

    @Override
    public List<String> fileExtensions() {
        return List.of(".orc");
    }

    @Override
    public RowPositionStrategy rowPositionStrategy() {
        // OrcPageIterator fills the {@code _rowPosition} slot natively from
        // {@code RecordReader#getRowNumber()}; nothing for the dispatcher to splice.
        return PassThroughRowPositionStrategy.INSTANCE;
    }

    @Override
    public void close() throws IOException {
        // No resources to close at the reader level
    }

    /**
     * Maximum recursion depth for nested STRUCT flattening; mirrors
     * {@code ParquetFormatReader.MAX_STRUCT_FLATTENING_DEPTH}. Groups deeper than the cap
     * surface as a single UNSUPPORTED attribute and a DEBUG log line.
     *
     * <p>Depth counts from 1 at the schema's top-level children, so the deepest reachable
     * group is at depth {@code MAX_STRUCT_FLATTENING_DEPTH}. The Parquet flattener uses the
     * same convention so the two formats accept the same set of valid paths.
     */
    static final int MAX_STRUCT_FLATTENING_DEPTH = 64;

    /**
     * Recursively converts an ORC {@link TypeDescription} into ESQL {@link Attribute}s, flattening
     * nested STRUCT fields into dotted attribute names (e.g. {@code event.action}). Primitive and
     * LIST-of-primitive fields emit at their parent's dotted path; MAP, LIST&lt;STRUCT&gt;, UNION,
     * and anything else not understood surface as a single {@link DataType#UNSUPPORTED} attribute
     * at the field's dotted path.
     *
     * <p>Recursion is bounded by {@link #MAX_STRUCT_FLATTENING_DEPTH}; groups deeper than the cap
     * are emitted as a single UNSUPPORTED attribute and a DEBUG log line is recorded.
     *
     * <p>Resolution rule for dotted projected names (see {@link #buildIncludeMask}): exact
     * top-level match against the file schema is attempted first, then dotted-path traversal.
     * This preserves files whose top-level field literally contains a dot.
     *
     * <p>ORC's {@link TypeDescription} carries no schema-level non-null guarantee — every column is
     * nullable at the schema level (per-file non-null observations live in footer column statistics,
     * not in the type itself). Attributes are built as {@link Nullability#TRUE} so downstream planner
     * rules (e.g. {@code Coalesce} simplification, {@code IS NULL}/{@code IS NOT NULL} rewriting)
     * don't drop legitimate null rows based on a wrong type-level assumption.
     */
    private static List<Attribute> convertOrcSchemaToAttributes(TypeDescription schema) {
        List<Attribute> attributes = new ArrayList<>();
        walkDottedLeaves(schema, (dottedPath, type, truncated) -> {
            if (truncated) {
                LOGGER.debug(
                    "ORC field [{}] exceeds STRUCT flattening depth cap [{}]; emitting as UNSUPPORTED",
                    dottedPath,
                    MAX_STRUCT_FLATTENING_DEPTH
                );
                attributes.add(new ReferenceAttribute(Source.EMPTY, null, dottedPath, DataType.UNSUPPORTED, Nullability.TRUE, null, false));
            } else {
                attributes.add(
                    new ReferenceAttribute(Source.EMPTY, null, dottedPath, convertOrcTypeToEsql(type), Nullability.TRUE, null, false)
                );
            }
        });
        return attributes;
    }

    private static DataType convertOrcTypeToEsql(TypeDescription orcType) {
        return switch (orcType.getCategory()) {
            case BOOLEAN -> DataType.BOOLEAN;
            case BYTE, SHORT, INT -> DataType.INTEGER;
            case LONG -> DataType.LONG;
            case FLOAT, DOUBLE -> DataType.DOUBLE;
            case STRING, VARCHAR, CHAR -> DataType.KEYWORD;
            case TIMESTAMP, TIMESTAMP_INSTANT -> DataType.DATETIME;
            case DATE -> DataType.DATETIME;
            case DECIMAL -> DataType.DOUBLE;
            case LIST -> convertOrcTypeToEsql(orcType.getChildren().get(0));
            // BINARY holds arbitrary bytes (KEYWORD would assume valid UTF-8), and MAP/STRUCT/UNION and the
            // geospatial types have no scalar ESQL equivalent, so all map to UNSUPPORTED. They are enumerated
            // explicitly so that default is reserved for a genuinely unexpected ORC category (a future enum constant).
            case BINARY, MAP, STRUCT, UNION, Geometry, Geography -> DataType.UNSUPPORTED;
            default -> throw new IllegalArgumentException("Unexpected ORC type category: " + orcType.getCategory());
        };
    }

    /**
     * Builds a map from dotted attribute names to the corresponding leaf (or truncated-group) ORC
     * {@link TypeDescription}, mirroring the flattening done in {@link #convertOrcSchemaToAttributes}.
     * Used by {@link #buildIncludeMask} and {@link OrcPageIterator} to walk struct vectors.
     */
    private static Map<String, TypeDescription> buildDottedNameToType(TypeDescription schema) {
        Map<String, TypeDescription> map = new HashMap<>();
        walkDottedLeaves(schema, (dottedPath, type, truncated) -> map.put(dottedPath, type));
        return map;
    }

    /**
     * Walks {@code schema}'s STRUCT children and invokes {@code visitor} once per dotted-path
     * leaf, with {@code truncated == true} when the depth cap stops recursion (the visitor sees
     * the surviving group as the leaf type). Single source of truth for the flattening shape so
     * {@link #convertOrcSchemaToAttributes} and {@link #buildDottedNameToType} stay in sync.
     */
    @FunctionalInterface
    private interface DottedLeafVisitor {
        void visit(String dottedPath, TypeDescription type, boolean truncated);
    }

    private static void walkDottedLeaves(TypeDescription schema, DottedLeafVisitor visitor) {
        List<String> fieldNames = schema.getFieldNames();
        List<TypeDescription> children = schema.getChildren();
        for (int i = 0; i < fieldNames.size(); i++) {
            walkDottedLeaves(children.get(i), fieldNames.get(i), 1, visitor);
        }
    }

    private static void walkDottedLeaves(TypeDescription type, String dottedPath, int depth, DottedLeafVisitor visitor) {
        if (depth > MAX_STRUCT_FLATTENING_DEPTH) {
            visitor.visit(dottedPath, type, true);
            return;
        }
        if (type.getCategory() == TypeDescription.Category.STRUCT) {
            List<String> childNames = type.getFieldNames();
            List<TypeDescription> children = type.getChildren();
            for (int i = 0; i < childNames.size(); i++) {
                walkDottedLeaves(children.get(i), dottedPath + "." + childNames.get(i), depth + 1, visitor);
            }
            return;
        }
        visitor.visit(dottedPath, type, false);
    }

    private static final class StripeSkipTable {
        private final DynamicThreshold threshold;
        private final long[] startRows;
        private final boolean[] active;
        private final boolean[] hasStats;
        private final boolean[] nullOnly;
        private final long[] nullCounts;
        private final long[] rawMins;
        private final long[] rawMaxs;
        // Populated instead of rawMins/rawMaxs when the threshold is BYTES_REF (keyword/text).
        private final BytesRef[] rawMinBytes;
        private final BytesRef[] rawMaxBytes;

        private StripeSkipTable(
            DynamicThreshold threshold,
            long[] startRows,
            boolean[] active,
            boolean[] hasStats,
            boolean[] nullOnly,
            long[] nullCounts,
            long[] rawMins,
            long[] rawMaxs,
            BytesRef[] rawMinBytes,
            BytesRef[] rawMaxBytes
        ) {
            this.threshold = threshold;
            this.startRows = startRows;
            this.active = active;
            this.hasStats = hasStats;
            this.nullOnly = nullOnly;
            this.nullCounts = nullCounts;
            this.rawMins = rawMins;
            this.rawMaxs = rawMaxs;
            this.rawMinBytes = rawMinBytes;
            this.rawMaxBytes = rawMaxBytes;
        }

        static StripeSkipTable build(Reader reader, TypeDescription schema, DynamicThreshold threshold, long rangeStart, long rangeEnd)
            throws IOException {
            if (threshold == null) {
                return null;
            }
            TypeDescription sortType = buildDottedNameToType(schema).get(threshold.columnName());
            if (sortType == null) {
                return null;
            }
            List<StripeInformation> stripes = reader.getStripes();
            List<StripeStatistics> stripeStats = reader.getStripeStatistics();
            if (stripes.isEmpty() || stripeStats.isEmpty()) {
                return null;
            }
            boolean bytesRef = threshold.elementType() == ElementType.BYTES_REF;
            long[] startRows = new long[stripes.size() + 1];
            boolean[] active = new boolean[stripes.size()];
            boolean[] hasStats = new boolean[stripes.size()];
            boolean[] nullOnly = new boolean[stripes.size()];
            long[] nullCounts = new long[stripes.size()];
            long[] rawMins = new long[stripes.size()];
            long[] rawMaxs = new long[stripes.size()];
            BytesRef[] rawMinBytes = bytesRef ? new BytesRef[stripes.size()] : null;
            BytesRef[] rawMaxBytes = bytesRef ? new BytesRef[stripes.size()] : null;
            long row = 0L;
            int columnId = sortType.getId();
            for (int i = 0; i < stripes.size(); i++) {
                StripeInformation stripe = stripes.get(i);
                startRows[i] = row;
                row += stripe.getNumberOfRows();
                active[i] = stripe.getOffset() >= rangeStart && stripe.getOffset() < rangeEnd;
                if (active[i] && i < stripeStats.size()) {
                    ColumnStatistics[] columnStatistics = stripeStats.get(i).getColumnStatistics();
                    if (columnId < columnStatistics.length) {
                        ColumnStatistics stats = columnStatistics[columnId];
                        if (stats == null) {
                            continue;
                        }
                        long nulls = stripe.getNumberOfRows() - stats.getNumberOfValues();
                        if (bytesRef) {
                            BytesRef min = rawMinBytes(stats, sortType);
                            BytesRef max = rawMaxBytes(stats, sortType);
                            if (min != null && max != null) {
                                hasStats[i] = true;
                                nullCounts[i] = nulls;
                                rawMinBytes[i] = min;
                                rawMaxBytes[i] = max;
                            } else if (stats.getNumberOfValues() == 0 && nulls > 0) {
                                hasStats[i] = true;
                                nullOnly[i] = true;
                                nullCounts[i] = nulls;
                            }
                        } else {
                            Long min = rawMin(stats, sortType, threshold.elementType());
                            Long max = rawMax(stats, sortType, threshold.elementType());
                            if (min != null && max != null) {
                                hasStats[i] = true;
                                nullCounts[i] = nulls;
                                rawMins[i] = min;
                                rawMaxs[i] = max;
                            } else if (stats.getNumberOfValues() == 0 && nulls > 0) {
                                hasStats[i] = true;
                                nullOnly[i] = true;
                                nullCounts[i] = nulls;
                            }
                        }
                    }
                }
            }
            startRows[stripes.size()] = row;
            return new StripeSkipTable(
                threshold,
                startRows,
                active,
                hasStats,
                nullOnly,
                nullCounts,
                rawMins,
                rawMaxs,
                rawMinBytes,
                rawMaxBytes
            );
        }

        boolean noFurtherCandidates() {
            return threshold.noFurtherCandidates();
        }

        int stripeIndexForRow(long rowNumber) {
            int low = 0;
            int high = active.length - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                if (rowNumber < startRows[mid]) {
                    high = mid - 1;
                } else if (rowNumber >= startRows[mid + 1]) {
                    low = mid + 1;
                } else {
                    return mid;
                }
            }
            return -1;
        }

        boolean dominated(int stripeIndex) {
            if (stripeIndex < 0 || stripeIndex >= active.length || active[stripeIndex] == false || hasStats[stripeIndex] == false) {
                return false;
            }
            if (nullOnly[stripeIndex]) {
                return threshold.dominatesNulls(nullCounts[stripeIndex]);
            }
            if (rawMinBytes != null) {
                return threshold.dominates(rawMinBytes[stripeIndex], rawMaxBytes[stripeIndex], nullCounts[stripeIndex]);
            }
            return threshold.dominates(rawMins[stripeIndex], rawMaxs[stripeIndex], nullCounts[stripeIndex]);
        }

        int nextNonDominatedStripe(int fromStripe) {
            for (int i = fromStripe; i < active.length; i++) {
                if (active[i] && dominated(i) == false) {
                    return i;
                }
            }
            return -1;
        }

        long stripeStartRow(int stripeIndex) {
            return startRows[stripeIndex];
        }

        private static Long rawMin(ColumnStatistics stats, TypeDescription sortType, ElementType elementType) {
            return switch (elementType) {
                case LONG -> sortType.getCategory() == TypeDescription.Category.LONG && stats instanceof IntegerColumnStatistics intStats
                    ? intStats.getMinimum()
                    : null;
                case INT -> sortType.getCategory() == TypeDescription.Category.INT && stats instanceof IntegerColumnStatistics intStats
                    ? intStats.getMinimum()
                    : null;
                case DOUBLE -> stats instanceof DoubleColumnStatistics doubleStats ? rawDouble(doubleStats.getMinimum()) : null;
                case BOOLEAN -> stats instanceof BooleanColumnStatistics booleanStats ? (booleanStats.getFalseCount() > 0 ? 0L : 1L) : null;
                default -> null;
            };
        }

        private static Long rawMax(ColumnStatistics stats, TypeDescription sortType, ElementType elementType) {
            return switch (elementType) {
                case LONG -> sortType.getCategory() == TypeDescription.Category.LONG && stats instanceof IntegerColumnStatistics intStats
                    ? intStats.getMaximum()
                    : null;
                case INT -> sortType.getCategory() == TypeDescription.Category.INT && stats instanceof IntegerColumnStatistics intStats
                    ? intStats.getMaximum()
                    : null;
                case DOUBLE -> stats instanceof DoubleColumnStatistics doubleStats ? rawDouble(doubleStats.getMaximum()) : null;
                case BOOLEAN -> stats instanceof BooleanColumnStatistics booleanStats ? (booleanStats.getTrueCount() > 0 ? 1L : 0L) : null;
                default -> null;
            };
        }

        private static Long rawDouble(double value) {
            return Double.isNaN(value) ? null : NumericUtils.doubleToSortableLong(value);
        }

        /**
         * Lexicographic min/max for a {@code BYTES_REF} threshold. Only string-family columns carry a
         * comparable byte range; {@link StringColumnStatistics} min/max use UTF-8 (Text) ordering,
         * which matches {@link BytesRef#compareTo}. Truncated stats stay safe because ORC reports a
         * lower bound for the minimum and an upper bound for the maximum, so the true value range is
         * always contained within {@code [min, max]} and we never skip a stripe that could contribute.
         * Returns {@code null} for any other column type so a coerced (non-string) column is never
         * skipped.
         */
        private static BytesRef rawMinBytes(ColumnStatistics stats, TypeDescription sortType) {
            if (isStringFamily(sortType) && stats instanceof StringColumnStatistics strStats && strStats.getMinimum() != null) {
                return new BytesRef(strStats.getMinimum());
            }
            return null;
        }

        private static BytesRef rawMaxBytes(ColumnStatistics stats, TypeDescription sortType) {
            if (isStringFamily(sortType) && stats instanceof StringColumnStatistics strStats && strStats.getMaximum() != null) {
                return new BytesRef(strStats.getMaximum());
            }
            return null;
        }

        private static boolean isStringFamily(TypeDescription sortType) {
            return switch (sortType.getCategory()) {
                case STRING, VARCHAR, CHAR -> true;
                default -> false;
            };
        }
    }

    private static class OrcPageIterator implements CloseableIterator<Page> {
        private final Reader reader;
        private final RecordReader rows;
        private final List<Attribute> attributes;
        private final BlockFactory blockFactory;
        private final VectorizedRowBatch batch;
        private final StripeSkipTable stripeSkipTable;
        private boolean exhausted = false;
        private boolean batchReady = false;
        /**
         * For each projected attribute, the path of child indices from the root struct down to
         * the leaf column. Top-level fields have length-1 paths; nested struct subfields have
         * longer paths that walk through {@link StructColumnVector#fields}.
         * Attributes absent from the file map to {@code null}.
         */
        private final Map<String, int[]> fieldNameToPath;
        /**
         * Per-projected-attribute leaf {@link TypeDescription} in the FILE's schema ({@code null} for columns absent
         * from the file and for the synthetic {@code _rowPosition}). Carries the physical side of a declared-type
         * coercion: the block conversion needs it to tell an ORC {@code DATE} (days since epoch, LongColumnVector)
         * apart from a {@code bigint} declared {@code datetime} (raw epoch millis, also LongColumnVector).
         */
        private final TypeDescription[] leafTypes;
        /**
         * Per-projected-attribute declared date parse pattern ({@code null} when none declared), resolved once from
         * the reader's {@code declaredDateFormats}. Consumed only by the string&rarr;datetime coerced conversion.
         */
        private final DateFormatter[] declaredFormatters;
        /** Index of the synthetic {@code _rowPosition} attribute, or {@code -1} when not projected. */
        private final int rowPositionColumnIndex;
        /** File location for warning/error messages. */
        private final String fileLocation;
        /** See {@link #coercionWarnings()}. */
        private SkipWarnings coercionWarnings;
        /** The read's error policy; strict ({@code fail_fast}) makes {@link #coercionWarnings()} return {@code null}. */
        private final ErrorPolicy errorPolicy;
        /**
         * File-global row number of the first row in the current batch, captured from
         * {@link RecordReader#getRowNumber()} immediately before each {@code nextBatch}. ORC reports
         * an absolute, file-global row number even on stripe-ranged reads, so it is split-invariant.
         */
        private long batchStartRow;

        private final OrcReaderCounters counters;

        OrcPageIterator(
            Reader reader,
            RecordReader rows,
            TypeDescription schema,
            List<Attribute> attributes,
            int batchSize,
            BlockFactory blockFactory,
            StripeSkipTable stripeSkipTable,
            OrcReaderCounters counters,
            Map<String, String> declaredDateFormats,
            Set<String> declaredTypeColumns,
            String fileLocation,
            ErrorPolicy errorPolicy
        ) {
            this.errorPolicy = errorPolicy;
            this.reader = reader;
            this.rows = rows;
            this.attributes = attributes;
            this.blockFactory = blockFactory;
            this.batch = schema.createRowBatch(batchSize);
            this.stripeSkipTable = stripeSkipTable;
            this.counters = counters;

            this.rowPositionColumnIndex = SyntheticColumns.rowPositionIndexInAttributes(attributes);
            this.fileLocation = fileLocation;

            this.fieldNameToPath = new HashMap<>(attributes.size());
            this.leafTypes = new TypeDescription[attributes.size()];
            this.declaredFormatters = new DateFormatter[attributes.size()];
            // Top-level field index, computed once for literal-name lookups.
            Map<String, Integer> topLevelToIndex = new HashMap<>();
            List<String> topLevelNames = schema.getFieldNames();
            for (int i = 0; i < topLevelNames.size(); i++) {
                topLevelToIndex.put(topLevelNames.get(i), i);
            }
            for (int col = 0; col < attributes.size(); col++) {
                Attribute attr = attributes.get(col);
                String name = attr.name();
                int[] path = fieldNameToPath.get(name);
                if (path == null) {
                    Integer topLevelIdx = topLevelToIndex.get(name);
                    path = topLevelIdx != null ? new int[] { topLevelIdx } : resolveDottedPath(schema, name);
                    if (path != null) {
                        fieldNameToPath.put(name, path);
                    }
                }
                if (path != null) {
                    leafTypes[col] = leafTypeForPath(schema, path);
                }
                String pattern = declaredDateFormats.get(name);
                if (pattern != null) {
                    declaredFormatters[col] = DateFormatter.forPattern(pattern);
                }
            }
            validatePlannerTypesAgainstFile(fileLocation, declaredTypeColumns);
        }

        /** Walks {@code path} down the file schema's STRUCT children to the leaf {@link TypeDescription}. */
        private static TypeDescription leafTypeForPath(TypeDescription schema, int[] path) {
            TypeDescription current = schema;
            for (int idx : path) {
                current = current.getChildren().get(idx);
            }
            return current;
        }

        /**
         * The per-file manifestation of the declared-coercion castability check ({@code DeclaredTypeCoercions}): a
         * projected attribute whose planner type can be satisfied from this file's ORC-derived type — same type, a
         * {@link EsqlDataTypeConverter#commonType} widening, or a supported coercion — decodes; anything else emits a
         * response Warning header (and a log warning) and reads as null instead of decoding garbage or failing with a
         * vector class cast. Mirrors the Parquet reader's {@code validatePlannerTypesAgainstFile}: the resolver has
         * already fail-fasted against the anchor footer, but a multi-file glob can drift from the anchor.
         * <p>
         * The {@code DeclaredTypeCoercions#supports} escape — which admits lossy narrowing — is honored only for a column
         * in {@code declaredTypeColumns} (target type from an explicit declaration, so a per-value coerce is licensed).
         * For an INFERRED target the escape does not apply: a cross-file clash must widen-or-null, never downcast.
         */
        private void validatePlannerTypesAgainstFile(String fileLocation, Set<String> declaredTypeColumns) {
            SkipWarnings skipWarnings = null;
            for (int col = 0; col < attributes.size(); col++) {
                Attribute attr = attributes.get(col);
                TypeDescription leafType = leafTypes[col];
                if (leafType == null || col == rowPositionColumnIndex) {
                    continue;
                }
                DataType planner = attr.dataType();
                if (planner == DataType.NULL || planner == DataType.UNSUPPORTED) {
                    continue;
                }
                DataType actualInFile = convertOrcTypeToEsql(leafType);
                DataType widened = EsqlDataTypeConverter.commonType(planner, actualInFile);
                boolean compatible = planner == actualInFile || (widened != null && widened == planner)
                // Lossy-narrowing coercion escape is reserved for DECLARED columns; an inferred target may only widen.
                    || (declaredTypeColumns.contains(attr.name()) && DeclaredTypeCoercions.supports(actualInFile, planner));
                if (compatible == false) {
                    if (skipWarnings == null) {
                        skipWarnings = new SkipWarnings(
                            "ORC file ["
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
                            + planner
                            + "]; returning nulls for this column"
                    );
                    LOGGER.warn(
                        "Column [{}] in file [{}] has type [{}] incompatible with planner type [{}]; " + "returning nulls for this column",
                        attr.name(),
                        fileLocation,
                        actualInFile,
                        planner
                    );
                    fieldNameToPath.remove(attr.name());
                    leafTypes[col] = null;
                }
            }
        }

        /**
         * Resolves a dotted attribute name to a root-to-leaf child-index path. Returns
         * {@code null} when the name has no match. Stops at the first non-STRUCT segment, so
         * dotted paths into LIST/MAP children are not resolved (matches the schema flattening).
         */
        private static int[] resolveDottedPath(TypeDescription schema, String dottedName) {
            String[] segments = dottedName.split("\\.");
            int[] tentative = new int[segments.length];
            TypeDescription current = schema;
            for (int i = 0; i < segments.length; i++) {
                if (current.getCategory() != TypeDescription.Category.STRUCT) {
                    return null;
                }
                List<String> names = current.getFieldNames();
                int idx = names.indexOf(segments[i]);
                if (idx < 0) {
                    return null;
                }
                tentative[i] = idx;
                current = current.getChildren().get(idx);
            }
            return tentative;
        }

        @Override
        public boolean hasNext() {
            if (exhausted) {
                return false;
            }
            if (batchReady) {
                return true;
            }
            long startNanos = System.nanoTime();
            try {
                while (true) {
                    if (stripeSkipTable != null && stripeSkipTable.noFurtherCandidates()) {
                        exhausted = true;
                        return false;
                    }
                    if (skipDominatedStripes()) {
                        if (exhausted) {
                            return false;
                        }
                        continue;
                    }
                    // Capture the absolute file-global row number BEFORE reading the batch: after
                    // nextBatch advances the cursor, getRowNumber() points past the batch.
                    long startRow = rows.getRowNumber();
                    if (rows.nextBatch(batch)) {
                        batchStartRow = startRow;
                        batchReady = true;
                        return true;
                    } else {
                        exhausted = true;
                        return false;
                    }
                }
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to read ORC batch", e);
            } finally {
                counters.addReadNanos(System.nanoTime() - startNanos);
            }
        }

        private boolean skipDominatedStripes() throws IOException {
            if (stripeSkipTable == null) {
                return false;
            }
            int stripeIndex = stripeSkipTable.stripeIndexForRow(rows.getRowNumber());
            if (stripeSkipTable.dominated(stripeIndex) == false) {
                return false;
            }
            int nextStripe = stripeSkipTable.nextNonDominatedStripe(stripeIndex + 1);
            if (nextStripe < 0) {
                exhausted = true;
                return true;
            }
            rows.seekToRow(stripeSkipTable.stripeStartRow(nextStripe));
            return true;
        }

        @Override
        public Page next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            batchReady = false;
            counters.addRowsEmitted(batch.size);
            return convertToPage();
        }

        private Page convertToPage() {
            int rowCount = batch.size;
            Block[] blocks = new Block[attributes.size()];

            for (int col = 0; col < attributes.size(); col++) {
                Attribute attribute = attributes.get(col);
                String fieldName = attribute.name();
                DataType dataType = attribute.dataType();

                try {
                    if (col == rowPositionColumnIndex) {
                        blocks[col] = buildRowPositionBlock(rowCount);
                        continue;
                    }
                    int[] path = fieldNameToPath.get(fieldName);
                    if (path == null) {
                        blocks[col] = blockFactory.newConstantNullBlock(rowCount);
                        continue;
                    }
                    ColumnVector vector = batch.cols[path[0]];
                    // Collect ancestor struct nulls so that a null parent forces a null child
                    // regardless of the child vector's own per-row state. ORC populates child
                    // vector slots independently of parent nulls (Risk 1 in the implementation
                    // plan); without OR'ing the ancestor null bits, we can leak stale child
                    // values for rows where the parent struct was null.
                    BitSet ancestorNulls = null;
                    for (int i = 1; i < path.length; i++) {
                        StructColumnVector sv = (StructColumnVector) vector;
                        if (sv.noNulls == false) {
                            if (sv.isRepeating) {
                                if (sv.isNull[0]) {
                                    // Whole batch is null at this ancestor.
                                    blocks[col] = blockFactory.newConstantNullBlock(rowCount);
                                    ancestorNulls = null;
                                    vector = null;
                                    break;
                                }
                            } else {
                                // Reuse the shared boolean[]->BitSet helper plus BitSet.or so each
                                // ancestor contributes via a single bulk operation rather than a
                                // hand-rolled per-row loop; matches the pattern used elsewhere in
                                // the reader for converting ORC's raw null arrays.
                                BitSet svNulls = ColumnBlockConversions.toBitSet(sv.isNull, rowCount);
                                if (ancestorNulls == null) {
                                    ancestorNulls = svNulls;
                                } else if (svNulls != null) {
                                    ancestorNulls.or(svNulls);
                                }
                            }
                        }
                        vector = sv.fields[path[i]];
                    }
                    if (vector == null) {
                        continue;
                    }
                    blocks[col] = createBlock(
                        vector,
                        dataType,
                        rowCount,
                        ancestorNulls,
                        leafTypes[col],
                        declaredFormatters[col],
                        fieldName
                    );
                } catch (Exception e) {
                    Releasables.closeExpectNoException(blocks);
                    throw e;
                }
            }

            return new Page(blocks);
        }

        /**
         * Emits the synthetic {@code _rowPosition} column: the file-global row index of each row in
         * the batch, {@code [batchStartRow, batchStartRow + rowCount)}. Never null. This is the
         * opaque, split-invariant per-record token the producer pipeline renders as
         * {@code _file.record_ref} / composes into {@code _id}.
         *
         * <p>Direct array fill + {@link BlockFactory#newLongArrayVector} rather than
         * {@link LongVector.Builder#appendLong}: the values are a known-size arithmetic sequence,
         * so we skip the builder's per-element method-call overhead and finalization copy. The
         * tight primitive loop is SIMD-vectorizable; circuit-breaker accounting is preserved by
         * {@code newLongArrayVector}.
         */
        private Block buildRowPositionBlock(int rowCount) {
            long[] values = new long[rowCount];
            long base = batchStartRow;
            for (int i = 0; i < rowCount; i++) {
                values[i] = base + i;
            }
            return blockFactory.newLongArrayVector(values, rowCount).asBlock();
        }

        /**
         * Builds a block from {@code vector}, OR'ing in {@code ancestorNulls} (the per-row null
         * mask synthesized from any STRUCT ancestors on the path to this leaf). Pass {@code null}
         * for top-level columns. For nested leaves, this is the only place ancestor null
         * propagation happens — ORC populates child vector slots independently of parent struct
         * nulls, so callers must compose the two before block construction.
         *
         * <p>When the leaf vector has {@code isRepeating == true} (all values identical) and
         * {@code ancestorNulls} carries per-row variation, the repeating optimization in
         * {@link ColumnBlockConversions} (which only inspects position 0) would erase that
         * per-row signal. In this case we both materialize {@code leafNulls} per-row and force
         * the downstream conversion to iterate position-by-position by passing
         * {@code effectiveRepeating == false}. Because the helpers iterate
         * {@code values[0..rowCount-1]} in their non-repeating branch, we also broadcast the
         * underlying value array via {@link #longValuesFor}/{@link #doubleValuesFor} (and the
         * inline {@code readFromZero} pattern in the bytes/decimal/datetime paths) so positions
         * {@code >0} read the only meaningful slot ({@code 0}) rather than stale data.
         */
        private Block createBlock(
            ColumnVector vector,
            DataType dataType,
            int rowCount,
            BitSet ancestorNulls,
            TypeDescription leafType,
            DateFormatter dateFormatter,
            String columnName
        ) {
            // Declared-type coercion beyond the fused pairs: decode the column (or LIST element)
            // at the file's own type with the arms below, then coerce the block to the declared
            // type. Per-value failures follow the read's error policy: the default nulls the cell
            // + emits a response Warning, fail_fast fails the read (null sink from
            // coercionWarnings()). Mirrors the Parquet readers so the two columnar formats cannot
            // drift.
            DataType fileType = leafType != null ? convertOrcTypeToEsql(leafType) : null;
            if (fileType != null
                && dataType != fileType
                && DeclaredTypeCoercions.fusedInDecode(fileType, dataType) == false
                && DeclaredTypeCoercions.supports(fileType, dataType)) {
                Block physical = createBlockAs(vector, fileType, rowCount, ancestorNulls, leafType, dateFormatter, columnName);
                try {
                    return DeclaredTypeCoercions.castBlock(
                        physical,
                        fileType,
                        dataType,
                        dateFormatter,
                        blockFactory,
                        columnName,
                        coercionWarnings()
                    );
                } finally {
                    physical.close();
                }
            }
            return createBlockAs(vector, dataType, rowCount, ancestorNulls, leafType, dateFormatter, columnName);
        }

        /**
         * The coercion-failure sink for this read, or {@code null} under {@code fail_fast} — the
         * strict contract of {@code DeclaredTypeCoercions}, where a {@code null} sink means the
         * failure propagates and the read fails instead of warn+null.
         */
        @Nullable
        private SkipWarnings coercionWarnings() {
            if (errorPolicy.isStrict()) {
                return null;
            }
            if (coercionWarnings == null) {
                coercionWarnings = new SkipWarnings(
                    "ORC file ["
                        + fileLocation
                        + "] has values that could not be coerced to the declared column type; "
                        + "they are returned as null"
                );
            }
            return coercionWarnings;
        }

        /** The physical decode arms; {@code dataType} is the type the block is decoded AS (see {@link #createBlock}). */
        private Block createBlockAs(
            ColumnVector vector,
            DataType dataType,
            int rowCount,
            BitSet ancestorNulls,
            TypeDescription leafType,
            DateFormatter dateFormatter,
            String columnName
        ) {
            if (vector instanceof ListColumnVector listCol) {
                // LIST<primitive> is unreachable below a STRUCT ancestor today (LIST<STRUCT> is
                // intentionally unsupported); fall back to the existing path which does not
                // consume ancestorNulls. If/when nested LIST<primitive> projection is added the
                // listCol path needs the same OR.
                // The element's file type drives the LIST<datetime> decode the same way leafType drives the flat one.
                TypeDescription elementType = leafType != null
                    && leafType.getChildren() != null
                    && leafType.getChildren().isEmpty() == false ? leafType.getChildren().get(0) : null;
                return createListBlock(listCol, dataType, rowCount, elementType, dateFormatter, columnName);
            }
            boolean ancestorContributes = ancestorNulls != null && ancestorNulls.isEmpty() == false;
            boolean effectiveNoNulls = vector.noNulls && ancestorContributes == false;
            BitSet leafNulls = leafNullsFor(vector, rowCount);
            if (ancestorNulls != null) {
                leafNulls.or(ancestorNulls);
            }
            // A repeating leaf below per-row ancestor nulls must be expanded: the conversion
            // helpers treat isRepeating as "constant block from position 0" which would drop
            // ancestor-null variation at positions >0. We also need to materialize the value
            // array because the helpers iterate values[0..rowCount-1] in their non-repeating
            // branch, and ORC only guarantees values[0] is meaningful when isRepeating==true.
            boolean expandRepeating = vector.isRepeating && ancestorContributes;
            boolean effectiveRepeating = vector.isRepeating && ancestorContributes == false;
            return switch (dataType) {
                case BOOLEAN -> ColumnBlockConversions.booleanColumnFromLongs(
                    blockFactory,
                    longValuesFor((LongColumnVector) vector, rowCount, expandRepeating),
                    rowCount,
                    effectiveNoNulls,
                    effectiveRepeating,
                    leafNulls
                );
                case INTEGER -> ColumnBlockConversions.intColumnFromLongs(
                    blockFactory,
                    longValuesFor((LongColumnVector) vector, rowCount, expandRepeating),
                    rowCount,
                    effectiveNoNulls,
                    effectiveRepeating,
                    leafNulls
                );
                case LONG -> ColumnBlockConversions.longColumn(
                    blockFactory,
                    longValuesFor((LongColumnVector) vector, rowCount, expandRepeating),
                    rowCount,
                    effectiveNoNulls,
                    effectiveRepeating,
                    leafNulls,
                    true
                );
                case DOUBLE -> createDoubleBlock(vector, rowCount, effectiveNoNulls, leafNulls, effectiveRepeating, expandRepeating);
                case KEYWORD, TEXT -> createBytesRefBlock(vector, rowCount, effectiveNoNulls, leafNulls, effectiveRepeating);
                case DATETIME -> createDatetimeBlock(
                    vector,
                    rowCount,
                    effectiveNoNulls,
                    leafNulls,
                    effectiveRepeating,
                    leafType,
                    dateFormatter,
                    columnName
                );
                default -> blockFactory.newConstantNullBlock(rowCount);
            };
        }

        /**
         * Returns the long-valued backing array for {@code vector}, broadcasting position 0 to a
         * fresh per-row array when {@code expandRepeating} is true. ORC's repeating contract
         * only populates index 0, so iterating positions {@code >0} on the original array
         * would read stale data.
         */
        private static long[] longValuesFor(LongColumnVector vector, int rowCount, boolean expandRepeating) {
            if (expandRepeating == false) {
                return vector.vector;
            }
            long[] expanded = new long[rowCount];
            long v0 = vector.vector[0];
            for (int i = 0; i < rowCount; i++) {
                expanded[i] = v0;
            }
            return expanded;
        }

        private static double[] doubleValuesFor(DoubleColumnVector vector, int rowCount, boolean expandRepeating) {
            if (expandRepeating == false) {
                return vector.vector;
            }
            double[] expanded = new double[rowCount];
            double v0 = vector.vector[0];
            for (int i = 0; i < rowCount; i++) {
                expanded[i] = v0;
            }
            return expanded;
        }

        /**
         * Builds a per-row null bitset for {@code vector}. When the vector is repeating with
         * nulls, ORC only guarantees {@code isNull[0]} is meaningful — positions {@code >0} may
         * be stale, so broadcast position 0 to all rows rather than reading the array.
         */
        private static BitSet leafNullsFor(ColumnVector vector, int rowCount) {
            if (vector.noNulls) {
                return new BitSet(rowCount);
            }
            if (vector.isRepeating) {
                BitSet bits = new BitSet(rowCount);
                if (vector.isNull[0]) {
                    bits.set(0, rowCount);
                }
                return bits;
            }
            return ColumnBlockConversions.toBitSet(vector.isNull, rowCount);
        }

        private Block createListBlock(
            ListColumnVector listCol,
            DataType elementType,
            int rowCount,
            TypeDescription elementFileType,
            DateFormatter dateFormatter,
            String columnName
        ) {
            return switch (elementType) {
                case KEYWORD, TEXT -> createListBytesRefBlock(listCol, rowCount);
                case INTEGER -> createListIntBlock(listCol, rowCount);
                case LONG -> createListLongBlock(listCol, rowCount);
                case DOUBLE -> createListDoubleBlock(listCol, rowCount);
                case BOOLEAN -> createListBooleanBlock(listCol, rowCount);
                case DATETIME -> createListDatetimeBlock(listCol, rowCount, elementFileType, dateFormatter, columnName);
                default -> blockFactory.newConstantNullBlock(rowCount);
            };
        }

        /**
         * Number of non-null child elements in {@code [start, start+len)}. A null child element is
         * skipped rather than emitted (as a zero) — matching the Parquet list decode
         * ({@code ParquetColumnDecoding.readListRow} appends only defined elements) and ORC's own
         * {@code createListDatetimeBlock}. Callers treat a zero count (empty list or all-null
         * elements) as a null position.
         */
        private static int countNonNullElements(ColumnVector child, int start, int len) {
            if (child.noNulls) {
                return len;
            }
            int count = 0;
            for (int j = 0; j < len; j++) {
                if (child.isNull[start + j] == false) {
                    count++;
                }
            }
            return count;
        }

        private Block createListBytesRefBlock(ListColumnVector listCol, int rowCount) {
            BytesColumnVector child = (BytesColumnVector) listCol.child;
            try (var builder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (listCol.noNulls == false && listCol.isNull[i]) {
                        builder.appendNull();
                        continue;
                    }
                    int start = (int) listCol.offsets[i];
                    int len = (int) listCol.lengths[i];
                    if (countNonNullElements(child, start, len) == 0) {
                        builder.appendNull();
                        continue;
                    }
                    builder.beginPositionEntry();
                    for (int j = 0; j < len; j++) {
                        int idx = start + j;
                        if (child.noNulls == false && child.isNull[idx]) {
                            continue; // null element: skip, like the Parquet list decode
                        }
                        builder.appendBytesRef(
                            Utf8Sanitizer.sanitize(
                                new org.apache.lucene.util.BytesRef(child.vector[idx], child.start[idx], child.length[idx])
                            )
                        );
                    }
                    builder.endPositionEntry();
                }
                return builder.build();
            }
        }

        private Block createListIntBlock(ListColumnVector listCol, int rowCount) {
            LongColumnVector child = (LongColumnVector) listCol.child;
            try (var builder = blockFactory.newIntBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (listCol.noNulls == false && listCol.isNull[i]) {
                        builder.appendNull();
                        continue;
                    }
                    int start = (int) listCol.offsets[i];
                    int len = (int) listCol.lengths[i];
                    if (countNonNullElements(child, start, len) == 0) {
                        builder.appendNull();
                        continue;
                    }
                    builder.beginPositionEntry();
                    for (int j = 0; j < len; j++) {
                        int idx = start + j;
                        if (child.noNulls == false && child.isNull[idx]) {
                            continue; // null element: skip, like the Parquet list decode
                        }
                        builder.appendInt((int) child.vector[idx]);
                    }
                    builder.endPositionEntry();
                }
                return builder.build();
            }
        }

        private Block createListLongBlock(ListColumnVector listCol, int rowCount) {
            LongColumnVector child = (LongColumnVector) listCol.child;
            try (var builder = blockFactory.newLongBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (listCol.noNulls == false && listCol.isNull[i]) {
                        builder.appendNull();
                        continue;
                    }
                    int start = (int) listCol.offsets[i];
                    int len = (int) listCol.lengths[i];
                    if (countNonNullElements(child, start, len) == 0) {
                        builder.appendNull();
                        continue;
                    }
                    builder.beginPositionEntry();
                    for (int j = 0; j < len; j++) {
                        int idx = start + j;
                        if (child.noNulls == false && child.isNull[idx]) {
                            continue; // null element: skip, like the Parquet list decode
                        }
                        builder.appendLong(child.vector[idx]);
                    }
                    builder.endPositionEntry();
                }
                return builder.build();
            }
        }

        private Block createListDoubleBlock(ListColumnVector listCol, int rowCount) {
            ColumnVector child = listCol.child;
            double d64ScaleFactor = child instanceof Decimal64ColumnVector d64 ? Math.pow(10, d64.scale) : 0;
            try (var builder = blockFactory.newDoubleBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (listCol.noNulls == false && listCol.isNull[i]) {
                        builder.appendNull();
                        continue;
                    }
                    int start = (int) listCol.offsets[i];
                    int len = (int) listCol.lengths[i];
                    if (countNonNullElements(child, start, len) == 0) {
                        builder.appendNull();
                        continue;
                    }
                    builder.beginPositionEntry();
                    for (int j = 0; j < len; j++) {
                        int idx = start + j;
                        if (child.noNulls == false && child.isNull[idx]) {
                            continue; // null element: skip, like the Parquet list decode
                        }
                        builder.appendDouble(readDoubleFrom(child, idx, d64ScaleFactor));
                    }
                    builder.endPositionEntry();
                }
                return builder.build();
            }
        }

        private static double readDoubleFrom(ColumnVector vector, int idx, double d64ScaleFactor) {
            if (vector instanceof DoubleColumnVector dv) {
                return dv.vector[idx];
            } else if (vector instanceof DecimalColumnVector decV) {
                return decV.vector[idx].doubleValue();
            } else if (vector instanceof Decimal64ColumnVector d64) {
                return d64.vector[idx] / d64ScaleFactor;
            }
            throw new IllegalArgumentException("Unsupported list element type: " + vector.getClass().getSimpleName());
        }

        private Block createListBooleanBlock(ListColumnVector listCol, int rowCount) {
            LongColumnVector child = (LongColumnVector) listCol.child;
            try (var builder = blockFactory.newBooleanBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (listCol.noNulls == false && listCol.isNull[i]) {
                        builder.appendNull();
                        continue;
                    }
                    int start = (int) listCol.offsets[i];
                    int len = (int) listCol.lengths[i];
                    if (countNonNullElements(child, start, len) == 0) {
                        builder.appendNull();
                        continue;
                    }
                    builder.beginPositionEntry();
                    for (int j = 0; j < len; j++) {
                        int idx = start + j;
                        if (child.noNulls == false && child.isNull[idx]) {
                            continue; // null element: skip, like the Parquet list decode
                        }
                        builder.appendBoolean(child.vector[idx] != 0);
                    }
                    builder.endPositionEntry();
                }
                return builder.build();
            }
        }

        /**
         * LIST&lt;datetime&gt; decode. Null elements are skipped — never emitted as epoch 0 —
         * matching the Parquet list decode ({@code ParquetColumnDecoding.readListRow} appends
         * only defined elements); a row whose elements are all null (or an empty/null list) is a
         * null position. The string&rarr;datetime coercion arm parses each element via the shared
         * scalar with the declared format (ISO default); a parse failure follows the read's error
         * policy with {@code castBlock}'s bulk semantics — the whole position nulls + warns, or
         * propagates under {@code fail_fast} — so each row is gathered into a primitive scratch
         * before appending.
         */
        private Block createListDatetimeBlock(
            ListColumnVector listCol,
            int rowCount,
            TypeDescription elementFileType,
            DateFormatter dateFormatter,
            String columnName
        ) {
            ColumnVector child = listCol.child;
            // Same discriminator as the flat datetime path: ORC DATE elements store days (scale to
            // millis); integer elements declared `datetime` are already epoch millis (reinterpret).
            long scale = elementFileType == null || elementFileType.getCategory() == TypeDescription.Category.DATE ? MILLIS_PER_DAY : 1L;
            long[] parsed = new long[8];
            try (var builder = blockFactory.newLongBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (listCol.noNulls == false && listCol.isNull[i]) {
                        builder.appendNull();
                        continue;
                    }
                    int start = (int) listCol.offsets[i];
                    int len = (int) listCol.lengths[i];
                    if (parsed.length < len) {
                        parsed = new long[len];
                    }
                    int count = 0;
                    boolean failed = false;
                    for (int j = 0; j < len && failed == false; j++) {
                        int idx = start + j;
                        if (child.noNulls == false && child.isNull[idx]) {
                            continue; // null element: skip, like the Parquet list decode
                        }
                        if (child instanceof TimestampColumnVector ts) {
                            parsed[count++] = ts.getTime(idx);
                        } else if (child instanceof LongColumnVector lv) {
                            parsed[count++] = lv.vector[idx] * scale;
                        } else if (child instanceof BytesColumnVector bv) {
                            String value = new String(
                                bv.vector[idx],
                                bv.start[idx],
                                bv.length[idx],
                                java.nio.charset.StandardCharsets.UTF_8
                            );
                            try {
                                parsed[count++] = DeclaredTypeCoercions.parseDatetimeMillis(value, dateFormatter);
                            } catch (IllegalArgumentException | DateTimeException e) {
                                DeclaredTypeCoercions.onCoercionFailure(
                                    columnName,
                                    DataType.KEYWORD,
                                    DataType.DATETIME,
                                    e,
                                    coercionWarnings()
                                );
                                failed = true;
                            }
                        } else {
                            throw new IllegalArgumentException(
                                "Unsupported list child type for DATETIME: " + child.getClass().getSimpleName()
                            );
                        }
                    }
                    if (failed || count == 0) {
                        // failed: bulk semantics null the whole position (already warned above).
                        // count == 0: empty list or all-null elements — a null position.
                        builder.appendNull();
                        continue;
                    }
                    builder.beginPositionEntry();
                    for (int v = 0; v < count; v++) {
                        builder.appendLong(parsed[v]);
                    }
                    builder.endPositionEntry();
                }
                return builder.build();
            }
        }

        private Block createDoubleBlock(
            ColumnVector vector,
            int rowCount,
            boolean effectiveNoNulls,
            BitSet effectiveNulls,
            boolean effectiveRepeating,
            boolean expandRepeating
        ) {
            if (vector instanceof DoubleColumnVector doubleVector) {
                return ColumnBlockConversions.doubleColumn(
                    blockFactory,
                    doubleValuesFor(doubleVector, rowCount, expandRepeating),
                    rowCount,
                    effectiveNoNulls,
                    effectiveRepeating,
                    effectiveNulls,
                    true
                );
            } else if (vector instanceof DecimalColumnVector decVector) {
                return createDecimalDoubleBlock(decVector, rowCount, effectiveNoNulls, effectiveNulls, effectiveRepeating);
            } else if (vector instanceof Decimal64ColumnVector dec64Vector) {
                // Decimal64ColumnVector extends LongColumnVector — must check before LongColumnVector
                return createDecimal64DoubleBlock(dec64Vector, rowCount, effectiveNoNulls, effectiveNulls, effectiveRepeating);
            } else if (vector instanceof LongColumnVector longVector) {
                return ColumnBlockConversions.doubleColumnFromLongs(
                    blockFactory,
                    longValuesFor(longVector, rowCount, expandRepeating),
                    rowCount,
                    effectiveNoNulls,
                    effectiveRepeating,
                    effectiveNulls
                );
            }
            throw new IllegalArgumentException("Unsupported column type: " + vector.getClass().getSimpleName());
        }

        /**
         * Converts a {@link DecimalColumnVector} (arbitrary precision) to a double block.
         * Each element is a {@code HiveDecimalWritable} whose {@code doubleValue()} returns the
         * properly scaled value. Precision loss beyond ~15 significant digits is inherent to double.
         */
        private Block createDecimalDoubleBlock(
            DecimalColumnVector decVector,
            int rowCount,
            boolean effectiveNoNulls,
            BitSet effectiveNulls,
            boolean effectiveRepeating
        ) {
            if (effectiveRepeating) {
                if (effectiveNoNulls == false && (decVector.isNull[0] || (effectiveNulls != null && effectiveNulls.get(0)))) {
                    return blockFactory.newConstantNullBlock(rowCount);
                }
                return blockFactory.newConstantDoubleBlockWith(decVector.vector[0].doubleValue(), rowCount);
            }
            boolean readFromZero = decVector.isRepeating;
            try (var builder = blockFactory.newDoubleBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (effectiveNoNulls == false && effectiveNulls.get(i)) {
                        builder.appendNull();
                    } else {
                        builder.appendDouble(decVector.vector[readFromZero ? 0 : i].doubleValue());
                    }
                }
                return builder.build();
            }
        }

        /**
         * Converts a {@link Decimal64ColumnVector} (precision &le; 18) to a double block.
         * Values are stored as unscaled longs; dividing by 10^scale recovers the decimal value.
         */
        private Block createDecimal64DoubleBlock(
            Decimal64ColumnVector dec64Vector,
            int rowCount,
            boolean effectiveNoNulls,
            BitSet effectiveNulls,
            boolean effectiveRepeating
        ) {
            double scaleFactor = Math.pow(10, dec64Vector.scale);
            if (effectiveRepeating) {
                if (effectiveNoNulls == false && (dec64Vector.isNull[0] || (effectiveNulls != null && effectiveNulls.get(0)))) {
                    return blockFactory.newConstantNullBlock(rowCount);
                }
                return blockFactory.newConstantDoubleBlockWith(dec64Vector.vector[0] / scaleFactor, rowCount);
            }
            boolean readFromZero = dec64Vector.isRepeating;
            try (var builder = blockFactory.newDoubleBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (effectiveNoNulls == false && effectiveNulls.get(i)) {
                        builder.appendNull();
                    } else {
                        builder.appendDouble(dec64Vector.vector[readFromZero ? 0 : i] / scaleFactor);
                    }
                }
                return builder.build();
            }
        }

        private Block createBytesRefBlock(
            ColumnVector vector,
            int rowCount,
            boolean effectiveNoNulls,
            BitSet effectiveNulls,
            boolean effectiveRepeating
        ) {
            Check.isTrue(vector instanceof BytesColumnVector, "Unsupported column type: " + vector.getClass().getSimpleName());
            BytesColumnVector bytesVector = (BytesColumnVector) vector;
            if (effectiveRepeating) {
                if (effectiveNoNulls == false && (bytesVector.isNull[0] || (effectiveNulls != null && effectiveNulls.get(0)))) {
                    return blockFactory.newConstantNullBlock(rowCount);
                }
                return blockFactory.newConstantBytesRefBlockWith(
                    Utf8Sanitizer.sanitize(
                        new org.apache.lucene.util.BytesRef(bytesVector.vector[0], bytesVector.start[0], bytesVector.length[0])
                    ),
                    rowCount
                );
            }
            // If the underlying vector is repeating but the leaf isn't (ancestor nulls forced us
            // here), positions >0 hold stale bytes. Read from position 0 for every non-null row.
            boolean readFromZero = vector.isRepeating;
            try (var builder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (effectiveNoNulls == false && effectiveNulls.get(i)) {
                        builder.appendNull();
                    } else {
                        int idx = readFromZero ? 0 : i;
                        builder.appendBytesRef(
                            Utf8Sanitizer.sanitize(
                                new org.apache.lucene.util.BytesRef(
                                    bytesVector.vector[idx],
                                    bytesVector.start[idx],
                                    bytesVector.length[idx]
                                )
                            )
                        );
                    }
                }
                return builder.build();
            }
        }

        private Block createDatetimeBlock(
            ColumnVector vector,
            int rowCount,
            boolean effectiveNoNulls,
            BitSet effectiveNulls,
            boolean effectiveRepeating,
            TypeDescription leafType,
            DateFormatter dateFormatter,
            String columnName
        ) {
            if (vector instanceof TimestampColumnVector tsVector) {
                if (effectiveRepeating) {
                    if (effectiveNoNulls == false && (tsVector.isNull[0] || (effectiveNulls != null && effectiveNulls.get(0)))) {
                        return blockFactory.newConstantNullBlock(rowCount);
                    }
                    return blockFactory.newConstantLongBlockWith(tsVector.getTime(0), rowCount);
                }
                long[] millis = new long[rowCount];
                // Under !effectiveRepeating but vector.isRepeating, only position 0 is meaningful;
                // positions where leafNulls is set are skipped downstream so stale reads are OK.
                if (vector.isRepeating) {
                    long t0 = tsVector.getTime(0);
                    for (int i = 0; i < rowCount; i++) {
                        millis[i] = t0;
                    }
                } else {
                    for (int i = 0; i < rowCount; i++) {
                        millis[i] = tsVector.getTime(i);
                    }
                }
                if (effectiveNoNulls) {
                    return blockFactory.newLongArrayVector(millis, rowCount).asBlock();
                }
                return blockFactory.newLongArrayBlock(millis, rowCount, null, effectiveNulls, Block.MvOrdering.UNORDERED);
            } else if (vector instanceof LongColumnVector longVector) {
                // Two file types land here and need opposite scaling: an ORC DATE stores days since
                // epoch (scale to millis), while an integer column declared `datetime`
                // (DeclaredTypeCoercions LONG->DATETIME) already stores epoch millis (reinterpret,
                // no scaling). The file schema's leaf category is the discriminator; DATE is the
                // historical default for a null leaf type (pre-coercion, DATE was the only
                // LongColumnVector source that mapped to DATETIME).
                long scale = leafType == null || leafType.getCategory() == TypeDescription.Category.DATE ? MILLIS_PER_DAY : 1L;
                if (effectiveRepeating) {
                    if (effectiveNoNulls == false && (longVector.isNull[0] || (effectiveNulls != null && effectiveNulls.get(0)))) {
                        return blockFactory.newConstantNullBlock(rowCount);
                    }
                    return blockFactory.newConstantLongBlockWith(longVector.vector[0] * scale, rowCount);
                }
                long[] millis = new long[rowCount];
                if (vector.isRepeating) {
                    long v0 = longVector.vector[0] * scale;
                    for (int i = 0; i < rowCount; i++) {
                        millis[i] = v0;
                    }
                } else {
                    for (int i = 0; i < rowCount; i++) {
                        millis[i] = longVector.vector[i] * scale;
                    }
                }
                if (effectiveNoNulls) {
                    return blockFactory.newLongArrayVector(millis, rowCount).asBlock();
                }
                return blockFactory.newLongArrayBlock(millis, rowCount, null, effectiveNulls, Block.MvOrdering.UNORDERED);
            } else if (vector instanceof BytesColumnVector bytesVector) {
                // Declared string->datetime coercion: parse each value with the column's declared
                // format (ISO default) via the shared DeclaredTypeCoercions scalar - the same
                // conversion the text readers apply at parse time. A parse failure follows the
                // read's error policy through onCoercionFailure: fail_fast propagates, anything
                // else nulls the cell + warns — the same per-cell outcome as castBlock.
                try (var builder = blockFactory.newLongBlockBuilder(rowCount)) {
                    for (int i = 0; i < rowCount; i++) {
                        int idx = bytesVector.isRepeating ? 0 : i;
                        boolean isNull = (effectiveNoNulls == false && effectiveNulls != null && effectiveNulls.get(i))
                            || (bytesVector.noNulls == false && bytesVector.isNull[idx]);
                        if (isNull) {
                            builder.appendNull();
                        } else {
                            String value = new String(
                                bytesVector.vector[idx],
                                bytesVector.start[idx],
                                bytesVector.length[idx],
                                java.nio.charset.StandardCharsets.UTF_8
                            );
                            try {
                                builder.appendLong(DeclaredTypeCoercions.parseDatetimeMillis(value, dateFormatter));
                            } catch (IllegalArgumentException | DateTimeException e) {
                                DeclaredTypeCoercions.onCoercionFailure(
                                    columnName,
                                    DataType.KEYWORD,
                                    DataType.DATETIME,
                                    e,
                                    coercionWarnings()
                                );
                                builder.appendNull();
                            }
                        }
                    }
                    return builder.build();
                }
            }
            return blockFactory.newConstantNullBlock(rowCount);
        }

        @Override
        public void close() throws IOException {
            try {
                rows.close();
            } finally {
                reader.close();
            }
        }
    }

    private static class RowLimitingIterator implements CloseableIterator<Page> {
        private final CloseableIterator<Page> delegate;
        private int remaining;

        RowLimitingIterator(CloseableIterator<Page> delegate, int rowLimit) {
            if (rowLimit <= 0) {
                throw new QlIllegalArgumentException("rowLimit must be positive, got: " + rowLimit);
            }
            this.delegate = delegate;
            this.remaining = rowLimit;
        }

        @Override
        public boolean hasNext() {
            if (remaining <= 0) {
                return false;
            }
            return delegate.hasNext();
        }

        @Override
        public Page next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            Page page = delegate.next();
            int rows = page.getPositionCount();
            if (rows > remaining) {
                Page truncated;
                try {
                    truncated = page.slice(0, remaining);
                } finally {
                    page.releaseBlocks();
                }
                page = truncated;
                remaining = 0;
                try {
                    delegate.close();
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to close ORC reader", e);
                }
            } else {
                remaining -= rows;
            }
            return page;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

}
