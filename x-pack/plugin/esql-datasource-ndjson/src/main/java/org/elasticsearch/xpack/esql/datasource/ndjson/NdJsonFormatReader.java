/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStats;
import org.elasticsearch.xpack.esql.datasources.cache.SchemaCacheKey;
import org.elasticsearch.xpack.esql.datasources.cache.TextFormatStats;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * FormatReader implementation for NDJSON files.
 * Implements {@link SegmentableFormatReader} for intra-file parallel parsing.
 */
public class NdJsonFormatReader implements SegmentableFormatReader {

    private static final Logger logger = LogManager.getLogger(NdJsonFormatReader.class);
    private static final NdJsonRecordSplitter DEFAULT_RECORD_SPLITTER = new NdJsonRecordSplitter(
        SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES
    );

    public static final String SCHEMA_SAMPLE_SIZE_SETTING = "esql.datasource.ndjson.schema_sample_size";
    public static final int DEFAULT_SCHEMA_SAMPLE_SIZE = 20_000;

    /**
     * Node-level setting for the parallel-parsing segment size. Larger segments amortise the fixed
     * Java/Jackson per-segment setup cost; smaller segments enable parallelism on smaller files.
     * Also overridable per-query via the {@code segment_size} key in {@code WITH {...}}.
     */
    public static final String SEGMENT_SIZE_SETTING = "esql.datasource.ndjson.segment_size";

    /**
     * 4 MiB, larger than the SPI's 1 MiB. Each NDJSON segment pays a fixed Java/Jackson setup cost
     * (schema lookup, {@link FormatReadContext} creation, {@link NdJsonPageIterator} +
     * {@link NdJsonPageDecoder} construction, range-stream wrapping, queue coordination), so cutting
     * the segment count by 4x cuts that overhead by ~4x. ClickHouse's 1 MiB sweet spot does not
     * carry over because their per-chunk overhead is much lower (no per-chunk object allocation in
     * the C++ path). Files below {@code 2 * segment_size} (~8 MiB at the default) parse
     * single-threaded; that matches where per-chunk setup actually amortises.
     */
    public static final ByteSizeValue DEFAULT_SEGMENT_SIZE = ByteSizeValue.ofMb(4);

    /** Below 64 KiB, per-chunk overhead dominates parse cost; reject silly configurations early. */
    static final ByteSizeValue MIN_SEGMENT_SIZE = ByteSizeValue.ofKb(64);

    /** Buffer size used to accelerate schema-inference line skipping on cold (unbuffered) streams. */
    private static final int SCAN_BUFFER_SIZE = 8 * 1024;

    static final String CONFIG_SCHEMA_SAMPLE_SIZE = "schema_sample_size";
    static final String CONFIG_SEGMENT_SIZE = "segment_size";
    static final String CONFIG_DATETIME_FORMAT = "datetime_format";

    /** Keys recognised by {@link #withConfigTrackingConsumedKeys(Map)}. */
    static final Set<String> RECOGNIZED_KEYS = Set.of(CONFIG_SCHEMA_SAMPLE_SIZE, CONFIG_SEGMENT_SIZE, CONFIG_DATETIME_FORMAT);

    private final BlockFactory blockFactory;
    private final Settings settings;
    private final List<Attribute> resolvedSchema;
    private final int schemaSampleSize;
    private final long segmentSizeBytes;
    private final DateFormatter datetimeFormatter;
    /**
     * Per-column declared date parse-patterns, keyed by <b>physical</b> (file) column name (the caller applied any
     * {@code path} rename); empty when none. Set via {@link #withDeclaredDateFormats}; each {@link NdJsonPageDecoder}
     * resolves its per-column {@link DateFormatter} from it.
     */
    private final Map<String, String> declaredDateFormats;
    /**
     * Physical (file) names of declared-type columns; empty when none. Set via {@link #withDeclaredTypeColumns} and
     * threaded to each {@link NdJsonPageDecoder} so a cross-kind token on a declared column routes through the error
     * policy instead of silently reading as null (an inferred column keeps the schema-on-read null tolerance). The
     * text formats keep the SPI no-op default for the whole-column null-fill decision; NDJSON only needs the set to
     * pick the per-value cross-kind policy, so it consumes it here rather than in the SPI default.
     */
    private final Set<String> declaredTypeColumns;
    /**
     * Node-stable identity of the row-interpretation-affecting {@code WITH} config, per
     * {@link SchemaCacheKey#buildFormatConfig} — the external-stats cache fingerprint. Derived from
     * the canonical config rather than the projected/resolved schema so a data node's shipped-back
     * contribution matches the coordinator's cache entry across JVMs. Empty until {@link #withConfig}.
     */
    private final String canonicalConfig;
    // Mutable reader-level counters surfaced as a Map<String, Object> via {@link #statusSnapshot()};
    // shared across the parallel {@link NdJsonPageDecoder} segments spawned by {@link #read}.
    private final NdJsonReaderCounters counters = new NdJsonReaderCounters();

    public NdJsonFormatReader(Settings settings, BlockFactory blockFactory, List<Attribute> resolvedSchema) {
        this(settings, blockFactory, resolvedSchema, schemaSampleSize(settings), segmentSize(settings), null, "", Map.of(), Set.of());
    }

    NdJsonFormatReader(Settings settings, BlockFactory blockFactory) {
        this(settings, blockFactory, null);
    }

    private NdJsonFormatReader(
        Settings settings,
        BlockFactory blockFactory,
        List<Attribute> resolvedSchema,
        int schemaSampleSize,
        long segmentSizeBytes,
        DateFormatter datetimeFormatter,
        String canonicalConfig,
        Map<String, String> declaredDateFormats,
        Set<String> declaredTypeColumns
    ) {
        this.blockFactory = blockFactory;
        this.settings = settings == null ? Settings.EMPTY : settings;
        this.resolvedSchema = resolvedSchema;
        this.schemaSampleSize = schemaSampleSize;
        this.segmentSizeBytes = segmentSizeBytes;
        this.datetimeFormatter = datetimeFormatter;
        this.canonicalConfig = canonicalConfig;
        this.declaredDateFormats = declaredDateFormats != null ? Map.copyOf(declaredDateFormats) : Map.of();
        this.declaredTypeColumns = declaredTypeColumns != null ? Set.copyOf(declaredTypeColumns) : Set.of();
    }

    @Override
    public NdJsonFormatReader withSchema(List<Attribute> schema) {
        return new NdJsonFormatReader(
            settings,
            blockFactory,
            schema,
            schemaSampleSize,
            segmentSizeBytes,
            datetimeFormatter,
            canonicalConfig,
            declaredDateFormats,
            declaredTypeColumns
        );
    }

    @Override
    public NdJsonFormatReader withDeclaredDateFormats(Map<String, String> physicalNameToPattern) {
        if (physicalNameToPattern == null || physicalNameToPattern.isEmpty()) {
            return this;
        }
        return new NdJsonFormatReader(
            settings,
            blockFactory,
            resolvedSchema,
            schemaSampleSize,
            segmentSizeBytes,
            datetimeFormatter,
            canonicalConfig,
            physicalNameToPattern,
            declaredTypeColumns
        );
    }

    /**
     * Declared-type columns keyed by physical (file) name. NDJSON does not make the whole-column null-fill
     * decision the by-name columnar readers do; it consumes this set to decide, per value, whether a cross-kind
     * token routes through the error policy (declared) or keeps the schema-on-read silent null (inferred).
     */
    @Override
    public NdJsonFormatReader withDeclaredTypeColumns(Set<String> physicalDeclaredColumns) {
        if (physicalDeclaredColumns == null || physicalDeclaredColumns.isEmpty()) {
            return this;
        }
        return new NdJsonFormatReader(
            settings,
            blockFactory,
            resolvedSchema,
            schemaSampleSize,
            segmentSizeBytes,
            datetimeFormatter,
            canonicalConfig,
            declaredDateFormats,
            physicalDeclaredColumns
        );
    }

    @Override
    public Configured<FormatReader> withConfigTrackingConsumedKeys(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return Configured.empty(this);
        }
        int newSampleSize = parseInt(config.get(CONFIG_SCHEMA_SAMPLE_SIZE), schemaSampleSize);
        Check.isTrue(newSampleSize > 0, CONFIG_SCHEMA_SAMPLE_SIZE + " must be positive, got: {}", newSampleSize);
        long newSegmentSize = parseSegmentSize(config.get(CONFIG_SEGMENT_SIZE), segmentSizeBytes);
        DateFormatter newDatetimeFormatter = parseDatetimeFormat(config.get(CONFIG_DATETIME_FORMAT), datetimeFormatter);

        // Pin the node-stable config identity from THIS query's WITH config (see CsvFormatReader).
        String canon = SchemaCacheKey.buildFormatConfig(config);

        FormatReader result = new NdJsonFormatReader(
            settings,
            blockFactory,
            resolvedSchema,
            newSampleSize,
            newSegmentSize,
            newDatetimeFormatter,
            canon,
            declaredDateFormats,
            declaredTypeColumns
        );
        return Configured.fromKnownSubset(result, config, RECOGNIZED_KEYS);
    }

    private List<Attribute> inferSchemaIfNeeded(List<Attribute> attributes, StorageObject object, boolean skipFirstLine)
        throws IOException {
        if (attributes != null) {
            // Empty schema means the optimizer pruned every column (COUNT(*) etc.); skip inference
            // entirely. The decoder treats an empty projection list as "structure-only", so there
            // is nothing to type-check against.
            if (attributes.isEmpty()) {
                return attributes;
            }
            if (needsFullSchemaSupplement(attributes)) {
                List<Attribute> inferred;
                try (var stream = openForSchemaInference(object, skipFirstLine)) {
                    inferred = NdJsonSchemaInferrer.inferSchema(stream, schemaSampleSize, datetimeFormatter);
                }
                return mergeInferredWithPreferred(inferred, attributes);
            }
            return attributes;
        }

        try (var stream = openForSchemaInference(object, skipFirstLine)) {
            return NdJsonSchemaInferrer.inferSchema(stream, schemaSampleSize, datetimeFormatter);
        }
    }

    /**
     * Opens a stream for schema inference, skipping the first partial line when the underlying
     * range does not start at a record boundary (non-first splits). This prevents the inferrer
     * from tripping over the truncated record fragment that precedes the first newline in a
     * mid-file split.
     *
     * <p>Package-visible for testing.
     */
    static InputStream openForSchemaInference(StorageObject object, boolean skipFirstLine) throws IOException {
        InputStream raw = object.newStream();
        // Buffering up front gives the scan below and the returned stream the 8 KB fast path;
        // the Pushback wrapper lets the lone-CR path unread its peeked byte so the returned
        // stream starts on the first byte of the next record without allocating a prefix stream.
        PushbackInputStream stream = new PushbackInputStream(new BufferedInputStream(raw, SCAN_BUFFER_SIZE), 1);
        if (skipFirstLine) {
            try {
                NdJsonRecordSplitter splitter = defaultRecordSplitter();
                NdJsonRecordSplitter.LineScan scan = splitter.scanForTerminator(stream);
                if (scan.consumed() == RecordSplitter.RECORD_TOO_LARGE) {
                    throw splitter.recordTooLargeException();
                }
                if (scan.peekedByte() != -1) {
                    stream.unread(scan.peekedByte());
                }
            } catch (IOException e) {
                try {
                    object.abortStream(raw);
                } catch (IOException ignored) {}
                throw e;
            }
        }
        // Override close() to abort the raw stream rather than drain it: the caller reads
        // only a schema sample, so providers that drain on close (e.g. S3) would otherwise
        // block for as long as it takes to consume the remaining object bytes. The closed flag
        // honours the InputStream.close() idempotency contract — callers that close defensively
        // twice (or chain close in finally blocks) must not double-abort the underlying stream,
        // since Abortable does not contractually guarantee abort() idempotency.
        return new FilterInputStream(stream) {
            private volatile boolean closed;

            @Override
            public void close() throws IOException {
                if (closed) {
                    return;
                }
                closed = true;
                // only abort the raw stream; the PushbackInputStream/BufferedInputStream wrappers hold no external resources
                object.abortStream(raw);
            }
        };
    }

    /**
     * Coordinator-supplied schemas may only list projected columns. Dotted columns such as {@code languages.long}
     * need their prefix column ({@code languages}) present for NDJSON decoding; detect that case and merge with a
     * fresh file inference pass.
     */
    private static boolean needsFullSchemaSupplement(List<Attribute> attributes) {
        for (Attribute a : attributes) {
            String name = a.name();
            int dot = name.indexOf('.');
            if (dot <= 0) {
                continue;
            }
            String prefix = name.substring(0, dot);
            boolean hasPrefix = false;
            for (Attribute o : attributes) {
                if (o.name().equals(prefix)) {
                    hasPrefix = true;
                    break;
                }
            }
            if (hasPrefix == false) {
                return true;
            }
        }
        return false;
    }

    /**
     * Resolve the effective schema when the planner has bound a read schema. When the
     * coordinator-side projection ({@code resolvedSchema}) is unavailable, the bound schema is used
     * as-is. Otherwise the bound schema's column order is preserved and projection types/nullability
     * overlay matching names — same semantics as {@link #mergeInferredWithPreferred}, just with the
     * planner-supplied schema standing in for the per-file inference result.
     */
    private static List<Attribute> mergeBoundWithProjection(List<Attribute> bound, List<Attribute> projection) {
        if (projection == null || projection.isEmpty()) {
            return bound;
        }
        return mergeInferredWithPreferred(bound, projection);
    }

    /**
     * Union by column name: inferred file order first, then overlay coordinator types/nullability for matching names.
     */
    private static List<Attribute> mergeInferredWithPreferred(List<Attribute> inferred, List<Attribute> preferred) {
        Map<String, Attribute> byName = new LinkedHashMap<>();
        for (Attribute a : inferred) {
            byName.put(a.name(), a);
        }
        for (Attribute p : preferred) {
            byName.put(p.name(), p);
        }
        return List.copyOf(byName.values());
    }

    private static int schemaSampleSize(Settings settings) {
        Settings resolved = settings == null ? Settings.EMPTY : settings;
        return resolved.getAsInt(SCHEMA_SAMPLE_SIZE_SETTING, DEFAULT_SCHEMA_SAMPLE_SIZE);
    }

    private static long segmentSize(Settings settings) {
        Settings resolved = settings == null ? Settings.EMPTY : settings;
        ByteSizeValue value = resolved.getAsBytesSize(SEGMENT_SIZE_SETTING, DEFAULT_SEGMENT_SIZE);
        long bytes = value.getBytes();
        Check.isTrue(bytes >= MIN_SEGMENT_SIZE.getBytes(), "{} must be >= {}, got: {}", SEGMENT_SIZE_SETTING, MIN_SEGMENT_SIZE, value);
        return bytes;
    }

    private static int parseInt(Object value, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value [" + value + "]", e);
        }
    }

    private static long parseSegmentSize(Object value, long defaultValueBytes) {
        if (value == null) {
            return defaultValueBytes;
        }
        ByteSizeValue parsed = ByteSizeValue.parseBytesSizeValue(value.toString(), CONFIG_SEGMENT_SIZE);
        long bytes = parsed.getBytes();
        Check.isTrue(bytes >= MIN_SEGMENT_SIZE.getBytes(), CONFIG_SEGMENT_SIZE + " must be >= {}, got: {}", MIN_SEGMENT_SIZE, parsed);
        return bytes;
    }

    private static DateFormatter parseDatetimeFormat(Object value, DateFormatter baseline) {
        if (value == null || value.toString().isEmpty()) {
            return baseline;
        }
        try {
            return DateFormatter.forPattern(value.toString());
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid datetime_format [" + value + "]", e);
        }
    }

    @Override
    public SourceMetadata metadata(StorageObject object) throws IOException {
        InputStream stream = object.newStream();
        // Abort rather than close so S3-style providers don't drain the remaining body to reuse
        // the connection (the schema sample touches only a small prefix). Wrapping the abort in
        // a Closeable lets try-with-resources attach any abort-time error as a suppressed
        // exception on the primary failure rather than replacing it.
        try (Closeable abortOnExit = () -> object.abortStream(stream)) {
            List<Attribute> schema = NdJsonSchemaInferrer.inferSchema(stream, schemaSampleSize, datetimeFormatter);
            String location = object.path().toString();
            long mtimeMillis;
            try {
                Instant mtime = object.lastModified();
                if (mtime == null) {
                    return new SimpleSourceMetadata(schema, formatName(), location);
                }
                mtimeMillis = mtime.toEpochMilli();
            } catch (IOException e) {
                return new SimpleSourceMetadata(schema, formatName(), location);
            }
            OptionalLong cachedSize;
            try {
                cachedSize = OptionalLong.of(object.length());
            } catch (IOException | UnsupportedOperationException e) {
                cachedSize = OptionalLong.empty();
            }
            String configFingerprint = computeConfigFingerprint();
            // Cold resolution publishes only the file size + identity; row/column stats arrive via the
            // data-node capture → coordinator reconcile into SchemaCacheEntry.
            SourceStatistics stats = TextFormatStats.build(Optional.empty(), cachedSize, schema);
            Map<String, Object> baseSourceMetadata = Map.of(
                ExternalStats.MTIME_MILLIS_KEY,
                mtimeMillis,
                ExternalStats.CONFIG_FINGERPRINT_KEY,
                configFingerprint
            );
            Map<String, Object> sourceMetadata = SourceStatisticsSerializer.embedStatistics(baseSourceMetadata, stats);
            return new SimpleSourceMetadata(schema, formatName(), location, stats, null, sourceMetadata, null);
        }
    }

    /**
     * Node-stable identity of the row-interpretation-affecting {@code WITH} config — the same
     * canonical string {@link SchemaCacheKey#buildFormatConfig} stores on the cache key, so a data
     * node's contribution and the coordinator's entry compare equal across JVMs. Derived from the
     * canonical config rather than the resolved schema (which is projection-dependent and would
     * differ between a coordinator's full-schema resolution and a data node's projected read).
     */
    private String computeConfigFingerprint() {
        return canonicalConfig;
    }

    /**
     * Convenience overload that builds a minimal {@link FormatReadContext} without a planner-resolved
     * {@code readSchema}. Safe for single-file reads (reader falls back to per-file inference); do
     * NOT use to iterate the files of a multi-file glob — cross-file type drift will not be prevented.
     *
     * @deprecated use {@link #read(StorageObject, FormatReadContext)} so callers express
     *             {@code readSchema} intent (or {@code null}) explicitly.
     */
    @Deprecated
    @Override
    public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
        return read(
            object,
            FormatReadContext.builder().projectedColumns(projectedColumns).batchSize(batchSize).errorPolicy(defaultErrorPolicy()).build()
        );
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug(
                "NDJSON read [{}]: readSchema={}, firstSplit={}, recordAligned={}, projection={}",
                object.path(),
                context.readSchema() == null ? "null" : "present(" + context.readSchema().size() + ")",
                context.firstSplit(),
                context.recordAligned(),
                context.projectedColumns() == null ? "null" : context.projectedColumns().size()
            );
        }
        // Mirror {@link org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader}: parallel byte-range
        // splits from {@link org.elasticsearch.xpack.esql.datasources.ParallelParsingCoordinator} set
        // {@code recordAligned=true}, signalling segment boundaries already fall on {@code \n}. Do not drop the
        // first complete row or trim a trailing partial row in that mode — doing so mis-handles aligned segments
        // (skipped rows, or truncated JSON when trim interacts with bounded range streams).
        boolean skipFirstLine = context.firstSplit() == false && context.recordAligned() == false;
        boolean trimLastPartialLine = context.lastSplit() == false && context.recordAligned() == false;
        ErrorPolicy errorPolicy = context.errorPolicy() != null ? context.errorPolicy() : defaultErrorPolicy();
        // Bound read schema wins when non-null; null falls through to per-file inference.
        // Prevents cross-file type drift on multi-file globs (e.g. y:LONG vs file-with-1.5 y:DOUBLE).
        List<Attribute> effectiveSchema = context.readSchema() == null
            ? inferSchemaIfNeeded(resolvedSchema, object, skipFirstLine)
            : mergeBoundWithProjection(context.readSchema(), resolvedSchema);
        // Whole-file read: first + last split, no parallel slicing. See CsvFormatReader.read for the
        // rationale. mtime is pinned here at open-time so a mid-scan file replacement cannot pair a
        // new mtime with old data.
        boolean wholeFileRead = context.firstSplit() && context.recordAligned() == false && context.lastSplit();
        boolean chunkMode = context.recordAligned();
        boolean cacheable = wholeFileRead || chunkMode;
        long pinnedMtimeMillis = -1L;
        if (cacheable) {
            try {
                Instant openMtime = object.lastModified();
                if (openMtime != null) {
                    pinnedMtimeMillis = openMtime.toEpochMilli();
                } else {
                    cacheable = false;
                    chunkMode = false;
                }
            } catch (IOException e) {
                cacheable = false;
                chunkMode = false;
            }
        }
        // Fingerprint is computed lazily in the iterator's close hook once the decoder has resolved
        // its projected attributes — schema resolution can lag past iterator construction.
        return new NdJsonPageIterator(
            object,
            context.projectedColumns(),
            context.batchSize(),
            context.rowLimit(),
            blockFactory,
            skipFirstLine,
            trimLastPartialLine,
            effectiveSchema,
            errorPolicy,
            cacheable ? object : null,
            pinnedMtimeMillis,
            // The fingerprint is the node-stable canonical config; the iterator's schema arg is ignored.
            cacheable ? ignoredSchema -> computeConfigFingerprint() : null,
            chunkMode,
            counters,
            context.splitStartByte(),
            context.maxRecordBytes(),
            datetimeFormatter,
            declaredDateFormats,
            declaredTypeColumns,
            context.statsBaseOffset(),
            context.statsStripeSize(),
            context.statsFileFinal(),
            context.statsColumnScope()
        );
    }

    /**
     * Returns an immutable typed snapshot of the NDJSON reader's counters for the operator-status
     * envelope. Zeroed counters when no decoders have run.
     */
    @Override
    public NdJsonReaderStatus statusSnapshot() {
        return counters.snapshot();
    }

    @Override
    public RecordSplitter recordSplitter() {
        return defaultRecordSplitter();
    }

    @Override
    public RecordSplitter recordSplitter(int maxRecordBytes) {
        return new NdJsonRecordSplitter(maxRecordBytes);
    }

    private static NdJsonRecordSplitter defaultRecordSplitter() {
        return DEFAULT_RECORD_SPLITTER;
    }

    /**
     * Resolved per-reader from {@link #SEGMENT_SIZE_SETTING} (node-level) or the {@code segment_size}
     * key in the per-query {@code WITH {...}} config. Defaults to {@link #DEFAULT_SEGMENT_SIZE}.
     */
    @Override
    public long minimumSegmentSize() {
        return segmentSizeBytes;
    }

    @Override
    public String formatName() {
        return "ndjson";
    }

    @Override
    public List<String> fileExtensions() {
        return List.of(".ndjson", ".jsonl", ".json");
    }

    @Override
    public org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport aggregatePushdownSupport() {
        return new org.elasticsearch.xpack.esql.datasources.TextAggregatePushdownSupport();
    }

    @Override
    public RowPositionStrategy rowPositionStrategy() {
        // NdJsonPageDecoder fills the {@code _rowPosition} slot natively from the file-global
        // byte offset of each record (see {@code NdJsonPageDecoder.recordFileOffset}).
        return PassThroughRowPositionStrategy.INSTANCE;
    }

    @Override
    public void close() {
        // Nothing to close at reader level
    }
}
