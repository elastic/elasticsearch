/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * FormatReader implementation for NDJSON files.
 * Implements {@link SegmentableFormatReader} for intra-file parallel parsing.
 */
public class NdJsonFormatReader implements SegmentableFormatReader {

    public static final String SCHEMA_SAMPLE_SIZE_SETTING = "esql.datasource.ndjson.schema_sample_size";
    public static final int DEFAULT_SCHEMA_SAMPLE_SIZE = 20_000;

    /**
     * Node-level setting for the parallel-parsing segment size. Larger segments amortise the fixed
     * Java/Jackson per-segment setup cost; smaller segments enable parallelism on smaller files.
     * Also overridable per-query via the {@code segment_size} key in {@code WITH (...)}.
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

    /** Buffer size used to accelerate {@link #scanForTerminator} on cold (unbuffered) streams. */
    private static final int SCAN_BUFFER_SIZE = 8 * 1024;

    static final String CONFIG_SCHEMA_SAMPLE_SIZE = "schema_sample_size";
    static final String CONFIG_SEGMENT_SIZE = "segment_size";

    /** Keys recognised by {@link #withConfigTrackingConsumedKeys(Map)}. */
    static final Set<String> RECOGNIZED_KEYS = Set.of(CONFIG_SCHEMA_SAMPLE_SIZE, CONFIG_SEGMENT_SIZE);

    private final BlockFactory blockFactory;
    private final Settings settings;
    private final List<Attribute> resolvedSchema;
    private final int schemaSampleSize;
    private final long segmentSizeBytes;

    public NdJsonFormatReader(Settings settings, BlockFactory blockFactory, List<Attribute> resolvedSchema) {
        this(settings, blockFactory, resolvedSchema, schemaSampleSize(settings), segmentSize(settings));
    }

    NdJsonFormatReader(Settings settings, BlockFactory blockFactory) {
        this(settings, blockFactory, null);
    }

    private NdJsonFormatReader(
        Settings settings,
        BlockFactory blockFactory,
        List<Attribute> resolvedSchema,
        int schemaSampleSize,
        long segmentSizeBytes
    ) {
        this.blockFactory = blockFactory;
        this.settings = settings == null ? Settings.EMPTY : settings;
        this.resolvedSchema = resolvedSchema;
        this.schemaSampleSize = schemaSampleSize;
        this.segmentSizeBytes = segmentSizeBytes;
    }

    @Override
    public NdJsonFormatReader withSchema(List<Attribute> schema) {
        return new NdJsonFormatReader(settings, blockFactory, schema, schemaSampleSize, segmentSizeBytes);
    }

    @Override
    public Configured<FormatReader> withConfigTrackingConsumedKeys(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return Configured.empty(this);
        }
        int newSampleSize = parseInt(config.get(CONFIG_SCHEMA_SAMPLE_SIZE), schemaSampleSize);
        Check.isTrue(newSampleSize > 0, CONFIG_SCHEMA_SAMPLE_SIZE + " must be positive, got: {}", newSampleSize);
        long newSegmentSize = parseSegmentSize(config.get(CONFIG_SEGMENT_SIZE), segmentSizeBytes);
        FormatReader result = (newSampleSize == schemaSampleSize && newSegmentSize == segmentSizeBytes)
            ? this
            : new NdJsonFormatReader(settings, blockFactory, resolvedSchema, newSampleSize, newSegmentSize);
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
                    inferred = NdJsonSchemaInferrer.inferSchema(stream, schemaSampleSize);
                }
                return mergeInferredWithPreferred(inferred, attributes);
            }
            return attributes;
        }

        try (var stream = openForSchemaInference(object, skipFirstLine)) {
            return NdJsonSchemaInferrer.inferSchema(stream, schemaSampleSize);
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
        if (skipFirstLine == false) {
            return stream;
        }
        try {
            LineScan scan = scanForTerminator(stream);
            if (scan.peekedByte() != -1) {
                stream.unread(scan.peekedByte());
            }
            return stream;
        } catch (IOException e) {
            try {
                stream.close();
            } catch (IOException ignored) {}
            throw e;
        }
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

    @Override
    public SourceMetadata metadata(StorageObject object) throws IOException {
        List<Attribute> schema;
        try (var stream = object.newStream()) {
            schema = NdJsonSchemaInferrer.inferSchema(stream, schemaSampleSize);
        }
        return new SimpleSourceMetadata(schema, formatName(), object.path().toString());
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
        return read(
            object,
            FormatReadContext.builder().projectedColumns(projectedColumns).batchSize(batchSize).errorPolicy(defaultErrorPolicy()).build()
        );
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
        // Mirror {@link org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader}: parallel byte-range
        // splits from {@link org.elasticsearch.xpack.esql.datasources.ParallelParsingCoordinator} set
        // {@code recordAligned=true}, signalling segment boundaries already fall on {@code \n}. Do not drop the
        // first complete row or trim a trailing partial row in that mode — doing so mis-handles aligned segments
        // (skipped rows, or truncated JSON when trim interacts with bounded range streams).
        boolean skipFirstLine = context.firstSplit() == false && context.recordAligned() == false;
        boolean trimLastPartialLine = context.lastSplit() == false && context.recordAligned() == false;
        ErrorPolicy errorPolicy = context.errorPolicy() != null ? context.errorPolicy() : defaultErrorPolicy();
        return new NdJsonPageIterator(
            object,
            context.projectedColumns(),
            context.batchSize(),
            context.rowLimit(),
            blockFactory,
            skipFirstLine,
            trimLastPartialLine,
            inferSchemaIfNeeded(resolvedSchema, object, skipFirstLine),
            errorPolicy
        );
    }

    @Override
    public long findNextRecordBoundary(InputStream stream) throws IOException {
        // The caller only cares about the byte offset of the terminator; a lone CR followed by
        // some non-LF byte may have been consumed from the stream by the scanner, but the
        // caller discards the stream after this call so that is acceptable.
        // Wrap cold streams to restore the 8 KB fast path; if already buffered, pass through.
        InputStream buffered = stream instanceof BufferedInputStream ? stream : new BufferedInputStream(stream, SCAN_BUFFER_SIZE);
        return scanForTerminator(buffered).consumed();
    }

    /**
     * NDJSON records never contain embedded newlines, so a backward scan for a line terminator
     * is always correct and O(1) from the end of the buffer — no per-record allocations needed.
     * Matches the LF / CRLF / lone-CR contract of {@link #scanForTerminator}.
     */
    @Override
    public int findLastRecordBoundary(byte[] buf, int length) {
        for (int i = length - 1; i >= 0; i--) {
            byte b = buf[i];
            if (b == '\n' || b == '\r') {
                return i;
            }
        }
        return -1;
    }

    /** Outcome of a single scan for the next record terminator. */
    record LineScan(long consumed, int peekedByte) {
        /** Sentinel returned when the stream ended before any terminator. */
        static final LineScan EOF = new LineScan(-1, -1);
    }

    /**
     * Reads one byte at a time from {@code in} until a record terminator (LF, CRLF, or lone CR)
     * is consumed. Returns {@link LineScan#consumed} as the number of bytes read through-and-
     * including the terminator; for the lone-CR case the byte that follows (which is the first
     * byte of the next record) is read from the stream and exposed via {@link LineScan#peekedByte}
     * so callers can preserve it. Returns {@link LineScan#EOF} if the stream ends before any
     * terminator is seen.
     *
     * <p>Single source of truth for the three NDJSON line-terminator consumers:
     * {@link NdJsonPageIterator#skipToNextLine}, {@link #findNextRecordBoundary}, and
     * {@link #openForSchemaInference}.
     */
    static LineScan scanForTerminator(InputStream in) throws IOException {
        long consumed = 0;
        int b;
        while ((b = in.read()) != -1) {
            consumed++;
            if (b == '\n') {
                return new LineScan(consumed, -1);
            }
            if (b == '\r') {
                int next = in.read();
                if (next == '\n') {
                    return new LineScan(consumed + 1, -1);
                }
                // EOF after CR is reported as a clean terminator with no peeked byte.
                return new LineScan(consumed, next);
            }
        }
        return LineScan.EOF;
    }

    /**
     * Resolved per-reader from {@link #SEGMENT_SIZE_SETTING} (node-level) or the {@code segment_size}
     * key in the per-query {@code WITH (...)} config. Defaults to {@link #DEFAULT_SEGMENT_SIZE}.
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
    public void close() {
        // Nothing to close at reader level
    }
}
