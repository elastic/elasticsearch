/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.exc.InputCoercionException;
import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import com.fasterxml.jackson.core.io.JsonEOFException;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.data.AbstractBlockBuilder;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.SyntheticColumns;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Parses NDJSON into {@link Page}s for a single input stream.
 * <p>
 * <strong>Not thread-safe:</strong> each instance is intended for use by a single consumer (one
 * {@link NdJsonPageIterator}); do not call {@link #decodePage()} concurrently from multiple threads.
 */
public class NdJsonPageDecoder implements Closeable {

    private static final Logger logger = LogManager.getLogger(NdJsonPageDecoder.class);

    /**
     * Floor for the per-{@code BlockDecoder} identity-cache bound (see
     * {@code BlockDecoder#identityCacheMaxEntries}). High enough that the common NDJSON STATS
     * shape (a handful of projected columns plus tens of unprojected ones) fits entirely; low
     * enough that the worst-case retention on a dynamic-key input stays in the kilobytes range
     * per decoder level.
     */
    static final int IDENTITY_CACHE_MIN_CAP = 256;

    /**
     * Multiplier on the local projected {@code children.size()} when sizing the identity-cache
     * bound. The fixed floor gives narrow projections room for common unprojected field names;
     * this multiplier gives wider projections extra space without scaling with dynamic JSON keys.
     */
    static final int IDENTITY_CACHE_FANOUT_MULT = 4;

    private InputStream input;
    /**
     * Non-null when the decoder is reading from a fully buffered byte array (no {@link InputStream}
     * indirection); in that case {@link #input} is {@code null} and recovery uses the byte-array
     * fast path. Streaming-parallel parsing uses this path because every chunk is already a
     * bounded {@code byte[]} region — going straight to Jackson's {@code createParser(byte[])}
     * skips the per-call dispatch through {@link InputStream#read} and lets Jackson 2.16+ pick
     * its small-input fast paths.
     */
    private final byte[] sourceBytes;
    /** Factory used to create (and recreate after recovery) the underlying {@link JsonParser}. */
    private final JsonFactory jsonFactory;
    /**
     * Exclusive end of the readable region in {@link #sourceBytes}. The decoder may read bytes in
     * the half-open range {@code [parserSliceStart, sourceEnd)}; everything outside it must not
     * be touched (e.g. when the byte array is a large pooled buffer and only a prefix is data).
     */
    private final int sourceEnd;
    /**
     * Total readable bytes for the byte-array path ({@code sourceEnd - sourceOffset}), or {@code -1}
     * on the {@link InputStream} path. Used by {@link #setMaxRecordBytes(int)} to decide whether the
     * per-record cap can ever trip: a record can never be longer than the buffer that fully contains
     * it, so a byte-array whose whole length is {@code <= external_max_record_size} needs no enforcement at all.
     */
    private final int sourceDataLength;
    /**
     * Absolute offset (within {@link #sourceBytes}) where {@link #parser}'s input slice starts;
     * tracked because {@code JsonParser.getCurrentLocation().getByteOffset()} is relative to the
     * slice the parser was created over, not to the underlying byte array. Updated each time
     * {@link #recoverFromParseException} restarts the parser at a later offset.
     */
    private int parserSliceStart;

    /**
     * Record-offset tracking for the orthogonal per-stripe stats path. Enabled by
     * {@link #enableRecordOffsetTracking(long)}: {@link #decodePage()} then records every decoded record's
     * own file-global start offset (the byte of its opening brace, scan-invariant) into
     * {@link #lastPageRecordOffsets} so the iterator can attribute each row to its canonical stripe
     * ({@code floor(offset / B)}) — exactly as the CSV reader uses its per-row {@code rowStartBytes}. The
     * page is NOT capped at stripe lines: byte-range cover attribution by record offset needs no page
     * alignment. {@code baseOffset} is this read's first byte in file/decompressed coordinates; the absolute
     * offset of the START of the parser's current token (a record's opening brace) is {@code baseOffset +
     * parserSliceStart + parser.getTokenLocation().getByteOffset()} — see {@link #tokenStartOffset()}, which
     * uses the token-start location (not the current/end location) so attribution is scan-invariant. Disabled
     * by default — a pure stats overlay, never affecting page contents.
     */
    private long statsBaseOffset = 0L;
    private boolean recordOffsetTracking = false;
    /**
     * Per-record file-global start offsets of the page {@link #decodePage()} last returned, filled positionally
     * with the page's rows when {@link #recordOffsetTracking} is on. Reused across pages; only the first
     * {@link #lastPageRecordCount} entries are meaningful.
     */
    private long[] lastPageRecordOffsets = new long[0];
    /** Number of meaningful entries in {@link #lastPageRecordOffsets} for the last page. */
    private int lastPageRecordCount;

    private final BlockDecoder decoder;
    private final int batchSize;
    private final BlockFactory blockFactory;
    private JsonParser parser;
    private final List<Attribute> projectedAttributes;
    /**
     * Index of the synthetic {@code _rowPosition} attribute in {@link #projectedAttributes}, or
     * {@code -1} when not projected. When non-negative, each decoded record's file-global start
     * byte is emitted into this slot (see {@link #recordFileOffset(long)}).
     */
    private final int rowPositionSlot;
    /**
     * Logical start offset of the parser's initial slice (the {@code sourceOffset} the first parser
     * was created over). Used to relativize {@link #parserSliceStart}, which is updated to absolute
     * positions within {@link #sourceBytes} on recovery.
     */
    private final int initialSliceStart;
    /**
     * File-global byte offset of the first byte this decoder reads (i.e. the split's start byte plus
     * any leading partial record skipped before the decoder was handed the stream). Base for the
     * {@code _rowPosition} / {@code _file.record_ref} emit; {@code 0} when not relevant. Set by the
     * caller via {@link #setRecordOffsetBase(long)} before the first {@link #decodePage()}.
     */
    private long recordOffsetBase = 0L;

    /**
     * Per-record {@code external_max_record_size} byte cap. Enforced inside the decode loop on the same pass
     * Jackson already makes (no separate full sweep), so it replaces the pre-#965 stream-wrapper /
     * pre-scan. Defaults to {@link Integer#MAX_VALUE} (no cap) until {@link #setMaxRecordBytes(int)}
     * is called by the iterator.
     */
    private int maxRecordBytes = Integer.MAX_VALUE;
    /**
     * True only when an oversized record is actually reachable on this input, so the hot path (a
     * byte-array segment whose whole length is within the cap — the streaming-parallel chunk case)
     * pays nothing. See {@link #setMaxRecordBytes(int)}.
     */
    private boolean capEnforced = false;
    /**
     * Set when a non-strict policy stops the read at an oversized record on the {@link InputStream}
     * (streaming / fallback) path. Unlike the byte-array path — where a fully-buffered oversized
     * record can be dropped and decoding continues — a streaming oversized record has no cheap
     * resumption point, so the read truncates at the failure (matching the segmentator's behavior).
     * The records emitted before it are a partial prefix; {@link NdJsonPageIterator} surfaces a
     * client warning and keeps the under-count out of the stats cache.
     */
    private boolean truncated = false;
    /** File-global byte offset where the oversized record that triggered {@link #truncated} began. */
    private long truncatedAtByte = -1L;
    /**
     * Set when the BYTE-ARRAY path drops an oversized record and keeps decoding. Unlike {@link #truncated}
     * (streaming, which stops at the record), the byte-array path recovers, so the emitted rows are complete
     * EXCEPT the dropped one — a {@code external_max_record_size}-dependent under-count. Since {@code external_max_record_size}
     * is a query pragma and not in the cache fingerprint ({@code SchemaCacheKey.FORMAT_AFFECTING_PARAMS}), a
     * warm aggregate under a different cap would count differently, so {@link NdJsonPageIterator} must keep
     * this scan out of the stats cache (safe-miss). Mirrors CSV's {@code recordCapDropped} guard.
     */
    private boolean capDropped = false;
    /**
     * Set when a lenient-mode parse-error recovery on the STREAMING ({@link InputStream}) path rebuilt the parser
     * over the remaining stream ({@link #recoverFromParseException}'s {@code sourceBytes == null} branch): the new
     * parser's byte offsets restart at the recovery point while {@link #parserSliceStart} stays 0, so every
     * subsequent {@link #tokenStartOffset()} is short by the bytes consumed before recovery — record offsets are no
     * longer file-global. Per-stripe attribution derived from them would commit records to EARLIER stripes, and
     * NDJSON has no emit-time byte-exactness tripwire (unlike CSV), so {@link NdJsonPageIterator} must safe-miss
     * stripe capture. The byte-array recovery path re-anchors {@link #parserSliceStart} exactly and is immune.
     */
    private boolean offsetBaselineLost = false;

    /** Page block layout: index {@code i} corresponds to {@code projectedAttributes().get(i)}. */
    List<Attribute> projectedAttributes() {
        return projectedAttributes;
    }

    // What blocks got a value on the current line? Needed because Block.Builder doesn't provide
    // the number of positions that were added.
    private final BitSet blockTracker;
    /**
     * Tracks which projected columns were present in at least one committed record. Used at {@link #close()}
     * to emit absent-column warnings for columns never seen in any committed record (effectively absent from the
     * file). We do NOT warn for columns absent from individual records but present in others: that is normal sparse
     * NdJson data and not an error condition.
     * <p>
     * The bit is set whenever a field was decoded into a block builder for a committed record — including records
     * where the field's value was explicit JSON {@code null}. The semantics are "field present in at least one
     * committed record", not "field ever non-null".
     */
    private BitSet columnEverPresent;
    /**
     * Number of records successfully committed to a page across all batches. Guards the absent-column check in
     * {@link #close()} against false positives when all records were dropped by {@code skip_row}.
     */
    private long committedRowCount;
    /** Informational warning sink for absent declared columns; {@code null} when no sink is wired. */
    @Nullable
    private Consumer<String> absentColumnWarningSink;
    private final ErrorPolicy errorPolicy;
    private final SkipWarnings skipWarnings;
    private final NdJsonReaderCounters counters;
    private long totalRowCount;
    private long errorCount;
    private final DateFormatter datetimeFormatter;
    /**
     * Per-column declared date parse-patterns keyed by <b>physical</b> (file) column name; empty when none. Each
     * {@link BlockDecoder} resolves its own {@link DateFormatter} once from this map in {@link BlockDecoder#setAttribute}
     * and parses that column's timestamps with it instead of {@link #datetimeFormatter}.
     */
    private final Map<String, String> declaredDateFormats;

    /**
     * Set by {@link BlockDecoder#coercionFailure} while decoding the current record when the policy is
     * {@link ErrorPolicy.Mode#SKIP_ROW}: the record carries an uncoercible value and must be dropped whole
     * rather than committed with a null cell. Reset per record by {@link #decodePageLenient}, which is the
     * only decode loop that can drop a record ({@code FAIL_FAST} throws instead).
     */
    private boolean rowDroppedBySkipRow;

    /** Whether the current record has already been charged to the error budget; see {@link #chargeErrorBudget}. */
    private boolean recordChargedToBudget;

    /** Number of malformed records observed during decoding (lenient policies swallow these). */
    long errorCount() {
        return errorCount;
    }

    /**
     * Enables per-record offset tracking so {@link #decodePage()} fills {@link #lastPageRecordOffsets} with
     * each row's own file-global start byte. Does NOT cap pages at stripe lines — the iterator attributes
     * rows to stripes by their recorded offsets via the byte-range cover model.
     */
    void enableRecordOffsetTracking(long baseOffset) {
        this.statsBaseOffset = baseOffset;
        this.recordOffsetTracking = true;
    }

    /**
     * Absolute file offset of the START of the most recently read token — for a record's {@code START_OBJECT}
     * this is the byte of its opening brace. A record's own start is independent of how the file is chunked,
     * so {@code floor(thisOffset / B)} attributes the record to the same stripe under every scan.
     */
    private long tokenStartOffset() {
        return statsBaseOffset + parserSliceStart + parser.getTokenLocation().getByteOffset();
    }

    /** Per-record file-global start offsets of the last decoded page; valid for the first {@link #lastPageRecordCount()} rows. */
    long[] lastPageRecordOffsets() {
        return lastPageRecordOffsets;
    }

    /** Number of meaningful entries in {@link #lastPageRecordOffsets()} (== the last page's row count when tracking is on). */
    int lastPageRecordCount() {
        return lastPageRecordCount;
    }

    /**
     * Lazily allocated for {@link #decodePageLenient} only; reused across rows within this decoder
     * (avoids per-row {@code new Block.Builder[n]}).
     */
    @Nullable
    private Block.Builder[] lenientScratchBuilders;

    /**
     * Reused buffer for {@link #appendDecodedScratchRow}; paired with {@link #lenientScratchBuilders}.
     */
    @Nullable
    private Block[] lenientScratchRowBlocks;

    /**
     * Reused for every keyword field; see {@link #toScratchBytesRef(String)}.
     */
    private final BytesRef keywordScratch = new BytesRef(BytesRef.EMPTY_BYTES);

    /** No-declared-date-formats, no-sink convenience (tests and callers that need neither feature). */
    NdJsonPageDecoder(
        InputStream input,
        DateFormatter datetimeFormatter,
        List<Attribute> attributes,
        List<String> projectedColumns,
        int batchSize,
        BlockFactory blockFactory,
        ErrorPolicy errorPolicy,
        String sourceLocation,
        NdJsonReaderCounters counters
    ) throws IOException {
        this(
            input,
            datetimeFormatter,
            attributes,
            projectedColumns,
            batchSize,
            blockFactory,
            errorPolicy,
            sourceLocation,
            counters,
            Map.of(),
            null
        );
    }

    /** Test-only: back-compat overload for callers that don't need sink-routed warnings. */
    NdJsonPageDecoder(
        InputStream input,
        DateFormatter datetimeFormatter,
        List<Attribute> attributes,
        List<String> projectedColumns,
        int batchSize,
        BlockFactory blockFactory,
        ErrorPolicy errorPolicy,
        String sourceLocation,
        NdJsonReaderCounters counters,
        Map<String, String> declaredDateFormats
    ) throws IOException {
        this(
            input,
            datetimeFormatter,
            attributes,
            projectedColumns,
            batchSize,
            blockFactory,
            errorPolicy,
            sourceLocation,
            counters,
            declaredDateFormats,
            null
        );
    }

    /** Test-only: back-compat overload for callers that don't need declared-date/type info. */
    NdJsonPageDecoder(
        InputStream input,
        DateFormatter datetimeFormatter,
        List<Attribute> attributes,
        List<String> projectedColumns,
        int batchSize,
        BlockFactory blockFactory,
        ErrorPolicy errorPolicy,
        String sourceLocation,
        NdJsonReaderCounters counters,
        @Nullable Consumer<String> warningSink
    ) throws IOException {
        this(
            input,
            datetimeFormatter,
            attributes,
            projectedColumns,
            batchSize,
            blockFactory,
            errorPolicy,
            sourceLocation,
            counters,
            Map.of(),
            warningSink
        );
    }

    NdJsonPageDecoder(
        InputStream input,
        DateFormatter datetimeFormatter,
        List<Attribute> attributes,
        List<String> projectedColumns,
        int batchSize,
        BlockFactory blockFactory,
        ErrorPolicy errorPolicy,
        String sourceLocation,
        NdJsonReaderCounters counters,
        Map<String, String> declaredDateFormats,
        @Nullable Consumer<String> warningSink
    ) throws IOException {
        this(
            input,
            null,
            0,
            0,
            attributes,
            projectedColumns,
            batchSize,
            blockFactory,
            errorPolicy,
            sourceLocation,
            datetimeFormatter,
            counters,
            NdJsonUtils.JSON_FACTORY,
            declaredDateFormats,
            warningSink
        );
    }

    /** No-declared-date-formats, no-sink convenience for the byte[] path (see the fully-loaded ctor below). */
    NdJsonPageDecoder(
        byte[] data,
        int offset,
        int length,
        DateFormatter datetimeFormatter,
        List<Attribute> attributes,
        List<String> projectedColumns,
        int batchSize,
        BlockFactory blockFactory,
        ErrorPolicy errorPolicy,
        String sourceLocation,
        NdJsonReaderCounters counters
    ) throws IOException {
        this(
            data,
            offset,
            length,
            datetimeFormatter,
            attributes,
            projectedColumns,
            batchSize,
            blockFactory,
            errorPolicy,
            sourceLocation,
            counters,
            Map.of(),
            null
        );
    }

    /**
     * Buffered-bytes constructor for the streaming-parallel path: {@code data[offset .. offset+length)}
     * is the entire input. Recovery from a whole-line parse failure stays inside the byte array
     * (no buffered-bytes shuttling through {@link NdJsonUtils#moveToNextLine}) by scanning for the
     * next {@code '\n'} from the parser's current byte offset.
     */
    /** Test-only: back-compat overload for callers that don't need sink-routed warnings. */
    NdJsonPageDecoder(
        byte[] data,
        int offset,
        int length,
        DateFormatter datetimeFormatter,
        List<Attribute> attributes,
        List<String> projectedColumns,
        int batchSize,
        BlockFactory blockFactory,
        ErrorPolicy errorPolicy,
        String sourceLocation,
        NdJsonReaderCounters counters,
        Map<String, String> declaredDateFormats
    ) throws IOException {
        this(
            data,
            offset,
            length,
            datetimeFormatter,
            attributes,
            projectedColumns,
            batchSize,
            blockFactory,
            errorPolicy,
            sourceLocation,
            counters,
            declaredDateFormats,
            null
        );
    }

    /** Test-only: back-compat overload for callers that don't need declared-date/type info. */
    NdJsonPageDecoder(
        byte[] data,
        int offset,
        int length,
        DateFormatter datetimeFormatter,
        List<Attribute> attributes,
        List<String> projectedColumns,
        int batchSize,
        BlockFactory blockFactory,
        ErrorPolicy errorPolicy,
        String sourceLocation,
        NdJsonReaderCounters counters,
        @Nullable Consumer<String> warningSink
    ) throws IOException {
        this(
            data,
            offset,
            length,
            datetimeFormatter,
            attributes,
            projectedColumns,
            batchSize,
            blockFactory,
            errorPolicy,
            sourceLocation,
            counters,
            Map.of(),
            warningSink
        );
    }

    NdJsonPageDecoder(
        byte[] data,
        int offset,
        int length,
        DateFormatter datetimeFormatter,
        List<Attribute> attributes,
        List<String> projectedColumns,
        int batchSize,
        BlockFactory blockFactory,
        ErrorPolicy errorPolicy,
        String sourceLocation,
        NdJsonReaderCounters counters,
        Map<String, String> declaredDateFormats,
        @Nullable Consumer<String> warningSink
    ) throws IOException {
        this(
            null,
            data,
            offset,
            length,
            attributes,
            projectedColumns,
            batchSize,
            blockFactory,
            errorPolicy,
            sourceLocation,
            datetimeFormatter,
            counters,
            NdJsonUtils.JSON_FACTORY,
            declaredDateFormats,
            warningSink
        );
    }

    /**
     * Test-only: accepts an injected {@link JsonFactory} so tests can wrap the created parser in a
     * delegate (e.g. to count token-advance calls) without reflection. Uses a fresh counters
     * instance since these tests don't assert on the counter snapshot.
     */
    NdJsonPageDecoder(
        InputStream input,
        List<Attribute> attributes,
        List<String> projectedColumns,
        int batchSize,
        BlockFactory blockFactory,
        ErrorPolicy errorPolicy,
        String sourceLocation,
        JsonFactory factory
    ) throws IOException {
        this(input, attributes, projectedColumns, batchSize, blockFactory, errorPolicy, sourceLocation, factory, null);
    }

    /** Test-only: like the above, but also allows asserting on the {@link SkipWarnings} sink. */
    NdJsonPageDecoder(
        InputStream input,
        List<Attribute> attributes,
        List<String> projectedColumns,
        int batchSize,
        BlockFactory blockFactory,
        ErrorPolicy errorPolicy,
        String sourceLocation,
        JsonFactory factory,
        @Nullable Consumer<String> warningSink
    ) throws IOException {
        this(
            input,
            null,
            0,
            0,
            attributes,
            projectedColumns,
            batchSize,
            blockFactory,
            errorPolicy,
            sourceLocation,
            null,
            new NdJsonReaderCounters(),
            factory,
            Map.of(),
            warningSink
        );
    }

    private NdJsonPageDecoder(
        InputStream input,
        byte[] sourceBytes,
        int sourceOffset,
        int sourceLength,
        List<Attribute> attributes,
        List<String> projectedColumns,
        int batchSize,
        BlockFactory blockFactory,
        ErrorPolicy errorPolicy,
        String sourceLocation,
        DateFormatter datetimeFormatter,
        NdJsonReaderCounters counters,
        JsonFactory factory,
        Map<String, String> declaredDateFormats,
        @Nullable Consumer<String> warningSink
    ) throws IOException {
        this.jsonFactory = factory;
        this.input = input;
        this.sourceBytes = sourceBytes;
        if (sourceBytes != null) {
            Check.isTrue(sourceOffset >= 0, "sourceOffset must be non-negative, got: {}", sourceOffset);
            Check.isTrue(sourceLength >= 0, "sourceLength must be non-negative, got: {}", sourceLength);
            int end = Math.addExact(sourceOffset, sourceLength);
            Check.isTrue(end <= sourceBytes.length, "byte slice [{}, {}) exceeds buffer length {}", sourceOffset, end, sourceBytes.length);
            this.sourceEnd = end;
            this.parserSliceStart = sourceOffset;
            this.sourceDataLength = sourceLength;
        } else {
            // The default-zero values are unreachable on the InputStream path: every read of these
            // fields is gated on {@code sourceBytes != null}. Assign explicitly so the dependency is
            // self-documenting and a future refactor that lifts the gate fails at the source rather
            // than reading silently from a zero-initialized field.
            this.sourceEnd = 0;
            this.parserSliceStart = 0;
            this.sourceDataLength = -1;
        }
        Check.isTrue(errorPolicy != null, "errorPolicy must not be null");
        Check.isTrue(counters != null, "counters must not be null");
        this.errorPolicy = errorPolicy;
        this.counters = counters;
        this.datetimeFormatter = datetimeFormatter != null ? datetimeFormatter : NdJsonSchemaInferrer.STRICT_DATE_OPTIONAL_TIME;
        this.declaredDateFormats = declaredDateFormats != null ? Map.copyOf(declaredDateFormats) : Map.of();
        this.skipWarnings = SkipWarnings.of(
            errorPolicy,
            "NDJSON read from ["
                + sourceLocation
                + "] encountered parse errors handled per policy (policy: "
                + errorPolicy.modeName()
                + "); affected rows are listed below",
            warningSink
        );

        // Three projection cases:
        // - null : caller has no projection info (e.g. metadata path); materialize every attribute.
        // - empty : optimizer pruned every column (COUNT(*) and similar); produce row-count-only Pages.
        // - other : project the listed columns in the requested order, with NULL for missing names.
        List<Attribute> projectedAttributes;
        if (projectedColumns == null) {
            projectedAttributes = attributes;
        } else if (projectedColumns.isEmpty()) {
            projectedAttributes = List.of();
        } else {
            // Build the lookup map once: O(N) here vs. O(N*M) for a per-projection nested scan,
            // which matters on wide schemas (the NYC taxis fixture has 100+ columns). putIfAbsent
            // preserves the first-wins semantics of the prior findFirst() call so a schema with
            // duplicated names (rare, but legal) keeps the same attribute the optimizer would have
            // picked. We never iterate the map, so HashMap suffices (no LinkedHashMap overhead).
            // Capacity 2*N keeps us safely above the 0.75 load factor for at most N entries.
            Map<String, Attribute> byName = new HashMap<>(attributes.size() * 2);
            for (Attribute a : attributes) {
                byName.putIfAbsent(a.name(), a);
            }
            var resolved = new ArrayList<Attribute>(projectedColumns.size());
            for (String col : projectedColumns) {
                if (ColumnExtractor.ROW_POSITION_COLUMN.equals(col)) {
                    // Synthetic file-global record offset, not a JSON field. Typed LONG (not NULL) so
                    // setupBuilders allocates a Long builder; the decode loop fills it from the
                    // parser's byte offset rather than from JSON.
                    resolved.add(NdJsonSchemaInferrer.attribute(col, DataType.LONG, false));
                    continue;
                }
                Attribute match = byName.get(col);
                resolved.add(match != null ? match : NdJsonSchemaInferrer.attribute(col, DataType.NULL, false));
            }
            projectedAttributes = resolved;
        }

        this.decoder = prepareSchema(projectedAttributes);
        this.batchSize = batchSize;
        this.blockFactory = blockFactory;
        this.projectedAttributes = projectedAttributes;
        this.blockTracker = new BitSet(projectedAttributes.size());
        if (warningSink != null) {
            this.absentColumnWarningSink = warningSink;
            this.columnEverPresent = new BitSet(projectedAttributes.size());
        }
        this.initialSliceStart = sourceOffset;
        this.rowPositionSlot = SyntheticColumns.rowPositionIndexInAttributes(projectedAttributes);

        if (sourceBytes != null) {
            this.parser = factory.createParser(sourceBytes, sourceOffset, sourceLength);
        } else {
            this.parser = factory.createParser(input);
        }
    }

    private void recoverFromParseException(JsonParser failedParser) throws IOException {
        if (sourceBytes != null) {
            int next = nextLineStartByteAfter(failedParser);
            failedParser.close();
            this.parserSliceStart = next;
            this.parser = jsonFactory.createParser(sourceBytes, next, sourceEnd - next);
        } else {
            this.input = NdJsonUtils.moveToNextLine(failedParser, this.input);
            this.parser = jsonFactory.createParser(this.input);
            // The fresh parser's byte offsets restart at the recovery point while parserSliceStart stays 0, so
            // every subsequent tokenStartOffset() is short by the pre-recovery bytes. Record offsets are no longer
            // file-global — any per-stripe attribution derived from them is skewed; NdJsonPageIterator safe-misses
            // stripe capture. (The byte-array branch above re-anchors parserSliceStart exactly, so it is immune.)
            this.offsetBaselineLost = true;
        }
    }

    /**
     * Returns the absolute byte offset (into {@link #sourceBytes}) of the start of the line
     * following the failed parser's current position (the byte after the next {@code '\n'} or
     * {@code '\r'}, or {@link #sourceEnd} on EOF). The new parser created from this offset will
     * not re-encounter the malformed line. {@code getByteOffset()} is added to
     * {@link #parserSliceStart} because it is relative to the slice the failed parser was created
     * over, not to {@link #sourceBytes}. Both LF and CR terminate a line so the byte-array path
     * handles the same record terminators as {@link NdJsonUtils#moveToNextLine}.
     */
    private int nextLineStartByteAfter(JsonParser failedParser) {
        long sliceOffsetLong = failedParser.getCurrentLocation().getByteOffset();
        // getByteOffset() returns -1 only for non-byte-backed sources; we always pass byte[].
        int sliceOffset = sliceOffsetLong < 0 ? (sourceEnd - parserSliceStart) : Math.toIntExact(sliceOffsetLong);
        int from = Math.min(parserSliceStart + sliceOffset, sourceEnd);
        for (int i = from; i < sourceEnd; i++) {
            byte b = sourceBytes[i];
            if (b == '\n') {
                return i + 1;
            }
            if (b == '\r') {
                // Consume an immediately-following LF so CRLF advances by two bytes (matches the
                // streaming-side scanForTerminator semantics).
                if (i + 1 < sourceEnd && sourceBytes[i + 1] == '\n') {
                    return i + 2;
                }
                return i + 1;
            }
        }
        return sourceEnd;
    }

    /**
     * Whole-line JSON failures always drop the line. {@link ErrorPolicy.Mode#NULL_FIELD} is treated
     * like {@link ErrorPolicy.Mode#SKIP_ROW} here; per-field null-fill would require partial decode support.
     * <p>
     * Two Jackson failures belong to this class, which is why the parameter is their common supertype rather
     * than {@link JsonParseException}: malformed JSON, and a {@link StreamConstraintsException} from a token
     * that trips one of {@code StreamReadConstraints}' limits.
     * <p>
     * Three of the four limits enabled by default -- number length, field-name length and nesting depth -- are
     * raised by the token scanner, before the decoder has dispatched on a type, so they cannot reach
     * {@link BlockDecoder#coercionFailure}, the per-cell sink, which needs a decoded value to attribute. Often
     * there is no cell to attribute at all: the name-length limit trips on a field that may not even be
     * projected, and the depth limit trips on structure rather than on a value.
     * <p>
     * String length is the exception and is worth knowing about before trusting "the scanner raised it" as an
     * invariant of this class. Jackson validates it lazily, on {@code getValueAsString()} and
     * {@code getTextCharacters()} -- the accessors the string-shaped decode arms call -- so the token has
     * already been returned and dispatched on, and for a projected column there IS a cell to attribute. It is
     * given the whole-line treatment anyway, deliberately: splitting one limit off into a per-cell null-fill
     * would make the outcome depend on which limit the record happened to trip.
     * <p>
     * Dropping the line is therefore the outcome for every member of the class, and it matches how
     * {@code CsvFormatReader} routes its own constraint violation (a field over {@code max_field_size}) through
     * {@code onRowError} rather than {@code onFieldError}.
     */
    private void onNdjsonLineParseError(JsonProcessingException e, long logicalRowIndex, String phaseLabel) {
        // Described once, for the strict message, the client warning and the log alike. The row index is the
        // one part a user can act on -- it names the line to go and look at -- and CsvFormatReader's own
        // "at row [N]" says so too, so the strict message carries it rather than the phase alone.
        String description = lineFailureKind(e)
            + " NDJSON at logical row ["
            + logicalRowIndex
            + "] ("
            + phaseLabel
            + "): "
            + e.getOriginalMessage();
        if (errorPolicy.isStrict()) {
            // The remedy hint mirrors coercionFailure and CsvFormatReader.onRowErrorImpl, phrased for a
            // whole-line failure: both non-strict modes drop the line here, so neither is "null-fill" the way
            // it is for a per-cell failure.
            // ParsingException (client-class, 400) rather than EsqlIllegalArgumentException (Ql SERVER family,
            // 500): a line this reader cannot interpret is bad input, not a broken invariant of ours, which is
            // the split ExternalFailures documents and CsvFormatReader.onRowErrorImpl already implements. The
            // single "{}" arg keeps LoggerMessageFormat away from the braces an NDJSON record is full of.
            throw new ParsingException(
                e,
                Source.EMPTY,
                "{}",
                description + "; set error_mode=skip_row (or null_field) to skip the line and warn instead of failing"
            );
        }
        if (recordChargedToBudget == false) {
            // Once per record across the two sinks: a per-cell coercion failure earlier in this same record may
            // already have charged it. (Per-cell charges among themselves are still per-cell under null_field --
            // see coercionFailure, whose own suppression is gated on skip_row.) Warn either way: under null_field
            // that earlier warning said the cell was nulled and the record kept, which this failure overrides by
            // dropping the record whole.
            chargeErrorBudget();
        }
        skipWarnings.add(description);
        checkErrorBudgetOrThrow();
        logger.log(errorPolicy.logErrors() ? Level.INFO : Level.DEBUG, description);
    }

    /**
     * Names the whole-line failure for the message, the client warning and the log, so all three agree.
     * The three arms are the whole membership of the class {@link #onNdjsonLineParseError} accepts; anything
     * else reaching it is a routing bug at one of its call sites, not an input the reader can describe.
     *
     * @see #onNdjsonLineParseError
     */
    private static String lineFailureKind(JsonProcessingException e) {
        return switch (e) {
            // Ordered before JsonParseException, which it extends.
            case JsonEOFException ignored -> "Truncated";
            // Well-formed JSON that exceeds one of StreamReadConstraints' limits -- number length, field-name
            // length or nesting depth -- so "malformed" would misdescribe it. Jackson's own message, appended
            // by the caller, names which limit.
            case StreamConstraintsException ignored -> "Over-limit";
            case JsonParseException ignored -> "Malformed";
            default -> throw new AssertionError("unexpected NDJSON whole-line failure [" + e.getClass().getName() + "]");
        };
    }

    /**
     * Counts one error against the current record. Every non-strict sink in this class charges here and nowhere
     * else, so the running total and the per-record "already paid" flag cannot drift apart as sinks are added.
     * <p>
     * {@code max_errors} and {@code max_error_ratio} are documented in records ("maximum malformed rows"), so a
     * record that fails twice -- a per-cell coercion failure, then a whole-line failure raised while the rest of
     * the record is drained -- must still cost one. This method does not enforce that itself; it records that the
     * record has paid, and two guards consume the flag: {@link BlockDecoder#coercionFailure} suppresses further
     * per-cell charges on a record already dropped by {@code skip_row} (via {@link #rowDroppedBySkipRow}), and
     * {@link #onNdjsonLineParseError} skips its charge when {@link #recordChargedToBudget} is set. A new sink
     * must decide which of the two it is.
     */
    private void chargeErrorBudget() {
        errorCount++;
        recordChargedToBudget = true;
    }

    /** Records that column {@code idx} was present in a committed record. */
    private void markColumnSeen(int idx) {
        if (columnEverPresent != null) {
            columnEverPresent.set(idx);
        }
    }

    /**
     * Throws when the non-strict error budget ({@code max_errors}/{@code max_error_ratio}) has been
     * exceeded, after first surfacing a client warning describing what tripped it. Shared by every
     * non-strict error path ({@link #onNdjsonLineParseError} and {@link BlockDecoder#coercionFailure})
     * so the budget is enforced consistently regardless of which kind of
     * error incremented {@link #errorCount}. Callers must have already settled the current error's charge via
     * {@link #chargeErrorBudget}, or deliberately suppressed it.
     */
    private void checkErrorBudgetOrThrow() {
        if (errorPolicy.isBudgetExceeded(errorCount, totalRowCount)) {
            // Surface the budget-exceeded condition as a warning so clients see exactly what tripped it.
            skipWarnings.add(
                "NDJSON error budget exceeded at row ["
                    + totalRowCount
                    + "]: ["
                    + errorCount
                    + "] errors, maximum ["
                    + errorPolicy.maxErrors()
                    + "] or ratio ["
                    + errorPolicy.maxErrorRatio()
                    + "]"
            );
            // Client-class for the same reason as the whole-line failure above: the budget was set by the user
            // and exhausted by the user's data.
            throw new ParsingException(
                "NDJSON error budget exceeded: [{}] errors in [{}] rows, maximum allowed is [{}] errors or [{}] ratio",
                errorCount,
                totalRowCount,
                errorPolicy.maxErrors(),
                errorPolicy.maxErrorRatio()
            );
        }
    }

    /**
     * Sets the file-global byte offset of this decoder's first input byte (split start + any leading
     * partial record skipped upstream). Must be called before the first {@link #decodePage()} when
     * {@code _rowPosition} is projected; harmless otherwise.
     */
    void setRecordOffsetBase(long recordOffsetBase) {
        this.recordOffsetBase = recordOffsetBase;
    }

    /**
     * Sets the per-record {@code external_max_record_size} cap (in bytes). Must be called before the first
     * {@link #decodePage()}. Enforcement is gated on {@link #capEnforced}: on the byte-array path a
     * record can never exceed the buffer that fully contains it, so when the whole segment is within
     * the cap the loop skips offset tracking entirely (the streaming-parallel chunk hot path pays
     * nothing — see issue 965). The {@link InputStream} path has no such bound, so it always enforces
     * when a finite cap is configured.
     */
    void setMaxRecordBytes(int maxRecordBytes) {
        Check.isTrue(maxRecordBytes > 0, "maxRecordBytes must be positive, got: {}", maxRecordBytes);
        this.maxRecordBytes = maxRecordBytes;
        this.capEnforced = maxRecordBytes != Integer.MAX_VALUE && (sourceDataLength < 0 || maxRecordBytes < sourceDataLength);
    }

    /**
     * Whether the per-record {@code external_max_record_size} check runs in the decode loop. False on the
     * byte-array hot path when the whole segment is within the cap (no record can exceed the buffer
     * that contains it) — the streaming-parallel chunk case that issue 965 must keep free of any
     * extra per-record work. Package-private for tests that pin that gate.
     */
    boolean capEnforced() {
        return capEnforced;
    }

    /**
     * True when a non-strict read stopped early at an oversized record on the streaming/fallback
     * path. The emitted rows are a partial prefix of the input.
     */
    boolean truncated() {
        return truncated;
    }

    /** File-global byte offset of the oversized record that caused {@link #truncated}, or {@code -1}. */
    long truncatedAtByte() {
        return truncatedAtByte;
    }

    /**
     * True when the byte-array path dropped an oversized record and kept decoding — a
     * {@code external_max_record_size}-dependent under-count that must not be cached. See {@link #capDropped}.
     */
    boolean capDropped() {
        return capDropped;
    }

    /** Whether a streaming-path recovery reset the parser byte baseline (record offsets no longer file-global). */
    boolean offsetBaselineLost() {
        return offsetBaselineLost;
    }

    /**
     * Parser byte offset relative to its current slice. Stable to subtract between two points within
     * a single record's decode (no recovery happens between {@code nextToken} and a successful
     * {@code decodeObject}), so {@code endOffset - startOffset} is the record's parsed JSON span.
     */
    private long parserSliceByteOffset() {
        return parser.getCurrentLocation().getByteOffset();
    }

    /**
     * Throws the strict-policy {@code external_max_record_size} failure for a record whose parsed span is
     * {@code spanBytes}. Shares {@link NdJsonRecordSplitter}'s {@code NDJSON line exceeded external_max_record_size [N]}
     * prefix so the user-facing wording is consistent regardless of which layer detects the overflow, and
     * appends the decode-time span for diagnostics.
     */
    private IOException recordTooLarge(long spanBytes) {
        return new IOException(
            "NDJSON line exceeded external_max_record_size [" + maxRecordBytes + "]: spans at least [" + spanBytes + "] bytes"
        );
    }

    /**
     * File-global byte offset of a record whose slice-relative start is {@code startSliceOffset} (captured via
     * {@link #parserSliceByteOffset()} before {@code decodeObject} advances the parser). {@link #parserSliceStart}
     * is the parser slice's absolute start within {@link #sourceBytes} (updated on recovery);
     * {@link #initialSliceStart} relativizes it so the result stays anchored to {@link #recordOffsetBase}.
     * Stable across split layouts because it is the record's intrinsic position in the file. Single source of
     * the offset formula shared by the strict and lenient decode loops.
     */
    private long recordFileOffset(long startSliceOffset) {
        return recordOffsetBase + (parserSliceStart - initialSliceStart) + startSliceOffset;
    }

    Page decodePage() throws IOException {
        if (truncated) {
            // A prior page stopped at an oversized record on the streaming path; nothing more to read.
            return null;
        }
        long startNanos = System.nanoTime();
        long startTotalRowCount = totalRowCount;
        long startErrorCount = errorCount;
        var blockBuilders = new Block.Builder[projectedAttributes.size()];
        // Per-record offset tracking: each decoded record's own start offset is recorded into
        // lastPageRecordOffsets so the iterator can attribute rows to canonical stripes by the byte-range
        // cover model. Pages are NOT capped at stripe lines — a page may span stripes; the iterator splits
        // its rows by their recorded offsets. Reset the per-page count before decoding.
        lastPageRecordCount = 0;
        if (recordOffsetTracking && lastPageRecordOffsets.length < batchSize) {
            lastPageRecordOffsets = new long[batchSize];
        }
        // Setting up builders may trip the circuit breaker. Make sure they're all always closed
        try {
            decoder.setupBuilders(blockBuilders, batchSize);
            return errorPolicy.isStrict() ? decodePageFailFast(blockBuilders) : decodePageLenient(blockBuilders);
        } finally {
            Releasables.close(blockBuilders);
            long deltaTotal = totalRowCount - startTotalRowCount;
            long deltaErrors = errorCount - startErrorCount;
            counters.addRowsEmitted(deltaTotal - deltaErrors);
            counters.addParseErrors(deltaErrors);
            counters.addReadNanos(System.nanoTime() - startNanos);
        }
    }

    /**
     * {@link ErrorPolicy.Mode#FAIL_FAST}: abort on the first whole-line parse failure
     * (see {@link #onNdjsonLineParseError}) -- no recovery, no scratch-row path.
     */
    private Page decodePageFailFast(Block.Builder[] blockBuilders) throws IOException {
        int lineCount = 0;
        while (lineCount < batchSize) {
            try {
                if (parser.nextToken() == null) {
                    break; // End of stream
                }
            } catch (JsonParseException | StreamConstraintsException e) {
                totalRowCount++;
                onNdjsonLineParseError(e, totalRowCount, "nextToken"); // FAIL_FAST: throws
            }
            // Record-canonical stripe attribution: this record belongs to floor(itsOwnStart / B), captured
            // from its START_OBJECT byte before decodeObject advances the parser. Pages are not capped at
            // stripe lines; the iterator splits the page's rows by their offsets (byte-range cover model).
            long stripeRecordStart = recordOffsetTracking ? tokenStartOffset() : 0L;

            totalRowCount++;
            this.blockTracker.clear();
            // Capture the record's start offset before decodeObject advances the parser. The slice-relative
            // offset feeds the external_max_record_size span check; the file-global offset feeds _rowPosition.
            boolean trackOffset = capEnforced || rowPositionSlot >= 0;
            long startSliceOffset = trackOffset ? parserSliceByteOffset() : 0L;
            long recordOffset = trackOffset ? recordFileOffset(startSliceOffset) : 0L;

            try {
                decoder.decodeObject(parser, ArrayEntry.NONE);
            } catch (JsonParseException | StreamConstraintsException e) {
                onNdjsonLineParseError(e, totalRowCount, "decodeObject");
            }

            if (capEnforced) {
                // span runs from just after the record's opening '{' (startSliceOffset was captured after
                // nextToken consumed START_OBJECT) through its closing '}', so it omits both the opening brace
                // and the line terminator — a couple of bytes under the splitter's terminator-inclusive count.
                // That can only make the decoder slightly more permissive at very small caps, never stricter, so
                // it never spuriously rejects a record a coordinator chunk already accepted. The record was fully
                // decoded before this check (the buffer is already bounded — byte-array segments are <= 16 MiB and
                // Jackson's StreamReadConstraints bound a single streamed token), which trades a fail-fast
                // pre-scan for the single-pass decode that issue 965 requires.
                long span = parserSliceByteOffset() - startSliceOffset;
                if (span > maxRecordBytes) {
                    // Keep the failed row out of the emitted-rows counter (the finally adds totalRowCount).
                    totalRowCount--;
                    throw recordTooLarge(span);
                }
            }

            if (rowPositionSlot >= 0) {
                ((LongBlock.Builder) blockBuilders[rowPositionSlot]).appendLong(recordOffset);
                blockTracker.set(rowPositionSlot);
            }

            if (recordOffsetTracking) {
                lastPageRecordOffsets[lineCount] = stripeRecordStart;
            }
            lineCount++;
            committedRowCount++;
            for (int i = 0; i < blockBuilders.length; i++) {
                if (blockTracker.get(i) == false) {
                    blockBuilders[i].appendNull();
                } else {
                    markColumnSeen(i);
                }
            }
        }
        if (recordOffsetTracking) {
            lastPageRecordCount = lineCount;
        }
        return buildPageFromBuildersOrNull(blockBuilders, lineCount);
    }

    /**
     * Lenient modes: skip bad lines up to the error budget, using scratch builders so partial rows
     * are never committed to the page.
     */
    private Page decodePageLenient(Block.Builder[] blockBuilders) throws IOException {
        ensureLenientScratchBuffers();
        final Block.Builder[] rowScratch = lenientScratchBuilders;
        if (rowScratch == null) {
            throw new EsqlIllegalArgumentException("lenient scratch builders missing after ensureLenientScratchBuffers");
        }

        int lineCount = 0;
        while (lineCount < batchSize) {
            // Reset before the record-opening token is read, not after: a constraint violation on that token
            // is already the next record's failure, and must not inherit the previous record's charge.
            this.recordChargedToBudget = false;
            try {
                if (parser.nextToken() == null) {
                    break; // End of stream
                }
            } catch (JsonParseException | StreamConstraintsException e) {
                totalRowCount++;
                onNdjsonLineParseError(e, totalRowCount, "nextToken");
                recoverFromParseException(parser);
                continue;
            }
            // Record-canonical stripe attribution (see decodePageFailFast): the record's own START_OBJECT byte,
            // captured before decodeObject / recovery advance the parser. Recorded only for committed rows.
            long stripeRecordStart = recordOffsetTracking ? tokenStartOffset() : 0L;

            totalRowCount++;
            this.blockTracker.clear();
            this.rowDroppedBySkipRow = false;
            // Capture before decodeObject / recovery advance the parser. The slice-relative offset feeds
            // the external_max_record_size span check; the file-global offset feeds _rowPosition and truncation.
            boolean trackOffset = capEnforced || rowPositionSlot >= 0;
            long startSliceOffset = trackOffset ? parserSliceByteOffset() : 0L;
            long recordOffset = trackOffset ? recordFileOffset(startSliceOffset) : 0L;

            try {
                // One record's worth, not one page's worth: these scratch builders hold exactly the row being
                // decoded and are built-and-released before the next one. Sizing them at batchSize would zero a
                // page-sized array (and reserve it on the breaker) per record — the whole cost of the lenient
                // path — to hold a single value. Multivalued cells grow the scratch on demand.
                decoder.setupBuilders(rowScratch, 1);
                try {
                    decoder.decodeObject(parser, ArrayEntry.NONE);
                } catch (JsonParseException | StreamConstraintsException e) {
                    onNdjsonLineParseError(e, totalRowCount, "decodeObject");
                    recoverFromParseException(parser);
                    continue;
                }
                if (capEnforced) {
                    // The cap is checked only after a successful decode, so an oversized record that is ALSO
                    // malformed JSON is classified by the parse-error path above (counts against the lenient
                    // error budget) rather than being silently dropped as "too large". This is intentional:
                    // the alternative is a raw-byte line scan on the recovery path, i.e. the redundant sweep
                    // issue 965 removed. Both outcomes keep the bad row out of the result; only the warning
                    // wording and budget attribution differ.
                    long span = parserSliceByteOffset() - startSliceOffset;
                    if (span > maxRecordBytes) {
                        // Oversized record under a non-strict policy. Undo the pre-decode row count so the
                        // skipped record stays out of rowsEmitted / error-budget accounting, matching the
                        // pre-#965 byte-array filter (which dropped the record before it reached the
                        // decoder). Crucially the buffer is NOT compacted, so retained rows keep their true
                        // file offsets — the compaction in the old filter shifted _rowPosition /
                        // _file.record_ref for every row after a skip (issue 965 feedback).
                        totalRowCount--;
                        if (sourceBytes == null) {
                            // Streaming/fallback: no cheap resumption point, so stop at the oversized record.
                            // Rows already in blockBuilders are a partial prefix. Emit a one-shot partial-results
                            // warning (best-effort, via the same thread-bound HeaderWarning path as skip warnings);
                            // NdJsonPageIterator keeps the under-count out of the stats cache (see truncated()).
                            skipWarnings.add(
                                "NDJSON read truncated at byte ["
                                    + recordOffset
                                    + "]: a record exceeded external_max_record_size ["
                                    + maxRecordBytes
                                    + "]; results are partial"
                            );
                            truncated = true;
                            truncatedAtByte = recordOffset;
                            break;
                        }
                        // Byte-array: the oversized record is fully buffered, so drop it and keep decoding. The
                        // dropped record makes the row count external_max_record_size-dependent, so mark the scan
                        // uncacheable (the iterator safe-misses on capDropped) — the cap is not fingerprinted.
                        // Track column presence even for dropped records so absent-column warnings reflect
                        // file-level absence, not just committed-record absence (same as the skip_row path).
                        for (int i = 0; i < rowScratch.length; i++) {
                            if (blockTracker.get(i)) {
                                markColumnSeen(i);
                            }
                        }
                        capDropped = true;
                        continue;
                    }
                }
                if (rowPositionSlot >= 0) {
                    ((LongBlock.Builder) rowScratch[rowPositionSlot]).appendLong(recordOffset);
                    blockTracker.set(rowPositionSlot);
                }
                if (rowDroppedBySkipRow) {
                    // error_mode: skip_row. An uncoercible value makes the whole record bad, so it never reaches
                    // the page — matching CsvFormatReader, and matching ErrorPolicy.Mode.SKIP_ROW's contract
                    // ("drop the entire bad row"). The scratch builders are released by the finally below and
                    // rebuilt for the next record; nothing partial is committed. NULL_FIELD still null-fills.
                    // Still track column presence for absent-column warnings: a column that only appears in
                    // dropped records is present in the FILE and must not be falsely reported as absent.
                    for (int i = 0; i < rowScratch.length; i++) {
                        if (blockTracker.get(i)) {
                            markColumnSeen(i);
                        }
                    }
                    continue;
                }
                for (int i = 0; i < rowScratch.length; i++) {
                    if (blockTracker.get(i) == false) {
                        rowScratch[i].appendNull();
                    } else {
                        markColumnSeen(i);
                    }
                }
                appendDecodedScratchRow(blockBuilders, rowScratch);
            } finally {
                Releasables.close(rowScratch);
            }

            if (recordOffsetTracking) {
                lastPageRecordOffsets[lineCount] = stripeRecordStart;
            }
            lineCount++;
            committedRowCount++;
        }
        if (recordOffsetTracking) {
            lastPageRecordCount = lineCount;
        }
        return buildPageFromBuildersOrNull(blockBuilders, lineCount);
    }

    private void ensureLenientScratchBuffers() {
        if (lenientScratchBuilders == null) {
            lenientScratchBuilders = new Block.Builder[projectedAttributes.size()];
            lenientScratchRowBlocks = new Block[projectedAttributes.size()];
        }
    }

    private Page buildPageFromBuildersOrNull(Block.Builder[] blockBuilders, int lineCount) {
        if (lineCount == 0) {
            return null;
        }
        // Row-count-only Page (zero-column projection, e.g. COUNT(*)). new Page(Block[]) rejects an
        // empty block array, so route through the explicit positionCount constructor.
        if (blockBuilders.length == 0) {
            return new Page(lineCount);
        }

        var blocks = new Block[this.projectedAttributes.size()];
        var success = false;
        try {
            for (int i = 0; i < blockBuilders.length; i++) {
                blocks[i] = blockBuilders[i].build();
            }
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(blocks);
            }
        }
        return new Page(blocks);
    }

    /**
     * Copies one fully decoded logical row from per-line scratch builders into the page builders.
     * Scratch builders are {@link Block.Builder#build() built} and released; callers still close
     * any non-built scratch builders via {@link Releasables#close}.
     */
    private void appendDecodedScratchRow(Block.Builder[] pageBuilders, Block.Builder[] scratchBuilders) {
        final int columns = scratchBuilders.length;
        Block[] rowBlocks = lenientScratchRowBlocks;
        if (rowBlocks == null) {
            throw new EsqlIllegalArgumentException("lenient scratch row blocks missing after ensureLenientScratchBuffers");
        }
        try {
            for (int i = 0; i < columns; i++) {
                rowBlocks[i] = scratchBuilders[i].build();
            }
            for (int i = 0; i < columns; i++) {
                pageBuilders[i].copyFrom(rowBlocks[i], 0, 1);
            }
        } finally {
            for (int i = 0; i < columns; i++) {
                Releasables.close(rowBlocks[i]);
                rowBlocks[i] = null;
            }
        }
    }

    /**
     * Prepare the tree of property decoders and return the root decoder. A dotted column name is a path of segments
     * ({@link NdJsonUtils#isFieldPath}), so one node serves both spellings of the column: the nested spelling walks
     * the tree segment by segment and the flat spelling resolves the same path at read time
     * ({@link BlockDecoder#resolveDottedPath}).
     * <p>
     * A node can be both a leaf and a prefix: a scalar column {@code a} beside a column {@code a.b}, which is what
     * treating a dot as an ordinary character in a column name means. Nothing about the tree shape depends on whether
     * a value is spelled flat or nested; only what a shape mismatch means at read time does (see
     * {@link BlockDecoder#decodeValue}).
     */
    private BlockDecoder prepareSchema(List<Attribute> projected) {
        BlockDecoder root = new BlockDecoder();
        int idx = 0;
        for (var attribute : projected) {
            // attribute.name() is the file's PHYSICAL field name: a declared `source` rename is resolved centrally
            // upstream (PhysicalNames), so the reader receives already-physical attributes and is rename-agnostic.
            // setAttribute keeps this physical attribute at channel idx; the block is relabeled to the logical name by
            // position downstream (ColumnMapping / queryDataSchema).
            String name = attribute.name();
            BlockDecoder decoder = root;
            if (NdJsonUtils.isFieldPath(name)) {
                int start = 0;
                int dot;
                while ((dot = name.indexOf('.', start)) >= 0) {
                    decoder = decoder.child(name.substring(start, dot));
                    start = dot + 1;
                }
                decoder = decoder.child(name.substring(start));
            } else {
                decoder = decoder.child(name);
            }
            decoder.setAttribute(attribute, idx);
            idx++;
        }
        return root;
    }

    /**
     * Encode {@code value} as UTF-8 into {@link #keywordScratch}, growing the backing array on demand,
     * and return the scratch view. Callers must pass the returned {@link BytesRef} straight to a sink
     * that copies the bytes synchronously (e.g. {@link BytesRefBlock.Builder#appendBytesRef}, which
     * delegates to {@link org.elasticsearch.common.util.BytesRefArray#append(BytesRef)} and copies
     * before returning); the next call to this method overwrites the scratch.
     */
    private BytesRef toScratchBytesRef(CharSequence value) {
        int maxLen = UnicodeUtil.maxUTF8Length(value.length());
        if (keywordScratch.bytes.length < maxLen) {
            keywordScratch.bytes = new byte[maxLen];
        }
        keywordScratch.offset = 0;
        keywordScratch.length = UnicodeUtil.UTF16toUTF8(value, 0, value.length(), keywordScratch.bytes);
        return keywordScratch;
    }

    @Override
    public void close() throws IOException {
        // Emit absent-column warnings for columns that were declared but never appeared in any committed
        // record. We wait until close() because we need to see all records before we can distinguish
        // "always absent" (warn) from "absent in some records but present in others" (normal sparse data,
        // do not warn). Only fires when at least one record was committed — guards against false positives
        // when all records were dropped by skip_row (totalRowCount > 0 but nothing committed). A column
        // absent from every committed record is effectively absent from the file, so we use
        // absentDeclaredColumnMessage to deduplicate cleanly with Parquet/SAI warnings via InformationalWarningBudget.
        if (absentColumnWarningSink != null && committedRowCount > 0) {
            for (int i = 0; i < projectedAttributes.size(); i++) {
                if (columnEverPresent.get(i) == false) {
                    Attribute attr = projectedAttributes.get(i);
                    if (attr.dataType() != DataType.NULL && attr.dataType() != DataType.UNSUPPORTED) {
                        absentColumnWarningSink.accept(SkipWarnings.absentDeclaredColumnMessage(attr.name()));
                    }
                }
            }
        }
        // input may be null on the byte-array fast path; IOUtils.close tolerates null entries.
        // We also close `parser` so its internal buffers (small but real) are released on the byte-array
        // path, where there is no `input` to close. AUTO_CLOSE_SOURCE is disabled on the shared
        // JsonFactory, so closing the parser does not double-close the wrapping codec stream.
        IOUtils.close(parser, input);
        input = null;
        parser = null;
    }

    /**
     * Total number of {@code children.get(String)} (HashMap) fallback lookups across the decoder
     * tree on this decoder's lifetime. Each {@link BlockDecoder#decodeObject} field probes the
     * per-object identity cache first; the HashMap is consulted only on identity-cache miss (i.e.
     * the first time a given canonicalised {@code String} instance is seen by this object's
     * decoder). Once a name is cached, repeat occurrences across pages cost a single identity
     * compare. Package-private for tests that assert the cache is effective.
     */
    long hashMapFallbacks() {
        return hashMapFallbacks;
    }

    private long hashMapFallbacks = 0L;

    /**
     * Size of the root {@code BlockDecoder}'s identity cache, or {@code 0} when the cache has
     * not been allocated (no JSON object decoded yet, or the root decoder has {@code null}
     * children). Package-private so tests can pin the bound semantics on dynamic-key inputs
     * where the cache is intentionally capped rather than allowed to grow with each new field
     * name on the wire.
     */
    int rootIdentityCacheSize() {
        var cache = decoder.identityCache;
        return cache == null ? 0 : cache.size();
    }

    /**
     * Sentinel returned by {@link BlockDecoder#lookupChild} for canonicalised field-name
     * {@code String} instances that have been resolved to "no matching projection". One per
     * decoder; only identity comparisons are performed against it (never any method calls), so
     * its inner-class outer-{@code this} binding is irrelevant.
     */
    private final BlockDecoder unprojected = new BlockDecoder();

    /**
     * Which of a node's position entries the enclosing JSON array opened, and therefore what that node may append
     * while one of the array's elements is decoded into it. An entry that is open must receive a value (an empty
     * multivalue cannot be committed) and one that is not open cannot, so the element kinds a node accepts follow
     * directly from this.
     * <p>
     * It travels down the recursion rather than living on the node because the answer depends on how the node was
     * reached, not on the node: {@link BlockDecoder#beginPositionEntry} recurses into children as
     * {@code (true, true)}, so a node inside an ancestor's array of objects has both its own and its children's
     * entries open, while a node that opened an array itself has exactly one of the two.
     */
    private enum ArrayEntry {
        /** Not inside an array. No entry is open, so this node may open its own. */
        NONE,
        /** An array of primitives on this node: its own entry is open, its children's are not. */
        SELF,
        /** An array of objects on this node: its children's entries are open, its own is not. */
        CHILDREN,
        /** An array of objects on an ancestor: this node's own entry and its children's are both open. */
        BOTH
    }

    // ---------------------------------------------------------------------------------------------
    // A tree of decoders. Avoids path reconstruction when traversing nested objects.
    private class BlockDecoder {
        @Nullable
        DataType dataType;
        String name;
        int blockIdx;
        Block.Builder blockBuilder;
        /** Declared date parser for this column, or {@code null} to use the file-level {@link #datetimeFormatter}. */
        @Nullable
        DateFormatter declaredFormatter;
        Map<String, BlockDecoder> children;
        /**
         * Identity-keyed cache of field-name {@link String} instances previously seen by this
         * object's decoder, mapped to either a child {@link BlockDecoder} (projected) or
         * {@link #unprojected} (unprojected). Lazily allocated on the first field probe so
         * leaf decoders (which never call {@link #decodeObject}) pay nothing.
         * <p>
         * Correctness rests on Jackson's {@link com.fasterxml.jackson.core.sym.ByteQuadsCanonicalizer}
         * (enabled by default; {@code JsonFactory.Feature#CANONICALIZE_FIELD_NAMES}) returning the
         * <em>same</em> {@code String} instance for repeat occurrences of a name across pages — and
         * on the {@link NdJsonUtils#JSON_FACTORY} root canonicalizer being shared so subsequent
         * parsers from the same factory inherit those instances. A name that hash-collides with a
         * different identity falls through to the slow HashMap lookup and re-primes the cache; the
         * code is therefore safe even when an instance does turn over (e.g. canonicaliser rehash).
         */
        @Nullable
        IdentityHashMap<String, BlockDecoder> identityCache;

        /** The child decoder for one field-name segment, created on first use. */
        BlockDecoder child(String segment) {
            if (children == null) {
                children = new HashMap<>();
            }
            return children.computeIfAbsent(segment, k -> new BlockDecoder());
        }

        void setAttribute(Attribute attribute, int blockIdx) {
            this.dataType = attribute.dataType();
            this.name = attribute.name();
            this.blockIdx = blockIdx;
            // Resolve the declared date formatter ONCE (prepareSchema runs once per decoder), keyed by the column's
            // physical (file) name — the same key space FileSourceFactory physicalized declaredDateFormats into.
            String pattern = declaredDateFormats.get(attribute.name());
            this.declaredFormatter = pattern != null ? DateFormatter.forPattern(pattern) : null;
        }

        /**
         * Builders are set up independently as we need to create new ones for each page — and, on the lenient
         * path, for each record. {@code estimatedSize} is how many positions the caller expects these builders
         * to hold: {@code batchSize} for the page builders, {@code 1} for the per-record scratch builders. It is
         * only an initial capacity — a builder grows on demand — but it is eagerly allocated and charged to the
         * circuit breaker, so a caller that over-states it pays for the whole array on every setup.
         */
        void setupBuilders(Block.Builder[] blockBuilders, int estimatedSize) {
            if (dataType != null) {
                // The type -> block-shape mapping is not re-derived here: it belongs to the declared-read SPI,
                // which delegates the enumeration to PlannerUtils.toElementType. A reader-local copy of it is
                // exactly how unsigned_long came to pass dataset validation and then throw at page setup.
                try {
                    blockBuilder = DeclaredTypeCoercions.builderFor(dataType, blockFactory, estimatedSize);
                } catch (IllegalArgumentException e) {
                    throw unsupportedTypeForNdjson(dataType, e);
                }
                blockBuilders[blockIdx] = blockBuilder;
            }

            if (children != null) {
                for (var child : children.values()) {
                    child.setupBuilders(blockBuilders, estimatedSize);
                }
            }
        }

        /**
         * The declared type passed create + resolution (it is in {@code DeclaredSchemaValidator.DECLARABLE_TYPES})
         * but NDJSON has no decoder arm for it, or the declared-read SPI has no block shape for it. Names the
         * column and type so the failure is actionable rather than a bare internal error.
         * Raised from builder setup, where the SPI rejects the type, and from the {@code default} arm of the value
         * decode switch. Either is a coverage gap, not a routine per-record condition: a bad <em>value</em> of a
         * supported type takes the per-cell error policy instead.
         */
        private IllegalArgumentException unsupportedTypeForNdjson(DataType type) {
            return unsupportedTypeForNdjson(type, null);
        }

        private IllegalArgumentException unsupportedTypeForNdjson(DataType type, Throwable cause) {
            return new IllegalArgumentException(
                "column [" + name + "] has declared type [" + type.typeName() + "] which is not supported for NDJSON reads",
                cause
            );
        }

        private void decodeObject(JsonParser parser, ArrayEntry entry) throws IOException {
            if (parser.currentToken() != JsonToken.START_OBJECT) {
                throw new NdJsonParseException(parser, "Expected JSON object");
            }
            // This object's members decode into the children of this node. When the object is an element of an
            // enclosing array, beginPositionEntry recursed into those children as (true, true), so each of them has
            // both its own entry and its children's open.
            ArrayEntry childEntry = entry == ArrayEntry.NONE ? ArrayEntry.NONE : ArrayEntry.BOTH;
            String fieldName;
            boolean poisoned = false;
            while ((fieldName = parser.nextFieldName()) != null) {
                var childDecoder = lookupChild(fieldName);
                parser.nextToken();
                if (childDecoder == unprojected || poisoned) {
                    // Unknown/unprojected field: advance to its value then skip (no decode).
                    // For string values nextFieldName() uses _skipString() internally on the next
                    // call, so we avoid _finishString2 for non-projected string fields.
                    // Also used to drain remaining fields after a child poisoned this position.
                    parser.skipChildren();
                } else {
                    try {
                        childDecoder.decodeValue(parser, childEntry);
                    } catch (PoisonedPositionException e) {
                        poisoned = true;
                        parser.skipChildren(); // drain current field's value, then loop drains the rest
                    }
                }
            }
            // Parser is now at END_OBJECT. Re-throw so the enclosing array handler can cancel the position.
            if (poisoned) {
                throw PoisonedPositionException.INSTANCE;
            }
        }

        /**
         * Resolve {@code fieldName} to either a projected child {@link BlockDecoder} or the
         * {@link #unprojected} sentinel, using an identity-keyed cache to avoid the
         * {@link String#hashCode}/{@link HashMap#get} pair on repeat occurrences of the same
         * canonicalised {@code String} instance.
         * <p>
         * On cache miss (first time this object's decoder sees this identity) the call falls
         * back to a single {@code children.get(fieldName)} probe and primes the cache with
         * either the child decoder or {@link #unprojected}. The fallback count is exposed via
         * {@link #hashMapFallbacks()} so tests can pin that the cache is doing its job.
         * <p>
         * When {@code children} is {@code null} the decoder cannot match any projection, so the
         * loop short-circuits to {@link #unprojected} without allocating an identity cache or
         * incrementing the fallback counter — there is no HashMap probe to avoid in that case.
         * <p>
         * The cache is bounded at {@link #identityCacheMaxEntries()} entries to keep
         * dynamic-key NDJSON inputs (per-tenant column names, event ids embedded as JSON keys,
         * sparse extensions) from growing the cache without bound. Once full it stops accepting
         * new entries — existing entries keep serving identity hits and the rest pay the
         * {@code HashMap} probe — so correctness degrades to "no cache" for the tail, not to a
         * memory leak.
         */
        private BlockDecoder lookupChild(String fieldName) {
            if (children == null) {
                return unprojected;
            }
            int maxEntries = identityCacheMaxEntries();
            if (identityCache == null) {
                // Seed to the bound so narrow projections over wide objects avoid rehashing during
                // warm-up as unprojected names fill the floor-sized working set.
                identityCache = new IdentityHashMap<>(maxEntries);
            }
            BlockDecoder cached = identityCache.get(fieldName);
            if (cached != null) {
                return cached;
            }
            hashMapFallbacks++;
            BlockDecoder resolved = children.get(fieldName);
            if (resolved == null && NdJsonUtils.isFieldPath(fieldName)) {
                // A flat dotted key (e.g. "a.b") with no direct child: ES ingest reinterprets a dotted field
                // name as the equivalent nested object, so a record spelling a column flat must reach the same
                // leaf as the nested {"a":{"b":...}} spelling. Resolve it as a path through this decoder's
                // subtree. The walk runs only on this first-seen identity (the result is then cached like any
                // direct child), so the common non-dotted / cache-hit path is unaffected.
                resolved = resolveDottedPath(fieldName);
            }
            BlockDecoder toCache = resolved == null ? unprojected : resolved;
            if (identityCache.size() < maxEntries) {
                identityCache.put(fieldName, toCache);
            }
            return toCache;
        }

        /**
         * Resolve a flat dotted field name (e.g. {@code "a.b"} or {@code "a.b.c"}) as a path through this
         * decoder's {@code children} subtree, splitting on {@code .} and descending one segment at a time.
         * The walk is relative to {@code this} node, so a caller on the {@code x} structural node resolves
         * {@code "a.b"} as {@code x → a → b}, not from the tree root.
         * Returns the node the whole path lands on: a leaf decoder (the flat spelling of a dotted column) or a
         * structural prefix node (a flat prefix whose remainder is spelled nested, e.g. {@code {"a.b":{"c":1}}}
         * against schema {@code a.b.c}). The caller then decodes the value into that node exactly as it would
         * for the nested spelling. Returns {@code null} when any segment is missing: the field is unreachable and
         * treated as unprojected, which null-fills the cell exactly as an unknown field does.
         */
        private BlockDecoder resolveDottedPath(String fieldName) {
            BlockDecoder node = this;
            int start = 0;
            while (true) {
                if (node.children == null) {
                    return null;
                }
                int dot = fieldName.indexOf('.', start);
                String segment = dot < 0 ? fieldName.substring(start) : fieldName.substring(start, dot);
                node = node.children.get(segment);
                if (node == null) {
                    return null;
                }
                if (dot < 0) {
                    return node;
                }
                start = dot + 1;
            }
        }

        /**
         * Upper bound for {@link #identityCache} entries on this decoder. The local
         * {@code children} fanout is the projected fanout at this object level, not the full
         * observed object width. The hard floor ({@value #IDENTITY_CACHE_MIN_CAP}) gives narrow
         * projections a usable working set for unprojected names, while wider projections get
         * additional space proportional to their projected children.
         */
        private int identityCacheMaxEntries() {
            return Math.max(IDENTITY_CACHE_MIN_CAP, children.size() * IDENTITY_CACHE_FANOUT_MULT);
        }

        /**
         * @param includeSelf     when {@code true}, begins an MV entry on this node's own column. Use this for an array
         *        of primitives, whose elements land in this column. An array of objects contributes nothing to it, and
         *        opening it would leave an empty multivalue that cannot be committed.
         * @param includeChildren when {@code true}, also begins MV entries on child decoders. Use this only for JSON
         *        arrays of objects (e.g. {@code [{"a":1},{"a":2}]}) where every child column shares one MV slot per
         *        element. For arrays of primitives (e.g. {@code salary_change} as doubles while {@code salary_change.int}
         *        is a separate top-level field), {@code false} so children are not opened for values they will never
         *        receive from this array.
         */
        private void beginPositionEntry(boolean includeSelf, boolean includeChildren) {
            // We may have DataType.NULL for unknown columns. And NullBlock.Builder throws on beginPositionEntry()
            if (includeSelf && blockBuilder != null && dataType != DataType.NULL) {
                if (blockTracker.get(blockIdx)) {
                    // A flat spelling already committed this leaf; a later array on an ancestor merges into that
                    // cell. reopenLastPositionEntry returns false when the cell is already null: a null is a
                    // property of the whole position, not a member of its value list, so it cannot widen. This
                    // node then has NO open entry for the rest of the array, which every path that assumes one
                    // must check for (see decodeValue's append guard, endPositionEntry and
                    // cancelAndNullPositionEntry): appending anyway would start a second position for this
                    // column and misalign it from its siblings.
                    ((AbstractBlockBuilder) blockBuilder).reopenLastPositionEntry();
                } else {
                    blockBuilder.beginPositionEntry();
                }
            }
            if (includeChildren && children != null) {
                for (var child : children.values()) {
                    child.beginPositionEntry(true, true);
                }
            }
        }

        private void endPositionEntry(boolean includeSelf, boolean includeChildren) {
            if (includeSelf && blockBuilder != null && dataType != DataType.NULL) {
                AbstractBlockBuilder abb = (AbstractBlockBuilder) blockBuilder;
                // A refused reopen left no entry open and the cell as the null it already was: nothing to commit.
                if (abb.isPositionEntryOpen()) {
                    if (abb.currentPositionEntryIsEmpty()) {
                        // An array of objects opened this entry, then no element wrote a value here (a
                        // leaf-and-prefix sibling was filled instead, or every element omitted this field).
                        // Commit a null so the position stays aligned with its siblings; endPositionEntry
                        // asserts on an empty entry.
                        abb.cancelPositionEntry();
                        blockTracker.set(blockIdx);
                        blockBuilder.appendNull();
                    } else {
                        blockBuilder.endPositionEntry();
                    }
                }
            }
            if (includeChildren && children != null) {
                for (var child : children.values()) {
                    child.endPositionEntry(true, true);
                }
            }
        }

        /**
         * Cancels the current position entry (rolling back all values appended since
         * {@link #beginPositionEntry}) and writes a null for this position instead. Used
         * when a coercion failure poisoned an array: the whole position is nulled rather
         * than committed as a partial multivalue, matching the columnar reader contract.
         * <p>
         * A node whose reopen {@link #beginPositionEntry} refused has no entry to cancel, and its cell already
         * holds the null this method would write, so it is left alone: {@code cancelPositionEntry} asserts when
         * no entry is open.
         */
        private void cancelAndNullPositionEntry(boolean includeSelf, boolean includeChildren) {
            if (includeSelf && blockBuilder != null && dataType != DataType.NULL) {
                AbstractBlockBuilder abb = (AbstractBlockBuilder) blockBuilder;
                if (abb.isPositionEntryOpen()) {
                    abb.cancelPositionEntry();
                    blockTracker.set(blockIdx);
                    blockBuilder.appendNull();
                }
            }
            if (includeChildren && children != null) {
                for (var child : children.values()) {
                    child.cancelAndNullPositionEntry(true, true);
                }
            }
        }

        /**
         * Merges a further occurrence of this column in the same record into the cell the first occurrence committed,
         * as a multivalue: {@code {"a":{"b":1},"a.b":2}} yields {@code [1, 2]}, the same values indexing that document
         * would produce. The already-committed position is reopened and this occurrence decodes into it as if it were an
         * array element, so a scalar contributes one value, an array contributes each of its elements, and a JSON null
         * contributes none. Closing the reopened position always succeeds: it holds the first occurrence's value even
         * when this one appends nothing.
         *
         * <p>A cell an error policy already nulled cannot be widened, so this occurrence is dropped: the policy has
         * decided (and warned) that the column is null for this record. A failure in this occurrence nulls the whole
         * cell, matching the array contract that a poisoned position is nulled rather than committed in part.
         */
        private void appendFurtherOccurrence(JsonParser parser) throws IOException {
            if (parser.currentToken() == JsonToken.VALUE_NULL) {
                // Contributes no value, so the cell is already final: return before reopening, which would otherwise
                // build the multivalue index for a block that stays single-valued.
                return;
            }
            if (((AbstractBlockBuilder) blockBuilder).reopenLastPositionEntry() == false) {
                parser.skipChildren();
                return;
            }
            // This occurrence fills this node's own reopened entry, so an object among its values has no open entry of
            // its own and is dropped like a stray array element.
            try {
                decodeValue(parser, ArrayEntry.SELF);
                blockBuilder.endPositionEntry();
            } catch (PoisonedPositionException e) {
                cancelAndNullPositionEntry(true, false);
            }
        }

        /**
         * An empty JSON array {@code []} contributes no values, exactly like a missing field, which is what ingest
         * does with {@code subobjects: false}: {@code {"a.b":[],"a":[{"b":1}]}} indexes {@code a.b} as {@code [1]}.
         * So this neither appends a cell nor sets {@link #blockTracker}. Leaving the cell unclaimed is what lets a
         * later spelling in the same record still fill it; when none does, the end-of-record fill nulls it.
         * <p>
         * Claiming it here instead (appending the null eagerly) would both pin the column to {@code null} against
         * a later value and, because a null cannot be reopened to gain values, leave a following array on an
         * ancestor with no open entry to append into.
         * <p>
         * This also keeps {@link Block.Builder#beginPositionEntry()} from being run with no values, which
         * {@link org.elasticsearch.compute.data.AbstractBlockBuilder#endPositionEntry()} asserts against.
         * <p>
         * The key itself was present, so a leaf counts as seen for the absent-declared-column warning, the same
         * way a JSON null does. Columns nested <em>under</em> an empty array get no such mark: their own keys
         * never appeared, which is exactly the missing-field case.
         */
        private void noteEmptyArray() {
            if (blockBuilder != null && dataType != DataType.NULL) {
                markColumnSeen(blockIdx);
            }
        }

        /**
         * Decodes the current JSON value into this decoder's block (or, for a structural prefix node, recurses into
         * its children). NDJSON is schema-on-read: the inferred/bound schema flattens nested objects to dotted leaf
         * columns. Purely STRUCTURAL shape mismatches are not errors: they are null-filled for the affected column(s)
         * and {@code DEBUG}-logged, never failing the query regardless of {@code error_mode}:
         * <ul>
         *   <li>a JSON {@code null} where an object was expected on a structural prefix node leaves its leaf columns
         *       null for that row (e.g. an intermittently-null nested object across millions of records), logged at
         *       {@code DEBUG} only, never {@code WARN}, since surfacing it by default would flood the log without
         *       giving the cluster admin an actionable signal;</li>
         *   <li>a stray scalar among a heterogeneous array of objects is likewise null-filled and {@code DEBUG}-logged,
         *       and symmetrically a stray object among a heterogeneous array of scalars is simply omitted from that
         *       column's multi-value entry and {@code DEBUG}-logged. Neither direction is a value error.</li>
         * </ul>
         * A cell that genuinely cannot be REPRESENTED under the column's type, by contrast, is governed by
         * {@code error_mode}, identically for a declared or an inferred column: a bad value or a cross-kind token
         * ({@link #coercionFailure} / {@link #crossKindDrift}) routes through {@link ErrorPolicy}: {@code FAIL_FAST}
         * fails the query, {@code SKIP_ROW} drops the whole record, {@code NULL_FIELD} nulls the cell and warns.
         * A scalar and an object at one name are NOT such a conflict here: a dot is an ordinary character in a column
         * name, so {@code a} and {@code a.b} are independent columns and neither shape contradicts the other.
         */
        private void decodeValue(JsonParser parser, ArrayEntry entry) throws IOException {
            JsonToken token = parser.currentToken();

            if (dataType == DataType.NULL) {
                // Don't do anything. We must do a single appendNull() on null blocks, this will be done
                // at the end of decodePage() when we check that all blocks have moved forward.
                parser.skipChildren();
                return;
            }

            // A record can spell one column more than once: a flat "a.b" beside the nested {"a":{"b":...}} (either
            // order), or a repeated key. Every occurrence contributes to the cell, as a multivalue, which is what the
            // same document produces on ingest. The column keeps exactly one position either way, so it stays aligned
            // with its siblings. Only leaf decoders (blockBuilder != null) commit a cell; structural prefix nodes
            // recurse and are guarded at their own leaves. An array element belongs to one occurrence and appends
            // into the entry that occurrence opened. A plain JSON null does not set blockTracker (see below),
            // so it neither claims the cell nor merges into it.
            if (blockBuilder != null && entry == ArrayEntry.NONE && blockTracker.get(blockIdx)) {
                if (token == JsonToken.START_OBJECT && children != null) {
                    // A later object at a leaf-and-prefix node still populates dotted children; it does not
                    // merge into this node's already-claimed scalar cell.
                    decodeObject(parser, entry);
                    return;
                }
                appendFurtherOccurrence(parser);
                return;
            }

            if (token == JsonToken.START_ARRAY) {
                // Start a multi-value entry on this decoder and all its children (nested arrays are flattened).
                // Note: the entry state is needed because blockBuilder.beginPositionEntry() is not idempotent.
                // Calling it twice implicitly calls endPositionEntry().
                if (entry == ArrayEntry.NONE) {
                    // `includeChildren` gates opening the child MV entries and must reflect whether the array
                    // actually contains an object: otherwise later objects append into never-opened child builders,
                    // misaligning rows across columns. Skip leading elements that cannot open this node's MV entry:
                    // - a structural (prefix) node carries no scalar values of its own, so it skips leading
                    // stray scalars (e.g. [null, "x", {"type":"a"}]) until the first object or the array end;
                    // - symmetrically, a scalar leaf skips leading stray objects (e.g. [null, {"x":1}, "a"]) until
                    // the first scalar or the array end: without this, an all-object array on a scalar leaf would
                    // call beginPositionEntry() and then never append a value before endPositionEntry(), which
                    // AbstractBlockBuilder#endPositionEntry() asserts against (see noteEmptyArray).
                    JsonToken first = parser.nextToken();
                    // What this node can take from an array: scalars when it has a column of its own, objects when it
                    // has columns underneath. A scalar column that also prefixes dotted columns can take both, and the
                    // element kind that opens the position decides which one this array contributes to.
                    boolean takesScalars = blockBuilder != null;
                    boolean takesObjects = children != null;
                    while (first == JsonToken.VALUE_NULL
                        || (takesScalars == false && first != null && first != JsonToken.START_OBJECT && first != JsonToken.END_ARRAY)
                        || (takesObjects == false && first == JsonToken.START_OBJECT)) {
                        if (first != JsonToken.VALUE_NULL && logger.isDebugEnabled()) {
                            if (blockBuilder == null) {
                                logger.debug(
                                    "Expected object in array for nested field [{}] but got {} at {}",
                                    parser.getParsingContext().pathAsPointer(),
                                    first,
                                    parser.getTokenLocation()
                                );
                            } else {
                                logger.debug(
                                    "Expected scalar type [{}] for attribute [{}] but got object at {}",
                                    dataType.typeName(),
                                    name,
                                    parser.getTokenLocation()
                                );
                            }
                        }
                        parser.skipChildren(); // no-op for scalar/null tokens; safe to call here
                        first = parser.nextToken();
                    }
                    if (first == JsonToken.END_ARRAY) {
                        noteEmptyArray();
                        return;
                    }
                    boolean includeChildren = first == JsonToken.START_OBJECT;
                    // Only the entries the array's element kind will actually fill are opened: an entry that is opened
                    // and never appended to cannot be committed (endPositionEntry rejects an empty multivalue).
                    boolean includeSelf = includeChildren == false;
                    ArrayEntry elementEntry = includeChildren ? ArrayEntry.CHILDREN : ArrayEntry.SELF;
                    beginPositionEntry(includeSelf, includeChildren);
                    try {
                        decodeValue(parser, elementEntry);
                        while (parser.nextToken() != JsonToken.END_ARRAY) {
                            decodeValue(parser, elementEntry);
                        }
                        endPositionEntry(includeSelf, includeChildren);
                    } catch (PoisonedPositionException e) {
                        while (parser.nextToken() != JsonToken.END_ARRAY) {
                            parser.skipChildren();
                        }
                        cancelAndNullPositionEntry(includeSelf, includeChildren);
                    }
                    return;
                }
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    try {
                        // A nested array flattens into the entry the enclosing one already opened.
                        decodeValue(parser, entry);
                    } catch (PoisonedPositionException e) {
                        // Drain the rest of this nested array, then rethrow so the
                        // enclosing array handler can drain its own remaining elements
                        // and cancel the position entry correctly.
                        while (parser.nextToken() != JsonToken.END_ARRAY) {
                            parser.skipChildren();
                        }
                        throw e;
                    }
                }
                return;
            }

            if (token == JsonToken.START_OBJECT) {
                if (children != null && entry != ArrayEntry.SELF) {
                    // Descend, whether or not this node also carries a scalar column of its own. A node with both is
                    // the flattened reading of a dot: the object's members reach the dotted columns underneath, and
                    // this node's own cell takes no value from an object (the end-of-record fill nulls it). Refused
                    // only for an array of primitives, whose elements have no child entry to append into.
                    decodeObject(parser, entry);
                    return;
                }
                if (dataType != null) {
                    if (entry != ArrayEntry.NONE) {
                        // A stray object among a heterogeneous array of scalars is a distinct, supported shape
                        // (mirrors the stray-scalar-among-objects case below), not the record-level scalar/object
                        // conflict this issue targets: the array's other scalar elements still decode and
                        // contribute to this column's multi-value entry, this element is simply omitted from it.
                        // Guarded by isDebugEnabled() so the JsonLocation allocation is skipped when DEBUG is off,
                        // since this can fire per-element across millions of records.
                        if (logger.isDebugEnabled()) {
                            logger.debug(
                                "Expected scalar type [{}] for attribute [{}] but got object at {}",
                                dataType.typeName(),
                                name,
                                parser.getTokenLocation()
                            );
                        }
                        parser.skipChildren();
                        return;
                    }
                    // The object's members address flattened names under this one (a.b, a.b.c), none of which is a
                    // column here. This node has no children. They are unreachable exactly as an unprojected
                    // field is, so they are skipped silently and this node's cell is left for the end-of-record
                    // fill. The scalar this column resolved to is not contradicted by an object at the same name.
                    parser.skipChildren();
                    return;
                }
                decodeObject(parser, entry);
                return;
            }

            if (blockBuilder == null || entry == ArrayEntry.CHILDREN) {
                // No column here can take this scalar: either a structural (prefix) node with no scalar builder of its
                // own, whose schema knows only dotted leaf columns for this field (e.g. "address.city"/"address.zip"),
                // or a stray scalar in an array that was opened for objects, whose own entry is not open. A JSON null
                // is the common, legitimate case (e.g. CloudTrail "responseElements": null) and stays silent either way.
                if (token != JsonToken.VALUE_NULL) {
                    if (entry != ArrayEntry.NONE) {
                        // A stray scalar among a heterogeneous array of objects is a distinct, supported
                        // shape (see the array-handling block above), not the record-level scalar/object
                        // conflict this issue targets. Leave the leaf descendants untracked so the
                        // end-of-row fill assigns them null, mirroring missing fields/empty arrays.
                        // Guarded by isDebugEnabled() so the JsonPointer/JsonLocation allocations are
                        // skipped when DEBUG is off, since this can fire per-row across millions of records.
                        if (logger.isDebugEnabled()) {
                            logger.debug(
                                "Expected object for nested field [{}] but got {} at {}",
                                parser.getParsingContext().pathAsPointer(),
                                token,
                                parser.getTokenLocation()
                            );
                        }
                    }
                    // This node's name is a prefix of column names, never a column itself, so a scalar spelled at
                    // it is an unprojected field rather than a contradiction of the columns below: they are named
                    // a.b, and nothing names a. Skipped silently, and their cells are left for the end-of-record fill.
                }
                parser.skipChildren();
                return;
            }

            if (token == JsonToken.VALUE_NULL) {
                // A JSON null contributes no value and does not claim the row: it neither appends a cell nor sets
                // blockTracker. The end-of-record fill supplies the null when no spelling of this column provides
                // a value, and a spelling that does provide one is not merged with the null. The key was present,
                // so mark the column seen (absent-column warnings track file presence, not a committed non-null).
                // Nulls inside an array are unsupported and skipped either way, so this single return covers both.
                markColumnSeen(blockIdx);
                return;
            }
            if (entry != ArrayEntry.NONE && ((AbstractBlockBuilder) blockBuilder).isPositionEntryOpen() == false) {
                // An enclosing array asked beginPositionEntry to reopen this leaf's cell and was refused: the cell
                // is already null and cannot widen. With no entry open, appending here would not join a multivalue,
                // it would start a SECOND position for this column, leaving it one position longer than its
                // siblings for this record (a Page only asserts equal position counts). The array therefore
                // contributes nothing to this column, exactly as appendFurtherOccurrence drops a further
                // occurrence into an already-nulled cell.
                markColumnSeen(blockIdx);
                return;
            }
            blockTracker.set(blockIdx);

            // This node's own entry is open for every state that reaches here (CHILDREN returned above), so the leaf
            // decoders only need to know whether they are appending into one.
            boolean inArray = entry != ArrayEntry.NONE;
            switch (dataType) {
                case BOOLEAN -> decodeBooleanValue(parser, token, inArray);
                case INTEGER -> decodeIntValue(parser, token, inArray);
                case LONG -> decodeLongValue(parser, token, inArray);
                case UNSIGNED_LONG -> decodeUnsignedLongValue(parser, token, inArray);
                case DOUBLE -> decodeDoubleValue(parser, token, inArray);
                case DATETIME -> decodeDatetimeValue(parser, token, inArray);
                case DATE_NANOS -> decodeDateNanosValue(parser, token, inArray);
                case KEYWORD, TEXT -> {
                    var chars = CharBuffer.wrap(parser.getTextCharacters(), parser.getTextOffset(), parser.getTextLength());
                    ((BytesRefBlock.Builder) blockBuilder).appendBytesRef(toScratchBytesRef(chars));
                }
                case IP -> decodeIpValue(parser, token, inArray);
                // Unreachable: a NULL-typed column returns at the top of decodeValue (its cell is null-filled at
                // end-of-page), so the switch is never entered for it.
                case NULL -> throw new AssertionError("NULL-typed column must be handled by the early return in decodeValue");
                default -> throw unsupportedTypeForNdjson(dataType);
            }
        }

        /**
         * The scalar-coercion arms below make an NDJSON read match the columnar and CSV readers, routing every
         * unrepresentable cell through {@link #coercionFailure} so the outcome depends only on {@code error_mode}:
         * <ul>
         *   <li>A <b>supported</b> coercion — a JSON string for any scalar column, a fractional number for a
         *       whole-number column, an epoch number for a datetime column — is coerced through the same
         *       {@code ::} cast engine (string→number rounds like {@code ::long}; string→boolean is strict
         *       case-insensitive; string→double preserves NaN). A parse failure or numeric overflow on such a
         *       token is a genuine value error and is routed through {@link #coercionFailure} — so it fails
         *       {@code fail_fast}, warns, and counts against the error budget exactly like a malformed CSV value.</li>
         *   <li>An <b>unsupported cross-kind</b> token — a boolean in a numeric/datetime column, a number in a
         *       boolean column: {@code supports(from, to)} is false, the pair the columnar readers reject at
         *       resolution. NDJSON has no physical schema to reject upfront, so {@link #crossKindDrift} routes the
         *       drift through {@link #coercionFailure} for a declared or an inferred column alike — no silent null,
         *       because {@code error_mode} governs the outcome regardless of where the type came from.</li>
         * </ul>
         * The common case (a JSON number in a numeric column, a JSON boolean in a boolean column) still decodes
         * straight from the parser with no string allocation.
         */
        private void decodeBooleanValue(JsonParser parser, JsonToken token, boolean inArray) throws IOException {
            if (token == JsonToken.VALUE_TRUE) {
                ((BooleanBlock.Builder) blockBuilder).appendBoolean(true);
            } else if (token == JsonToken.VALUE_FALSE) {
                ((BooleanBlock.Builder) blockBuilder).appendBoolean(false);
            } else if (token == JsonToken.VALUE_STRING) {
                try {
                    // strict + case-insensitive, matching the columnar/CSV declared-boolean coercion
                    ((BooleanBlock.Builder) blockBuilder).appendBoolean(
                        DeclaredTypeCoercions.strictParseBoolean(parser.getValueAsString())
                    );
                } catch (IllegalArgumentException | InvalidArgumentException e) {
                    coercionFailure(blockBuilder, parser, inArray, DataType.BOOLEAN);
                }
            } else {
                // A number in a boolean column: an unsupported cross-kind drift, not a coercion attempt
                // (supports(numeric, boolean) is false).
                crossKindDrift(parser, inArray, DataType.BOOLEAN);
            }
        }

        /**
         * A cross-kind token — a JSON kind that has no coercion to this column's type (a boolean in a
         * numeric/datetime column, a number in a boolean column, a non-string in an IP column). The columnar readers
         * reject such a pair at schema resolution; NDJSON has no physical schema to reject upfront, so it routes the
         * drift through {@link #coercionFailure}, the single policy sink — for a DECLARED or an INFERRED column alike,
         * because {@code error_mode} is one axis independent of where the type came from: {@code fail_fast} fails,
         * {@code null_field} warns + nulls + budget, {@code skip_row} drops the record + budget. (A declared type
         * additionally may never silently read as null, per {@link DeclaredTypeCoercions}; routing every column the
         * same way satisfies that and removes the former inferred-only silent null.)
         */
        private void crossKindDrift(JsonParser parser, boolean inArray, DataType target) throws IOException {
            coercionFailure(blockBuilder, parser, inArray, target);
        }

        private void decodeIntValue(JsonParser parser, JsonToken token, boolean inArray) throws IOException {
            if (token == JsonToken.VALUE_NUMBER_INT) {
                try {
                    ((IntBlock.Builder) blockBuilder).appendInt(parser.getIntValue());
                } catch (InputCoercionException e) {
                    coercionFailure(blockBuilder, parser, inArray, DataType.INTEGER); // out-of-int-range: a real value error
                }
            } else if (token == JsonToken.VALUE_NUMBER_FLOAT || token == JsonToken.VALUE_STRING) {
                try {
                    // fractional number or string: parse + ROUND through :: (matches ::integer / columnar / CSV)
                    ((IntBlock.Builder) blockBuilder).appendInt(EsqlDataTypeConverter.stringToInt(parser.getValueAsString()));
                } catch (IllegalArgumentException | InvalidArgumentException e) {
                    coercionFailure(blockBuilder, parser, inArray, DataType.INTEGER);
                }
            } else {
                crossKindDrift(parser, inArray, DataType.INTEGER); // boolean in a number column: unsupported cross-kind drift
            }
        }

        private void decodeLongValue(JsonParser parser, JsonToken token, boolean inArray) throws IOException {
            if (token == JsonToken.VALUE_NUMBER_INT) {
                try {
                    ((LongBlock.Builder) blockBuilder).appendLong(parser.getLongValue());
                } catch (InputCoercionException e) {
                    coercionFailure(blockBuilder, parser, inArray, DataType.LONG);
                }
            } else if (token == JsonToken.VALUE_NUMBER_FLOAT || token == JsonToken.VALUE_STRING) {
                try {
                    ((LongBlock.Builder) blockBuilder).appendLong(EsqlDataTypeConverter.stringToLong(parser.getValueAsString()));
                } catch (IllegalArgumentException | InvalidArgumentException e) {
                    coercionFailure(blockBuilder, parser, inArray, DataType.LONG);
                }
            } else {
                crossKindDrift(parser, inArray, DataType.LONG); // boolean in a number column: unsupported cross-kind drift
            }
        }

        /**
         * The {@code unsigned_long} twin of {@link #decodeLongValue}. A JSON integer is read as a
         * {@link BigInteger} rather than a {@code long} because the interesting half of the domain --
         * {@code (2^63, 2^64)} -- does not fit a signed long and would trip {@code getLongValue}. Float and
         * string tokens go through the same {@link DeclaredTypeCoercions#coerceToUnsignedLong} scalar the CSV
         * and columnar readers use, so truncation-toward-zero and the {@code [0, 2^64-1]} range check are
         * identical across every format. A bad value fails the cell through the error policy; only a
         * cross-kind token (a boolean in a numeric column) takes the drift path.
         */
        private void decodeUnsignedLongValue(JsonParser parser, JsonToken token, boolean inArray) throws IOException {
            if (token == JsonToken.VALUE_NUMBER_INT) {
                try {
                    long encoded = DeclaredTypeCoercions.coerceToUnsignedLong(parser.getBigIntegerValue());
                    ((LongBlock.Builder) blockBuilder).appendLong(encoded);
                } catch (IllegalArgumentException | InputCoercionException e) {
                    coercionFailure(blockBuilder, parser, inArray, DataType.UNSIGNED_LONG);
                }
            } else if (token == JsonToken.VALUE_NUMBER_FLOAT || token == JsonToken.VALUE_STRING) {
                try {
                    long encoded = DeclaredTypeCoercions.coerceToUnsignedLong(parser.getValueAsString());
                    ((LongBlock.Builder) blockBuilder).appendLong(encoded);
                } catch (IllegalArgumentException e) {
                    // coerceToUnsignedLong signals every bad token with an IllegalArgumentException (its range guard,
                    // the ArithmeticException remap, and the NumberFormatException subclass from BigDecimal); unlike
                    // strictParseBoolean it never throws InvalidArgumentException, so one catch clause covers it.
                    coercionFailure(blockBuilder, parser, inArray, DataType.UNSIGNED_LONG);
                }
            } else {
                crossKindDrift(parser, inArray, DataType.UNSIGNED_LONG);
            }
        }

        private void decodeDoubleValue(JsonParser parser, JsonToken token, boolean inArray) throws IOException {
            if (token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT) {
                try {
                    ((DoubleBlock.Builder) blockBuilder).appendDouble(parser.getDoubleValue());
                } catch (InputCoercionException e) {
                    coercionFailure(blockBuilder, parser, inArray, DataType.DOUBLE);
                }
            } else if (token == JsonToken.VALUE_STRING) {
                try {
                    // Double.parseDouble accepts NaN/Infinity — an external read preserves the IEEE value the
                    // file holds, matching the native columnar double read and CSV (see DeclaredTypeCoercions).
                    ((DoubleBlock.Builder) blockBuilder).appendDouble(Double.parseDouble(parser.getValueAsString()));
                } catch (NumberFormatException e) {
                    coercionFailure(blockBuilder, parser, inArray, DataType.DOUBLE);
                }
            } else {
                crossKindDrift(parser, inArray, DataType.DOUBLE); // boolean in a double column: unsupported cross-kind drift
            }
        }

        private void decodeDatetimeValue(JsonParser parser, JsonToken token, boolean inArray) throws IOException {
            if (declaredFormatter != null
                && (token == JsonToken.VALUE_STRING || token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT)) {
                // A declared `format` is authoritative and OVERRIDES the numeric-epoch shortcut, exactly as
                // CsvFormatReader.tryParseDatetime does (declaredFormatters win over looksNumeric): a column
                // declared {datetime, format:"yyyyMMdd"} reads the token 20260101 as 2026-01-01, NOT as epoch
                // millis, and {datetime, format:"epoch_second"} reads 1704067200.5 as fractional seconds. Parses
                // through the shared DeclaredTypeCoercions.parseDatetimeMillis — the SAME string->datetime
                // conversion the columnar readers use — so identical bytes + declared format yield the same
                // instant across every format. getValueAsString returns the token's source text verbatim; a
                // token the format cannot parse (e.g. a scientific-notation float like 1.7E9 under
                // epoch_second) fails per value through the read's error policy, never silently.
                try {
                    ((LongBlock.Builder) blockBuilder).appendLong(
                        DeclaredTypeCoercions.parseDatetimeMillis(parser.getValueAsString(), declaredFormatter)
                    );
                } catch (IllegalArgumentException | InvalidArgumentException | DateTimeException e) {
                    coercionFailure(blockBuilder, parser, inArray, DataType.DATETIME);
                }
            } else if (token == JsonToken.VALUE_NUMBER_INT) {
                // No declared format: a JSON number is epoch milliseconds, matching the columnar long->datetime
                // fused reinterpret (supports(LONG, DATETIME) is true). This is a genuine improvement over the
                // old file-level path, which silently null-filled an epoch number.
                try {
                    ((LongBlock.Builder) blockBuilder).appendLong(parser.getLongValue());
                } catch (InputCoercionException e) {
                    coercionFailure(blockBuilder, parser, inArray, DataType.DATETIME);
                }
            } else if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                // No declared format: a fractional JSON number is epoch milliseconds and rounds to the nearest
                // milli, matching the ::datetime semantic (ToDatetime maps DOUBLE via safeDoubleToLong) and the
                // columnar double->datetime coercion (supports(DOUBLE, DATETIME) is true).
                try {
                    ((LongBlock.Builder) blockBuilder).appendLong(DataTypeConverter.safeDoubleToLong(parser.getDoubleValue()));
                } catch (IllegalArgumentException | InvalidArgumentException | DateTimeException e) {
                    coercionFailure(blockBuilder, parser, inArray, DataType.DATETIME);
                }
            } else if (token == JsonToken.VALUE_STRING) {
                // No declared format: parse with the file-level formatter (STRICT_DATE_OPTIONAL_TIME by default).
                try {
                    ((LongBlock.Builder) blockBuilder).appendLong(datetimeFormatter.parseMillis(parser.getValueAsString()));
                } catch (IllegalArgumentException | InvalidArgumentException | DateTimeException e) {
                    coercionFailure(blockBuilder, parser, inArray, DataType.DATETIME);
                }
            } else {
                // a boolean (or a non-scalar) in a datetime column: unsupported cross-kind drift
                crossKindDrift(parser, inArray, DataType.DATETIME);
            }
        }

        /**
         * The {@code date_nanos} twin of {@link #decodeDatetimeValue}, one rail down — mirroring
         * {@code CsvFormatReader.tryParseDateNanos} exactly:
         * <ul>
         *   <li>a declared {@code format} is authoritative and OVERRIDES the numeric-epoch shortcut, exactly as
         *       the datetime arm above (declared formatters win over token kind);</li>
         *   <li>a numeric token without one is epoch <b>nanoseconds</b> — the declared type names the numeric
         *       unit ({@code datetime} = millis, {@code date_nanos} = nanos; see {@code DeclaredTypeCoercions}).
         *       A negative epoch has no {@code date_nanos} representation, so it fails the cell through the
         *       error policy rather than ever emitting a negative nanos long;</li>
         *   <li>a string token without one parses with the file-level {@link #datetimeFormatter} — the same
         *       rail the datetime arm and CSV use ({@code strict_date_optional_time} by default, which parses
         *       nanosecond fractions) — but through {@code dateNanosToLong} so the instant lands in nanos.</li>
         * </ul>
         * Every parse arm goes through {@link EsqlDataTypeConverter#dateNanosToLong}, the SAME string -&gt;
         * date_nanos conversion the columnar declared coercion and CSV use, so identical bytes with an
         * identical declared format yield the same instant across every format. A boolean or a fractional
         * number is an unsupported cross-kind drift, matching the datetime arm.
         */
        private void decodeDateNanosValue(JsonParser parser, JsonToken token, boolean inArray) throws IOException {
            if (declaredFormatter != null
                && (token == JsonToken.VALUE_STRING || token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT)) {
                // The unit rule, mirroring the datetime arm: a declared format names the unit / parse dialect, so a
                // fractional token is meaningful through it (epoch_second reads 1704067200.5 as sub-second precision,
                // which date_nanos can actually represent). Without a format a fractional token stays cross-kind drift
                // below — a fraction of a nanosecond has no meaning, nanos being the type's finest unit.
                try {
                    ((LongBlock.Builder) blockBuilder).appendLong(
                        EsqlDataTypeConverter.dateNanosToLong(parser.getValueAsString(), declaredFormatter)
                    );
                } catch (IllegalArgumentException | InvalidArgumentException | DateTimeException e) {
                    coercionFailure(blockBuilder, parser, inArray, DataType.DATE_NANOS);
                }
            } else if (token == JsonToken.VALUE_NUMBER_INT) {
                try {
                    long nanos = parser.getLongValue();
                    if (nanos < 0) {
                        // pre-epoch: no date_nanos representation — per-cell failure, never a negative nanos long
                        coercionFailure(blockBuilder, parser, inArray, DataType.DATE_NANOS);
                    } else {
                        ((LongBlock.Builder) blockBuilder).appendLong(nanos);
                    }
                } catch (InputCoercionException e) {
                    coercionFailure(blockBuilder, parser, inArray, DataType.DATE_NANOS); // beyond-long epoch: a real value error
                }
            } else if (token == JsonToken.VALUE_STRING) {
                try {
                    ((LongBlock.Builder) blockBuilder).appendLong(
                        EsqlDataTypeConverter.dateNanosToLong(parser.getValueAsString(), datetimeFormatter)
                    );
                } catch (IllegalArgumentException | InvalidArgumentException | DateTimeException e) {
                    coercionFailure(blockBuilder, parser, inArray, DataType.DATE_NANOS);
                }
            } else {
                // a boolean, or a fractional number, in a date_nanos column: unsupported cross-kind drift
                crossKindDrift(parser, inArray, DataType.DATE_NANOS);
            }
        }

        /**
         * Decodes a declared {@code ip} column. A {@code VALUE_STRING} is parsed and encoded to the 16-byte
         * {@link InetAddressPoint} form (matching {@code CsvFormatReader.tryParseIp} so identical bytes yield the
         * same value across formats); a string that is not a valid IP is a {@link #coercionFailure}. A cross-kind
         * non-string token routes through {@link #crossKindDrift} to the same policy sink.
         */
        private void decodeIpValue(JsonParser parser, JsonToken token, boolean inArray) throws IOException {
            if (token == JsonToken.VALUE_STRING) {
                try {
                    ((BytesRefBlock.Builder) blockBuilder).appendBytesRef(
                        new BytesRef(InetAddressPoint.encode(InetAddresses.forString(parser.getValueAsString())))
                    );
                } catch (IllegalArgumentException e) {
                    coercionFailure(blockBuilder, parser, inArray, DataType.IP);
                }
            } else {
                // a boolean or a number in an ip column: unsupported cross-kind drift
                crossKindDrift(parser, inArray, DataType.IP);
            }
        }

        /**
         * Handles a scalar value that cannot be coerced into a column's declared type — a string that is not a
         * number for a numeric column, a non-{@code true}/{@code false} token for a boolean column, a number that
         * overflows the target, a string the declared date {@code format} cannot parse, or a token whose JSON kind
         * has no coercion to the target. Routed through {@link ErrorPolicy} here rather than through the shared
         * {@link DeclaredTypeCoercions#onCoercionFailure} the columnar readers call -- this decoder owns its own
         * warning text and budget accounting -- but to the SAME observable outcome, which is the contract that
         * matters across formats: {@link ErrorPolicy.Mode#FAIL_FAST} fails the query with an
         * actionable message; {@link ErrorPolicy.Mode#NULL_FIELD} nulls this cell only and warns; and
         * {@link ErrorPolicy.Mode#SKIP_ROW} drops the whole record and warns (both subject to the error budget). Every
         * unrepresentable cell reaches this one sink — a bad value here, or a cross-kind token
         * ({@link #crossKindDrift}) — for a DECLARED or an INFERRED column alike, so the observable outcome depends
         * only on {@code error_mode}, never on where the type came from.
         */
        private void coercionFailure(Block.Builder builder, JsonParser parser, boolean inArray, DataType target) throws IOException {
            if (rowDroppedBySkipRow) {
                // This record is already being dropped by an earlier skip_row error. Advance past this value but do
                // not double-count it: CsvFormatReader charges the error budget once per dropped row (it stops at the
                // first bad field), not once per bad field. The record's scratch is discarded, so no null-fill is
                // needed and further coercion failures on the same doomed record must not consume the budget again.
                parser.skipChildren();
                if (inArray) {
                    // Inside an array, a normal return would let the array loop call endPositionEntry with no
                    // values appended — an AssertionError. Throw so the array handler drains and cancels instead.
                    throw PoisonedPositionException.INSTANCE;
                }
                return;
            }
            String value = parser.getValueAsString();
            // Not "the declared type": this path also fires for a supported-pair failure on an INFERRED column
            // (e.g. a bad string in an inferred long), where the target type was not declared.
            String base = "column ["
                + name
                + "] at line ["
                + totalRowCount
                + "]: value ["
                + value
                + "] could not be coerced to type ["
                + target.typeName()
                + "]";
            parser.skipChildren();
            if (errorPolicy.isStrict()) {
                // Mirror CsvFormatReader.onRowErrorImpl's field-error hint so the fail-fast message is actionable,
                // and its client-class exception so an unrepresentable value is a 400 rather than a 500.
                throw new ParsingException(
                    Source.EMPTY,
                    "{}",
                    base + "; set error_mode=null_field (or skip_row) to null-fill/skip and warn instead of failing"
                );
            }
            // A value coercion failure under skip_row drops the whole record (matching CsvFormatReader and the
            // Mode.SKIP_ROW "drop the entire bad row" contract); null_field keeps the record and nulls this one cell.
            // Both warn. crossKindDrift routes here too, for declared and inferred columns alike, so every
            // unrepresentable cell drops under skip_row uniformly.
            boolean skipRow = errorPolicy.mode() == ErrorPolicy.Mode.SKIP_ROW;
            String message = base + (skipRow ? " — this record is skipped" : " — this record's [" + name + "] is null");
            if (inArray == false) {
                builder.appendNull();
            }
            if (skipRow) {
                rowDroppedBySkipRow = true;
            }
            chargeErrorBudget();
            skipWarnings.add(message);
            checkErrorBudgetOrThrow();
            logger.log(errorPolicy.logErrors() ? Level.INFO : Level.DEBUG, message);
            if (inArray) {
                // Inside an array: throw to signal that the whole position must be nulled.
                // The array decode loop catches PoisonedPositionException, drains remaining elements,
                // and calls cancelAndNullPositionEntry to roll back any good elements already appended.
                throw PoisonedPositionException.INSTANCE;
            }
        }

    }

    /**
     * Thrown by {@link BlockDecoder#coercionFailure} when a value inside a JSON array fails coercion,
     * to signal that the entire array position must be nulled. Caught by the array decode loop in
     * {@link BlockDecoder#decodeValue}, which drains remaining elements and calls
     * {@link BlockDecoder#cancelAndNullPositionEntry}. Propagates through
     * {@link BlockDecoder#decodeObject} (which drains remaining fields and re-throws) so that a
     * failure inside a nested object also cancels the enclosing array position.
     * <p>
     * Uses a static singleton with a suppressed stack trace to keep the throw/catch overhead minimal
     * on the failure path, since no stack context is needed — the error details are recorded by
     * {@link BlockDecoder#coercionFailure} before throwing.
     */
    private static final class PoisonedPositionException extends RuntimeException {
        static final PoisonedPositionException INSTANCE = new PoisonedPositionException();

        private PoisonedPositionException() {
            super(null, null, true, false);
        }
    }
}
