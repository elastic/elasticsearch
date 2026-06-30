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
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.exc.InputCoercionException;
import com.fasterxml.jackson.core.io.JsonEOFException;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ConstantNullBlock;
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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.SyntheticColumns;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

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
     * it, so a byte-array whose whole length is {@code <= max_record_size} needs no enforcement at all.
     */
    private final int sourceDataLength;
    /**
     * Absolute offset (within {@link #sourceBytes}) where {@link #parser}'s input slice starts;
     * tracked because {@code JsonParser.getCurrentLocation().getByteOffset()} is relative to the
     * slice the parser was created over, not to the underlying byte array. Updated each time
     * {@link #recoverFromParseException} restarts the parser at a later offset.
     */
    private int parserSliceStart;
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
     * Per-record {@code max_record_size} byte cap. Enforced inside the decode loop on the same pass
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

    /** Page block layout: index {@code i} corresponds to {@code projectedAttributes().get(i)}. */
    List<Attribute> projectedAttributes() {
        return projectedAttributes;
    }

    // What blocks got a value on the current line? Needed because Block.Builder doesn't provide
    // the number of positions that were added.
    private final BitSet blockTracker;
    private final ErrorPolicy errorPolicy;
    private final SkipWarnings skipWarnings;
    private final NdJsonReaderCounters counters;
    private long totalRowCount;
    private long errorCount;
    private final DateFormatter datetimeFormatter;

    /** Number of malformed records observed during decoding (lenient policies swallow these). */
    long errorCount() {
        return errorCount;
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
            NdJsonUtils.JSON_FACTORY
        );
    }

    /**
     * Buffered-bytes constructor for the streaming-parallel path: {@code data[offset .. offset+length)}
     * is the entire input. Recovery from {@link JsonParseException} stays inside the byte array
     * (no buffered-bytes shuttling through {@link NdJsonUtils#moveToNextLine}) by scanning for the
     * next {@code '\n'} from the parser's current byte offset.
     */
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
            NdJsonUtils.JSON_FACTORY
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
            factory
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
        JsonFactory factory
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
        this.skipWarnings = SkipWarnings.of(
            errorPolicy,
            "NDJSON read from ["
                + sourceLocation
                + "] encountered parse errors handled per policy (policy: "
                + errorPolicy.modeName()
                + "); affected rows are listed below"
        );

        List<Attribute> fullSchema = attributes;
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

        this.decoder = prepareSchema(projectedAttributes, fullSchema);
        this.batchSize = batchSize;
        this.blockFactory = blockFactory;
        this.projectedAttributes = projectedAttributes;
        this.blockTracker = new BitSet(projectedAttributes.size());
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
     */
    private void onNdjsonLineParseError(JsonParseException e, long logicalRowIndex, String phaseLabel) {
        if (errorPolicy.isStrict()) {
            throw new EsqlIllegalArgumentException(e, "Malformed NDJSON [{}]: {}", phaseLabel, e.getOriginalMessage());
        }
        errorCount++;
        skipWarnings.add(
            (e instanceof JsonEOFException ? "Truncated" : "Malformed")
                + " NDJSON at logical row ["
                + logicalRowIndex
                + "] ("
                + phaseLabel
                + "): "
                + e.getOriginalMessage()
        );
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
            throw new EsqlIllegalArgumentException(
                "NDJSON error budget exceeded: [{}] errors in [{}] rows, maximum allowed is [{}] errors or [{}] ratio",
                errorCount,
                totalRowCount,
                errorPolicy.maxErrors(),
                errorPolicy.maxErrorRatio()
            );
        }
        logger.log(
            errorPolicy.logErrors() ? Level.INFO : Level.DEBUG,
            LoggerMessageFormat.format(
                "{} NDJSON at logical row [{}] ({}): {}",
                e instanceof JsonEOFException ? "Truncated" : "Malformed",
                logicalRowIndex,
                phaseLabel,
                e.getOriginalMessage()
            )
        );
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
     * Sets the per-record {@code max_record_size} cap (in bytes). Must be called before the first
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
     * Whether the per-record {@code max_record_size} check runs in the decode loop. False on the
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
     * Parser byte offset relative to its current slice. Stable to subtract between two points within
     * a single record's decode (no recovery happens between {@code nextToken} and a successful
     * {@code decodeObject}), so {@code endOffset - startOffset} is the record's parsed JSON span.
     */
    private long parserSliceByteOffset() {
        return parser.getCurrentLocation().getByteOffset();
    }

    /**
     * Throws the strict-policy {@code max_record_size} failure for a record whose parsed span is
     * {@code spanBytes}. Shares {@link NdJsonRecordSplitter}'s {@code NDJSON line exceeded max_record_size [N]}
     * prefix so the user-facing wording is consistent regardless of which layer detects the overflow, and
     * appends the decode-time span for diagnostics.
     */
    private IOException recordTooLarge(long spanBytes) {
        return new IOException("NDJSON line exceeded max_record_size [" + maxRecordBytes + "]: spans at least [" + spanBytes + "] bytes");
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
        // Setting up builders may trip the circuit breaker. Make sure they're all always closed
        try {
            decoder.setupBuilders(blockBuilders);
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
     * {@link ErrorPolicy.Mode#FAIL_FAST}: abort on the first {@link JsonParseException} on a line
     * (no recovery, no scratch-row path).
     */
    private Page decodePageFailFast(Block.Builder[] blockBuilders) throws IOException {
        int lineCount = 0;
        while (lineCount < batchSize) {
            try {
                if (parser.nextToken() == null) {
                    break; // End of stream
                }
            } catch (JsonParseException e) {
                totalRowCount++;
                onNdjsonLineParseError(e, totalRowCount, "nextToken");
            }

            totalRowCount++;
            this.blockTracker.clear();
            // Capture the record's start offset before decodeObject advances the parser. The slice-relative
            // offset feeds the max_record_size span check; the file-global offset feeds _rowPosition.
            boolean trackOffset = capEnforced || rowPositionSlot >= 0;
            long startSliceOffset = trackOffset ? parserSliceByteOffset() : 0L;
            long recordOffset = trackOffset ? recordFileOffset(startSliceOffset) : 0L;

            try {
                decoder.decodeObject(parser, false);
            } catch (JsonParseException e) {
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

            lineCount++;
            for (int i = 0; i < blockBuilders.length; i++) {
                if (blockTracker.get(i) == false) {
                    blockBuilders[i].appendNull();
                }
            }
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
            try {
                if (parser.nextToken() == null) {
                    break; // End of stream
                }
            } catch (JsonParseException e) {
                totalRowCount++;
                onNdjsonLineParseError(e, totalRowCount, "nextToken");
                recoverFromParseException(parser);
                continue;
            }

            totalRowCount++;
            this.blockTracker.clear();
            // Capture before decodeObject / recovery advance the parser. The slice-relative offset feeds
            // the max_record_size span check; the file-global offset feeds _rowPosition and truncation.
            boolean trackOffset = capEnforced || rowPositionSlot >= 0;
            long startSliceOffset = trackOffset ? parserSliceByteOffset() : 0L;
            long recordOffset = trackOffset ? recordFileOffset(startSliceOffset) : 0L;

            try {
                decoder.setupBuilders(rowScratch);
                try {
                    decoder.decodeObject(parser, false);
                } catch (JsonParseException e) {
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
                                    + "]: a record exceeded max_record_size ["
                                    + maxRecordBytes
                                    + "]; results are partial"
                            );
                            truncated = true;
                            truncatedAtByte = recordOffset;
                            break;
                        }
                        // Byte-array: the oversized record is fully buffered, so drop it and keep decoding.
                        continue;
                    }
                }
                if (rowPositionSlot >= 0) {
                    ((LongBlock.Builder) rowScratch[rowPositionSlot]).appendLong(recordOffset);
                    blockTracker.set(rowPositionSlot);
                }
                for (int i = 0; i < rowScratch.length; i++) {
                    if (blockTracker.get(i) == false) {
                        rowScratch[i].appendNull();
                    }
                }
                appendDecodedScratchRow(blockBuilders, rowScratch);
            } finally {
                Releasables.close(rowScratch);
            }

            lineCount++;
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

    // Prepare the tree of property decoders and return the root decoder.
    private BlockDecoder prepareSchema(List<Attribute> projected, List<Attribute> fullSchema) {
        BlockDecoder root = new BlockDecoder();
        int idx = 0;
        for (var attribute : projected) {
            String name = attribute.name();
            BlockDecoder decoder;
            if (hasDottedPrefixConflict(name, fullSchema)) {
                // CSV-style flat keys such as "languages.long" are single JSON field names; they cannot be reached
                // via a nested "languages" object when "languages" is also a scalar column.
                if (root.children == null) {
                    root.children = new HashMap<>();
                }
                decoder = root.children.computeIfAbsent(name, k -> new BlockDecoder());
            } else {
                decoder = root;
                var path = name.split("\\.");
                for (var part : path) {
                    if (decoder.children == null) {
                        decoder.children = new HashMap<>();
                    }
                    decoder = decoder.children.computeIfAbsent(part, k -> new BlockDecoder());
                }
            }
            decoder.setAttribute(attribute, idx);
            idx++;
        }
        return root;
    }

    /**
     * Whether {@code name} is a dotted field that shares a prefix with another attribute (e.g. {@code languages}
     * vs {@code languages.long}). In that case the NDJSON uses a single field name equal to the full attribute name.
     */
    private static boolean hasDottedPrefixConflict(String name, List<Attribute> attributes) {
        for (var other : attributes) {
            String o = other.name();
            if (name.equals(o) == false && name.startsWith(o + ".")) {
                return true;
            }
        }
        return false;
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

    // ---------------------------------------------------------------------------------------------
    // A tree of decoders. Avoids path reconstruction when traversing nested objects.
    private class BlockDecoder {
        @Nullable
        DataType dataType;
        String name;
        int blockIdx;
        Block.Builder blockBuilder;
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

        void setAttribute(Attribute attribute, int blockIdx) {
            this.dataType = attribute.dataType();
            this.name = attribute.name();
            this.blockIdx = blockIdx;
        }

        // Builders setup independently as we need to create new ones for each page.
        void setupBuilders(Block.Builder[] blockBuilders) {
            if (dataType != null) {
                blockBuilder = switch (dataType) {
                    // Keep in sync with NdJsonSchemaInferrer.inferValueSchema
                    case BOOLEAN -> blockFactory.newBooleanBlockBuilder(batchSize);
                    case NULL -> new ConstantNullBlock.Builder(blockFactory);
                    case INTEGER -> blockFactory.newIntBlockBuilder(batchSize);
                    case LONG -> blockFactory.newLongBlockBuilder(batchSize);
                    case DOUBLE -> blockFactory.newDoubleBlockBuilder(batchSize);
                    case KEYWORD -> blockFactory.newBytesRefBlockBuilder(batchSize);
                    case DATETIME -> blockFactory.newLongBlockBuilder(batchSize); // milliseconds since epoch
                    default -> throw new IllegalArgumentException("Unsupported data type: " + dataType);
                };
                blockBuilders[blockIdx] = blockBuilder;
            }

            if (children != null) {
                for (var child : children.values()) {
                    child.setupBuilders(blockBuilders);
                }
            }
        }

        private void decodeObject(JsonParser parser, boolean inArray) throws IOException {
            if (parser.currentToken() != JsonToken.START_OBJECT) {
                throw new NdJsonParseException(parser, "Expected JSON object");
            }
            String fieldName;
            while ((fieldName = parser.nextFieldName()) != null) {
                var childDecoder = lookupChild(fieldName);
                parser.nextToken();
                if (childDecoder == unprojected) {
                    // Unknown/unprojected field: advance to its value then skip (no decode).
                    // For string values nextFieldName() uses _skipString() internally on the next
                    // call, so we avoid _finishString2 for non-projected string fields.
                    parser.skipChildren();
                } else {
                    childDecoder.decodeValue(parser, inArray);
                }
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
            BlockDecoder toCache = resolved == null ? unprojected : resolved;
            if (identityCache.size() < maxEntries) {
                identityCache.put(fieldName, toCache);
            }
            return toCache;
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
         * @param includeChildren when {@code true}, also begins MV entries on child decoders. Use this only for JSON
         *        arrays of objects (e.g. {@code [{"a":1},{"a":2}]}) where every child column shares one MV slot per
         *        element. For arrays of primitives (e.g. {@code salary_change} as doubles while {@code salary_change.int}
         *        is a separate top-level field), {@code false} so children are not opened for values they will never
         *        receive from this array.
         */
        private void beginPositionEntry(boolean includeChildren) {
            // We may have DataType.NULL for unknown columns. And NullBlock.Builder throws on beginPositionEntry()
            if (blockBuilder != null && dataType != DataType.NULL) {
                blockBuilder.beginPositionEntry();
            }
            if (includeChildren && children != null) {
                for (var child : children.values()) {
                    child.beginPositionEntry(includeChildren);
                }
            }
        }

        private void endPositionEntry(boolean includeChildren) {
            if (blockBuilder != null && dataType != DataType.NULL) {
                blockBuilder.endPositionEntry();
            }
            if (includeChildren && children != null) {
                for (var child : children.values()) {
                    child.endPositionEntry(includeChildren);
                }
            }
        }

        /**
         * An empty JSON array {@code []} must not run {@link Block.Builder#beginPositionEntry()} with no values:
         * {@link org.elasticsearch.compute.data.AbstractBlockBuilder#endPositionEntry()} asserts on empty multi-value
         * slots. Treat {@code []} like a missing field for every leaf column under this decoder subtree.
         */
        private void appendNullsForEmptyArray() {
            if (blockBuilder != null) {
                if (dataType != DataType.NULL) {
                    blockTracker.set(blockIdx);
                    blockBuilder.appendNull();
                }
            } else if (children != null) {
                for (var child : children.values()) {
                    child.appendNullsForEmptyArray();
                }
            }
        }

        private void decodeValue(JsonParser parser, boolean inArray) throws IOException {
            JsonToken token = parser.currentToken();

            if (dataType == DataType.NULL) {
                // Don't do anything. We must do a single appendNull() on null blocks, this will be done
                // at the end of decodePage() when we check that all blocks have moved forward.
                parser.skipChildren();
                return;
            }

            if (token == JsonToken.START_ARRAY) {
                // Start a multi-value entry on this decoder and all its children (nested arrays are flattened).
                // Note: the `inArray` flag is needed because blockBuilder.beginPositionEntry() is not idempotent.
                // Calling it twice implicitly calls endPositionEntry().
                if (!inArray) {
                    JsonToken first = parser.nextToken();
                    if (first == JsonToken.END_ARRAY) {
                        appendNullsForEmptyArray();
                        return;
                    }
                    boolean includeChildren = first == JsonToken.START_OBJECT;
                    beginPositionEntry(includeChildren);
                    decodeValue(parser, true);
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        decodeValue(parser, true);
                    }
                    endPositionEntry(includeChildren);
                    return;
                }
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    decodeValue(parser, true);
                }
                return;
            }

            if (token == JsonToken.START_OBJECT) {
                decodeObject(parser, inArray);
                return;
            }

            blockTracker.set(blockIdx);
            if (token == JsonToken.VALUE_NULL) {
                // Nulls in arrays aren't supported. Furthermore, appendNull will implicitly call endPositionEntry()
                if (inArray == false) {
                    blockBuilder.appendNull();
                }
                return;
            }

            switch (dataType) {
                case BOOLEAN -> {
                    if (token == JsonToken.VALUE_TRUE) {
                        ((BooleanBlock.Builder) blockBuilder).appendBoolean(true);
                    } else if (token == JsonToken.VALUE_FALSE) {
                        ((BooleanBlock.Builder) blockBuilder).appendBoolean(false);
                    } else {
                        unexpectedValue(blockBuilder, parser, inArray);
                    }
                }
                case NULL -> {
                    // NULL handled above
                    unexpectedValue(blockBuilder, parser, inArray);
                }
                case INTEGER -> {
                    if (token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT) {
                        try {
                            ((IntBlock.Builder) blockBuilder).appendInt(parser.getIntValue());
                        } catch (InputCoercionException e) {
                            unexpectedValue(blockBuilder, parser, inArray);
                        }
                    } else {
                        unexpectedValue(blockBuilder, parser, inArray);
                    }
                }
                case LONG -> {
                    if (token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT) {
                        try {
                            ((LongBlock.Builder) blockBuilder).appendLong(parser.getLongValue());
                        } catch (InputCoercionException e) {
                            unexpectedValue(blockBuilder, parser, inArray);
                        }
                    } else {
                        unexpectedValue(blockBuilder, parser, inArray);
                    }
                }
                case DOUBLE -> {
                    if (token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT) {
                        try {
                            ((DoubleBlock.Builder) blockBuilder).appendDouble(parser.getDoubleValue());
                        } catch (InputCoercionException e) {
                            unexpectedValue(blockBuilder, parser, inArray);
                        }
                    } else {
                        unexpectedValue(blockBuilder, parser, inArray);
                    }
                }
                case DATETIME -> {
                    try {
                        var millis = datetimeFormatter.parseMillis(parser.getValueAsString());
                        ((LongBlock.Builder) blockBuilder).appendLong(millis);
                    } catch (Exception e) {
                        unexpectedValue(blockBuilder, parser, inArray);
                    }
                }
                case KEYWORD -> {
                    var chars = CharBuffer.wrap(parser.getTextCharacters(), parser.getTextOffset(), parser.getTextLength());
                    ((BytesRefBlock.Builder) blockBuilder).appendBytesRef(toScratchBytesRef(chars));
                }
                default -> throw new IllegalArgumentException("Unsupported data type: " + dataType);
            }
        }

        private void unexpectedValue(Block.Builder builder, JsonParser parser, boolean inArray) throws IOException {
            // Append a null and log the problem
            if (inArray == false) {
                // See previous comment about nulls and arrays
                builder.appendNull();
            }

            logger.debug("Unexpected token type: {} for attribute: {} at {}", parser.currentToken(), name, parser.getTokenLocation());
            // Ignore any children to keep reading other values
            parser.skipChildren();
        }
    }
}
