/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.ExternalFailures;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.cache.ColumnStatsAccumulator;
import org.elasticsearch.xpack.esql.datasources.cache.CountingInputStream;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStats;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStatsCapture;
import org.elasticsearch.xpack.esql.datasources.cache.TextFormatStats;
import org.elasticsearch.xpack.esql.datasources.spi.BufferingPageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;

/**
 * Iterator that reads NDJSON lines and produces ESQL Pages.
 *
 * <p>When {@code skipFirstLine} is true (for non-first splits), discards bytes up to
 * and including the first newline before starting to parse. This implements the standard
 * line-alignment protocol for split boundary handling.
 *
 * <p>When {@code resolvedAttributes} is provided, uses those instead of inferring schema
 * from the split data, avoiding the risk of schema divergence across splits.
 */
final class NdJsonPageIterator extends BufferingPageIterator {

    private static final Logger logger = LogManager.getLogger(NdJsonPageIterator.class);

    private final NdJsonPageDecoder pageDecoder;
    private final int rowLimit;
    private long rowsEmitted;
    private boolean endOfFile = false;
    /** Non-null iff the iterator is eligible to populate {@link ExternalStats} on close (whole-file read). */
    private final StorageObject cacheableObject;
    /** Stream-side byte counter for stream-only sources (length() throws). Null for byte-array fast path. */
    private final CountingInputStream byteCounter;
    /** True only when the decoder returned a natural EOF (not on {@code rowLimit} truncation). */
    private boolean naturallyExhausted = false;
    /** Lazily built once the first page emits, so we use the decoder's resolved projected attributes. */
    private ColumnStatsAccumulator columnStats;
    /** Snapshotted at byte-array fast-path init; the streaming path queries {@link #byteCounter}. */
    private final long byteArrayBytesRead;

    /**
     * Storage objects up to this size are eagerly slurped into a {@code byte[]} so the decoder can
     * use Jackson's {@code createParser(byte[])} fast path instead of the {@code InputStream}
     * dispatch. Streaming-parallel chunks are ~1 MiB, single-file reads are usually well under
     * this; very large files fall back to streaming so they do not pin a multi-hundred-MiB array.
     */
    static final int BYTE_ARRAY_FAST_PATH_MAX_SIZE = 16 * 1024 * 1024;

    /** Pinned at iterator open; closes the open-vs-close mtime-race window for the cache key. */
    private final long pinnedMtimeMillis;
    /** Computes the cache fingerprint from the FULL file schema at close time — must match {@code metadata()}'s input. */
    private final Function<List<Attribute>, String> fingerprinter;
    /** Full file schema as passed by the planner. Non-null on the wholeFileRead path; used for fingerprint at close. */
    private final List<Attribute> fingerprintSchema;
    private final String sourceLocation;
    /** True for parallel-parsing chunks — close-time publish carries the partial-chunk marker. */
    private final boolean chunkMode;

    NdJsonPageIterator(
        StorageObject object,
        List<String> projectedColumns,
        int batchSize,
        int rowLimit,
        BlockFactory blockFactory,
        boolean skipFirstLine,
        boolean trimLastPartialLine,
        List<Attribute> resolvedAttributes,
        ErrorPolicy errorPolicy,
        StorageObject cacheableObject,
        long pinnedMtimeMillis,
        Function<List<Attribute>, String> fingerprinter,
        boolean chunkMode,
        NdJsonReaderCounters counters,
        int maxRecordBytes
    ) throws IOException {
        Check.isTrue(errorPolicy != null, "errorPolicy must not be null");
        Check.isTrue(counters != null, "counters must not be null");
        this.cacheableObject = cacheableObject;
        this.pinnedMtimeMillis = pinnedMtimeMillis;
        this.fingerprinter = fingerprinter;
        this.fingerprintSchema = resolvedAttributes;
        this.sourceLocation = object.path().toString();
        this.chunkMode = chunkMode;
        InputStream inputStream = object.newStream();
        NdJsonRecordSplitter recordSplitter = new NdJsonRecordSplitter(maxRecordBytes);
        if (skipFirstLine) {
            skipToNextLine(inputStream, recordSplitter);
        }
        if (trimLastPartialLine) {
            inputStream = trimLastPartialLine(inputStream, errorPolicy, sourceLocation, recordSplitter);
        }
        this.rowLimit = rowLimit;
        if (canUseByteArrayFastPath(object)) {
            // Strict policy: wrap with the byte-cap stream so an oversized line trips during the single
            // readAllBytes() pull — no second walk over the buffer like the previous enforceMaxRecordBytes
            // pre-scan. Lenient policy: that wrap can't preserve the row-drop contract (an IOException
            // mid-bulk-read leaves the underlying stream at an undefined offset), so we keep the
            // post-read filtering pass — bounded to the byte-array fast path's ≤16 MiB segments — to
            // drop oversized lines while leaving the rest of the file intact. Splitter-side enforcement
            // (NdJsonRecordSplitter.findLastRecordBoundary at split discovery time) still covers parallel
            // chunks; this branch protects whole-file and byte-range macro-split reads that bypass splitting.
            byte[] data;
            if (errorPolicy.isStrict()) {
                try (InputStream toClose = new NdJsonRecordCappingInputStream(inputStream, recordSplitter)) {
                    data = toClose.readAllBytes();
                }
            } else {
                try (InputStream toClose = inputStream) {
                    data = toClose.readAllBytes();
                }
                data = filterOversizedRecords(data, recordSplitter);
            }
            this.byteCounter = null;
            this.byteArrayBytesRead = data.length;
            this.pageDecoder = new NdJsonPageDecoder(
                data,
                0,
                data.length,
                resolvedAttributes,
                projectedColumns,
                batchSize,
                blockFactory,
                errorPolicy,
                this.sourceLocation,
                counters
            );
        } else {
            // Wrap on the streaming path so close-time bytesRead works for stream-only sources
            // (bzip2 / zstd-streamed) whose length() throws.
            CountingInputStream counted = new CountingInputStream(inputStream);
            this.byteCounter = counted;
            this.byteArrayBytesRead = -1;
            this.pageDecoder = new NdJsonPageDecoder(
                counted,
                resolvedAttributes,
                projectedColumns,
                batchSize,
                blockFactory,
                errorPolicy,
                this.sourceLocation,
                counters
            );
        }
    }

    /**
     * Lenient-mode byte-array post-filter: walks the freshly buffered segment once and drops every
     * record whose terminator-inclusive byte count exceeds {@code maxRecordBytes}, leaving the rest
     * of the file intact for downstream parsing. This preserves the pre-existing skip-row contract
     * for oversized NDJSON lines while still avoiding a redundant pre-scan on the strict path
     * (which goes through {@link NdJsonRecordCappingInputStream}). Bounded by the byte-array fast
     * path's segment cap, so the extra walk is a single bounded pass rather than open-ended work.
     */
    private static byte[] filterOversizedRecords(byte[] data, NdJsonRecordSplitter recordSplitter) {
        int max = recordSplitter.maxRecordBytes();
        ByteArrayOutputStream filtered = null;
        int recordStart = 0;
        for (int i = 0; i < data.length; i++) {
            byte b = data[i];
            if (b == '\n' || b == '\r') {
                int boundary = i;
                if (b == '\r' && i + 1 < data.length && data[i + 1] == '\n') {
                    boundary = ++i;
                }
                filtered = copyOrSkipRecord(data, recordStart, boundary + 1, max, filtered);
                recordStart = boundary + 1;
            }
        }
        if (recordStart < data.length) {
            filtered = copyOrSkipRecord(data, recordStart, data.length, max, filtered);
        }
        return filtered == null ? data : filtered.toByteArray();
    }

    private static ByteArrayOutputStream copyOrSkipRecord(
        byte[] data,
        int recordStart,
        int recordEnd,
        int maxRecordBytes,
        ByteArrayOutputStream filtered
    ) {
        int recordBytes = recordEnd - recordStart;
        if (recordBytes > maxRecordBytes) {
            if (filtered == null) {
                // First skip materializes the kept-prefix once; subsequent records are appended as we go.
                filtered = new ByteArrayOutputStream(data.length);
                filtered.write(data, 0, recordStart);
            }
            return filtered;
        }
        if (filtered != null) {
            filtered.write(data, recordStart, recordBytes);
        }
        return filtered;
    }

    /**
     * The byte-array fast path is available for storage objects with a known, bounded length;
     * decompressing wrappers throw {@link UnsupportedOperationException} from {@link StorageObject#length()}
     * and stay on the streaming path. Transient {@link IOException}s from {@link StorageObject#length()}
     * also fall back to streaming so a metadata hiccup does not abort an open call; the streaming
     * read will surface the same condition if the data itself is unreachable.
     */
    // package-private for testing: pins the invariant that segments above the threshold stream rather than
    // buffering the whole segment, which is what bounds per-open-segment memory under the open-segment cap.
    static boolean canUseByteArrayFastPath(StorageObject object) {
        try {
            long len = object.length();
            return len >= 0 && len <= BYTE_ARRAY_FAST_PATH_MAX_SIZE;
        } catch (UnsupportedOperationException e) {
            return false;
        } catch (IOException e) {
            // Surface the metadata hiccup at DEBUG so a transient S3 head-object failure is not
            // completely invisible during diagnosis. The streaming path will surface the same
            // condition on read if the data itself is unreachable.
            logger.debug(() -> "byte-array fast path disabled for [" + object.path() + "]: object length unavailable", e);
            return false;
        }
    }

    @Override
    public boolean hasNext() {
        if (nextPage != null) {
            return true;
        }
        if (endOfFile || isClosed()) {
            return false;
        }
        if (rowLimit != FormatReader.NO_LIMIT && rowsEmitted >= rowLimit) {
            endOfFile = true;
            return false;
        }
        try {
            nextPage = pageDecoder.decodePage();
            if (nextPage == null) {
                endOfFile = true;
                naturallyExhausted = true;
                return false;
            }
            if (rowLimit != FormatReader.NO_LIMIT) {
                long allowed = (long) rowLimit - rowsEmitted;
                if (allowed <= 0) {
                    nextPage.releaseBlocks();
                    nextPage = null;
                    endOfFile = true;
                    return false;
                }
                int positionCount = nextPage.getPositionCount();
                if (positionCount > allowed) {
                    Page sliced = nextPage.slice(0, (int) allowed);
                    nextPage.releaseBlocks();
                    nextPage = sliced;
                    endOfFile = true;
                }
                rowsEmitted += nextPage.getPositionCount();
            } else {
                rowsEmitted += nextPage.getPositionCount();
            }
            return true;
        } catch (IOException e) {
            throw ExternalFailures.surface(e, "Failed to read NDJSON page");
        }
    }

    @Override
    public Page next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }
        Page result = nextPage;
        nextPage = null;
        captureBlockStats(result);
        return result;
    }

    private void captureBlockStats(Page page) {
        if (cacheableObject == null || page.getBlockCount() == 0) {
            return;
        }
        if (columnStats == null) {
            List<Attribute> projected = pageDecoder.projectedAttributes();
            if (projected == null || projected.isEmpty()) {
                return;
            }
            columnStats = ColumnStatsAccumulator.forProjectedAttributes(projected.toArray(new Attribute[0]));
        }
        if (columnStats.isEmpty()) {
            return;
        }
        int blocks = page.getBlockCount();
        for (int i = 0; i < blocks; i++) {
            columnStats.acceptBlockAt(i, page.getBlock(i));
        }
    }

    @Override
    protected void closeInternal() throws IOException {
        // Cache only on clean whole-file drain. Runs before closing the decoder so its errorCount is still readable.
        // SKIP_ROW with parse errors in a chunk publishes a poison marker so the coordinator's reconciler
        // discards the file's merge rather than committing an under-counted COUNT(*).
        if (cacheableObject != null
            && naturallyExhausted
            && pinnedMtimeMillis >= 0
            && fingerprinter != null
            && pageDecoder.errorCount() > 0
            && chunkMode) {
            Map<String, Object> poison = new HashMap<>();
            poison.put(ExternalStats.MTIME_MILLIS_KEY, pinnedMtimeMillis);
            poison.put(ExternalStats.CHUNK_HAD_ERRORS_KEY, Boolean.TRUE);
            ExternalStatsCapture.record(sourceLocation, poison);
        }
        if (cacheableObject != null
            && naturallyExhausted
            && pageDecoder.errorCount() == 0
            && pinnedMtimeMillis >= 0
            && fingerprinter != null) {
            // Fingerprint must use the FULL file schema for parity with NdJsonFormatReader.metadata().
            // Prefer the planner-provided schema (resolvedAttributes), fall back to the decoder's
            // projected attributes only when those equal the full schema (no projection pruning).
            List<Attribute> fullSchema = fingerprintSchema != null ? fingerprintSchema : pageDecoder.projectedAttributes();
            if (fullSchema != null && fullSchema.isEmpty() == false) {
                Map<String, ExternalStats.ColumnStats> cols = columnStats == null ? Map.of() : columnStats.snapshot();
                OptionalLong bytesRead = byteCounter != null
                    ? OptionalLong.of(byteCounter.getBytesRead())
                    : (byteArrayBytesRead >= 0 ? OptionalLong.of(byteArrayBytesRead) : OptionalLong.empty());
                String fingerprint = fingerprinter.apply(fullSchema);
                ExternalStats.Stats statsRecord = new ExternalStats.Stats(rowsEmitted, bytesRead, cols);
                // Surface to thread-bound capture sink so the contribution rides back to the
                // coordinator via DriverCompletionInfo for multi-JVM warm-path consumption.
                SourceStatistics sourceStats = TextFormatStats.build(Optional.of(statsRecord), sizeInBytesFromLength(), fullSchema);
                Map<String, Object> base = new HashMap<>();
                base.put(ExternalStats.MTIME_MILLIS_KEY, pinnedMtimeMillis);
                base.put(ExternalStats.CONFIG_FINGERPRINT_KEY, fingerprint);
                if (chunkMode) {
                    base.put(ExternalStats.PARTIAL_CHUNK_KEY, Boolean.TRUE);
                }
                Map<String, Object> flat = SourceStatisticsSerializer.embedStatistics(base, sourceStats);
                ExternalStatsCapture.record(sourceLocation, flat);
            }
        }
        IOUtils.close(pageDecoder);
    }

    private OptionalLong sizeInBytesFromLength() {
        if (cacheableObject == null) {
            return OptionalLong.empty();
        }
        try {
            return OptionalLong.of(cacheableObject.length());
        } catch (IOException | UnsupportedOperationException e) {
            return OptionalLong.empty();
        }
    }

    /**
     * Reader-side half of the split-alignment protocol: drop the leading partial record that the
     * splitter's previous macro-split already emitted via finish-current-line.
     *
     * <p>Protocol cross-references (prose because these live in sibling plugin modules):
     * <ul>
     *   <li>Codec side — {@code Bzip2DecompressionCodec.BlockBoundedDecompressStream} in
     *       the {@code esql-datasource-bzip2} module emits bytes past the split boundary up to
     *       (and including) the next {@code '\n'}.</li>
     *   <li>Splitter side — {@code FileSplitProvider.tryBlockAlignedSplits} in the
     *       {@code esql} module sets the first-split vs. non-first-split markers that
     *       {@link NdJsonFormatReader#read} uses to decide whether to call this method.</li>
     * </ul>
     *
     * <p>Delegates to {@link NdJsonRecordSplitter#scanForTerminator} so LF/CRLF/CR are handled
     * uniformly; in practice the codec's finish-current-line always ends on {@code '\n'}, so
     * only the LF branch fires, but routing through one implementation removes the coupling.
     */
    static void skipToNextLine(InputStream stream) throws IOException {
        skipToNextLine(stream, new NdJsonRecordSplitter(SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES));
    }

    static void skipToNextLine(InputStream stream, NdJsonRecordSplitter recordSplitter) throws IOException {
        NdJsonRecordSplitter.LineScan scan = recordSplitter.scanForTerminator(stream);
        if (scan.consumed() == RecordSplitter.RECORD_TOO_LARGE) {
            throw recordSplitter.recordTooLargeException();
        }
    }

    /**
     * Returns a stream that exposes the same bytes as fully reading {@code in} and truncating after
     * the last {@code '\n'}, without materializing the whole stream in memory. The delegate is closed
     * when the returned stream is closed. Oversized partial lines follow {@code errorPolicy}.
     */
    static InputStream trimLastPartialLine(InputStream in, ErrorPolicy errorPolicy, String sourceLocation) {
        return trimLastPartialLine(
            in,
            errorPolicy,
            sourceLocation,
            new NdJsonRecordSplitter(SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES)
        );
    }

    static InputStream trimLastPartialLine(
        InputStream in,
        ErrorPolicy errorPolicy,
        String sourceLocation,
        NdJsonRecordSplitter recordSplitter
    ) {
        return new TrimLastPartialLineInputStream(in, errorPolicy, sourceLocation, recordSplitter);
    }
}
