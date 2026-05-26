/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStats;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.NoSuchElementException;

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
final class NdJsonPageIterator implements CloseableIterator<Page> {

    private static final Logger logger = LogManager.getLogger(NdJsonPageIterator.class);

    private final NdJsonPageDecoder pageDecoder;
    private final int rowLimit;
    private long rowsEmitted;
    private boolean endOfFile = false;
    private Page nextPage;
    /** Non-null iff the iterator is eligible to populate {@link ExternalStats} on close (whole-file read). */
    private final StorageObject cacheableObject;
    /** Stream-side byte counter for stream-only sources (length() throws). Null for byte-array fast path. */
    private final org.elasticsearch.xpack.esql.datasources.cache.CountingInputStream byteCounter;
    /** True only when the decoder returned a natural EOF (not on {@code rowLimit} truncation). */
    private boolean naturallyExhausted = false;
    /** Lazily built once the first page emits, so we use the decoder's resolved projected attributes. */
    private org.elasticsearch.xpack.esql.datasources.cache.ColumnStatsAccumulator columnStats;
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
    private final java.util.function.Function<List<Attribute>, String> fingerprinter;
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
        java.util.function.Function<List<Attribute>, String> fingerprinter,
        boolean chunkMode
    ) throws IOException {
        Check.isTrue(errorPolicy != null, "errorPolicy must not be null");
        this.cacheableObject = cacheableObject;
        this.pinnedMtimeMillis = pinnedMtimeMillis;
        this.fingerprinter = fingerprinter;
        this.fingerprintSchema = resolvedAttributes;
        this.sourceLocation = object.path().toString();
        this.chunkMode = chunkMode;
        InputStream inputStream = object.newStream();
        if (skipFirstLine) {
            skipToNextLine(inputStream);
        }
        if (trimLastPartialLine) {
            inputStream = trimLastPartialLine(inputStream, errorPolicy, this.sourceLocation);
        }
        this.rowLimit = rowLimit;
        if (canUseByteArrayFastPath(object)) {
            byte[] data;
            try (InputStream toClose = inputStream) {
                data = toClose.readAllBytes();
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
                this.sourceLocation
            );
        } else {
            // Wrap on the streaming path so close-time bytesRead works for stream-only sources
            // (bzip2 / zstd-streamed) whose length() throws.
            org.elasticsearch.xpack.esql.datasources.cache.CountingInputStream counted =
                new org.elasticsearch.xpack.esql.datasources.cache.CountingInputStream(inputStream);
            this.byteCounter = counted;
            this.byteArrayBytesRead = -1;
            this.pageDecoder = new NdJsonPageDecoder(
                counted,
                resolvedAttributes,
                projectedColumns,
                batchSize,
                blockFactory,
                errorPolicy,
                this.sourceLocation
            );
        }
    }

    /**
     * The byte-array fast path is available for storage objects with a known, bounded length;
     * decompressing wrappers throw {@link UnsupportedOperationException} from {@link StorageObject#length()}
     * and stay on the streaming path. Transient {@link IOException}s from {@link StorageObject#length()}
     * also fall back to streaming so a metadata hiccup does not abort an open call; the streaming
     * read will surface the same condition if the data itself is unreachable.
     */
    private static boolean canUseByteArrayFastPath(StorageObject object) {
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
        if (endOfFile) {
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
            throw new RuntimeException(e);
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
            columnStats = org.elasticsearch.xpack.esql.datasources.cache.ColumnStatsAccumulator.forProjectedAttributes(
                projected.toArray(new Attribute[0])
            );
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
    public void close() throws IOException {
        // Cache only on clean whole-file drain. Runs before closing the decoder so its errorCount is still readable.
        // SKIP_ROW with parse errors in a chunk publishes a poison marker so the coordinator's reconciler
        // discards the file's merge rather than committing an under-counted COUNT(*).
        if (cacheableObject != null
            && naturallyExhausted
            && pinnedMtimeMillis >= 0
            && fingerprinter != null
            && pageDecoder.errorCount() > 0
            && chunkMode) {
            java.util.Map<String, Object> poison = new java.util.HashMap<>();
            poison.put(ExternalStats.MTIME_MILLIS_KEY, pinnedMtimeMillis);
            poison.put(ExternalStats.CHUNK_HAD_ERRORS_KEY, Boolean.TRUE);
            org.elasticsearch.xpack.esql.datasources.cache.ExternalStatsCapture.record(sourceLocation, poison);
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
                java.util.Map<String, ExternalStats.ColumnStats> cols = columnStats == null ? java.util.Map.of() : columnStats.snapshot();
                java.util.OptionalLong bytesRead = byteCounter != null
                    ? java.util.OptionalLong.of(byteCounter.getBytesRead())
                    : (byteArrayBytesRead >= 0 ? java.util.OptionalLong.of(byteArrayBytesRead) : java.util.OptionalLong.empty());
                String fingerprint = fingerprinter.apply(fullSchema);
                ExternalStats.Stats statsRecord = new ExternalStats.Stats(rowsEmitted, bytesRead, cols);
                // Surface to thread-bound capture sink so the contribution rides back to the
                // coordinator via DriverCompletionInfo for multi-JVM warm-path consumption.
                org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics sourceStats =
                    org.elasticsearch.xpack.esql.datasources.cache.TextFormatStats.build(
                        java.util.Optional.of(statsRecord),
                        sizeInBytesFromLength(),
                        fullSchema
                    );
                java.util.Map<String, Object> base = new java.util.HashMap<>();
                base.put(ExternalStats.MTIME_MILLIS_KEY, pinnedMtimeMillis);
                base.put(ExternalStats.CONFIG_FINGERPRINT_KEY, fingerprint);
                if (chunkMode) {
                    base.put(ExternalStats.PARTIAL_CHUNK_KEY, Boolean.TRUE);
                }
                java.util.Map<String, Object> flat = org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer.embedStatistics(
                    base,
                    sourceStats
                );
                org.elasticsearch.xpack.esql.datasources.cache.ExternalStatsCapture.record(sourceLocation, flat);
            }
        }
        IOUtils.close(pageDecoder);
    }

    private java.util.OptionalLong sizeInBytesFromLength() {
        if (cacheableObject == null) {
            return java.util.OptionalLong.empty();
        }
        try {
            return java.util.OptionalLong.of(cacheableObject.length());
        } catch (IOException | UnsupportedOperationException e) {
            return java.util.OptionalLong.empty();
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
     * <p>Delegates to {@link NdJsonFormatReader#scanForTerminator} so LF/CRLF/CR are handled
     * uniformly; in practice the codec's finish-current-line always ends on {@code '\n'}, so
     * only the LF branch fires, but routing through one implementation removes the coupling.
     */
    static void skipToNextLine(InputStream stream) throws IOException {
        NdJsonFormatReader.scanForTerminator(stream);
    }

    /**
     * Returns a stream that exposes the same bytes as fully reading {@code in} and truncating after
     * the last {@code '\n'}, without materializing the whole stream in memory. The delegate is closed
     * when the returned stream is closed. Oversized partial lines follow {@code errorPolicy}.
     */
    static InputStream trimLastPartialLine(InputStream in, ErrorPolicy errorPolicy, String sourceLocation) {
        // TODO: thread in a centralized error counter?
        return new TrimLastPartialLineInputStream(in, errorPolicy, sourceLocation);
    }
}
