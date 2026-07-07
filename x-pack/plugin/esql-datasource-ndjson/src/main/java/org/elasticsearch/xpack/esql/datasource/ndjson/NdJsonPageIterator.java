/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.ExternalFailures;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.SyntheticColumns;
import org.elasticsearch.xpack.esql.datasources.cache.ColumnStatsAccumulator;
import org.elasticsearch.xpack.esql.datasources.cache.CountingInputStream;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStats;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStatsCapture;
import org.elasticsearch.xpack.esql.datasources.cache.StripeStatsHarvester;
import org.elasticsearch.xpack.esql.datasources.cache.TextFormatStats;
import org.elasticsearch.xpack.esql.datasources.spi.BufferingPageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StripeColumnScope;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
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
    /**
     * Stripe grid B (bytes). Positive iff this chunk attributes records to the canonical stripe grid and
     * emits one per-stripe contribution per {@code (chunk, stripe)} at close via the byte-range cover model;
     * -1 disables (whole-file read, or stripe addressing not wired). The decoder records each record's own
     * file-global start offset; the iterator attributes the page's rows to stripes by those offsets — no
     * page-capping needed.
     */
    private final long statsStripeSize;
    /**
     * The file-global offset of the FIRST byte the decoder actually reads, i.e. this chunk's absolute
     * first-byte offset plus any leading partial record dropped by {@code skipFirstLine}. Both the per-record offset
     * tracking and the close-hook byte-range cover anchor on this so that, on a split that started
     * mid-record, records are attributed to the correct canonical stripe rather than {@code skipped}
     * bytes too early. Equals {@code statsBaseOffset} on record-aligned chunks (the common parallel path).
     */
    private final long statsStripeBaseOffset;
    /**
     * True iff this chunk reaches the file's true end. Only then may the chunk's last stripe be marked
     * complete-on-the-right ({@code atStripeEnd}) and terminal ({@code eof}); a non-final chunk ends
     * mid-stripe at a chunk boundary, so its trailing stripe is a partial right fragment the next chunk
     * continues — marking it complete would silently undercount.
     */
    private final boolean statsFileFinal;
    /**
     * How much per-stripe statistics this read harvests. {@link StripeColumnScope#NONE} harvests nothing;
     * {@link StripeColumnScope#COUNT} per-stripe row count only; {@link StripeColumnScope#PROJECTED} row
     * count plus min/max/null for the projected columns; {@link StripeColumnScope#ALL} adds min/max/null for
     * EVERY file column. Row count is harvested in every mode except {@code NONE} — including a zero-projection
     * {@code COUNT(*)} read, the regression this gate fixes. {@code ALL} is implemented by WIDENING the decode
     * projection to the full file schema (so every column is materialised), folding each widened page into the
     * file-schema accumulator in {@link #harvestAllColumns}, then slicing the page back to the query's
     * projected columns before it reaches the operator — so ALL's committed column set is a strict superset of
     * PROJECTED's.
     */
    private final StripeColumnScope statsColumnScope;
    /**
     * ALL-scope harvest layout. {@code >= 0} only when scope is ALL and the read is cacheable: the number of
     * leading decoded blocks that form the query's real projection (sliced into the output page). The decoder
     * materialises a WIDENED projection (projected ++ unprojected file columns); blocks past this count are
     * harvest-only and dropped before the page reaches the operator. {@code -1} disables (every narrower scope).
     */
    private final int harvestOutputColumnCount;
    /** The full file schema the ALL-scope accumulator is keyed to; {@code null} unless ALL-scope harvest is on. */
    private final List<Attribute> harvestFileSchema;
    /**
     * For each decoded block index, the index into {@link #harvestFileSchema} of the file column it carries,
     * or {@code -1} for a non-file column (the synthetic {@code _rowPosition}, a NULL-typed missing column).
     * Lets {@link #harvestAllColumns} feed every file column — projected and unprojected alike — into the
     * accumulator from one widened page. {@code null} unless ALL-scope harvest is on.
     */
    private final int[] harvestBlockToFileColumnIndex;
    /** ALL-scope, non-stripe (whole-file) accumulator over the full file schema. {@code null} until first fed. */
    private ColumnStatsAccumulator allFileColumnStats;
    /**
     * Shared canonical-stripe harvester (byte-range cover model — the same one the CSV reader uses). Owns the
     * per-stripe accumulators and the close-time emit loop. {@code null} when stripe capture is off.
     */
    private final StripeStatsHarvester stripeHarvester;
    /** Set when per-stripe capture must safe-miss (page rows don't align with the decoder's offset array). */
    private boolean stripeCaptureDisabled = false;

    /**
     * Widened decode projection for the ALL scope: the query's projected columns first (so the leading
     * {@code projectedColumns.size()} decoded blocks ARE the output page after slicing), then every file
     * column not already projected, so the decoder materialises the whole file schema. A {@code null}
     * projection ("read every column") and an empty projection ({@code COUNT(*)}) both reduce to "the full
     * file schema"; the leading-block count is captured separately by the caller.
     */
    private static List<String> computeAllColumnDecodeProjection(List<String> projectedColumns, List<Attribute> fileSchema) {
        LinkedHashSet<String> ordered = new LinkedHashSet<>();
        if (projectedColumns != null) {
            ordered.addAll(projectedColumns);
        }
        for (Attribute a : fileSchema) {
            ordered.add(a.name());
        }
        return new ArrayList<>(ordered);
    }

    /**
     * For each decoded block (one per entry of {@code decodeColumns}), the position of the file column it
     * carries within {@code fileSchema}, or {@code -1} when the block is not a file column (a synthetic such
     * as {@code _rowPosition}). Drives {@link #harvestAllColumns}'s feed of the file-schema accumulator.
     */
    private static int[] mapDecodeBlocksToFileColumns(List<String> decodeColumns, List<Attribute> fileSchema) {
        Map<String, Integer> fileIndexByName = new HashMap<>(fileSchema.size() * 2);
        for (int i = 0; i < fileSchema.size(); i++) {
            fileIndexByName.putIfAbsent(fileSchema.get(i).name(), i);
        }
        int[] map = new int[decodeColumns.size()];
        for (int b = 0; b < decodeColumns.size(); b++) {
            String name = decodeColumns.get(b);
            // A synthetic column (_rowPosition / metadata) is not a file column even if a same-named file
            // column does not exist; kindOf identifies it so we never harvest stats for it.
            map[b] = SyntheticColumns.kindOf(name) != null ? -1 : fileIndexByName.getOrDefault(name, -1);
        }
        return map;
    }

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
        long splitStartByte,
        int maxRecordBytes,
        DateFormatter datetimeFormatter,
        Map<String, String> declaredDateFormats,
        Set<String> declaredTypeColumns,
        long statsBaseOffset,
        long statsStripeSize,
        boolean statsFileFinal,
        StripeColumnScope statsColumnScope
    ) throws IOException {
        Check.isTrue(errorPolicy != null, "errorPolicy must not be null");
        Check.isTrue(counters != null, "counters must not be null");
        this.cacheableObject = cacheableObject;
        this.pinnedMtimeMillis = pinnedMtimeMillis;
        this.fingerprinter = fingerprinter;
        this.fingerprintSchema = resolvedAttributes;
        this.sourceLocation = object.path().toString();
        this.chunkMode = chunkMode;
        this.statsColumnScope = statsColumnScope != null ? statsColumnScope : StripeColumnScope.PROJECTED;
        // Per-stripe stats capture is for the chunk-parallel paths (recordAligned); a whole-file read
        // (parallelism=1) keeps the simpler authoritative WholeFile contribution. NONE scope disables
        // stripe harvest entirely (the warm aggregate then always re-scans). A rowLimit slice truncates a
        // page after the decoder recorded its full record count, which would desync the offset array from the
        // sliced page and trip forEachRun's recordCount==positionCount assert on a legitimate truncation — so
        // disable stripe tracking under a rowLimit (safe-miss, never assert on a data-shaped condition).
        this.statsStripeSize = (chunkMode && this.statsColumnScope != StripeColumnScope.NONE && rowLimit == FormatReader.NO_LIMIT)
            ? statsStripeSize
            : -1L;
        this.statsFileFinal = statsFileFinal;
        this.stripeHarvester = this.statsStripeSize > 0 ? new StripeStatsHarvester(this.statsStripeSize, statsFileFinal) : null;
        InputStream inputStream = object.newStream();
        NdJsonRecordSplitter recordSplitter = new NdJsonRecordSplitter(maxRecordBytes);
        // File-global byte offset of the first byte the decoder will read. When this split starts
        // mid-record (skipFirstLine), the leading partial record is dropped here, so the first full
        // record begins splitStartByte + (skipped bytes) — folding the skip in keeps the emitted
        // _rowPosition / _file.record_ref file-global-correct and split-invariant.
        long skipped = skipFirstLine ? Math.max(0L, skipToNextLine(inputStream, recordSplitter)) : 0L;
        long recordOffsetBase = splitStartByte + skipped;
        // Decoder reads from statsBaseOffset + skipped; both stripe-attribution anchors derive from here.
        // Today `skipped` is always 0 whenever stripe capture is active: the harvester needs chunkMode
        // (recordAligned) chunks, and skipFirstLine only fires on !recordAligned reads — so the two are
        // mutually exclusive. The fold-in is kept as a correctness invariant for any future overlap.
        this.statsStripeBaseOffset = statsBaseOffset + skipped;
        if (trimLastPartialLine) {
            inputStream = trimLastPartialLine(inputStream, errorPolicy, sourceLocation, recordSplitter);
        }
        this.rowLimit = rowLimit;
        // ALL scope harvests min/max/null for EVERY file column, not just the projected ones. The output
        // page must still carry only the query's projected columns, so we widen the DECODE projection to
        // {projected columns} ++ {every file column not already projected} (the synthetic _rowPosition /
        // metadata columns are not file columns and are excluded from the harvest set). The decoder then
        // materialises every file column; each page is harvested into the all-column accumulator over the
        // file schema (see harvestAllColumns) and SLICED back to the query's projected columns before it
        // reaches the operator. The widening reuses the decoder's full type/multi-value/nested machinery —
        // ALL pays to decode every column, which is its inherent opt-in cost. Narrower scopes decode only
        // the projected columns, unchanged.
        boolean harvestAll = statsColumnScope == StripeColumnScope.ALL && cacheableObject != null && resolvedAttributes != null;
        List<String> decodeColumns = projectedColumns;
        if (harvestAll) {
            List<String> widened = computeAllColumnDecodeProjection(projectedColumns, resolvedAttributes);
            this.harvestOutputColumnCount = projectedColumns == null ? resolvedAttributes.size() : projectedColumns.size();
            this.harvestFileSchema = resolvedAttributes;
            this.harvestBlockToFileColumnIndex = mapDecodeBlocksToFileColumns(widened, resolvedAttributes);
            decodeColumns = widened;
        } else {
            this.harvestOutputColumnCount = -1;
            this.harvestFileSchema = null;
            this.harvestBlockToFileColumnIndex = null;
        }
        // byte[] path keeps record offsets exact across parse-error recovery (streaming resets the
        // parser baseline on recovery). Capped at BYTE_ARRAY_FAST_PATH_MAX_SIZE for whole-file
        // reads of huge unsplit NDJSON; above the cap, fall back to streaming and accept a
        // possible offset shift on lenient-mode recovery rather than risk unbounded allocation.
        if (canUseByteArrayFastPath(object)) {
            // Slurp the bounded (≤16 MiB) segment in one pull. max_record_size is enforced per-record
            // inside NdJsonPageDecoder on the pass Jackson already makes — no second walk over the buffer
            // (the pre-#965 strict cap stream / lenient filter both re-scanned every byte before the
            // decoder re-walked them; see issue 965). Splitter-side enforcement
            // (NdJsonRecordSplitter.findLastRecordBoundary at split-discovery time) still bounds parallel
            // chunks; the decoder cap protects whole-file and byte-range macro-split reads that bypass it.
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
                datetimeFormatter,
                resolvedAttributes,
                decodeColumns,
                batchSize,
                blockFactory,
                errorPolicy,
                this.sourceLocation,
                counters,
                declaredDateFormats,
                declaredTypeColumns
            );
        } else {
            // Streaming/fallback path (object too large for the fast path, unknown length, or a
            // single-threaded read). max_record_size is enforced per-record by the decoder here too, which
            // closes the pre-#965 gap where this branch wrapped only a CountingInputStream and parsed
            // oversized records with no cap at all (issue 965 feedback). CountingInputStream still gives
            // close-time bytesRead for stream-only sources (bzip2 / zstd-streamed) whose length() throws.
            CountingInputStream counted = new CountingInputStream(inputStream);
            this.byteCounter = counted;
            this.byteArrayBytesRead = -1;
            this.pageDecoder = new NdJsonPageDecoder(
                counted,
                datetimeFormatter,
                resolvedAttributes,
                decodeColumns,
                batchSize,
                blockFactory,
                errorPolicy,
                this.sourceLocation,
                counters,
                declaredDateFormats,
                declaredTypeColumns
            );
        }
        // _rowPosition / _file.record_ref substrate: file-global per-record start offset.
        this.pageDecoder.setRecordOffsetBase(recordOffsetBase);
        this.pageDecoder.setMaxRecordBytes(maxRecordBytes);
        if (this.statsStripeSize > 0) {
            // Tell the decoder to record each record's own file-global start offset into a per-page array,
            // so the iterator can attribute the page's rows to canonical stripes by the byte-range cover
            // model. statsBaseOffset is this chunk's absolute file offset; the decoder adds the parser's
            // within-chunk byte offset. When this split started mid-record the leading partial record was
            // already dropped (skipped bytes), so the decoder reads from statsBaseOffset + skipped — fold
            // the skip in exactly as the _rowPosition substrate above (recordOffsetBase) does, otherwise
            // every record is attributed `skipped` bytes too early and lands in the wrong stripe. No
            // page-capping — a page may span stripes.
            this.pageDecoder.enableRecordOffsetTracking(statsStripeBaseOffset);
        }
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
                // A streaming/fallback truncation (decoder stopped at an oversized record under a
                // non-strict policy) is NOT a clean drain: leave naturallyExhausted false so the
                // under-counted partial result is never published to the stats cache. The decoder has
                // already emitted the client-facing partial-results warning.
                if (pageDecoder.truncated()) {
                    logger.warn(
                        "NDJSON read of [{}] truncated at byte [{}]: a record exceeded max_record_size; results are partial",
                        sourceLocation,
                        pageDecoder.truncatedAtByte()
                    );
                } else {
                    naturallyExhausted = true;
                }
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
        // ALL scope decodes a WIDENED projection (every file column). Harvest the all-column stats off the
        // wide page FIRST, then slice it down to the query's projected columns before it reaches the operator.
        if (harvestOutputColumnCount >= 0) {
            harvestAllColumns(result);
            result = sliceToOutputColumns(result);
        }
        captureBlockStats(result);
        return result;
    }

    /**
     * Drops the harvest-only trailing blocks of a widened ALL-scope page, returning a page carrying only the
     * query's projected columns (the leading {@link #harvestOutputColumnCount} blocks). The dropped blocks are
     * released; the kept blocks transfer ownership to the new page. A zero-projection {@code COUNT(*)} returns
     * a row-count-only page.
     */
    private Page sliceToOutputColumns(Page widePage) {
        int kept = harvestOutputColumnCount;
        int total = widePage.getBlockCount();
        if (kept == total) {
            return widePage;
        }
        int positions = widePage.getPositionCount();
        try {
            if (kept == 0) {
                return new Page(positions);
            }
            Block[] outBlocks = new Block[kept];
            for (int i = 0; i < kept; i++) {
                outBlocks[i] = widePage.getBlock(i);
            }
            return new Page(positions, outBlocks);
        } finally {
            // Release only the harvest-only trailing blocks; the kept blocks are now owned by the new page.
            for (int i = kept; i < total; i++) {
                widePage.getBlock(i).close();
            }
        }
    }

    /**
     * ALL-scope side-pass: fold every FILE column of the widened decoded page into the file-schema
     * accumulator — including the unprojected columns the output page never carries. On the stripe path each
     * run of consecutive same-stripe rows folds into that stripe's {@code allCols} (a page may span stripes,
     * so it is split by the decoder's per-record offsets); off the stripe path the whole page folds into the
     * single whole-file accumulator. Synthetic blocks (e.g. {@code _rowPosition}) map to file-column index
     * {@code -1} and are skipped.
     */
    private void harvestAllColumns(Page widePage) {
        if (harvestFileSchema == null || widePage.getBlockCount() == 0) {
            return;
        }
        if (statsStripeSize > 0) {
            forEachStripeRun(widePage, (ordinal, acc, page, from, to) -> {
                if (acc.allCols == null) {
                    acc.allCols = ColumnStatsAccumulator.forSchema(harvestFileSchema);
                }
                if (acc.allCols.isEmpty()) {
                    return;
                }
                feedFileColumns(acc.allCols, page, from, to);
            });
            return;
        }
        if (allFileColumnStats == null) {
            allFileColumnStats = ColumnStatsAccumulator.forSchema(harvestFileSchema);
        }
        if (allFileColumnStats.isEmpty()) {
            return;
        }
        feedFileColumns(allFileColumnStats, widePage, 0, widePage.getPositionCount());
    }

    /**
     * Feeds the widened page's file-column blocks (mapped via {@link #harvestBlockToFileColumnIndex}) for
     * positions {@code [from, to)} into {@code acc}. When the run is the whole page the blocks are fed
     * directly; otherwise the relevant positions are sliced out first (and released).
     */
    private void feedFileColumns(ColumnStatsAccumulator acc, Page widePage, int from, int to) {
        boolean wholePage = from == 0 && to == widePage.getPositionCount();
        int[] positions = wholePage ? null : positionRange(from, to);
        int blocks = widePage.getBlockCount();
        for (int b = 0; b < blocks && b < harvestBlockToFileColumnIndex.length; b++) {
            int fileColumn = harvestBlockToFileColumnIndex[b];
            if (fileColumn < 0) {
                continue;
            }
            if (wholePage) {
                acc.acceptBlockAt(fileColumn, widePage.getBlock(b));
            } else {
                var filtered = widePage.getBlock(b).filter(false, positions);
                try {
                    acc.acceptBlockAt(fileColumn, filtered);
                } finally {
                    filtered.close();
                }
            }
        }
    }

    private static int[] positionRange(int from, int to) {
        int[] positions = new int[to - from];
        for (int p = 0; p < positions.length; p++) {
            positions[p] = from + p;
        }
        return positions;
    }

    private void captureBlockStats(Page page) {
        // NONE harvests nothing. Otherwise we harvest at minimum the per-stripe row count, including for a
        // zero-projection COUNT(*) read (page.getBlockCount() == 0) — the regression this gate fixes: the old
        // `getBlockCount() == 0` early-return at the top killed all harvest, so a warm COUNT(*) re-scanned.
        if (cacheableObject == null || statsColumnScope == StripeColumnScope.NONE) {
            return;
        }
        if (statsStripeSize > 0) {
            captureStripeStats(page);
            return;
        }
        // Non-stripe whole-file path. COUNT harvests no per-column stats (the close hook still publishes the
        // whole-file row count); PROJECTED harvests the projected columns here from the output page. ALL has
        // already harvested EVERY file column (incl. unprojected) off the widened page in harvestAllColumns,
        // so the projected re-harvest is skipped — allFileColumnStats is the strict superset.
        if (statsColumnScope.harvestsColumns() == false || page.getBlockCount() == 0 || harvestOutputColumnCount >= 0) {
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

    /**
     * Attribute the page's rows to canonical stripes by each record's own file-global start offset (the
     * decoder's per-record offset array), summing rows + projected-column stats per stripe. A page may span
     * stripes; it is split into runs of consecutive same-stripe rows. This counts each run's rows (the
     * authoritative per-stripe row count) and folds the projected columns; the byte ranges and cover anchors
     * are derived from the chunk's byte geometry at emission (the shared {@link StripeStatsHarvester#emit}),
     * so record attribution stays scan-invariant. The ALL-scope all-column harvest runs separately in
     * {@link #harvestAllColumns} (no row counting there) so rows are counted exactly once.
     */
    private void captureStripeStats(Page page) {
        forEachStripeRun(page, (ordinal, acc, p, from, to) -> {
            acc.rows += (to - from);
            // COUNT scope harvests rows only — never a column accumulator. PROJECTED builds one for the
            // projected columns when there are any (a zero-projection COUNT(*) read has none, so acc.cols stays
            // null and only acc.rows advances). ALL has ALREADY folded every file column (incl. unprojected)
            // into acc.allCols off the widened page in harvestAllColumns, so the projected re-harvest is
            // skipped here.
            if (acc.cols == null && statsColumnScope.harvestsColumns() && harvestOutputColumnCount < 0) {
                List<Attribute> projected = pageDecoder.projectedAttributes();
                acc.cols = (projected == null || projected.isEmpty())
                    ? null
                    : ColumnStatsAccumulator.forProjectedAttributes(projected.toArray(new Attribute[0]));
            }
            if (acc.cols != null && acc.cols.isEmpty() == false) {
                boolean wholePage = from == 0 && to == p.getPositionCount();
                int[] positions = wholePage ? null : positionRange(from, to);
                int blocks = p.getBlockCount();
                for (int i = 0; i < blocks; i++) {
                    if (wholePage) {
                        acc.cols.acceptBlockAt(i, p.getBlock(i));
                    } else {
                        var filtered = p.getBlock(i).filter(false, positions);
                        try {
                            acc.cols.acceptBlockAt(i, filtered);
                        } finally {
                            filtered.close();
                        }
                    }
                }
            }
        });
    }

    /** Per-stripe-run callback: {@code acc} is the stripe's accumulator, {@code [from, to)} its rows in {@code page}. */
    @FunctionalInterface
    private interface StripeRunConsumer {
        void accept(long ordinal, StripeStatsHarvester.StripeAccum acc, Page page, int from, int to);
    }

    /**
     * Splits {@code page} into maximal runs of consecutive same-stripe rows and invokes {@code consumer} once per
     * run so the caller can fold that run's column stats. NDJSON's per-record offsets come from the decoder (a
     * dropped line advances neither the page nor the offset array); the shared run-walk in
     * {@link StripeStatsHarvester#forEachRun} owns the offset/page alignment invariant (fail-loud assert +
     * safe-miss). On a count mismatch stripe capture safe-misses for the whole file (a warm aggregate re-scans).
     */
    private void forEachStripeRun(Page page, StripeRunConsumer consumer) {
        if (stripeCaptureDisabled) {
            return;
        }
        if (pageDecoder.offsetBaselineLost()) {
            // A streaming-path recovery reset the parser byte baseline; every record offset recorded after it is
            // short by the pre-recovery bytes, so attribution would commit records to EARLIER stripes. forEachRun's
            // alignment check is count-only and cannot catch value skew, and NDJSON has no emit-time byte tripwire —
            // safe-miss the whole file. (Recovery happens during decode; this runs after the page returns, so the
            // flag is set before the first skewed row is accumulated.)
            stripeCaptureDisabled = true;
            return;
        }
        long[] offsets = pageDecoder.lastPageRecordOffsets();
        boolean aligned = stripeHarvester.forEachRun(
            offsets,
            pageDecoder.lastPageRecordCount(),
            page.getPositionCount(),
            (ordinal, acc, from, to) -> consumer.accept(ordinal, acc, page, from, to)
        );
        if (aligned == false) {
            stripeCaptureDisabled = true; // alignment lost — safe miss
        }
    }

    @Override
    protected void closeInternal() throws IOException {
        // Close the decoder even if a stats publish throws — the publish is best-effort caching, the close is not.
        try {
            // Cache on clean whole-file drain. Runs before closing the decoder so its errorCount is still readable.
            // A DROPPED line (NDJSON drops the whole line on any parse error) does NOT make the cached stats
            // wrong: which lines survive is a deterministic function of the file bytes and the error policy
            // (pinned by the cache fingerprint -- error_mode/max_errors are format-affecting), so every statistic
            // (row count AND extrema) over the survivors equals what re-running this query computes. So commit
            // normally. NONE scope suppresses all publishing. A scan cut short mid-way (LIMIT, cancellation, a
            // chunk exceeding its error budget) leaves naturallyExhausted false or an uncovered stripe, so it
            // safe-misses rather than serving; the coordinator's whole-file poison covers the non-clean-close case.
            if (cacheableObject != null
                && naturallyExhausted
                && pinnedMtimeMillis >= 0
                && fingerprinter != null
                && pageDecoder.capDropped() == false
                && statsColumnScope != StripeColumnScope.NONE) {
                // Fingerprint must use the FULL file schema for parity with NdJsonFormatReader.metadata().
                // Prefer the planner-provided schema (resolvedAttributes), fall back to the decoder's
                // projected attributes only when those equal the full schema (no projection pruning).
                List<Attribute> fullSchema = fingerprintSchema != null ? fingerprintSchema : pageDecoder.projectedAttributes();
                if (fullSchema != null && fullSchema.isEmpty() == false) {
                    if (statsStripeSize > 0) {
                        // Byte-range cover emit (shared with CSV): one fragment per stripe the chunk's byte range
                        // overlaps, including empty edge stripes. Safe-miss if row/offset alignment was lost.
                        if (stripeCaptureDisabled == false && stripeHarvester.isEmpty() == false) {
                            long chunkBytes = byteCounter != null ? byteCounter.getBytesRead() : byteArrayBytesRead;
                            // Anchor the byte-range cover at the first byte actually decoded (statsBaseOffset +
                            // skipped); chunkBytes counts only the post-skip bytes, so [statsStripeBaseOffset,
                            // statsStripeBaseOffset + chunkBytes) is the true covered range. Using statsBaseOffset
                            // would under-shoot by `skipped` on a mid-record split and mis-attribute the edge stripes.
                            stripeHarvester.emit(
                                sourceLocation,
                                statsStripeBaseOffset,
                                chunkBytes,
                                pinnedMtimeMillis,
                                fingerprinter.apply(fullSchema),
                                fullSchema
                            );
                        }
                    } else {
                        emitWholeChunk(fullSchema);
                    }
                }
            }
        } finally {
            IOUtils.close(pageDecoder);
        }
    }

    /**
     * Whole-chunk / whole-file emission: one authoritative contribution carrying the chunk's full row
     * count and column stats. Used when stripe addressing is off (parallelism=1 whole-file read).
     */
    private void emitWholeChunk(List<Attribute> fullSchema) {
        // PROJECTED/COUNT commit columnStats. ALL commits allFileColumnStats (every file column) instead —
        // the strict superset of what PROJECTED would commit (see StripeStatsHarvester.mergeColumnStats).
        Map<String, ExternalStats.ColumnStats> cols = StripeStatsHarvester.mergeColumnStats(columnStats, allFileColumnStats);
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
    static long skipToNextLine(InputStream stream) throws IOException {
        return skipToNextLine(stream, new NdJsonRecordSplitter(SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES));
    }

    static long skipToNextLine(InputStream stream, NdJsonRecordSplitter recordSplitter) throws IOException {
        NdJsonRecordSplitter.LineScan scan = recordSplitter.scanForTerminator(stream);
        if (scan.consumed() == RecordSplitter.RECORD_TOO_LARGE) {
            throw recordSplitter.recordTooLargeException();
        }
        return scan.consumed();
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
