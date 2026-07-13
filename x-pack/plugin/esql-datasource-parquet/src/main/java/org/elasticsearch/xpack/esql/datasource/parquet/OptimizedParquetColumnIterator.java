/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.UninitializedArrays;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorProducer;
import org.elasticsearch.xpack.esql.datasources.spi.DynamicThreshold;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Default Parquet column iterator with vectorized decoding and I/O prefetch.
 *
 * <p>Adds parallel column chunk prefetch: while decoding row group N, prefetches row group N+1's
 * data via {@link CoalescedRangeReader} and feeds it directly to {@link PrefetchedRowGroupBuilder}
 * so that subsequent reads are served from memory instead of network I/O.
 *
 * <p>Always uses {@link PageColumnReader} for vectorized batch decoding of flat columns and
 * {@code ColumnReader} for list columns. When {@link RowRanges} are provided (from
 * {@link ColumnIndexRowRangesComputer}), pages whose row span doesn't overlap are skipped,
 * and prefetch is scoped to only the surviving pages.
 *
 * <p>Both this iterator and the baseline {@code ParquetColumnIterator} share list-column
 * decoding and utility helpers via {@link ParquetColumnDecoding}. The baseline remains
 * the stable fallback when {@code optimized_reader=false} is explicitly set via config.
 *
 * <p><b>Memory:</b> Prefetched bytes are accounted on the REQUEST circuit breaker (via
 * {@link BlockFactory#breaker()}) by the Arrow allocator that backs the per-range
 * {@code ArrowBuf}s; the bytes are returned when the chunks' {@link Releasable} is closed at
 * row-group rollover. If the allocator's breaker check fails during a prefetch, the future
 * fails, prefetch is skipped, and the query falls back to synchronous I/O for that row group.
 *
 * <p><b>Trivially-passes guard:</b> when late materialization is enabled and row-group
 * statistics prove every row satisfies the pushed filter ({@link TriviallyPassesChecker}),
 * the iterator routes that row group through {@code nextStandard} for the remainder of the
 * row group, skipping per-row filter evaluation and survivor compaction. This benefits queries
 * that mix selective and non-selective row groups (e.g., time-bucketed data with skewed
 * filter selectivity).
 */
final class OptimizedParquetColumnIterator implements CloseableIterator<Page>, ColumnExtractorProducer {

    private static final Logger logger = LogManager.getLogger(OptimizedParquetColumnIterator.class);

    private final ParquetFileReader reader;
    private final MessageType projectedSchema;
    private final List<Attribute> attributes;
    private final int batchSize;
    private final BlockFactory blockFactory;
    private final String createdBy;
    private final String fileLocation;
    /** See {@link #coercionWarnings()}. */
    private SkipWarnings coercionWarnings;
    /** The read's error policy; strict ({@code fail_fast}) makes {@link #coercionWarnings()} return {@code null}. */
    private final ErrorPolicy errorPolicy;
    private final ColumnInfo[] columnInfos;
    private final PreloadedRowGroupMetadata preloadedMetadata;
    private final StorageObject storageObject;
    private final Set<String> projectedColumnPaths;
    private final Set<String> predicateColumnPaths;
    private final Set<String> projectionOnlyColumnPaths;
    private final CircuitBreaker breaker;
    private final RowRanges[] allRowRanges;
    /**
     * Per-row-group survival flag: {@code true} = read this row group; {@code false} = skip
     * (dropped by row-group level filter — stats, bloom, dictionary). When {@code null}, all
     * row groups survive.
     */
    private final boolean[] survivingRowGroups;
    /**
     * Precomputed lookup mapping every ordinal {@code i} to the smallest surviving ordinal
     * {@code >= i} (or {@code length} if none survive). Built once in the constructor so each
     * call to {@link #nextSurvivingRowGroupOrdinal(int)} is O(1) instead of O(K).
     */
    private final int[] nextSurvivor;
    /**
     * File-global row index of the first row in each row group, computed as a prefix sum over
     * {@link BlockMetaData#getRowCount()} in footer iteration order. Used to materialise the
     * synthetic {@link ColumnExtractor#ROW_POSITION_COLUMN} column when present in the projection;
     * the iterator turns each emitted row's (row-group ordinal, in-block index) coordinate into
     * a stable file-global {@code long} the deferred-extraction path can later use to look the
     * row up directly from the file's footer. {@code null} when the projection does not include
     * {@code _rowPosition} — building the prefix sum is cheap but pointless if no row-position
     * column is asked for.
     */
    private final long[] rowGroupFirstRowGlobal;
    /**
     * Index of the {@code _rowPosition} slot in {@link #columnInfos}, or {@code -1} when the
     * projection does not include the synthetic column. Cached up-front so the per-batch emit
     * paths skip the regular column read for that slot and fill it from
     * {@link #rowGroupFirstRowGlobal} instead.
     */
    private final int rowPositionColumnIndex;
    /**
     * Owning {@link ParquetFormatReader}; only consulted when the iterator builds a matching
     * {@link ColumnExtractor} via {@link #createColumnExtractor()}. Kept out of the regular emit
     * paths so the iterator does not depend on the broader format reader's state for forward scan.
     */
    private final ParquetFormatReader formatReader;
    /**
     * The file's full footer; passed to the produced {@link ColumnExtractor} unchanged so it
     * addresses every row group regardless of whether the iterator's reader was opened on the
     * full file or on a range. {@code null} iff {@link #rowPositionColumnIndex} is {@code -1}.
     */
    private final ParquetMetadata fullFooter;
    private final DynamicThreshold dynamicThreshold;
    private final ColumnDescriptor sortColumnDescriptor;
    private final String sortColumnPath;
    private final PrimitiveType sortColumnPrimitiveType;
    /**
     * High bits OR-ed into every emitted {@code _rowPosition} value once
     * {@link #setExtractorId(int)} has been called: {@code ((long) extractorId) << LOCAL_POSITION_BITS}.
     * The factory installs the id between {@link #createColumnExtractor()} and the first call to
     * {@link #next()}, so by the time we materialise {@code _rowPosition} we always have a value
     * for it. Stays at {@code -1} when the projection has no row-position column ({@link
     * #rowPositionColumnIndex} {@code < 0}); we never read the field in that case.
     * <p>
     * Encoding here saves a per-page block re-allocation that the previous
     * {@code EncodingRowRefIterator} wrapper was forced to do — we already own the {@code long[]}
     * holding the values and can stamp the high bits as we fill it.
     */
    private long rowPositionEncodingHighBits = -1L;
    private final CompressionCodecFactory codecFactory;
    private int rowBudget;
    /**
     * Async prefetches allowed ahead of the consumed row group. Initialized from
     * {@link #computePrefetchDepth} based on projected column size (1-3), then adapted
     * at runtime: grows by {@link #PREFETCH_DEPTH_GROWTH} on stall detection (the
     * prefetch future was not ready when consumed), shrinks by 1 after
     * {@link #SHRINK_AFTER_NO_STALLS} consecutive no-stall row groups. Growth is
     * suppressed when the circuit breaker exceeds {@link #BREAKER_GROWTH_THRESHOLD}
     * utilization. Bounded by [{@link #prefetchDepthFloor}, {@link #MAX_PREFETCH_DEPTH}].
     */
    private int prefetchDepth;
    private final int prefetchDepthFloor;
    private int consecutiveNoStalls;
    private final ArrayDeque<PendingPrefetch> pendingPrefetches = new ArrayDeque<>();
    /**
     * Allocator-backed memory holding the chunks currently in use by {@link #rowGroup}. Released
     * at row-group rollover so the direct memory is freed eagerly rather than waiting for the
     * JVM {@code Cleaner}. Closing the releasable decrements the underlying {@code ArrowBuf}
     * reference counts, which in turn returns the bytes to the circuit breaker via
     * {@link org.elasticsearch.compute.data.arrow.CircuitBreakerAllocationListener}.
     */
    private Releasable currentChunksReleasable;

    private PrefetchedPageReadStore rowGroup;
    private ColumnReader[] columnReaders;
    private PageColumnReader[] pageColumnReaders;
    private long rowsRemainingInGroup;
    private boolean exhausted = false;
    private int rowGroupOrdinal = -1;
    private int pageBatchIndexInRowGroup = 0;

    private final boolean lateMaterialization;
    /**
     * When {@code true}, switches the prefetch + decode pipeline to a per-row-group two-phase
     * I/O flow: Phase 1 fetches only predicate column chunks, the iterator fully decodes them
     * to evaluate the filter and accumulate a global survivor mask, and Phase 2 then fetches
     * only the projection column pages that contain at least one survivor. On remote storage
     * with a selective filter this avoids the dominant cost of transferring projection-only
     * column data (typically large strings) for rows that are immediately filtered out.
     *
     * <p>Disabled when:
     * <ul>
     *   <li>Late materialization is off ({@link #lateMaterialization} is false).</li>
     *   <li>The storage object is local (non-async) — the extra Phase-1 → Phase-2 sequencing
     *       adds latency without saving meaningful I/O on a memory-mapped file.</li>
     *   <li>Predicate columns are themselves a large fraction of projected bytes — the savings
     *       on projection columns no longer justify the per-row-group sequential dependency
     *       (see {@link #TWO_PHASE_PREDICATE_BYTE_RATIO_THRESHOLD}).</li>
     *   <li>Any projected column is a list column — list decoding goes through
     *       {@code ColumnReadStoreImpl} which expects a single combined {@link PageReader}
     *       store; supporting it under two-phase is left for follow-up work.</li>
     *   <li>There are no projection-only columns (every projected column is also a predicate
     *       column) — there is nothing for Phase 2 to save.</li>
     * </ul>
     */
    private final boolean useTwoPhase;
    private final boolean[] isPredicateColumn;
    private final ParquetPushedExpressions pushedExpressions;
    /**
     * The parquet-mr {@link FilterPredicate} resolved by {@code ParquetFormatReader} for this scan,
     * passed through unchanged so the trivially-passes check sees the same predicate that drove
     * row-group pruning and ColumnIndex {@link RowRanges} computation. Translating again here
     * would be wasted work and could subtly diverge if the caller's schema differs from
     * {@link #projectedSchema}.
     *
     * <p>{@code null} when the trivially-passes guard is inactive: late materialization is off,
     * the reader did not resolve a file-level predicate, or predicate resolution failed earlier
     * (in which case the reader logged a warning and passed {@code null} through).
     */
    private final FilterPredicate triviallyPassesPredicate;
    private final WordMask survivorMask;
    /**
     * Memoizes the dictionary-match bitmap computed by {@link ParquetPushedExpressions} for
     * every pushed leaf predicate, so the predicate is evaluated against the dictionary
     * once per row group rather than once per batch. The underlying {@code BytesRefArray}
     * is fixed for the lifetime of a row group (see {@code PageColumnReader#ensureCachedDictArray}),
     * which is what makes the entries valid across batches.
     *
     * <p>Lifecycle by stamp, not by callback: {@link #dictionaryBitmapsForCurrentRowGroup}
     * compares the cached row-group ordinal with the current one and wipes the map on a
     * mismatch. This makes the cache self-invalidating — a missed wipe in {@code advanceRowGroup}
     * (e.g. on the two-phase "all rows filtered, try next row group" retry path) cannot
     * leak entries into a different row group's evaluation.
     *
     * <p>{@code null} when late materialization is off; in that case no dictionary fast path
     * runs and the map would be pure overhead.
     */
    private final Map<Expression, boolean[]> dictionaryBitmaps;
    private int dictionaryBitmapsRowGroup = -1;
    private long rowsEliminatedByLateMaterialization;
    /**
     * When {@code true}, row-group statistics prove every row in the current row group satisfies
     * the pushed filter, so late materialization is bypassed for this row group: filter
     * evaluation and survivor compaction are skipped, and the standard read path is used.
     */
    private boolean currentRowGroupTriviallyPasses;
    /** Diagnostic counter: number of row groups for which the filter was proven to trivially pass. */
    private long rowGroupsWithTrivialFilter;

    /**
     * Per-row-group two-phase decode state. Populated in {@link #advanceRowGroup()} when
     * {@link #useTwoPhase} is active and the current row group survives Phase-1 filter
     * evaluation; consumed batch-by-batch in {@link #nextTwoPhaseBatch(int)} and cleared in
     * {@link #closeTwoPhaseState()} when the iterator advances to the next row group or is
     * closed.
     */
    private TwoPhaseRowGroup twoPhase;
    /**
     * Diagnostic counter: number of row groups served via the two-phase Phase-2 page-skipping
     * path (i.e., where Phase 2 fetched only surviving pages instead of full chunks).
     */
    private long twoPhaseRowGroupsWithPageSkipping;
    /**
     * Diagnostic counter: number of row groups where two-phase fell back to fetching whole
     * projection chunks because the survivor RowRanges was too dense or fragmented to
     * benefit from page skipping.
     */
    private long twoPhaseRowGroupsFullProjectionFetch;
    /** Diagnostic counter: number of row groups skipped entirely because no row survived the filter. */
    private long twoPhaseRowGroupsAllFiltered;

    /** Reader-level counters shared with the owning {@link ParquetFormatReader}. */
    private final ParquetReaderCounters counters;

    OptimizedParquetColumnIterator(
        ParquetFileReader reader,
        MessageType projectedSchema,
        List<Attribute> attributes,
        int batchSize,
        BlockFactory blockFactory,
        int rowLimit,
        String createdBy,
        String fileLocation,
        ColumnInfo[] columnInfos,
        PreloadedRowGroupMetadata preloadedMetadata,
        StorageObject storageObject,
        RowRanges[] allRowRanges,
        boolean[] survivingRowGroups,
        long[] rowGroupFirstRowGlobalOverride,
        CompressionCodecFactory codecFactory,
        ParquetPushedExpressions pushedExpressions,
        FilterPredicate triviallyPassesPredicate,
        ParquetFormatReader formatReader,
        ParquetMetadata fullFooter,
        DynamicThreshold dynamicThreshold,
        ColumnDescriptor sortColumnDescriptor,
        ParquetReaderCounters counters,
        ErrorPolicy errorPolicy
    ) {
        this.errorPolicy = errorPolicy;
        this.reader = reader;
        this.projectedSchema = projectedSchema;
        this.attributes = attributes;
        this.batchSize = batchSize;
        this.blockFactory = blockFactory;
        this.rowBudget = rowLimit;
        this.createdBy = createdBy != null ? createdBy : "";
        this.fileLocation = fileLocation;
        this.columnInfos = columnInfos;
        this.preloadedMetadata = preloadedMetadata;
        this.storageObject = storageObject;
        this.breaker = blockFactory.breaker();
        this.allRowRanges = allRowRanges;
        this.survivingRowGroups = survivingRowGroups;
        this.nextSurvivor = buildNextSurvivorLookup(survivingRowGroups);
        this.rowPositionColumnIndex = findRowPositionSlot(columnInfos);
        // For full-file reads the prefix sum can be derived from the reader's blocks directly.
        // For range-restricted reads the caller must supply file-global offsets — those are the
        // identities the deferred-extraction path expects to look up against the full footer.
        this.rowGroupFirstRowGlobal = rowPositionColumnIndex >= 0
            ? (rowGroupFirstRowGlobalOverride != null ? rowGroupFirstRowGlobalOverride : buildRowGroupFirstRowGlobal(reader.getRowGroups()))
            : null;
        this.formatReader = formatReader;
        this.fullFooter = fullFooter;
        this.dynamicThreshold = dynamicThreshold;
        this.sortColumnDescriptor = sortColumnDescriptor;
        this.sortColumnPath = sortColumnDescriptor == null ? null : String.join(".", sortColumnDescriptor.getPath());
        this.sortColumnPrimitiveType = sortColumnDescriptor == null ? null : sortColumnDescriptor.getPrimitiveType();
        this.codecFactory = codecFactory;
        this.counters = counters;
        this.pushedExpressions = pushedExpressions;
        this.isPredicateColumn = classifyPredicateColumns(attributes, columnInfos, pushedExpressions);
        this.lateMaterialization = pushedExpressions != null;
        this.survivorMask = lateMaterialization ? new WordMask() : null;
        this.dictionaryBitmaps = lateMaterialization ? new IdentityHashMap<>() : null;
        // Caller supplies null when late materialization is off; defensively also drop it here so
        // the trivially-passes check is gated by a single condition below.
        this.triviallyPassesPredicate = lateMaterialization ? triviallyPassesPredicate : null;

        this.projectedColumnPaths = buildProjectedColumnPaths(columnInfos);
        this.predicateColumnPaths = buildColumnPaths(columnInfos, isPredicateColumn, true);
        this.projectionOnlyColumnPaths = buildColumnPaths(columnInfos, isPredicateColumn, false);
        this.useTwoPhase = shouldUseTwoPhase(
            lateMaterialization,
            storageObject,
            columnInfos,
            reader.getRowGroups(),
            projectedColumnPaths,
            predicateColumnPaths,
            projectionOnlyColumnPaths,
            fileLocation
        );
        this.prefetchDepthFloor = computePrefetchDepth(reader.getRowGroups(), this.projectedColumnPaths);
        this.prefetchDepth = this.prefetchDepthFloor;

        reader.setRequestedSchema(projectedSchema);

        prefetchFirstRowGroup();
    }

    /**
     * Seeds the prefetch queue at construction time so that the first {@link #advanceRowGroup()}
     * call finds ready data instead of falling through to synchronous I/O.
     */
    private void prefetchFirstRowGroup() {
        if (storageObject == null) {
            return;
        }
        if (dynamicThreshold != null && dynamicThreshold.noFurtherCandidates()) {
            return;
        }
        int startOrdinal = nextSurvivingRowGroupOrdinal(0);
        fillPrefetchQueue(startOrdinal);
    }

    /**
     * Fills the prefetch queue up to {@link #prefetchDepth} entries, starting from ordinal
     * {@code fromOrdinal}. Stops early when no more surviving row groups remain. Breaker
     * accounting for the prefetched bytes happens implicitly inside the backend's
     * {@code readBytesAsync} via the Arrow allocator; if a prefetch trips the breaker it
     * surfaces as a failed future and {@link #takePendingPrefetch} falls back to sync I/O
     * for that row group.
     */
    private void fillPrefetchQueue(int fromOrdinal) {
        List<BlockMetaData> rowGroups = reader.getRowGroups();
        // Under two-phase, the queued (Phase 1) prefetch only covers predicate columns; the
        // projection columns are fetched synchronously in advanceRowGroup once the survivor mask
        // is known. We therefore size the I/O ranges and the column set against the same,
        // smaller column subset.
        Set<String> phaseColumns = useTwoPhase ? predicateColumnPaths : projectedColumnPaths;
        int nextOrdinal = fromOrdinal;
        while (pendingPrefetches.size() < prefetchDepth && nextOrdinal < rowGroups.size()) {
            if (dynamicThreshold != null && dynamicThreshold.noFurtherCandidates()) {
                break;
            }
            BlockMetaData nextBlock = rowGroups.get(nextOrdinal);
            if (rowGroupDominatedByThreshold(nextBlock)) {
                nextOrdinal = nextSurvivingRowGroupOrdinal(nextOrdinal + 1);
                continue;
            }
            long prefetchBytes = ColumnChunkPrefetcher.computePrefetchBytes(nextBlock, phaseColumns);
            if (prefetchBytes <= 0) {
                nextOrdinal = nextSurvivingRowGroupOrdinal(nextOrdinal + 1);
                continue;
            }
            try {
                RowRanges nextRowRanges = allRowRanges != null && nextOrdinal < allRowRanges.length ? allRowRanges[nextOrdinal] : null;
                RowRanges thresholdRanges = thresholdRowRanges(nextOrdinal, nextBlock);
                if (thresholdRanges != null) {
                    if (thresholdRanges.isEmpty()) {
                        nextOrdinal = nextSurvivingRowGroupOrdinal(nextOrdinal + 1);
                        continue;
                    }
                    nextRowRanges = nextRowRanges == null ? thresholdRanges : nextRowRanges.intersect(thresholdRanges);
                }
                if (lateMaterialization) {
                    // Late-mat (single-phase or two-phase) handles row-level filtering itself via
                    // the survivor mask and would double-filter if ColumnIndex RowRanges were
                    // applied to the predicate-column prefetch here.
                    nextRowRanges = null;
                }
                CompletableFuture<ColumnChunkPrefetcher.PrefetchedChunks> future;
                if (nextRowRanges != null) {
                    future = ColumnChunkPrefetcher.prefetchAsync(
                        storageObject,
                        nextBlock,
                        phaseColumns,
                        nextRowRanges,
                        preloadedMetadata,
                        nextOrdinal,
                        nextBlock.getRowCount(),
                        blockFactory.arrowAllocator()
                    );
                } else {
                    future = ColumnChunkPrefetcher.prefetchAsync(storageObject, nextBlock, phaseColumns, blockFactory.arrowAllocator());
                }
                pendingPrefetches.addLast(new PendingPrefetch(nextOrdinal, future));
            } catch (Exception e) {
                logger.debug("Failed to initiate prefetch for row group [{}] in [{}]: {}", nextOrdinal, fileLocation, e.getMessage());
                break;
            }
            nextOrdinal = nextSurvivingRowGroupOrdinal(nextOrdinal + 1);
        }
    }

    private boolean rowGroupDominatedByThreshold(BlockMetaData block) {
        if (dynamicThreshold == null || sortColumnPath == null || sortColumnPrimitiveType == null) {
            return false;
        }
        ColumnChunkMetaData sortColumn = sortColumn(block);
        if (sortColumn == null) {
            return false;
        }
        Statistics<?> statistics = sortColumn.getStatistics();
        if (statistics == null || statistics.isEmpty()) {
            return false;
        }
        if (statistics.hasNonNullValue() == false) {
            return dynamicThreshold.dominatesNulls(statistics.getNumNulls());
        }
        if (dynamicThreshold.elementType() == ElementType.BYTES_REF) {
            BytesRef min = bytesRefFromStats(statistics.genericGetMin());
            BytesRef max = bytesRefFromStats(statistics.genericGetMax());
            return min != null && max != null && dynamicThreshold.dominates(min, max, statistics.getNumNulls());
        }
        Long rawMin = rawValueFromStats(statistics.genericGetMin());
        Long rawMax = rawValueFromStats(statistics.genericGetMax());
        return rawMin != null && rawMax != null && dynamicThreshold.dominates(rawMin, rawMax, statistics.getNumNulls());
    }

    private RowRanges thresholdRowRanges(int rowGroupOrdinal, BlockMetaData block) {
        if (dynamicThreshold == null || sortColumnPath == null || sortColumnPrimitiveType == null) {
            return null;
        }
        // BYTES_REF thresholding is intentionally row-group level only for now: the page-index path
        // below interprets bounds as fixed-width numerics, which does not apply to variable-length
        // string min/max. String TopN therefore prunes at row-group granularity while numeric TopN
        // also prunes at page granularity. Page-level string pruning (decoding ColumnIndex string
        // bounds) is a possible follow-up, not an oversight.
        if (dynamicThreshold.elementType() == ElementType.BYTES_REF) {
            return null;
        }
        ColumnIndex columnIndex = preloadedMetadata.getColumnIndex(rowGroupOrdinal, sortColumnPath);
        OffsetIndex offsetIndex = preloadedMetadata.getOffsetIndex(rowGroupOrdinal, sortColumnPath);
        if (columnIndex == null || offsetIndex == null) {
            return null;
        }
        int pageCount = offsetIndex.getPageCount();
        List<ByteBuffer> minValues = columnIndex.getMinValues();
        List<ByteBuffer> maxValues = columnIndex.getMaxValues();
        List<Boolean> nullPages = columnIndex.getNullPages();
        List<Long> nullCounts = columnIndex.getNullCounts();
        if (minValues.size() != pageCount || maxValues.size() != pageCount) {
            return null;
        }
        List<long[]> surviving = new ArrayList<>();
        for (int page = 0; page < pageCount; page++) {
            long pageStart = offsetIndex.getFirstRowIndex(page);
            long pageEnd = (page + 1 < pageCount) ? offsetIndex.getFirstRowIndex(page + 1) : block.getRowCount();
            if (nullPages != null && page < nullPages.size() && Boolean.TRUE.equals(nullPages.get(page))) {
                if (dynamicThreshold.dominatesNulls(pageEnd - pageStart) == false) {
                    surviving.add(new long[] { pageStart, pageEnd });
                }
                continue;
            }
            Long rawMin = rawValueFromPageIndex(minValues.get(page));
            Long rawMax = rawValueFromPageIndex(maxValues.get(page));
            boolean hasNullCount = nullCounts != null && page < nullCounts.size();
            if (hasNullCount == false && dynamicThreshold.nullsFirst()) {
                surviving.add(new long[] { pageStart, pageEnd });
                continue;
            }
            long nullCount = hasNullCount ? nullCounts.get(page) : 0L;
            if (rawMin == null || rawMax == null || dynamicThreshold.dominates(rawMin, rawMax, nullCount) == false) {
                surviving.add(new long[] { pageStart, pageEnd });
            }
        }
        return RowRanges.fromUnsorted(surviving, block.getRowCount());
    }

    private ColumnChunkMetaData sortColumn(BlockMetaData block) {
        for (ColumnChunkMetaData column : block.getColumns()) {
            if (column.getPath().toDotString().equals(sortColumnPath)) {
                return column;
            }
        }
        return null;
    }

    /**
     * Extract a column's raw string statistic ({@code min}/{@code max}) as a {@link BytesRef} for
     * {@code BYTES_REF} thresholding. The column must be physically {@code BINARY} <em>and</em> carry
     * a {@link LogicalTypeAnnotation.StringLogicalTypeAnnotation}, which is the only annotation that
     * guarantees the column's statistics use the unsigned UTF-8 byte order of {@link BytesRef#compareTo}
     * and the encoded TopN bound. This mirrors the ORC reader's {@code isStringFamily} gate.
     * <p>
     * Restricting to that annotation is deliberately conservative. A {@code DECIMAL}-as-binary column
     * (which a UNION_BY_NAME shape can route here when the logical column is a string) computes its
     * min/max with a <em>numeric</em> comparator, so reusing those bounds against a string bound would
     * be incorrect; other unannotated {@code BINARY} columns (raw bytes, JSON/BSON) are byte-comparable
     * in principle but are skipped here for safety and ORC parity rather than pruned. All such columns
     * return {@code null} so the row group is never skipped.
     */
    private BytesRef bytesRefFromStats(Object value) {
        if (value instanceof Binary binary
            && sortColumnPrimitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY
            && sortColumnPrimitiveType.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
            return new BytesRef(binary.getBytes());
        }
        return null;
    }

    private Long rawValueFromStats(Object value) {
        if (value == null) {
            return null;
        }
        ElementType elementType = dynamicThreshold.elementType();
        return switch (elementType) {
            case LONG -> sortColumnPrimitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT64
                ? ((Number) value).longValue()
                : null;
            case INT -> sortColumnPrimitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32
                ? (long) ((Number) value).intValue()
                : null;
            case DOUBLE -> {
                if (sortColumnPrimitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.DOUBLE
                    || sortColumnPrimitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.FLOAT) {
                    double d = ((Number) value).doubleValue();
                    yield Double.isNaN(d) ? null : NumericUtils.doubleToSortableLong(d);
                }
                yield null;
            }
            case BOOLEAN -> sortColumnPrimitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BOOLEAN
                ? (((Boolean) value) ? 1L : 0L)
                : null;
            default -> null;
        };
    }

    private Long rawValueFromPageIndex(ByteBuffer value) {
        if (value == null || value.remaining() == 0) {
            return null;
        }
        // Parquet page-index bounds use the type's plain encoding; numeric plain values are little-endian.
        ByteBuffer ordered = value.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        return switch (dynamicThreshold.elementType()) {
            case LONG -> sortColumnPrimitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT64 ? ordered.getLong() : null;
            case INT -> sortColumnPrimitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32
                ? (long) ordered.getInt()
                : null;
            case DOUBLE -> switch (sortColumnPrimitiveType.getPrimitiveTypeName()) {
                case FLOAT -> {
                    float f = ordered.getFloat();
                    yield Float.isNaN(f) ? null : NumericUtils.doubleToSortableLong(f);
                }
                case DOUBLE -> {
                    double d = ordered.getDouble();
                    yield Double.isNaN(d) ? null : NumericUtils.doubleToSortableLong(d);
                }
                default -> null;
            };
            case BOOLEAN -> sortColumnPrimitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BOOLEAN
                ? (ordered.get() != 0 ? 1L : 0L)
                : null;
            default -> null;
        };
    }

    private static final long DEEPER_PREFETCH_BYTES = 32_000_000L;
    private static final long SHALLOW_PREFETCH_BYTES = 8_000_000L;
    private static final int MAX_PREFETCH_DEPTH = 8;
    private static final int PREFETCH_DEPTH_GROWTH = 2;
    private static final int SHRINK_AFTER_NO_STALLS = 3;
    private static final double BREAKER_GROWTH_THRESHOLD = 0.75;

    /**
     * Computes the initial (floor) prefetch depth from the projected byte footprint of the
     * first row group. This value serves as the floor for the adaptive depth — the runtime
     * stall detector may increase depth up to {@link #MAX_PREFETCH_DEPTH} but never below
     * this byte-based result.
     */
    private static int computePrefetchDepth(List<BlockMetaData> rowGroups, Set<String> projectedColumnPaths) {
        if (rowGroups.isEmpty()) {
            return 1;
        }
        BlockMetaData block = rowGroups.get(0);
        long projectedBytes = 0;
        for (ColumnChunkMetaData col : block.getColumns()) {
            if (projectedColumnPaths.contains(col.getPath().toDotString())) {
                projectedBytes += col.getTotalSize();
            }
        }
        // Larger projected sizes need deeper prefetch: an S3 GET for a 20MB string column takes
        // ~200ms while decode takes ~10ms, so single-ahead can't hide the latency. The circuit
        // breaker caps actual memory regardless of depth.
        //
        // Two-phase note: under two-phase the queued prefetches carry only predicate columns, so
        // the actual queued bytes are a fraction of {@code projectedBytes}. We intentionally keep
        // the depth sized on the full projection footprint because the synchronous Phase-2 fetch
        // we issue inside {@code advanceRowGroup} is what dominates per-row-group latency under
        // two-phase, and a deeper queue lets Phase 1 of the next row group overlap that wait.
        if (projectedBytes > DEEPER_PREFETCH_BYTES) {
            return 3;
        }
        if (projectedBytes > SHALLOW_PREFETCH_BYTES) {
            return 2;
        }
        return 1;
    }

    private static Set<String> buildProjectedColumnPaths(ColumnInfo[] columnInfos) {
        Set<String> paths = new HashSet<>();
        for (ColumnInfo info : columnInfos) {
            // The synthetic row-position slot has no Parquet descriptor — skip it: column-path
            // sets feed the prefetcher and column-reader-store wiring, both of which only deal
            // with real on-disk columns. {@link #buildRowPositionBlock} fills its slot directly.
            if (info != null && info.isRowPosition() == false) {
                paths.add(String.join(".", info.descriptor().getPath()));
            }
        }
        return paths;
    }

    /**
     * Builds an immutable subset of {@link #projectedColumnPaths}: when {@code wantPredicate} is
     * {@code true} the result contains the dotted paths of predicate columns, otherwise it
     * contains the projection-only columns (i.e., projected columns that are not predicate
     * columns).
     */
    private static Set<String> buildColumnPaths(ColumnInfo[] columnInfos, boolean[] isPredicateColumn, boolean wantPredicate) {
        Set<String> paths = new HashSet<>();
        for (int i = 0; i < columnInfos.length; i++) {
            if (columnInfos[i] != null && columnInfos[i].isRowPosition() == false && isPredicateColumn[i] == wantPredicate) {
                paths.add(String.join(".", columnInfos[i].descriptor().getPath()));
            }
        }
        return paths;
    }

    /**
     * Predicate-byte ratio threshold above which two-phase I/O is disabled because the projection
     * columns are not large enough relative to predicate columns to justify the extra round trip.
     * The single-phase late-mat path remains in effect in that case — late-mat itself has no
     * byte-ratio gate (it is enabled whenever the iterator has projection-only columns to defer
     * decode for); only the more expensive two-phase prefetch is gated here. The threshold is
     * deliberately conservative: two-phase amortizes a sequential predicate-then-projection
     * dependency and only wins when the projection columns clearly dominate the byte footprint.
     */
    static final double TWO_PHASE_PREDICATE_BYTE_RATIO_THRESHOLD = 0.4;

    /**
     * Returns {@code true} when the iterator should run with two-phase I/O for the lifetime of
     * this scan. The decision is fixed at construction time so the prefetch pipeline and the
     * decode loop can specialize accordingly without per-row-group branching.
     *
     * <p>Two-phase requires (a) late materialization to be active, (b) at least one
     * projection-only column (otherwise Phase 2 has nothing to fetch), (c) a storage object that
     * benefits from skipping bytes (i.e., remote — proxied here via
     * {@link StorageObject#supportsNativeAsync()}; today this is the de-facto remote signal,
     * overridden by every cloud-backed {@code StorageObject} in the codebase — {@code S3StorageObject},
     * {@code GcsStorageObject}, {@code AzureStorageObject}, and {@code HttpStorageObject} — and not
     * by any local-only implementation. If a future {@code StorageObject} returns {@code true}
     * without remote-like fetch cost, this gate may activate two-phase where it isn't profitable;
     * the fallback is the single-phase late-mat path, so the worst case is wasted CPU on the
     * unnecessary survivor-mask plumbing rather than incorrect results.), (d) no list columns
     * (their decode path needs a unified {@code ColumnReadStoreImpl} that we don't split between
     * predicate and projection stores), and (e) the projected bytes must be dominated by
     * projection-only columns rather than predicate columns.
     */
    private static boolean shouldUseTwoPhase(
        boolean lateMaterialization,
        StorageObject storageObject,
        ColumnInfo[] columnInfos,
        List<BlockMetaData> rowGroups,
        Set<String> projectedColumnPaths,
        Set<String> predicateColumnPaths,
        Set<String> projectionOnlyColumnPaths,
        String fileLocation
    ) {
        if (lateMaterialization == false) {
            return false;
        }
        if (projectionOnlyColumnPaths.isEmpty() || predicateColumnPaths.isEmpty()) {
            return false;
        }
        if (storageObject == null || storageObject.supportsNativeAsync() == false) {
            return false;
        }
        for (ColumnInfo info : columnInfos) {
            if (info != null && info.maxRepLevel() > 0) {
                // List columns need a single ColumnReadStoreImpl across all columns of a row
                // group; we'd have to either build that store from a merged page-read store or
                // teach two-phase about list decoding. Defer to follow-up work and stay on the
                // single-phase path when any list column appears.
                return false;
            }
        }
        long predicateBytes = 0;
        long totalBytes = 0;
        for (BlockMetaData block : rowGroups) {
            for (ColumnChunkMetaData col : block.getColumns()) {
                String path = col.getPath().toDotString();
                if (projectedColumnPaths.contains(path) == false) {
                    continue;
                }
                long size = col.getTotalSize();
                totalBytes += size;
                if (predicateColumnPaths.contains(path)) {
                    predicateBytes += size;
                }
            }
        }
        if (totalBytes <= 0) {
            return false;
        }
        double ratio = (double) predicateBytes / totalBytes;
        if (ratio >= TWO_PHASE_PREDICATE_BYTE_RATIO_THRESHOLD) {
            logger.debug(
                "Two-phase I/O disabled for [{}]: predicate columns occupy [{}%] of projected bytes (>= threshold [{}%])",
                fileLocation,
                (long) (ratio * 100),
                (long) (TWO_PHASE_PREDICATE_BYTE_RATIO_THRESHOLD * 100)
            );
            return false;
        }
        logger.debug(
            "Two-phase I/O enabled for [{}]: predicate columns occupy [{}%] of projected bytes, projection-only columns [{}]",
            fileLocation,
            (long) (ratio * 100),
            projectionOnlyColumnPaths.size()
        );
        return true;
    }

    private static boolean[] classifyPredicateColumns(
        List<Attribute> attributes,
        ColumnInfo[] columnInfos,
        ParquetPushedExpressions pushed
    ) {
        boolean[] predicate = new boolean[columnInfos.length];
        if (pushed == null) {
            return predicate;
        }
        Set<String> predicateNames = pushed.predicateColumnNames();
        for (int i = 0; i < columnInfos.length; i++) {
            if (columnInfos[i] != null && predicateNames.contains(attributes.get(i).name())) {
                predicate[i] = true;
            }
        }
        return predicate;
    }

    private static boolean hasProjectionOnlyColumns(boolean[] isPredicateColumn, ColumnInfo[] columnInfos) {
        for (int i = 0; i < columnInfos.length; i++) {
            if (columnInfos[i] != null && isPredicateColumn[i] == false) {
                return true;
            }
        }
        return false;
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
        // Under two-phase, drain leading fully-filtered batches before deciding: the source-row
        // counter would otherwise claim there is data when every remaining batch is empty.
        if (twoPhase != null) {
            drainEmptyTwoPhaseBatches();
            if (twoPhase != null && twoPhase.hasMoreBatches() == false) {
                rowsRemainingInGroup = 0;
            }
        }
        // Serve rows already materialized for the current row group before consulting the
        // early-termination side channel. dynamicThreshold.noFurtherCandidates() is a volatile flag
        // that a concurrent TopN worker can flip at any moment (e.g. once a NULLS FIRST top-K
        // saturates with nulls). Checking it first would let a flip land between a caller's hasNext()
        // and its next() and turn an already-available batch into a NoSuchElementException — the race
        // behind the intermittent "Unexpected failure reading external source" surfaced from next().
        // Early termination still takes effect below, where it stops us from advancing to (and
        // prefetching) any further row groups; the only extra work is draining the already-decoded,
        // in-memory current group, which the TopN simply discards.
        if (rowsRemainingInGroup > 0) {
            return true;
        }
        if (dynamicThreshold != null && dynamicThreshold.noFurtherCandidates()) {
            exhausted = true;
            cancelPendingPrefetch();
            return false;
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

    /**
     * Skips past any leading fully-filtered batches in the current two-phase row group, advancing
     * the projection {@link PageColumnReader}s by the same number of source rows so subsequent
     * survivor-bearing batches stay aligned with their internal row cursor.
     */
    private void drainEmptyTwoPhaseBatches() {
        TwoPhaseRowGroup state = twoPhase;
        while (state.hasMoreBatches() && state.currentSurvivorCount() == 0) {
            int skippedSource = state.currentSourceRows();
            advanceProjectionReadersBy(skippedSource);
            Block[] empty = state.takeCurrentPredicateBlocks();
            Releasables.closeExpectNoException(empty);
            pageBatchIndexInRowGroup++;
            rowsRemainingInGroup -= skippedSource;
        }
    }

    /**
     * Returns the per-row-group dictionary bitmap memo for the current {@link #rowGroupOrdinal},
     * resetting it on a row-group transition. The memo is owned by the iterator and shared
     * with {@link ParquetPushedExpressions#evaluateFilter}; the caller writes new bitmaps to
     * it as predicates are evaluated and reads them on subsequent batches within the same
     * row group. Returns {@code null} when late materialization is off.
     */
    private Map<Expression, boolean[]> dictionaryBitmapsForCurrentRowGroup() {
        if (dictionaryBitmaps == null) {
            return null;
        }
        if (dictionaryBitmapsRowGroup != rowGroupOrdinal) {
            dictionaryBitmaps.clear();
            dictionaryBitmapsRowGroup = rowGroupOrdinal;
        }
        return dictionaryBitmaps;
    }

    /**
     * Advances to the next surviving row group: applies the row-group statistics filter to skip
     * dropped row groups, then builds a {@link PrefetchedPageReadStore} from the prefetched
     * (or sync-fetched) bytes for the surviving row group. Triggers prefetch for the row group
     * after that.
     */
    private boolean advanceRowGroup() throws IOException {
        closeTwoPhaseState();
        if (rowGroup != null) {
            rowGroup.close();
            rowGroup = null;
        }

        // Loop is for the two-phase "all rows filtered out" case, where the current row group
        // contributes zero rows and we must immediately attempt the next surviving ordinal.
        // Single-phase always returns within the first iteration via the standard return below.
        // No cache wipe needed here: dictionaryBitmapsForCurrentRowGroup() self-invalidates on
        // a row-group ordinal mismatch, so any retry through this loop with a different
        // rowGroupOrdinal will see a fresh map at the next evaluateFilter call.
        while (true) {
            int nextOrdinal = nextSurvivingRowGroupOrdinal(rowGroupOrdinal + 1);
            if (nextOrdinal >= reader.getRowGroups().size()) {
                exhausted = true;
                cancelPendingPrefetch();
                releaseCurrentReservation();
                logIteratorDiagnostics();
                return false;
            }
            rowGroupOrdinal = nextOrdinal;
            pageBatchIndexInRowGroup = 0;

            BlockMetaData block = reader.getRowGroups().get(rowGroupOrdinal);
            if (rowGroupDominatedByThreshold(block)) {
                continue;
            }
            // Per-row-group trivially-passes check: when stats prove every row matches the
            // filter, the late-materialization machinery (decode predicate columns → evaluate
            // filter → compact survivors) is pure overhead. Switching to the standard path
            // eliminates filter evaluation. Note: when the trivially-passes case applies we
            // also force the single-phase code path even if useTwoPhase is enabled, since
            // there are no survivors-only pages to skip in Phase 2.
            currentRowGroupTriviallyPasses = triviallyPassesPredicate != null
                && TriviallyPassesChecker.check(triviallyPassesPredicate, block);
            if (currentRowGroupTriviallyPasses) {
                rowGroupsWithTrivialFilter++;
            }
            NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = takePendingPrefetch(rowGroupOrdinal);
            try {
                if (useTwoPhase) {
                    if (currentRowGroupTriviallyPasses) {
                        // Phase-1 chunks contain only predicate columns; the trivially-passes path
                        // bypasses late-mat entirely and decodes every projected column through
                        // {@link #nextStandard}. Synchronously fetch the projection columns now so
                        // the standard read path sees a complete row-group store.
                        prepareTwoPhaseTriviallyPassesRowGroup(block, chunks);
                        triggerNextRowGroupPrefetch();
                        return rowsRemainingInGroup > 0;
                    }
                    // Two-phase decode: pre-decode predicate columns from chunks, accumulate the
                    // global survivor mask, fetch projection columns for surviving pages only, and
                    // emit per-batch results in nextTwoPhaseBatch. When all rows are filtered out
                    // we drop the row group entirely and continue with the next survivor.
                    boolean prepared = prepareTwoPhaseRowGroup(block, chunks);
                    if (prepared == false) {
                        // All rows filtered out; loop and try the next surviving row group.
                        continue;
                    }
                    triggerNextRowGroupPrefetch();
                    return rowsRemainingInGroup > 0;
                }

                RowRanges currentRowRanges = resolveCurrentRowRanges(block);
                if (currentRowRanges != null && currentRowRanges.isEmpty()) {
                    continue;
                }
                // When late materialization is active, skip ColumnIndex page filtering — late-mat
                // handles row-level filtering itself via the survivor mask. Applying both
                // ColumnIndex RowRanges AND late-mat evaluation causes double-filtering that
                // drops rows incorrectly. The trivially-passes case is handled the same way:
                // we already know all rows match, so leaving page filtering off is consistent and
                // safe (RowRanges would be all() anyway).
                RowRanges buildRowRanges = lateMaterialization ? null : currentRowRanges;
                rowGroup = PrefetchedRowGroupBuilder.build(
                    block,
                    rowGroupOrdinal,
                    projectedSchema,
                    projectedColumnPaths,
                    buildRowRanges,
                    preloadedMetadata,
                    chunks,
                    storageObject,
                    codecFactory,
                    blockFactory.arrowAllocator()
                );
                rowsRemainingInGroup = buildRowRanges != null ? buildRowRanges.selectedRowCount() : rowGroup.getRowCount();
                triggerNextRowGroupPrefetch();
                initColumnReaders(buildRowRanges);
                return rowsRemainingInGroup > 0;
            } catch (Exception e) {
                releaseCurrentReservation();
                throw e;
            }
        }
    }

    private void logIteratorDiagnostics() {
        if (rowsEliminatedByLateMaterialization > 0) {
            logger.debug("Late materialization eliminated [{}] rows in [{}]", rowsEliminatedByLateMaterialization, fileLocation);
        }
        if (rowGroupsWithTrivialFilter > 0) {
            logger.debug(
                "Trivially-passes guard skipped late-materialization for [{}] row groups in [{}]",
                rowGroupsWithTrivialFilter,
                fileLocation
            );
        }
        if (twoPhaseRowGroupsWithPageSkipping > 0 || twoPhaseRowGroupsFullProjectionFetch > 0 || twoPhaseRowGroupsAllFiltered > 0) {
            logger.debug(
                "Two-phase I/O for [{}]: page-skipping [{}], full-projection-fetch [{}], all-filtered [{}]",
                fileLocation,
                twoPhaseRowGroupsWithPageSkipping,
                twoPhaseRowGroupsFullProjectionFetch,
                twoPhaseRowGroupsAllFiltered
            );
        }
    }

    /**
     * Returns the smallest row group ordinal {@code >= from} that survives the pre-computed
     * row-group level filter. When no filter is set, {@code from} is returned unchanged.
     * O(1) thanks to the {@link #nextSurvivor} lookup built in the constructor.
     */
    private int nextSurvivingRowGroupOrdinal(int from) {
        if (nextSurvivor == null) {
            return from;
        }
        if (from >= nextSurvivor.length) {
            return nextSurvivor.length;
        }
        return nextSurvivor[from];
    }

    private static int[] buildNextSurvivorLookup(boolean[] survivingRowGroups) {
        if (survivingRowGroups == null) {
            return null;
        }
        int[] next = UninitializedArrays.newIntArray(survivingRowGroups.length);
        int last = survivingRowGroups.length;
        for (int i = survivingRowGroups.length - 1; i >= 0; i--) {
            next[i] = survivingRowGroups[i] ? i : last;
            if (survivingRowGroups[i]) {
                last = i;
            }
        }
        return next;
    }

    /**
     * Locates the index of the synthetic {@link ColumnExtractor#ROW_POSITION_COLUMN} slot in the
     * column-info array, or returns {@code -1} when the projection does not include it. Identity
     * comparison against {@link ColumnInfo#rowPosition()} is what makes this unambiguous: a real
     * user column also typed {@code LONG} would have its own descriptor-bound {@link ColumnInfo},
     * never the shared sentinel.
     */
    private static int findRowPositionSlot(ColumnInfo[] columnInfos) {
        for (int i = 0; i < columnInfos.length; i++) {
            if (columnInfos[i] != null && columnInfos[i].isRowPosition()) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Builds a per-row-group prefix sum of {@link BlockMetaData#getRowCount()} entries in footer
     * iteration order. Index {@code i} holds the file-global row index of the first row in row
     * group {@code i}; index {@code length} holds the total file row count. This is the address
     * space the deferred extraction path uses to look rows up later — building it once at
     * iterator construction guarantees all emit paths share the same view of file-global
     * row identities, regardless of which row groups are skipped at scan time.
     */
    private static long[] buildRowGroupFirstRowGlobal(List<BlockMetaData> rowGroups) {
        long[] offsets = new long[rowGroups.size() + 1];
        long sum = 0L;
        for (int i = 0; i < rowGroups.size(); i++) {
            offsets[i] = sum;
            sum += rowGroups.get(i).getRowCount();
        }
        offsets[rowGroups.size()] = sum;
        return offsets;
    }

    /**
     * Returns the pre-computed {@link RowRanges} for the current row group. {@code rowGroupOrdinal}
     * is the physical block index in the file (we walk row groups ourselves now), so it directly
     * indexes {@code allRowRanges}. Slots for row groups dropped by the row-group level filter are
     * left {@code null} on purpose by {@code ParquetFormatReader} - those ordinals are skipped via
     * {@link #nextSurvivingRowGroupOrdinal} so this method never sees them, but we guard against a
     * stray {@code null} just in case.
     */
    private RowRanges resolveCurrentRowRanges(BlockMetaData block) {
        RowRanges ranges = null;
        if (allRowRanges != null && rowGroupOrdinal < allRowRanges.length) {
            ranges = allRowRanges[rowGroupOrdinal];
        }
        RowRanges thresholdRanges = thresholdRowRanges(rowGroupOrdinal, block);
        if (thresholdRanges != null) {
            ranges = ranges == null ? thresholdRanges : ranges.intersect(thresholdRanges);
        }
        if (ranges == null) {
            return null;
        }
        assert ranges.totalRows() == block.getRowCount()
            : "RowRanges total rows ["
                + ranges.totalRows()
                + "] != row group row count ["
                + block.getRowCount()
                + "] at ordinal ["
                + rowGroupOrdinal
                + "]";
        return ranges;
    }

    /**
     * Phase 1 of the two-phase decode pipeline: decodes predicate columns, accumulates a
     * row-group-wide survivor mask, then issues a synchronous Phase-2 prefetch for the
     * projection columns and stitches the decoded predicate blocks together with projection
     * {@link PageColumnReader}s ready to emit per-batch.
     *
     * @param block metadata for the row group being prepared
     * @param phase1Chunks Phase-1 prefetched chunks (predicate columns only); may be {@code null}
     *            when the prefetch failed and we need to fall back to a synchronous phase-1 build
     * @return {@code true} if the row group has at least one surviving row and is ready to emit;
     *         {@code false} when every row was filtered out and the iterator should advance to
     *         the next surviving row group ordinal
     */
    private boolean prepareTwoPhaseRowGroup(BlockMetaData block, NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> phase1Chunks) {
        long rowGroupRowCount = block.getRowCount();
        if (rowGroupRowCount > Integer.MAX_VALUE) {
            throw new IllegalStateException(
                "Two-phase decode does not support row groups with more than Integer.MAX_VALUE rows; row group ["
                    + rowGroupOrdinal
                    + "] in ["
                    + fileLocation
                    + "] has ["
                    + rowGroupRowCount
                    + "] rows"
            );
        }
        // Build predicate-only PageReadStore. We pass predicateColumnPaths so PrefetchedRowGroupBuilder
        // skips projection columns entirely; phase1Chunks were prefetched for the same subset, so
        // the storeObject fallback should not be needed except when prefetch failed (handled below
        // via the standard PrefetchedRowGroupBuilder.build code path).
        PrefetchedPageReadStore predicateStore = PrefetchedRowGroupBuilder.build(
            block,
            rowGroupOrdinal,
            projectedSchema,
            predicateColumnPaths,
            null,
            preloadedMetadata,
            phase1Chunks,
            storageObject,
            codecFactory,
            blockFactory.arrowAllocator()
        );
        rowGroup = predicateStore;
        initPredicateColumnReaders();

        // Pre-decode all predicate batches for this row group, accumulating the global survivor
        // mask. Predicate compacted blocks stay live in TwoPhaseRowGroup; we re-emit them
        // alongside fresh projection blocks during nextWithTwoPhase.
        WordMask globalSurvivors = new WordMask();
        globalSurvivors.reset((int) rowGroupRowCount);
        List<Block[]> predicateBatches = new ArrayList<>();
        List<int[]> survivorPositionsPerBatch = new ArrayList<>();
        List<Integer> sourceRowsPerBatch = new ArrayList<>();
        List<Integer> survivorCountsPerBatch = new ArrayList<>();
        long totalSurvivors = 0;
        long rowsConsumed = 0;
        try {
            while (rowsConsumed < rowGroupRowCount) {
                int rowsToRead = (int) Math.min(batchSize, rowGroupRowCount - rowsConsumed);
                BatchPredicateResult batch = decodePredicateBatch(rowsToRead, (int) rowsConsumed, globalSurvivors);
                predicateBatches.add(batch.compactedPredicateBlocks);
                survivorPositionsPerBatch.add(batch.survivorPositions);
                survivorCountsPerBatch.add(batch.survivorCount);
                sourceRowsPerBatch.add(rowsToRead);
                totalSurvivors += batch.survivorCount;
                rowsConsumed += rowsToRead;
            }
        } catch (Exception e) {
            // Mirror the success path's cleanup so a partially-decoded row group doesn't leak the
            // predicate PageReadStore / PageColumnReaders even if the caller forgets to close()
            // the iterator. close() itself is still defensive against this state, so doing the
            // teardown here just narrows the leak window to "exception propagated and iterator
            // immediately discarded".
            for (Block[] arr : predicateBatches) {
                Releasables.closeExpectNoException(arr);
            }
            closePageColumnReaders();
            if (rowGroup != null) {
                try {
                    rowGroup.close();
                } catch (Exception closeEx) {
                    e.addSuppressed(closeEx);
                }
                rowGroup = null;
            }
            throw e;
        }

        // Predicate readers are exhausted; their per-column dictionary caches are still useful
        // only if we kept reading predicates, which we don't. Releasing them now frees the
        // BytesRefArray refcounts before we hand the projection store to the same field.
        closePageColumnReaders();
        rowGroup.close();
        rowGroup = null;
        // Phase 1 chunks are no longer referenced by any reader: dictionary entries were
        // deep-copied into their own BytesRefArray, plain values were copied into compacted
        // predicate Blocks, and the page-column-readers themselves have been closed. Releasing
        // the breaker reservation here halves the memory footprint during Phase 2 fetch +
        // decode, which is the regime where the iterator typically holds the most memory.
        releaseCurrentReservation();

        if (totalSurvivors == 0) {
            twoPhaseRowGroupsAllFiltered++;
            rowsEliminatedByLateMaterialization += rowGroupRowCount;
            for (Block[] arr : predicateBatches) {
                Releasables.closeExpectNoException(arr);
            }
            return false;
        }
        rowsEliminatedByLateMaterialization += (rowGroupRowCount - totalSurvivors);

        RowRanges survivorRanges = WordMaskRowRangesConverter.fromWordMask(globalSurvivors, rowGroupRowCount);
        // Phase-2 fetch path:
        // - If projection-only columns is empty (shouldn't happen because the gate excludes that
        // case, but defend against it): no fetch needed, projection store is empty.
        // - If survivorRanges.shouldDiscard(): the survivors are too dense or fragmented to
        // benefit from page skipping; fetch full projection chunks instead.
        // - Otherwise: fetch only pages that overlap survivor ranges.
        boolean usePageFiltering = survivorRanges.shouldDiscard() == false;
        PrefetchedPageReadStore projectionStore;
        try {
            projectionStore = fetchProjectionPhase(block, survivorRanges, usePageFiltering);
        } catch (RuntimeException e) {
            for (Block[] arr : predicateBatches) {
                Releasables.closeExpectNoException(arr);
            }
            throw e;
        }
        if (usePageFiltering) {
            twoPhaseRowGroupsWithPageSkipping++;
        } else {
            twoPhaseRowGroupsFullProjectionFetch++;
        }

        TwoPhaseRowGroup state = new TwoPhaseRowGroup(
            predicateBatches,
            survivorPositionsPerBatch,
            toIntArray(survivorCountsPerBatch),
            toIntArray(sourceRowsPerBatch),
            totalSurvivors,
            projectionStore,
            // Pass the survivor RowRanges to the projection PageColumnReaders only when page
            // filtering is actually in play; otherwise we have full chunks and the readers
            // should not skip pages.
            usePageFiltering ? survivorRanges : null
        );
        this.twoPhase = state;
        // The iterator's row counter tracks source rows so existing per-batch bookkeeping
        // (row budget, batch index, etc.) keeps working unchanged. Survivor count is
        // applied per-batch via TwoPhaseRowGroup state.
        rowsRemainingInGroup = rowGroupRowCount;
        rowGroup = projectionStore;
        initProjectionColumnReaders(state.survivorRowRanges());
        return true;
    }

    /**
     * Decodes one batch of predicate columns and updates {@code globalSurvivors} for the rows
     * that survived. Returns the compacted predicate {@link Block}s for the batch alongside the
     * per-batch survivor positions used to align projection column decoding.
     *
     * <p>The per-batch survivor mask is materialized inside {@code pushedExpressions.evaluateFilter}
     * which reuses {@link #survivorMask}; we then OR-set the corresponding bits into
     * {@code globalSurvivors} at offset {@code rowsConsumedSoFar}.
     */
    private BatchPredicateResult decodePredicateBatch(int rowsToRead, int rowsConsumedSoFar, WordMask globalSurvivors) {
        Block[] predicateBlocks = new Block[attributes.size()];
        Map<String, Block> predicateBlockMap = new HashMap<>();
        try {
            for (int col = 0; col < columnInfos.length; col++) {
                if (isPredicateColumn[col] == false) {
                    continue;
                }
                ColumnInfo info = columnInfos[col];
                if (info == null) {
                    predicateBlocks[col] = blockFactory.newConstantNullBlock(rowsToRead);
                } else {
                    predicateBlocks[col] = readColumnBlockNoCleanup(col, info, rowsToRead);
                }
                predicateBlockMap.put(attributes.get(col).name(), predicateBlocks[col]);
            }
            WordMask mask = pushedExpressions.evaluateFilter(
                predicateBlockMap,
                rowsToRead,
                survivorMask,
                dictionaryBitmapsForCurrentRowGroup()
            );
            int survivorCount;
            int[] positions;
            if (mask == null) {
                // All rows survive in this batch.
                survivorCount = rowsToRead;
                positions = null;
                // Mark all rowsToRead bits in the global mask.
                globalSurvivors.setRange(rowsConsumedSoFar, rowsConsumedSoFar + rowsToRead);
            } else {
                survivorCount = mask.popCount();
                positions = survivorCount == 0 || survivorCount == rowsToRead ? null : mask.survivingPositions();
                if (survivorCount == rowsToRead) {
                    globalSurvivors.setRange(rowsConsumedSoFar, rowsConsumedSoFar + rowsToRead);
                } else if (survivorCount > 0) {
                    int[] toMark = positions != null ? positions : mask.survivingPositions();
                    for (int p : toMark) {
                        globalSurvivors.set(rowsConsumedSoFar + p);
                    }
                }
            }
            // Compact predicate blocks to surviving rows. When all rows survive (positions==null
            // and survivorCount==rowsToRead) we keep the full-size blocks as-is.
            if (positions != null) {
                for (int col = 0; col < columnInfos.length; col++) {
                    if (isPredicateColumn[col] && predicateBlocks[col] != null) {
                        predicateBlocks[col] = PageColumnReader.filterBlock(predicateBlocks[col], positions, survivorCount, blockFactory);
                    }
                }
            } else if (survivorCount == 0) {
                // Nothing survives: drop the predicate blocks; the batch contributes 0 rows.
                for (int col = 0; col < columnInfos.length; col++) {
                    if (predicateBlocks[col] != null) {
                        predicateBlocks[col].close();
                        predicateBlocks[col] = null;
                    }
                }
            }
            return new BatchPredicateResult(predicateBlocks, positions, survivorCount);
        } catch (RuntimeException e) {
            Releasables.closeExpectNoException(predicateBlocks);
            throw e;
        }
    }

    /** Result of decoding and filtering a single predicate-column batch under two-phase. */
    private record BatchPredicateResult(Block[] compactedPredicateBlocks, int[] survivorPositions, int survivorCount) {}

    /**
     * Variant of {@link #prepareTwoPhaseRowGroup} for the trivially-passes case: every row matches
     * the filter, so late-materialization is bypassed and the standard read path is used. The
     * Phase-1 chunks already include the predicate columns; we synchronously fetch the projection
     * columns and merge them into a unified row-group store so {@link #nextStandard} can read all
     * columns from a single store.
     */
    private void prepareTwoPhaseTriviallyPassesRowGroup(
        BlockMetaData block,
        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> phase1Chunks
    ) {
        ColumnChunkPrefetcher.PrefetchedChunks phase2Result;
        try {
            phase2Result = ColumnChunkPrefetcher.prefetchAsync(
                storageObject,
                block,
                projectionOnlyColumnPaths,
                blockFactory.arrowAllocator()
            ).join();
        } catch (Exception e) {
            // No manual breaker accounting here: the Arrow allocator that backs the
            // prefetch's direct memory automatically releases the reservation when the
            // failed future is drained by the caller's cleanup path.
            throw new IllegalArgumentException(
                "Trivially-passes Phase-2 fetch failed for row group ["
                    + rowGroupOrdinal
                    + "] in ["
                    + fileLocation
                    + "]: "
                    + e.getMessage(),
                e
            );
        }
        // Merge the two chunk maps. Both phases prefetched disjoint columns (predicate vs.
        // projection-only), so file offsets cannot collide.
        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> phase2Chunks = phase2Result != null ? phase2Result.chunks() : null;
        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> merged = new TreeMap<>();
        if (phase1Chunks != null) {
            merged.putAll(phase1Chunks);
        }
        if (phase2Chunks != null) {
            merged.putAll(phase2Chunks);
        }
        // Compose the Phase-1 (still live in currentChunksReleasable) and Phase-2 releasables so
        // both sets of chunks are freed together at the next row-group rollover. Breaker
        // accounting for both phases is tracked by the allocator-backed ArrowBufs they hold.
        Releasable phase1Releasable = currentChunksReleasable;
        Releasable phase2Releasable = phase2Result != null ? phase2Result.release() : () -> {};
        currentChunksReleasable = () -> Releasables.close(phase1Releasable, phase2Releasable);
        rowGroup = PrefetchedRowGroupBuilder.build(
            block,
            rowGroupOrdinal,
            projectedSchema,
            projectedColumnPaths,
            null,
            preloadedMetadata,
            merged,
            storageObject,
            codecFactory,
            blockFactory.arrowAllocator()
        );
        rowsRemainingInGroup = rowGroup.getRowCount();
        initColumnReaders(null);
    }

    /**
     * Synchronously fetches the Phase-2 (projection-only) chunks for the current row group and
     * builds the projection {@link PrefetchedPageReadStore}. Breaker accounting for the fetched
     * bytes is tracked by the allocator-backed {@code ArrowBuf}s inside the returned releasable.
     *
     * @param block metadata for the row group
     * @param survivorRanges row ranges of survivors as produced by {@link WordMaskRowRangesConverter}
     * @param usePageFiltering when {@code true}, only pages overlapping {@code survivorRanges}
     *            are fetched; when {@code false}, full column chunks are fetched (i.e., page
     *            filtering would not pay off because survivors are too dense or fragmented)
     */
    private PrefetchedPageReadStore fetchProjectionPhase(BlockMetaData block, RowRanges survivorRanges, boolean usePageFiltering) {
        ColumnChunkPrefetcher.PrefetchedChunks result;
        try {
            CompletableFuture<ColumnChunkPrefetcher.PrefetchedChunks> future = usePageFiltering
                ? ColumnChunkPrefetcher.prefetchAsync(
                    storageObject,
                    block,
                    projectionOnlyColumnPaths,
                    survivorRanges,
                    preloadedMetadata,
                    rowGroupOrdinal,
                    block.getRowCount(),
                    blockFactory.arrowAllocator()
                )
                : ColumnChunkPrefetcher.prefetchAsync(storageObject, block, projectionOnlyColumnPaths, blockFactory.arrowAllocator());
            result = future.join();
        } catch (Exception e) {
            // No manual breaker accounting here: the Arrow allocator that backs the
            // prefetch's direct memory automatically releases the reservation when the
            // failed future is drained by the caller's cleanup path.
            throw new IllegalArgumentException(
                "Phase 2 prefetch failed for row group [" + rowGroupOrdinal + "] in [" + fileLocation + "]: " + e.getMessage(),
                e
            );
        }
        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = result != null ? result.chunks() : null;
        // The Phase-2 chunks' allocator-backed memory is the sole breaker accounting for this row
        // group's projection columns; closing currentChunksReleasable returns those bytes. The
        // Phase-1 chunks were already released by the caller after predicate decode — assert that
        // here so any future refactor that breaks the precondition surfaces as a test failure
        // instead of a silent leak of Phase-1 bytes.
        assert currentChunksReleasable == null : "Phase-1 releasable must be closed before fetchProjectionPhase overwrites it";
        currentChunksReleasable = result != null ? result.release() : null;
        return PrefetchedRowGroupBuilder.build(
            block,
            rowGroupOrdinal,
            projectedSchema,
            projectionOnlyColumnPaths,
            usePageFiltering ? survivorRanges : null,
            preloadedMetadata,
            chunks,
            storageObject,
            codecFactory,
            blockFactory.arrowAllocator()
        );
    }

    /**
     * Initializes {@link #pageColumnReaders} for predicate columns only. Projection-only columns
     * are intentionally left {@code null}; the iterator routes their decode through
     * {@link #initProjectionColumnReaders} once Phase 2 has materialized.
     */
    private void initPredicateColumnReaders() {
        closePageColumnReaders();
        pageColumnReaders = new PageColumnReader[columnInfos.length];
        columnReaders = null;
        for (int i = 0; i < columnInfos.length; i++) {
            if (columnInfos[i] != null
                && columnInfos[i].isRowPosition() == false
                && isPredicateColumn[i]
                && columnInfos[i].maxRepLevel() == 0) {
                ColumnDescriptor desc = columnInfos[i].descriptor();
                PageReader pr = rowGroup.getPageReader(desc);
                pageColumnReaders[i] = new PageColumnReader(pr, desc, columnInfos[i], null, coercionWarnings());
            }
        }
    }

    /**
     * Initializes {@link #pageColumnReaders} for projection-only columns under the projection
     * {@link PrefetchedPageReadStore}. Predicate columns stay {@code null} because their decoded
     * blocks are already cached inside {@link #twoPhase}.
     */
    private void initProjectionColumnReaders(RowRanges survivorRowRanges) {
        closePageColumnReaders();
        pageColumnReaders = new PageColumnReader[columnInfos.length];
        columnReaders = null;
        for (int i = 0; i < columnInfos.length; i++) {
            if (columnInfos[i] != null
                && columnInfos[i].isRowPosition() == false
                && isPredicateColumn[i] == false
                && columnInfos[i].maxRepLevel() == 0) {
                ColumnDescriptor desc = columnInfos[i].descriptor();
                PageReader pr = rowGroup.getPageReader(desc);
                pageColumnReaders[i] = new PageColumnReader(pr, desc, columnInfos[i], survivorRowRanges, coercionWarnings());
            }
        }
    }

    private void initColumnReaders(RowRanges currentRowRanges) {
        // Close any readers from the previous row group before discarding the array — each reader
        // holds a ref-counted BytesRefArray (the cached dictionary) that must be released here.
        // Pages already emitted are unaffected: they own their own BytesRefArrayVector wrappers,
        // which hold independent refs to the underlying array.
        closePageColumnReaders();
        pageColumnReaders = new PageColumnReader[columnInfos.length];
        columnReaders = null;
        for (int i = 0; i < columnInfos.length; i++) {
            if (columnInfos[i] != null && columnInfos[i].isRowPosition() == false && columnInfos[i].maxRepLevel() == 0) {
                ColumnDescriptor desc = columnInfos[i].descriptor();
                PageReader pr = rowGroup.getPageReader(desc);
                pageColumnReaders[i] = new PageColumnReader(pr, desc, columnInfos[i], currentRowRanges, coercionWarnings());
            }
        }
        boolean hasListColumns = false;
        for (int i = 0; i < columnInfos.length; i++) {
            if (columnInfos[i] != null && columnInfos[i].isRowPosition() == false && columnInfos[i].maxRepLevel() > 0) {
                hasListColumns = true;
                break;
            }
        }
        if (hasListColumns) {
            ColumnReadStoreImpl store = new ColumnReadStoreImpl(
                rowGroup,
                new ParquetColumnDecoding.NoOpGroupConverter(projectedSchema),
                projectedSchema,
                createdBy
            );
            columnReaders = new ColumnReader[columnInfos.length];
            for (int i = 0; i < columnInfos.length; i++) {
                if (columnInfos[i] != null && columnInfos[i].isRowPosition() == false && columnInfos[i].maxRepLevel() > 0) {
                    columnReaders[i] = store.getColumnReader(columnInfos[i].descriptor());
                }
            }
        }
    }

    /**
     * Dequeues the head of the prefetch queue if it matches {@code expectedOrdinal}, joining its
     * future and returning the prefetched chunks. Entries whose ordinals don't match (because
     * the stats filter skipped intermediate row groups) are cancelled and any direct memory they
     * had already produced is drained. Returns {@code null} when there is no usable prefetch
     * (queue empty, empty result, or the prefetch failed — e.g. allocator tripped the breaker —
     * in which case the caller falls back to sync I/O for the requested row group).
     *
     * <p>On success the chunks' {@link Releasable} is handed off to
     * {@link #currentChunksReleasable} and released by {@link #releaseCurrentReservation} at
     * row-group rollover. The releasable owns the allocator-backed {@code ArrowBuf}s, which is
     * how the breaker accounting for the prefetched bytes is tracked.
     */
    private NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> takePendingPrefetch(int expectedOrdinal) {
        releaseCurrentReservation();

        while (pendingPrefetches.isEmpty() == false) {
            PendingPrefetch head = pendingPrefetches.peekFirst();
            if (head.ordinal() == expectedOrdinal) {
                break;
            }
            assert head.ordinal() <= expectedOrdinal : "prefetch queue has ordinal " + head.ordinal() + " > expected " + expectedOrdinal;
            pendingPrefetches.pollFirst();
            FutureUtils.cancel(head.future());
            head.release();
        }

        if (pendingPrefetches.isEmpty()) {
            return null;
        }

        PendingPrefetch head = pendingPrefetches.pollFirst();
        try {
            boolean wasReady = head.future().isDone();
            ColumnChunkPrefetcher.PrefetchedChunks result = head.future().join();
            adaptPrefetchDepth(wasReady);
            NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> data = result != null ? result.chunks() : null;
            if (data != null && data.isEmpty() == false) {
                logger.trace("Took [{}] prefetched column chunks for row group [{}] in [{}]", data.size(), expectedOrdinal, fileLocation);
                currentChunksReleasable = result.release();
                return data;
            }
            if (result != null) {
                // Empty result still owns the breaker-tracked direct memory until released; any
                // exception from release().close() (e.g. double-decrement of the underlying
                // ArrowBuf) is a real bug we want to surface rather than silently swallow.
                result.release().close();
            }
            return null;
        } catch (Exception e) {
            logger.debug(
                "Prefetch for row group [{}] failed in [{}], falling back to synchronous I/O: {}",
                expectedOrdinal,
                fileLocation,
                e.getMessage()
            );
            consecutiveNoStalls = 0;
            prefetchDepth = Math.max(prefetchDepthFloor, prefetchDepth - 1);
            return null;
        }
    }

    /**
     * Adjusts {@link #prefetchDepth} based on whether the consumed prefetch future was already
     * complete. A stall ({@code wasReady == false}) means the consumer outpaced the producer —
     * grow depth by {@link #PREFETCH_DEPTH_GROWTH} unless the circuit breaker is under pressure.
     * Sustained no-stalls mean the queue is deep enough — shrink by 1 after
     * {@link #SHRINK_AFTER_NO_STALLS} consecutive hits.
     */
    // Package-private for testing
    void adaptPrefetchDepth(boolean wasReady) {
        if (wasReady) {
            if (++consecutiveNoStalls >= SHRINK_AFTER_NO_STALLS) {
                prefetchDepth = Math.max(prefetchDepthFloor, prefetchDepth - 1);
                consecutiveNoStalls = 0;
            }
        } else {
            if (breakerPressure() < BREAKER_GROWTH_THRESHOLD) {
                prefetchDepth = Math.min(prefetchDepth + PREFETCH_DEPTH_GROWTH, MAX_PREFETCH_DEPTH);
            }
            consecutiveNoStalls = 0;
        }
    }

    /**
     * Returns the node-level circuit breaker utilization (0.0–1.0). Intentionally node-level:
     * a lightweight query should still back off when the node is under global memory pressure,
     * since deeper prefetch would compete with other concurrent queries for the same heap.
     */
    private double breakerPressure() {
        long limit = breaker.getLimit();
        return limit <= 0 ? 0.0 : (double) breaker.getUsed() / limit;
    }

    // Visible for testing
    int prefetchDepth() {
        return prefetchDepth;
    }

    /**
     * Refills the prefetch queue after consuming an entry in {@link #advanceRowGroup()}.
     * Starts from the ordinal after the last queued entry (or after the current row group
     * if the queue is empty). Skips row groups dropped by the statistics filter.
     */
    private void triggerNextRowGroupPrefetch() {
        if (storageObject == null) {
            return;
        }
        int startOrdinal;
        if (pendingPrefetches.isEmpty()) {
            startOrdinal = nextSurvivingRowGroupOrdinal(rowGroupOrdinal + 1);
        } else {
            startOrdinal = nextSurvivingRowGroupOrdinal(pendingPrefetches.peekLast().ordinal() + 1);
        }
        fillPrefetchQueue(startOrdinal);
    }

    /**
     * Cancels all pending prefetches and drains their results so any direct memory the prefetch
     * may already have produced is released back to the breaker via the allocator. Called when
     * the iterator is exhausted or closed.
     */
    private void cancelPendingPrefetch() {
        while (pendingPrefetches.isEmpty() == false) {
            PendingPrefetch entry = pendingPrefetches.pollFirst();
            FutureUtils.cancel(entry.future());
            entry.release();
        }
    }

    /**
     * Frees the allocator-backed direct memory holding the chunks currently in use by the row
     * group. Called when those chunks are about to be replaced (next advance) or dropped (close).
     * Closing the releasable returns the bytes to the breaker via the Arrow allocator listener.
     * Idempotent.
     */
    private void releaseCurrentReservation() {
        if (currentChunksReleasable != null) {
            Releasable r = currentChunksReleasable;
            currentChunksReleasable = null;
            try {
                r.close();
            } catch (RuntimeException e) {
                logger.warn("Failed to release prefetched chunks for row group [{}] in [{}]", rowGroupOrdinal, fileLocation, e);
            }
        }
    }

    @Override
    public Page next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }
        if (twoPhase != null) {
            // Captured before nextTwoPhaseBatch decrements rowsRemainingInGroup: the in-block index
            // of the first source row this batch is about to emit. Same shape as the standard /
            // late-mat paths, just computed in the two-phase branch where the source-row delta is
            // queried from {@link TwoPhaseRowGroup#currentSourceRows} rather than {@link #batchSize}.
            int firstRowOfBatchInRG = (int) (reader.getRowGroups().get(rowGroupOrdinal).getRowCount() - rowsRemainingInGroup);
            return nextTwoPhaseBatch(firstRowOfBatchInRG);
        }
        int effectiveBatch = batchSize;
        if (rowBudget != FormatReader.NO_LIMIT) {
            effectiveBatch = Math.min(effectiveBatch, rowBudget);
        }
        int rowsToRead = (int) Math.min(effectiveBatch, rowsRemainingInGroup);
        // Captured before the row counts are subtracted: the in-block index of the first row this
        // batch is about to emit. The row-position injector uses it to compute file-global ids.
        int firstRowOfBatchInRG = (int) (reader.getRowGroups().get(rowGroupOrdinal).getRowCount() - rowsRemainingInGroup);

        boolean useLateMaterialization = lateMaterialization && currentRowGroupTriviallyPasses == false;
        Page result = useLateMaterialization
            ? nextWithLateMaterialization(rowsToRead, firstRowOfBatchInRG)
            : nextStandard(rowsToRead, firstRowOfBatchInRG);

        pageBatchIndexInRowGroup++;
        rowsRemainingInGroup -= rowsToRead;
        if (rowBudget != FormatReader.NO_LIMIT) {
            rowBudget -= useLateMaterialization ? result.getPositionCount() : rowsToRead;
        }
        return result;
    }

    /**
     * Emits the next batch under two-phase decoding. The predicate columns for the batch were
     * already decoded and compacted in {@link #prepareTwoPhaseRowGroup}; this method only has
     * to fetch the corresponding projection-column slice from the (already in-memory) projection
     * store and combine the two halves into a {@link Page}.
     *
     * <p>The row budget is enforced on emitted (post-filter) rows since downstream operators
     * see survivor counts and that's what {@code rowBudget} is meant to limit. We respect both
     * the budget and the survivor count of the current batch by emitting the smaller of the
     * two as the first row range; if a partial slice is needed, the remainder of this batch's
     * survivors is dropped (its predicate blocks are released so we don't leak).
     */
    private Page nextTwoPhaseBatch(int firstRowOfBatchInRG) {
        TwoPhaseRowGroup state = twoPhase;
        // hasNext() drains any leading fully-filtered batches before returning true, so the
        // current cursor must point at a batch with at least one survivor by the time we get
        // here. An assertion guards against an unexpected entry from a buggy hasNext path.
        assert state.hasMoreBatches() && state.currentSurvivorCount() > 0
            : "nextTwoPhaseBatch invoked on empty/missing batch (hasNext should have advanced or returned false)";
        int sourceRows = state.currentSourceRows();
        int survivorCount = state.currentSurvivorCount();
        int[] survivorPositions = state.currentSurvivorPositions();
        Block[] predicateBlocks = state.takeCurrentPredicateBlocks();

        // Ownership invariant for the rest of this method: every predicate Block lives in EXACTLY
        // one of {predicateBlocks[col], blocks[col]} at any moment. We enforce this by nulling
        // predicateBlocks[col] the instant we hand the reference off to blocks[col], so the catch
        // below never double-closes a transferred Block. readColumnBlockNoCleanup leaves cleanup
        // entirely to the outer catch (the sole owner of both arrays).
        //
        // The earlier implementation copied the reference into blocks[col] and only nulled
        // predicateBlocks[col] after the loop completed successfully. A mid-loop exception (e.g.
        // CircuitBreakingException from a projection allocation) then triggered a double-release
        // when the catch closed both arrays — observed as the production crash on q23 with a
        // selective LIKE filter.
        Block[] blocks = new Block[attributes.size()];
        int emitCount = survivorCount;
        boolean budgetExhaustsBatch = false;
        try {
            // Apply the row budget to survivor count: if the budget is smaller than the number of
            // surviving rows in this batch, truncate predicate and projection blocks to the first
            // {@code rowBudget} survivors and mark the iterator as exhausted after emission. We
            // re-slice predicate blocks via {@link #sliceBlockHead} (cheap because they're already
            // compacted) and pass {@code emitCount} to the projection read so it doesn't decode
            // beyond the budget either. This must run inside the try so that a mid-iteration
            // sliceBlockHead failure leaves cleanup to the catch rather than leaking the partially
            // re-sliced predicate array.
            if (rowBudget != FormatReader.NO_LIMIT && rowBudget < emitCount) {
                int newCount = rowBudget;
                int[] truncated;
                if (survivorPositions != null) {
                    truncated = Arrays.copyOf(survivorPositions, newCount);
                } else {
                    // All rows in this batch survived; the projection reader will read sourceRows
                    // and we'll filter to the first newCount via a synthesized positions array.
                    truncated = UninitializedArrays.newIntArray(newCount);
                    for (int i = 0; i < newCount; i++) {
                        truncated[i] = i;
                    }
                }
                survivorPositions = truncated;
                // sliceBlockHead either returns the same block (no slice needed) or closes the
                // source on success. predicateBlocks[col] is reassigned to the result before any
                // subsequent call so a failure on column N+1 leaves columns 0..N owned by
                // predicateBlocks[] for the catch to release.
                for (int col = 0; col < columnInfos.length; col++) {
                    if (isPredicateColumn[col] && predicateBlocks[col] != null) {
                        predicateBlocks[col] = sliceBlockHead(predicateBlocks[col], newCount);
                    }
                }
                emitCount = newCount;
                budgetExhaustsBatch = true;
            }

            for (int col = 0; col < columnInfos.length; col++) {
                if (isPredicateColumn[col]) {
                    // Transfer ownership: hand the reference to blocks[] and clear the
                    // predicateBlocks[] slot so the catch closes each Block at most once.
                    blocks[col] = predicateBlocks[col];
                    predicateBlocks[col] = null;
                    continue;
                }
                ColumnInfo info = columnInfos[col];
                if (info != null && info.isRowPosition()) {
                    // survivorPositions is null when every source row survived (and the budget did
                    // not truncate); the contiguous run is already aligned with firstRowOfBatchInRG.
                    // Otherwise survivorPositions indexes into the source batch in survivor order
                    // and may have been head-truncated by the row budget — either form yields the
                    // emitCount file-global ids the projection blocks correspond to one-to-one.
                    blocks[col] = buildRowPositionBlock(firstRowOfBatchInRG, emitCount, survivorPositions);
                    continue;
                }
                if (info == null) {
                    blocks[col] = blockFactory.newConstantNullBlock(emitCount);
                    continue;
                }
                if (survivorCount == 0) {
                    // Should not happen — TwoPhaseRowGroup never queues a fully-empty batch
                    // (those are dropped before reaching here). Defensive null block.
                    blocks[col] = blockFactory.newConstantNullBlock(0);
                    continue;
                }
                if (survivorPositions == null) {
                    // All sourceRows survived this batch. Use the no-cleanup variant: our catch
                    // owns both blocks[] and predicateBlocks[], and letting the helper close
                    // blocks here would double-close every previously-assigned slot (including
                    // transferred predicate Blocks) — that's the production crash signature.
                    Block fullBlock = readColumnBlockNoCleanup(col, info, sourceRows);
                    if (budgetExhaustsBatch) {
                        // sliceBlockHead returns the same block when sizes match (no slice), or
                        // closes source on success. On failure we own fullBlock and must close it.
                        try {
                            blocks[col] = sliceBlockHead(fullBlock, emitCount);
                        } catch (RuntimeException sliceEx) {
                            Releasables.closeExpectNoException(fullBlock);
                            throw sliceEx;
                        }
                    } else {
                        blocks[col] = fullBlock;
                    }
                } else if (pageColumnReaders != null && pageColumnReaders[col] != null) {
                    blocks[col] = pageColumnReaders[col].readBatchSparse(sourceRows, blockFactory, survivorPositions, emitCount);
                } else {
                    // Read the full source-rows block and immediately filter to survivors.
                    // We hand fullBlock to filterBlock which closes it on success; on failure
                    // (e.g. a breaker trip during the new filtered allocation) filterBlock does
                    // NOT close source, so we must close it explicitly to avoid a leak.
                    Block fullBlock = readColumnBlockNoCleanup(col, info, sourceRows);
                    try {
                        blocks[col] = PageColumnReader.filterBlock(fullBlock, survivorPositions, emitCount, blockFactory);
                    } catch (RuntimeException filterEx) {
                        Releasables.closeExpectNoException(fullBlock);
                        throw filterEx;
                    }
                }
            }
        } catch (CircuitBreakingException e) {
            // Invariant: blocks[] and predicateBlocks[] hold disjoint references at every point
            // in the loop above (eager null-on-transfer for predicate slots; readColumnBlockNoCleanup
            // for projection slots leaves blocks[] alone on failure). Closing both arrays here
            // therefore releases each Block exactly once. Surface the breaker exception unwrapped
            // to match the sibling iterator paths so upstream operators (LIMIT, exchange, breaker
            // telemetry) classify it correctly.
            Releasables.closeExpectNoException(blocks);
            Releasables.closeExpectNoException(predicateBlocks);
            throw e;
        } catch (RuntimeException e) {
            Releasables.closeExpectNoException(blocks);
            Releasables.closeExpectNoException(predicateBlocks);
            throw new IllegalArgumentException(
                "Failed to emit two-phase Page at row group ["
                    + (rowGroupOrdinal + 1)
                    + "] batch ["
                    + state.nextBatchIndex()
                    + "] in file ["
                    + fileLocation
                    + "]: "
                    + e.getMessage(),
                e
            );
        }

        pageBatchIndexInRowGroup++;
        rowsRemainingInGroup -= sourceRows;
        if (rowBudget != FormatReader.NO_LIMIT) {
            rowBudget -= emitCount;
        }
        if (budgetExhaustsBatch) {
            // Drop any remaining batches in this row group; the budget will short-circuit hasNext.
            // Their predicate blocks will be closed via closeTwoPhaseState on next advanceRowGroup
            // or close().
            rowsRemainingInGroup = 0;
        }
        counters.addRowsEmitted(emitCount);
        return new Page(blocks);
    }

    /**
     * Advances every active projection {@link PageColumnReader} by {@code rows} source positions
     * without producing blocks, so the readers stay aligned with the consumer's expected row
     * coordinate after we discarded a fully-filtered batch.
     */
    private void advanceProjectionReadersBy(int rows) {
        if (rows <= 0 || pageColumnReaders == null) {
            return;
        }
        for (int col = 0; col < columnInfos.length; col++) {
            if (isPredicateColumn[col]) {
                continue;
            }
            if (pageColumnReaders[col] != null) {
                pageColumnReaders[col].skipRows(rows);
            }
        }
    }

    private Block sliceBlockHead(Block source, int newCount) {
        if (newCount == source.getPositionCount()) {
            return source;
        }
        int[] head = UninitializedArrays.newIntArray(newCount);
        for (int i = 0; i < newCount; i++) {
            head[i] = i;
        }
        return PageColumnReader.filterBlock(source, head, newCount, blockFactory);
    }

    private void closeTwoPhaseState() {
        if (twoPhase != null) {
            twoPhase.close();
            twoPhase = null;
        }
    }

    private static int[] toIntArray(List<Integer> values) {
        int[] out = UninitializedArrays.newIntArray(values.size());
        for (int i = 0; i < values.size(); i++) {
            out[i] = values.get(i);
        }
        return out;
    }

    private Page nextStandard(int rowsToRead, int firstRowOfBatchInRG) {
        Block[] blocks = new Block[attributes.size()];
        int producedRows = -1;
        try {
            for (int col = 0; col < columnInfos.length; col++) {
                ColumnInfo info = columnInfos[col];
                if (info == null) {
                    continue;
                }
                if (info.isRowPosition()) {
                    // Standard path: every source row is emitted; positions are contiguous so the
                    // injector fills the block in run order.
                    blocks[col] = buildRowPositionBlock(firstRowOfBatchInRG, rowsToRead, null);
                    if (producedRows < 0) {
                        producedRows = rowsToRead;
                    }
                    continue;
                }
                try {
                    blocks[col] = readColumnBlock(col, info, rowsToRead);
                    if (producedRows < 0) {
                        producedRows = blocks[col].getPositionCount();
                    }
                } catch (CircuitBreakingException e) {
                    // Let breaker exceptions flow through unwrapped: callers (and tests) match
                    // on the exact type to distinguish memory-pressure failures from data errors.
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
            if (producedRows < 0) {
                producedRows = rowsToRead;
            }
            for (int col = 0; col < columnInfos.length; col++) {
                if (blocks[col] == null) {
                    blocks[col] = blockFactory.newConstantNullBlock(producedRows);
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
        counters.addRowsEmitted(rowsToRead);
        return new Page(blocks);
    }

    private Page nextWithLateMaterialization(int rowsToRead, int firstRowOfBatchInRG) {
        Block[] blocks = new Block[attributes.size()];
        try {
            // Phase 1: decode predicate columns
            Map<String, Block> predicateBlockMap = new HashMap<>();
            for (int col = 0; col < columnInfos.length; col++) {
                if (isPredicateColumn[col]) {
                    ColumnInfo info = columnInfos[col];
                    if (info == null) {
                        blocks[col] = blockFactory.newConstantNullBlock(rowsToRead);
                    } else {
                        blocks[col] = readColumnBlockNoCleanup(col, info, rowsToRead);
                    }
                    predicateBlockMap.put(attributes.get(col).name(), blocks[col]);
                }
            }
            // _rowPosition is never a predicate column (the optimizer never references it from a
            // pushed expression) so it's always handled below in Phase 3, where the survivor mask
            // is already known and the values can be filled in survivor order.

            // Phase 2: evaluate filter
            WordMask mask = pushedExpressions.evaluateFilter(
                predicateBlockMap,
                rowsToRead,
                survivorMask,
                dictionaryBitmapsForCurrentRowGroup()
            );

            int survivorCount = rowsToRead;
            int[] positions = null;
            if (mask != null) {
                survivorCount = mask.popCount();
                rowsEliminatedByLateMaterialization += (rowsToRead - survivorCount);
                if (survivorCount < rowsToRead) {
                    positions = mask.survivingPositions();
                    // Compact predicate blocks
                    for (int col = 0; col < columnInfos.length; col++) {
                        if (isPredicateColumn[col] && blocks[col] != null) {
                            blocks[col] = PageColumnReader.filterBlock(blocks[col], positions, survivorCount, blockFactory);
                        }
                    }
                }
            }

            // Phase 3: decode projection-only columns
            for (int col = 0; col < columnInfos.length; col++) {
                if (isPredicateColumn[col]) {
                    continue;
                }
                ColumnInfo info = columnInfos[col];
                if (info != null && info.isRowPosition()) {
                    // positions[] is null when every source row survives — the injector uses the
                    // contiguous run; otherwise it's the compacted survivor index list and the
                    // injector emits in survivor order (matching the other projection blocks).
                    blocks[col] = buildRowPositionBlock(firstRowOfBatchInRG, survivorCount, positions);
                    continue;
                }
                if (info == null) {
                    blocks[col] = blockFactory.newConstantNullBlock(survivorCount);
                } else if (survivorCount == 0) {
                    // Skip entirely
                    if (pageColumnReaders != null && pageColumnReaders[col] != null) {
                        pageColumnReaders[col].skipRows(rowsToRead);
                    } else if (columnReaders != null && columnReaders[col] != null) {
                        ParquetColumnDecoding.skipValues(columnReaders[col], rowsToRead);
                    }
                    blocks[col] = blockFactory.newConstantNullBlock(0);
                } else if (positions == null) {
                    blocks[col] = readColumnBlockNoCleanup(col, info, rowsToRead);
                } else if (pageColumnReaders != null && pageColumnReaders[col] != null) {
                    blocks[col] = pageColumnReaders[col].readBatchFiltered(rowsToRead, blockFactory, positions, survivorCount);
                } else {
                    Block fullBlock = readColumnBlockNoCleanup(col, info, rowsToRead);
                    blocks[col] = PageColumnReader.filterBlock(fullBlock, positions, survivorCount, blockFactory);
                }
            }

            // Fill any remaining null slots
            for (int col = 0; col < columnInfos.length; col++) {
                if (blocks[col] == null) {
                    blocks[col] = blockFactory.newConstantNullBlock(survivorCount);
                }
            }

            counters.addRowsEmitted(survivorCount);
            return new Page(blocks);
        } catch (IllegalArgumentException | CircuitBreakingException e) {
            Releasables.closeExpectNoException(blocks);
            throw e;
        } catch (Exception e) {
            Releasables.closeExpectNoException(blocks);
            throw new IllegalArgumentException(
                "Failed to create late-materialized Page at row group ["
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

    /**
     * Builds the {@code _rowPosition} block for a batch about to be emitted. When
     * {@code positions} is {@code null} the batch is contiguous: its rows start at
     * {@code firstRowOfBatchInRG} within the current row group and run for {@code count} entries.
     * Otherwise {@code positions[0..count)} indexes into the source batch (still based at
     * {@code firstRowOfBatchInRG}); the resulting file-global indexes follow whichever order the
     * caller arranged the survivors in.
     * <p>
     * Values are emitted pre-encoded with the registry-assigned extractor id installed via
     * {@link #setExtractorId(int)} — i.e. the returned block's values are
     * {@code (id << 48) | fileGlobalRowIndex}, ready for downstream operators to decode against
     * the matching {@code SourceExtractors} entry. Encoding here avoids the per-page
     * block-rebuild that a wrapping iterator would have to do; we already own the {@code long[]}
     * buffer and just OR the high bits in as we fill it. The returned block is a dense
     * {@link Block} backed by a {@link org.elasticsearch.compute.data.LongVector} — never null,
     * never with nulls.
     */
    private Block buildRowPositionBlock(int firstRowOfBatchInRG, int count, int[] positions) {
        assert rowPositionEncodingHighBits != -1L
            : "setExtractorId(int) must be called before _rowPosition is materialised — see ColumnExtractorProducer";
        long base = rowGroupFirstRowGlobal[rowGroupOrdinal] + firstRowOfBatchInRG;
        long encodingHighBits = rowPositionEncodingHighBits;
        long[] values = new long[count];
        if (positions == null) {
            for (int i = 0; i < count; i++) {
                values[i] = encodingHighBits | (base + i);
            }
        } else {
            for (int i = 0; i < count; i++) {
                values[i] = encodingHighBits | (base + positions[i]);
            }
        }
        return blockFactory.newLongArrayVector(values, count).asBlock();
    }

    /**
     * Reads one column block with exception attribution but without any cleanup of sibling blocks.
     * Every call site that builds a {@code blocks[]} (or {@code predicateBlocks[]}) array in a
     * loop must use this helper rather than closing the array on failure itself, because the outer
     * {@code catch} is the sole owner of that array: a helper that also closes it would
     * double-close every slot populated by previous loop iterations (including transferred predicate
     * slots), triggering {@code IllegalStateException: can't release already released object} that
     * masks the original circuit-breaker exception and leaks block memory.
     */
    private Block readColumnBlockNoCleanup(int colIndex, ColumnInfo info, int rowsToRead) {
        try {
            return readColumnBlock(colIndex, info, rowsToRead);
        } catch (CircuitBreakingException e) {
            throw e;
        } catch (Exception e) {
            throw wrapColumnReadException(colIndex, e);
        }
    }

    private IllegalArgumentException wrapColumnReadException(int colIndex, Exception e) {
        Attribute attr = attributes.get(colIndex);
        return new IllegalArgumentException(
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

    private Block readColumnBlock(int colIndex, ColumnInfo info, int rowsToRead) {
        if (pageColumnReaders != null && pageColumnReaders[colIndex] != null) {
            // PageColumnReader handles maxDefLevel > 1 for nested-flat STRUCT leaves (e.g.
            // `event.action`). maxRepLevel must stay 0 because LIST<STRUCT> is intentionally
            // unsupported in this PR — see issue elastic/esql-planning#435. Enforce the invariant
            // with a hard throw rather than an assertion so it survives production builds where
            // assertions are disabled — a LIST<STRUCT> leaf reaching this branch would otherwise
            // silently emit wrong data.
            if (info.maxRepLevel() != 0) {
                throw new IllegalStateException("PageColumnReader path expects maxRepLevel == 0 for [" + info + "]");
            }
            return pageColumnReaders[colIndex].readBatch(rowsToRead, blockFactory);
        }
        ColumnReader cr = columnReaders != null ? columnReaders[colIndex] : null;
        if (cr == null) {
            return blockFactory.newConstantNullBlock(rowsToRead);
        }
        if (info.maxRepLevel() > 0) {
            return ParquetColumnDecoding.readListColumn(
                cr,
                info,
                rowsToRead,
                blockFactory,
                attributes.get(colIndex).name(),
                coercionWarnings()
            );
        }
        ParquetColumnDecoding.skipValues(cr, rowsToRead);
        return blockFactory.newConstantNullBlock(rowsToRead);
    }

    /**
     * Lazily-created sink for per-value declared-coercion failures (capped response Warning
     * headers + nulled cells); shared by every column and row group of this iterator so the cap
     * is per read, not per column chunk. Returns {@code null} under {@code fail_fast} — the
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
                "Parquet file ["
                    + fileLocation
                    + "] has values that could not be coerced to the declared column type; "
                    + "they are returned as null"
            );
        }
        return coercionWarnings;
    }

    @Override
    public ColumnExtractor createColumnExtractor() {
        if (rowPositionColumnIndex < 0) {
            throw new IllegalStateException(
                "createColumnExtractor called on iterator without [" + ColumnExtractor.ROW_POSITION_COLUMN + "] in projection"
            );
        }
        // The iterator emits file-global identities, so the extractor sees the full footer
        // regardless of whether this iterator was opened on the full file or on a range. That's
        // the whole point of file-global addressing: any iterator that emits identities and any
        // extractor over the same file agree without coordination.
        return new ParquetColumnExtractor(storageObject, formatReader, fullFooter, errorPolicy);
    }

    @Override
    public void setExtractorId(int id) {
        if (rowPositionColumnIndex < 0) {
            throw new IllegalStateException(
                "setExtractorId called on iterator without [" + ColumnExtractor.ROW_POSITION_COLUMN + "] in projection"
            );
        }
        if (id < 0) {
            throw new IllegalArgumentException("extractor id [" + id + "] must be non-negative");
        }
        if (rowPositionEncodingHighBits != -1L) {
            throw new IllegalStateException("setExtractorId already called on this iterator");
        }
        // Pre-shift once so the per-row hot loop in buildRowPositionBlock is a single OR. The
        // shift width comes from the SPI ({@link ColumnExtractor#LOCAL_POSITION_BITS}), so this
        // module's only contract with the host plugin is the SPI itself — not the registry impl.
        rowPositionEncodingHighBits = ((long) id) << ColumnExtractor.LOCAL_POSITION_BITS;
    }

    @Override
    public void close() throws IOException {
        cancelPendingPrefetch();
        try {
            closeTwoPhaseState();
            closePageColumnReaders();
            if (rowGroup != null) {
                rowGroup.close();
                rowGroup = null;
            }
        } finally {
            releaseCurrentReservation();
            try {
                if (preloadedMetadata != null) {
                    preloadedMetadata.close();
                }
            } finally {
                reader.close();
            }
        }
    }

    /**
     * Closes any non-null entries in {@link #pageColumnReaders}, releasing each reader's cached
     * dictionary {@link org.elasticsearch.common.util.BytesRefArray}. Idempotent — null entries
     * and a null array are both fine. The array is then discarded by the caller.
     */
    private void closePageColumnReaders() {
        if (pageColumnReaders == null) {
            return;
        }
        Releasables.close(pageColumnReaders);
        pageColumnReaders = null;
    }

    /**
     * Wraps an in-flight prefetch future.
     *
     * <p>The {@link #future} resolves to a {@link ColumnChunkPrefetcher.PrefetchedChunks} whose
     * {@link ColumnChunkPrefetcher.PrefetchedChunks#release()} owns the underlying direct memory
     * (and, transitively, the circuit-breaker accounting via the Arrow allocator listener).
     * If the prefetch is dequeued for use, that {@link Releasable} is handed off to
     * {@link #currentChunksReleasable}. If it is skipped or cancelled, {@link #release()} drains
     * any already-produced chunks so the breaker bytes return immediately.
     */
    private record PendingPrefetch(int ordinal, CompletableFuture<ColumnChunkPrefetcher.PrefetchedChunks> future) {

        void release() {
            // Drain the future so the direct memory the prefetch may have already produced is
            // released. We use getNow() to avoid a wait if the I/O is still in flight; in that
            // case FutureUtils.cancel() (called by the queue-management code before us) will
            // either complete the future exceptionally (no chunks to release) or propagate the
            // cancellation up to the storage backend.
            ColumnChunkPrefetcher.PrefetchedChunks chunks;
            try {
                chunks = future.getNow(null);
            } catch (CompletionException | CancellationException ignored) {
                // Future completed exceptionally (or was cancelled) — no chunks were ever produced
                // so there is nothing for us to release. Narrowing the catch keeps real bugs in
                // release().close() (e.g. ArrowBuf double-decrement) visible to the caller.
                return;
            }
            if (chunks != null) {
                chunks.release().close();
            }
        }
    }

}
