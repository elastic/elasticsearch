/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.MessageType;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

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
 * <p><b>Memory:</b> Prefetch bytes are reserved on the REQUEST circuit breaker (via
 * {@link BlockFactory#breaker()}) before async I/O starts. The reservation is released
 * when prefetched data is consumed and cleared. If the breaker would trip, prefetch is
 * skipped and the query falls back to synchronous I/O for that row group.
 *
 * <p><b>Trivially-passes guard:</b> when late materialization is enabled and row-group
 * statistics prove every row satisfies the pushed filter ({@link TriviallyPassesChecker}),
 * the iterator routes that row group through {@code nextStandard} for the remainder of the
 * row group, skipping per-row filter evaluation and survivor compaction. This benefits queries
 * that mix selective and non-selective row groups (e.g., time-bucketed data with skewed
 * filter selectivity).
 */
final class OptimizedParquetColumnIterator implements CloseableIterator<Page> {

    private static final Logger logger = LogManager.getLogger(OptimizedParquetColumnIterator.class);

    private final ParquetFileReader reader;
    private final MessageType projectedSchema;
    private final List<Attribute> attributes;
    private final int batchSize;
    private final BlockFactory blockFactory;
    private final String createdBy;
    private final String fileLocation;
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
    private final CompressionCodecFactory codecFactory;
    private int rowBudget;
    /** Async prefetches allowed ahead of the consumed row group (1-3 based on projected column size). */
    private final int prefetchDepth;
    private final ArrayDeque<PendingPrefetch> pendingPrefetches = new ArrayDeque<>();
    /** Bytes reserved on the breaker for the chunks currently in use by {@link #rowGroup}. */
    private long currentReservedBytes = 0;

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
     * evaluation; consumed batch-by-batch in {@link #nextTwoPhaseBatch()} and cleared in
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
        CompressionCodecFactory codecFactory,
        ParquetPushedExpressions pushedExpressions,
        FilterPredicate triviallyPassesPredicate
    ) {
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
        this.codecFactory = codecFactory;
        this.pushedExpressions = pushedExpressions;
        this.isPredicateColumn = classifyPredicateColumns(attributes, columnInfos, pushedExpressions);
        this.lateMaterialization = pushedExpressions != null && hasProjectionOnlyColumns(isPredicateColumn, columnInfos);
        this.survivorMask = lateMaterialization ? new WordMask() : null;
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
        this.prefetchDepth = computePrefetchDepth(reader.getRowGroups(), this.projectedColumnPaths);

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
        int startOrdinal = nextSurvivingRowGroupOrdinal(0);
        fillPrefetchQueue(startOrdinal);
    }

    /**
     * Fills the prefetch queue up to {@link #prefetchDepth} entries, starting from ordinal
     * {@code fromOrdinal}. Each entry reserves bytes on the circuit breaker before starting
     * async I/O. Filling stops early if the breaker would trip or if no more surviving row
     * groups remain.
     */
    private void fillPrefetchQueue(int fromOrdinal) {
        List<BlockMetaData> rowGroups = reader.getRowGroups();
        // Under two-phase, the queued (Phase 1) prefetch only covers predicate columns; the
        // projection columns are fetched synchronously in advanceRowGroup once the survivor mask
        // is known. We therefore size the breaker reservation, the I/O ranges, and the column
        // set against the same, smaller column subset.
        Set<String> phaseColumns = useTwoPhase ? predicateColumnPaths : projectedColumnPaths;
        int nextOrdinal = fromOrdinal;
        while (pendingPrefetches.size() < prefetchDepth && nextOrdinal < rowGroups.size()) {
            BlockMetaData nextBlock = rowGroups.get(nextOrdinal);
            long prefetchBytes = ColumnChunkPrefetcher.computePrefetchBytes(nextBlock, phaseColumns);
            if (prefetchBytes <= 0) {
                nextOrdinal = nextSurvivingRowGroupOrdinal(nextOrdinal + 1);
                continue;
            }
            try {
                breaker.addEstimateBytesAndMaybeBreak(prefetchBytes, "esql_parquet_prefetch");
            } catch (CircuitBreakingException e) {
                logger.debug(
                    "Stopping prefetch queue fill at row group [{}] in [{}]: circuit breaker limit reached ({} bytes requested)",
                    nextOrdinal,
                    fileLocation,
                    prefetchBytes
                );
                break;
            }
            try {
                RowRanges nextRowRanges = allRowRanges != null && nextOrdinal < allRowRanges.length ? allRowRanges[nextOrdinal] : null;
                if (lateMaterialization) {
                    // Late-mat (single-phase or two-phase) handles row-level filtering itself via
                    // the survivor mask and would double-filter if ColumnIndex RowRanges were
                    // applied to the predicate-column prefetch here.
                    nextRowRanges = null;
                }
                CompletableFuture<NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk>> future;
                if (nextRowRanges != null) {
                    future = ColumnChunkPrefetcher.prefetchAsync(
                        storageObject,
                        nextBlock,
                        phaseColumns,
                        nextRowRanges,
                        preloadedMetadata,
                        nextOrdinal,
                        nextBlock.getRowCount()
                    );
                } else {
                    future = ColumnChunkPrefetcher.prefetchAsync(storageObject, nextBlock, phaseColumns);
                }
                pendingPrefetches.addLast(new PendingPrefetch(nextOrdinal, future, prefetchBytes));
            } catch (Exception e) {
                logger.debug("Failed to initiate prefetch for row group [{}] in [{}]: {}", nextOrdinal, fileLocation, e.getMessage());
                breaker.addWithoutBreaking(-prefetchBytes);
                break;
            }
            nextOrdinal = nextSurvivingRowGroupOrdinal(nextOrdinal + 1);
        }
    }

    private static final long DEEPER_PREFETCH_BYTES = 32_000_000L;
    private static final long SHALLOW_PREFETCH_BYTES = 8_000_000L;

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
            if (info != null) {
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
            if (columnInfos[i] != null && isPredicateColumn[i] == wantPredicate) {
                paths.add(String.join(".", columnInfos[i].descriptor().getPath()));
            }
        }
        return paths;
    }

    /**
     * Predicate-byte ratio threshold above which two-phase I/O is disabled because the projection
     * columns are not large enough relative to predicate columns to justify the extra round trip.
     * The single-phase late-mat path remains in effect in that case. A separate, lower threshold
     * than {@code LATE_MATERIALIZATION_PREDICATE_RATIO_THRESHOLD} is intentional: late-mat already
     * pays off at 50% predicate share, but two-phase amortizes a sequential dependency and only
     * wins when the projection columns clearly dominate.
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
        if (rowsRemainingInGroup > 0) {
            return true;
        }
        try {
            return advanceRowGroup();
        } catch (IOException e) {
            throw new ElasticsearchException(
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
                    codecFactory
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
        int[] next = new int[survivingRowGroups.length];
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
     * Returns the pre-computed {@link RowRanges} for the current row group. {@code rowGroupOrdinal}
     * is the physical block index in the file (we walk row groups ourselves now), so it directly
     * indexes {@code allRowRanges}. Slots for row groups dropped by the row-group level filter are
     * left {@code null} on purpose by {@code ParquetFormatReader} - those ordinals are skipped via
     * {@link #nextSurvivingRowGroupOrdinal} so this method never sees them, but we guard against a
     * stray {@code null} just in case.
     */
    private RowRanges resolveCurrentRowRanges(BlockMetaData block) {
        if (allRowRanges == null || rowGroupOrdinal >= allRowRanges.length || allRowRanges[rowGroupOrdinal] == null) {
            return null;
        }
        assert allRowRanges[rowGroupOrdinal].totalRows() == block.getRowCount()
            : "RowRanges total rows ["
                + allRowRanges[rowGroupOrdinal].totalRows()
                + "] != row group row count ["
                + block.getRowCount()
                + "] at ordinal ["
                + rowGroupOrdinal
                + "]";
        return allRowRanges[rowGroupOrdinal];
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
            codecFactory
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
                    predicateBlocks[col] = readColumnBlockWithAttribution(col, info, rowsToRead, predicateBlocks);
                }
                predicateBlockMap.put(attributes.get(col).name(), predicateBlocks[col]);
            }
            WordMask mask = pushedExpressions.evaluateFilter(predicateBlockMap, rowsToRead, survivorMask);
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
        long projectionBytes = ColumnChunkPrefetcher.computePrefetchBytes(block, projectionOnlyColumnPaths);
        long phase1Reserved = currentReservedBytes;
        if (projectionBytes > 0) {
            try {
                breaker.addEstimateBytesAndMaybeBreak(projectionBytes, "esql_parquet_phase2");
            } catch (CircuitBreakingException e) {
                logger.debug(
                    "Trivially-passes Phase-2 reservation failed for row group [{}] in [{}] ({} bytes): {}",
                    rowGroupOrdinal,
                    fileLocation,
                    projectionBytes,
                    e.getMessage()
                );
                throw e;
            }
        }
        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> phase2Chunks;
        try {
            phase2Chunks = ColumnChunkPrefetcher.prefetchAsync(storageObject, block, projectionOnlyColumnPaths).join();
        } catch (Exception e) {
            if (projectionBytes > 0) {
                breaker.addWithoutBreaking(-projectionBytes);
            }
            throw new ElasticsearchException(
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
        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> merged = new TreeMap<>();
        if (phase1Chunks != null) {
            merged.putAll(phase1Chunks);
        }
        if (phase2Chunks != null) {
            merged.putAll(phase2Chunks);
        }
        currentReservedBytes = phase1Reserved + projectionBytes;
        rowGroup = PrefetchedRowGroupBuilder.build(
            block,
            rowGroupOrdinal,
            projectedSchema,
            projectedColumnPaths,
            null,
            preloadedMetadata,
            merged,
            storageObject,
            codecFactory
        );
        rowsRemainingInGroup = rowGroup.getRowCount();
        initColumnReaders(null);
    }

    /**
     * Synchronously fetches the Phase-2 (projection-only) chunks for the current row group,
     * reserves the appropriate amount on the circuit breaker, and builds the projection
     * {@link PrefetchedPageReadStore}.
     *
     * @param block metadata for the row group
     * @param survivorRanges row ranges of survivors as produced by {@link WordMaskRowRangesConverter}
     * @param usePageFiltering when {@code true}, only pages overlapping {@code survivorRanges}
     *            are fetched; when {@code false}, full column chunks are fetched (i.e., page
     *            filtering would not pay off because survivors are too dense or fragmented)
     */
    private PrefetchedPageReadStore fetchProjectionPhase(BlockMetaData block, RowRanges survivorRanges, boolean usePageFiltering) {
        long projectionBytes;
        if (usePageFiltering) {
            // Mirror what ColumnChunkPrefetcher.computeFilteredPageRanges will allocate, including
            // the dictionary pages and merged adjacencies. Reusing the helper keeps the breaker
            // estimate consistent with the actual coalesced fetch.
            List<CoalescedRangeReader.ByteRange> ranges = ColumnChunkPrefetcher.computeFilteredPageRanges(
                block,
                survivorRanges,
                preloadedMetadata,
                rowGroupOrdinal,
                projectionOnlyColumnPaths,
                block.getRowCount()
            );
            long total = 0;
            for (CoalescedRangeReader.ByteRange r : ranges) {
                total += r.length();
            }
            projectionBytes = total;
        } else {
            projectionBytes = ColumnChunkPrefetcher.computePrefetchBytes(block, projectionOnlyColumnPaths);
        }
        if (projectionBytes > 0) {
            try {
                breaker.addEstimateBytesAndMaybeBreak(projectionBytes, "esql_parquet_phase2");
            } catch (CircuitBreakingException e) {
                logger.debug(
                    "Phase 2 prefetch reservation failed for row group [{}] in [{}] ({} bytes): {}",
                    rowGroupOrdinal,
                    fileLocation,
                    projectionBytes,
                    e.getMessage()
                );
                throw e;
            }
        }

        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks;
        try {
            CompletableFuture<NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk>> future = usePageFiltering
                ? ColumnChunkPrefetcher.prefetchAsync(
                    storageObject,
                    block,
                    projectionOnlyColumnPaths,
                    survivorRanges,
                    preloadedMetadata,
                    rowGroupOrdinal,
                    block.getRowCount()
                )
                : ColumnChunkPrefetcher.prefetchAsync(storageObject, block, projectionOnlyColumnPaths);
            chunks = future.join();
        } catch (Exception e) {
            // Release the reservation we just made; it isn't pinned to a row-group store yet.
            if (projectionBytes > 0) {
                breaker.addWithoutBreaking(-projectionBytes);
            }
            throw new ElasticsearchException(
                "Phase 2 prefetch failed for row group [" + rowGroupOrdinal + "] in [" + fileLocation + "]: " + e.getMessage(),
                e
            );
        }
        // Pin the Phase-2 reservation to the standard `currentReservedBytes` field so the existing
        // advance / close path releases it without special-casing two-phase. The Phase-1 reservation
        // was already released by the caller after predicate decode.
        currentReservedBytes = projectionBytes;
        return PrefetchedRowGroupBuilder.build(
            block,
            rowGroupOrdinal,
            projectedSchema,
            projectionOnlyColumnPaths,
            usePageFiltering ? survivorRanges : null,
            preloadedMetadata,
            chunks,
            storageObject,
            codecFactory
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
            if (columnInfos[i] != null && isPredicateColumn[i] && columnInfos[i].maxRepLevel() == 0) {
                ColumnDescriptor desc = columnInfos[i].descriptor();
                PageReader pr = rowGroup.getPageReader(desc);
                pageColumnReaders[i] = new PageColumnReader(pr, desc, columnInfos[i], null);
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
            if (columnInfos[i] != null && isPredicateColumn[i] == false && columnInfos[i].maxRepLevel() == 0) {
                ColumnDescriptor desc = columnInfos[i].descriptor();
                PageReader pr = rowGroup.getPageReader(desc);
                pageColumnReaders[i] = new PageColumnReader(pr, desc, columnInfos[i], survivorRowRanges);
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
            if (columnInfos[i] != null && columnInfos[i].maxRepLevel() == 0) {
                ColumnDescriptor desc = columnInfos[i].descriptor();
                PageReader pr = rowGroup.getPageReader(desc);
                pageColumnReaders[i] = new PageColumnReader(pr, desc, columnInfos[i], currentRowRanges);
            }
        }
        boolean hasListColumns = false;
        for (int i = 0; i < columnInfos.length; i++) {
            if (columnInfos[i] != null && columnInfos[i].maxRepLevel() > 0) {
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
                if (columnInfos[i] != null && columnInfos[i].maxRepLevel() > 0) {
                    columnReaders[i] = store.getColumnReader(columnInfos[i].descriptor());
                }
            }
        }
    }

    /**
     * Dequeues the head of the prefetch queue if it matches {@code expectedOrdinal}, joining its
     * future and returning the prefetched chunks. Entries whose ordinals don't match (because
     * the stats filter skipped intermediate row groups) are cancelled and their breaker
     * reservations released. Returns {@code null} when there is no usable prefetch.
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
            head.release(breaker);
        }

        if (pendingPrefetches.isEmpty()) {
            return null;
        }

        PendingPrefetch head = pendingPrefetches.pollFirst();
        try {
            NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> data = head.future().join();
            if (data != null && data.isEmpty() == false) {
                logger.trace("Took [{}] prefetched column chunks for row group [{}] in [{}]", data.size(), expectedOrdinal, fileLocation);
                currentReservedBytes = head.reservedBytes();
                return data;
            }
            head.release(breaker);
            return null;
        } catch (Exception e) {
            logger.debug(
                "Prefetch for row group [{}] failed in [{}], falling back to synchronous I/O: {}",
                expectedOrdinal,
                fileLocation,
                e.getMessage()
            );
            head.release(breaker);
            return null;
        }
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
     * Cancels all pending prefetches and releases their breaker reservations. Called when the
     * iterator is exhausted or closed.
     */
    private void cancelPendingPrefetch() {
        while (pendingPrefetches.isEmpty() == false) {
            PendingPrefetch entry = pendingPrefetches.pollFirst();
            FutureUtils.cancel(entry.future());
            entry.release(breaker);
        }
    }

    /**
     * Releases the breaker reservation held for the chunks currently in use by the row group.
     * Called when those chunks are about to be replaced (next advance) or dropped (close).
     */
    private void releaseCurrentReservation() {
        if (currentReservedBytes > 0) {
            breaker.addWithoutBreaking(-currentReservedBytes);
            currentReservedBytes = 0;
        }
    }

    @Override
    public Page next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }
        if (twoPhase != null) {
            return nextTwoPhaseBatch();
        }
        int effectiveBatch = batchSize;
        if (rowBudget != FormatReader.NO_LIMIT) {
            effectiveBatch = Math.min(effectiveBatch, rowBudget);
        }
        int rowsToRead = (int) Math.min(effectiveBatch, rowsRemainingInGroup);

        boolean useLateMaterialization = lateMaterialization && currentRowGroupTriviallyPasses == false;
        Page result = useLateMaterialization ? nextWithLateMaterialization(rowsToRead) : nextStandard(rowsToRead);

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
    private Page nextTwoPhaseBatch() {
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

        // Apply the row budget to survivor count: if the budget is smaller than the number of
        // surviving rows in this batch, truncate predicate and projection blocks to the first
        // {@code rowBudget} survivors and mark the iterator as exhausted after emission. We
        // re-slice predicate blocks via {@link #sliceBlockHead} (cheap because they're already
        // compacted) and pass {@code emitCount} to the projection read so it doesn't decode
        // beyond the budget either.
        int emitCount = survivorCount;
        boolean budgetExhaustsBatch = false;
        if (rowBudget != FormatReader.NO_LIMIT && rowBudget < emitCount) {
            // Truncate this batch to the remaining budget: keep the first `rowBudget` survivors.
            // We rebuild a positions array for the slice when needed.
            int newCount = rowBudget;
            int[] truncated;
            if (survivorPositions != null) {
                truncated = Arrays.copyOf(survivorPositions, newCount);
            } else {
                // All rows in this batch survived; the projection reader will read sourceRows
                // and we'll filter to the first newCount via a synthesized positions array.
                truncated = new int[newCount];
                for (int i = 0; i < newCount; i++) {
                    truncated[i] = i;
                }
            }
            survivorPositions = truncated;
            // Compact predicate blocks down to the first newCount survivors.
            for (int col = 0; col < columnInfos.length; col++) {
                if (isPredicateColumn[col] && predicateBlocks[col] != null) {
                    predicateBlocks[col] = sliceBlockHead(predicateBlocks[col], newCount);
                }
            }
            emitCount = newCount;
            budgetExhaustsBatch = true;
        }

        Block[] blocks = new Block[attributes.size()];
        try {
            for (int col = 0; col < columnInfos.length; col++) {
                if (isPredicateColumn[col]) {
                    blocks[col] = predicateBlocks[col];
                    continue;
                }
                ColumnInfo info = columnInfos[col];
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
                    // All sourceRows survived this batch.
                    blocks[col] = readColumnBlockWithAttribution(col, info, sourceRows, blocks);
                    if (budgetExhaustsBatch) {
                        blocks[col] = sliceBlockHead(blocks[col], emitCount);
                    }
                } else if (pageColumnReaders != null && pageColumnReaders[col] != null) {
                    blocks[col] = pageColumnReaders[col].readBatchFiltered(sourceRows, blockFactory, survivorPositions, emitCount);
                } else {
                    Block fullBlock = readColumnBlockWithAttribution(col, info, sourceRows, blocks);
                    blocks[col] = PageColumnReader.filterBlock(fullBlock, survivorPositions, emitCount, blockFactory);
                }
            }
            // Predicate slot ownership has been transferred into blocks[]; clear references in
            // predicateBlocks[] so the failure path below does not double-close them.
            for (int col = 0; col < columnInfos.length; col++) {
                if (isPredicateColumn[col]) {
                    predicateBlocks[col] = null;
                }
            }
        } catch (CircuitBreakingException e) {
            // Surface breaker failures unwrapped, matching nextStandard / nextWithLateMaterialization
            // so upstream operators (LIMIT, exchange, breaker telemetry) classify them correctly.
            Releasables.closeExpectNoException(blocks);
            Releasables.closeExpectNoException(predicateBlocks);
            throw e;
        } catch (RuntimeException e) {
            Releasables.closeExpectNoException(blocks);
            Releasables.closeExpectNoException(predicateBlocks);
            throw new ElasticsearchException(
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
        int[] head = new int[newCount];
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
        int[] out = new int[values.size()];
        for (int i = 0; i < values.size(); i++) {
            out[i] = values.get(i);
        }
        return out;
    }

    private Page nextStandard(int rowsToRead) {
        Block[] blocks = new Block[attributes.size()];
        int producedRows = -1;
        try {
            for (int col = 0; col < columnInfos.length; col++) {
                ColumnInfo info = columnInfos[col];
                if (info == null) {
                    continue;
                } else {
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
                        throw new ElasticsearchException(
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
            if (producedRows < 0) {
                producedRows = rowsToRead;
            }
            for (int col = 0; col < columnInfos.length; col++) {
                if (blocks[col] == null) {
                    blocks[col] = blockFactory.newConstantNullBlock(producedRows);
                }
            }
        } catch (ElasticsearchException e) {
            throw e;
        } catch (Exception e) {
            Releasables.closeExpectNoException(blocks);
            throw new ElasticsearchException(
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
        return new Page(blocks);
    }

    private Page nextWithLateMaterialization(int rowsToRead) {
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
                        blocks[col] = readColumnBlockWithAttribution(col, info, rowsToRead, blocks);
                    }
                    predicateBlockMap.put(attributes.get(col).name(), blocks[col]);
                }
            }

            // Phase 2: evaluate filter
            WordMask mask = pushedExpressions.evaluateFilter(predicateBlockMap, rowsToRead, survivorMask);

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
                    blocks[col] = readColumnBlockWithAttribution(col, info, rowsToRead, blocks);
                } else if (pageColumnReaders != null && pageColumnReaders[col] != null) {
                    blocks[col] = pageColumnReaders[col].readBatchFiltered(rowsToRead, blockFactory, positions, survivorCount);
                } else {
                    Block fullBlock = readColumnBlockWithAttribution(col, info, rowsToRead, blocks);
                    blocks[col] = PageColumnReader.filterBlock(fullBlock, positions, survivorCount, blockFactory);
                }
            }

            // Fill any remaining null slots
            for (int col = 0; col < columnInfos.length; col++) {
                if (blocks[col] == null) {
                    blocks[col] = blockFactory.newConstantNullBlock(survivorCount);
                }
            }

            return new Page(blocks);
        } catch (ElasticsearchException e) {
            Releasables.closeExpectNoException(blocks);
            throw e;
        } catch (Exception e) {
            Releasables.closeExpectNoException(blocks);
            throw new ElasticsearchException(
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

    private Block readColumnBlockWithAttribution(int colIndex, ColumnInfo info, int rowsToRead, Block[] blocks) {
        try {
            return readColumnBlock(colIndex, info, rowsToRead);
        } catch (CircuitBreakingException e) {
            Releasables.closeExpectNoException(blocks);
            throw e;
        } catch (Exception e) {
            Releasables.closeExpectNoException(blocks);
            Attribute attr = attributes.get(colIndex);
            throw new ElasticsearchException(
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

    private Block readColumnBlock(int colIndex, ColumnInfo info, int rowsToRead) {
        if (pageColumnReaders != null && pageColumnReaders[colIndex] != null) {
            return pageColumnReaders[colIndex].readBatch(rowsToRead, blockFactory);
        }
        ColumnReader cr = columnReaders != null ? columnReaders[colIndex] : null;
        if (cr == null) {
            return blockFactory.newConstantNullBlock(rowsToRead);
        }
        if (info.maxRepLevel() > 0) {
            return ParquetColumnDecoding.readListColumn(cr, info, rowsToRead, blockFactory);
        }
        ParquetColumnDecoding.skipValues(cr, rowsToRead);
        return blockFactory.newConstantNullBlock(rowsToRead);
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
            reader.close();
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

    /** Bundles an in-flight prefetch future with its breaker reservation for paired release. */
    private record PendingPrefetch(
        int ordinal,
        CompletableFuture<NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk>> future,
        long reservedBytes
    ) {
        PendingPrefetch {
            assert reservedBytes >= 0 : "reservedBytes must be non-negative: " + reservedBytes;
        }

        void release(CircuitBreaker breaker) {
            if (reservedBytes > 0) {
                breaker.addWithoutBreaking(-reservedBytes);
            }
        }
    }

}
