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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
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
    private final boolean[] isPredicateColumn;
    private final ParquetPushedExpressions pushedExpressions;
    private final WordMask survivorMask;
    private long rowsEliminatedByLateMaterialization;

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
        ParquetPushedExpressions pushedExpressions
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

        this.projectedColumnPaths = buildProjectedColumnPaths(columnInfos);
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
        int nextOrdinal = fromOrdinal;
        while (pendingPrefetches.size() < prefetchDepth && nextOrdinal < rowGroups.size()) {
            BlockMetaData nextBlock = rowGroups.get(nextOrdinal);
            long prefetchBytes = ColumnChunkPrefetcher.computePrefetchBytes(nextBlock, projectedColumnPaths);
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
                    nextRowRanges = null;
                }
                CompletableFuture<NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk>> future;
                if (nextRowRanges != null) {
                    future = ColumnChunkPrefetcher.prefetchAsync(
                        storageObject,
                        nextBlock,
                        projectedColumnPaths,
                        nextRowRanges,
                        preloadedMetadata,
                        nextOrdinal,
                        nextBlock.getRowCount()
                    );
                } else {
                    future = ColumnChunkPrefetcher.prefetchAsync(storageObject, nextBlock, projectedColumnPaths);
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
     * Advances to the next surviving row group: applies the row-group statistics filter to skip
     * dropped row groups, then builds a {@link PrefetchedPageReadStore} from the prefetched
     * (or sync-fetched) bytes for the surviving row group. Triggers prefetch for the row group
     * after that.
     */
    private boolean advanceRowGroup() throws IOException {
        if (rowGroup != null) {
            rowGroup.close();
            rowGroup = null;
        }

        int nextOrdinal = nextSurvivingRowGroupOrdinal(rowGroupOrdinal + 1);
        if (nextOrdinal >= reader.getRowGroups().size()) {
            exhausted = true;
            cancelPendingPrefetch();
            releaseCurrentReservation();
            if (rowsEliminatedByLateMaterialization > 0) {
                logger.debug("Late materialization eliminated [{}] rows in [{}]", rowsEliminatedByLateMaterialization, fileLocation);
            }
            return false;
        }
        rowGroupOrdinal = nextOrdinal;
        pageBatchIndexInRowGroup = 0;

        BlockMetaData block = reader.getRowGroups().get(rowGroupOrdinal);
        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = takePendingPrefetch(rowGroupOrdinal);
        try {
            RowRanges currentRowRanges = resolveCurrentRowRanges(block);
            // When late materialization is active, skip ColumnIndex page filtering — late-mat handles
            // row-level filtering itself via the survivor mask. Applying both ColumnIndex RowRanges
            // AND late-mat evaluation causes double-filtering that drops rows incorrectly.
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

    private void initColumnReaders(RowRanges currentRowRanges) {
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
        int effectiveBatch = batchSize;
        if (rowBudget != FormatReader.NO_LIMIT) {
            effectiveBatch = Math.min(effectiveBatch, rowBudget);
        }
        int rowsToRead = (int) Math.min(effectiveBatch, rowsRemainingInGroup);

        Page result = lateMaterialization ? nextWithLateMaterialization(rowsToRead) : nextStandard(rowsToRead);

        pageBatchIndexInRowGroup++;
        rowsRemainingInGroup -= rowsToRead;
        if (rowBudget != FormatReader.NO_LIMIT) {
            rowBudget -= lateMaterialization ? result.getPositionCount() : rowsToRead;
        }
        return result;
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
            if (rowGroup != null) {
                rowGroup.close();
                rowGroup = null;
            }
        } finally {
            releaseCurrentReservation();
            reader.close();
        }
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
