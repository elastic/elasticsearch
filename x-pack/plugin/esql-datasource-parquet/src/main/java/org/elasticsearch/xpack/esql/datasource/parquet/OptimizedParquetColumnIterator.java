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
import org.apache.parquet.schema.MessageType;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
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
import java.util.HashSet;
import java.util.List;
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
    /**
     * Bytes reserved on the breaker for the pending (in-flight) prefetch. Only mutated from the
     * iterator thread (the prefetch I/O thread populates the {@link CompletableFuture} but never
     * touches this field), so a plain {@code long} is sufficient.
     */
    private long pendingReservedBytes = 0;
    /** Bytes reserved on the breaker for the chunks currently in use by {@link #rowGroup}. */
    private long currentReservedBytes = 0;

    private PrefetchedPageReadStore rowGroup;
    private ColumnReader[] columnReaders;
    private PageColumnReader[] pageColumnReaders;
    private long rowsRemainingInGroup;
    private boolean exhausted = false;
    private int rowGroupOrdinal = -1;
    private int pageBatchIndexInRowGroup = 0;
    private CompletableFuture<NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk>> pendingPrefetch;
    /** Ordinal of the row group whose chunks are pending prefetch; -1 when no prefetch is in flight. */
    private int pendingPrefetchOrdinal = -1;

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
        CompressionCodecFactory codecFactory
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

        this.projectedColumnPaths = buildProjectedColumnPaths(columnInfos);

        reader.setRequestedSchema(projectedSchema);
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
            return false;
        }
        rowGroupOrdinal = nextOrdinal;
        pageBatchIndexInRowGroup = 0;

        BlockMetaData block = reader.getRowGroups().get(rowGroupOrdinal);
        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = takePendingPrefetch(rowGroupOrdinal);
        RowRanges currentRowRanges = resolveCurrentRowRanges(block);
        rowGroup = PrefetchedRowGroupBuilder.build(
            block,
            rowGroupOrdinal,
            projectedSchema,
            projectedColumnPaths,
            currentRowRanges,
            preloadedMetadata,
            chunks,
            storageObject,
            codecFactory
        );
        // When RowRanges narrow the row group, only the surviving row count is consumed -
        // mirrors parquet-mr's filtered PageReadStore.getRowCount(). The PageReader's per-page
        // value count is unchanged; PageColumnReader caps at maxRows on each readBatch call so
        // columns whose pages span more rows than the filtered count return aligned blocks.
        rowsRemainingInGroup = currentRowRanges != null ? currentRowRanges.selectedRowCount() : rowGroup.getRowCount();
        triggerNextRowGroupPrefetch();
        initColumnReaders(currentRowRanges);
        return rowsRemainingInGroup > 0;
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
     * Joins the pending prefetch and returns the prefetched chunks for the row group whose ordinal
     * matches {@code expectedOrdinal}. Returns {@code null} when there is no usable prefetch (no
     * future scheduled, future failed, future returned empty data, or the prefetched ordinal does
     * not match the row group we're about to read because stats dropped intermediate row groups).
     * The breaker reservation is released for any prefetch whose data is not handed back to the
     * caller.
     */
    private NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> takePendingPrefetch(int expectedOrdinal) {
        // The chunks from the previous row group are no longer referenced by anyone; release
        // their reservation regardless of whether the new prefetch produces usable data.
        releaseCurrentReservation();

        if (pendingPrefetch == null) {
            return null;
        }
        if (pendingPrefetchOrdinal != expectedOrdinal) {
            // Stats filter advanced past the prefetched row group. The bytes we fetched are now
            // useless; drop them and free the reservation so the next prefetch can run.
            cancelPendingPrefetch();
            return null;
        }
        long reserved = pendingReservedBytes;
        pendingReservedBytes = 0;
        CompletableFuture<NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk>> future = pendingPrefetch;
        pendingPrefetch = null;
        pendingPrefetchOrdinal = -1;
        try {
            NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> data = future.join();
            if (data != null && data.isEmpty() == false) {
                logger.trace("Took [{}] prefetched column chunks for row group [{}] in [{}]", data.size(), expectedOrdinal, fileLocation);
                // Hold on to the reservation while these chunks are in use by the row group.
                currentReservedBytes = reserved;
                return data;
            }
            if (reserved > 0) {
                breaker.addWithoutBreaking(-reserved);
            }
            return null;
        } catch (Exception e) {
            logger.debug(
                "Prefetch for row group [{}] failed in [{}], falling back to synchronous I/O: {}",
                expectedOrdinal,
                fileLocation,
                e.getMessage()
            );
            if (reserved > 0) {
                breaker.addWithoutBreaking(-reserved);
            }
            return null;
        }
    }

    /**
     * Triggers an async prefetch of column chunk data for the next surviving row group (skipping
     * row groups dropped by the statistics filter so we never pay for I/O we won't use). Reserves
     * the estimated bytes on the circuit breaker before starting I/O; if the breaker would trip,
     * prefetch is skipped and the query falls back to synchronous I/O for that row group. The
     * reservation is released when the prefetched data is taken in {@link #advanceRowGroup()} or
     * on failure/cancel.
     */
    private void triggerNextRowGroupPrefetch() {
        if (storageObject == null) {
            return;
        }
        List<BlockMetaData> rowGroups = reader.getRowGroups();
        int nextRgOrdinal = nextSurvivingRowGroupOrdinal(rowGroupOrdinal + 1);
        if (nextRgOrdinal >= rowGroups.size()) {
            return;
        }
        BlockMetaData nextBlock = rowGroups.get(nextRgOrdinal);
        long prefetchBytes = ColumnChunkPrefetcher.computePrefetchBytes(nextBlock, projectedColumnPaths);
        if (prefetchBytes <= 0) {
            // No data to prefetch (empty projection or zero-byte columns). This is safe because
            // installPendingPrefetch tolerates pendingPrefetch == null — no invariant is broken.
            return;
        }
        try {
            breaker.addEstimateBytesAndMaybeBreak(prefetchBytes, "esql_parquet_prefetch");
            pendingReservedBytes = prefetchBytes;
        } catch (CircuitBreakingException e) {
            logger.debug(
                "Skipping prefetch for row group [{}] in [{}]: circuit breaker limit reached ({} bytes requested)",
                nextRgOrdinal,
                fileLocation,
                prefetchBytes
            );
            return;
        }
        try {
            // nextRgOrdinal is always a surviving ordinal (nextSurvivingRowGroupOrdinal skips
            // dropped row groups), so allRowRanges[nextRgOrdinal] is non-null when allRowRanges
            // itself is non-null. The explicit null guard makes the contract obvious.
            RowRanges nextRowRanges = allRowRanges != null && nextRgOrdinal < allRowRanges.length ? allRowRanges[nextRgOrdinal] : null;
            if (nextRowRanges != null) {
                pendingPrefetch = ColumnChunkPrefetcher.prefetchAsync(
                    storageObject,
                    nextBlock,
                    projectedColumnPaths,
                    nextRowRanges,
                    preloadedMetadata,
                    nextRgOrdinal,
                    nextBlock.getRowCount()
                );
            } else {
                pendingPrefetch = ColumnChunkPrefetcher.prefetchAsync(storageObject, nextBlock, projectedColumnPaths);
            }
            pendingPrefetchOrdinal = nextRgOrdinal;
        } catch (Exception e) {
            logger.debug("Failed to initiate prefetch for row group [{}] in [{}]: {}", nextRgOrdinal, fileLocation, e.getMessage());
            pendingPrefetch = null;
            pendingPrefetchOrdinal = -1;
            releasePendingReservation();
        }
    }

    /**
     * Cancels the pending prefetch future and releases the breaker reservation. Note that
     * {@link org.elasticsearch.common.util.concurrent.FutureUtils#cancel} only flips the
     * future's cancelled state — it does not interrupt in-flight storage SDK reads. If the
     * async read is already in progress, the SDK may still allocate a buffer that becomes
     * untracked by the breaker until GC. Draining the future before releasing would risk
     * blocking {@link #close()}, so we accept this brief discrepancy.
     */
    private void cancelPendingPrefetch() {
        if (pendingPrefetch != null) {
            org.elasticsearch.common.util.concurrent.FutureUtils.cancel(pendingPrefetch);
            pendingPrefetch = null;
        }
        pendingPrefetchOrdinal = -1;
        releasePendingReservation();
    }

    /**
     * Releases the breaker reservation held for the in-flight (pending) prefetch. Idempotent —
     * safe to call multiple times (subsequent calls are no-ops when reservation is zero).
     */
    private void releasePendingReservation() {
        long reserved = pendingReservedBytes;
        pendingReservedBytes = 0;
        if (reserved > 0) {
            breaker.addWithoutBreaking(-reserved);
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

        Page result = nextStandard(rowsToRead);

        pageBatchIndexInRowGroup++;
        rowsRemainingInGroup -= rowsToRead;
        if (rowBudget != FormatReader.NO_LIMIT) {
            rowBudget -= rowsToRead;
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

}
