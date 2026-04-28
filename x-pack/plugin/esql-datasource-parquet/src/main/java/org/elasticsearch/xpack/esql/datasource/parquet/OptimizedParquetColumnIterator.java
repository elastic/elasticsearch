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
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default Parquet column iterator with vectorized decoding and I/O prefetch.
 *
 * <p>Adds parallel column chunk prefetch: while decoding row group N, prefetches row group N+1's
 * data via {@link CoalescedRangeReader} and installs it into the {@link ParquetStorageObjectAdapter}
 * so that subsequent Parquet reads are served from memory instead of network I/O.
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
    private final ParquetStorageObjectAdapter adapter;
    private final Set<String> projectedColumnPaths;
    private final CircuitBreaker breaker;
    private final RowRanges[] allRowRanges;
    private int rowBudget;
    private final AtomicLong prefetchReservedBytes = new AtomicLong();

    private PageReadStore rowGroup;
    private ColumnReader[] columnReaders;
    private PageColumnReader[] pageColumnReaders;
    private long rowsRemainingInGroup;
    private boolean exhausted = false;
    private int rowGroupOrdinal = -1;
    private int pageBatchIndexInRowGroup = 0;
    private CompletableFuture<NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk>> pendingPrefetch;

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
        ParquetStorageObjectAdapter adapter,
        RowRanges[] allRowRanges
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
        this.adapter = adapter;
        this.breaker = blockFactory.breaker();
        this.allRowRanges = allRowRanges;

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
     * Advances to the next row group, installing any pending prefetch data and
     * triggering prefetch for the row group after that.
     */
    private boolean advanceRowGroup() throws IOException {
        if (rowGroup != null) {
            rowGroup.close();
            rowGroup = null;
        }

        installPendingPrefetch();

        rowGroup = reader.readNextFilteredRowGroup();

        adapter.clearPrefetchedData();
        releasePrefetchReservation();

        if (rowGroup == null) {
            exhausted = true;
            cancelPendingPrefetch();
            return false;
        }
        rowGroupOrdinal++;
        pageBatchIndexInRowGroup = 0;
        rowsRemainingInGroup = rowGroup.getRowCount();

        RowRanges currentRowRanges = resolveCurrentRowRanges();
        triggerNextRowGroupPrefetch();
        initColumnReaders(currentRowRanges);
        return rowsRemainingInGroup > 0;
    }

    /**
     * Returns the pre-computed {@link RowRanges} for the current row group. When
     * {@code allRowRanges} is present, {@code rowGroupOrdinal} directly indexes into it
     * because we disable {@code useColumnIndexFilter}, causing
     * {@code readNextFilteredRowGroup()} to iterate row groups sequentially without skipping.
     */
    private RowRanges resolveCurrentRowRanges() {
        if (allRowRanges == null || rowGroupOrdinal >= allRowRanges.length) {
            return null;
        }
        assert allRowRanges[rowGroupOrdinal].totalRows() == rowGroup.getRowCount()
            : "RowRanges row count ["
                + allRowRanges[rowGroupOrdinal].totalRows()
                + "] != row group row count ["
                + rowGroup.getRowCount()
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
     * Installs any previously prefetched row group data into the adapter so that
     * the next {@code readNextFilteredRowGroup()} can read from memory instead of
     * issuing network I/O. Falls back gracefully on failure, releasing the breaker
     * reservation if the prefetch did not produce usable data.
     */
    private void installPendingPrefetch() {
        if (pendingPrefetch == null) {
            return;
        }
        try {
            NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> data = pendingPrefetch.join();
            if (data != null && data.isEmpty() == false) {
                adapter.installPrefetchedData(data);
                logger.trace(
                    "Installed [{}] prefetched column chunks for row group [{}] in [{}]",
                    data.size(),
                    rowGroupOrdinal + 1,
                    fileLocation
                );
            } else {
                releasePrefetchReservation();
            }
        } catch (Exception e) {
            logger.debug(
                "Prefetch for row group [{}] failed in [{}], falling back to synchronous I/O: {}",
                rowGroupOrdinal + 1,
                fileLocation,
                e.getMessage()
            );
            releasePrefetchReservation();
        } finally {
            pendingPrefetch = null;
        }
    }

    /**
     * Triggers an async prefetch of column chunk data for the next row group.
     * Reserves the estimated bytes on the circuit breaker before starting I/O;
     * if the breaker would trip, prefetch is skipped and the query falls back
     * to synchronous I/O for that row group. The reservation is released when
     * the prefetched data is cleared in {@link #advanceRowGroup()} or on
     * failure/cancel.
     */
    private void triggerNextRowGroupPrefetch() {
        if (storageObject == null) {
            return;
        }
        List<BlockMetaData> rowGroups = reader.getRowGroups();
        int nextRgOrdinal = rowGroupOrdinal + 1;
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
            prefetchReservedBytes.set(prefetchBytes);
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
            if (allRowRanges != null && nextRgOrdinal < allRowRanges.length) {
                pendingPrefetch = ColumnChunkPrefetcher.prefetchAsync(
                    storageObject,
                    nextBlock,
                    projectedColumnPaths,
                    allRowRanges[nextRgOrdinal],
                    preloadedMetadata,
                    nextRgOrdinal,
                    nextBlock.getRowCount()
                );
            } else {
                pendingPrefetch = ColumnChunkPrefetcher.prefetchAsync(storageObject, nextBlock, projectedColumnPaths);
            }
        } catch (Exception e) {
            logger.debug("Failed to initiate prefetch for row group [{}] in [{}]: {}", nextRgOrdinal, fileLocation, e.getMessage());
            pendingPrefetch = null;
            releasePrefetchReservation();
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
        releasePrefetchReservation();
    }

    /**
     * Releases any circuit breaker reservation held for prefetched data. Idempotent —
     * safe to call multiple times (subsequent calls are no-ops when reservation is zero).
     */
    private void releasePrefetchReservation() {
        long reserved = prefetchReservedBytes.getAndSet(0);
        if (reserved > 0) {
            breaker.addWithoutBreaking(-reserved);
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
        adapter.clearPrefetchedData();
        releasePrefetchReservation();
        try {
            if (rowGroup != null) {
                rowGroup.close();
            }
        } finally {
            reader.close();
        }
    }

}
