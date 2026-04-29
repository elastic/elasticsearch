/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/**
 * Fetches column chunk data for a Parquet row group in parallel via {@link CoalescedRangeReader}.
 *
 * <p>Column chunks within a row group are typically stored sequentially but may be spread across
 * a wide byte range. For remote storage (S3, HTTP), issuing a single large GET is wasteful when
 * only a subset of columns is projected. Instead, this class computes the byte ranges for the
 * needed column chunks, merges nearby ranges (via {@link CoalescedRangeReader#mergeRanges}),
 * and fetches them concurrently using {@link StorageObject#readBytesAsync}.
 *
 * <p>The prefetched data is stored in a position-indexed map that
 * {@link ParquetStorageObjectAdapter} can consult before issuing its own I/O.
 *
 * <p><b>Threading model:</b> {@link #prefetch} is called from the {@code esql_worker} thread.
 * For native async storage (S3), I/O runs on the SDK's event loop. For local/default storage,
 * the default {@link StorageObject#readBytesAsync} runs inline via {@code Runnable::run}.
 */
final class ColumnChunkPrefetcher {

    private static final Logger logger = LogManager.getLogger(ColumnChunkPrefetcher.class);

    private ColumnChunkPrefetcher() {}

    /**
     * Synchronous prefetch: blocks the caller until all I/O completes. Intended for tests and
     * non-hot paths only — production code uses {@link #prefetchAsync} to overlap I/O with decode.
     *
     * @param storageObject the storage backend
     * @param block metadata for the row group to prefetch
     * @param projectedColumns column paths to include (null = all columns)
     * @return a future that, on completion, holds a navigable map from file position to buffer
     */
    static CompletableFuture<NavigableMap<Long, PrefetchedChunk>> prefetch(
        StorageObject storageObject,
        BlockMetaData block,
        java.util.Set<String> projectedColumns
    ) {
        List<CoalescedRangeReader.ByteRange> ranges = computeColumnChunkRanges(block, projectedColumns);
        if (ranges.isEmpty()) {
            return CompletableFuture.completedFuture(new TreeMap<>());
        }

        logger.debug(
            "Prefetching [{}] column chunk ranges for row group at [{}] ({} bytes)",
            ranges.size(),
            block.getStartingPos(),
            block.getTotalByteSize()
        );

        CompletableFuture<NavigableMap<Long, PrefetchedChunk>> result = new CompletableFuture<>();

        PlainActionFuture<Map<CoalescedRangeReader.ByteRange, ByteBuffer>> ioFuture = new PlainActionFuture<>();
        CoalescedRangeReader.readCoalesced(storageObject, ranges, CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP, Runnable::run, ioFuture);

        try {
            Map<CoalescedRangeReader.ByteRange, ByteBuffer> fetched = ioFuture.actionGet();
            // Keyed by file offset. Column chunks in a valid Parquet file have unique start
            // positions; duplicate offsets would indicate a corrupt or pathological file.
            NavigableMap<Long, PrefetchedChunk> prefetched = new TreeMap<>();
            for (var entry : fetched.entrySet()) {
                CoalescedRangeReader.ByteRange range = entry.getKey();
                prefetched.put(range.offset(), new PrefetchedChunk(range.offset(), range.length(), entry.getValue()));
            }
            result.complete(prefetched);
        } catch (Exception e) {
            result.completeExceptionally(e);
        }

        return result;
    }

    /**
     * Asynchronous variant that dispatches the coalesced read and returns immediately.
     * The future completes on whatever thread the storage I/O completes on.
     */
    static CompletableFuture<NavigableMap<Long, PrefetchedChunk>> prefetchAsync(
        StorageObject storageObject,
        BlockMetaData block,
        java.util.Set<String> projectedColumns
    ) {
        List<CoalescedRangeReader.ByteRange> ranges = computeColumnChunkRanges(block, projectedColumns);
        if (ranges.isEmpty()) {
            return CompletableFuture.completedFuture(new TreeMap<>());
        }

        logger.debug(
            "Async prefetching [{}] column chunk ranges for row group at [{}] ({} bytes)",
            ranges.size(),
            block.getStartingPos(),
            block.getTotalByteSize()
        );

        CompletableFuture<NavigableMap<Long, PrefetchedChunk>> result = new CompletableFuture<>();

        CoalescedRangeReader.readCoalesced(
            storageObject,
            ranges,
            CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP,
            Runnable::run,
            new ActionListener<>() {
                @Override
                public void onResponse(Map<CoalescedRangeReader.ByteRange, ByteBuffer> fetched) {
                    NavigableMap<Long, PrefetchedChunk> prefetched = new TreeMap<>();
                    for (var entry : fetched.entrySet()) {
                        CoalescedRangeReader.ByteRange range = entry.getKey();
                        prefetched.put(range.offset(), new PrefetchedChunk(range.offset(), range.length(), entry.getValue()));
                    }
                    result.complete(prefetched);
                }

                @Override
                public void onFailure(Exception e) {
                    result.completeExceptionally(e);
                }
            }
        );

        return result;
    }

    /**
     * Computes the byte ranges for column chunks in a row group. Each column chunk has a
     * starting position and total size in the file metadata.
     */
    static List<CoalescedRangeReader.ByteRange> computeColumnChunkRanges(BlockMetaData block, java.util.Set<String> projectedColumns) {
        List<CoalescedRangeReader.ByteRange> ranges = new ArrayList<>();
        for (ColumnChunkMetaData col : block.getColumns()) {
            if (projectedColumns != null && projectedColumns.contains(col.getPath().toDotString()) == false) {
                continue;
            }
            long startPos = col.getStartingPos();
            long totalSize = col.getTotalSize();
            if (totalSize > 0) {
                ranges.add(new CoalescedRangeReader.ByteRange(startPos, totalSize));
            }
        }
        return ranges;
    }

    /**
     * Computes byte ranges for only the surviving data pages within each column chunk.
     * Pages whose row span does not overlap with {@code rowRanges} are excluded, reducing
     * the number of bytes fetched from remote storage.
     *
     * <p><b>Note:</b> The filtered variants ({@code computeFilteredPageRanges}, filtered
     * {@code prefetch/prefetchAsync}) are not yet wired into the iterator — the current
     * optimized path always prefetches full column chunks. These exist for a planned
     * follow-up that integrates row-range-aware prefetch with page-index filtering.
     *
     * <p>Dictionary pages (which sit before data pages) are always included since they are
     * needed to decode any surviving page. Adjacent page ranges are merged by the caller
     * via {@link CoalescedRangeReader#mergeRanges}.
     *
     * @param block metadata for the row group
     * @param rowRanges selected row ranges (null = fall back to whole chunks)
     * @param metadata preloaded row group metadata with offset indexes
     * @param rowGroupOrdinal ordinal of the row group in the file
     * @param projectedColumns column paths to include (null = all columns)
     * @param rowGroupRowCount total rows in the row group
     * @return merged byte ranges covering only surviving pages
     */
    static List<CoalescedRangeReader.ByteRange> computeFilteredPageRanges(
        BlockMetaData block,
        RowRanges rowRanges,
        PreloadedRowGroupMetadata metadata,
        int rowGroupOrdinal,
        Set<String> projectedColumns,
        long rowGroupRowCount
    ) {
        if (rowRanges == null || rowRanges.isAll()) {
            return computeColumnChunkRanges(block, projectedColumns);
        }

        List<CoalescedRangeReader.ByteRange> ranges = new ArrayList<>();
        for (ColumnChunkMetaData col : block.getColumns()) {
            String path = col.getPath().toDotString();
            if (projectedColumns != null && projectedColumns.contains(path) == false) {
                continue;
            }

            OffsetIndex oi = metadata.getOffsetIndex(rowGroupOrdinal, path);
            if (oi == null) {
                long totalSize = col.getTotalSize();
                if (totalSize > 0) {
                    ranges.add(new CoalescedRangeReader.ByteRange(col.getStartingPos(), totalSize));
                }
                continue;
            }

            long dictOffset = col.getDictionaryPageOffset();
            long firstDataPageOffset = oi.getOffset(0);
            if (dictOffset > 0 && dictOffset < firstDataPageOffset) {
                ranges.add(new CoalescedRangeReader.ByteRange(dictOffset, firstDataPageOffset - dictOffset));
            }

            int pageCount = oi.getPageCount();
            for (int p = 0; p < pageCount; p++) {
                long pageStart = oi.getFirstRowIndex(p);
                long pageEnd = (p + 1 < pageCount) ? oi.getFirstRowIndex(p + 1) : rowGroupRowCount;
                if (rowRanges.overlaps(pageStart, pageEnd)) {
                    ranges.add(new CoalescedRangeReader.ByteRange(oi.getOffset(p), oi.getCompressedPageSize(p)));
                }
            }
        }

        if (ranges.isEmpty()) {
            return ranges;
        }
        List<CoalescedRangeReader.MergedRange> merged = CoalescedRangeReader.mergeRanges(
            ranges,
            CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP
        );
        List<CoalescedRangeReader.ByteRange> result = new ArrayList<>(merged.size());
        for (CoalescedRangeReader.MergedRange mr : merged) {
            result.add(new CoalescedRangeReader.ByteRange(mr.offset(), mr.length()));
        }
        return result;
    }

    /**
     * Synchronous filtered prefetch: blocks the caller. Test-only; see {@link #prefetchAsync}.
     */
    static CompletableFuture<NavigableMap<Long, PrefetchedChunk>> prefetch(
        StorageObject storageObject,
        BlockMetaData block,
        Set<String> projectedColumns,
        RowRanges rowRanges,
        PreloadedRowGroupMetadata metadata,
        int rowGroupOrdinal,
        long rowGroupRowCount
    ) {
        if (rowRanges == null || rowRanges.isAll()) {
            return prefetch(storageObject, block, projectedColumns);
        }

        List<CoalescedRangeReader.ByteRange> ranges = computeFilteredPageRanges(
            block,
            rowRanges,
            metadata,
            rowGroupOrdinal,
            projectedColumns,
            rowGroupRowCount
        );
        if (ranges.isEmpty()) {
            return CompletableFuture.completedFuture(new TreeMap<>());
        }

        logger.debug(
            "Prefetching [{}] filtered page ranges for row group at [{}] (row ranges: {} selected of {})",
            ranges.size(),
            block.getStartingPos(),
            rowRanges != null ? rowRanges.selectedRowCount() : rowGroupRowCount,
            rowGroupRowCount
        );

        CompletableFuture<NavigableMap<Long, PrefetchedChunk>> result = new CompletableFuture<>();
        PlainActionFuture<Map<CoalescedRangeReader.ByteRange, ByteBuffer>> ioFuture = new PlainActionFuture<>();
        CoalescedRangeReader.readCoalesced(storageObject, ranges, CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP, Runnable::run, ioFuture);

        try {
            Map<CoalescedRangeReader.ByteRange, ByteBuffer> fetched = ioFuture.actionGet();
            NavigableMap<Long, PrefetchedChunk> prefetched = new TreeMap<>();
            for (var entry : fetched.entrySet()) {
                CoalescedRangeReader.ByteRange range = entry.getKey();
                prefetched.put(range.offset(), new PrefetchedChunk(range.offset(), range.length(), entry.getValue()));
            }
            result.complete(prefetched);
        } catch (Exception e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    static CompletableFuture<NavigableMap<Long, PrefetchedChunk>> prefetchAsync(
        StorageObject storageObject,
        BlockMetaData block,
        Set<String> projectedColumns,
        RowRanges rowRanges,
        PreloadedRowGroupMetadata metadata,
        int rowGroupOrdinal,
        long rowGroupRowCount
    ) {
        if (rowRanges == null || rowRanges.isAll()) {
            return prefetchAsync(storageObject, block, projectedColumns);
        }

        List<CoalescedRangeReader.ByteRange> ranges = computeFilteredPageRanges(
            block,
            rowRanges,
            metadata,
            rowGroupOrdinal,
            projectedColumns,
            rowGroupRowCount
        );
        if (ranges.isEmpty()) {
            return CompletableFuture.completedFuture(new TreeMap<>());
        }

        logger.debug("Async prefetching [{}] filtered page ranges for row group at [{}]", ranges.size(), block.getStartingPos());

        CompletableFuture<NavigableMap<Long, PrefetchedChunk>> result = new CompletableFuture<>();
        CoalescedRangeReader.readCoalesced(
            storageObject,
            ranges,
            CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP,
            Runnable::run,
            new ActionListener<>() {
                @Override
                public void onResponse(Map<CoalescedRangeReader.ByteRange, ByteBuffer> fetched) {
                    NavigableMap<Long, PrefetchedChunk> prefetched = new TreeMap<>();
                    for (var entry : fetched.entrySet()) {
                        CoalescedRangeReader.ByteRange range = entry.getKey();
                        prefetched.put(range.offset(), new PrefetchedChunk(range.offset(), range.length(), entry.getValue()));
                    }
                    result.complete(prefetched);
                }

                @Override
                public void onFailure(Exception e) {
                    result.completeExceptionally(e);
                }
            }
        );
        return result;
    }

    /**
     * A prefetched chunk of column data at a specific file position.
     */
    record PrefetchedChunk(long offset, long length, ByteBuffer data) {
        boolean covers(long position, int requestedLength) {
            return position >= offset && position + requestedLength <= offset + length;
        }
    }
}
