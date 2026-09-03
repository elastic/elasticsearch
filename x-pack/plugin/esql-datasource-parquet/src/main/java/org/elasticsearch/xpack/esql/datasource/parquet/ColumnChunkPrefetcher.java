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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
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
 * and fetches them using synchronous or asynchronous coalesced reads.
 *
 * <p>The prefetched data is stored in a position-indexed map that
 * {@link ParquetStorageObjectAdapter} can consult before issuing its own I/O.
 *
 * <p><b>Threading model:</b> {@link #fetchSync} runs on the {@code esql_worker} thread.
 * {@link #prefetchAsync} dispatches native async storage I/O to the provider; for local/default
 * storage, the default {@link StorageObject#readBytesAsync} runs inline via {@code Runnable::run}.
 */
final class ColumnChunkPrefetcher {

    private static final Logger logger = LogManager.getLogger(ColumnChunkPrefetcher.class);

    private ColumnChunkPrefetcher() {}

    /**
     * Result of a prefetch: the chunks indexed by file position, plus a {@link Releasable} that
     * owns the underlying buffers. The caller must close {@link #release()} once the
     * chunks are no longer needed (typically at row-group rollover).
     */
    record PrefetchedChunks(NavigableMap<Long, PrefetchedChunk> chunks, Releasable release) {}

    /**
     * Synchronously fetches all selected column chunks.
     *
     * @param storageObject the storage backend
     * @param block metadata for the row group to prefetch
     * @param projectedColumns column paths to include (null = all columns)
     * @param breaker circuit breaker charged for each merged-range buffer
     * @return the chunk index plus a {@link Releasable}
     */
    static PrefetchedChunks fetchSync(
        StorageObject storageObject,
        BlockMetaData block,
        Set<String> projectedColumns,
        CircuitBreaker breaker
    ) {
        try {
            List<CoalescedRangeReader.ByteRange> ranges = computeColumnChunkRanges(block, projectedColumns);
            if (ranges.isEmpty()) {
                return new PrefetchedChunks(new TreeMap<>(), () -> {});
            }
            logger.debug(
                "Synchronously fetching [{}] column chunk ranges for row group at [{}] ({} bytes)",
                ranges.size(),
                block.getStartingPos(),
                block.getTotalByteSize()
            );
            validateSyncRanges(block, projectedColumns, ranges);
            return buildPrefetched(
                CoalescedRangeReader.readCoalescedSync(storageObject, ranges, CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP, breaker)
            );
        } catch (Exception e) {
            throw ParquetReadFailures.wrap(e, "Failed to fetch column chunks for row group at [" + block.getStartingPos() + "]");
        }
    }

    /**
     * Asynchronous variant that dispatches the coalesced read and returns immediately.
     * The future completes on whatever thread the storage I/O completes on.
     */
    static CompletableFuture<PrefetchedChunks> prefetchAsync(
        StorageObject storageObject,
        BlockMetaData block,
        Set<String> projectedColumns,
        CircuitBreaker breaker
    ) {
        List<CoalescedRangeReader.ByteRange> ranges = computeColumnChunkRanges(block, projectedColumns);
        if (ranges.isEmpty()) {
            return CompletableFuture.completedFuture(new PrefetchedChunks(new TreeMap<>(), () -> {}));
        }

        logger.debug(
            "Async prefetching [{}] column chunk ranges for row group at [{}] ({} bytes)",
            ranges.size(),
            block.getStartingPos(),
            block.getTotalByteSize()
        );

        return prefetchCoalesced(storageObject, ranges, breaker);
    }

    /**
     * Computes the total bytes that a prefetch would actually allocate for the given row group and
     * projection. This accounts for coalescing gaps between column chunks (up to
     * {@link CoalescedRangeReader#DEFAULT_MAX_COALESCE_GAP} bytes per gap) so the estimate matches
     * what {@link CoalescedRangeReader#readCoalesced} will allocate.
     */
    static long computePrefetchBytes(BlockMetaData block, Set<String> projectedColumns) {
        List<CoalescedRangeReader.ByteRange> ranges = computeColumnChunkRanges(block, projectedColumns);
        if (ranges.isEmpty()) {
            return 0;
        }
        List<CoalescedRangeReader.MergedRange> merged = CoalescedRangeReader.mergeRanges(
            ranges,
            CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP
        );
        long totalBytes = 0;
        for (CoalescedRangeReader.MergedRange mr : merged) {
            totalBytes += mr.length();
        }
        return totalBytes;
    }

    /**
     * Computes the byte ranges for column chunks in a row group. Each column chunk has a
     * starting position and total size in the file metadata.
     */
    static List<CoalescedRangeReader.ByteRange> computeColumnChunkRanges(BlockMetaData block, Set<String> projectedColumns) {
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
     * <p>Dictionary pages are included via {@link #dictionaryPageRange} when encodings
     * advertise one and the OffsetIndex leaves a positive gap before the first data page.
     * Adjacent page ranges are merged by the caller via {@link CoalescedRangeReader#mergeRanges}.
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

            int pageCount = oi.getPageCount();
            if (pageCount > 0) {
                CoalescedRangeReader.ByteRange dictRange = dictionaryPageRange(col, oi.getOffset(0));
                if (dictRange != null) {
                    ranges.add(dictRange);
                }
            }

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
     * Byte range covering the dictionary page of a filtered column chunk, or {@code null} when
     * there is no dictionary to fetch.
     *
     * <p>{@code dictionary_page_offset == 0} means the Thrift field was omitted, not file offset
     * 0 ({@code PAR1}). When encodings advertise a dictionary and that field is unset, the page
     * sits in {@code [getStartingPos(), firstIndexedDataPageOffset)} — the same gap a sequential
     * walk already consumes as {@code DICTIONARY_PAGE}. Writers may leave {@code data_page_offset}
     * equal to the chunk start while the OffsetIndex points at the first real data page, so the
     * offset-index bound is the one that locates the gap.
     *
     * @param column column chunk metadata
     * @param firstIndexedDataPageOffset {@link OffsetIndex#getOffset(int) OffsetIndex.getOffset(0)} for this column
     * @return range {@code [dictStart, firstIndexedDataPageOffset)}, or {@code null}
     */
    static CoalescedRangeReader.ByteRange dictionaryPageRange(ColumnChunkMetaData column, long firstIndexedDataPageOffset) {
        if (column.hasDictionaryPage() == false) {
            return null;
        }
        long dictOffset = column.getDictionaryPageOffset();
        long dictStart = dictOffset > 0 ? dictOffset : column.getStartingPos();
        if (dictStart > 0 && dictStart < firstIndexedDataPageOffset) {
            return new CoalescedRangeReader.ByteRange(dictStart, firstIndexedDataPageOffset - dictStart);
        }
        return null;
    }

    /**
     * Synchronously fetches the selected pages of the selected column chunks.
     */
    static PrefetchedChunks fetchSync(
        StorageObject storageObject,
        BlockMetaData block,
        Set<String> projectedColumns,
        RowRanges rowRanges,
        PreloadedRowGroupMetadata metadata,
        int rowGroupOrdinal,
        long rowGroupRowCount,
        CircuitBreaker breaker
    ) {
        try {
            List<CoalescedRangeReader.ByteRange> ranges = computeFilteredPageRanges(
                block,
                rowRanges,
                metadata,
                rowGroupOrdinal,
                projectedColumns,
                rowGroupRowCount
            );
            if (ranges.isEmpty()) {
                return new PrefetchedChunks(new TreeMap<>(), () -> {});
            }
            logger.debug(
                "Synchronously fetching [{}] filtered page ranges for row group at [{}] (row ranges: {} selected of {})",
                ranges.size(),
                block.getStartingPos(),
                rowRanges != null ? rowRanges.selectedRowCount() : rowGroupRowCount,
                rowGroupRowCount
            );
            validateSyncRanges(block, projectedColumns, ranges);
            return buildPrefetched(
                CoalescedRangeReader.readCoalescedSync(storageObject, ranges, CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP, breaker)
            );
        } catch (Exception e) {
            throw ParquetReadFailures.wrap(e, "Failed to fetch column chunks for row group at [" + block.getStartingPos() + "]");
        }
    }

    static CompletableFuture<PrefetchedChunks> prefetchAsync(
        StorageObject storageObject,
        BlockMetaData block,
        Set<String> projectedColumns,
        RowRanges rowRanges,
        PreloadedRowGroupMetadata metadata,
        int rowGroupOrdinal,
        long rowGroupRowCount,
        CircuitBreaker breaker
    ) {
        if (rowRanges == null || rowRanges.isAll()) {
            return prefetchAsync(storageObject, block, projectedColumns, breaker);
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
            return CompletableFuture.completedFuture(new PrefetchedChunks(new TreeMap<>(), () -> {}));
        }

        logger.debug("Async prefetching [{}] filtered page ranges for row group at [{}]", ranges.size(), block.getStartingPos());

        return prefetchCoalesced(storageObject, ranges, breaker);
    }

    /**
     * Dispatches a coalesced async read and wires wrapper-future cancel to the in-flight GETs.
     */
    private static CompletableFuture<PrefetchedChunks> prefetchCoalesced(
        StorageObject storageObject,
        List<CoalescedRangeReader.ByteRange> ranges,
        CircuitBreaker breaker
    ) {
        CompletableFuture<PrefetchedChunks> result = new CompletableFuture<>();
        Releasable cancelIo = CoalescedRangeReader.readCoalesced(
            storageObject,
            ranges,
            CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP,
            breaker,
            Runnable::run,
            new ActionListener<>() {
                @Override
                public void onResponse(CoalescedRangeReader.CoalescedRangeResult fetched) {
                    try {
                        PrefetchedChunks chunks = buildPrefetched(fetched);
                        if (result.complete(chunks) == false) {
                            // The future was cancelled between I/O completion and here; release
                            // the buffers we just allocated so the breaker charge returns.
                            chunks.release().close();
                        }
                    } catch (Throwable e) {
                        // buildPrefetched failed mid-way; the helper has already released its
                        // tracked buffers — surface the failure. Catching Throwable (not just
                        // RuntimeException) is intentional: buildPrefetched re-throws Errors
                        // such as OutOfMemoryError, and if those escaped here the future would
                        // never complete, permanently hanging any caller that joins it.
                        result.completeExceptionally(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    result.completeExceptionally(e);
                }
            }
        );
        result.whenComplete((ignored, error) -> {
            if (result.isCancelled()) {
                cancelIo.close();
            }
        });
        return result;
    }

    private static void validateSyncRanges(BlockMetaData block, Set<String> projectedColumns, List<CoalescedRangeReader.ByteRange> ranges) {
        for (CoalescedRangeReader.ByteRange range : ranges) {
            if (range.length() <= Integer.MAX_VALUE) {
                continue;
            }
            String column = columnForRangeOffset(block, projectedColumns, range.offset());
            String subject = column == null ? "Range at offset [" + range.offset() + "]" : "Range for column [" + column + "]";
            throw new IllegalArgumentException(
                subject
                    + " has length ["
                    + range.length()
                    + "], which exceeds the maximum supported synchronous read size ["
                    + Integer.MAX_VALUE
                    + "]"
            );
        }
    }

    private static String columnForRangeOffset(BlockMetaData block, Set<String> projectedColumns, long offset) {
        for (ColumnChunkMetaData column : block.getColumns()) {
            String path = column.getPath().toDotString();
            if (projectedColumns != null && projectedColumns.contains(path) == false) {
                continue;
            }
            if (column.getStartingPos() == offset || (column.getDictionaryPageOffset() > 0 && column.getDictionaryPageOffset() == offset)) {
                return path;
            }
        }
        return null;
    }

    /**
     * Assembles a {@link PrefetchedChunks} from the coalesced read result. Heap buffers stay heap;
     * {@link ByteBuffer#slice()} normalizes position so {@code PrefetchedSource} can treat
     * {@code offsetInChunk} as 0-based.
     */
    private static PrefetchedChunks buildPrefetched(CoalescedRangeReader.CoalescedRangeResult fetched) {
        // Keyed by file offset. Column chunks in a valid Parquet file have unique start positions;
        // duplicate offsets would indicate a corrupt or pathological file.
        NavigableMap<Long, PrefetchedChunk> prefetched = new TreeMap<>();
        try {
            for (var entry : fetched.ranges().entrySet()) {
                CoalescedRangeReader.ByteRange range = entry.getKey();
                // CoalescedRangeReader delivers slices with position == relativeOffset within the
                // merged range, not 0. PrefetchedSource.slice() treats offsetInChunk as a 0-based
                // index into chunk.data(), so normalise position here via slice().
                ByteBuffer data = entry.getValue().slice();
                prefetched.put(range.offset(), new PrefetchedChunk(range.offset(), range.length(), data));
            }
        } catch (Throwable t) {
            // Release the read result before re-throwing so the caller sees a clean failure with
            // no outstanding breaker reservation. We catch Throwable (not just RuntimeException)
            // so that Errors like OutOfMemoryError also run the cleanup path; otherwise the
            // breaker reservation would leak for the lifetime of the JVM.
            try {
                fetched.release().close();
            } catch (Throwable releaseFailure) {
                t.addSuppressed(releaseFailure);
            }
            throw t;
        }
        return new PrefetchedChunks(prefetched, fetched.release());
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
