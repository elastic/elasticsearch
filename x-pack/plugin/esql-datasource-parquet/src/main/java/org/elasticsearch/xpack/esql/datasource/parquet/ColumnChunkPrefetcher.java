/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.spi.DirectMemoryDebug;
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
     * Result of a prefetch: the chunks indexed by file position, plus a {@link Releasable} that
     * owns the underlying direct memory (one child allocator per merged range, plus any extras
     * produced by {@link #promoteToDirect}). The caller must close {@link #release()} once the
     * chunks are no longer needed (typically at row-group rollover).
     */
    record PrefetchedChunks(NavigableMap<Long, PrefetchedChunk> chunks, Releasable release) {}

    /**
     * Synchronous prefetch: blocks the caller until all I/O completes. Intended for tests and
     * non-hot paths only — production code uses {@link #prefetchAsync} to overlap I/O with decode.
     *
     * @param storageObject the storage backend
     * @param block metadata for the row group to prefetch
     * @param projectedColumns column paths to include (null = all columns)
     * @param allocator parent allocator from which per-merged-range child allocators are spawned
     * @return a future that, on completion, holds the chunk index plus a {@link Releasable}
     */
    static CompletableFuture<PrefetchedChunks> prefetch(
        StorageObject storageObject,
        BlockMetaData block,
        Set<String> projectedColumns,
        BufferAllocator allocator
    ) {
        List<CoalescedRangeReader.ByteRange> ranges = computeColumnChunkRanges(block, projectedColumns);
        if (ranges.isEmpty()) {
            return CompletableFuture.completedFuture(new PrefetchedChunks(new TreeMap<>(), () -> {}));
        }

        logger.debug(
            "Prefetching [{}] column chunk ranges for row group at [{}] ({} bytes)",
            ranges.size(),
            block.getStartingPos(),
            block.getTotalByteSize()
        );

        CompletableFuture<PrefetchedChunks> result = new CompletableFuture<>();

        PlainActionFuture<CoalescedRangeReader.CoalescedRangeResult> ioFuture = new PlainActionFuture<>();
        CoalescedRangeReader.readCoalesced(
            storageObject,
            ranges,
            CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP,
            allocator,
            Runnable::run,
            ioFuture
        );

        try {
            CoalescedRangeReader.CoalescedRangeResult fetched = ioFuture.actionGet();
            result.complete(buildPrefetched(fetched, allocator));
        } catch (Throwable e) {
            result.completeExceptionally(e);
        }

        return result;
    }

    /**
     * Asynchronous variant that dispatches the coalesced read and returns immediately.
     * The future completes on whatever thread the storage I/O completes on.
     */
    static CompletableFuture<PrefetchedChunks> prefetchAsync(
        StorageObject storageObject,
        BlockMetaData block,
        Set<String> projectedColumns,
        BufferAllocator allocator
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

        CompletableFuture<PrefetchedChunks> result = new CompletableFuture<>();

        CoalescedRangeReader.readCoalesced(
            storageObject,
            ranges,
            CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP,
            allocator,
            Runnable::run,
            new ActionListener<>() {
                @Override
                public void onResponse(CoalescedRangeReader.CoalescedRangeResult fetched) {
                    try {
                        PrefetchedChunks chunks = buildPrefetched(fetched, allocator);
                        if (result.complete(chunks) == false) {
                            // The future was cancelled between I/O completion and here; release
                            // the direct memory we just allocated so the breaker charge returns.
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

        return result;
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
    static CompletableFuture<PrefetchedChunks> prefetch(
        StorageObject storageObject,
        BlockMetaData block,
        Set<String> projectedColumns,
        RowRanges rowRanges,
        PreloadedRowGroupMetadata metadata,
        int rowGroupOrdinal,
        long rowGroupRowCount,
        BufferAllocator allocator
    ) {
        if (rowRanges == null || rowRanges.isAll()) {
            return prefetch(storageObject, block, projectedColumns, allocator);
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

        logger.debug(
            "Prefetching [{}] filtered page ranges for row group at [{}] (row ranges: {} selected of {})",
            ranges.size(),
            block.getStartingPos(),
            rowRanges != null ? rowRanges.selectedRowCount() : rowGroupRowCount,
            rowGroupRowCount
        );

        CompletableFuture<PrefetchedChunks> result = new CompletableFuture<>();
        PlainActionFuture<CoalescedRangeReader.CoalescedRangeResult> ioFuture = new PlainActionFuture<>();
        CoalescedRangeReader.readCoalesced(
            storageObject,
            ranges,
            CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP,
            allocator,
            Runnable::run,
            ioFuture
        );

        try {
            CoalescedRangeReader.CoalescedRangeResult fetched = ioFuture.actionGet();
            result.complete(buildPrefetched(fetched, allocator));
        } catch (Throwable e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    static CompletableFuture<PrefetchedChunks> prefetchAsync(
        StorageObject storageObject,
        BlockMetaData block,
        Set<String> projectedColumns,
        RowRanges rowRanges,
        PreloadedRowGroupMetadata metadata,
        int rowGroupOrdinal,
        long rowGroupRowCount,
        BufferAllocator allocator
    ) {
        if (rowRanges == null || rowRanges.isAll()) {
            return prefetchAsync(storageObject, block, projectedColumns, allocator);
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

        CompletableFuture<PrefetchedChunks> result = new CompletableFuture<>();
        CoalescedRangeReader.readCoalesced(
            storageObject,
            ranges,
            CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP,
            allocator,
            Runnable::run,
            new ActionListener<>() {
                @Override
                public void onResponse(CoalescedRangeReader.CoalescedRangeResult fetched) {
                    try {
                        PrefetchedChunks chunks = buildPrefetched(fetched, allocator);
                        if (result.complete(chunks) == false) {
                            // The future was cancelled between I/O completion and here; release
                            // the direct memory we just allocated so the breaker charge returns.
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
        return result;
    }

    /**
     * Assembles a {@link PrefetchedChunks} from the coalesced read result. Wraps the result's
     * own {@link Releasable} together with any extra promote-to-direct allocations so the caller
     * can close them all in one shot.
     */
    private static PrefetchedChunks buildPrefetched(CoalescedRangeReader.CoalescedRangeResult fetched, BufferAllocator allocator) {
        // Keyed by file offset. Column chunks in a valid Parquet file have unique start positions;
        // duplicate offsets would indicate a corrupt or pathological file.
        NavigableMap<Long, PrefetchedChunk> prefetched = new TreeMap<>();
        List<Releasable> extra = new ArrayList<>();
        try {
            for (var entry : fetched.ranges().entrySet()) {
                CoalescedRangeReader.ByteRange range = entry.getKey();
                // promoteToDirect's fast-path (already-direct) returns the slice as delivered
                // by CoalescedRangeReader: position == relativeOffset within the merged S3 range,
                // not 0. PrefetchedSource.slice() treats offsetInChunk as a 0-based index into
                // chunk.data(), so normalise position here via slice(). The heap→direct copy path
                // already flips the buffer to position=0, so slice() is a no-op for that case.
                ByteBuffer data = promoteToDirect(entry.getValue(), allocator, extra).slice();
                prefetched.put(range.offset(), new PrefetchedChunk(range.offset(), range.length(), data));
            }
        } catch (Throwable t) {
            // Release the read result and any extras we managed to create before re-throwing so
            // the caller sees a clean failure with no outstanding breaker reservation. We catch
            // Throwable (not just RuntimeException) so that Errors like OutOfMemoryError — which
            // are a realistic outcome of allocating large direct buffers — also run the cleanup
            // path; otherwise the breaker reservation would leak for the lifetime of the JVM.
            Releasables.close(extra);
            try {
                fetched.release().close();
            } catch (Throwable releaseFailure) {
                t.addSuppressed(releaseFailure);
            }
            throw t;
        }
        Releasable composite = () -> {
            // Close the underlying coalesced-range child allocators first, then any extras created
            // by promote-to-direct. Order is irrelevant for correctness but keeps allocator names
            // visible in leak reports in the original allocation order.
            try {
                fetched.release().close();
            } finally {
                Releasables.close(extra);
            }
        };
        return new PrefetchedChunks(prefetched, composite);
    }

    /**
     * Returns a direct {@link ByteBuffer} view of {@code buffer}. If the input is already direct
     * (the production path: every backend now returns ArrowBuf-backed direct memory), this is a
     * no-op. Test stubs that return heap buffers fall through to an allocator-backed copy so the
     * downstream JNI decompressors can still take their direct-to-direct fast path and the
     * promoted bytes are also breaker-accounted.
     */
    private static ByteBuffer promoteToDirect(ByteBuffer buffer, BufferAllocator allocator, List<Releasable> extra) {
        if (buffer.isDirect()) {
            return buffer;
        }
        int length = buffer.remaining();
        ArrowBuf promoted = allocator.buffer(length);
        ByteBuffer direct = promoted.nioBuffer(0, length);
        // Poison the region just before release (assertions only) so a page slice that aliases this
        // promoted chunk and is read after free fails deterministically instead of flakily.
        extra.add(() -> {
            DirectMemoryDebug.poison(direct);
            promoted.close();
        });
        direct.put(buffer);
        direct.flip();
        return direct;
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
