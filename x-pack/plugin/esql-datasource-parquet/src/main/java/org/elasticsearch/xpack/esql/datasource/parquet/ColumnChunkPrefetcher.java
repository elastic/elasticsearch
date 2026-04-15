/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
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
     * Computes byte ranges for all column chunks in the given row group and fetches them
     * in parallel. Returns a future that completes with a position-indexed map of the
     * prefetched data.
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
     * A prefetched chunk of column data at a specific file position.
     */
    record PrefetchedChunk(long offset, long length, ByteBuffer data) {
        boolean covers(long position, int requestedLength) {
            return position >= offset && position + requestedLength <= offset + length;
        }
    }
}
