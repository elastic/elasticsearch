/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds pre-fetched metadata for a Parquet file's row groups: column indexes and offset indexes.
 * When a {@link StorageObject} is provided, uses {@link CoalescedRangeReader} to batch all index
 * byte ranges into minimal I/O requests — critical for remote storage (S3) where each GET has
 * ~50-100ms latency.
 *
 * <p>This class is populated once during file open and then read during row group
 * processing. All fields are effectively immutable after construction.
 */
final class PreloadedRowGroupMetadata {

    private static final Logger logger = LogManager.getLogger(PreloadedRowGroupMetadata.class);

    private final Map<String, ColumnIndex> columnIndexes;
    private final Map<String, OffsetIndex> offsetIndexes;

    PreloadedRowGroupMetadata(Map<String, ColumnIndex> columnIndexes, Map<String, OffsetIndex> offsetIndexes) {
        this.columnIndexes = Map.copyOf(columnIndexes);
        this.offsetIndexes = Map.copyOf(offsetIndexes);
    }

    static PreloadedRowGroupMetadata empty() {
        return new PreloadedRowGroupMetadata(Map.of(), Map.of());
    }

    /**
     * Preloads column indexes and offset indexes for all row groups.
     *
     * <p>When a {@link StorageObject} is provided, all index byte ranges across all row groups
     * are collected and fetched in a single batched {@link CoalescedRangeReader#readCoalesced}
     * call — merging adjacent ranges and issuing parallel async reads. This reduces hundreds
     * of sequential I/O operations to a handful of coalesced requests.
     *
     * <p><b>Threading model:</b> This method is called from {@code ParquetFormatReader.read()}
     * and {@code readRange()}, which in production are always dispatched on the
     * {@code esql_worker} thread pool by {@code AsyncExternalSourceOperatorFactory}
     * ({@code executor.execute(() -> formatReader.read(...))}). The coalesced reads use
     * {@code Runnable::run} as the async executor, which means:
     * <ul>
     *   <li><b>S3 / native async:</b> {@code S3StorageObject.readBytesAsync()} ignores the
     *       executor entirely — the AWS SDK's Netty event loop handles I/O. Reads are truly
     *       parallel and non-blocking.</li>
     *   <li><b>Local / default async:</b> The default {@code StorageObject.readBytesAsync()}
     *       wraps sync {@code newStream()} on the provided executor. With {@code Runnable::run},
     *       this runs inline on the calling {@code esql_worker} thread. Local I/O is fast
     *       enough that a dedicated thread pool adds more overhead than it saves.</li>
     * </ul>
     *
     * <p>Falls back to {@link ParquetFileReader}'s sequential reading when no storage object
     * is provided (e.g., in-memory test files).
     */
    static PreloadedRowGroupMetadata preload(ParquetFileReader reader, StorageObject storageObject) {
        List<BlockMetaData> rowGroups = reader.getRowGroups();
        if (rowGroups.isEmpty()) {
            return empty();
        }

        if (storageObject != null) {
            try {
                return preloadCoalesced(reader, rowGroups, storageObject);
            } catch (Exception e) {
                logger.debug("Coalesced metadata preload failed, falling back to sequential: {}", e.getMessage());
            }
        }
        return preloadSequential(reader, rowGroups);
    }

    /**
     * Batched preloading via {@link CoalescedRangeReader}. Collects all column index and
     * offset index byte ranges across all row groups, fetches them in one coalesced batch,
     * then parses each range into its typed index object.
     */
    private static PreloadedRowGroupMetadata preloadCoalesced(
        ParquetFileReader reader,
        List<BlockMetaData> rowGroups,
        StorageObject storageObject
    ) {
        List<CoalescedRangeReader.ByteRange> ranges = new ArrayList<>();
        record RangeMeta(CoalescedRangeReader.ByteRange range, int rowGroupIdx, ColumnChunkMetaData col, boolean isColumnIndex) {}
        List<RangeMeta> rangeMetas = new ArrayList<>();

        for (int rgIdx = 0; rgIdx < rowGroups.size(); rgIdx++) {
            BlockMetaData block = rowGroups.get(rgIdx);
            for (ColumnChunkMetaData col : block.getColumns()) {
                IndexReference ciRef = col.getColumnIndexReference();
                if (ciRef != null && ciRef.getLength() > 0) {
                    var br = new CoalescedRangeReader.ByteRange(ciRef.getOffset(), ciRef.getLength());
                    ranges.add(br);
                    rangeMetas.add(new RangeMeta(br, rgIdx, col, true));
                }
                IndexReference oiRef = col.getOffsetIndexReference();
                if (oiRef != null && oiRef.getLength() > 0) {
                    var br = new CoalescedRangeReader.ByteRange(oiRef.getOffset(), oiRef.getLength());
                    ranges.add(br);
                    rangeMetas.add(new RangeMeta(br, rgIdx, col, false));
                }
            }
        }

        if (ranges.isEmpty()) {
            return preloadSequential(reader, rowGroups);
        }

        logger.debug("Coalesced metadata preload: [{}] index ranges across [{}] row groups", ranges.size(), rowGroups.size());

        PlainActionFuture<Map<CoalescedRangeReader.ByteRange, ByteBuffer>> future = new PlainActionFuture<>();
        CoalescedRangeReader.readCoalesced(storageObject, ranges, CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP, Runnable::run, future);
        Map<CoalescedRangeReader.ByteRange, ByteBuffer> fetched = future.actionGet();

        Map<String, ColumnIndex> columnIndexes = new HashMap<>();
        Map<String, OffsetIndex> offsetIndexes = new HashMap<>();

        for (RangeMeta meta : rangeMetas) {
            ByteBuffer buf = fetched.get(meta.range());
            if (buf == null) {
                continue;
            }
            String k = key(meta.rowGroupIdx(), meta.col());
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            try {
                if (meta.isColumnIndex()) {
                    org.apache.parquet.format.ColumnIndex thriftCI = Util.readColumnIndex(new ByteArrayInputStream(bytes));
                    if (thriftCI != null) {
                        ColumnIndex ci = ParquetMetadataConverter.fromParquetColumnIndex(meta.col().getPrimitiveType(), thriftCI);
                        if (ci != null) {
                            columnIndexes.put(k, ci);
                        }
                    }
                } else {
                    org.apache.parquet.format.OffsetIndex thriftOI = Util.readOffsetIndex(new ByteArrayInputStream(bytes));
                    if (thriftOI != null) {
                        OffsetIndex oi = ParquetMetadataConverter.fromParquetOffsetIndex(thriftOI);
                        if (oi != null) {
                            offsetIndexes.put(k, oi);
                        }
                    }
                }
            } catch (Exception e) {
                logger.debug(
                    "Failed to parse [{}] for [{}] in row group [{}]: {}",
                    meta.isColumnIndex() ? "column index" : "offset index",
                    meta.col().getPath(),
                    meta.rowGroupIdx(),
                    e.getMessage()
                );
            }
        }

        return new PreloadedRowGroupMetadata(columnIndexes, offsetIndexes);
    }

    /**
     * Sequential fallback using {@link ParquetFileReader}'s built-in methods.
     */
    private static PreloadedRowGroupMetadata preloadSequential(ParquetFileReader reader, List<BlockMetaData> rowGroups) {
        Map<String, ColumnIndex> columnIndexes = new HashMap<>();
        Map<String, OffsetIndex> offsetIndexes = new HashMap<>();

        for (int rgIdx = 0; rgIdx < rowGroups.size(); rgIdx++) {
            BlockMetaData block = rowGroups.get(rgIdx);
            for (ColumnChunkMetaData col : block.getColumns()) {
                String k = key(rgIdx, col);
                try {
                    ColumnIndex ci = reader.readColumnIndex(col);
                    if (ci != null) {
                        columnIndexes.put(k, ci);
                    }
                } catch (IOException e) {
                    logger.debug("Failed to read column index for [{}] in row group [{}]: {}", col.getPath(), rgIdx, e.getMessage());
                }
                try {
                    OffsetIndex oi = reader.readOffsetIndex(col);
                    if (oi != null) {
                        offsetIndexes.put(k, oi);
                    }
                } catch (IOException e) {
                    logger.debug("Failed to read offset index for [{}] in row group [{}]: {}", col.getPath(), rgIdx, e.getMessage());
                }
            }
        }

        return new PreloadedRowGroupMetadata(columnIndexes, offsetIndexes);
    }

    ColumnIndex getColumnIndex(int rowGroupOrdinal, String columnPath) {
        return columnIndexes.get(key(rowGroupOrdinal, columnPath));
    }

    OffsetIndex getOffsetIndex(int rowGroupOrdinal, String columnPath) {
        return offsetIndexes.get(key(rowGroupOrdinal, columnPath));
    }

    boolean hasColumnIndexes() {
        return columnIndexes.isEmpty() == false;
    }

    boolean hasOffsetIndexes() {
        return offsetIndexes.isEmpty() == false;
    }

    private static String key(int rowGroupOrdinal, String columnPath) {
        return rowGroupOrdinal + ":" + columnPath;
    }

    static String key(int rowGroupOrdinal, ColumnChunkMetaData column) {
        return key(rowGroupOrdinal, column.getPath().toDotString());
    }
}
