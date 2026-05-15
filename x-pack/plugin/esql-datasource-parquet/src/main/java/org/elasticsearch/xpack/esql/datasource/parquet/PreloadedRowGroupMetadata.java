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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Holds pre-fetched metadata for a Parquet file's row groups: column indexes, offset indexes, and
 * optionally raw byte buffers for the dictionary pages and bloom filters of <em>predicate</em>
 * columns. When a {@link StorageObject} is provided, uses {@link CoalescedRangeReader} to batch
 * all of these byte ranges into minimal I/O requests — critical for remote storage (S3) where
 * each GET has ~50-100ms latency.
 *
 * <p>The pre-warmed dictionary/bloom buffers are exposed via {@link #preWarmedChunks()} so that
 * {@link ParquetStorageObjectAdapter#installPreWarmedChunks} can serve subsequent
 * {@code RowGroupFilter} reads from memory instead of issuing one synchronous range GET per row
 * group.
 *
 * <p>This class is populated once during file open and then read during row group
 * processing. All fields are effectively immutable after construction.
 */
final class PreloadedRowGroupMetadata {

    private static final Logger logger = LogManager.getLogger(PreloadedRowGroupMetadata.class);

    private final Map<String, ColumnIndex> columnIndexes;
    private final Map<String, OffsetIndex> offsetIndexes;

    /**
     * Pre-fetched dictionary-page and bloom-filter byte chunks for predicate columns, keyed by
     * file offset. Empty when no predicate columns were supplied or when batch fetching was not
     * possible. Consumed by {@link ParquetStorageObjectAdapter#installPreWarmedChunks} so parquet-mr's
     * subsequent {@code RowGroupFilter} reads hit memory instead of issuing per-row-group GETs.
     */
    private final NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> preWarmedChunks;

    PreloadedRowGroupMetadata(Map<String, ColumnIndex> columnIndexes, Map<String, OffsetIndex> offsetIndexes) {
        this(columnIndexes, offsetIndexes, new TreeMap<>());
    }

    PreloadedRowGroupMetadata(
        Map<String, ColumnIndex> columnIndexes,
        Map<String, OffsetIndex> offsetIndexes,
        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> preWarmedChunks
    ) {
        this.columnIndexes = Map.copyOf(columnIndexes);
        this.offsetIndexes = Map.copyOf(offsetIndexes);
        // Defensive copy: the caller's map is no longer referenced after construction.
        this.preWarmedChunks = preWarmedChunks.isEmpty() ? new TreeMap<>() : new TreeMap<>(preWarmedChunks);
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
        return preload(reader, storageObject, null);
    }

    /**
     * Variant that additionally batch-fetches dictionary pages and bloom filters for the
     * supplied predicate columns. Pre-warming these byte ranges before {@code RowGroupFilter}
     * runs collapses what would be hundreds of synchronous per-row-group S3 GETs into a single
     * coalesced async batch — the dominant cost of filtered queries on remote storage.
     *
     * <p>Pass {@code null} or an empty set when no predicate columns exist; the result will then
     * carry no pre-warmed chunks and the caller can install nothing into the adapter.
     */
    static PreloadedRowGroupMetadata preload(ParquetFileReader reader, StorageObject storageObject, Set<String> predicateColumnPaths) {
        List<BlockMetaData> rowGroups = reader.getRowGroups();
        if (rowGroups.isEmpty()) {
            return empty();
        }

        if (storageObject != null) {
            try {
                return preloadCoalesced(reader, rowGroups, storageObject, predicateColumnPaths);
            } catch (Exception e) {
                logger.debug("Coalesced metadata preload failed, falling back to sequential: {}", e.getMessage());
            }
        }
        return preloadSequential(reader, rowGroups);
    }

    /**
     * Batched preloading via {@link CoalescedRangeReader}. Collects all column index and
     * offset index byte ranges across all row groups, plus dictionary-page and bloom-filter
     * ranges for predicate columns when supplied, fetches them in one coalesced batch, then
     * parses index ranges into typed objects and retains dictionary/bloom ranges as raw byte
     * chunks for {@link ParquetStorageObjectAdapter} pre-warming.
     *
     * <p><b>Parallelism:</b> {@link CoalescedRangeReader#readCoalesced} dispatches one
     * {@code readBytesAsync} call per merged range back-to-back without waiting between calls.
     * Dictionary pages from different row groups typically do not coalesce with each other
     * (they are separated by the row group's data pages), so each row group contributes its own
     * merged range. For native async storage backends like S3, the SDK runs all those requests
     * on its own event loop in parallel — turning what was N synchronous TLS handshakes into one
     * batch of concurrent connections. For local/default storage the dispatch is sequential on
     * the calling thread, but local reads are microseconds so the lack of parallelism is moot.
     */
    private static PreloadedRowGroupMetadata preloadCoalesced(
        ParquetFileReader reader,
        List<BlockMetaData> rowGroups,
        StorageObject storageObject,
        Set<String> predicateColumnPaths
    ) {
        List<CoalescedRangeReader.ByteRange> ranges = new ArrayList<>();
        List<RangeMeta> rangeMetas = new ArrayList<>();

        boolean fetchPreWarm = predicateColumnPaths != null && predicateColumnPaths.isEmpty() == false;
        for (int rgIdx = 0; rgIdx < rowGroups.size(); rgIdx++) {
            BlockMetaData block = rowGroups.get(rgIdx);
            for (ColumnChunkMetaData col : block.getColumns()) {
                IndexReference ciRef = col.getColumnIndexReference();
                if (ciRef != null && ciRef.getLength() > 0) {
                    addRange(ranges, rangeMetas, ciRef.getOffset(), ciRef.getLength(), rgIdx, col, RangeKind.COLUMN_INDEX);
                }
                IndexReference oiRef = col.getOffsetIndexReference();
                if (oiRef != null && oiRef.getLength() > 0) {
                    addRange(ranges, rangeMetas, oiRef.getOffset(), oiRef.getLength(), rgIdx, col, RangeKind.OFFSET_INDEX);
                }
                if (fetchPreWarm && predicateColumnPaths.contains(col.getPath().toDotString())) {
                    addDictionaryRange(ranges, rangeMetas, rgIdx, col);
                    addBloomFilterRange(ranges, rangeMetas, rgIdx, col);
                }
            }
        }

        if (ranges.isEmpty()) {
            return preloadSequential(reader, rowGroups);
        }

        logger.debug("Coalesced metadata preload: [{}] ranges across [{}] row groups", ranges.size(), rowGroups.size());

        PlainActionFuture<Map<CoalescedRangeReader.ByteRange, ByteBuffer>> future = new PlainActionFuture<>();
        CoalescedRangeReader.readCoalesced(storageObject, ranges, CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP, Runnable::run, future);
        Map<CoalescedRangeReader.ByteRange, ByteBuffer> fetched = future.actionGet();

        Map<String, ColumnIndex> columnIndexes = new HashMap<>();
        Map<String, OffsetIndex> offsetIndexes = new HashMap<>();
        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> preWarmedChunks = new TreeMap<>();

        for (RangeMeta meta : rangeMetas) {
            ByteBuffer buf = fetched.get(meta.range());
            if (buf == null) {
                continue;
            }
            String k = key(meta.rowGroupIdx(), meta.col());
            try {
                switch (meta.kind()) {
                    case COLUMN_INDEX -> {
                        byte[] bytes = toByteArray(buf);
                        org.apache.parquet.format.ColumnIndex thriftCI = Util.readColumnIndex(new ByteArrayInputStream(bytes));
                        if (thriftCI != null) {
                            ColumnIndex ci = ParquetMetadataConverter.fromParquetColumnIndex(meta.col().getPrimitiveType(), thriftCI);
                            if (ci != null) {
                                columnIndexes.put(k, ci);
                            }
                        }
                    }
                    case OFFSET_INDEX -> {
                        byte[] bytes = toByteArray(buf);
                        org.apache.parquet.format.OffsetIndex thriftOI = Util.readOffsetIndex(new ByteArrayInputStream(bytes));
                        if (thriftOI != null) {
                            OffsetIndex oi = ParquetMetadataConverter.fromParquetOffsetIndex(thriftOI);
                            if (oi != null) {
                                offsetIndexes.put(k, oi);
                            }
                        }
                    }
                    case DICTIONARY_PAGE, BLOOM_FILTER -> {
                        // Retain the raw buffer for the pre-warm cache; coalesced fetch already
                        // returned a sliced ByteBuffer of exactly the requested length.
                        preWarmedChunks.put(
                            meta.range().offset(),
                            new ColumnChunkPrefetcher.PrefetchedChunk(meta.range().offset(), meta.range().length(), buf)
                        );
                    }
                }
            } catch (Exception e) {
                logger.debug(
                    "Failed to parse [{}] for [{}] in row group [{}]: {}",
                    meta.kind(),
                    meta.col().getPath(),
                    meta.rowGroupIdx(),
                    e.getMessage()
                );
            }
        }

        return new PreloadedRowGroupMetadata(columnIndexes, offsetIndexes, preWarmedChunks);
    }

    private enum RangeKind {
        COLUMN_INDEX,
        OFFSET_INDEX,
        DICTIONARY_PAGE,
        BLOOM_FILTER
    }

    private record RangeMeta(CoalescedRangeReader.ByteRange range, int rowGroupIdx, ColumnChunkMetaData col, RangeKind kind) {}

    private static void addRange(
        List<CoalescedRangeReader.ByteRange> ranges,
        List<RangeMeta> rangeMetas,
        long offset,
        long length,
        int rowGroupIdx,
        ColumnChunkMetaData col,
        RangeKind kind
    ) {
        var br = new CoalescedRangeReader.ByteRange(offset, length);
        ranges.add(br);
        rangeMetas.add(new RangeMeta(br, rowGroupIdx, col, kind));
    }

    /**
     * Adds the dictionary page byte range for {@code col} when one is present. The dictionary
     * page sits at {@code [getDictionaryPageOffset(), getFirstDataPageOffset())} in the file.
     * Skips columns without a dictionary, signalled by either {@code hasDictionaryPage()} returning
     * false or by the offset/length being non-positive.
     */
    private static void addDictionaryRange(
        List<CoalescedRangeReader.ByteRange> ranges,
        List<RangeMeta> rangeMetas,
        int rowGroupIdx,
        ColumnChunkMetaData col
    ) {
        if (col.hasDictionaryPage() == false) {
            return;
        }
        long dictOffset = col.getDictionaryPageOffset();
        long firstDataPageOffset = col.getFirstDataPageOffset();
        // Defensive guard: writers occasionally emit non-monotonic offsets when the column has
        // no dictionary; treat the range as absent rather than fetching garbage bytes.
        if (dictOffset <= 0 || firstDataPageOffset <= dictOffset) {
            return;
        }
        addRange(ranges, rangeMetas, dictOffset, firstDataPageOffset - dictOffset, rowGroupIdx, col, RangeKind.DICTIONARY_PAGE);
    }

    /**
     * Adds the bloom filter byte range for {@code col} when one is present and its length is
     * known up front. parquet-mr allows bloom filter length to be omitted from the column chunk
     * metadata; in that case the length must be discovered by reading the bloom-filter header
     * from the file, which would defeat the point of pre-fetching. We skip those filters here
     * and let parquet-mr fall back to its existing synchronous read path.
     */
    private static void addBloomFilterRange(
        List<CoalescedRangeReader.ByteRange> ranges,
        List<RangeMeta> rangeMetas,
        int rowGroupIdx,
        ColumnChunkMetaData col
    ) {
        long bloomOffset = col.getBloomFilterOffset();
        int bloomLength = col.getBloomFilterLength();
        if (bloomOffset <= 0 || bloomLength <= 0) {
            return;
        }
        addRange(ranges, rangeMetas, bloomOffset, bloomLength, rowGroupIdx, col, RangeKind.BLOOM_FILTER);
    }

    private static byte[] toByteArray(ByteBuffer buf) {
        byte[] bytes = new byte[buf.remaining()];
        // Use a duplicate so the caller's buffer position is untouched — important for ranges we
        // also keep in the pre-warm cache.
        buf.duplicate().get(bytes);
        return bytes;
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

    /**
     * Returns the pre-warmed dictionary-page and bloom-filter byte chunks, keyed by file offset.
     * The map is empty when no predicate columns were supplied to {@link #preload} or when batch
     * fetching was not possible. Callers should pass the map to
     * {@link ParquetStorageObjectAdapter#installPreWarmedChunks} so subsequent reads issued by
     * parquet-mr's {@code RowGroupFilter} can be served from memory.
     *
     * <p>The returned map is unmodifiable to protect the adapter's snapshot semantics: streams
     * that capture the reference must observe a stable structure for their lifetime.
     */
    NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> preWarmedChunks() {
        return Collections.unmodifiableNavigableMap(preWarmedChunks);
    }

    private static String key(int rowGroupOrdinal, String columnPath) {
        return rowGroupOrdinal + ":" + columnPath;
    }

    static String key(int rowGroupOrdinal, ColumnChunkMetaData column) {
        return key(rowGroupOrdinal, column.getPath().toDotString());
    }
}
