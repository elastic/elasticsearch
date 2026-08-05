/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Extension of {@link FormatReader} for columnar formats (Parquet, ORC) that support
 * row-group-level split parallelism.
 * <p>
 * Unlike line-oriented formats that use {@link SegmentableFormatReader} with byte-range
 * views, columnar formats need full-file access (e.g. Parquet's footer is at EOF) but
 * can selectively read row groups within a byte range. This interface separates the
 * range specification from the file access, allowing the reader to open the full file
 * and use format-native range filtering (e.g. {@code ParquetReadOptions.withRange()}).
 * <p>
 * The split discovery flow:
 * <ol>
 *   <li>{@link #discoverSplitRanges} reads file metadata (e.g. Parquet footer) and
 *       returns byte ranges for each independently readable unit (e.g. row group).</li>
 *   <li>The framework creates one split per range, distributed across drivers/nodes.</li>
 *   <li>At read time, {@link #readRange} receives the full-file object and the assigned
 *       byte range, reading only the relevant row groups.</li>
 * </ol>
 */
public interface RangeAwareFormatReader extends FormatReader {

    /**
     * A byte range within a file with optional per-range statistics (e.g. per-row-group
     * Parquet statistics). Statistics use the same {@code _stats.*} key convention as
     * {@code sourceMetadata} for consistency with {@code SourceStatisticsSerializer}.
     */
    record SplitRange(long offset, long length, Map<String, Object> statistics) {
        public SplitRange {
            if (statistics == null) {
                statistics = Map.of();
            }
        }

        public SplitRange(long offset, long length) {
            this(offset, length, Map.of());
        }
    }

    /**
     * Discovers independently readable byte ranges within a file by reading its metadata.
     * Each range typically corresponds to one row group (Parquet) or stripe (ORC).
     * <p>
     * Returns a list of {@link SplitRange} objects. An empty list means the
     * file cannot be split (e.g. single row group) and should be read as a whole.
     *
     * @param object the storage object representing the full file
     * @return list of split ranges with optional per-range statistics
     */
    List<SplitRange> discoverSplitRanges(StorageObject object) throws IOException;

    /**
     * Reads only the row groups / stripes that fall within the given byte range.
     * The storage object must represent the full file (not a range-limited view),
     * because columnar formats need access to file-level metadata (e.g. footer).
     *
     * @param object  the full-file storage object
     * @param context per-split read parameters and optional cross-split file context
     * @return an iterator that yields pages from the matching row groups
     */
    CloseableIterator<Page> readRange(StorageObject object, RangeReadContext context) throws IOException;

    /**
     * Returns {@code true} if this reader supports batch multi-file reads via
     * {@link #readAll}. When supported the execution framework calls {@link #readAll}
     * for a batch of files instead of calling {@link #readRange} once per split,
     * allowing the reader to process multiple files concurrently in a single call.
     * <p>
     * Only enabled when there are no per-file virtual partition columns (those require
     * per-split injection that is incompatible with a unified batch iterator).
     * The default implementation returns {@code false}.
     */
    default boolean supportsBatchRead() {
        return false;
    }

    /**
     * Reads all given objects in a single batched call, returning a unified page iterator.
     * Called by the framework instead of {@link #readRange} when {@link #supportsBatchRead()}
     * returns {@code true}.
     * <p>
     * The reader is responsible for applying any pushed filters and projections internally.
     * The returned iterator may interleave pages from different files.
     *
     * @param splits           per-split descriptors; each carries a storage object plus the byte
     *                         range {@code [offset, offset+length)} that identifies which row
     *                         groups within the file belong to this split
     * @param projectedColumns columns to project, or {@code null} for all columns
     * @param batchSize        target page size in rows
     */
    default CloseableIterator<Page> readAll(List<SplitRef> splits, List<String> projectedColumns, int batchSize) throws IOException {
        throw new UnsupportedOperationException("readAll not supported by " + getClass().getSimpleName());
    }

    /**
     * Per-split descriptor passed to {@link #readAll}. Carries the storage object together
     * with the byte range {@code [offset, offset+length)} that identifies the row groups
     * belonging to this split within the file.
     */
    record SplitRef(StorageObject object, long offset, long length) {}
}
