/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

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
     * @param object            the full-file storage object
     * @param projectedColumns  columns to project
     * @param batchSize         rows per page
     * @param rangeStart        start byte offset of the assigned range
     * @param rangeEnd          end byte offset (exclusive) of the assigned range
     * @param resolvedAttributes schema attributes resolved from metadata
     * @param errorPolicy       error handling policy
     * @return an iterator that yields pages from the matching row groups
     */
    CloseableIterator<Page> readRange(
        StorageObject object,
        List<String> projectedColumns,
        int batchSize,
        long rangeStart,
        long rangeEnd,
        List<Attribute> resolvedAttributes,
        ErrorPolicy errorPolicy
    ) throws IOException;
}
