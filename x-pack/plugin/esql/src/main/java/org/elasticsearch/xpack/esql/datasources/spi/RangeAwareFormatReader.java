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
 *   <li>{@link #resolveFileLayout} opens the file's footer/metadata once and returns both
 *       schema/statistics and byte ranges for each independently readable unit
 *       (e.g. row group / stripe).</li>
 *   <li>The framework creates one split per range, distributed across drivers/nodes.</li>
 *   <li>At read time, {@link #readRange} receives the full-file object and the assigned
 *       byte range, reading only the relevant row groups.</li>
 * </ol>
 * <p>
 * {@link #resolveFileLayout} is the single primitive for this SPI: implementations must
 * perform exactly one format reader open (i.e. one footer/metadata read) per invocation
 * and emit both pieces of information together. {@link #metadata(StorageObject)} is a
 * derived view that delegates to {@code resolveFileLayout} and discards the split ranges;
 * metadata-only callers therefore still pay exactly one footer read per call. The CPU cost
 * of building the discarded {@link FileLayout} is negligible compared to the remote I/O
 * the contract avoids.
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
     * Resolves both schema/statistics and split ranges from a single open of the file's
     * footer/metadata. This is the single SPI primitive for columnar formats: callers that
     * only need metadata go through {@link #metadata(StorageObject)}, which is a derived
     * view over this same method. Implementations must perform exactly one format reader
     * open (one footer/metadata read) per invocation; multiple internal CPU passes over
     * the already-loaded footer are allowed.
     *
     * @param object the storage object representing the full file
     * @return the file's {@link FileLayout}: metadata and split ranges
     */
    FileLayout resolveFileLayout(StorageObject object) throws IOException;

    /**
     * Returns the metadata for the given file. Derived from {@link #resolveFileLayout};
     * range information from that single footer read is discarded for metadata-only
     * callers, preserving the "exactly one footer read per call" guarantee.
     */
    @Override
    default SourceMetadata metadata(StorageObject object) throws IOException {
        return resolveFileLayout(object).metadata();
    }

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
