/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.datasources.SplitStats;

import java.io.IOException;
import java.util.List;

/**
 * Optional capability for {@link FormatReader} implementations that can produce per-file
 * aggregate statistics (row count, per-column null count, min, max) without scanning the
 * file's row data.
 * <p>
 * This is used by {@code PushAggregatesToExternalSource} to push ungrouped {@code COUNT(*)},
 * {@code MIN}, and {@code MAX} aggregates to the runtime when planning-time pushdown is not
 * possible (e.g. multi-file globs where stats would need to be read serially at planning
 * time). At execution time, each split is dispatched to a driver in parallel and the
 * {@code MetadataAggregateOperator} calls
 * {@link #aggregateMetadata(StorageObject, List, long, long)} to obtain a {@link SplitStats}
 * that the existing aggregation pipeline reduces.
 * <p>
 * Implementations must aggregate across all row groups / stripes within the file whose
 * starting byte offset falls in the half-open range {@code [byteRangeStart, byteRangeEnd)}.
 * This mirrors Parquet's {@code withRange} convention: a row group "belongs to" whichever
 * split's byte range contains its starting position, ensuring disjoint coverage when a
 * file is split into multiple ranges. Pass {@code 0, Long.MAX_VALUE} to aggregate the
 * whole file.
 * <p>
 * Returning {@code null} signals that aggregate metadata is unavailable (e.g. footer
 * cannot be read); the caller falls back to a normal scan in that case.
 * <p>
 * The returned {@link SplitStats} should expose {@link SplitStats#rowCount()} for
 * {@code COUNT(*)}, and per-column {@code nullCount}/{@code min}/{@code max} for the
 * requested columns. Columns absent from the file or lacking statistics may be omitted.
 */
public interface MetadataAggregateReader extends FormatReader {

    /**
     * Returns aggregated stats for the row groups/stripes of the file whose starting
     * byte offset falls in {@code [byteRangeStart, byteRangeEnd)}.
     *
     * @param object         the file to inspect
     * @param columns        column names whose null-count/min/max are needed; readers may
     *                       still return stats for additional columns, but at minimum
     *                       should cover this set when statistics are present
     * @param byteRangeStart inclusive lower bound on row-group starting byte offset
     * @param byteRangeEnd   exclusive upper bound on row-group starting byte offset
     * @return aggregated stats for row groups in the range, or {@code null} if metadata
     *         is unavailable
     */
    SplitStats aggregateMetadata(StorageObject object, List<String> columns, long byteRangeStart, long byteRangeEnd) throws IOException;
}
