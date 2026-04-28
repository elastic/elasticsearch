/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;

import java.io.IOException;

/**
 * Optional capability for {@link FormatReader} implementations that can compute ungrouped
 * aggregates directly inside the scan, choosing per row group whether to derive the
 * intermediate-state page from column statistics or by scanning the row data.
 * <p>
 * This is used by {@code PushAggregatesToExternalSource} to push ungrouped {@code COUNT(*)},
 * {@code COUNT(field)}, {@code MIN(field)}, and {@code MAX(field)} aggregates to the runtime
 * when planning-time pushdown is unavailable (e.g. multi-file globs where stats would need
 * to be read serially at planning time). At execution time, each split is dispatched to a
 * driver in parallel and the runtime aggregate-scan operator iterates pages emitted by
 * {@link #scanForAggregates}.
 * <p>
 * Implementations consider every row group / stripe in the file whose starting byte offset
 * falls in the half-open range {@code [byteRangeStart, byteRangeEnd)}. This mirrors Parquet's
 * {@code withRange} convention: a row group "belongs to" whichever split's byte range
 * contains its starting position, ensuring disjoint coverage when a file is split into
 * multiple ranges. Pass {@code 0, Long.MAX_VALUE} to scan the whole file.
 * <p>
 * For each row group the implementation chooses internally:
 * <ul>
 *   <li>if column statistics cover every column referenced by {@code spec.ops()}, build the
 *       intermediate-state page from stats (fast path);</li>
 *   <li>otherwise read the row data for those columns and accumulate, then emit a single
 *       intermediate-state page (slow path).</li>
 * </ul>
 * Both paths produce a {@link Page} matching {@code spec.intermediateAttributes()} so the
 * caller can feed pages straight into the FINAL aggregator.
 * <p>
 * The returned iterator yields one {@link Page} per row group (or per logical unit of work).
 * Implementations should bound memory: accumulators are constant-size per aggregate, so
 * even a row group with millions of rows produces a single intermediate-state page.
 */
public interface AggregateScanReader extends FormatReader {

    /**
     * Open an iterator over intermediate-state pages for the row groups whose starting byte
     * offset falls in {@code [byteRangeStart, byteRangeEnd)}. The caller must close the
     * iterator when done (typically via try-with-resources).
     *
     * @param object         the file to scan
     * @param spec           aggregate operations and intermediate-state shape; columns derive
     *                       from {@code spec.ops()}
     * @param blockFactory   block factory for emitted pages
     * @param byteRangeStart inclusive lower bound on row-group starting byte offset
     * @param byteRangeEnd   exclusive upper bound on row-group starting byte offset
     */
    CloseableIterator<Page> scanForAggregates(
        StorageObject object,
        AggregateScanSpec spec,
        BlockFactory blockFactory,
        long byteRangeStart,
        long byteRangeEnd
    ) throws IOException;
}
