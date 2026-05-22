/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Count-only iterator over a Parquet file: emits {@link Page} instances that carry only a
 * position count and no blocks, derived from the row group metadata in the footer.
 * <p>
 * Used by {@link ParquetFormatReader} when the planner's projection is empty (e.g. a query that
 * only references {@code _file.*} virtual columns, or a bare {@code COUNT(*)} atop the source) and
 * no record filter is active. Avoids constructing {@code ParquetColumnIterator} /
 * {@code OptimizedParquetColumnIterator}, both of which would either fall back to reading the full
 * file schema (when the projection is empty) or pay the column-reader setup cost for nothing —
 * and the deferred-extraction setup historically leaked allocator buffers via
 * {@code CircuitBreakerByteBufferAllocator} when no column reader ever consumed them.
 * <p>
 * Pages produced here are forwarded through {@link org.elasticsearch.xpack.esql.datasources.VirtualColumnIterator}
 * which slots in the constant {@code _file.*} blocks against the request circuit breaker.
 */
final class ParquetCountOnlyIterator implements CloseableIterator<Page> {

    private final ParquetFileReader reader;
    private final int batchSize;
    private final List<BlockMetaData> rowGroups;

    private int rowGroupIndex = 0;
    private long rowsRemainingInGroup = 0L;
    private int rowBudget;

    ParquetCountOnlyIterator(ParquetFileReader reader, int batchSize, int rowLimit) {
        this.reader = reader;
        this.batchSize = batchSize;
        this.rowBudget = rowLimit;
        this.rowGroups = reader.getRowGroups();
    }

    @Override
    public boolean hasNext() {
        if (rowBudget != FormatReader.NO_LIMIT && rowBudget <= 0) {
            return false;
        }
        while (rowsRemainingInGroup == 0L && rowGroupIndex < rowGroups.size()) {
            rowsRemainingInGroup = rowGroups.get(rowGroupIndex).getRowCount();
            rowGroupIndex++;
        }
        return rowsRemainingInGroup > 0L;
    }

    @Override
    public Page next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }
        long take = Math.min(rowsRemainingInGroup, batchSize);
        if (rowBudget != FormatReader.NO_LIMIT) {
            take = Math.min(take, rowBudget);
        }
        int positions = (int) take;
        rowsRemainingInGroup -= positions;
        if (rowBudget != FormatReader.NO_LIMIT) {
            rowBudget -= positions;
        }
        // Zero-block page: position count is the only payload. The downstream
        // {@link org.elasticsearch.xpack.esql.datasources.VirtualColumnIterator} fills in the
        // {@code _file.*} constant blocks against the request circuit breaker.
        return new Page(positions);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
