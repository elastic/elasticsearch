/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.statisticslevel.StatisticsFilter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;

/**
 * Row-group level statistics filter: returns true when {@code predicate} guarantees that
 * {@code block} contains no matching rows, so the row group can be skipped without reading
 * any data pages.
 *
 * <p>Restores the row-group statistics check that we lose by no longer calling
 * {@code ParquetFileReader.readNextFilteredRowGroup()} - delegates to parquet-mr's
 * {@link StatisticsFilter#canDrop} to keep behavior identical to the upstream stats filter.
 */
final class StatisticsRowGroupFilter {

    private StatisticsRowGroupFilter() {}

    static boolean canDrop(FilterPredicate predicate, BlockMetaData block) {
        if (predicate == null || block == null) {
            return false;
        }
        return StatisticsFilter.canDrop(predicate, block.getColumns());
    }
}
