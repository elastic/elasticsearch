/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

public class StatisticsRowGroupFilterTests extends ESTestCase {

    public void testCanDropWhenStatsExcludeValue() {
        BlockMetaData block = blockWithIntColumn("id", 0, 99);
        FilterPredicate predicate = FilterApi.eq(FilterApi.intColumn("id"), 500);
        assertTrue(StatisticsRowGroupFilter.canDrop(predicate, block));
    }

    public void testCannotDropWhenStatsContainValue() {
        BlockMetaData block = blockWithIntColumn("id", 0, 999);
        FilterPredicate predicate = FilterApi.eq(FilterApi.intColumn("id"), 500);
        assertFalse(StatisticsRowGroupFilter.canDrop(predicate, block));
    }

    public void testCannotDropOnNullPredicate() {
        BlockMetaData block = blockWithIntColumn("id", 0, 5);
        assertFalse(StatisticsRowGroupFilter.canDrop(null, block));
    }

    public void testCannotDropOnNullBlock() {
        FilterPredicate predicate = FilterApi.eq(FilterApi.intColumn("id"), 1);
        assertFalse(StatisticsRowGroupFilter.canDrop(predicate, null));
    }

    public void testCanDropOnRangePredicateOutsideStats() {
        BlockMetaData block = blockWithIntColumn("id", 100, 200);
        FilterPredicate predicate = FilterApi.gt(FilterApi.intColumn("id"), 1000);
        assertTrue(StatisticsRowGroupFilter.canDrop(predicate, block));
    }

    private static BlockMetaData blockWithIntColumn(String name, int min, int max) {
        IntStatistics stats = new IntStatistics();
        stats.setMinMax(min, max);
        stats.incrementNumNulls(0);
        ColumnChunkMetaData column = ColumnChunkMetaData.get(
            ColumnPath.fromDotString(name),
            PrimitiveType.PrimitiveTypeName.INT32,
            CompressionCodecName.UNCOMPRESSED,
            Set.of(),
            stats,
            0L,
            0L,
            (max - min) + 1L,
            16L,
            16L
        );
        BlockMetaData block = new BlockMetaData();
        block.addColumn(column);
        block.setRowCount((max - min) + 1L);
        block.setTotalByteSize(16L);
        return block;
    }
}
