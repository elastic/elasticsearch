/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.test.ESTestCase;

import java.util.EnumSet;
import java.util.Set;

/**
 * Unit tests for {@link TriviallyPassesChecker}. These tests construct {@link BlockMetaData}
 * directly with synthetic {@link Statistics} so that each predicate / stats combination can be
 * exercised in isolation without writing real Parquet files.
 */
public class TriviallyPassesCheckerTests extends ESTestCase {

    private static final String COL = "id";

    // ----- Eq -----

    public void testEqTriviallyPassesWhenMinEqualsMaxEqualsValue() {
        BlockMetaData block = blockWithIntStats(5, 5, 0L, 100L);
        assertTrue(TriviallyPassesChecker.check(FilterApi.eq(FilterApi.intColumn(COL), 5), block));
    }

    public void testEqDoesNotTriviallyPassWhenRangeStraddlesValue() {
        BlockMetaData block = blockWithIntStats(3, 7, 0L, 100L);
        assertFalse(TriviallyPassesChecker.check(FilterApi.eq(FilterApi.intColumn(COL), 5), block));
    }

    public void testEqDoesNotTriviallyPassWhenColumnHasNulls() {
        BlockMetaData block = blockWithIntStats(5, 5, 1L, 100L);
        assertFalse(TriviallyPassesChecker.check(FilterApi.eq(FilterApi.intColumn(COL), 5), block));
    }

    public void testEqWithNullLiteralTriviallyPassesWhenAllRowsNull() {
        BlockMetaData block = blockWithAllNullIntStats(50L);
        assertTrue(TriviallyPassesChecker.check(FilterApi.eq(FilterApi.intColumn(COL), null), block));
    }

    public void testEqWithNullLiteralDoesNotTriviallyPassWhenSomeRowsNonNull() {
        BlockMetaData block = blockWithIntStats(1, 1, 5L, 50L);
        assertFalse(TriviallyPassesChecker.check(FilterApi.eq(FilterApi.intColumn(COL), null), block));
    }

    // ----- NotEq -----

    public void testNotEqTriviallyPassesWhenValueAboveMax() {
        BlockMetaData block = blockWithIntStats(1, 5, 0L, 100L);
        assertTrue(TriviallyPassesChecker.check(FilterApi.notEq(FilterApi.intColumn(COL), 6), block));
    }

    public void testNotEqTriviallyPassesWhenValueBelowMin() {
        BlockMetaData block = blockWithIntStats(10, 20, 0L, 100L);
        assertTrue(TriviallyPassesChecker.check(FilterApi.notEq(FilterApi.intColumn(COL), 9), block));
    }

    public void testNotEqDoesNotTriviallyPassWhenValueWithinRange() {
        BlockMetaData block = blockWithIntStats(1, 10, 0L, 100L);
        assertFalse(TriviallyPassesChecker.check(FilterApi.notEq(FilterApi.intColumn(COL), 5), block));
    }

    public void testNotEqDoesNotTriviallyPassWhenColumnHasNulls() {
        BlockMetaData block = blockWithIntStats(1, 5, 1L, 100L);
        assertFalse(TriviallyPassesChecker.check(FilterApi.notEq(FilterApi.intColumn(COL), 100), block));
    }

    public void testNotEqWithNullLiteralTriviallyPassesWhenNoNulls() {
        BlockMetaData block = blockWithIntStats(1, 5, 0L, 100L);
        assertTrue(TriviallyPassesChecker.check(FilterApi.notEq(FilterApi.intColumn(COL), null), block));
    }

    public void testNotEqWithNullLiteralDoesNotTriviallyPassWhenAnyNulls() {
        BlockMetaData block = blockWithIntStats(1, 5, 1L, 100L);
        assertFalse(TriviallyPassesChecker.check(FilterApi.notEq(FilterApi.intColumn(COL), null), block));
    }

    // ----- Lt / LtEq / Gt / GtEq -----

    public void testLtTriviallyPasses() {
        BlockMetaData block = blockWithIntStats(0, 9, 0L, 100L);
        assertTrue(TriviallyPassesChecker.check(FilterApi.lt(FilterApi.intColumn(COL), 10), block));
    }

    public void testLtBoundaryDoesNotTriviallyPass() {
        // max == value, so Lt fails for max
        BlockMetaData block = blockWithIntStats(0, 10, 0L, 100L);
        assertFalse(TriviallyPassesChecker.check(FilterApi.lt(FilterApi.intColumn(COL), 10), block));
    }

    public void testLtEqTriviallyPassesAtBoundary() {
        BlockMetaData block = blockWithIntStats(0, 10, 0L, 100L);
        assertTrue(TriviallyPassesChecker.check(FilterApi.ltEq(FilterApi.intColumn(COL), 10), block));
    }

    public void testGtEqTriviallyPasses() {
        BlockMetaData block = blockWithIntStats(15, 20, 0L, 100L);
        assertTrue(TriviallyPassesChecker.check(FilterApi.gtEq(FilterApi.intColumn(COL), 10), block));
    }

    public void testGtEqDoesNotTriviallyPassWhenStatsStraddleThreshold() {
        BlockMetaData block = blockWithIntStats(5, 20, 0L, 100L);
        assertFalse(TriviallyPassesChecker.check(FilterApi.gtEq(FilterApi.intColumn(COL), 10), block));
    }

    public void testGtTriviallyPassesAtBoundary() {
        // min == 11, value == 10, so min > 10 holds for every row
        BlockMetaData block = blockWithIntStats(11, 20, 0L, 100L);
        assertTrue(TriviallyPassesChecker.check(FilterApi.gt(FilterApi.intColumn(COL), 10), block));
    }

    public void testGtEqualBoundaryDoesNotTriviallyPass() {
        BlockMetaData block = blockWithIntStats(10, 20, 0L, 100L);
        assertFalse(TriviallyPassesChecker.check(FilterApi.gt(FilterApi.intColumn(COL), 10), block));
    }

    public void testRangePredicateAndTriviallyPasses() {
        // x BETWEEN 10 AND 20 (inclusive both ends)
        FilterPredicate pred = FilterApi.and(FilterApi.gtEq(FilterApi.intColumn(COL), 10), FilterApi.ltEq(FilterApi.intColumn(COL), 20));
        BlockMetaData block = blockWithIntStats(12, 18, 0L, 100L);
        assertTrue(TriviallyPassesChecker.check(pred, block));
    }

    public void testRangePredicateAndDoesNotTriviallyPassWhenOneSideFails() {
        FilterPredicate pred = FilterApi.and(FilterApi.gtEq(FilterApi.intColumn(COL), 10), FilterApi.ltEq(FilterApi.intColumn(COL), 20));
        BlockMetaData block = blockWithIntStats(12, 25, 0L, 100L);
        assertFalse(TriviallyPassesChecker.check(pred, block));
    }

    // ----- In / NotIn -----

    public void testInWithSingletonValueRangeTriviallyPasses() {
        // min == max == 5, set contains 5
        BlockMetaData block = blockWithIntStats(5, 5, 0L, 50L);
        assertTrue(TriviallyPassesChecker.check(FilterApi.in(FilterApi.intColumn(COL), Set.of(5, 10)), block));
    }

    public void testInWithMultiValueRangeDoesNotTriviallyPass() {
        BlockMetaData block = blockWithIntStats(5, 7, 0L, 50L);
        assertFalse(TriviallyPassesChecker.check(FilterApi.in(FilterApi.intColumn(COL), Set.of(5, 10)), block));
    }

    public void testInDoesNotTriviallyPassWhenColumnHasNulls() {
        BlockMetaData block = blockWithIntStats(5, 5, 1L, 50L);
        assertFalse(TriviallyPassesChecker.check(FilterApi.in(FilterApi.intColumn(COL), Set.of(5, 10)), block));
    }

    public void testNotInTriviallyPassesWhenSetEntirelyOutsideRange() {
        BlockMetaData block = blockWithIntStats(50, 60, 0L, 100L);
        assertTrue(TriviallyPassesChecker.check(FilterApi.notIn(FilterApi.intColumn(COL), Set.of(5, 10)), block));
    }

    public void testNotInDoesNotTriviallyPassWhenSetIntersectsRange() {
        BlockMetaData block = blockWithIntStats(5, 60, 0L, 100L);
        assertFalse(TriviallyPassesChecker.check(FilterApi.notIn(FilterApi.intColumn(COL), Set.of(50, 70)), block));
    }

    // ----- Or -----

    public void testOrTriviallyPassesWhenEitherSideTriviallyPasses() {
        // x == 5 (min=max=5, no nulls) OR x > 100 (min=20, max=30 → false)
        BlockMetaData block = blockWithIntStats(5, 5, 0L, 50L);
        FilterPredicate pred = FilterApi.or(FilterApi.eq(FilterApi.intColumn(COL), 5), FilterApi.gt(FilterApi.intColumn(COL), 100));
        assertTrue(TriviallyPassesChecker.check(pred, block));
    }

    public void testOrDoesNotTriviallyPassWhenNeitherSideTriviallyPasses() {
        BlockMetaData block = blockWithIntStats(0, 50, 0L, 100L);
        FilterPredicate pred = FilterApi.or(FilterApi.eq(FilterApi.intColumn(COL), 5), FilterApi.gt(FilterApi.intColumn(COL), 100));
        assertFalse(TriviallyPassesChecker.check(pred, block));
    }

    // ----- Not -----

    public void testNotTriviallyPassesWhenInnerProvablyEmpty() {
        // Stats: id ∈ [50, 60]. Inner predicate id == 5 cannot match any row → Not trivially passes.
        BlockMetaData block = blockWithIntStats(50, 60, 0L, 100L);
        FilterPredicate pred = FilterApi.not(FilterApi.eq(FilterApi.intColumn(COL), 5));
        assertTrue(TriviallyPassesChecker.check(pred, block));
    }

    public void testNotDoesNotTriviallyPassWhenInnerCanMatch() {
        BlockMetaData block = blockWithIntStats(0, 100, 0L, 100L);
        FilterPredicate pred = FilterApi.not(FilterApi.eq(FilterApi.intColumn(COL), 5));
        assertFalse(TriviallyPassesChecker.check(pred, block));
    }

    public void testNestedNotReturnsFalseGracefully() {
        // parquet-mr's StatisticsFilter throws IllegalArgumentException on nested Not (asks
        // for LogicalInverseRewriter). The checker must degrade to false, not throw.
        BlockMetaData block = blockWithIntStats(50, 60, 0L, 100L);
        FilterPredicate inner = FilterApi.not(FilterApi.eq(FilterApi.intColumn(COL), 5));
        FilterPredicate nestedNot = FilterApi.not(inner);
        assertFalse(TriviallyPassesChecker.check(nestedNot, block));
    }

    // ----- Stats absent / missing column -----

    public void testReturnsFalseWhenStatsAreEmpty() {
        IntStatistics stats = new IntStatistics();
        // do not set min/max → empty
        BlockMetaData block = blockWithStats(stats, 100L);
        assertFalse(TriviallyPassesChecker.check(FilterApi.eq(FilterApi.intColumn(COL), 5), block));
    }

    public void testReturnsFalseWhenColumnMissingFromBlock() {
        BlockMetaData block = blockWithIntStats(5, 5, 0L, 100L);
        FilterPredicate predOnOtherCol = FilterApi.eq(FilterApi.intColumn("other"), 5);
        assertFalse(TriviallyPassesChecker.check(predOnOtherCol, block));
    }

    public void testNullsArguments() {
        BlockMetaData block = blockWithIntStats(5, 5, 0L, 100L);
        assertFalse(TriviallyPassesChecker.check(null, block));
        assertFalse(TriviallyPassesChecker.check(FilterApi.eq(FilterApi.intColumn(COL), 5), null));
    }

    // ----- Long type smoke test -----

    public void testLongComparisonTriviallyPasses() {
        LongStatistics stats = new LongStatistics();
        stats.setMinMax(100L, 200L);
        stats.setNumNulls(0L);
        PrimitiveType type = Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(COL);
        ColumnChunkMetaData ccm = ColumnChunkMetaData.get(
            ColumnPath.get(COL),
            type,
            CompressionCodecName.UNCOMPRESSED,
            null,
            EnumSet.of(Encoding.PLAIN),
            stats,
            0L,
            0L,
            500L,
            0L,
            0L
        );
        BlockMetaData block = new BlockMetaData();
        block.setRowCount(500L);
        block.addColumn(ccm);
        assertTrue(TriviallyPassesChecker.check(FilterApi.gtEq(FilterApi.longColumn(COL), 50L), block));
        assertFalse(TriviallyPassesChecker.check(FilterApi.gtEq(FilterApi.longColumn(COL), 150L), block));
    }

    // ----- helpers -----

    private static BlockMetaData blockWithIntStats(int min, int max, long numNulls, long valueCount) {
        IntStatistics stats = new IntStatistics();
        stats.setMinMax(min, max);
        stats.setNumNulls(numNulls);
        return blockWithStats(stats, valueCount);
    }

    private static BlockMetaData blockWithAllNullIntStats(long valueCount) {
        IntStatistics stats = new IntStatistics();
        // no setMinMax: column is all-null, no observed values
        stats.setNumNulls(valueCount);
        return blockWithStats(stats, valueCount);
    }

    private static BlockMetaData blockWithStats(Statistics<?> stats, long valueCount) {
        PrimitiveType type = Types.required(stats.type().getPrimitiveTypeName()).named(COL);
        ColumnChunkMetaData ccm = ColumnChunkMetaData.get(
            ColumnPath.get(COL),
            type,
            CompressionCodecName.UNCOMPRESSED,
            null,
            EnumSet.of(Encoding.PLAIN),
            stats,
            0L,
            0L,
            valueCount,
            0L,
            0L
        );
        BlockMetaData block = new BlockMetaData();
        block.setRowCount(valueCount);
        block.addColumn(ccm);
        return block;
    }
}
