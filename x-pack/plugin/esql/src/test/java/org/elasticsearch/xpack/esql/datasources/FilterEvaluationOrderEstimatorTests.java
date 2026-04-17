/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FilterEvaluationOrderEstimatorTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    public void testNoMetadata_preservesOriginalOrder() {
        List<Expression> filters = List.of(gt("a", 10), lt("b", 20));
        assertSame(filters, FilterEvaluationOrderEstimator.orderByEstimatedCost(filters, null));
        assertSame(filters, FilterEvaluationOrderEstimator.orderByEstimatedCost(filters, Map.of()));
    }

    public void testSinglePredicate_unchangedOrder() {
        Map<String, Object> meta = metadata(1000, "a", 0L, 0, 100, 50_000L);
        List<Expression> filters = List.of(gt("a", 50));
        assertSame(filters, FilterEvaluationOrderEstimator.orderByEstimatedCost(filters, meta));
    }

    public void testRangeSelectivity_narrowRangeFirst() {
        Map<String, Object> meta = new HashMap<>();
        meta.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 10000L);
        putColumnStats(meta, "timestamp", 0L, 0, 1000, 100_000L);
        putColumnStats(meta, "level", 0L, 0, 5, 10_000L);

        // timestamp > 990 eliminates ~99% of range; level < 3 eliminates ~40%
        Expression tsFilter = gt("timestamp", 990);
        Expression levelFilter = lt("level", 3);

        List<Expression> result = FilterEvaluationOrderEstimator.orderByEstimatedCost(List.of(levelFilter, tsFilter), meta);
        assertSame(tsFilter, result.get(0));
        assertSame(levelFilter, result.get(1));
    }

    public void testEqualitySelectivity_smallDomainFirst() {
        Map<String, Object> meta = new HashMap<>();
        meta.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 10000L);
        putColumnStats(meta, "status", 0L, 1, 5, 5_000L);
        putColumnStats(meta, "user_id", 0L, 1, 100000, 50_000L);

        // status == 3 has selectivity 1/4 = 0.25; user_id == 500 has selectivity 1/99999 ≈ 0.00001
        Expression statusFilter = eq("status", 3);
        Expression userIdFilter = eq("user_id", 500);

        List<Expression> result = FilterEvaluationOrderEstimator.orderByEstimatedCost(List.of(statusFilter, userIdFilter), meta);
        assertSame(userIdFilter, result.get(0));
        assertSame(statusFilter, result.get(1));
    }

    public void testNullSelectivity_highNullRatioFirst() {
        Map<String, Object> meta = new HashMap<>();
        meta.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 10000L);
        putColumnStats(meta, "sparse_col", 9500L, 0, 100, 50_000L);
        putColumnStats(meta, "dense_col", 10L, 0, 100, 50_000L);

        Expression sparseIsNull = new IsNull(SRC, fieldAttr("sparse_col"));
        Expression denseIsNull = new IsNull(SRC, fieldAttr("dense_col"));

        List<Expression> result = FilterEvaluationOrderEstimator.orderByEstimatedCost(List.of(sparseIsNull, denseIsNull), meta);
        // sparse_col IS NULL has selectivity 0.95 (high), dense_col IS NULL has selectivity 0.001 (low)
        // Lower selectivity comes first
        assertSame(denseIsNull, result.get(0));
        assertSame(sparseIsNull, result.get(1));
    }

    public void testIsNotNull_ordering() {
        Map<String, Object> meta = new HashMap<>();
        meta.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 10000L);
        putColumnStats(meta, "sparse_col", 9500L, 0, 100, 50_000L);
        putColumnStats(meta, "dense_col", 10L, 0, 100, 50_000L);

        Expression sparseNotNull = new IsNotNull(SRC, fieldAttr("sparse_col"));
        Expression denseNotNull = new IsNotNull(SRC, fieldAttr("dense_col"));

        List<Expression> result = FilterEvaluationOrderEstimator.orderByEstimatedCost(List.of(denseNotNull, sparseNotNull), meta);
        // sparse IS NOT NULL = 1 - 0.95 = 0.05 (very selective) -> first
        assertSame(sparseNotNull, result.get(0));
        assertSame(denseNotNull, result.get(1));
    }

    public void testColumnSizeTiebreaker_smallerColumnFirst() {
        Map<String, Object> meta = new HashMap<>();
        meta.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 10000L);
        putColumnStats(meta, "small_col", 0L, 0, 100, 10_000L);
        putColumnStats(meta, "big_col", 0L, 0, 100, 1_000_000L);

        // Same range => same selectivity => tiebreak on size_bytes
        Expression smallFilter = gt("small_col", 50);
        Expression bigFilter = gt("big_col", 50);

        List<Expression> result = FilterEvaluationOrderEstimator.orderByEstimatedCost(List.of(bigFilter, smallFilter), meta);
        assertSame(smallFilter, result.get(0));
        assertSame(bigFilter, result.get(1));
    }

    public void testMixedPredicates_selectiveThenCheap() {
        Map<String, Object> meta = new HashMap<>();
        meta.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100000L);
        putColumnStats(meta, "timestamp", 0L, 0, 1000, 200_000L);
        putColumnStats(meta, "status", 100L, 1, 5, 10_000L);
        putColumnStats(meta, "user_id", 0L, 1, 1000000, 80_000L);

        // timestamp > 999: selectivity ~ 0.001 (very selective)
        // status == 3: selectivity ~ 0.25
        // user_id > 500000: selectivity ~ 0.5
        Expression tsFilter = gt("timestamp", 999);
        Expression statusFilter = eq("status", 3);
        Expression userFilter = gt("user_id", 500000);

        List<Expression> result = FilterEvaluationOrderEstimator.orderByEstimatedCost(List.of(userFilter, statusFilter, tsFilter), meta);
        assertSame(tsFilter, result.get(0));
        assertSame(statusFilter, result.get(1));
        assertSame(userFilter, result.get(2));
    }

    public void testNoStatsPredicates_preserveRelativeOrder() {
        Map<String, Object> meta = new HashMap<>();
        meta.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 10000L);

        Expression a = gt("unknown_a", 50);
        Expression b = gt("unknown_b", 50);
        Expression c = gt("unknown_c", 50);

        List<Expression> result = FilterEvaluationOrderEstimator.orderByEstimatedCost(List.of(a, b, c), meta);
        assertSame(a, result.get(0));
        assertSame(b, result.get(1));
        assertSame(c, result.get(2));
    }

    public void testAllSameScore_preserveOriginalOrder() {
        Map<String, Object> meta = new HashMap<>();
        meta.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 10000L);
        putColumnStats(meta, "a", 0L, 0, 100, 50_000L);
        putColumnStats(meta, "b", 0L, 0, 100, 50_000L);

        Expression filterA = gt("a", 50);
        Expression filterB = gt("b", 50);

        List<Expression> result = FilterEvaluationOrderEstimator.orderByEstimatedCost(List.of(filterA, filterB), meta);
        assertSame(filterA, result.get(0));
        assertSame(filterB, result.get(1));
    }

    public void testInSelectivity() {
        Map<String, Object> meta = new HashMap<>();
        meta.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 10000L);
        putColumnStats(meta, "status", 0L, 1, 1000, 10_000L);
        putColumnStats(meta, "code", 0L, 1, 100, 10_000L);

        // IN(status, [1, 2, 3]) -> 3/999 ≈ 0.003
        // code > 90 -> (100-90)/99 ≈ 0.101
        Expression inFilter = new In(SRC, fieldAttr("status"), List.of(intLiteral(1), intLiteral(2), intLiteral(3)));
        Expression codeFilter = gt("code", 90);

        List<Expression> result = FilterEvaluationOrderEstimator.orderByEstimatedCost(List.of(codeFilter, inFilter), meta);
        assertSame(inFilter, result.get(0));
        assertSame(codeFilter, result.get(1));
    }

    public void testEqualityOutsideRange() {
        Map<String, Object> meta = new HashMap<>();
        meta.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 10000L);
        putColumnStats(meta, "x", 0L, 10, 20, 10_000L);
        putColumnStats(meta, "y", 0L, 0, 100, 10_000L);

        // x == 99 is outside [10, 20] => selectivity 0
        // y > 50 => selectivity 0.5
        Expression xFilter = eq("x", 99);
        Expression yFilter = gt("y", 50);

        List<Expression> result = FilterEvaluationOrderEstimator.orderByEstimatedCost(List.of(yFilter, xFilter), meta);
        assertSame(xFilter, result.get(0));
        assertSame(yFilter, result.get(1));
    }

    public void testZeroRangeWidth_equalityMatch() {
        Map<String, Object> meta = new HashMap<>();
        meta.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 10000L);
        putColumnStats(meta, "const_col", 0L, 42, 42, 10_000L);

        double sel = FilterEvaluationOrderEstimator.estimateSelectivity(eq("const_col", 42), meta, 10000L);
        assertEquals(1.0, sel, 0.001);
    }

    public void testZeroRangeWidth_equalityMismatch() {
        Map<String, Object> meta = new HashMap<>();
        meta.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 10000L);
        putColumnStats(meta, "const_col", 0L, 42, 42, 10_000L);

        double sel = FilterEvaluationOrderEstimator.estimateSelectivity(eq("const_col", 99), meta, 10000L);
        assertEquals(0.0, sel, 0.001);
    }

    // -- helpers --

    private static FieldAttribute fieldAttr(String name) {
        return new FieldAttribute(SRC, name, new EsField(name, DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static Literal intLiteral(int value) {
        return new Literal(SRC, value, DataType.INTEGER);
    }

    private static Expression gt(String field, int value) {
        return new GreaterThan(SRC, fieldAttr(field), intLiteral(value), null);
    }

    private static Expression lt(String field, int value) {
        return new LessThan(SRC, fieldAttr(field), intLiteral(value), null);
    }

    private static Expression eq(String field, int value) {
        return new Equals(SRC, fieldAttr(field), intLiteral(value), null);
    }

    private static Map<String, Object> metadata(long rowCount, String col, long nullCount, int min, int max, long sizeBytes) {
        Map<String, Object> meta = new HashMap<>();
        meta.put(SourceStatisticsSerializer.STATS_ROW_COUNT, rowCount);
        putColumnStats(meta, col, nullCount, min, max, sizeBytes);
        return meta;
    }

    private static void putColumnStats(Map<String, Object> meta, String col, long nullCount, int min, int max, long sizeBytes) {
        meta.put(SourceStatisticsSerializer.columnNullCountKey(col), nullCount);
        meta.put(SourceStatisticsSerializer.columnMinKey(col), min);
        meta.put(SourceStatisticsSerializer.columnMaxKey(col), max);
        meta.put(SourceStatisticsSerializer.columnSizeBytesKey(col), sizeBytes);
    }
}
