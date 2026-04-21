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

import java.util.List;
import java.util.Map;

public class FilterEvaluationOrderEstimatorTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    public void testNoMetadata_preservesOriginalOrder() {
        List<Expression> filters = List.of(gt("a", 10), lt("b", 20));
        assertSame(filters, FilterEvaluationOrderEstimator.orderByEstimatedCost(filters, null));
    }

    public void testSinglePredicate_unchangedOrder() {
        SplitStats stats = metadata(1000, "a", 0L, 0, 100, 50_000L);
        List<Expression> filters = List.of(gt("a", 50));
        assertSame(filters, FilterEvaluationOrderEstimator.orderByEstimatedCost(filters, stats));
    }

    public void testRangeSelectivity_narrowRangeFirst() {
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(10000);
        builder.addColumn("timestamp", 0L, 0, 1000, 100_000L);
        builder.addColumn("level", 0L, 0, 5, 10_000L);
        SplitStats stats = builder.build();

        Expression tsFilter = gt("timestamp", 990);
        Expression levelFilter = lt("level", 3);

        List<Expression> result = FilterEvaluationOrderEstimator.orderByEstimatedCost(List.of(levelFilter, tsFilter), stats);
        assertSame(tsFilter, result.get(0));
        assertSame(levelFilter, result.get(1));
    }

    public void testEqualitySelectivity_smallDomainFirst() {
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(10000);
        builder.addColumn("status", 0L, 1, 5, 5_000L);
        builder.addColumn("user_id", 0L, 1, 100000, 50_000L);
        SplitStats stats = builder.build();

        Expression statusFilter = eq("status", 3);
        Expression userIdFilter = eq("user_id", 500);

        List<Expression> result = FilterEvaluationOrderEstimator.orderByEstimatedCost(List.of(statusFilter, userIdFilter), stats);
        assertSame(userIdFilter, result.get(0));
        assertSame(statusFilter, result.get(1));
    }

    public void testNullSelectivity_highNullRatioFirst() {
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(10000);
        builder.addColumn("sparse_col", 9500L, 0, 100, 50_000L);
        builder.addColumn("dense_col", 10L, 0, 100, 50_000L);
        SplitStats stats = builder.build();

        Expression sparseIsNull = new IsNull(SRC, fieldAttr("sparse_col"));
        Expression denseIsNull = new IsNull(SRC, fieldAttr("dense_col"));

        List<Expression> result = FilterEvaluationOrderEstimator.orderByEstimatedCost(List.of(sparseIsNull, denseIsNull), stats);
        assertSame(denseIsNull, result.get(0));
        assertSame(sparseIsNull, result.get(1));
    }

    public void testIsNotNull_ordering() {
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(10000);
        builder.addColumn("sparse_col", 9500L, 0, 100, 50_000L);
        builder.addColumn("dense_col", 10L, 0, 100, 50_000L);
        SplitStats stats = builder.build();

        Expression sparseNotNull = new IsNotNull(SRC, fieldAttr("sparse_col"));
        Expression denseNotNull = new IsNotNull(SRC, fieldAttr("dense_col"));

        List<Expression> result = FilterEvaluationOrderEstimator.orderByEstimatedCost(List.of(denseNotNull, sparseNotNull), stats);
        assertSame(sparseNotNull, result.get(0));
        assertSame(denseNotNull, result.get(1));
    }

    public void testColumnSizeTiebreaker_smallerColumnFirst() {
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(10000);
        builder.addColumn("small_col", 0L, 0, 100, 10_000L);
        builder.addColumn("big_col", 0L, 0, 100, 1_000_000L);
        SplitStats stats = builder.build();

        Expression smallFilter = gt("small_col", 50);
        Expression bigFilter = gt("big_col", 50);

        List<Expression> result = FilterEvaluationOrderEstimator.orderByEstimatedCost(List.of(bigFilter, smallFilter), stats);
        assertSame(smallFilter, result.get(0));
        assertSame(bigFilter, result.get(1));
    }

    public void testMixedPredicates_selectiveThenCheap() {
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(100000);
        builder.addColumn("timestamp", 0L, 0, 1000, 200_000L);
        builder.addColumn("status", 100L, 1, 5, 10_000L);
        builder.addColumn("user_id", 0L, 1, 1000000, 80_000L);
        SplitStats stats = builder.build();

        Expression tsFilter = gt("timestamp", 999);
        Expression statusFilter = eq("status", 3);
        Expression userFilter = gt("user_id", 500000);

        List<Expression> result = FilterEvaluationOrderEstimator.orderByEstimatedCost(List.of(userFilter, statusFilter, tsFilter), stats);
        assertSame(tsFilter, result.get(0));
        assertSame(statusFilter, result.get(1));
        assertSame(userFilter, result.get(2));
    }

    public void testNoStatsPredicates_preserveRelativeOrder() {
        SplitStats stats = new SplitStats.Builder().rowCount(10000).build();

        Expression a = gt("unknown_a", 50);
        Expression b = gt("unknown_b", 50);
        Expression c = gt("unknown_c", 50);

        List<Expression> result = FilterEvaluationOrderEstimator.orderByEstimatedCost(List.of(a, b, c), stats);
        assertSame(a, result.get(0));
        assertSame(b, result.get(1));
        assertSame(c, result.get(2));
    }

    public void testAllSameScore_preserveOriginalOrder() {
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(10000);
        builder.addColumn("a", 0L, 0, 100, 50_000L);
        builder.addColumn("b", 0L, 0, 100, 50_000L);
        SplitStats stats = builder.build();

        Expression filterA = gt("a", 50);
        Expression filterB = gt("b", 50);

        List<Expression> result = FilterEvaluationOrderEstimator.orderByEstimatedCost(List.of(filterA, filterB), stats);
        assertSame(filterA, result.get(0));
        assertSame(filterB, result.get(1));
    }

    public void testInSelectivity() {
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(10000);
        builder.addColumn("status", 0L, 1, 1000, 10_000L);
        builder.addColumn("code", 0L, 1, 100, 10_000L);
        SplitStats stats = builder.build();

        Expression inFilter = new In(SRC, fieldAttr("status"), List.of(intLiteral(1), intLiteral(2), intLiteral(3)));
        Expression codeFilter = gt("code", 90);

        List<Expression> result = FilterEvaluationOrderEstimator.orderByEstimatedCost(List.of(codeFilter, inFilter), stats);
        assertSame(inFilter, result.get(0));
        assertSame(codeFilter, result.get(1));
    }

    public void testEqualityOutsideRange() {
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(10000);
        builder.addColumn("x", 0L, 10, 20, 10_000L);
        builder.addColumn("y", 0L, 0, 100, 10_000L);
        SplitStats stats = builder.build();

        Expression xFilter = eq("x", 99);
        Expression yFilter = gt("y", 50);

        List<Expression> result = FilterEvaluationOrderEstimator.orderByEstimatedCost(List.of(yFilter, xFilter), stats);
        assertSame(xFilter, result.get(0));
        assertSame(yFilter, result.get(1));
    }

    public void testZeroRangeWidth_equalityMatch() {
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(10000);
        builder.addColumn("const_col", 0L, 42, 42, 10_000L);
        SplitStats stats = builder.build();

        double sel = FilterEvaluationOrderEstimator.estimateSelectivity(eq("const_col", 42), stats, 10000L);
        assertEquals(1.0, sel, 0.001);
    }

    public void testZeroRangeWidth_equalityMismatch() {
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(10000);
        builder.addColumn("const_col", 0L, 42, 42, 10_000L);
        SplitStats stats = builder.build();

        double sel = FilterEvaluationOrderEstimator.estimateSelectivity(eq("const_col", 99), stats, 10000L);
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

    private static SplitStats metadata(long rowCount, String col, long nullCount, int min, int max, long sizeBytes) {
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(rowCount);
        builder.addColumn(col, nullCount, min, max, sizeBytes);
        return builder.build();
    }
}
