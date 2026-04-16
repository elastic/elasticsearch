/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.time.ZoneOffset;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

public class ParquetFilterPushdownSupportTests extends ESTestCase {

    private final ParquetFilterPushdownSupport support = new ParquetFilterPushdownSupport();

    // --- Equality tests ---

    public void testEqualsIntegerPushed() {
        Attribute col = attr("salary", DataType.INTEGER);
        Expression filter = new Equals(Source.EMPTY, col, intLit(50000), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
        assertThat(result.pushedFilter(), instanceOf(ParquetPushedExpressions.class));
        assertEquals(1, result.remainder().size());
    }

    public void testEqualsLongPushed() {
        Attribute col = attr("id", DataType.LONG);
        Expression filter = new Equals(Source.EMPTY, col, longLit(123456789L), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
        assertEquals(1, result.remainder().size());
    }

    public void testEqualsDoublePushed() {
        Attribute col = attr("price", DataType.DOUBLE);
        Expression filter = new Equals(Source.EMPTY, col, doubleLit(99.99), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    public void testEqualsKeywordPushed() {
        Attribute col = attr("name", DataType.KEYWORD);
        Expression filter = new Equals(Source.EMPTY, col, keywordLit("alice"), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    public void testEqualsBooleanPushed() {
        Attribute col = attr("active", DataType.BOOLEAN);
        Expression filter = new Equals(Source.EMPTY, col, boolLit(true), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    public void testEqualsNullNotPushed() {
        Attribute col = attr("salary", DataType.INTEGER);
        Expression filter = new Equals(Source.EMPTY, col, new Literal(Source.EMPTY, null, DataType.INTEGER), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertFalse(result.hasPushedFilter());
    }

    // --- NotEquals tests ---

    public void testNotEqualsIntegerPushed() {
        Attribute col = attr("salary", DataType.INTEGER);
        Expression filter = new NotEquals(Source.EMPTY, col, intLit(50000), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
        assertEquals(1, result.remainder().size());
    }

    public void testNotEqualsLongPushed() {
        Attribute col = attr("id", DataType.LONG);
        Expression filter = new NotEquals(Source.EMPTY, col, longLit(999L), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    public void testNotEqualsDoublePushed() {
        Attribute col = attr("price", DataType.DOUBLE);
        Expression filter = new NotEquals(Source.EMPTY, col, doubleLit(0.0), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    public void testNotEqualsKeywordPushed() {
        Attribute col = attr("name", DataType.KEYWORD);
        Expression filter = new NotEquals(Source.EMPTY, col, keywordLit("deleted"), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    public void testNotEqualsBooleanPushed() {
        Attribute col = attr("active", DataType.BOOLEAN);
        Expression filter = new NotEquals(Source.EMPTY, col, boolLit(false), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    public void testNotEqualsNullNotPushed() {
        Attribute col = attr("salary", DataType.INTEGER);
        Expression filter = new NotEquals(Source.EMPTY, col, new Literal(Source.EMPTY, null, DataType.INTEGER), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertFalse(result.hasPushedFilter());
    }

    // --- IsNull / IsNotNull tests ---

    public void testIsNullPushed() {
        Attribute col = attr("name", DataType.KEYWORD);
        Expression filter = new IsNull(Source.EMPTY, col);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
        assertEquals(1, result.remainder().size());
    }

    public void testIsNullIntegerPushed() {
        Attribute col = attr("salary", DataType.INTEGER);
        Expression filter = new IsNull(Source.EMPTY, col);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    public void testIsNotNullPushed() {
        Attribute col = attr("name", DataType.KEYWORD);
        Expression filter = new IsNotNull(Source.EMPTY, col);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
        assertEquals(1, result.remainder().size());
    }

    public void testIsNotNullDoublePushed() {
        Attribute col = attr("price", DataType.DOUBLE);
        Expression filter = new IsNotNull(Source.EMPTY, col);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    public void testIsNullUnsupportedTypeNotPushed() {
        Attribute col = attr("loc", DataType.GEO_POINT);
        Expression filter = new IsNull(Source.EMPTY, col);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertFalse(result.hasPushedFilter());
    }

    // --- Range tests ---

    public void testRangePushedInclusiveBoth() {
        Attribute col = attr("age", DataType.INTEGER);
        Expression filter = new Range(Source.EMPTY, col, intLit(18), true, intLit(65), true, ZoneOffset.UTC);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
        assertEquals(1, result.remainder().size());
    }

    public void testRangePushedExclusiveBoth() {
        Attribute col = attr("price", DataType.DOUBLE);
        Expression filter = new Range(Source.EMPTY, col, doubleLit(0.0), false, doubleLit(100.0), false, ZoneOffset.UTC);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    public void testRangePushedMixedBounds() {
        Attribute col = attr("salary", DataType.LONG);
        Expression filter = new Range(Source.EMPTY, col, longLit(30000L), true, longLit(100000L), false, ZoneOffset.UTC);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    public void testRangeKeywordPushed() {
        Attribute col = attr("name", DataType.KEYWORD);
        Expression filter = new Range(Source.EMPTY, col, keywordLit("a"), true, keywordLit("z"), true, ZoneOffset.UTC);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    public void testRangeUnsupportedTypeNotPushed() {
        Attribute col = attr("loc", DataType.GEO_POINT);
        Literal lower = new Literal(Source.EMPTY, new BytesRef("point1"), DataType.GEO_POINT);
        Literal upper = new Literal(Source.EMPTY, new BytesRef("point2"), DataType.GEO_POINT);
        Expression filter = new Range(Source.EMPTY, col, lower, true, upper, true, ZoneOffset.UTC);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertFalse(result.hasPushedFilter());
    }

    // --- Comparison tests (GT, GTE, LT, LTE) ---

    public void testGreaterThanPushed() {
        Attribute col = attr("age", DataType.INTEGER);
        Expression filter = new GreaterThan(Source.EMPTY, col, intLit(25), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
        assertEquals(1, result.remainder().size());
    }

    public void testLessThanPushed() {
        Attribute col = attr("age", DataType.INTEGER);
        Expression filter = new LessThan(Source.EMPTY, col, intLit(65), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    public void testGreaterThanOrEqualPushed() {
        Attribute col = attr("salary", DataType.LONG);
        Expression filter = new GreaterThanOrEqual(Source.EMPTY, col, longLit(50000L), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    public void testLessThanOrEqualPushed() {
        Attribute col = attr("salary", DataType.LONG);
        Expression filter = new LessThanOrEqual(Source.EMPTY, col, longLit(100000L), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    // --- IN list tests ---

    public void testInListPushed() {
        Attribute col = attr("dept", DataType.KEYWORD);
        Expression filter = new In(Source.EMPTY, col, List.of(keywordLit("eng"), keywordLit("sales"), keywordLit("hr")));

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
        assertEquals(1, result.remainder().size());
    }

    public void testInListIntegerPushed() {
        Attribute col = attr("status", DataType.INTEGER);
        Expression filter = new In(Source.EMPTY, col, List.of(intLit(1), intLit(2), intLit(3)));

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    // --- And tests ---

    public void testAndPushed() {
        Attribute salary = attr("salary", DataType.LONG);
        Attribute age = attr("age", DataType.INTEGER);
        Expression left = new GreaterThan(Source.EMPTY, salary, longLit(50000L), null);
        Expression right = new LessThan(Source.EMPTY, age, intLit(65), null);
        Expression filter = new And(Source.EMPTY, left, right);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
        assertEquals(1, result.remainder().size());
    }

    public void testAndPartialPushdown() {
        Attribute salary = attr("salary", DataType.LONG);
        Attribute loc = attr("loc", DataType.GEO_POINT);
        Expression supported = new GreaterThan(Source.EMPTY, salary, longLit(50000L), null);
        Expression unsupported = new Equals(Source.EMPTY, loc, new Literal(Source.EMPTY, new BytesRef("point"), DataType.GEO_POINT), null);
        Expression filter = new And(Source.EMPTY, supported, unsupported);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
        assertEquals(1, result.remainder().size());
    }

    public void testAndBothUnsupportedNotPushed() {
        Attribute loc1 = attr("loc1", DataType.GEO_POINT);
        Attribute loc2 = attr("loc2", DataType.GEO_POINT);
        Expression left = new Equals(Source.EMPTY, loc1, new Literal(Source.EMPTY, new BytesRef("p1"), DataType.GEO_POINT), null);
        Expression right = new Equals(Source.EMPTY, loc2, new Literal(Source.EMPTY, new BytesRef("p2"), DataType.GEO_POINT), null);
        Expression filter = new And(Source.EMPTY, left, right);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertFalse(result.hasPushedFilter());
    }

    // --- Or tests ---

    public void testOrPushed() {
        Attribute col = attr("status", DataType.INTEGER);
        Expression left = new Equals(Source.EMPTY, col, intLit(1), null);
        Expression right = new Equals(Source.EMPTY, col, intLit(2), null);
        Expression filter = new Or(Source.EMPTY, left, right);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
        assertEquals(1, result.remainder().size());
    }

    public void testOrWithUnsupportedSideNotPushed() {
        Attribute salary = attr("salary", DataType.LONG);
        Attribute loc = attr("loc", DataType.GEO_POINT);
        Expression supported = new GreaterThan(Source.EMPTY, salary, longLit(50000L), null);
        Expression unsupported = new Equals(Source.EMPTY, loc, new Literal(Source.EMPTY, new BytesRef("point"), DataType.GEO_POINT), null);
        Expression filter = new Or(Source.EMPTY, supported, unsupported);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertFalse(result.hasPushedFilter());
    }

    // --- Not tests ---

    public void testNotPushed() {
        Attribute col = attr("active", DataType.BOOLEAN);
        Expression inner = new Equals(Source.EMPTY, col, boolLit(true), null);
        Expression filter = new Not(Source.EMPTY, inner);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
        assertEquals(1, result.remainder().size());
    }

    public void testNotWithUnsupportedInnerNotPushed() {
        Attribute loc = attr("loc", DataType.GEO_POINT);
        Expression inner = new Equals(Source.EMPTY, loc, new Literal(Source.EMPTY, new BytesRef("point"), DataType.GEO_POINT), null);
        Expression filter = new Not(Source.EMPTY, inner);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertFalse(result.hasPushedFilter());
    }

    // --- DATETIME pushdown tests ---

    public void testDatetimeEqualsPushed() {
        Attribute col = attr("ts", DataType.DATETIME);
        Expression filter = new Equals(Source.EMPTY, col, datetimeLit(1234567890L), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
        assertThat(result.pushedFilter(), instanceOf(ParquetPushedExpressions.class));
        assertEquals(1, result.remainder().size());
    }

    public void testDatetimeRangePushed() {
        Attribute col = attr("ts", DataType.DATETIME);
        Expression filter = new Range(Source.EMPTY, col, datetimeLit(1000L), true, datetimeLit(2000L), true, ZoneOffset.UTC);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
        assertEquals(1, result.remainder().size());
    }

    public void testDatetimeGreaterThanPushed() {
        Attribute col = attr("ts", DataType.DATETIME);
        Expression filter = new GreaterThan(Source.EMPTY, col, datetimeLit(1700000000000L), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    public void testDatetimeIsNullPushed() {
        Attribute col = attr("ts", DataType.DATETIME);
        Expression filter = new IsNull(Source.EMPTY, col);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    public void testDatetimeInListPushed() {
        Attribute col = attr("ts", DataType.DATETIME);
        Expression filter = new In(Source.EMPTY, col, List.of(datetimeLit(1000L), datetimeLit(2000L), datetimeLit(3000L)));

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    public void testDatetimeAndLongBothPushed() {
        Attribute salary = attr("salary", DataType.LONG);
        Attribute ts = attr("ts", DataType.DATETIME);
        Expression left = new GreaterThan(Source.EMPTY, salary, longLit(50000L), null);
        Expression right = new Equals(Source.EMPTY, ts, datetimeLit(1234567890L), null);
        Expression filter = new And(Source.EMPTY, left, right);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
        assertEquals(1, result.remainder().size());
    }

    public void testDatetimeOrIntegerBothPushed() {
        Attribute ts = attr("ts", DataType.DATETIME);
        Attribute status = attr("status", DataType.INTEGER);
        Expression left = new GreaterThan(Source.EMPTY, ts, datetimeLit(1000L), null);
        Expression right = new Equals(Source.EMPTY, status, intLit(1), null);
        Expression filter = new Or(Source.EMPTY, left, right);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    // --- Mixed filters ---

    public void testMixedFiltersSplitCorrectly() {
        Attribute salary = attr("salary", DataType.LONG);
        Attribute name = attr("name", DataType.KEYWORD);
        Attribute ts = attr("ts", DataType.DATETIME);

        Expression eq = new Equals(Source.EMPTY, name, keywordLit("alice"), null);
        Expression gt = new GreaterThan(Source.EMPTY, salary, longLit(50000L), null);
        Expression tsFilter = new Equals(Source.EMPTY, ts, datetimeLit(1234567890L), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(eq, gt, tsFilter));

        assertTrue(result.hasPushedFilter());
        assertEquals(3, result.remainder().size());
    }

    public void testMultiplePredicatesCombined() {
        Attribute salary = attr("salary", DataType.LONG);
        Attribute age = attr("age", DataType.INTEGER);

        Expression eq = new Equals(Source.EMPTY, salary, longLit(50000L), null);
        Expression gt = new GreaterThan(Source.EMPTY, age, intLit(25), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(eq, gt));

        assertTrue(result.hasPushedFilter());
        assertEquals(2, result.remainder().size());
    }

    public void testNoTranslatableExpressions() {
        Attribute loc = attr("loc", DataType.GEO_POINT);
        Expression filter = new Equals(Source.EMPTY, loc, new Literal(Source.EMPTY, new BytesRef("point"), DataType.GEO_POINT), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertFalse(result.hasPushedFilter());
        assertEquals(1, result.remainder().size());
    }

    // --- canPush tests ---

    public void testCanPushReturnsRecheckForSupported() {
        Attribute col = attr("salary", DataType.INTEGER);
        Expression filter = new Equals(Source.EMPTY, col, intLit(50000), null);

        assertEquals(FilterPushdownSupport.Pushability.RECHECK, support.canPush(filter));
    }

    public void testCanPushReturnsNoForUnsupported() {
        Attribute col = attr("loc", DataType.GEO_POINT);
        Expression filter = new Equals(Source.EMPTY, col, new Literal(Source.EMPTY, new BytesRef("point"), DataType.GEO_POINT), null);

        assertEquals(FilterPushdownSupport.Pushability.NO, support.canPush(filter));
    }

    // --- canConvert tests ---

    public void testCanConvertDatetime() {
        Attribute col = attr("ts", DataType.DATETIME);
        Expression filter = new Equals(Source.EMPTY, col, datetimeLit(1234567890L), null);

        assertTrue(ParquetFilterPushdownSupport.canConvert(filter));
    }

    public void testCanConvertNotEquals() {
        Attribute col = attr("salary", DataType.INTEGER);
        Expression filter = new NotEquals(Source.EMPTY, col, intLit(0), null);

        assertTrue(ParquetFilterPushdownSupport.canConvert(filter));
    }

    public void testCanConvertIsNull() {
        Attribute col = attr("name", DataType.KEYWORD);
        Expression filter = new IsNull(Source.EMPTY, col);

        assertTrue(ParquetFilterPushdownSupport.canConvert(filter));
    }

    public void testCanConvertIsNotNull() {
        Attribute col = attr("name", DataType.KEYWORD);
        Expression filter = new IsNotNull(Source.EMPTY, col);

        assertTrue(ParquetFilterPushdownSupport.canConvert(filter));
    }

    public void testCanConvertRange() {
        Attribute col = attr("age", DataType.INTEGER);
        Expression filter = new Range(Source.EMPTY, col, intLit(18), true, intLit(65), true, ZoneOffset.UTC);

        assertTrue(ParquetFilterPushdownSupport.canConvert(filter));
    }

    public void testCanConvertAndPartial() {
        Attribute salary = attr("salary", DataType.LONG);
        Attribute loc = attr("loc", DataType.GEO_POINT);
        Expression supported = new Equals(Source.EMPTY, salary, longLit(50000L), null);
        Expression unsupported = new Equals(Source.EMPTY, loc, new Literal(Source.EMPTY, new BytesRef("point"), DataType.GEO_POINT), null);

        assertTrue(ParquetFilterPushdownSupport.canConvert(new And(Source.EMPTY, supported, unsupported)));
    }

    public void testCanConvertOrRequiresBothSides() {
        Attribute salary = attr("salary", DataType.LONG);
        Attribute loc = attr("loc", DataType.GEO_POINT);
        Expression supported = new Equals(Source.EMPTY, salary, longLit(50000L), null);
        Expression unsupported = new Equals(Source.EMPTY, loc, new Literal(Source.EMPTY, new BytesRef("point"), DataType.GEO_POINT), null);

        assertFalse(ParquetFilterPushdownSupport.canConvert(new Or(Source.EMPTY, supported, unsupported)));
    }

    public void testCanConvertNot() {
        Attribute col = attr("active", DataType.BOOLEAN);
        Expression inner = new Equals(Source.EMPTY, col, boolLit(true), null);

        assertTrue(ParquetFilterPushdownSupport.canConvert(new Not(Source.EMPTY, inner)));
    }

    public void testCanConvertNotWithUnsupportedInner() {
        Attribute loc = attr("loc", DataType.GEO_POINT);
        Expression inner = new Equals(Source.EMPTY, loc, new Literal(Source.EMPTY, new BytesRef("point"), DataType.GEO_POINT), null);

        assertFalse(ParquetFilterPushdownSupport.canConvert(new Not(Source.EMPTY, inner)));
    }

    public void testBooleanOrderedComparisonNotConvertible() {
        Attribute col = attr("active", DataType.BOOLEAN);
        assertFalse(ParquetFilterPushdownSupport.canConvert(new GreaterThan(Source.EMPTY, col, boolLit(false), null)));
        assertFalse(ParquetFilterPushdownSupport.canConvert(new LessThan(Source.EMPTY, col, boolLit(true), null)));
    }

    public void testBooleanRangeNotConvertible() {
        Attribute col = attr("active", DataType.BOOLEAN);
        Expression filter = new Range(Source.EMPTY, col, boolLit(false), true, boolLit(true), true, ZoneOffset.UTC);

        assertFalse(ParquetFilterPushdownSupport.canConvert(filter));
    }

    // --- StartsWith tests ---

    public void testStartsWithKeywordPushed() {
        Attribute col = attr("name", DataType.KEYWORD);
        Expression filter = new StartsWith(Source.EMPTY, col, keywordLit("alice"));

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
        assertEquals(1, result.remainder().size());
    }

    public void testStartsWithNonKeywordNotPushed() {
        Attribute col = attr("age", DataType.INTEGER);
        Expression filter = new StartsWith(Source.EMPTY, col, intLit(10));

        assertFalse(ParquetFilterPushdownSupport.canConvert(filter));
    }

    public void testStartsWithNonFoldableNotPushed() {
        Attribute col = attr("name", DataType.KEYWORD);
        Attribute prefix = attr("prefix", DataType.KEYWORD);
        Expression filter = new StartsWith(Source.EMPTY, col, prefix);

        assertFalse(ParquetFilterPushdownSupport.canConvert(filter));
    }

    public void testStartsWithNullPrefixNotPushed() {
        Attribute col = attr("name", DataType.KEYWORD);
        Expression filter = new StartsWith(Source.EMPTY, col, new Literal(Source.EMPTY, null, DataType.KEYWORD));

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertFalse(result.hasPushedFilter());
    }

    public void testStartsWithCombinedWithEquals() {
        Attribute name = attr("name", DataType.KEYWORD);
        Attribute age = attr("age", DataType.INTEGER);
        Expression sw = new StartsWith(Source.EMPTY, name, keywordLit("alice"));
        Expression eq = new Equals(Source.EMPTY, age, intLit(30), null);
        Expression filter = new And(Source.EMPTY, sw, eq);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
        assertEquals(1, result.remainder().size());
    }

    public void testStartsWithInOr() {
        Attribute col = attr("name", DataType.KEYWORD);
        Expression sw = new StartsWith(Source.EMPTY, col, keywordLit("a"));
        Expression eq = new Equals(Source.EMPTY, col, keywordLit("z"), null);
        Expression filter = new Or(Source.EMPTY, sw, eq);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
        assertEquals(1, result.remainder().size());
    }

    // --- helpers ---

    private static Attribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }

    private static Literal intLit(int value) {
        return new Literal(Source.EMPTY, value, DataType.INTEGER);
    }

    private static Literal longLit(long value) {
        return new Literal(Source.EMPTY, value, DataType.LONG);
    }

    private static Literal doubleLit(double value) {
        return new Literal(Source.EMPTY, value, DataType.DOUBLE);
    }

    private static Literal keywordLit(String value) {
        return new Literal(Source.EMPTY, new BytesRef(value), DataType.KEYWORD);
    }

    private static Literal boolLit(boolean value) {
        return new Literal(Source.EMPTY, value, DataType.BOOLEAN);
    }

    private static Literal datetimeLit(long millis) {
        return new Literal(Source.EMPTY, millis, DataType.DATETIME);
    }
}
