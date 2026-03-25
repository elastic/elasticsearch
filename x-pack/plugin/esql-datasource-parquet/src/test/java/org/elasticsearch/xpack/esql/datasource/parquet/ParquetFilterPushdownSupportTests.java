/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;

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
        assertThat(result.pushedFilter(), instanceOf(FilterCompat.Filter.class));
        // RECHECK: original expression stays in remainder
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

    public void testEqualsSwappedPushed() {
        // literal = column (swapped)
        Attribute col = attr("salary", DataType.INTEGER);
        Expression filter = new Equals(Source.EMPTY, intLit(50000), col, null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    public void testEqualsNullNotPushed() {
        Attribute col = attr("salary", DataType.INTEGER);
        Expression filter = new Equals(Source.EMPTY, col, new Literal(Source.EMPTY, null, DataType.INTEGER), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertFalse(result.hasPushedFilter());
    }

    // --- Range tests ---

    public void testGreaterThanPushed() {
        Attribute col = attr("age", DataType.INTEGER);
        Expression filter = new GreaterThan(Source.EMPTY, col, intLit(25), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
        assertEquals(1, result.remainder().size()); // RECHECK
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

    public void testRangeSwappedOperator() {
        // literal > column → column < literal
        Attribute col = attr("age", DataType.INTEGER);
        Expression filter = new GreaterThan(Source.EMPTY, intLit(65), col, null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    // --- IN list tests ---

    public void testInListPushed() {
        Attribute col = attr("dept", DataType.KEYWORD);
        Expression filter = new In(Source.EMPTY, col, List.of(keywordLit("eng"), keywordLit("sales"), keywordLit("hr")));

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
        assertEquals(1, result.remainder().size()); // RECHECK
    }

    public void testInListIntegerPushed() {
        Attribute col = attr("status", DataType.INTEGER);
        Expression filter = new In(Source.EMPTY, col, List.of(intLit(1), intLit(2), intLit(3)));

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertTrue(result.hasPushedFilter());
    }

    // --- Unsupported types ---

    public void testUnsupportedDatetimeNotPushed() {
        Attribute col = attr("ts", DataType.DATETIME);
        Expression filter = new Equals(Source.EMPTY, col, new Literal(Source.EMPTY, 1234567890L, DataType.DATETIME), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        assertFalse(result.hasPushedFilter());
    }

    // --- Mixed filters ---

    public void testMixedFiltersSplitCorrectly() {
        Attribute salary = attr("salary", DataType.LONG);
        Attribute name = attr("name", DataType.KEYWORD);
        Attribute ts = attr("ts", DataType.DATETIME);

        Expression eq = new Equals(Source.EMPTY, name, keywordLit("alice"), null);
        Expression gt = new GreaterThan(Source.EMPTY, salary, longLit(50000L), null);
        Expression tsFilter = new Equals(Source.EMPTY, ts, new Literal(Source.EMPTY, 1234567890L, DataType.DATETIME), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(eq, gt, tsFilter));

        // eq and gt should be pushed, tsFilter should not
        assertTrue(result.hasPushedFilter());
        // All 3 are in remainder (RECHECK for pushed, untranslatable for ts)
        assertEquals(3, result.remainder().size());
    }

    public void testMultiplePredicatesCombined() {
        Attribute salary = attr("salary", DataType.LONG);
        Attribute age = attr("age", DataType.INTEGER);

        Expression eq = new Equals(Source.EMPTY, salary, longLit(50000L), null);
        Expression gt = new GreaterThan(Source.EMPTY, age, intLit(25), null);

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(eq, gt));

        assertTrue(result.hasPushedFilter());
        // Both pushed with RECHECK
        assertEquals(2, result.remainder().size());
    }

    public void testNoTranslatableExpressions() {
        Attribute ts = attr("ts", DataType.DATETIME);
        Expression filter = new Equals(Source.EMPTY, ts, new Literal(Source.EMPTY, 1234567890L, DataType.DATETIME), null);

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
        Attribute col = attr("ts", DataType.DATETIME);
        Expression filter = new Equals(Source.EMPTY, col, new Literal(Source.EMPTY, 1234567890L, DataType.DATETIME), null);

        assertEquals(FilterPushdownSupport.Pushability.NO, support.canPush(filter));
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
}
