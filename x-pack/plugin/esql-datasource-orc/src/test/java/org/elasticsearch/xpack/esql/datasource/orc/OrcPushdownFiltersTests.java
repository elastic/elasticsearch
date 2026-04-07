/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
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

import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType;

public class OrcPushdownFiltersTests extends ESTestCase {

    private static final Source SOURCE = Source.EMPTY;

    // --- canConvert tests ---

    public void testCanConvertEquals() {
        assertTrue(OrcPushdownFilters.canConvert(eq("age", DataType.INTEGER, 30)));
    }

    public void testCanConvertNotEquals() {
        assertTrue(OrcPushdownFilters.canConvert(neq("status", DataType.KEYWORD, new BytesRef("active"))));
    }

    public void testCanConvertLessThan() {
        assertTrue(OrcPushdownFilters.canConvert(lt("score", DataType.DOUBLE, 9.5)));
    }

    public void testCanConvertLessThanOrEqual() {
        assertTrue(OrcPushdownFilters.canConvert(lte("score", DataType.LONG, 100L)));
    }

    public void testCanConvertGreaterThan() {
        assertTrue(OrcPushdownFilters.canConvert(gt("age", DataType.INTEGER, 18)));
    }

    public void testCanConvertGreaterThanOrEqual() {
        assertTrue(OrcPushdownFilters.canConvert(gte("count", DataType.LONG, 0L)));
    }

    public void testCanConvertIsNull() {
        assertTrue(OrcPushdownFilters.canConvert(isNull("name", DataType.KEYWORD)));
    }

    public void testCanConvertIsNotNull() {
        assertTrue(OrcPushdownFilters.canConvert(isNotNull("name", DataType.KEYWORD)));
    }

    public void testCanConvertIn() {
        assertTrue(OrcPushdownFilters.canConvert(in("status", DataType.KEYWORD, new BytesRef("a"), new BytesRef("b"))));
    }

    public void testCanConvertRange() {
        assertTrue(OrcPushdownFilters.canConvert(range("age", DataType.INTEGER, 10, 50)));
    }

    public void testCanConvertBoolean() {
        assertTrue(OrcPushdownFilters.canConvert(eq("active", DataType.BOOLEAN, true)));
    }

    public void testCanConvertDatetime() {
        assertTrue(OrcPushdownFilters.canConvert(eq("ts", DataType.DATETIME, 1700000000000L)));
    }

    public void testCannotConvertUnsupportedType() {
        assertFalse(OrcPushdownFilters.canConvert(eq("data", DataType.UNSUPPORTED, "x")));
    }

    public void testCannotConvertNonFoldableRight() {
        FieldAttribute left = field("a", DataType.INTEGER);
        FieldAttribute right = field("b", DataType.INTEGER);
        Equals expr = new Equals(SOURCE, left, right);
        assertFalse(OrcPushdownFilters.canConvert(expr));
    }

    // --- AND partial pushdown ---

    public void testAndPartialPushdown() {
        // One convertible, one not → AND is still convertible (partial pushdown safe)
        Expression convertible = eq("age", DataType.INTEGER, 30);
        Expression unconvertible = eq("data", DataType.UNSUPPORTED, "x");
        And and = new And(SOURCE, convertible, unconvertible);
        assertTrue(OrcPushdownFilters.canConvert(and));
    }

    public void testAndBothUnconvertible() {
        Expression a = eq("data1", DataType.UNSUPPORTED, "x");
        Expression b = eq("data2", DataType.UNSUPPORTED, "y");
        And and = new And(SOURCE, a, b);
        assertFalse(OrcPushdownFilters.canConvert(and));
    }

    // --- OR requires both sides ---

    public void testOrBothConvertible() {
        Expression a = eq("age", DataType.INTEGER, 30);
        Expression b = eq("score", DataType.DOUBLE, 9.5);
        Or or = new Or(SOURCE, a, b);
        assertTrue(OrcPushdownFilters.canConvert(or));
    }

    public void testOrOneUnconvertible() {
        Expression a = eq("age", DataType.INTEGER, 30);
        Expression b = eq("data", DataType.UNSUPPORTED, "x");
        Or or = new Or(SOURCE, a, b);
        assertFalse(OrcPushdownFilters.canConvert(or));
    }

    // --- NOT requires child convertible ---

    public void testNotConvertible() {
        Expression inner = eq("age", DataType.INTEGER, 30);
        Not not = new Not(SOURCE, inner);
        assertTrue(OrcPushdownFilters.canConvert(not));
    }

    public void testNotUnconvertible() {
        Expression inner = eq("data", DataType.UNSUPPORTED, "x");
        Not not = new Not(SOURCE, inner);
        assertFalse(OrcPushdownFilters.canConvert(not));
    }

    // --- IN with nulls ---

    public void testInWithSomeNulls() {
        // IN with mix of non-null and null values → still convertible
        FieldAttribute f = field("x", DataType.INTEGER);
        In inExpr = new In(SOURCE, f, List.of(literal(1, DataType.INTEGER), new Literal(SOURCE, null, DataType.INTEGER)));
        assertTrue(OrcPushdownFilters.canConvert(inExpr));
    }

    public void testInWithAllNulls() {
        // IN with only null values → not convertible
        FieldAttribute f = field("x", DataType.INTEGER);
        In inExpr = new In(SOURCE, f, List.of(new Literal(SOURCE, null, DataType.INTEGER), new Literal(SOURCE, null, DataType.INTEGER)));
        assertFalse(OrcPushdownFilters.canConvert(inExpr));
    }

    // --- buildSearchArgument tests ---

    public void testBuildSingleEquals() {
        SearchArgument sarg = OrcPushdownFilters.buildSearchArgument(List.of(eq("age", DataType.LONG, 30L)));
        assertNotNull(sarg);
        assertEquals(1, sarg.getLeaves().size());
        PredicateLeaf leaf = sarg.getLeaves().get(0);
        assertEquals("age", leaf.getColumnName());
        assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
        assertEquals(PredicateLeaf.Operator.EQUALS, leaf.getOperator());
        assertEquals(30L, leaf.getLiteral());
    }

    public void testBuildGreaterThan() {
        // GT is expressed as NOT(LTE)
        SearchArgument sarg = OrcPushdownFilters.buildSearchArgument(List.of(gt("score", DataType.DOUBLE, 9.5)));
        assertNotNull(sarg);
        assertEquals(1, sarg.getLeaves().size());
        PredicateLeaf leaf = sarg.getLeaves().get(0);
        assertEquals("score", leaf.getColumnName());
        assertEquals(PredicateLeaf.Type.FLOAT, leaf.getType());
        assertEquals(PredicateLeaf.Operator.LESS_THAN_EQUALS, leaf.getOperator());
        assertEquals(9.5, leaf.getLiteral());
    }

    public void testBuildMultipleFiltersAndCombined() {
        SearchArgument sarg = OrcPushdownFilters.buildSearchArgument(List.of(eq("a", DataType.LONG, 1L), eq("b", DataType.LONG, 2L)));
        assertNotNull(sarg);
        assertEquals(2, sarg.getLeaves().size());
    }

    public void testBuildInFiltersNullValues() {
        // IN with mix of non-null and null — nulls should be filtered out
        FieldAttribute f = field("x", DataType.INTEGER);
        In inExpr = new In(
            SOURCE,
            f,
            List.of(literal(1, DataType.INTEGER), new Literal(SOURCE, null, DataType.INTEGER), literal(3, DataType.INTEGER))
        );
        SearchArgument sarg = OrcPushdownFilters.buildSearchArgument(List.of(inExpr));
        assertNotNull(sarg);
        assertEquals(1, sarg.getLeaves().size());
        PredicateLeaf leaf = sarg.getLeaves().get(0);
        assertEquals(PredicateLeaf.Operator.IN, leaf.getOperator());
        // Should have 2 values (1 and 3), null filtered out
        assertEquals(2, leaf.getLiteralList().size());
    }

    public void testBuildDatetimeEquals() {
        long millis = 1704877200000L; // 2024-01-10T10:00:00Z
        SearchArgument sarg = OrcPushdownFilters.buildSearchArgument(List.of(eq("ts", DataType.DATETIME, millis)));
        assertNotNull(sarg);
        assertEquals(1, sarg.getLeaves().size());
        PredicateLeaf leaf = sarg.getLeaves().get(0);
        assertEquals("ts", leaf.getColumnName());
        assertEquals(PredicateLeaf.Type.TIMESTAMP, leaf.getType());
        assertEquals(PredicateLeaf.Operator.EQUALS, leaf.getOperator());
        assertEquals(new Timestamp(millis), leaf.getLiteral());
    }

    public void testBuildEmptyList() {
        assertNull(OrcPushdownFilters.buildSearchArgument(List.of()));
    }

    public void testBuildAndPartialPushdown() {
        // AND with one convertible side → only convertible side in SARG
        Expression convertible = eq("age", DataType.INTEGER, 30);
        Expression unconvertible = eq("data", DataType.UNSUPPORTED, "x");
        And and = new And(SOURCE, convertible, unconvertible);
        SearchArgument sarg = OrcPushdownFilters.buildSearchArgument(List.of(and));
        assertNotNull(sarg);
        assertEquals(1, sarg.getLeaves().size());
        assertEquals("age", sarg.getLeaves().get(0).getColumnName());
    }

    // --- Type conversion tests ---

    public void testResolveTypeLong() {
        assertEquals(PredicateLeaf.Type.LONG, OrcPushdownFilters.resolveType(DataType.LONG));
    }

    public void testResolveTypeInteger() {
        assertEquals(PredicateLeaf.Type.LONG, OrcPushdownFilters.resolveType(DataType.INTEGER));
    }

    public void testResolveTypeDouble() {
        assertEquals(PredicateLeaf.Type.FLOAT, OrcPushdownFilters.resolveType(DataType.DOUBLE));
    }

    public void testResolveTypeKeyword() {
        assertEquals(PredicateLeaf.Type.STRING, OrcPushdownFilters.resolveType(DataType.KEYWORD));
    }

    public void testResolveTypeDatetime() {
        assertEquals(PredicateLeaf.Type.TIMESTAMP, OrcPushdownFilters.resolveType(DataType.DATETIME));
    }

    public void testResolveTypeBoolean() {
        assertEquals(PredicateLeaf.Type.BOOLEAN, OrcPushdownFilters.resolveType(DataType.BOOLEAN));
    }

    public void testResolveTypeUnsupported() {
        assertNull(OrcPushdownFilters.resolveType(DataType.UNSUPPORTED));
    }

    // --- Literal conversion tests ---

    public void testConvertLiteralIntegerToLong() {
        Object result = OrcPushdownFilters.convertLiteral(42, DataType.INTEGER);
        assertNotNull(result);
        assertEquals(Long.class, result.getClass());
        assertEquals(42L, result);
    }

    public void testConvertLiteralBytesRefToString() {
        Object result = OrcPushdownFilters.convertLiteral(new BytesRef("hello"), DataType.KEYWORD);
        assertNotNull(result);
        assertEquals(String.class, result.getClass());
        assertEquals("hello", result);
    }

    public void testConvertLiteralDatetimeToTimestamp() {
        long millis = 1700000000000L;
        Object result = OrcPushdownFilters.convertLiteral(millis, DataType.DATETIME);
        assertNotNull(result);
        assertEquals(Timestamp.class, result.getClass());
        assertEquals(new Timestamp(millis), result);
    }

    public void testConvertLiteralBooleanDirect() {
        Object result = OrcPushdownFilters.convertLiteral(true, DataType.BOOLEAN);
        assertEquals(Boolean.class, result.getClass());
        assertEquals(true, result);
    }

    public void testConvertLiteralDoubleDirect() {
        Object result = OrcPushdownFilters.convertLiteral(3.14, DataType.DOUBLE);
        assertEquals(Double.class, result.getClass());
        assertEquals(3.14, result);
    }

    public void testConvertLiteralNull() {
        assertNull(OrcPushdownFilters.convertLiteral(null, DataType.INTEGER));
    }

    // --- StartsWith tests ---

    public void testCanConvertStartsWithKeyword() {
        FieldAttribute f = field("name", DataType.KEYWORD);
        Expression sw = new StartsWith(SOURCE, f, literal("alice", DataType.KEYWORD));
        assertTrue(OrcPushdownFilters.canConvert(sw));
    }

    public void testCanConvertStartsWithText() {
        FieldAttribute f = field("description", DataType.TEXT);
        Expression sw = new StartsWith(SOURCE, f, literal("hello", DataType.TEXT));
        assertTrue(OrcPushdownFilters.canConvert(sw));
    }

    public void testCanConvertStartsWithInteger() {
        FieldAttribute f = field("age", DataType.INTEGER);
        Expression sw = new StartsWith(SOURCE, f, literal(10, DataType.INTEGER));
        assertFalse(OrcPushdownFilters.canConvert(sw));
    }

    public void testCanConvertStartsWithNonFoldable() {
        FieldAttribute f = field("name", DataType.KEYWORD);
        FieldAttribute prefix = field("prefix", DataType.KEYWORD);
        Expression sw = new StartsWith(SOURCE, f, prefix);
        assertFalse(OrcPushdownFilters.canConvert(sw));
    }

    public void testBuildSearchArgumentStartsWith() {
        FieldAttribute f = field("name", DataType.KEYWORD);
        Expression sw = new StartsWith(SOURCE, f, literal("alice", DataType.KEYWORD));
        SearchArgument sarg = OrcPushdownFilters.buildSearchArgument(List.of(sw));
        assertNotNull(sarg);
        assertEquals(2, sarg.getLeaves().size());
        PredicateLeaf lowerLeaf = sarg.getLeaves().get(0);
        assertEquals("name", lowerLeaf.getColumnName());
        assertEquals(PredicateLeaf.Type.STRING, lowerLeaf.getType());
        assertEquals(PredicateLeaf.Operator.LESS_THAN, lowerLeaf.getOperator());
        assertEquals("alice", lowerLeaf.getLiteral());
    }

    public void testStartsWithInAndExpression() {
        FieldAttribute nameField = field("name", DataType.KEYWORD);
        FieldAttribute ageField = field("age", DataType.INTEGER);
        Expression sw = new StartsWith(SOURCE, nameField, literal("alice", DataType.KEYWORD));
        Expression eqExpr = eq("age", DataType.INTEGER, 30);
        And and = new And(SOURCE, sw, eqExpr);
        SearchArgument sarg = OrcPushdownFilters.buildSearchArgument(List.of(and));
        assertNotNull(sarg);
        assertTrue(sarg.getLeaves().size() >= 2);
    }

    // --- Helpers ---

    private static FieldAttribute field(String name, DataType dataType) {
        return new FieldAttribute(SOURCE, name, new EsField(name, dataType, Collections.emptyMap(), true, TimeSeriesFieldType.NONE));
    }

    private static Literal literal(Object value, DataType dataType) {
        Object literalValue = value;
        if (value instanceof String s) {
            literalValue = new BytesRef(s);
        }
        return new Literal(SOURCE, literalValue, dataType);
    }

    private static Expression eq(String fieldName, DataType type, Object value) {
        return new Equals(SOURCE, field(fieldName, type), literal(value, type));
    }

    private static Expression neq(String fieldName, DataType type, Object value) {
        return new NotEquals(SOURCE, field(fieldName, type), literal(value, type));
    }

    private static Expression lt(String fieldName, DataType type, Object value) {
        return new LessThan(SOURCE, field(fieldName, type), literal(value, type));
    }

    private static Expression lte(String fieldName, DataType type, Object value) {
        return new LessThanOrEqual(SOURCE, field(fieldName, type), literal(value, type));
    }

    private static Expression gt(String fieldName, DataType type, Object value) {
        return new GreaterThan(SOURCE, field(fieldName, type), literal(value, type));
    }

    private static Expression gte(String fieldName, DataType type, Object value) {
        return new GreaterThanOrEqual(SOURCE, field(fieldName, type), literal(value, type));
    }

    private static Expression isNull(String fieldName, DataType type) {
        return new IsNull(SOURCE, field(fieldName, type));
    }

    private static Expression isNotNull(String fieldName, DataType type) {
        return new IsNotNull(SOURCE, field(fieldName, type));
    }

    private static Expression in(String fieldName, DataType type, Object... values) {
        FieldAttribute f = field(fieldName, type);
        List<Expression> list = new ArrayList<>();
        for (Object v : values) {
            list.add(literal(v, type));
        }
        return new In(SOURCE, f, list);
    }

    private static Expression range(String fieldName, DataType type, Object lower, Object upper) {
        return new Range(SOURCE, field(fieldName, type), literal(lower, type), true, literal(upper, type), true, ZoneOffset.UTC);
    }
}
