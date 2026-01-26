/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datalake.iceberg;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
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

import java.util.Arrays;
import java.util.List;

/**
 * Tests for converting ESQL expressions to Iceberg filter expressions.
 */
public class IcebergFilterPushdownTests extends ESTestCase {

    private static final Source EMPTY = Source.EMPTY;

    // Helper to create attribute
    private ReferenceAttribute attr(String name, DataType type) {
        return new ReferenceAttribute(EMPTY, name, type);
    }

    // Helper to create literal - converts Strings to BytesRef for KEYWORD type
    private Literal literal(Object value) {
        if (value instanceof String s) {
            return new Literal(EMPTY, new BytesRef(s), DataType.KEYWORD);
        }
        return new Literal(EMPTY, value, DataType.fromJava(value));
    }

    /**
     * Helper method to assert that an Iceberg expression is not null and contains all expected strings.
     */
    private void assertIcebergFilter(org.apache.iceberg.expressions.Expression icebergExpr, String... expectedStrings) {
        assertNotNull(icebergExpr);
        String exprString = icebergExpr.toString();
        for (String expected : expectedStrings) {
            assertTrue("Expected '" + expected + "' in expression: " + exprString, exprString.contains(expected));
        }
    }

    public void testEqualsConversion() {
        ReferenceAttribute age = attr("age", DataType.INTEGER);
        Literal value = literal(25);
        Equals eq = new Equals(EMPTY, age, value);

        org.apache.iceberg.expressions.Expression icebergExpr = IcebergPushdownFilters.convert(eq);
        assertIcebergFilter(icebergExpr, "age", "25");
    }

    public void testNotEqualsConversion() {
        ReferenceAttribute status = attr("status", DataType.KEYWORD);
        Literal value = literal("inactive");
        NotEquals neq = new NotEquals(EMPTY, status, value);

        org.apache.iceberg.expressions.Expression icebergExpr = IcebergPushdownFilters.convert(neq);
        assertIcebergFilter(icebergExpr, "status", "inactive");
    }

    public void testLessThanConversion() {
        ReferenceAttribute age = attr("age", DataType.INTEGER);
        Literal value = literal(30);
        LessThan lt = new LessThan(EMPTY, age, value, null);

        org.apache.iceberg.expressions.Expression icebergExpr = IcebergPushdownFilters.convert(lt);
        assertIcebergFilter(icebergExpr, "age", "<");
    }

    public void testLessThanOrEqualConversion() {
        ReferenceAttribute age = attr("age", DataType.INTEGER);
        Literal value = literal(30);
        LessThanOrEqual lte = new LessThanOrEqual(EMPTY, age, value, null);

        org.apache.iceberg.expressions.Expression icebergExpr = IcebergPushdownFilters.convert(lte);
        assertIcebergFilter(icebergExpr, "age", "30");
    }

    public void testGreaterThanConversion() {
        ReferenceAttribute salary = attr("salary", DataType.DOUBLE);
        Literal value = literal(50000.0);
        GreaterThan gt = new GreaterThan(EMPTY, salary, value, null);

        org.apache.iceberg.expressions.Expression icebergExpr = IcebergPushdownFilters.convert(gt);
        assertIcebergFilter(icebergExpr, "salary", ">");
    }

    public void testGreaterThanOrEqualConversion() {
        ReferenceAttribute salary = attr("salary", DataType.DOUBLE);
        Literal value = literal(50000.0);
        GreaterThanOrEqual gte = new GreaterThanOrEqual(EMPTY, salary, value, null);

        org.apache.iceberg.expressions.Expression icebergExpr = IcebergPushdownFilters.convert(gte);
        assertIcebergFilter(icebergExpr, "salary", "50000");
    }

    public void testInConversion() {
        ReferenceAttribute dept = attr("dept", DataType.KEYWORD);
        List<org.elasticsearch.xpack.esql.core.expression.Expression> values = Arrays.asList(literal("eng"), literal("sales"));
        In in = new In(EMPTY, dept, values);

        org.apache.iceberg.expressions.Expression icebergExpr = IcebergPushdownFilters.convert(in);
        assertIcebergFilter(icebergExpr, "dept", "in");
    }

    public void testIsNullConversion() {
        ReferenceAttribute middleName = attr("middle_name", DataType.KEYWORD);
        IsNull isNull = new IsNull(EMPTY, middleName);

        org.apache.iceberg.expressions.Expression icebergExpr = IcebergPushdownFilters.convert(isNull);
        assertIcebergFilter(icebergExpr, "middle_name", "null");
    }

    public void testIsNotNullConversion() {
        ReferenceAttribute email = attr("email", DataType.KEYWORD);
        IsNotNull isNotNull = new IsNotNull(EMPTY, email);

        org.apache.iceberg.expressions.Expression icebergExpr = IcebergPushdownFilters.convert(isNotNull);
        assertIcebergFilter(icebergExpr, "email");
    }

    public void testRangeConversion() {
        ReferenceAttribute age = attr("age", DataType.INTEGER);
        Literal lower = literal(25);
        Literal upper = literal(65);
        Range range = new Range(EMPTY, age, lower, true, upper, true, null);

        org.apache.iceberg.expressions.Expression icebergExpr = IcebergPushdownFilters.convert(range);
        assertIcebergFilter(icebergExpr, "age", "and");
    }

    public void testAndConversion() {
        ReferenceAttribute age = attr("age", DataType.INTEGER);
        Equals eq1 = new Equals(EMPTY, age, literal(25));
        ReferenceAttribute salary = attr("salary", DataType.DOUBLE);
        GreaterThan gt = new GreaterThan(EMPTY, salary, literal(50000.0), null);
        And and = new And(EMPTY, eq1, gt);

        org.apache.iceberg.expressions.Expression icebergExpr = IcebergPushdownFilters.convert(and);
        assertIcebergFilter(icebergExpr, "and");
    }

    public void testOrConversion() {
        ReferenceAttribute age = attr("age", DataType.INTEGER);
        LessThan lt = new LessThan(EMPTY, age, literal(25), null);
        GreaterThan gt = new GreaterThan(EMPTY, age, literal(65), null);
        Or or = new Or(EMPTY, lt, gt);

        org.apache.iceberg.expressions.Expression icebergExpr = IcebergPushdownFilters.convert(or);
        assertIcebergFilter(icebergExpr, "or");
    }

    public void testNotConversion() {
        ReferenceAttribute active = attr("active", DataType.BOOLEAN);
        Equals eq = new Equals(EMPTY, active, literal(true));
        Not not = new Not(EMPTY, eq);

        org.apache.iceberg.expressions.Expression icebergExpr = IcebergPushdownFilters.convert(not);
        assertIcebergFilter(icebergExpr, "not");
    }

    public void testUnsupportedExpression() {
        // Create an unsupported expression (field on right side)
        ReferenceAttribute age = attr("age", DataType.INTEGER);
        Equals eq = new Equals(EMPTY, literal(25), age); // Value on left, field on right - not supported

        org.apache.iceberg.expressions.Expression icebergExpr = IcebergPushdownFilters.convert(eq);
        assertNull(icebergExpr);
    }

    public void testComplexExpression() {
        // (age >= 25 AND age <= 65) OR dept = "eng"
        ReferenceAttribute age = attr("age", DataType.INTEGER);
        GreaterThanOrEqual gte = new GreaterThanOrEqual(EMPTY, age, literal(25), null);
        LessThanOrEqual lte = new LessThanOrEqual(EMPTY, age, literal(65), null);
        And ageRange = new And(EMPTY, gte, lte);

        ReferenceAttribute dept = attr("dept", DataType.KEYWORD);
        Equals deptEq = new Equals(EMPTY, dept, literal("eng"));

        Or complex = new Or(EMPTY, ageRange, deptEq);

        org.apache.iceberg.expressions.Expression icebergExpr = IcebergPushdownFilters.convert(complex);
        assertIcebergFilter(icebergExpr, "or", "and");
    }
}
