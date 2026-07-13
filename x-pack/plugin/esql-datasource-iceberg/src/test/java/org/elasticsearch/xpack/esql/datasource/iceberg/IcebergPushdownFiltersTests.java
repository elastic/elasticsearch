/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.iceberg;

import org.apache.iceberg.expressions.Expression;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
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
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType;

/**
 * Unit tests for IcebergPushdownFilters.
 * Tests conversion of ESQL expressions to Iceberg filter expressions.
 */
public class IcebergPushdownFiltersTests extends ESTestCase {

    private static final Source SOURCE = Source.EMPTY;

    public void testEqualsStringField() {
        FieldAttribute field = createField("name", DataType.KEYWORD);
        Literal value = literal("Alice");

        Equals equals = new Equals(SOURCE, field, value);
        Expression result = IcebergPushdownFilters.convert(equals);

        assertNotNull(result);
        String resultStr = result.toString();
        assertTrue("Expected field 'name' in: " + resultStr, resultStr.contains("name"));
        assertTrue("Expected value 'Alice' in: " + resultStr, resultStr.contains("Alice"));
    }

    public void testEqualsIntegerField() {
        FieldAttribute field = createField("age", DataType.INTEGER);
        Literal value = literal(25);

        Equals equals = new Equals(SOURCE, field, value);
        Expression result = IcebergPushdownFilters.convert(equals);

        assertNotNull(result);
        String resultStr = result.toString();
        // Value is converted to string representation
        assertTrue("Expected field 'age' in: " + resultStr, resultStr.contains("age"));
        assertTrue("Expected value '25' in: " + resultStr, resultStr.contains("25"));
    }

    public void testNotEquals() {
        FieldAttribute field = createField("status", DataType.KEYWORD);
        Literal value = literal("inactive");

        NotEquals notEquals = new NotEquals(SOURCE, field, value);
        Expression result = IcebergPushdownFilters.convert(notEquals);

        assertNotNull(result);
        String resultStr = result.toString();
        assertTrue("Expected field 'status' in: " + resultStr, resultStr.contains("status"));
        assertTrue("Expected value 'inactive' in: " + resultStr, resultStr.contains("inactive"));
    }

    public void testLessThan() {
        FieldAttribute field = createField("price", DataType.DOUBLE);
        Literal value = literal(100.0);

        LessThan lessThan = new LessThan(SOURCE, field, value);
        Expression result = IcebergPushdownFilters.convert(lessThan);

        assertNotNull(result);
        String resultStr = result.toString();
        assertTrue("Expected field 'price' in: " + resultStr, resultStr.contains("price"));
        assertTrue("Expected value '100.0' in: " + resultStr, resultStr.contains("100.0"));
    }

    public void testLessThanOrEqual() {
        FieldAttribute field = createField("quantity", DataType.INTEGER);
        Literal value = literal(10);

        LessThanOrEqual lessThanOrEqual = new LessThanOrEqual(SOURCE, field, value);
        Expression result = IcebergPushdownFilters.convert(lessThanOrEqual);

        assertNotNull(result);
        String resultStr = result.toString();
        assertTrue("Expected field 'quantity' in: " + resultStr, resultStr.contains("quantity"));
        assertTrue("Expected value '10' in: " + resultStr, resultStr.contains("10"));
    }

    public void testGreaterThan() {
        FieldAttribute field = createField("score", DataType.DOUBLE);
        Literal value = literal(90.0);

        GreaterThan greaterThan = new GreaterThan(SOURCE, field, value);
        Expression result = IcebergPushdownFilters.convert(greaterThan);

        assertNotNull(result);
        String resultStr = result.toString();
        assertTrue("Expected field 'score' in: " + resultStr, resultStr.contains("score"));
        assertTrue("Expected value '90.0' in: " + resultStr, resultStr.contains("90.0"));
    }

    public void testGreaterThanOrEqual() {
        FieldAttribute field = createField("level", DataType.INTEGER);
        Literal value = literal(5);

        GreaterThanOrEqual greaterThanOrEqual = new GreaterThanOrEqual(SOURCE, field, value);
        Expression result = IcebergPushdownFilters.convert(greaterThanOrEqual);

        assertNotNull(result);
        String resultStr = result.toString();
        assertTrue("Expected field 'level' in: " + resultStr, resultStr.contains("level"));
        assertTrue("Expected value '5' in: " + resultStr, resultStr.contains("5"));
    }

    public void testIsNull() {
        FieldAttribute field = createField("email", DataType.KEYWORD);

        IsNull isNull = new IsNull(SOURCE, field);
        Expression result = IcebergPushdownFilters.convert(isNull);

        assertNotNull(result);
        String resultStr = result.toString();
        assertTrue("Expected is_null in: " + resultStr, resultStr.contains("is_null"));
        assertTrue("Expected field 'email' in: " + resultStr, resultStr.contains("email"));
    }

    public void testIsNotNull() {
        FieldAttribute field = createField("email", DataType.KEYWORD);

        IsNotNull isNotNull = new IsNotNull(SOURCE, field);
        Expression result = IcebergPushdownFilters.convert(isNotNull);

        assertNotNull(result);
        String resultStr = result.toString();
        assertTrue("Expected not_null in: " + resultStr, resultStr.contains("not_null"));
        assertTrue("Expected field 'email' in: " + resultStr, resultStr.contains("email"));
    }

    public void testIn() {
        FieldAttribute field = createField("category", DataType.KEYWORD);
        List<org.elasticsearch.xpack.esql.core.expression.Expression> values = List.of(literal("A"), literal("B"), literal("C"));

        In inExpr = new In(SOURCE, field, values);
        Expression result = IcebergPushdownFilters.convert(inExpr);

        assertNotNull(result);
        String resultStr = result.toString();
        assertTrue("Expected field 'category' in: " + resultStr, resultStr.contains("category"));
        assertTrue("Expected 'in' operator in: " + resultStr, resultStr.contains("in"));
        assertTrue("Expected value 'A' in: " + resultStr, resultStr.contains("A"));
        assertTrue("Expected value 'B' in: " + resultStr, resultStr.contains("B"));
        assertTrue("Expected value 'C' in: " + resultStr, resultStr.contains("C"));
    }

    public void testRangeInclusiveBoth() {
        FieldAttribute field = createField("value", DataType.INTEGER);
        Literal lower = literal(10);
        Literal upper = literal(20);

        Range range = new Range(SOURCE, field, lower, true, upper, true, ZoneOffset.UTC);
        Expression result = IcebergPushdownFilters.convert(range);

        assertNotNull(result);
        String resultStr = result.toString();
        assertTrue("Expected field 'value' in: " + resultStr, resultStr.contains("value"));
        assertTrue("Expected value '10' in: " + resultStr, resultStr.contains("10"));
        assertTrue("Expected value '20' in: " + resultStr, resultStr.contains("20"));
        assertTrue("Expected 'and' operator in: " + resultStr, resultStr.toLowerCase(java.util.Locale.ROOT).contains("and"));
    }

    public void testRangeExclusiveBoth() {
        FieldAttribute field = createField("value", DataType.INTEGER);
        Literal lower = literal(10);
        Literal upper = literal(20);

        Range range = new Range(SOURCE, field, lower, false, upper, false, ZoneOffset.UTC);
        Expression result = IcebergPushdownFilters.convert(range);

        assertNotNull(result);
        String resultStr = result.toString();
        assertTrue("Expected field 'value' in: " + resultStr, resultStr.contains("value"));
        assertTrue("Expected value '10' in: " + resultStr, resultStr.contains("10"));
        assertTrue("Expected value '20' in: " + resultStr, resultStr.contains("20"));
        assertTrue("Expected 'and' operator in: " + resultStr, resultStr.toLowerCase(java.util.Locale.ROOT).contains("and"));
    }

    public void testAndExpression() {
        FieldAttribute field1 = createField("status", DataType.KEYWORD);
        FieldAttribute field2 = createField("active", DataType.BOOLEAN);
        Literal value1 = literal("approved");
        Literal value2 = literal(true);

        Equals equals1 = new Equals(SOURCE, field1, value1);
        Equals equals2 = new Equals(SOURCE, field2, value2);
        And and = new And(SOURCE, equals1, equals2);

        Expression result = IcebergPushdownFilters.convert(and);

        assertNotNull(result);
        String resultStr = result.toString();
        assertTrue("Expected field 'status' in: " + resultStr, resultStr.contains("status"));
        assertTrue("Expected value 'approved' in: " + resultStr, resultStr.contains("approved"));
        assertTrue("Expected field 'active' in: " + resultStr, resultStr.contains("active"));
        assertTrue("Expected value 'true' in: " + resultStr, resultStr.contains("true"));
        assertTrue("Expected 'and' operator in: " + resultStr, resultStr.toLowerCase(java.util.Locale.ROOT).contains("and"));
    }

    public void testOrExpression() {
        FieldAttribute field = createField("category", DataType.KEYWORD);
        Literal value1 = literal("A");
        Literal value2 = literal("B");

        Equals equals1 = new Equals(SOURCE, field, value1);
        Equals equals2 = new Equals(SOURCE, field, value2);
        Or or = new Or(SOURCE, equals1, equals2);

        Expression result = IcebergPushdownFilters.convert(or);

        assertNotNull(result);
        String resultStr = result.toString();
        assertTrue("Expected field 'category' in: " + resultStr, resultStr.contains("category"));
        assertTrue("Expected value 'A' in: " + resultStr, resultStr.contains("A"));
        assertTrue("Expected value 'B' in: " + resultStr, resultStr.contains("B"));
        assertTrue("Expected 'or' operator in: " + resultStr, resultStr.toLowerCase(java.util.Locale.ROOT).contains("or"));
    }

    public void testNotExpression() {
        FieldAttribute field = createField("status", DataType.KEYWORD);
        Literal value = literal("inactive");

        Equals equals = new Equals(SOURCE, field, value);
        Not not = new Not(SOURCE, equals);

        Expression result = IcebergPushdownFilters.convert(not);

        assertNotNull(result);
        String resultStr = result.toString();
        assertTrue("Expected 'not' operator in: " + resultStr, resultStr.toLowerCase(java.util.Locale.ROOT).contains("not"));
        assertTrue("Expected field 'status' in: " + resultStr, resultStr.contains("status"));
        assertTrue("Expected value 'inactive' in: " + resultStr, resultStr.contains("inactive"));
    }

    public void testNestedAndOrExpression() {
        FieldAttribute field1 = createField("status", DataType.KEYWORD);
        FieldAttribute field2 = createField("priority", DataType.INTEGER);
        FieldAttribute field3 = createField("category", DataType.KEYWORD);

        Equals statusActive = new Equals(SOURCE, field1, literal("active"));
        GreaterThan highPriority = new GreaterThan(SOURCE, field2, literal(5));
        Equals categoryA = new Equals(SOURCE, field3, literal("A"));

        And andExpr = new And(SOURCE, statusActive, highPriority);
        Or orExpr = new Or(SOURCE, andExpr, categoryA);

        Expression result = IcebergPushdownFilters.convert(orExpr);

        assertNotNull(result);
        String resultStr = result.toString();
        assertTrue("Expected field 'status' in: " + resultStr, resultStr.contains("status"));
        assertTrue("Expected value 'active' in: " + resultStr, resultStr.contains("active"));
        assertTrue("Expected field 'priority' in: " + resultStr, resultStr.contains("priority"));
        assertTrue("Expected value '5' in: " + resultStr, resultStr.contains("5"));
        assertTrue("Expected field 'category' in: " + resultStr, resultStr.contains("category"));
        assertTrue("Expected value 'A' in: " + resultStr, resultStr.contains("A"));
    }

    public void testNullForUnsupportedExpression() {
        // A literal by itself should return null (not a supported predicate)
        Literal literal = literal("value");
        Expression result = IcebergPushdownFilters.convert(literal);

        assertNull(result);
    }

    public void testNullForAndWithUnsupportedChild() {
        FieldAttribute field = createField("status", DataType.KEYWORD);
        Equals equals = new Equals(SOURCE, field, literal("active"));
        Literal unsupported = literal("value");

        And and = new And(SOURCE, equals, unsupported);
        Expression result = IcebergPushdownFilters.convert(and);

        // Should return null because one child is unsupported
        assertNull(result);
    }

    public void testNullForOrWithUnsupportedChild() {
        FieldAttribute field = createField("status", DataType.KEYWORD);
        Equals equals = new Equals(SOURCE, field, literal("active"));
        Literal unsupported = literal("value");

        Or or = new Or(SOURCE, equals, unsupported);
        Expression result = IcebergPushdownFilters.convert(or);

        // Should return null because one child is unsupported
        assertNull(result);
    }

    public void testNullForNotWithUnsupportedChild() {
        Literal unsupported = literal("value");
        Not not = new Not(SOURCE, unsupported);

        Expression result = IcebergPushdownFilters.convert(not);

        // Should return null because child is unsupported
        assertNull(result);
    }

    public void testInWithNonFoldableValue() {
        FieldAttribute field = createField("category", DataType.KEYWORD);
        FieldAttribute nonFoldable = createField("other", DataType.KEYWORD);
        List<org.elasticsearch.xpack.esql.core.expression.Expression> values = List.of(
            literal("A"),
            nonFoldable // Not foldable
        );

        In inExpr = new In(SOURCE, field, values);
        Expression result = IcebergPushdownFilters.convert(inExpr);

        // Should return null because not all values are foldable
        assertNull(result);
    }

    public void testEqualsWithNonFoldableValue() {
        FieldAttribute field1 = createField("name", DataType.KEYWORD);
        FieldAttribute field2 = createField("alias", DataType.KEYWORD);

        // field = another_field (not a literal)
        Equals equals = new Equals(SOURCE, field1, field2);
        Expression result = IcebergPushdownFilters.convert(equals);

        // Should return null because right side is not foldable
        assertNull(result);
    }

    public void testBytesRefValueConversion() {
        FieldAttribute field = createField("name", DataType.KEYWORD);
        Literal value = new Literal(SOURCE, new BytesRef("test_value"), DataType.KEYWORD);

        Equals equals = new Equals(SOURCE, field, value);
        Expression result = IcebergPushdownFilters.convert(equals);

        assertNotNull(result);
        // BytesRef should be converted to string
        assertTrue(result.toString().contains("test_value"));
    }

    private FieldAttribute createField(String name, DataType dataType) {
        return new FieldAttribute(SOURCE, name, new EsField(name, dataType, Collections.emptyMap(), true, TimeSeriesFieldType.NONE));
    }

    private Literal literal(Object value) {
        DataType dataType;
        Object literalValue = value;
        if (value instanceof String s) {
            dataType = DataType.KEYWORD;
            literalValue = new BytesRef(s);
        } else if (value instanceof Integer) {
            dataType = DataType.INTEGER;
        } else if (value instanceof Long) {
            dataType = DataType.LONG;
        } else if (value instanceof Double) {
            dataType = DataType.DOUBLE;
        } else if (value instanceof Boolean) {
            dataType = DataType.BOOLEAN;
        } else {
            dataType = DataType.KEYWORD;
        }
        return new Literal(SOURCE, literalValue, dataType);
    }
}
