/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.sql.type.DataType;

public class ExpressionTests extends ESTestCase {

    private final SqlParser parser = new SqlParser();

    public void testTokenFunctionName() {
        Expression lt = parser.createExpression("LEFT()");
        assertEquals(UnresolvedFunction.class, lt.getClass());
        UnresolvedFunction uf = (UnresolvedFunction) lt;
        assertEquals("LEFT", uf.functionName());
    }


    public void testLiteralBoolean() {
        Expression lt = parser.createExpression("TRUE");
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals(Boolean.TRUE, l.value());
        assertEquals(DataType.BOOLEAN, l.dataType());
    }

    public void testLiteralDouble() {
        Expression lt = parser.createExpression(String.valueOf(Double.MAX_VALUE));
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals(Double.MAX_VALUE, l.value());
        assertEquals(DataType.DOUBLE, l.dataType());
    }

    public void testLiteralDoubleNegative() {
        Expression lt = parser.createExpression(String.valueOf(Double.MIN_VALUE));
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals(Double.MIN_VALUE, l.value());
        assertEquals(DataType.DOUBLE, l.dataType());
    }

    public void testLiteralDoublePositive() {
        Expression lt = parser.createExpression("+" + Double.MAX_VALUE);
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals(Double.MAX_VALUE, l.value());
        assertEquals(DataType.DOUBLE, l.dataType());
    }

    public void testLiteralLong() {
        Expression lt = parser.createExpression(String.valueOf(Long.MAX_VALUE));
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals(Long.MAX_VALUE, l.value());
        assertEquals(DataType.LONG, l.dataType());
    }

    public void testLiteralLongNegative() {
        Expression lt = parser.createExpression(String.valueOf(Long.MIN_VALUE));
        assertTrue(lt.foldable());
        assertEquals(Long.MIN_VALUE, lt.fold());
        assertEquals(DataType.LONG, lt.dataType());
    }

    public void testLiteralLongPositive() {
        Expression lt = parser.createExpression("+" + String.valueOf(Long.MAX_VALUE));
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals(Long.MAX_VALUE, l.value());
        assertEquals(DataType.LONG, l.dataType());
    }

    public void testLiteralInteger() {
        Expression lt = parser.createExpression(String.valueOf(Integer.MAX_VALUE));
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals(Integer.MAX_VALUE, l.value());
        assertEquals(DataType.INTEGER, l.dataType());
    }

    public void testLiteralIntegerWithShortValue() {
        Expression lt = parser.createExpression(String.valueOf(Short.MAX_VALUE));
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals((int) Short.MAX_VALUE, l.value());
        assertEquals(DataType.INTEGER, l.dataType());
    }

    public void testLiteralIntegerWithByteValue() {
        Expression lt = parser.createExpression(String.valueOf(Byte.MAX_VALUE));
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals((int) Byte.MAX_VALUE, l.value());
        assertEquals(DataType.INTEGER, l.dataType());
    }

    public void testLiteralIntegerInvalid() {
        ParsingException ex = expectThrows(ParsingException.class, () -> parser.createExpression("123456789098765432101"));
        assertEquals("Number [123456789098765432101] is too large", ex.getErrorMessage());
    }

    public void testLiteralDecimalTooBig() {
        ParsingException ex = expectThrows(ParsingException.class, () -> parser.createExpression("1.9976931348623157e+308"));
        assertEquals("Number [1.9976931348623157e+308] is too large", ex.getErrorMessage());
    }

    public void testLiteralTimesLiteral() {
        Expression expr = parser.createExpression("10*2");
        assertEquals(Mul.class, expr.getClass());
        Mul mul = (Mul) expr;
        assertEquals("10 * 2", mul.name());
        assertEquals(DataType.INTEGER, mul.dataType());
    }

    public void testFunctionTimesLiteral() {
        Expression expr = parser.createExpression("PI()*2");
        assertEquals(Mul.class, expr.getClass());
        Mul mul = (Mul) expr;
        assertEquals("(PI) * 2", mul.name());
    }
}
