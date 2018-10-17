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
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.sql.type.DataType;

import static org.hamcrest.core.StringStartsWith.startsWith;

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

    public void testComplexArithmetic() {
        Expression expr = parser.createExpression("-(((a-2)-(-3))+b)");
        assertEquals(Neg.class, expr.getClass());
        Neg neg = (Neg) expr;
        assertThat(neg.name(), startsWith("-(((a) - 2) - -3) + (b)#"));
        assertEquals(1, neg.children().size());
        assertEquals(Add.class, neg.children().get(0).getClass());
        Add add = (Add) neg.children().get(0);
        assertEquals("(((a) - 2) - -3) + (b)", add.name());
        assertEquals(2, add.children().size());
        assertEquals("?b", add.children().get(1).toString());
        assertEquals(Sub.class, add.children().get(0).getClass());
        Sub sub1 = (Sub) add.children().get(0);
        assertEquals("((a) - 2) - -3", sub1.name());
        assertEquals(2, sub1.children().size());
        assertEquals(Literal.class, sub1.children().get(1).getClass());
        assertEquals("-3", ((Literal) sub1.children().get(1)).name());
        assertEquals(Sub.class, sub1.children().get(0).getClass());
        Sub sub2 = (Sub) sub1.children().get(0);
        assertEquals(2, sub2.children().size());
        assertEquals("?a", sub2.children().get(0).toString());
        assertEquals(Literal.class, sub2.children().get(1).getClass());
        assertEquals("2", ((Literal) sub2.children().get(1)).name());
    }
}
