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
import org.elasticsearch.xpack.sql.expression.function.scalar.arithmetic.Neg;
import org.elasticsearch.xpack.sql.type.DataType;

public class ExpressionTests extends ESTestCase {

    private final SqlParser parser = new SqlParser();

    public void testTokenFunctionName() throws Exception {
        Expression lt = parser.createExpression("LEFT()");
        assertEquals(UnresolvedFunction.class, lt.getClass());
        UnresolvedFunction uf = (UnresolvedFunction) lt;
        assertEquals("LEFT", uf.functionName());
    }

    public void testLiteralLong() throws Exception {
        Expression lt = parser.createExpression(String.valueOf(Long.MAX_VALUE));
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals(Long.MAX_VALUE, l.value());
        assertEquals(DataType.LONG, l.dataType());
    }

    public void testLiteralLongNegative() throws Exception {
        // Long.MIN_VALUE doesn't work since it is being interpreted as negate positive.long which is 1 higher than Long.MAX_VALUE
        Expression lt = parser.createExpression(String.valueOf(-Long.MAX_VALUE));
        assertEquals(Neg.class, lt.getClass());
        Neg n = (Neg) lt;
        assertTrue(n.foldable());
        assertEquals(-Long.MAX_VALUE, n.fold());
        assertEquals(DataType.LONG, n.dataType());
    }

    public void testLiteralInteger() throws Exception {
        Expression lt = parser.createExpression(String.valueOf(Integer.MAX_VALUE));
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals(Integer.MAX_VALUE, l.value());
        assertEquals(DataType.INTEGER, l.dataType());
    }

    public void testLiteralIntegerWithShortValue() throws Exception {
        Expression lt = parser.createExpression(String.valueOf(Short.MAX_VALUE));
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals(Integer.valueOf(Short.MAX_VALUE), l.value());
        assertEquals(DataType.INTEGER, l.dataType());
    }

    public void testLiteralIntegerWithByteValue() throws Exception {
        Expression lt = parser.createExpression(String.valueOf(Byte.MAX_VALUE));
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals(Integer.valueOf(Byte.MAX_VALUE), l.value());
        assertEquals(DataType.INTEGER, l.dataType());
    }
}