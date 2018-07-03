/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.type.DataType;

public class ExpressionTests extends ESTestCase {

    private final SqlParser parser = new SqlParser();

    public void testLiteralLong() throws Exception {
        Expression lt = parser.createExpression(String.valueOf(Long.MAX_VALUE));
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals(Long.MAX_VALUE, l.value());
        assertEquals(DataType.LONG, l.dataType());
    }

    public void testLiteralInteger() throws Exception {
        Expression lt = parser.createExpression(String.valueOf(Integer.MAX_VALUE));
        assertEquals(Literal.class, lt.getClass());
        Literal l = (Literal) lt;
        assertEquals(Integer.MAX_VALUE, l.value());
        assertEquals(DataType.INTEGER, l.dataType());
    }
}